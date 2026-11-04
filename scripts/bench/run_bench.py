#!/usr/bin/env python3
"""基准起服务脚手架：「最小装配 + 真 hypercorn」起一个端口，再调 `http2_vs_http1.py` 取数。

**为什么要这一层**：评审后的 8B_01 判据是「功能五项验证 + **脚本与报告可复现**」，而基准脚本只测客户端，
被测服务得有人起。整套平台栈起法会把与传输无关的开销压进来——每请求要过租户远程解析、审计发件箱、
平台库写，两次实测都卡在 10s 读超时 / 连接池耗尽。因此这里用**同一份业务路由 + 内存存储 + 桩租户源**
起真 hypercorn 端口，两档只差传输层。

**与生产起法的差异**（写死在此，避免判读时误解）：

- 不启 lifespan：`build_frame_app()` 只构造应用对象，后台循环（延迟推进器 / 重放执行器）不跑；
- 租户源是桩（`demo` → 固定主键 7001），不经平台租户服务；
- 审计发件箱不装配；
- 平台库是临时 SQLite（只建 `msg_topic` / `msg_partition` 两张表，连接池 32+8 常驻）。

用法（工作目录随仓，解释器必须是能导入 `bms_core` 的那个）::

    uv run --project backend python scripts/bench/run_bench.py --total 1200 --concurrency 16 --warmup 200
    uv run --project backend python scripts/bench/run_bench.py --connection-requests 5000   # 复现连接退休
    uv run --project backend python scripts/bench/run_bench.py --json .cache/bench/http2.json

退出码：0 已产出报告；非 0 为基准脚本自身的退出码。
"""

from __future__ import annotations

import argparse
import asyncio
import importlib
import socket
import subprocess
import sys
import tempfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
"""msg 仓库根（本文件位于 `scripts/bench/`）。"""

BACKEND = REPO_ROOT / "backend"
"""后端工作区根（`bms_core` 与 `services` 的导入根）。"""

for _path in (BACKEND, BACKEND / "services" / "msg" / "src"):
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))

from bms_core.core.config import Settings  # noqa: E402
from bms_core.core.plugin import default_plugin_registry  # noqa: E402
from bms_core.core.run import serve_service  # noqa: E402
from bms_core.db.tenant import TenantContext  # noqa: E402
from bms_core.models.base import Base  # noqa: E402
from httpx import AsyncClient  # noqa: E402
from sqlalchemy.ext.asyncio import create_async_engine  # noqa: E402
from tests_support.frame_app import build_frame_app  # noqa: E402

TENANT = "demo"
"""基准租户编码（桩租户源认这一个）。"""

NAMESPACE = "msg"
"""命名空间（须为已启用 module_key）。"""

TOPIC = "bench.http2"
"""基准主题（脚本先建，已存在不算失败）。"""

TENANT_ID = 7001
"""桩租户主键（真栈里是雪花串，这里固定即可）。"""

HEADERS = {"X-Tenant-ID": TENANT}
"""租户头（与基准脚本一致）。"""


class DemoTenantSource:
    """租户源桩：`demo` → 固定主键（不经远程解析，避免把租户服务算进传输基准）。"""

    async def by_code(self, code: str) -> TenantContext:
        """按编码取租户。

        Args:
            code: 租户编码。

        Returns:
            TenantContext: 桩租户上下文。
        """
        return TenantContext(code=code, db_key=f"tenant_{code}", name=code, tenant_id=TENANT_ID)

    async def by_id(self, tenant_id: str) -> TenantContext:
        """按主键取租户。

        Args:
            tenant_id: 租户主键（桩实现忽略）。

        Returns:
            TenantContext: 桩租户上下文。
        """
        del tenant_id
        return TenantContext(code=TENANT, db_key="tenant_demo", name=TENANT, tenant_id=TENANT_ID)

    async def single_active(self) -> TenantContext:
        """取唯一活跃租户。

        Returns:
            TenantContext: 桩租户上下文。
        """
        return TenantContext(code=TENANT, db_key="tenant_demo", name=TENANT, tenant_id=TENANT_ID)


def free_port() -> int:
    """取一个空闲本地端口。

    Returns:
        int: 可用端口号。
    """
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


async def _wait_listening(client: AsyncClient) -> str:
    """等端口可达（以「能拿到任意 HTTP 响应」为准，最多 10s）。

    为什么不用 `/healthz == 200` 判就绪：最小装配**不挂健康探针**（实测 `/healthz` 返回 **404**），且不启
    lifespan、应用运行时没有初始化完成的状态；数据面从端口通的那一刻起就可服务。就绪判据因此收敛为
    「端口通了」，真正的可用性由随后的建主题请求验证（返回 200 或「已存在」才继续）。

    Args:
        client: 探测客户端。

    Returns:
        str: 首次拿到的 `/healthz` 状态行（仅用于打印，不参与判定）。

    Raises:
        SystemExit: 10s 内端口一直不可达。
    """
    last = "（无响应）"
    for _ in range(200):
        try:
            response = await client.get("/healthz")
            return f"/healthz → HTTP {response.status_code}"
        except Exception as exc:  # noqa: BLE001 - 端口未监听时任何连接异常都只代表「还没起来」
            last = repr(exc)
        await asyncio.sleep(0.05)
    raise SystemExit(f"[bench] 服务 10s 内未监听：{last}")


async def _ensure_topic(client: AsyncClient) -> None:
    """建基准主题（已存在不算失败；服务刚起时重试到可用）。

    Args:
        client: 客户端。

    Raises:
        SystemExit: 建主题失败。
    """
    last = ""
    for _ in range(50):
        response = await client.post(
            "/api/v1/msg/admin/topics",
            json={"namespace": NAMESPACE, "name": TOPIC, "partition_count": 1},
            headers=HEADERS,
        )
        last = f"HTTP {response.status_code} {response.text[:160]}"
        if response.status_code == 200:
            print(f"[bench] 建主题：{NAMESPACE}/{TOPIC} 已创建")
            return
        try:
            if response.json().get("code") == 350008:
                print(f"[bench] 建主题：{NAMESPACE}/{TOPIC} 已存在")
                return
        except ValueError:
            pass
        await asyncio.sleep(0.1)
    raise SystemExit(f"[bench] 建主题失败：{last}")


async def main() -> int:
    """起端口 → 建主题 → 调基准脚本 → 收端口。

    Returns:
        int: 基准脚本退出码（起不来为 1）。
    """
    parser = argparse.ArgumentParser(description="HTTP/2 基准起服务脚手架（最小装配 + 真 hypercorn）")
    parser.add_argument("--total", type=int, default=1_200, help="每档测量条数")
    parser.add_argument("--concurrency", type=int, default=16, help="并发在途请求数")
    parser.add_argument("--warmup", type=int, default=200, help="每档预热条数")
    parser.add_argument("--payload-bytes", type=int, default=1_024, help="载荷字节数（base64 后传输）")
    parser.add_argument(
        "--connection-requests",
        type=int,
        default=800,
        help="单条连接承载的请求上限（须小于服务端退休阈值 1000；传大值可复现连接退休现象）",
    )
    parser.add_argument("--json", default=str(REPO_ROOT / ".cache" / "bench" / "http2.json"), help="原始结果落点")
    args = parser.parse_args()

    importlib.import_module("services.module_registry")
    importlib.import_module("bms_core.permission.null")
    importlib.import_module("bms_core.tenant_module.null")
    default_plugin_registry().reset()

    workdir = Path(tempfile.mkdtemp())
    engine = create_async_engine(
        f"sqlite+aiosqlite:///{workdir / 'bench.db'}", pool_size=32, max_overflow=8
    )
    async with engine.begin() as connection:
        for table in ("msg_topic", "msg_partition"):
            await connection.run_sync(Base.metadata.tables[table].create)

    app = build_frame_app(engine=engine, tenant_source=DemoTenantSource())
    port = free_port()
    settings = Settings()
    settings.server.host = "127.0.0.1"
    settings.server.port = port
    settings.server.graceful_timeout_seconds = 1
    task = asyncio.create_task(serve_service(app, settings))
    base_url = f"http://127.0.0.1:{port}"
    print(f"[bench] 服务：{base_url}（最小装配 + 真 hypercorn，h2c prior knowledge 可用）")

    try:
        async with AsyncClient(base_url=base_url, timeout=10.0) as probe:
            ready = await _wait_listening(probe)
            print(f"[bench] 服务就绪：{ready}")
            await _ensure_topic(probe)
        child = await asyncio.create_subprocess_exec(
            sys.executable,
            str(REPO_ROOT / "scripts" / "bench" / "http2_vs_http1.py"),
            "--base-url",
            base_url,
            "--tenant",
            TENANT,
            "--namespace",
            NAMESPACE,
            "--topic",
            TOPIC,
            "--total",
            str(args.total),
            "--concurrency",
            str(args.concurrency),
            "--warmup",
            str(args.warmup),
            "--payload-bytes",
            str(args.payload_bytes),
            "--connection-requests",
            str(args.connection_requests),
            "--json",
            args.json,
            cwd=str(REPO_ROOT),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        out, _ = await child.communicate()
        print(out.decode("utf-8", errors="replace"))
        return child.returncode or 0
    finally:
        task.cancel()
        try:
            await asyncio.wait_for(task, 10)
        except (asyncio.CancelledError, TimeoutError):
            pass
        await engine.dispose()


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
