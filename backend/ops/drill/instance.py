"""隔离演练实例启动器（任务 07-6-1）：真装配子进程入口。

由 `ops.drill.runtime.DrillInstance` 以 `python -m ops.drill.instance` 拉起，**不直接手工调用**：
父进程负责选端口、传数据根与水位旋钮，本模块负责「自建环境 → 真装配应用 → 起服务」。

三处演练专用替代（逐条写入记录 `deviations`，不隐去）：

- 租户源替身：固定单租户，不连租户服务（否则真装配需要平台侧租户数据）；
- `[permission] provider=null`：权限链不在六类判据范围；
- SQLite 元数据库：避免演练污染共享 MySQL，且临时根可整体删除。

其余全部是真装配：真路由、真文件存储、真水位、真探针、真指标。
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
from pathlib import Path

from bms_msg.core.config import (
    DRILL_INSTANCE_PORT_ENV,
    DRILL_INSTANCE_ROOT_ENV,
    DRILL_TENANT_CODE,
    DRILL_TENANT_ID,
    STORAGE_REJECT_RATIO_ENV,
    STORAGE_WARN_RATIO_ENV,
)

__all__ = ["build_env", "ensure_signing_keys"]

_KEYS_DIR_NAME = "keys"
"""签名密钥目录名（落在演练工作根下，用完随临时根删除）。"""

_PRIVATE_KEY_FILE = "priv.pem"
"""私钥文件名。"""

_PUBLIC_KEY_FILE = "pub.pem"
"""公钥文件名。"""

_KEY_ID = "drill"
"""令牌密钥标识（单键；隔离实例不验外部令牌）。"""


def ensure_signing_keys(work_dir: Path) -> tuple[str, str]:
    """确保演练签名密钥存在（缺则生成 RSA 2048，已有则复用）。

    Args:
        work_dir: 演练工作根。

    Returns:
        tuple[str, str]: 私钥 PEM 与公钥 PEM。
    """
    keys_dir = work_dir / _KEYS_DIR_NAME
    keys_dir.mkdir(parents=True, exist_ok=True)
    priv_path = keys_dir / _PRIVATE_KEY_FILE
    pub_path = keys_dir / _PUBLIC_KEY_FILE
    if priv_path.is_file() and pub_path.is_file():
        return priv_path.read_text(encoding="utf-8"), pub_path.read_text(encoding="utf-8")

    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import rsa

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    private_pem = key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    ).decode("utf-8")
    public_pem = (
        key.public_key()
        .public_bytes(serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo)
        .decode("utf-8")
    )
    priv_path.write_text(private_pem, encoding="utf-8")
    pub_path.write_text(public_pem, encoding="utf-8")
    return private_pem, public_pem


def build_env(work_dir: Path, port: int) -> dict[str, str]:
    """构造隔离实例的环境变量（唯一装配入口，与 `deploy/drill/drill.env.example` 对齐）。

    Args:
        work_dir: 演练工作根。
        port: 实例监听端口（只绑 127.0.0.1）。

    Returns:
        dict[str, str]: 完整环境变量。
    """
    root = Path(work_dir).resolve()
    private_pem, public_pem = ensure_signing_keys(root)
    keys = json.dumps({_KEY_ID: {"public_key": public_pem, "private_key": private_pem, "algorithm": "RS256"}})
    data_root = root / "var"
    data_root.mkdir(parents=True, exist_ok=True)
    env = dict(os.environ)
    env.update(
        {
            "BMS_ENV": "test",
            "MSG_DATA_DIR": str(data_root),
            "BMS_DATABASE__AUTO_CREATE": "true",
            "BMS_DATABASE__PLATFORM__URL": f"sqlite+aiosqlite:///{(root / 'platform.db').as_posix()}",
            "BMS_DATABASE__TENANTS__URL": f"sqlite+aiosqlite:///{(root / 'tenant.db').as_posix()}",
            "BMS_DATABASE__ARCHIVE__URL": f"sqlite+aiosqlite:///{(root / 'archive.db').as_posix()}",
            "BMS_PERMISSION__PROVIDER": "null",
            "BMS_REGISTRY__REGISTER_ON_STARTUP": "false",
            "BMS_SERVER__HOST": "127.0.0.1",
            "BMS_SERVER__PORT": str(port),
            "BMS_LOG__LEVEL": "WARNING",
            "BMS_SERVICE_TOKEN__KEYS": keys,
            "BMS_SERVICE_TOKEN__ACTIVE_KID": _KEY_ID,
            "NO_PROXY": "127.0.0.1,localhost",
            "no_proxy": "127.0.0.1,localhost",
        }
    )
    # 水位阈值必须成对给出且满足 `warn < reject`：只改 reject 会启动即 `ValueError`。
    # 父进程（磁盘满场景）会给成对的滞回阈值；手工直启时按 reject 的一半兜底。
    reject_raw = (env.get(STORAGE_REJECT_RATIO_ENV) or "").strip()
    if reject_raw and not (env.get(STORAGE_WARN_RATIO_ENV) or "").strip():
        env[STORAGE_WARN_RATIO_ENV] = str(round(float(reject_raw) / 2, 6))
    return env


class DrillTenantSource:
    """演练实例的租户源替身：固定返回同一租户（不连租户服务）。"""

    def __init__(self, tenant: object) -> None:
        """初始化。

        Args:
            tenant: 租户上下文（由 `main` 构造，避免本模块导入基座类型）。
        """
        self._tenant = tenant

    async def by_code(self, code: str, *, readonly: bool = False) -> object:
        """按租户编码解析（恒返回固定租户）。

        Args:
            code: 租户编码。
            readonly: 是否只读（替身忽略）。

        Returns:
            object: 租户上下文。
        """
        return self._tenant

    async def by_domain(self, domain: str, *, readonly: bool = False) -> object:
        """按域名解析（恒返回固定租户）。

        Args:
            domain: 域名。
            readonly: 是否只读（替身忽略）。

        Returns:
            object: 租户上下文。
        """
        return self._tenant

    async def by_id(self, tenant_id: str, *, readonly: bool = False) -> object:
        """按主键解析（恒返回固定租户）。

        Args:
            tenant_id: 租户主键。
            readonly: 是否只读（替身忽略）。

        Returns:
            object: 租户上下文。
        """
        return self._tenant

    async def single_active(self) -> object:
        """单租户模式下的唯一活跃租户。

        Returns:
            object: 租户上下文。
        """
        return self._tenant


def main() -> None:
    """按父进程给定的端口与数据根启动真装配实例（阻塞至收到退出信号）。"""
    port = int(os.environ[DRILL_INSTANCE_PORT_ENV])
    work_dir = Path(os.environ[DRILL_INSTANCE_ROOT_ENV]).resolve()
    os.environ.update(build_env(work_dir, port))

    backend = Path(__file__).resolve().parents[2]
    for path in (backend, backend / "services" / "msg" / "src"):
        if str(path) not in sys.path:
            sys.path.insert(0, str(path))

    from bms_core.core.config import get_settings
    from bms_core.core.logging import configure_logging
    from bms_core.core.run import serve_service
    from bms_core.core.service import ServiceRuntime
    from bms_core.db.tenant import TenantContext

    from bms_msg.main import ApplicationFactory

    tenant = TenantContext(
        code=DRILL_TENANT_CODE,
        db_key=f"tenant_{DRILL_TENANT_CODE}",
        name="演练租户",
        tenant_id=DRILL_TENANT_ID,
    )
    settings = get_settings()
    configure_logging(settings)
    app = ApplicationFactory().create(None)
    app.state.tenant_source = DrillTenantSource(tenant)
    runtime = getattr(app.state, "service_runtime", None)
    asyncio.run(serve_service(app, settings, on_drain=runtime if isinstance(runtime, ServiceRuntime) else None))


if __name__ == "__main__":
    main()
