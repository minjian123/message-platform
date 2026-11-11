"""演练运行时（任务 07-6-1）：隔离实例生命周期 + HTTP 观测/操作客户端 + 三段采样。

**为什么要真子进程**：崩溃场景要「真进程被杀」，进程内演练证明不了重启后已确认消息可读；
**为什么客户端自己算**：指标直读 `/metrics` 文本，不依赖 Prometheus 可达（隔离实例没有指标入库）。
"""

from __future__ import annotations

import json
import math
import os
import shutil
import signal
import socket
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

import httpx
from bms_core.core.concurrent import ConcurrentStableDict, ConcurrentStableFrozenDict

from bms_msg.core.config import (
    DRILL_HTTP_TIMEOUT_SEC,
    DRILL_INSTANCE_PORT_ENV,
    DRILL_INSTANCE_ROOT_ENV,
    DRILL_OUTPUT_TAIL_CHARS,
    DRILL_PROBE_INTERVAL_MS,
    DRILL_READY_TIMEOUT_SEC,
    STORAGE_REJECT_RATIO_ENV,
    STORAGE_WARN_RATIO_ENV,
)
from ops.drill.env import DrillConfig, DrillError
from ops.drill.model import PhaseSample

__all__ = [
    "DrillHttp",
    "DrillInstance",
    "free_port",
    "now_iso",
    "parse_metrics",
    "payload_of",
    "sample_phase",
    "tight_watermark",
]


def tight_watermark(work_dir: Path, fill_mb: int) -> tuple[float, float, int]:
    """按当前磁盘用量算出「告警 / 拒绝水位」与实际填充量（磁盘满场景专用）。

    水位是**滞回**判定：进入拒绝后必须降到 `warn` 以下才解除。所以两个阈值必须夹住基线：

    - `warn = 基线 + 填充占比 / 2`：恢复后用量回到基线即 `< warn`，拒绝自动解除；
    - `reject = 基线 + 填充占比`：填充写满后四位小数正好跨到它（判定用 `round(used,4) >= reject`）。

    填充量至少占总容量 0.04%（4 位小数的可分辨下限），小盘自动放大到该值。

    Args:
        work_dir: 演练工作根（与实例数据根同卷）。
        fill_mb: 期望填充量（MiB）。

    Returns:
        tuple[float, float, int]: 告警水位、拒绝水位、实际填充量（MiB）。

    Raises:
        DrillError: 卷已接近满（无法再用受控填充制造跨阈）。
    """
    usage = shutil.disk_usage(work_dir)
    total_mb = max(usage.total, 1) / (1024 * 1024)
    need_mb = max(fill_mb, math.ceil(total_mb * 0.0004))
    base = round(usage.used / usage.total, 4)
    delta = round(need_mb / total_mb, 4)
    reject = round(base + delta, 4)
    warn = round(base + delta / 2, 4)
    if reject >= 0.999 or warn <= base:
        raise DrillError(f"drill.disk 卷用量 {base:.4f} 下无法构造滞回区间（warn={warn}、reject={reject}）")
    return warn, reject, need_mb


def now_iso() -> str:
    """当前 UTC 时刻（ISO8601，记录用）。

    Returns:
        str: ISO8601 文本。
    """
    return datetime.now(UTC).isoformat()


def free_port(start: int) -> int:
    """从 `start` 起探测一个空闲端口（只判 127.0.0.1）。

    Args:
        start: 起始端口。

    Returns:
        int: 可用端口。

    Raises:
        DrillError: 连续 32 个端口都被占用。
    """
    for offset in range(32):
        port = start + offset
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
            probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                probe.bind(("127.0.0.1", port))
            except OSError:
                continue
            return port
    raise DrillError(f"drill.port 端口段 {start}~{start + 31} 全被占用")


class DrillInstance:
    """隔离实例（真装配子进程）：启动 / 重启 / 信号 / 停止。"""

    def __init__(
        self,
        config: DrillConfig,
        *,
        port: int | None = None,
        reject_ratio: float | None = None,
        warn_ratio: float | None = None,
    ) -> None:
        """初始化（不启动进程）。

        Args:
            config: 演练配置。
            port: 指定端口（缺省按 `base_port` 探测）。
            reject_ratio: 拒绝水位（只有磁盘满场景收紧；None 表示用运行时缺省 0.85）。
            warn_ratio: 告警水位（滞回解除线；与 `reject_ratio` 成对给出）。
        """
        self._config = config
        self._reject_ratio = reject_ratio
        self._warn_ratio = warn_ratio
        self.port = port if port is not None else free_port(config.base_port)
        self.data_root = config.work_dir / "var"
        self.log_path = config.work_dir / "instance.log"
        self.process: subprocess.Popen[bytes] | None = None
        self.starts = 0

    @property
    def base_url(self) -> str:
        """实例基址。

        Returns:
            str: `http://127.0.0.1:<port>`。
        """
        return f"http://127.0.0.1:{self.port}"

    def start(self) -> None:
        """启动实例（`python -m ops.drill.instance`，独立进程组）。

        Raises:
            DrillError: 进程无法拉起。
        """
        self._config.work_dir.mkdir(parents=True, exist_ok=True)
        self.data_root.mkdir(parents=True, exist_ok=True)
        env = dict(os.environ)
        env.update(
            {
                DRILL_INSTANCE_PORT_ENV: str(self.port),
                DRILL_INSTANCE_ROOT_ENV: str(self._config.work_dir),
            }
        )
        if self._reject_ratio is not None:
            env[STORAGE_REJECT_RATIO_ENV] = str(self._reject_ratio)
        else:
            env.pop(STORAGE_REJECT_RATIO_ENV, None)
        if self._warn_ratio is not None:
            env[STORAGE_WARN_RATIO_ENV] = str(self._warn_ratio)
        else:
            env.pop(STORAGE_WARN_RATIO_ENV, None)
        backend = Path(__file__).resolve().parents[2]
        env.setdefault("PYTHONPATH", str(backend))
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        log = self.log_path.open("ab")
        try:
            self.process = subprocess.Popen(
                [sys.executable, "-m", "ops.drill.instance"],
                cwd=str(backend),
                env=env,
                stdout=log,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
        except OSError as exc:
            log.close()
            raise DrillError(f"drill.instance 实例进程无法拉起：{exc}") from exc
        self.starts += 1
        log.close()

    def wait_ready(self) -> None:
        """轮询 `/healthz` 直到 200。

        Raises:
            DrillError: 超时或进程提前退出（附日志尾部）。
        """
        deadline = time.monotonic() + DRILL_READY_TIMEOUT_SEC
        while time.monotonic() < deadline:
            if self.process is not None and self.process.poll() is not None:
                raise DrillError(f"drill.ready 实例提前退出（code={self.process.returncode}）{self.log_tail()}")
            try:
                response = httpx.get(f"{self.base_url}/healthz", timeout=2.0, trust_env=False)
            except httpx.HTTPError:
                time.sleep(DRILL_PROBE_INTERVAL_MS / 1000)
                continue
            if response.status_code == 200:
                return
            time.sleep(DRILL_PROBE_INTERVAL_MS / 1000)
        raise DrillError(f"drill.ready 实例未在 {DRILL_READY_TIMEOUT_SEC}s 内就绪{self.log_tail()}")

    def signal(self, sig: str) -> None:
        """向实例进程发信号（`SIGKILL` / `SIGTERM`）。

        Args:
            sig: 信号名。

        Raises:
            DrillError: 进程未启动。
        """
        if self.process is None:
            raise DrillError("drill.instance 实例未启动，无法发信号")
        if sig == "SIGKILL":
            self.process.kill()
            return
        if sig == "SIGTERM":
            self.process.terminate()
            return
        if os.name == "posix":
            os.kill(self.process.pid, cast("int", getattr(signal, sig)))
            return
        self.process.terminate()

    def wait_exit(self, timeout: float = 20.0) -> int | None:
        """等待进程退出。

        Args:
            timeout: 等待秒数。

        Returns:
            int | None: 退出码（超时返回 None）。
        """
        if self.process is None:
            return None
        try:
            return self.process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            return None

    def restart(self) -> None:
        """以同一数据根重启实例并等就绪（崩溃场景的恢复动作）。"""
        self.stop()
        self.start()
        self.wait_ready()

    def stop(self) -> None:
        """停止实例（先 SIGTERM，超时后 SIGKILL；幂等）。

        Raises:
            DrillError: 进程仍在（强杀后仍未退出）。
        """
        if self.process is None:
            return
        if self.process.poll() is None:
            self.process.terminate()
            if self.wait_exit(15.0) is None:
                self.process.kill()
                self.wait_exit(10.0)
        self.process = None

    def log_tail(self) -> str:
        """实例日志尾部（排障线索，最多 `DRILL_OUTPUT_TAIL_CHARS` 字符）。

        Returns:
            str: 尾部文本（无日志时为空串）。
        """
        try:
            text = self.log_path.read_text(encoding="utf-8", errors="replace")
        except FileNotFoundError:
            return ""
        return f"；日志尾部：{text[-DRILL_OUTPUT_TAIL_CHARS:]}"


class DrillHttp:
    """演练 HTTP 客户端（探针 / 指标 / 数据面 / 管理面）。"""

    def __init__(self, base_url: str) -> None:
        """初始化。

        Args:
            base_url: 实例基址。
        """
        self._base = base_url
        self._client = httpx.Client(base_url=base_url, timeout=DRILL_HTTP_TIMEOUT_SEC, trust_env=False)

    def close(self) -> None:
        """释放连接。"""
        self._client.close()

    def readyz(self) -> tuple[int, bool, str]:
        """读就绪探针。

        Returns:
            tuple: 状态码 + 是否通过 + 正文摘要。
        """
        try:
            response = self._client.get("/readyz")
        except httpx.HTTPError as exc:
            return 0, False, f"{type(exc).__name__}: {exc}"
        ok = response.status_code == 200 and '"status":"ok"' in response.text.replace(" ", "")
        return response.status_code, ok, response.text[:400]

    def healthz(self) -> bool:
        """读存活探针。

        Returns:
            bool: 是否 200。
        """
        try:
            return self._client.get("/healthz").status_code == 200
        except httpx.HTTPError:
            return False

    def metrics(self) -> str:
        """读指标文本（失败返回空串，由采样记 0，不判失败）。

        Returns:
            str: Prometheus 文本。
        """
        try:
            response = self._client.get("/metrics")
        except httpx.HTTPError:
            return ""
        return response.text if response.status_code == 200 else ""

    def create_topic(self, topic: str, partitions: int = 1) -> int:
        """建主题（已存在视为成功）。

        Args:
            topic: 主题名。
            partitions: 分区数。

        Returns:
            int: HTTP 状态码。
        """
        response = self._client.post(
            "/api/v1/msg/admin/topics",
            json={"namespace": "msg", "name": topic, "partition_count": partitions},
        )
        return response.status_code

    def join(self, topic: str, group: str, consumer: str) -> int:
        """加入消费组。

        Args:
            topic: 主题名。
            group: 组名。
            consumer: 成员标识。

        Returns:
            int: HTTP 状态码。
        """
        return self._client.post(
            "/api/v1/msg/join",
            json={"group": group, "consumer_id": consumer, "namespace": "msg", "topic": topic},
        ).status_code

    def heartbeat(self, topic: str, group: str, consumer: str) -> int:
        """成员心跳（会话超时 30s，长等待场景必须续活）。

        Args:
            topic: 主题名。
            group: 组名。
            consumer: 成员标识。

        Returns:
            int: HTTP 状态码。
        """
        return self._client.post(
            "/api/v1/msg/heartbeat",
            json={"group": group, "consumer_id": consumer, "namespace": "msg", "topic": topic},
        ).status_code

    def publish(self, topic: str, payload: bytes, key: str | None = None) -> httpx.Response:
        """发布一条消息。

        Args:
            topic: 主题名。
            payload: 消息体。
            key: 分区键。

        Returns:
            httpx.Response: 响应（调用方判状态码与业务码）。
        """
        import base64

        return self._client.post(
            "/api/v1/msg/publish",
            json={"namespace": "msg", "topic": topic, "payload": base64.b64encode(payload).decode(), "key": key},
        )

    def consume(
        self, topic: str, group: str, consumer: str, *, max_messages: int = 20, max_wait_ms: int = 0
    ) -> httpx.Response:
        """拉取消息。

        Args:
            topic: 主题名。
            group: 组名。
            consumer: 成员标识。
            max_messages: 本次最多条数（服务端上限 100）。
            max_wait_ms: 长轮询挂起上限（毫秒）。

        Returns:
            httpx.Response: 响应。
        """
        return self._client.post(
            "/api/v1/msg/consume",
            json={
                "group": group,
                "consumer_id": consumer,
                "namespace": "msg",
                "topic": topic,
                "partition": 0,
                "max_messages": max_messages,
                "max_wait_ms": max_wait_ms,
            },
        )

    def ack(self, topic: str, group: str, consumer: str, offset: int) -> httpx.Response:
        """确认一条消息。

        Args:
            topic: 主题名。
            group: 组名。
            consumer: 成员标识。
            offset: 分区内序号。

        Returns:
            httpx.Response: 响应。
        """
        return self._client.post(
            "/api/v1/msg/ack",
            json={
                "group": group,
                "consumer_id": consumer,
                "namespace": "msg",
                "topic": topic,
                "partition": 0,
                "offset": offset,
            },
        )

    def nack(self, topic: str, group: str, consumer: str, offset: int) -> httpx.Response:
        """拒绝一条消息（转死信）。

        Args:
            topic: 主题名。
            group: 组名。
            consumer: 成员标识。
            offset: 分区内序号。

        Returns:
            httpx.Response: 响应。
        """
        return self._client.post(
            "/api/v1/msg/nack",
            json={
                "namespace": "msg",
                "topic": topic,
                "group": group,
                "consumer_id": consumer,
                "partition": 0,
                "offset": offset,
                "reason": "schema_invalid",
                "detail": {"field": "payload"},
            },
        )

    def publish_invalid(self, topic: str, raw_payload: str) -> httpx.Response:
        """以非法 `payload` 字段发布（毒消息场景：验证入口拒绝，不进日志）。

        Args:
            topic: 主题名。
            raw_payload: 原样放进 `payload` 字段的文本（非法 base64）。

        Returns:
            httpx.Response: 响应。
        """
        return self._client.post(
            "/api/v1/msg/publish",
            json={"namespace": "msg", "topic": topic, "payload": raw_payload},
        )

    def dlq(self, topic: str) -> httpx.Response:
        """查死信（管理面只读）。

        Args:
            topic: 主题名。

        Returns:
            httpx.Response: 响应。
        """
        return self._client.get("/api/v1/msg/admin/dlq", params={"namespace": "msg", "topic": topic})


def parse_metrics(
    text: str, keys: ConcurrentStableFrozenDict[str, str] | None = None
) -> ConcurrentStableDict[str, float]:
    """把 Prometheus 文本解析成「指标名 → 值」（同名多标签取第一条）。

    直方图没有裸指标行，只有 `<name>_sum` / `<name>_count`；关心列表里给出 `<name>` 时按
    「sum / count」合成平均值（延迟类指标唯一的可读口径）。

    Args:
        text: 指标文本。
        keys: 关心的指标名集合（None 表示全部）。

    Returns:
        ConcurrentStableDict[str, float]: 指标取值。
    """
    values: ConcurrentStableDict[str, float] = ConcurrentStableDict()
    for line in text.splitlines():
        if not line or line.startswith("#"):
            continue
        name, _, rest = line.partition("{")
        if "}" in rest:
            _, _, rest = rest.partition("}")
        else:
            name, _, rest = line.partition(" ")
        metric = name.strip()
        if keys is not None and metric not in keys and not any(metric.startswith(f"{key}_") for key in keys):
            continue
        try:
            value = float(rest.strip().split(" ")[-1])
        except ValueError:
            continue
        if metric not in values:
            values.set(metric, value)
    if keys is not None:
        for key in keys:
            if key in values:
                continue
            total = values.get(f"{key}_sum")
            count = values.get(f"{key}_count")
            if total is not None and count:
                values.set(key, total / count)
    return values


def sample_phase(phase: str, http: DrillHttp, metrics: tuple[str, ...]) -> PhaseSample:
    """采集一段观测快照。

    Args:
        phase: `before` / `during` / `after`。
        http: 演练客户端。
        metrics: 关心的指标名。

    Returns:
        PhaseSample: 快照。
    """
    status, ok, detail = http.readyz()
    parsed = parse_metrics(http.metrics())
    subset: ConcurrentStableDict[str, float] = ConcurrentStableDict()
    for name in metrics:
        subset.set(name, parsed.get(name, 0.0))
    return PhaseSample(
        phase=phase,
        captured_at=now_iso(),
        readyz_status=status,
        readyz_ok=ok,
        healthz_ok=http.healthz(),
        metrics=ConcurrentStableFrozenDict(subset),
        detail=detail,
    )


def payload_of(response: httpx.Response) -> dict[str, object]:
    """解析响应 JSON（非 JSON 返回空表）。

    Args:
        response: HTTP 响应。

    Returns:
        dict[str, object]: 响应体。
    """
    try:
        body = response.json()
    except json.JSONDecodeError:
        return {}
    return cast("dict[str, object]", body) if isinstance(body, dict) else {}
