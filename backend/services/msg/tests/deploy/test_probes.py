"""探针端点（Kiwi 2473；任务 07-1-2 §6）：`/healthz` 身份、`/readyz` 正反、拒绝指标不被探测污染。

口径来源：

- 端点与聚合模板属基座（`bms_core/api/health.py`），本任务**不改实现**，只把「msg 的三项检查项挂上去
  之后的行为」钉住：可写才 200；元数据不可达 / 日志不可写 / 水位超阈值 / 启动未完成 / 摘流中一律 503，
  且 `checks` 指明失败项（`error` 只放异常类名）。
- 「恢复未完成」分两层：**启动期**恢复未完成 → `startup_complete = False` → 503（不提前接流量）；
  **运行期**发现数据有损 → `msg_recovery` 非必需项失败 → 仍 200（02-6 口径：摘实例不能修复损失）。
- `StorageWatermark.refusal()` 不计数（供 `/readyz`），`ensure_writable()` 才计数（供发布闸门）。
"""

from pathlib import Path
from typing import cast

import pytest
from bms_core.api.health import router as health_router
from bms_core.core.service import ServiceIdentity
from bms_core.db.registry import EngineRegistry
from bms_core.health.base import get_health_check_registry
from bms_core.health.checks import DatabaseHealthCheck
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from bms_msg.broker.recovery import RecoveryLedger, RecoveryReport
from bms_msg.broker.watermark import TOTAL_KEY, StorageMetrics, StorageWatermark
from bms_msg.core.config import RECOVERY_CHECK_KEY, STORAGE_CHECK_KEY
from bms_msg.errors import MsgStorageWatermarkExceededError
from bms_msg.health import MsgHealthCheckRegistry, MsgRecoveryCheck, MsgStorageCheck

pytestmark = pytest.mark.kiwi_id(2473)

TOTAL = 1_000
"""采样注入的卷总容量（字节）。"""

REJECTED_METRIC = "msg_storage_rejected_total"
"""拒绝计数指标名（06-3 出口与用例断言同一口径）。"""


class _Usage:
    """固定容量读数（`used` 可变，便于在同一用例里切换水位）。"""

    def __init__(self, used: int, *, total: int = TOTAL) -> None:
        """初始化。

        Args:
            used: 已用字节。
            total: 卷总容量。
        """
        self.used = used
        self.total = total

    def __call__(self, _path: Path) -> tuple[int, int, int]:
        """读一次容量。

        Args:
            _path: 被采样的路径。

        Returns:
            tuple[int, int, int]: `(total, used, free)`。
        """
        return self.total, self.used, self.total - self.used


class _Clock:
    """单调毫秒假时钟（TTL 判据；恒为 0 以复用同一份读数）。"""

    def __call__(self) -> float:
        """取当前时刻。

        Returns:
            float: 单调毫秒。
        """
        return 0.0


class _Connection:
    """平台库连接替身（`SELECT 1` 无副作用）。"""

    async def __aenter__(self) -> _Connection:
        """进入连接上下文。

        Returns:
            _Connection: 自身。
        """
        return self

    async def __aexit__(self, *_exc: object) -> bool:
        """退出连接上下文。

        Returns:
            bool: False（不吞异常）。
        """
        return False

    async def execute(self, _statement: object) -> None:
        """执行语句（替身：不做事）。

        Args:
            _statement: SQL 语句。
        """


class _Engine:
    """引擎替身：`connect()` 返回可用的连接上下文。"""

    def connect(self) -> _Connection:
        """开连接。

        Returns:
            _Connection: 连接替身。
        """
        return _Connection()


class _EngineRegistry:
    """引擎注册表替身（`down=True` 时模拟元数据库不可达）。"""

    def __init__(self, *, down: bool = False) -> None:
        """初始化。

        Args:
            down: 是否模拟不可达。
        """
        self._down = down

    async def get(self, _key: str) -> _Engine:
        """取平台引擎。

        Args:
            _key: 库键。

        Returns:
            _Engine: 引擎替身。

        Raises:
            ConnectionError: 模拟元数据库不可达。
        """
        if self._down:
            raise ConnectionError("platform db unreachable")
        return _Engine()


def _watermark(root: Path, used: int, *, metrics: StorageMetrics | None = None) -> StorageWatermark:
    """构造水位组件（显式阈值 / TTL / 时钟，避免环境变量干扰）。

    Args:
        root: 数据根。
        used: 已用字节（`TOTAL` 的占比决定水位）。
        metrics: 指标采集器（缺省不采集）。

    Returns:
        StorageWatermark: 水位组件。
    """
    return StorageWatermark(
        root=root,
        warn_ratio=0.70,
        reject_ratio=0.85,
        ttl_ms=1_000,
        clock=_Clock(),
        usage=_Usage(used),
        metrics=metrics,
    )


def _registry(
    root: Path,
    *,
    used: int = 100,
    down: bool = False,
    ledger: RecoveryLedger | None = None,
) -> MsgHealthCheckRegistry:
    """组装 msg 自持注册表（三项检查项，顺序即 `/readyz` 的 `checks` 顺序）。

    Args:
        root: 数据根。
        used: 已用字节。
        down: 平台库是否不可达。
        ledger: 恢复台账（缺省空台账）。

    Returns:
        MsgHealthCheckRegistry: 注册表。
    """
    registry = MsgHealthCheckRegistry()
    registry.register(MsgStorageCheck(watermark=_watermark(root, used), root=root))
    registry.register(MsgRecoveryCheck(ledger=lambda: ledger if ledger is not None else RecoveryLedger()))
    registry.register(DatabaseHealthCheck(cast("EngineRegistry", _EngineRegistry(down=down))))
    return registry


def _app(registry: MsgHealthCheckRegistry, *, startup: bool = True) -> FastAPI:
    """搭一个只挂探针路由的最小应用。

    Args:
        registry: 健康检查注册表。
        startup: 启动是否完成。

    Returns:
        FastAPI: 应用实例。
    """
    app = FastAPI()
    app.include_router(health_router)
    app.state.startup_complete = startup
    app.state.draining = False
    app.state.service_identity = ServiceIdentity(name="msg", version="9.9.9")
    app.dependency_overrides[get_health_check_registry] = lambda: registry
    return app


async def _get(app: FastAPI, path: str) -> tuple[int, dict[str, object]]:
    """发一次探针请求。

    Args:
        app: 应用实例。
        path: 端点路径。

    Returns:
        tuple[int, dict[str, object]]: 状态码与响应体。
    """
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://probe") as client:
        response = await client.get(path)
    return response.status_code, cast("dict[str, object]", response.json())


async def test_healthz_reports_service_identity(tmp_path: Path) -> None:
    """T6：`/healthz` 200 且带服务身份（存活 + 是谁）。"""
    status, body = await _get(_app(_registry(tmp_path)), "/healthz")

    assert status == 200
    assert body == {"status": "ok", "service": "msg", "version": "9.9.9"}


async def test_readyz_ok_when_writable(tmp_path: Path) -> None:
    """T7：可写 → 200，三项 `checks` 全 ok（顺序即登记序）。"""
    status, body = await _get(_app(_registry(tmp_path)), "/readyz")

    assert status == 200
    assert body["status"] == "ok"
    checks = cast("dict[str, dict[str, object]]", body["checks"])
    assert list(checks) == [STORAGE_CHECK_KEY, RECOVERY_CHECK_KEY, "database"]
    assert all(item["ok"] is True for item in checks.values())
    assert all(item["error"] is None for item in checks.values())


async def test_readyz_fails_when_startup_incomplete(tmp_path: Path) -> None:
    """T8-a：启动期恢复未完成 → 503 且 `checks` 为空（不提前接流量）。"""
    status, body = await _get(_app(_registry(tmp_path), startup=False), "/readyz")

    assert status == 503
    assert body == {"status": "down", "checks": {}, "service": "msg", "version": "9.9.9"}


async def test_readyz_fails_when_draining(tmp_path: Path) -> None:
    """T8-b：摘流中 → 503（网关据此摘节点）。"""
    app = _app(_registry(tmp_path))
    app.state.draining = True

    status, body = await _get(app, "/readyz")

    assert status == 503
    assert body["status"] == "down"


async def test_readyz_fails_when_disk_full(tmp_path: Path) -> None:
    """T8-c：水位超拒绝阈值 → 503，失败项为 `msg_storage`（`350022` 收敛成异常类名）。"""
    status, body = await _get(_app(_registry(tmp_path, used=950)), "/readyz")

    assert status == 503
    checks = cast("dict[str, dict[str, object]]", body["checks"])
    assert checks[STORAGE_CHECK_KEY]["ok"] is False
    assert checks[STORAGE_CHECK_KEY]["error"] == "MsgStorageWatermarkExceededError"


async def test_readyz_fails_when_dir_not_writable(tmp_path: Path) -> None:
    """T8-d：数据根不可写（此处是普通文件，建目录必失败）→ 503，`error=StorageProbeError`。"""
    blocked = tmp_path / "blocked"
    blocked.write_text("not a directory", encoding="utf-8")

    status, body = await _get(_app(_registry(blocked)), "/readyz")

    assert status == 503
    checks = cast("dict[str, dict[str, object]]", body["checks"])
    assert checks[STORAGE_CHECK_KEY]["error"] == "StorageProbeError"


async def test_readyz_fails_when_database_unreachable(tmp_path: Path) -> None:
    """T8-e：元数据不可达 → 503（元数据在平台库，不可达则一切发布无意义）。"""
    status, body = await _get(_app(_registry(tmp_path, down=True)), "/readyz")

    assert status == 503
    checks = cast("dict[str, dict[str, object]]", body["checks"])
    assert checks["database"]["ok"] is False
    assert checks["database"]["error"] == "ConnectionError"


async def test_readyz_ok_when_recovery_degraded(tmp_path: Path) -> None:
    """T9-a：运行期发现数据有损 → `msg_recovery` 失败但整体仍 200（非必需项，不摘实例）。"""
    ledger = RecoveryLedger()
    ledger.record(
        topic_id=7,
        partition_no=0,
        report=RecoveryReport(truncated_from_bytes=120, truncated_bytes=100, segments=1),
    )

    status, body = await _get(_app(_registry(tmp_path, ledger=ledger)), "/readyz")

    assert status == 200
    checks = cast("dict[str, dict[str, object]]", body["checks"])
    assert checks[RECOVERY_CHECK_KEY]["ok"] is False
    assert checks[RECOVERY_CHECK_KEY]["error"] == "MsgRecoveryDegradedError"


async def test_probes_do_not_count_storage_rejections(tmp_path: Path) -> None:
    """T9-b：拒绝态下连探 3 次不涨拒绝指标；`ensure_writable()`（发布闸门）才计一次。"""
    metrics = StorageMetrics()
    watermark = _watermark(tmp_path, 950, metrics=metrics)
    registry = MsgHealthCheckRegistry()
    registry.register(MsgStorageCheck(watermark=watermark, root=tmp_path))
    app = _app(registry)

    for _ in range(3):
        status, _body = await _get(app, "/readyz")
        assert status == 503
    rejected = cast("dict[str, int]", metrics.snapshot()[REJECTED_METRIC])
    assert rejected[TOTAL_KEY] == 0, "探测不得污染拒绝指标"

    with pytest.raises(MsgStorageWatermarkExceededError):
        watermark.ensure_writable()
    rejected = cast("dict[str, int]", metrics.snapshot()[REJECTED_METRIC])
    assert rejected[TOTAL_KEY] == 1, "发布闸门拒绝才计一次"
