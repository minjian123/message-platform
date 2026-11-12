"""就绪检查项与自持注册表（msg 任务 02-6；Kiwi 2430）。

覆盖（对应详细设计 §8 与 §12 的 T19~T22）：

- `msg_storage`：通过时清理探测文件并确保数据根存在；高水位 / 采样不可用 / 目录不可写分别抛对应异常；
- `msg_recovery`：台账为空即通过；有截断 / 段损坏即失败，且因 `required=False` **不产生 503**；
- 注册表：`provider = "msg"` 能解析到 msg 自持实现，检查项顺序为 `msg_storage` → `msg_recovery` → `database`。
"""

from pathlib import Path
from types import SimpleNamespace
from typing import cast

import pytest
from bms_core.core.config import Settings
from bms_core.core.plugin import default_plugin_registry, resolve_plugin
from bms_core.db.registry import EngineRegistry
from bms_core.health.base import BaseHealthCheckRegistry, HealthCheckResult
from fastapi import FastAPI

from bms_msg.broker.file_store import FileMessageStore, PartitionDirectory
from bms_msg.broker.recovery import RecoveryLedger, RecoveryReport
from bms_msg.broker.watermark import StorageUnavailableError, StorageWatermark
from bms_msg.core.config import RECOVERY_CHECK_KEY, STORAGE_CHECK_KEY, STORAGE_PROBE_NAME
from bms_msg.errors import MsgStorageWatermarkExceededError
from bms_msg.health import (
    MSG_HEALTH_REGISTRY,
    MsgHealthCheckRegistry,
    MsgHealthCheckRegistryFactory,
    MsgRecoveryCheck,
    MsgRecoveryDegradedError,
    MsgStorageCheck,
    StorageProbeError,
    register_msg_health_registry,
)

pytestmark = pytest.mark.kiwi_id(2430)

TOTAL = 1_000
"""采样注入的卷总容量（字节）。"""


class _Usage:
    """固定容量读数（used 可变，便于在同一用例里切换水位）。"""

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
    """单调毫秒假时钟（TTL 判据）。"""

    def __init__(self) -> None:
        """初始化（从 0 开始）。"""
        self.now = 0.0

    def __call__(self) -> float:
        """取当前时刻。

        Returns:
            float: 单调毫秒。
        """
        return self.now


def _watermark(root: Path, usage: _Usage) -> StorageWatermark:
    """构造水位组件（显式阈值与 TTL，避免环境变量干扰）。

    Args:
        root: 数据根。
        usage: 容量读数。

    Returns:
        StorageWatermark: 水位组件。
    """
    return StorageWatermark(
        root=root,
        warn_ratio=0.70,
        reject_ratio=0.85,
        ttl_ms=1_000,
        clock=_Clock(),
        usage=usage,
    )


def _settings() -> Settings:
    """构造最小配置（只有 `[health]` 两档超时被用到）。

    Returns:
        Settings: 配置替身。
    """
    return cast("Settings", SimpleNamespace(health=SimpleNamespace(check_timeout_ms=2_000, total_timeout_ms=5_000)))


async def test_storage_check_passes_and_cleans_probe(tmp_path: Path) -> None:
    """T19：正常水位 → `ok=True`，探测文件被清理，数据根被确保存在。"""
    root = tmp_path / "var" / "lib" / "msg"
    check = MsgStorageCheck(watermark=_watermark(root, _Usage(100)), root=root)

    result = await check.check()

    assert result == HealthCheckResult(name=STORAGE_CHECK_KEY, ok=True)
    assert check.describe().startswith("消息存储健康检查")
    assert root.is_dir()
    assert not (root / STORAGE_PROBE_NAME).exists()


async def test_storage_check_fails_on_high_watermark(tmp_path: Path) -> None:
    """T20a：高水位 → 抛 `350022`（`error` 收敛成异常类名）。"""
    check = MsgStorageCheck(watermark=_watermark(tmp_path, _Usage(950)), root=tmp_path)

    with pytest.raises(MsgStorageWatermarkExceededError) as excinfo:
        await check.check()

    assert excinfo.value.http_status == 507


async def test_storage_check_fails_when_dir_not_writable(tmp_path: Path) -> None:
    """T20b：数据根是个普通文件（建目录必失败）→ `StorageProbeError`。"""
    blocked = tmp_path / "blocked"
    blocked.write_text("not a directory", encoding="utf-8")
    check = MsgStorageCheck(watermark=_watermark(blocked, _Usage(100)), root=blocked)

    with pytest.raises(StorageProbeError):
        await check.check()


async def test_storage_check_fails_when_sampling_unavailable(tmp_path: Path) -> None:
    """T20c：采样失败 → `StorageUnavailableError`（拿不到容量就不保证能写）。"""

    def _boom(_path: Path) -> tuple[int, int, int]:
        raise PermissionError(13, "permission denied")

    check = MsgStorageCheck(watermark=_watermark(tmp_path, cast("_Usage", _boom)), root=tmp_path)

    with pytest.raises(StorageUnavailableError):
        await check.check()


async def test_recovery_check_passes_when_ledger_empty() -> None:
    """T21a：台账为空（启动期常态）→ 通过，**不误报**。"""
    ledger = RecoveryLedger()
    check = MsgRecoveryCheck(ledger=lambda: ledger)

    result = await check.check()

    assert result.ok is True
    assert result.name == RECOVERY_CHECK_KEY
    assert check.describe().startswith("恢复状态健康检查")


async def test_recovery_check_reports_degraded(tmp_path: Path) -> None:
    """T21b：有截断 → 失败；因非必需项，**整体仍为就绪**（不产生 503）。"""
    ledger = RecoveryLedger()
    ledger.record(
        topic_id=7,
        partition_no=0,
        report=RecoveryReport(truncated_from_bytes=120, truncated_bytes=100, segments=1),
    )
    failing = MsgRecoveryCheck(ledger=lambda: ledger)
    registry = MsgHealthCheckRegistry()
    registry.register(MsgStorageCheck(watermark=_watermark(tmp_path, _Usage(100)), root=tmp_path))
    registry.register(failing)

    report = await registry.aggregate()

    by_name = {item.name: item for item in report.checks}
    assert report.ok is True, "非必需项失败不得摘实例"
    assert by_name[RECOVERY_CHECK_KEY].ok is False
    assert by_name[RECOVERY_CHECK_KEY].error == MsgRecoveryDegradedError.__name__


async def test_registry_aggregate_fails_when_storage_unready(tmp_path: Path) -> None:
    """T21c：必需项（`msg_storage`）失败 → 整体不就绪，`error` 为异常类名。"""
    registry = MsgHealthCheckRegistry()
    registry.register(MsgStorageCheck(watermark=_watermark(tmp_path, _Usage(950)), root=tmp_path))
    registry.register(MsgRecoveryCheck(ledger=RecoveryLedger))

    report = await registry.aggregate()

    assert report.ok is False
    item = report.checks[0]
    assert (item.name, item.ok) == (STORAGE_CHECK_KEY, False)
    assert item.error == MsgStorageWatermarkExceededError.__name__


def test_factory_registers_checks_in_order(tmp_path: Path) -> None:
    """T22a：工厂按 `[health]` 超时构造，登记顺序即 `/readyz` 的 `checks` 顺序。"""
    root = tmp_path / "data"
    directory = PartitionDirectory(root=root)
    app = FastAPI()
    app.state.storage_watermark = _watermark(root, _Usage(100))
    app.state.file_store = FileMessageStore(directory=directory, fsync_mode="os")
    app.state.engine_registry = cast("EngineRegistry", SimpleNamespace())

    registry = MsgHealthCheckRegistryFactory(_settings(), app).create()

    assert [check.key for check in registry.checks()] == [STORAGE_CHECK_KEY, RECOVERY_CHECK_KEY, "database"]
    assert isinstance(registry, BaseHealthCheckRegistry)


def test_register_registry_resolves_by_plugin_name(tmp_path: Path) -> None:
    """T22b：登记后可按 `health_check_registry:msg` 解析（`configure_plugins()` 调用的同一入口）。"""
    plugins = default_plugin_registry()
    plugins.reset()
    app = FastAPI()
    app.state.storage_watermark = _watermark(tmp_path, _Usage(100))
    app.state.file_store = FileMessageStore(directory=PartitionDirectory(root=tmp_path), fsync_mode="os")
    app.state.engine_registry = cast("EngineRegistry", SimpleNamespace())
    try:
        register_msg_health_registry(app, _settings())
        resolved = resolve_plugin(
            "health_check_registry",
            MSG_HEALTH_REGISTRY,
            expected_version=BaseHealthCheckRegistry.contract_version,
        )
        assert isinstance(resolved, MsgHealthCheckRegistry)
    finally:
        plugins.reset()


async def test_internal_replication_check_and_factory(tmp_path: Path) -> None:
    """02-8：内部服务检查项按存活判定；多实例时注册且顺序在 database 之前。"""
    from bms_msg.health import InternalReplicationUnavailableError, MsgInternalReplicationCheck

    alive = True
    check = MsgInternalReplicationCheck(alive=lambda: alive)
    assert check.key == "msg_internal_replication"
    assert "内部复制" in check.describe()
    assert (await check.check()).ok is True
    alive = False
    with pytest.raises(InternalReplicationUnavailableError):
        await check.check()

    root = tmp_path / "data"
    app = FastAPI()
    app.state.storage_watermark = _watermark(root, _Usage(100))
    app.state.file_store = FileMessageStore(directory=PartitionDirectory(root=root), fsync_mode="os")
    app.state.engine_registry = cast("EngineRegistry", SimpleNamespace())
    app.state.internal_required = True
    app.state.internal_server = SimpleNamespace(alive=True)
    registry = MsgHealthCheckRegistryFactory(Settings(), app).create()
    keys = [item.key for item in registry.checks()]
    assert "msg_internal_replication" in keys

    app.state.internal_server = None
    registry = MsgHealthCheckRegistryFactory(Settings(), app).create()
    check_item = next(item for item in registry.checks() if item.key == "msg_internal_replication")
    with pytest.raises(InternalReplicationUnavailableError):
        await check_item.check()
