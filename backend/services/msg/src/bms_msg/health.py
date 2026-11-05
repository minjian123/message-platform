"""就绪检查项与自持注册表（02-6 详细设计 §8）。

三条结构性口径：

1. **自持注册表、不动基座**：基座 `[health_check_registry].provider` 缺省为空 → 解析到
   `NullHealthCheckRegistry`（`checks()` 恒空），所以 msg 的 `/readyz` 长期是「只看进程活着」。
   基座的 `local` 实现**无条件登记 redis 检查项**（msg 不用 redis，切过去就是「redis 未配 → 恒 503」），
   因此本模块以「产品自持插件」形态提供 `health_check_registry:msg`，配置一行即可切换。
2. **`error` 只能是异常类名**（基座契约）：检查项失败时**抛异常**，由 `aggregate()` 统一收敛成
   `{ok: false, error: "<类名>"}`；不在检查项里自造文案。
3. **就绪判「能写」而非「活着」**（架构 08 §8）：`msg_storage` 做一次**真实写探测**（建目录 + 写 0 字节
   临时文件 + 删除），而不是 `os.access`——磁盘满时 `access` 仍返回真，恰好漏掉最需要拦的场景。

`msg_recovery` 是**非必需项**（`required=False`）：恢复期截断 / 段损坏意味着数据有损，但服务仍可读写，
摘实例不能修复损失；只做降级可见（失败不产生 503），告警联动归 06-3。
"""

import os
from collections.abc import Callable
from pathlib import Path

from bms_core.core.config import Settings
from bms_core.core.factory import BasePluginFactory
from bms_core.core.plugin import register_plugin
from bms_core.health.base import BaseHealthCheck, BaseHealthCheckRegistry, HealthCheckResult
from bms_core.health.checks import DatabaseHealthCheck
from fastapi import FastAPI

from bms_msg.broker.recovery import RecoveryLedger
from bms_msg.broker.watermark import StorageUnavailableError, StorageWatermark
from bms_msg.core.config import RECOVERY_CHECK_KEY, STORAGE_CHECK_KEY, STORAGE_PROBE_NAME

__all__ = [
    "MSG_HEALTH_REGISTRY",
    "MsgHealthCheckRegistry",
    "MsgHealthCheckRegistryFactory",
    "MsgRecoveryCheck",
    "MsgRecoveryDegradedError",
    "MsgStorageCheck",
    "StorageProbeError",
    "register_msg_health_registry",
]

MSG_HEALTH_REGISTRY = "msg"
"""健康检查注册表实现名（配置 `[health_check_registry].provider = "msg"`）。"""

_WRITE_FLAGS = os.O_WRONLY | os.O_CREAT | os.O_TRUNC | getattr(os, "O_BINARY", 0)
"""写探测的打开标志：**带 `O_BINARY`**（Windows 默认文本模式会把换行改写，`\\n` 变 `\\r\\n`）。"""


class StorageProbeError(OSError):
    """数据目录不可写（建目录 / 写探测 / 删除探测文件失败）。

    继承 `OSError`：调用方按 `OSError` 捕获时能一并接住，语义上仍是「IO 层写不进去」。
    """


class MsgRecoveryDegradedError(RuntimeError):
    """恢复期发生过截断或段损坏（数据有损；非必需项，不摘实例）。"""


class MsgStorageCheck(BaseHealthCheck):
    """存储就绪检查项（必需）：目录可写 + 水位未超拒绝阈值。"""

    def __init__(self, *, watermark: StorageWatermark, root: Path) -> None:
        """初始化。

        Args:
            watermark: 水位闸门（读数与拒绝状态的唯一来源）。
            root: 消息数据根（写探测落在它下面）。
        """
        self._watermark = watermark
        self._root = root

    @property
    def key(self) -> str:
        """检查项键（`msg_storage`）。

        Returns:
            str: 检查项键。
        """
        return STORAGE_CHECK_KEY

    def describe(self) -> str:
        """元信息描述。

        Returns:
            str: 检查项说明。
        """
        return f"消息存储健康检查（{self.key}：目录可写 + 水位未超拒绝阈值）"

    async def check(self) -> HealthCheckResult:
        """执行检查：先判能写、再判水位。

        Returns:
            HealthCheckResult: 通过时的结果（`ok=True`）。

        Raises:
            StorageProbeError: 数据目录不可写（建目录 / 写盘 / 删除失败）。
            StorageUnavailableError: 水位采样失败或容量未知（拿不到就不保证能写）。
            MsgStorageWatermarkExceededError: 已进入拒绝态（`350022` / 507）。
        """
        self._probe_writable()
        reading = self._watermark.reading()
        if reading.unreadable:
            raise StorageUnavailableError(f"消息数据卷不可用（采样失败：{reading.error}）")
        refusal = self._watermark.refusal()
        if refusal is not None:
            raise refusal
        return HealthCheckResult(name=self.key, ok=True)

    def _probe_writable(self) -> None:
        """真实写一次（建目录 → 写 0 字节 → 删除）；任何 `OSError` 都收敛成 `StorageProbeError`。

        Raises:
            StorageProbeError: 目录不可写。
        """
        try:
            self._root.mkdir(parents=True, exist_ok=True)
            target = self._root / STORAGE_PROBE_NAME
            descriptor = os.open(target, _WRITE_FLAGS, 0o600)
            os.close(descriptor)
            target.unlink()
        except OSError as exc:
            raise StorageProbeError(str(exc)) from exc


class MsgRecoveryCheck(BaseHealthCheck):
    """恢复状态检查项（**非必需**）：已打开分区是否发生过截断 / 非末段损坏。"""

    required = False

    def __init__(self, *, ledger: Callable[[], RecoveryLedger]) -> None:
        """初始化。

        Args:
            ledger: 台账读取函数（惰性取，避免装配期与存储构造顺序耦合）。
        """
        self._ledger = ledger

    @property
    def key(self) -> str:
        """检查项键（`msg_recovery`）。

        Returns:
            str: 检查项键。
        """
        return RECOVERY_CHECK_KEY

    def describe(self) -> str:
        """元信息描述。

        Returns:
            str: 检查项说明。
        """
        return f"恢复状态健康检查（{self.key}；非必需项：数据有损不摘实例）"

    async def check(self) -> HealthCheckResult:
        """执行检查：台账里存在损坏条目即失败。

        Returns:
            HealthCheckResult: 通过时的结果（`ok=True`；启动期台账为空必然通过）。

        Raises:
            MsgRecoveryDegradedError: 至少一个分区在恢复期发生过截断或段损坏。
        """
        degraded = self._ledger().degraded()
        if degraded:
            raise MsgRecoveryDegradedError(f"恢复期发现数据损坏：{len(degraded)} 个分区（详见启动日志）")
        return HealthCheckResult(name=self.key, ok=True)


class MsgHealthCheckRegistry(BaseHealthCheckRegistry):
    """msg 自持检查项注册表（实现名 `msg`）：聚合模板与超时策略全部继承基座。"""

    @classmethod
    def _provider_key(cls, provider: BaseHealthCheck) -> str:
        """注册项键：检查项 `key`。

        Args:
            provider: 检查项。

        Returns:
            str: 检查项键。
        """
        return provider.key


class MsgHealthCheckRegistryFactory(BasePluginFactory[MsgHealthCheckRegistry]):
    """注册表工厂：按 `[health]` 超时配置构造，并登记三个检查项。

    登记顺序即 `/readyz` 的 `checks` 顺序：`msg_storage`（必需）→ `msg_recovery`（非必需）→
    `database`（必需，基座实现：主题元数据在平台库，元数据不可达则一切发布都无意义）。
    """

    plugin_key: str = "health_check_registry"
    plugin_name: str = MSG_HEALTH_REGISTRY

    def __init__(self, settings: Settings, app: FastAPI) -> None:
        """初始化。

        Args:
            settings: 应用配置（取 `[health]` 两档超时）。
            app: 应用实例（取水位组件、消息存储与引擎注册表）。
        """
        self._settings = settings
        self._app = app

    def create(self, options: None = None) -> MsgHealthCheckRegistry:
        """构造注册表并登记检查项。

        Args:
            options: 未使用（零参口径）。

        Returns:
            MsgHealthCheckRegistry: 注册表实例。
        """
        del options
        registry = MsgHealthCheckRegistry(
            check_timeout_ms=self._settings.health.check_timeout_ms,
            total_timeout_ms=self._settings.health.total_timeout_ms,
        )
        watermark: StorageWatermark = self._app.state.storage_watermark
        store = self._app.state.file_store
        registry.register(MsgStorageCheck(watermark=watermark, root=watermark.root))
        registry.register(MsgRecoveryCheck(ledger=store.recovery_ledger))
        registry.register(DatabaseHealthCheck(self._app.state.engine_registry))
        return registry


def register_msg_health_registry(app: FastAPI, settings: Settings) -> None:
    """登记 msg 自持健康检查注册表（装配钩子内调用，须在插件注册表冻结前）。

    Args:
        app: 应用实例（工厂创建时读取 `app.state`）。
        settings: 应用配置。
    """
    register_plugin("health_check_registry", MSG_HEALTH_REGISTRY, MsgHealthCheckRegistryFactory(settings, app))
