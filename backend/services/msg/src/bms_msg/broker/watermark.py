"""存储水位与拒绝闸门（02-6 详细设计 §4~§7、§9）：卷级采样 + 滞回 + 指标采集点。

六条结构性口径：

1. **只认卷，不认目录**：判据是 `shutil.disk_usage(数据根所在卷)` 的使用率——索引、位点、恢复点与
   段文件同卷，别的进程把卷写满时「我们只写了 3%」也会照写不误。目录占用只作观测，不作判据。
2. **读时惰性采样**：无后台任务（因此不触碰 02-2 / 02-3 的关闭偏序）；TTL 内复用同一份读数，
   publish 热路径只做一次时间比较，`/readyz` 与发布会得到同一个结论。
3. **滞回靠进程内一个布尔**：`>= 拒绝阈值` 进入拒绝，`< 告警阈值` 才解除，中间区间保持原状态。
   重启后重采即得真值，不落盘（落盘只会让「已经清理干净的盘」在重启后继续被拒）。
4. **不可用即拒绝**：数据根缺失时向上取最近存在的祖先算卷；连祖先都取不到（盘符不存在 / 未挂载 /
   权限拒绝）判**不可用**——绝不按「水位 0」放行，否则磁盘故障会退化成不可分类的 500。
5. **闸门只管外部生产**：`ensure_writable()` 由 `DeliveryService.publish` / `DelayService.publish`
   调用；延迟转投与重放直接走 `store.append`，不经本闸门（已收下的消息必须兑现，见详细设计 §6.3）。
6. **指标只收集**：`StorageMetrics.snapshot()` 返回纯数据结构，出口（Prometheus / 告警）归 06-3，
   同一条指标面不定义两遍（与 03-5 的 `BackpressureMetrics` 同构）。

`used_ratio` 一律按 `STORAGE_RATIO_DIGITS` 位小数比较后再判：浮点尾差会让「正好 85%」在阈值两侧
来回翻，表现为「磁盘没动、状态一直抖」。
"""

from __future__ import annotations

import os
import shutil
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol, runtime_checkable

from bms_core.core.concurrent import ConcurrentStableDict
from bms_core.core.logging import get_logger
from bms_core.core.objects import BaseValueObject
from bms_core.core.roots import BaseFrameworkObject

from bms_msg.broker.file_store import data_root
from bms_msg.core.config import (
    STORAGE_RATIO_DIGITS,
    STORAGE_REJECT_RATIO,
    STORAGE_REJECT_RATIO_ENV,
    STORAGE_SAMPLE_TTL_MS,
    STORAGE_SAMPLE_TTL_MS_ENV,
    STORAGE_VOLUME_KEY_LIMIT,
    STORAGE_WARN_RATIO,
    STORAGE_WARN_RATIO_ENV,
)
from bms_msg.errors import MsgStorageWatermarkExceededError

__all__ = [
    "OTHER_KEY",
    "TOTAL_KEY",
    "StorageGate",
    "StorageMetrics",
    "StorageReading",
    "StorageUnavailableError",
    "StorageWatermark",
]

_LOGGER = get_logger("bms_msg.watermark")
"""水位日志（只出结构化事件：进入拒绝 / 解除 / 不可读，每次跨越各一条）。"""

OTHER_KEY = "__other__"
"""分键溢出后的归并键（高基数保护）。"""

TOTAL_KEY = "__total__"
"""拒绝计数的合计键（读快照时求和，不单独维护）。"""

REASON_WATERMARK = "watermark"
"""拒绝原因：卷使用率超过拒绝阈值。"""

REASON_SAMPLING_FAILED = "sampling_failed"
"""拒绝原因：采样失败 / 数据卷不可用（拿不到容量就不保证能写）。"""

DiskUsage = Callable[[Path], tuple[int, int, int]]
"""容量读取函数签名（缺省 `shutil.disk_usage`；用例注入固定值以确定性控制水位）。"""


class StorageUnavailableError(RuntimeError):
    """数据卷不可用（采样失败 / 容量未知）。

    只承担两个角色：采样失败时作为「错误类名」写进 `StorageReading.error`，
    以及 `/readyz` 检查项抛出后由基座聚合收敛成 `{ok: false, error: "StorageUnavailableError"}`。
    """


@dataclass(frozen=True)
class StorageReading(BaseValueObject):
    """一次水位读数（缓存 TTL 内复用；不可用时 `used_ratio` 为 `None`）。"""

    volume: str
    """卷标识：数据根最近存在祖先的**绝对路径**（可读、可作指标分键；不用跨平台不可读的设备号）。"""

    used_ratio: float | None
    """卷使用率（0~1）；不可用时为 `None`。"""

    free_bytes: int | None
    """剩余字节；不可用时为 `None`。"""

    total_bytes: int | None
    """卷总容量；不可用时为 `None`。"""

    warn_ratio: float
    """告警阈值（读数自带，便于日志与响应体一次取全）。"""

    reject_ratio: float
    """拒绝阈值（读数自带）。"""

    error: str | None = None
    """不可用原因（异常类名）；可读时为 `None`。"""

    sampled_at_ms: float = 0.0
    """采样时刻（单调毫秒，TTL 判据）。"""

    @property
    def unreadable(self) -> bool:
        """数据卷是否不可用（采样失败或容量未知）。

        Returns:
            bool: True 表示拿不到可信读数。
        """
        return self.error is not None

    @property
    def exceeded(self) -> bool:
        """是否已达拒绝阈值（**不含**滞回状态：只在进入判定时用）。

        Returns:
            bool: 使用率（按 4 位小数比较）不低于拒绝阈值即 True。
        """
        if self.used_ratio is None:
            return False
        return _rounded(self.used_ratio) >= self.reject_ratio


@runtime_checkable
class StorageGate(Protocol):
    """存储闸门端口：不满足可写条件即抛 `MsgStorageWatermarkExceededError`。

    `DeliveryService` / `DelayService` 只依赖这个协议（用例可注入替身，`None` 表示不判）。
    """

    def ensure_writable(self) -> None:
        """判定当前是否允许写入（不满足即抛 `350022`）。"""


class StorageWatermark:
    """卷级水位：采样 + 缓存 + 滞回 + 闸门（进程级单例，装配时落 `app.state`）。

    线程 / 协程安全：所有操作都是**同步无 await** 的纯内存计算 + 一次 `statvfs`，
    单事件循环内天然原子（与 01-7 的 `RetentionThrottle` 同口径）。
    """

    def __init__(
        self,
        *,
        root: Path | None = None,
        warn_ratio: float | None = None,
        reject_ratio: float | None = None,
        ttl_ms: int | None = None,
        clock: Callable[[], float] | None = None,
        usage: DiskUsage | None = None,
        metrics: StorageMetrics | None = None,
    ) -> None:
        """初始化（首次读取时才采样，构造本身不碰磁盘）。

        Args:
            root: 数据根（缺省 `data_root()`：`MSG_DATA_DIR` 或 `var/lib/msg`）。
            warn_ratio: 告警阈值（缺省取环境变量 / 模块常量）。
            reject_ratio: 拒绝阈值（缺省取环境变量 / 模块常量）。
            ttl_ms: 采样缓存有效期（缺省取环境变量 / 模块常量）。
            clock: 单调毫秒时钟（用例注入以确定性控制 TTL）。
            usage: 容量读取函数（缺省 `shutil.disk_usage`；用例注入固定值）。
            metrics: 指标采集器（缺省不采集）。

        Raises:
            ValueError: 阈值或 TTL 取值非法（启动即失败，不静默降级成某个档）。
        """
        self._root = Path(root) if root is not None else data_root()
        self._warn_ratio = _resolve_ratio(warn_ratio, STORAGE_WARN_RATIO_ENV, STORAGE_WARN_RATIO)
        self._reject_ratio = _resolve_ratio(reject_ratio, STORAGE_REJECT_RATIO_ENV, STORAGE_REJECT_RATIO)
        if self._warn_ratio >= self._reject_ratio:
            raise ValueError(f"水位阈值须满足 0 < 告警 < 拒绝 < 1：告警 {self._warn_ratio}、拒绝 {self._reject_ratio}")
        self._ttl_ms = _resolve_ttl(ttl_ms)
        self._clock = clock if clock is not None else _monotonic_ms
        self._usage: DiskUsage = usage if usage is not None else _disk_usage
        self._metrics = metrics
        self._reading: StorageReading | None = None
        self._rejected = False
        self._unreadable = False

    @property
    def root(self) -> Path:
        """数据根（排障与用例断言用）。

        Returns:
            Path: 数据根路径（可能尚不存在）。
        """
        return self._root

    @property
    def rejected(self) -> bool:
        """当前是否处于拒绝态（含「不可用」；滞回状态的唯一读口）。

        Returns:
            bool: True 表示拒绝写入。
        """
        return self._rejected

    @property
    def warn_ratio(self) -> float:
        """告警阈值。

        Returns:
            float: 0~1 的比例。
        """
        return self._warn_ratio

    @property
    def reject_ratio(self) -> float:
        """拒绝阈值。

        Returns:
            float: 0~1 的比例。
        """
        return self._reject_ratio

    def reading(self) -> StorageReading:
        """取当前读数（缓存未过期即复用，过期才重采）。

        Returns:
            StorageReading: 水位读数（不可用时 `unreadable` 为 True）。
        """
        cached = self._reading
        if cached is not None and self._clock() - cached.sampled_at_ms < self._ttl_ms:
            return cached
        return self.refresh()

    def refresh(self) -> StorageReading:
        """立即重采一次并推进滞回状态（也是 06-2 清理 / 扩容后的强制刷新入口）。

        Returns:
            StorageReading: 新读数。
        """
        reading = self._sample()
        self._apply(reading)
        self._reading = reading
        if self._metrics is not None and not reading.unreadable:
            self._metrics.record_reading(reading)
        return reading

    def force_refresh(self) -> StorageReading:
        """缓存失效钩子（06-2 清理过期段 / 扩容后调用，立即按新状态判定）。

        本任务不接线：它存在是为了让「清理后无需重启」有个**确定**的即时入口，
        而不是只能等 TTL 到期（详细设计 §11）。

        Returns:
            StorageReading: 新读数。
        """
        return self.refresh()

    def ensure_writable(self) -> None:
        """闸门：处于拒绝态即抛 `350022`（并计一次拒绝指标）。

        Raises:
            MsgStorageWatermarkExceededError: 卷使用率超拒绝阈值，或数据卷不可用（507）。
        """
        refusal = self.refusal()
        if refusal is None:
            return
        if self._metrics is not None:
            self._metrics.record_rejection(_reason(self.reading()))
        raise refusal

    def refusal(self) -> MsgStorageWatermarkExceededError | None:
        """自检用：处于拒绝态时返回**待抛异常**，不计数（`/readyz` 每次探测不应污染拒绝指标）。

        Returns:
            MsgStorageWatermarkExceededError | None: 拒绝态下的异常实例；可写时 None。
        """
        reading = self.reading()
        if not self._rejected:
            return None
        return MsgStorageWatermarkExceededError(_message(reading), data=_payload(reading, _reason(reading)))

    def _sample(self) -> StorageReading:
        """采一次卷使用率（不推进状态、不写指标）。

        Returns:
            StorageReading: 读数；任何异常都收敛成 `error=<异常类名>` 的不可用读数。
        """
        sampled_at_ms = self._clock()
        try:
            probe = self._existing_ancestor()
            total, used, free = self._usage(probe)
        except OSError as exc:
            return self._unreadable_reading(volume=_volume_of(self._root), error=type(exc).__name__, at=sampled_at_ms)
        if total <= 0:
            return self._unreadable_reading(
                volume=_volume_of(probe), error=StorageUnavailableError.__name__, at=sampled_at_ms
            )
        return StorageReading(
            volume=_volume_of(probe),
            used_ratio=used / total,
            free_bytes=free,
            total_bytes=total,
            warn_ratio=self._warn_ratio,
            reject_ratio=self._reject_ratio,
            error=None,
            sampled_at_ms=sampled_at_ms,
        )

    def _unreadable_reading(self, *, volume: str, error: str, at: float) -> StorageReading:
        """构造「不可用」读数（容量三项一律 `None`，绝不用 0 顶替）。

        Args:
            volume: 卷标识（尽力给出，便于排障）。
            error: 异常类名。
            at: 采样时刻（单调毫秒）。

        Returns:
            StorageReading: 不可用读数。
        """
        return StorageReading(
            volume=volume,
            used_ratio=None,
            free_bytes=None,
            total_bytes=None,
            warn_ratio=self._warn_ratio,
            reject_ratio=self._reject_ratio,
            error=error,
            sampled_at_ms=at,
        )

    def _existing_ancestor(self) -> Path:
        """取数据根最近的已存在祖先（目录懒创建是常态；连祖先都没有才算不可用）。

        Returns:
            Path: 已存在的路径（可能是数据根本身）。

        Raises:
            FileNotFoundError: 一直上溯到根仍不存在（盘符不存在 / 卷未挂载）。
        """
        path = self._root
        while True:
            try:
                path.stat()
            except FileNotFoundError:
                parent = path.parent
                if parent == path:
                    raise
                path = parent
                continue
            return path

    def _apply(self, reading: StorageReading) -> None:
        """按读数推进滞回状态，并在**跨越**时各记一条日志（重复采样不重复记）。

        Args:
            reading: 新读数。
        """
        if reading.unreadable:
            if not self._unreadable:
                self._unreadable = True
                _LOGGER.warning(
                    "msg_storage_watermark_unreadable",
                    volume=reading.volume,
                    reason=reading.error,
                )
            self._rejected = True
            return
        if self._unreadable:
            self._unreadable = False
        if reading.exceeded:
            if not self._rejected:
                self._rejected = True
                _LOGGER.warning("msg_storage_watermark_enter", **_fields(reading))
            return
        if self._rejected and _rounded(reading.used_ratio or 0.0) < self._warn_ratio:
            self._rejected = False
            _LOGGER.info("msg_storage_watermark_release", **_fields(reading))


class StorageMetrics(BaseFrameworkObject):
    """水位指标采集器（进程级单例；判据读它、断言读它、06-3 也读它）。

    **只收集、不暴露**：`snapshot()` 返回纯数据结构，06-3 转成自己的格式即可。
    """

    def __init__(self, *, volume_key_limit: int = STORAGE_VOLUME_KEY_LIMIT) -> None:
        """初始化（全零）。

        Args:
            volume_key_limit: `volume` 分键上限（超出归 `__other__`）。
        """
        self._volume_key_limit = volume_key_limit
        self._volume_keys: set[str] = set()
        self._used_ratio: dict[str, float] = {}
        self._free_bytes: dict[str, int] = {}
        self._rejected: dict[str, int] = {}

    def record_reading(self, reading: StorageReading) -> None:
        """覆盖记录某卷的当前读数（不可用读数不写：避免把故障读成「0% 占用」）。

        Args:
            reading: 水位读数。
        """
        if reading.unreadable or reading.used_ratio is None or reading.free_bytes is None:
            return
        key = self._volume_key(reading.volume)
        self._used_ratio[key] = round(reading.used_ratio, STORAGE_RATIO_DIGITS)
        self._free_bytes[key] = reading.free_bytes

    def record_rejection(self, reason: str) -> None:
        """记一次 `350022` 拒绝（按原因分键）。

        Args:
            reason: 拒绝原因（`watermark` / `sampling_failed`）。
        """
        self._rejected[reason] = self._rejected.get(reason, 0) + 1

    def snapshot(self) -> ConcurrentStableDict[str, Any]:
        """导出当前指标快照（06-3 消费；用例断言）。

        Returns:
            ConcurrentStableDict[str, Any]: 指标名 → 值 / 分键字典。
        """
        snapshot: ConcurrentStableDict[str, Any] = ConcurrentStableDict()
        snapshot.set("msg_disk_used_ratio", ConcurrentStableDict(self._used_ratio))
        snapshot.set("msg_storage_free_bytes", ConcurrentStableDict(self._free_bytes))
        rejected: ConcurrentStableDict[str, int] = ConcurrentStableDict()
        rejected.set(TOTAL_KEY, sum(self._rejected.values()))
        for reason, count in self._rejected.items():
            rejected.set(reason, count)
        snapshot.set("msg_storage_rejected_total", rejected)
        return snapshot

    def _volume_key(self, volume: str) -> str:
        """把卷标识归一成有界的分键。

        Args:
            volume: 卷标识（数据根绝对路径）。

        Returns:
            str: 分键（溢出时为 `__other__`）。
        """
        if volume in self._volume_keys:
            return volume
        if len(self._volume_keys) >= self._volume_key_limit:
            return OTHER_KEY
        self._volume_keys.add(volume)
        return volume


def _monotonic_ms() -> float:
    """当前单调时钟毫秒（TTL 判据，不受系统时钟回拨影响）。

    Returns:
        float: 单调时钟（毫秒）。
    """
    return time.monotonic() * 1_000


def _disk_usage(path: Path) -> tuple[int, int, int]:
    """读卷容量（`shutil.disk_usage` 的唯一包装点，方便用例整体替换）。

    Args:
        path: 已存在的路径。

    Returns:
        tuple[int, int, int]: `(total, used, free)` 字节。
    """
    usage = shutil.disk_usage(path)
    return usage.total, usage.used, usage.free


def _rounded(ratio: float) -> float:
    """按 `STORAGE_RATIO_DIGITS` 位小数取整（阈值比较的唯一口径）。

    Args:
        ratio: 使用率。

    Returns:
        float: 取整后的使用率。
    """
    return round(ratio, STORAGE_RATIO_DIGITS)


def _resolve_ratio(explicit: float | None, env_name: str, default: float) -> float:
    """解析阈值：显式参数 > 环境变量 > 模块常量，并校验 0~1 开区间。

    Args:
        explicit: 显式传入值（None 表示取环境变量 / 常量）。
        env_name: 环境变量名。
        default: 缺省值。

    Returns:
        float: 合法阈值。

    Raises:
        ValueError: 取值非法（非数字或不在 0~1 开区间）。
    """
    raw = explicit if explicit is not None else os.environ.get(env_name)
    if raw is None or raw == "":
        return default
    try:
        value = float(raw)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{env_name} 取值非法：{raw!r}（须为 0~1 的小数）") from exc
    if not 0.0 < value < 1.0:
        raise ValueError(f"{env_name} 取值越界：{value}（须满足 0 < 值 < 1）")
    return value


def _resolve_ttl(ttl_ms: int | None) -> int:
    """解析采样缓存有效期（毫秒，须为正）。

    Args:
        ttl_ms: 显式传入值（None 表示取环境变量 / 常量）。

    Returns:
        int: 合法有效期（毫秒）。

    Raises:
        ValueError: 取值非正或非整数。
    """
    raw: object = ttl_ms if ttl_ms is not None else os.environ.get(STORAGE_SAMPLE_TTL_MS_ENV, STORAGE_SAMPLE_TTL_MS)
    try:
        value = int(str(raw))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{STORAGE_SAMPLE_TTL_MS_ENV} 取值非法：{raw!r}（须为正整数毫秒）") from exc
    if value <= 0:
        raise ValueError(f"{STORAGE_SAMPLE_TTL_MS_ENV} 须为正数：{value}")
    return value


def _volume_of(path: Path) -> str:
    """卷标识：路径的绝对形式（不解析符号链接，排障时保持与配置一致）。

    Args:
        path: 数据根或最近存在祖先。

    Returns:
        str: 绝对路径字符串。
    """
    try:
        return str(Path(os.path.abspath(path)))
    except OSError:  # pragma: no cover - abspath 只做字符串拼接，异常路径不可达
        return str(path)


def _reason(reading: StorageReading) -> str:
    """拒绝原因（`data.reason` 与指标分键的唯一出处）。

    Args:
        reading: 水位读数。

    Returns:
        str: `sampling_failed`（不可用）或 `watermark`（超高水位）。
    """
    return REASON_SAMPLING_FAILED if reading.unreadable else REASON_WATERMARK


def _fields(reading: StorageReading) -> ConcurrentStableDict[str, object]:
    """日志字段（进入 / 解除两条事件共用一份，防两处口径不一致）。

    Args:
        reading: 水位读数。

    Returns:
        ConcurrentStableDict[str, object]: 结构化字段。
    """
    return ConcurrentStableDict(
        {
            "volume": reading.volume,
            "used_ratio": round(reading.used_ratio or 0.0, STORAGE_RATIO_DIGITS),
            "threshold": reading.reject_ratio,
            "free_bytes": reading.free_bytes or 0,
        }
    )


# bare-collections:allow `ApiResponse.data` 走 pydantic 序列化，自定义 Mapping 会把 507 退化成 500
def _payload(reading: StorageReading, reason: str) -> dict[str, object]:
    """`350022` 的 `data` 段（JSON 与帧 `detail` 共用一份结构）。

    **必须是普通 dict**：`ApiResponse.data` 走 pydantic 序列化，自定义 Mapping 会抛
    `PydanticSerializationError`，把 507 退化成 500（02-7 实测，06-1-1 闭环）。

    Args:
        reading: 水位读数。
        reason: 拒绝原因。

    Returns:
        dict[str, object]: 五个字段。
    """
    return {
        "used_ratio": None if reading.used_ratio is None else round(reading.used_ratio, STORAGE_RATIO_DIGITS),
        "threshold": reading.reject_ratio,
        "volume": reading.volume,
        "free_bytes": reading.free_bytes,
        "reason": reason,
    }


def _message(reading: StorageReading) -> str:
    """构造 `350022` 的提示文案（**同时带数字**：日志与不解析 `data` 的客户端都要能直读）。

    Args:
        reading: 水位读数。

    Returns:
        str: 中文提示（含当前水位、阈值、数据卷与剩余空间）。
    """
    if reading.unreadable:
        return f"消息数据卷不可用（采样失败：{reading.error}），请检查挂载与权限后重试"
    used = round((reading.used_ratio or 0.0) * 100, 2)
    threshold = round(reading.reject_ratio * 100, 2)
    return (
        f"磁盘水位超过拒绝阈值：已用 {used}% ≥ {threshold}%"
        f"（卷 {reading.volume}，剩余 {_human_bytes(reading.free_bytes or 0)}），请清理过期段或扩容后重试"
    )


def _human_bytes(size: int) -> str:
    """字节数转可读文本（1 位小数，单位 GiB / MiB / KiB）。

    Args:
        size: 字节数。

    Returns:
        str: 形如 `1.3 GiB` 的文本。
    """
    value = float(size)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if value < 1024.0 or unit == "TiB":
            return f"{value:.0f} {unit}" if unit == "B" else f"{value:.1f} {unit}"
        value /= 1024.0
    return f"{value:.1f} TiB"  # pragma: no cover - 循环在 TiB 处必然返回
