"""文件段消息存储（02-1 详细设计 §4~§11）：`MessageStore` 端口的生产实现。

五条结构性口径：

1. **目录由登记表给，不由存储层查库**：端口只收 `topic_id`，而落盘路径要 `tenant_id` / `namespace` / 主题名
   三段。这三段由 `TopicRouter.resolve()` 在解析时登记进进程内 `PartitionDirectory`，store 只读它；
   未登记即抛 `PartitionNotBoundError`（fail-closed）——绝不把数据写到「猜出来的」目录。
2. **offset 只由 `append` 分配**：`offset = LEO`，写完 `LEO += 1`；其他方法一律不编号
   （概要设计 §5 的硬约束，端口签名不变的前提）。
3. **只前移起点、不动末端**：`truncate_before` 整段删除并把 `offset` 向下取整到段边界，`LEO` 不变——
   因此「位点早于起点 → `350003`」这条 01-2 契约原样成立。
4. **删光段要留一个空段**：`start == leo` 只存在于内存时，重启后 LEO 会归零、已提交位点指向别的消息。
   故删到空立刻建 `base = LEO` 的 0 字节段承载 LEO；它不出现在 `segments()` 里（对保留判定不可见）。
5. **不自己通知**：写入即唤醒由最外层 `NotifyingMessageStore` 在 `append` 之后完成（03 域收口回写），
   本实现不持有等待器、也不假定包装器可省。
"""

import asyncio
import hashlib
import os
import shutil
import struct
import time
from collections import OrderedDict
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Final
from uuid import uuid4

from bms_core.core.concurrent import ConcurrentStableDict, ConcurrentStableList, ConcurrentStableSet
from bms_core.core.exceptions import ParamError
from bms_core.core.logging import get_logger
from bms_core.core.objects import BaseValueObject
from bms_core.msgclient.payload import COMPRESSION_NONE, DEFAULT_CONTENT_TYPE

from bms_msg.broker.log import (
    MessageHeaderView,
    MessageStore,
    PartitionNotBoundError,
    SegmentMeta,
    StoredMessage,
    message_header_view,
)
from bms_msg.broker.offset import OffsetRealigner
from bms_msg.broker.record import (
    ParsedRecord,
    RecordHeader,
    encode_record,
    from_epoch_us,
    to_epoch_us,
)
from bms_msg.broker.recovery import (
    RecoveryLedger,
    RecoveryPoint,
    RecoveryProbe,
    RecoveryReport,
    SegmentStamp,
    read_recovery_point,
    write_recovery_point,
)
from bms_msg.broker.segment import SegmentFile, list_segment_bases
from bms_msg.core.config import (
    CORRUPT_TAIL_KEEP,
    DATA_DIR_ENV,
    DATA_ROOT_SUBDIR,
    DEFAULT_DATA_DIR,
    DEFAULT_FSYNC_MODE,
    FSYNC_BATCH_RECORDS,
    FSYNC_INTERVAL_MS,
    FSYNC_INTERVAL_MS_ENV,
    FSYNC_MODE_BATCH,
    FSYNC_MODE_EACH,
    FSYNC_MODE_ENV,
    FSYNC_MODE_OS,
    FSYNC_MODE_TIME,
    INDEX_INTERVAL_BYTES,
    LOG_ID_INDEX_LIMIT,
    RECORD_FORMAT_VERSION,
    RECOVERY_POINT_NAME,
    SEGMENT_MAX_BYTES,
    SEGMENT_ROLL_MS,
    TIME_INDEX_INTERVAL_MS,
)
from bms_msg.core.trace import TraceContext

__all__ = [
    "FileMessageStore",
    "FileSlice",
    "PartitionDirectory",
    "PartitionNotBoundError",
    "TopicLocation",
    "data_root",
]

_LOGGER = get_logger("bms_msg.store")
"""存储层日志（只出结构化事件：段轮转 / 删除 / 刷盘 / 恢复 / 目录登记冲突）。"""

_FSYNC_MODES: Final = frozenset({FSYNC_MODE_BATCH, FSYNC_MODE_TIME, FSYNC_MODE_EACH, FSYNC_MODE_OS})
"""合法的 fsync 档位（非法值即启动失败，不静默降级成某个档）。"""

LOG_INDEX_NAME = "log_id.idx"
"""每分区 `log_id` 持久化索引文件名（02-8-4；与 `.log` / `.index` / `.timeindex` 并列）。"""

LOG_ID_INDEX_SCAN_LIMIT = 4_096
"""单次全局查找最多扫描的分区索引文件数（02-8-4；防冷路径拖垮进程）。"""

_LOG_INDEX_ENTRY = struct.Struct(">QQ")
"""索引条目：`hash64(log_id)` + `offset`（16 字节定长；哈希只筛候选，命中后必须回读比对）。"""

_LOG_INDEX_OPEN_MODE = os.O_WRONLY | os.O_CREAT | os.O_APPEND | getattr(os, "O_BINARY", 0)
"""索引追加写的打开标志（必须带 `O_BINARY`，理由同段文件：Windows 文本模式会翻译字节）。"""


def log_id_hash(log_id: str) -> int:
    """算 `log_id` 的 64 位哈希（索引筛选用，不承担唯一性）。

    Args:
        log_id: 记录标识。

    Returns:
        int: 64 位无符号哈希。
    """
    return int.from_bytes(hashlib.blake2b(log_id.encode("utf-8"), digest_size=8).digest(), "big")


def append_log_index(path: Path, *, log_id: str, offset: int) -> None:
    """向分区索引文件追加一条 `log_id → offset`（fail-open：失败只告警）。

    Args:
        path: 索引文件路径。
        log_id: 记录标识。
        offset: 分区内序号。
    """
    payload = _LOG_INDEX_ENTRY.pack(log_id_hash(log_id), int(offset))
    try:
        fd = os.open(path, _LOG_INDEX_OPEN_MODE, 0o644)
        try:
            os.write(fd, payload)
        finally:
            os.close(fd)
    except OSError as exc:
        _LOGGER.warning("msg_log_index_append_failed", path=str(path), error=str(exc))


def iter_log_index(path: Path) -> ConcurrentStableList[tuple[int, int]]:
    """读一个分区索引文件（尾部残条忽略）。

    Args:
        path: 索引文件路径。

    Returns:
        ConcurrentStableList[tuple[int, int]]: `(hash64, offset)` 列表；文件不存在返回空。
    """
    try:
        raw = path.read_bytes()
    except OSError:
        return ConcurrentStableList()
    usable = len(raw) - (len(raw) % _LOG_INDEX_ENTRY.size)
    return ConcurrentStableList(
        _LOG_INDEX_ENTRY.unpack_from(raw, position) for position in range(0, usable, _LOG_INDEX_ENTRY.size)
    )


def lookup_log_index_file(path: Path, *, log_id: str) -> ConcurrentStableList[int]:
    """在索引文件里按哈希筛候选 offset（不做完整 `log_id` 校验）。

    Args:
        path: 索引文件路径。
        log_id: 记录标识。

    Returns:
        ConcurrentStableList[int]: 候选 offset（哈希碰撞时可能多于一个，调用方回读比对）。
    """
    target = log_id_hash(log_id)
    return ConcurrentStableList(offset for hashed, offset in iter_log_index(path) if hashed == target)


def _monotonic_ms() -> float:
    """当前单调时钟毫秒（fsync 时间窗判据，不受系统时钟回拨影响）。

    Returns:
        float: 单调时钟（毫秒）。
    """
    return time.monotonic() * 1_000


def data_root() -> Path:
    """数据根：`MSG_DATA_DIR` 优先，缺省相对 `var/lib/msg`（不写死绝对路径）。

    Returns:
        Path: 数据根目录（未创建）。
    """
    return Path(os.environ.get(DATA_DIR_ENV, DEFAULT_DATA_DIR))


def _timestamp() -> str:
    """截断取证文件名里的时间戳（UTC）。

    Returns:
        str: `%Y%m%dT%H%M%S%f` 形态的 UTC 时刻（可字典序排序，便于按时间淘汰）。
    """
    return datetime.now(UTC).strftime("%Y%m%dT%H%M%S%f")


@dataclass(frozen=True)
class TopicLocation(BaseValueObject):
    """主题在磁盘上的三段身份（登记表的值）。"""

    tenant_id: int
    """租户主键（不可变雪花，目录不随租户改名漂移）。"""

    namespace: str
    """命名空间。"""

    name: str
    """主题名（可读性用；与 topic_id 一起构成目录名）。"""

    topic_id: int
    """主题主键（目录名后缀，删除重建同名时天然隔离）。"""


class PartitionDirectory:
    """`topic_id` → 目录三段的进程内登记表（无 await，单事件循环内原子）。

    生命周期随进程：多实例各持一份（跨实例归 8B_05，与 01-7 主题缓存同口径）。
    """

    def __init__(self, *, root: Path) -> None:
        """初始化。

        Args:
            root: 数据根（分区目录为 `{root}/data/{tenant}/{namespace}/{topic}.{topic_id}/{partition}`）。
        """
        self._root = root
        self._locations: dict[int, TopicLocation] = {}

    @property
    def root(self) -> Path:
        """数据根（02-7 延迟区重建要遍历 `{root}/data/` 找 `.delay` 段）。

        Returns:
            Path: 数据根目录。
        """
        return self._root

    def bind(self, *, tenant_id: int, namespace: str, name: str, topic_id: int) -> bool:
        """登记主题目录身份（幂等；值冲突时保留首次绑定）。

        Args:
            tenant_id: 租户主键。
            namespace: 命名空间。
            name: 主题名。
            topic_id: 主题主键。

        Returns:
            bool: True 表示发生值冲突（首次绑定被保留），由调用方记日志。
        """
        existing = self._locations.get(topic_id)
        incoming = TopicLocation(tenant_id=int(tenant_id), namespace=namespace, name=name, topic_id=int(topic_id))
        if existing is None:
            self._locations[topic_id] = incoming
            return False
        if existing != incoming:
            _LOGGER.warning(
                "msg_partition_bind_conflict",
                topic_id=int(topic_id),
                bound=f"{existing.namespace}/{existing.name}",
                incoming=f"{namespace}/{name}",
            )
            return True
        return False

    def locate(self, topic_id: int) -> TopicLocation | None:
        """按主题主键取目录身份。

        Args:
            topic_id: 主题主键。

        Returns:
            TopicLocation | None: 已登记返回身份；未登记返回 None。
        """
        return self._locations.get(int(topic_id))

    def all_locations(self) -> ConcurrentStableList[TopicLocation]:
        """列出本进程已登记的全部主题目录身份（02-8-4：`log_id` 磁盘索引扫描用）。

        Returns:
            ConcurrentStableList[TopicLocation]: 目录身份（插入序）。
        """
        return ConcurrentStableList(self._locations.values())

    def forget(self, topic_id: int) -> None:
        """移除一个主题的目录登记（主题删除清理完成后调用）。

        Args:
            topic_id: 主题主键。
        """
        self._locations.pop(int(topic_id), None)

    def path_of(self, location: TopicLocation, partition_no: int) -> Path:
        """算一个分区的目录路径（不创建）。

        Args:
            location: 主题目录身份。
            partition_no: 分区号。

        Returns:
            Path: 分区目录。
        """
        return (
            self._root
            / DATA_ROOT_SUBDIR
            / str(location.tenant_id)
            / location.namespace
            / f"{location.name}.{location.topic_id}"
            / str(partition_no)
        )

    def partition_path(self, *, topic_id: int, partition_no: int) -> Path:
        """按主题主键与分区号取分区目录（未登记即从盘上反查，仍找不到才抛错）。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。

        Returns:
            Path: 分区目录。

        Raises:
            PartitionNotBoundError: 主题目录未登记且盘上不存在。
        """
        location = self._locations.get(int(topic_id))
        if location is None:
            location = self.recover(topic_id=int(topic_id))
        if location is None:
            raise PartitionNotBoundError(f"主题目录未登记：topic_id={topic_id}（须先经 TopicRouter.resolve 解析）")
        return self.path_of(location, partition_no)

    def recover(self, *, topic_id: int) -> TopicLocation | None:
        """从盘上反查已存在的目录并登记（只认「目录名以 `.{topic_id}` 结尾」，不猜名字）。

        为什么需要：重启后延迟转投、重放执行器、长驻流内部拉取都是**内部调用**，不经过
        `TopicRouter.resolve()`；若只认请求路径登记，这些写入会在首个外部请求之前直接失败。

        Args:
            topic_id: 主题主键。

        Returns:
            TopicLocation | None: 找回的目录身份；盘上不存在返回 None。
        """
        data_root = self._root / DATA_ROOT_SUBDIR
        if not data_root.is_dir():
            return None
        suffix = f".{int(topic_id)}"
        for tenant_dir in data_root.iterdir():
            if not tenant_dir.is_dir() or not tenant_dir.name.isdigit():
                continue
            for namespace_dir in tenant_dir.iterdir():
                if not namespace_dir.is_dir():
                    continue
                for topic_dir in namespace_dir.iterdir():
                    if not topic_dir.is_dir() or not topic_dir.name.endswith(suffix):
                        continue
                    location = TopicLocation(
                        tenant_id=int(tenant_dir.name),
                        namespace=namespace_dir.name,
                        name=topic_dir.name[: -len(suffix)],
                        topic_id=int(topic_id),
                    )
                    self._locations[int(topic_id)] = location
                    return location
        return None

    def __len__(self) -> int:
        """已登记主题数（排障与用例断言用）。

        Returns:
            int: 条目数。
        """
        return len(self._locations)


@dataclass(frozen=True)
class FileSlice(BaseValueObject):
    """一条记录在磁盘上的坐标（段文件 + 偏移 + 长度；供 03 域接 `sendfile`）。"""

    path: Path
    """段文件路径。"""

    position: int
    """记录起始的字节偏移。"""

    length: int
    """记录总长（含 4 字节 `length` 字段）。"""


class _PartitionLog:
    """单个分区的段列表 + LEO + 起点 + 写锁 + fsync 状态。"""

    def __init__(
        self,
        *,
        directory: Path,
        topic_id: int,
        partition_no: int,
        fsync_mode: str,
        fsync_interval_ms: int,
        fsync_batch_records: int,
        segment_max_bytes: int,
        segment_roll_ms: int,
        index_interval: int,
        time_index_interval_ms: int,
        clock: Callable[[], float],
        probe: RecoveryProbe | None = None,
    ) -> None:
        """初始化并加载已有段（加载即完成崩溃恢复，不做任何写入）。

        Args:
            directory: 分区目录。
            topic_id: 主题主键（恢复点归属自检）。
            partition_no: 分区号（恢复点归属自检）。
            fsync_mode: fsync 档位（已校验）。
            fsync_interval_ms: 刷盘时间窗（毫秒）。
            fsync_batch_records: 按批档的条数阈值。
            segment_max_bytes: 段滚动字节阈值。
            segment_roll_ms: 段滚动时长（毫秒）。
            index_interval: 稀疏 offset 索引间隔（字节）。
            time_index_interval_ms: 时间索引间隔（毫秒）。
            clock: 单调毫秒时钟（用例注入以确定性驱动刷盘窗口）。
            probe: 恢复读取量探针（缺省不统计）。
        """
        self._directory = directory
        self._topic_id = topic_id
        self._partition_no = partition_no
        self._fsync_mode = fsync_mode
        self._fsync_interval_ms = fsync_interval_ms
        self._fsync_batch_records = fsync_batch_records
        self._segment_max_bytes = segment_max_bytes
        self._segment_roll_ms = segment_roll_ms
        self._index_interval = index_interval
        self._time_index_interval_ms = time_index_interval_ms
        self._clock = clock
        self._probe = probe
        self._segments: list[SegmentFile] = []
        self._ensured = False
        self._leo = 0
        self._start = 0
        self._unflushed = 0
        self._last_fsync_ms = clock()
        self._report = RecoveryReport()
        self._corrupted: dict[int, str] = {}
        self._stamps: dict[int, SegmentStamp] = {}
        self._deleted: ConcurrentStableSet[str] = ConcurrentStableSet()
        self._log_index_path = directory / LOG_INDEX_NAME
        self._log_index: ConcurrentStableDict[int, int] | None = None
        self.lock = asyncio.Lock()
        self._load()
        self._rebuild_deleted()

    @property
    def leo(self) -> int:
        """分区日志末端位点。

        Returns:
            int: LEO。
        """
        return self._leo

    @property
    def start(self) -> int:
        """可读区间起点。

        Returns:
            int: 起点。
        """
        return self._start

    @property
    def needs_alignment(self) -> bool:
        """是否需要位点对齐（发生截断，或恢复点声明的 LEO 大于推导 LEO）。

        Returns:
            bool: True 表示该分区有待对齐的位点。
        """
        return self._report.needs_alignment

    @property
    def report(self) -> RecoveryReport:
        """本次加载的恢复结果。

        Returns:
            RecoveryReport: 恢复报告。
        """
        return self._report

    def corrupted_segments(self) -> tuple[int, ...]:
        """非末段对账失败的段 base 列表（只读查询，供排障与指标出口）。

        Returns:
            tuple[int, ...]: 段 base 升序。
        """
        return tuple(sorted(self._corrupted))

    def _rebuild_deleted(self) -> None:
        """扫描已加载段，重建删除标记集合（05-4-1；启动 / 首次打开一次，幂等）。

        损坏段在恢复阶段已隔离 / 取证，这里按段容错跳过，不因重建失败阻断启动。
        """
        for segment in self._segments:
            try:
                for frame_position, _total, _header_len in segment.iter_frames(from_position=0):
                    parsed = segment.read_record(position=frame_position)
                    target = parsed.header.delete_target
                    if target is not None:
                        self._deleted.add(target)
            except Exception as exc:  # pragma: no cover - 损坏段由恢复流程处理
                _LOGGER.warning(
                    "msg_deleted_rebuild_skip_segment",
                    base_offset=segment.base_offset,
                    error=repr(exc),
                )

    def segment_ends(self) -> list[int]:
        """每段的尾部 offset（末段为 LEO）。

        Returns:
            list[int]: 与段列表同长的 end offset 列表。
        """
        ends: list[int] = []
        for index in range(len(self._segments)):
            if index + 1 < len(self._segments):
                ends.append(self._segments[index + 1].base_offset)
            else:
                ends.append(self._leo)
        return ends

    def log_index_lookup(self, *, log_id: str) -> int | None:
        """在持久化 `log_id` 索引里查一个候选 offset（02-8-4）。

        索引缺失时按段头重建一次；返回的是**哈希候选**，调用方必须回读比对完整 `log_id`。

        Args:
            log_id: 记录标识。

        Returns:
            int | None: 候选 offset；无候选返回 None。
        """
        index = self._ensure_log_index()
        return index.get(log_id_hash(log_id))

    def log_index_path(self) -> Path:
        """本分区 `log_id` 索引文件路径。

        Returns:
            Path: 索引文件路径。
        """
        return self._log_index_path

    def _ensure_log_index(self) -> ConcurrentStableDict[int, int]:
        """载入索引（首次访问；文件缺失时按段头重建）。

        Returns:
            ConcurrentStableDict[int, int]: `hash64 → offset`。
        """
        if self._log_index is not None:
            return self._log_index
        if not self._log_index_path.exists():
            return self._rebuild_log_index()
        index: ConcurrentStableDict[int, int] = ConcurrentStableDict()
        for hashed, offset in iter_log_index(self._log_index_path):
            index[hashed] = offset
        self._log_index = index
        return index

    def _rebuild_log_index(self) -> ConcurrentStableDict[int, int]:
        """按段头重建索引文件（索引缺失 / 损坏时一次）。

        Returns:
            ConcurrentStableDict[int, int]: 重建后的 `hash64 → offset`。
        """
        index: ConcurrentStableDict[int, int] = ConcurrentStableDict()
        rows: ConcurrentStableList[bytes] = ConcurrentStableList()
        for offset, _position, header in self.iterate_headers():
            if not header.log_id:  # pragma: no cover - 记录恒有 log_id（防御分支）
                continue
            hashed = log_id_hash(header.log_id)
            index[hashed] = offset
            rows.append(_LOG_INDEX_ENTRY.pack(hashed, offset))
        try:
            self._ensure_directory()
            self._log_index_path.write_bytes(b"".join(rows))
        except OSError as exc:
            _LOGGER.warning("msg_log_index_rebuild_failed", path=str(self._log_index_path), error=str(exc))
        self._log_index = index
        if rows:
            _LOGGER.info("msg_log_index_rebuilt", partition_path=str(self._directory), entries=len(rows))
        return index

    def _append_log_index(self, *, log_id: str, offset: int) -> None:
        """写一条索引并同步内存视图（fail-open）。

        Args:
            log_id: 记录标识。
            offset: 分区内序号。
        """
        if self._log_index is not None:
            self._log_index[log_id_hash(log_id)] = offset
        append_log_index(self._log_index_path, log_id=log_id, offset=offset)

    def append(
        self,
        *,
        topic_id: int,
        partition_no: int,
        tenant_id: int,
        message_id: str,
        log_id: str,
        key: str | None,
        payload: bytes,
        headers: ConcurrentStableDict[str, str],
        trace: TraceContext,
        published_at: datetime,
        content_type: str,
        compression: str,
        uncompressed_len: int | None,
        epoch: int = 0,
        delete_target: str | None = None,
    ) -> StoredMessage:
        """追加一条消息并分配 offset（调用方须持锁）。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。
            tenant_id: 租户主键。
            message_id: 消息身份。
            log_id: 记录身份。
            key: 分区键（可空）。
            payload: 业务字节。
            headers: 结构化头。
            trace: 链路字段（06-4；四个字段一物一传）。
            published_at: 发布时间（必须带时区）。
            content_type: 内容类型。
            compression: 压缩算法。
            uncompressed_len: 解压后字节数（可空）。
            epoch: leader 任期（02-4；follower 复制落盘时沿用 leader 值）。

        Returns:
            StoredMessage: 含分配到的 offset 的完整记录。
        """
        published_at_us = to_epoch_us(published_at)
        header = RecordHeader(
            format_version=RECORD_FORMAT_VERSION,
            topic_id=topic_id,
            partition_no=partition_no,
            tenant_id=tenant_id,
            message_id=message_id,
            log_id=log_id,
            key=key,
            trace=trace,
            published_at_us=published_at_us,
            headers=tuple((str(name), str(value)) for name, value in headers.items()),
            content_type=content_type,
            compression=compression,
            uncompressed_len=uncompressed_len,
            epoch=epoch,
            delete_target=delete_target,
        )
        blob = encode_record(header=header, payload=payload)
        self._ensure_directory()
        offset = self._leo
        if self._needs_roll(record_bytes=len(blob), published_at_us=published_at_us):
            self._roll(new_base=offset)
        if not self._segments:
            self._open_segment(base_offset=offset)
        segment = self._segments[-1]
        segment.append_record(offset=offset, published_at_us=published_at_us, blob=blob)
        self._leo = offset + 1
        self._unflushed += 1
        self._maybe_fsync()
        self._append_log_index(log_id=log_id, offset=offset)
        if delete_target is not None:
            self._deleted.add(delete_target)
        return StoredMessage(
            message_id=message_id,
            log_id=log_id,
            topic_id=topic_id,
            partition_no=partition_no,
            offset=offset,
            key=key,
            payload=payload,
            headers=headers,
            trace=trace,
            published_at=published_at,
            content_type=content_type,
            compression=compression,
            uncompressed_len=uncompressed_len,
            epoch=epoch,
        )

    def read(self, *, from_offset: int, max_messages: int) -> ConcurrentStableList[StoredMessage]:
        """从 `from_offset` 起取至多 `max_messages` 条（跨段连续）。

        Args:
            from_offset: 起始位点（早于起点按起点）。
            max_messages: 本次上限。

        Returns:
            ConcurrentStableList[StoredMessage]: 按 offset 升序的记录。

        Raises:
            CorruptedRecordError: 记录损坏（不静默跳过）。
        """
        messages: ConcurrentStableList[StoredMessage] = ConcurrentStableList()
        if max_messages <= 0:
            return messages
        cursor = max(from_offset, self._start)
        ends = self.segment_ends()
        for index, segment in enumerate(self._segments):
            end = ends[index]
            if end <= cursor:
                continue
            if cursor < segment.base_offset:  # pragma: no cover - 段区间连续覆盖 [start, leo)，不会出现空隙
                cursor = segment.base_offset
            position, relative = segment.position_of(offset=cursor)
            current = segment.base_offset + relative
            for frame_position, _total, _header_len in segment.iter_frames(from_position=position):
                if current < cursor:
                    current += 1
                    continue
                if current >= end or len(messages) >= max_messages:
                    break
                parsed = segment.read_record(position=frame_position)
                if parsed.header.delete_target is not None or parsed.header.log_id in self._deleted:
                    current += 1
                    continue
                messages.add(_to_message(parsed, offset=current))
                current += 1
            cursor = max(cursor, current)
            if len(messages) >= max_messages:
                return messages
        return messages

    def segments(self) -> ConcurrentStableList[SegmentMeta]:
        """按 offset 升序取段元数据（空段不暴露）。

        Returns:
            ConcurrentStableList[SegmentMeta]: 段元数据。
        """
        metas: ConcurrentStableList[SegmentMeta] = ConcurrentStableList()
        ends = self.segment_ends()
        for index, segment in enumerate(self._segments):
            end = ends[index]
            if end <= segment.base_offset:
                continue
            metas.add(
                SegmentMeta(
                    base_offset=segment.base_offset,
                    end_offset=end,
                    max_published_at=from_epoch_us(segment.max_published_us()),
                    size_bytes=segment.size_bytes,
                )
            )
        return metas

    def truncate_before(self, *, offset: int) -> int:
        """把可读区间起点前移到 `offset`（向下取整到段边界；整段删除）。

        Args:
            offset: 目标起点。

        Returns:
            int: 前移后的起点。

        Raises:
            ParamError: `offset > LEO`（10001）。
        """
        if offset > self._leo:
            raise ParamError(f"清理起点越界：{offset} > LEO {self._leo}")
        if offset <= self._start:
            return self._start
        candidates = [segment.base_offset for segment in self._segments if segment.base_offset <= offset]
        if self._leo <= offset:
            # 目标等于 LEO：允许删到空（时间维度全过期就该全丢），由空段承载 LEO
            candidates.append(self._leo)
        target = max(candidates)
        if target <= self._start:
            return self._start
        removed = [segment for segment in self._segments if segment.base_offset < target]
        kept = [segment for segment in self._segments if segment.base_offset >= target]
        deleted_bytes = 0
        for segment in removed:
            deleted_bytes += segment.size_bytes
            self._stamps.pop(segment.base_offset, None)
            segment.delete()
        self._segments = kept
        self._start = target
        if not kept:
            self._open_segment(base_offset=target)
        self.persist_recovery_point()
        _LOGGER.info(
            "msg_segment_deleted",
            partition_path=str(self._directory),
            base_offset=target,
            files=len(removed) * 3,
            deleted_bytes=deleted_bytes,
        )
        return target

    def truncate_to(self, *, offset: int) -> int:
        """截掉 `offset` 及之后的记录（02-5 切换截断；调用方须持分区锁）。

        实现复用 02-2 的段级截断：定位 offset 所在段与字节位置 → `SegmentFile.truncate`
        截字节并裁剪越界索引 → 删除后续段 → 回写恢复点。不保留被截字节副本（有意截断，不是损坏）。

        Args:
            offset: 保留到的位置（不含；目标 LEO）。

        Returns:
            int: 截断后的 LEO。

        Raises:
            ParamError: `offset < start`（10001）。
        """
        if offset < self._start:
            raise ParamError(f"截断位置早于起点：{offset} < {self._start}")
        if offset >= self._leo:
            return self._leo
        ends = self.segment_ends()
        keep_index: int | None = None
        for index, segment in enumerate(self._segments):
            if segment.base_offset <= offset < ends[index]:
                keep_index = index
                break
        if keep_index is None:  # pragma: no cover - offset 在 [start, leo) 内必落在某段
            return self._leo
        keep = self._segments[keep_index]
        if offset == keep.base_offset:
            removed = self._segments[keep_index:]
            self._segments = self._segments[:keep_index]
        else:
            target_position = self._record_position(segment=keep, offset=offset)
            keep.truncate(size=target_position, max_relative_offset=offset - keep.base_offset - 1)
            self._stamps.pop(keep.base_offset, None)
            removed = self._segments[keep_index + 1 :]
            self._segments = self._segments[: keep_index + 1]
        for segment in removed:
            self._stamps.pop(segment.base_offset, None)
            segment.delete()
        self._leo = offset
        self._unflushed = 0
        if not self._segments:
            self._open_segment(base_offset=offset)
        self.persist_recovery_point()
        _LOGGER.warning(
            "msg_partition_tail_truncated",
            partition_path=str(self._directory),
            to_offset=offset,
            files=len(removed) * 3,
        )
        return offset

    def _record_position(self, *, segment: SegmentFile, offset: int) -> int:
        """取某条记录在段内的起始字节位置。

        Args:
            segment: 段。
            offset: 分区内序号（须在段内）。

        Returns:
            int: 记录起始字节位置。

        Raises:
            ParamError: offset 不在段内（调用方已按段区间筛选）。
        """
        position, relative = segment.position_of(offset=offset)
        current = segment.base_offset + relative
        for frame_position, _total, _header_len in segment.iter_frames(from_position=position):
            if current == offset:
                return frame_position
            current += 1
        raise ParamError(f"记录位置不存在：{offset}")  # pragma: no cover - 调用方已保证段内

    def locate(self, *, offset: int) -> FileSlice | None:
        """取一条记录的坐标（不读载荷）。

        Args:
            offset: 分区内序号。

        Returns:
            FileSlice | None: 坐标；越界返回 None。
        """
        if offset < self._start or offset >= self._leo:
            return None
        ends = self.segment_ends()
        for index, segment in enumerate(self._segments):
            if not segment.base_offset <= offset < ends[index]:
                continue
            position, relative = segment.position_of(offset=offset)
            current = segment.base_offset + relative
            # offset 落在本段区间内时，内层扫描必命中并返回；循环「正常结束」不可达
            for frame_position, total, _header_len in segment.iter_frames(from_position=position):  # pragma: no cover
                if current == offset:
                    return FileSlice(path=segment.log_path, position=frame_position, length=total)
                current += 1
        return None  # pragma: no cover - offset 在 [start, leo) 内必落在某段且段内必命中

    def read_range(self, *, from_offset: int, max_messages: int) -> ConcurrentStableList[FileSlice]:
        """取一批记录的坐标（不读载荷）。

        Args:
            from_offset: 起始位点。
            max_messages: 本次上限。

        Returns:
            ConcurrentStableList[FileSlice]: 坐标列表（按 offset 升序）。
        """
        slices: ConcurrentStableList[FileSlice] = ConcurrentStableList()
        cursor = max(from_offset, self._start)
        while len(slices) < max_messages and cursor < self._leo:
            located = self.locate(offset=cursor)
            if located is None:  # pragma: no cover - cursor < leo 时 locate 必返回
                break
            slices.add(located)
            cursor += 1
        return slices

    def read_bytes(self, *, offset: int) -> StoredMessage | None:
        """读一条并解码（校验 CRC）。

        Args:
            offset: 分区内序号。

        Returns:
            StoredMessage | None: 记录；越界返回 None。

        Raises:
            CorruptedRecordError: 记录损坏。
        """
        located = self.locate(offset=offset)
        if located is None:
            return None
        segment = self._segment_by_path(located.path)
        if segment is None:  # pragma: no cover - 同一临界区内段不会消失
            return None
        parsed = segment.read_record(position=located.position)
        if parsed.header.delete_target is not None or parsed.header.log_id in self._deleted:
            return None
        return _to_message(parsed, offset=offset)

    def iterate_headers(self) -> ConcurrentStableList[tuple[int, int, RecordHeader]]:
        """按 offset 升序产出全部记录的「offset / 坐标 / 头」（`log_id` 扫描用）。

        Returns:
            ConcurrentStableList[tuple[int, int, RecordHeader]]: offset / 字节偏移 / 头。
        """
        found: ConcurrentStableList[tuple[int, int, RecordHeader]] = ConcurrentStableList()
        ends = self.segment_ends()
        for index, segment in enumerate(self._segments):
            if ends[index] <= segment.base_offset:
                continue
            current = segment.base_offset
            for frame_position, _total, header in segment.iter_headers():
                if current >= ends[index]:  # pragma: no cover - ends 与段内条数一致，由 iter_headers 结束
                    break
                found.add((current, frame_position, header))
                current += 1
        return found

    def close(self) -> None:
        """关闭全部写句柄（幂等）。"""
        for segment in self._segments:
            segment.close()

    def log_ids_since(self, *, since_us: int, candidates: set[str]) -> set[str]:
        """取本分区日志里「发布时间不早于 `since_us`」且命中候选集的 `log_id`（02-7 恢复期去重）。

        **为什么以候选集为查询集**：`.delay` 侧条数有上限（≤ 延迟区条数），而「自某时刻起的日志条数」
        可能很大——反过来的集合会让内存与扫描都随日志量增长。

        **为什么锚点取「最后一条时间索引项 ≤ since_us」**：时间索引是稀疏的（每 1s 一条），
        直接取「第一条 ≥ since_us」的项会跳过它之前那批记录；取它前一条则一定覆盖
        （代价是多扫一个间隔内的记录）。

        Args:
            since_us: 发布时间下界（epoch 微秒）。
            candidates: 关心的 `log_id` 集合。

        Returns:
            set[str]: 命中的 `log_id`。
        """
        found: set[str] = set()
        if not candidates:
            return found
        started = False
        for segment in self._segments:
            if not started:
                if segment.max_published_us() < since_us:
                    continue
                anchor = _anchor_at_or_before(segment, since_us)
                started = True
            else:
                anchor = (0, 0)
            scan = segment.scan_tail(verify_crc=False, anchor=anchor)
            for _position, _relative, header in scan.entries:
                if header.log_id in candidates:
                    found.add(header.log_id)
            if found >= candidates:
                break
        return found

    def flush(self) -> None:
        """把当前写段刷盘、重置刷盘窗口并写恢复点（段轮转 / 优雅关闭用）。"""
        if self._segments:
            self._segments[-1].fsync()
        self._unflushed = 0
        self._last_fsync_ms = self._clock()
        if self._segments:
            self.persist_recovery_point()

    def _load(self) -> None:
        """枚举已有段并完成崩溃恢复（02-2 §6）：修残条 → 对账 → 扫描 → 截断取证 → 补索引。"""
        started = time.monotonic()
        bases = list_segment_bases(self._directory)
        if not bases:
            self._report = RecoveryReport()
            return
        self._segments = [self._make_segment(base) for base in bases]
        last = self._segments[-1]
        repair_bytes = last.repair_index_tail()
        readout = read_recovery_point(
            self._directory / RECOVERY_POINT_NAME,
            topic_id=self._topic_id,
            partition_no=self._partition_no,
        )
        point = readout.point
        corrupted = self._verify_stamps(point)
        scanned_before = last.size_bytes
        scan = last.scan_tail(verify_crc=True, anchor=self._choose_anchor(point, last))
        truncated = scan.complete_bytes < scanned_before
        isolated: Path | None = None
        if truncated:
            isolated = last.isolate_tail(keep_bytes=scan.complete_bytes, timestamp=_timestamp())
            last.prune_corrupt_tails(limit=CORRUPT_TAIL_KEEP)
            last.truncate(
                size=scan.complete_bytes,
                max_relative_offset=scan.from_rel + scan.count - 1,
            )
            _LOGGER.warning(
                "msg_tail_isolated",
                partition_path=str(self._directory),
                source=last.log_path.name,
                isolated=isolated.name,
                bytes=scanned_before - scan.complete_bytes,
                trailing_records=scan.trailing_records,
            )
        rebuilt = last.backfill_index(scan.entries)
        self._start = bases[0]
        self._leo = last.base_offset + scan.from_rel + scan.count
        point_leo = point.leo if point is not None else None
        self._report = RecoveryReport(
            segments=len(self._segments),
            start_offset=self._start,
            leo=self._leo,
            scanned_bytes=scanned_before,
            truncated_from_bytes=scanned_before,
            truncated_bytes=scan.complete_bytes,
            trailing_records=scan.trailing_records,
            index_rebuilt=rebuilt,
            index_repair_bytes=repair_bytes,
            corrupted_segments=corrupted,
            recovery_point=readout.reason,
            isolated=str(isolated) if isolated is not None else None,
            elapsed_ms=int((time.monotonic() - started) * 1_000),
            needs_alignment=truncated or (point_leo is not None and point_leo > self._leo),
        )
        fields = {
            "partition_path": str(self._directory),
            "segments": self._report.segments,
            "scanned_bytes": self._report.scanned_bytes,
            "truncated_bytes": self._report.truncated_bytes,
            "leo": self._leo,
            "trailing_records": self._report.trailing_records,
            "index_rebuilt": self._report.index_rebuilt,
            "index_repair_bytes": self._report.index_repair_bytes,
            "corrupted_segments": list(corrupted),
            "recovery_point": readout.reason,
            "elapsed_ms": self._report.elapsed_ms,
        }
        if truncated or corrupted:
            _LOGGER.warning("msg_partition_recovered", **fields)
        else:
            _LOGGER.info("msg_partition_recovered", **fields)

    def _choose_anchor(self, point: RecoveryPoint | None, last: SegmentFile) -> tuple[int, int] | None:
        """在「恢复点末段 size」与「最后索引项位置」之间取较大者作为扫描起点。

        Args:
            point: 恢复点（不可用为 None）。
            last: 末段。

        Returns:
            tuple[int, int] | None: `(字节偏移, 相对 offset)`；两锚点皆缺返回 None。
        """
        candidates: list[tuple[int, int]] = []
        index_anchor = last.index_anchor()
        if index_anchor is not None:
            candidates.append(index_anchor)
        if point is not None:
            stamp = next((item for item in point.segments if item.base == last.base_offset), None)
            # `stamp.size > 文件大小` 说明恢复点超前（先 fsync 后写恢复点本不该出现），视为不可信
            if stamp is not None and 0 < stamp.size <= last.size_bytes:
                candidates.append((stamp.size, stamp.last_offset + 1 - last.base_offset))
        if not candidates:
            return None
        return max(candidates, key=lambda item: item[0])

    def _verify_stamps(self, point: RecoveryPoint | None) -> tuple[int, ...]:
        """按恢复点段清单对账非末段（O(段数)，每段读一条记录）。

        Args:
            point: 恢复点（不可用为 None）。

        Returns:
            tuple[int, ...]: 对账失败的段 base 列表。
        """
        if point is None:
            return ()
        last_base = self._segments[-1].base_offset
        by_base = {segment.base_offset: segment for segment in self._segments}
        corrupted: list[int] = []
        for stamp in point.segments:
            segment = by_base.get(stamp.base)
            if segment is None or stamp.base == last_base:
                # 段已被保留期删除（恢复点落后）不是损坏；末段交给扫描，比 stamp 更权威
                continue
            mismatch = self._check_stamp(segment, stamp)
            if mismatch is None:
                continue
            reason, expected, actual = mismatch
            self._corrupted[stamp.base] = reason
            corrupted.append(stamp.base)
            _LOGGER.warning(
                "msg_segment_corrupted",
                partition_path=str(self._directory),
                base_offset=stamp.base,
                reason=reason,
                expected=expected,
                actual=actual,
            )
        return tuple(corrupted)

    def _check_stamp(self, segment: SegmentFile, stamp: SegmentStamp) -> tuple[str, int, int] | None:
        """校验一个非末段（size 对账 + 段尾记录 CRC）。

        Args:
            segment: 段。
            stamp: 恢复点里的快照。

        Returns:
            tuple[str, int, int] | None: `(原因, 期望值, 实际值)`；通过返回 None。
        """
        if segment.size_bytes != stamp.size:
            return "size_mismatch", stamp.size, segment.size_bytes
        if stamp.size == 0:
            return None
        matched, actual_crc = segment.verify_stamp(position=stamp.last_position, expected_crc=stamp.last_crc)
        return None if matched else ("crc_mismatch", stamp.last_crc, actual_crc)

    def recovery_point(self) -> RecoveryPoint:
        """按当前段状态构造恢复点（非末段用段关闭时固化的 stamp，末段现算）。

        Returns:
            RecoveryPoint: 恢复点。
        """
        stamps: list[SegmentStamp] = []
        last_index = len(self._segments) - 1
        for index, segment in enumerate(self._segments):
            cached = self._stamps.get(segment.base_offset)
            if cached is not None and index != last_index:
                stamps.append(cached)
                continue
            stamps.append(self._stamp_of(segment))
        return RecoveryPoint(
            topic_id=self._topic_id,
            partition_no=self._partition_no,
            leo=self._leo,
            segments=tuple(stamps),
            updated_at_us=int(time.time() * 1_000_000),
        )

    def persist_recovery_point(self) -> None:
        """写恢复点（fail-open：失败只告警，不阻断写入路径）。"""
        if not self._directory.is_dir():
            return
        try:
            write_recovery_point(self._directory / RECOVERY_POINT_NAME, self.recovery_point())
        except OSError as exc:
            _LOGGER.warning(
                "msg_recovery_point_failed",
                partition_path=str(self._directory),
                error=str(exc),
            )

    def _stamp_of(self, segment: SegmentFile) -> SegmentStamp:
        """算一个段的当前快照。

        Args:
            segment: 段。

        Returns:
            SegmentStamp: 快照。
        """
        record = segment.last_record_stamp()
        if record is None:
            return SegmentStamp(
                base=segment.base_offset,
                size=segment.size_bytes,
                last_offset=segment.base_offset - 1,
                last_position=0,
                last_crc=0,
            )
        position, relative, crc = record
        return SegmentStamp(
            base=segment.base_offset,
            size=segment.size_bytes,
            last_offset=segment.base_offset + relative,
            last_position=position,
            last_crc=crc,
        )

    def _ensure_directory(self) -> None:
        """首次写入前创建分区目录（幂等；读路径不创建目录）。"""
        if self._ensured:
            return
        self._directory.mkdir(parents=True, exist_ok=True)
        self._ensured = True

    def _make_segment(self, base_offset: int) -> SegmentFile:
        """构造一个段对象（不登记、不创建文件）。

        Args:
            base_offset: 段首 offset。

        Returns:
            SegmentFile: 段对象。
        """
        return SegmentFile(
            partition_dir=self._directory,
            base_offset=base_offset,
            index_interval=self._index_interval,
            time_interval_ms=self._time_index_interval_ms,
            probe=self._probe,
        )

    def _open_segment(self, *, base_offset: int) -> SegmentFile:
        """构造并登记一个新段（成为当前写段）。

        Args:
            base_offset: 段首 offset。

        Returns:
            SegmentFile: 新段。
        """
        segment = self._make_segment(base_offset)
        self._ensure_directory()
        # 建段即落一个 0 字节文件：删光后重启靠「段名里的 base offset」推导 LEO，
        # 不落文件就会得到空分区、offset 从 0 重排。
        segment.log_path.touch(exist_ok=True)
        self._segments.append(segment)
        return segment

    def _segment_by_path(self, path: Path) -> SegmentFile | None:
        """按 `.log` 路径反查段对象。

        Args:
            path: 段文件路径。

        Returns:
            SegmentFile | None: 段对象；已被删除返回 None。
        """
        for segment in self._segments:
            if segment.log_path == path:
                return segment
        return None

    def _needs_roll(self, *, record_bytes: int, published_at_us: int) -> bool:
        """判定本条写入前是否需要先滚动到新段。

        Args:
            record_bytes: 本条记录字节数。
            published_at_us: 本条发布时间（epoch 微秒）。

        Returns:
            bool: True 表示需要滚动。
        """
        if not self._segments:
            return False
        current = self._segments[-1]
        if current.size_bytes == 0:
            return False
        if current.size_bytes + record_bytes > self._segment_max_bytes:
            return True
        first_us = _first_published_us(current)
        return first_us > 0 and published_at_us - first_us >= self._segment_roll_ms * 1_000

    def _roll(self, *, new_base: int) -> None:
        """关闭当前段并开新段（base = 新 offset）。

        Args:
            new_base: 新段首 offset。
        """
        current = self._segments[-1]
        # 固化关闭段的 stamp：它此后不再变化，写恢复点不必每次重算（O(段数) → O(1)）
        self._stamps[current.base_offset] = self._stamp_of(current)
        current.fsync()
        current.close()
        self._open_segment(base_offset=new_base)
        self.persist_recovery_point()
        _LOGGER.info(
            "msg_segment_roll",
            partition_path=str(self._directory),
            closed_base=current.base_offset,
            new_base=new_base,
        )

    def _maybe_fsync(self) -> None:
        """按 fsync 档位决定是否刷盘（批次内合并；`each` 每条刷）。"""
        if self._fsync_mode == FSYNC_MODE_OS or not self._segments:
            return
        segment = self._segments[-1]
        if self._fsync_mode == FSYNC_MODE_EACH:
            segment.fsync()
            self._unflushed = 0
            self._last_fsync_ms = self._clock()
            self.persist_recovery_point()
            return
        now = self._clock()
        reached_batch = self._fsync_mode == FSYNC_MODE_BATCH and self._unflushed >= self._fsync_batch_records
        reached_window = now - self._last_fsync_ms >= self._fsync_interval_ms
        if not reached_batch and not reached_window:
            return
        segment.fsync()
        self._unflushed = 0
        self._last_fsync_ms = now
        self.persist_recovery_point()
        _LOGGER.info(
            "msg_fsync",
            partition_path=str(self._directory),
            mode=self._fsync_mode,
        )


def _first_published_us(segment: SegmentFile) -> int:
    """取段内首条记录的发布时间（段滚动的时间判据）。

    Args:
        segment: 段。

    Returns:
        int: epoch 微秒；无记录返回 0。
    """
    segment.ensure_indexes()
    entries = segment.time_index_entries()
    return entries[0][0] if entries else 0


def _anchor_at_or_before(segment: SegmentFile, since_us: int) -> tuple[int, int]:
    """取段内「最后一条时间索引项 ≤ `since_us`」的扫描锚点。

    Args:
        segment: 段。
        since_us: 发布时间下界（epoch 微秒）。

    Returns:
        tuple[int, int]: `(字节偏移, 相对 offset)`；没有更早的索引项时从段首扫。
    """
    chosen: tuple[int, int] | None = None
    for timestamp, relative in segment.time_index_entries():
        if timestamp > since_us:
            break
        chosen = (timestamp, relative)
    if chosen is None:
        return 0, 0
    position, relative = segment.position_of(offset=segment.base_offset + chosen[1])
    return position, relative


def _to_message(parsed: ParsedRecord, *, offset: int) -> StoredMessage:
    """解析结果 → 存储记录（字段口径唯一出处）。

    Args:
        parsed: 解析后的记录。
        offset: 该记录的分区内序号（位置属性，不在头里）。

    Returns:
        StoredMessage: 存储记录。
    """
    header = parsed.header
    return StoredMessage(
        message_id=header.message_id,
        log_id=header.log_id,
        topic_id=header.topic_id,
        partition_no=header.partition_no,
        offset=offset,
        key=header.key,
        payload=parsed.payload,
        headers=ConcurrentStableDict(dict(header.headers)),
        trace=header.trace,
        published_at=from_epoch_us(header.published_at_us),
        content_type=header.content_type or DEFAULT_CONTENT_TYPE,
        compression=header.compression or COMPRESSION_NONE,
        uncompressed_len=header.uncompressed_len,
        epoch=header.epoch,
    )


class FileMessageStore(MessageStore):
    """文件段消息存储（生产实现；端口签名与内存实现完全一致）。"""

    def __init__(
        self,
        *,
        directory: PartitionDirectory,
        fsync_mode: str | None = None,
        fsync_interval_ms: int | None = None,
        segment_max_bytes: int = SEGMENT_MAX_BYTES,
        segment_roll_ms: int = SEGMENT_ROLL_MS,
        index_interval: int = INDEX_INTERVAL_BYTES,
        time_index_interval_ms: int = TIME_INDEX_INTERVAL_MS,
        log_id_limit: int = LOG_ID_INDEX_LIMIT,
        clock: Callable[[], float] = _monotonic_ms,
        realigner: OffsetRealigner | None = None,
        probe: RecoveryProbe | None = None,
    ) -> None:
        """初始化（不创建目录、不扫描已有分区——首个访问触发懒加载）。

        Args:
            directory: 主题目录登记表（分区路径的唯一来源）。
            fsync_mode: fsync 档位（None 取环境变量，缺省按批）。
            fsync_interval_ms: 刷盘时间窗（None 取环境变量，缺省模块常量）。
            segment_max_bytes: 段滚动字节阈值。
            segment_roll_ms: 段滚动时长（毫秒）。
            index_interval: 稀疏 offset 索引间隔（字节）。
            time_index_interval_ms: 时间索引间隔（毫秒）。
            log_id_limit: `log_id` 索引的 LRU 上限。
            clock: 单调毫秒时钟（用例注入）。
            realigner: 位点对齐端口（缺省不注入，恢复时只告警跳过）。
            probe: 恢复读取量探针（缺省不统计）。

        Raises:
            ValueError: fsync 档位或时间窗取值非法（启动即失败，不静默降级）。
        """
        mode = fsync_mode if fsync_mode is not None else os.environ.get(FSYNC_MODE_ENV, DEFAULT_FSYNC_MODE)
        if mode not in _FSYNC_MODES:
            raise ValueError(f"MSG_FSYNC_MODE 取值非法：{mode!r}（可选 {sorted(_FSYNC_MODES)}）")
        raw_interval = (
            fsync_interval_ms
            if fsync_interval_ms is not None
            else int(os.environ.get(FSYNC_INTERVAL_MS_ENV, FSYNC_INTERVAL_MS))
        )
        if raw_interval <= 0:
            raise ValueError(f"MSG_FSYNC_INTERVAL_MS 须为正数：{raw_interval}")
        self._directory = directory
        self._fsync_mode = mode
        self._fsync_interval_ms = raw_interval
        self._segment_max_bytes = segment_max_bytes
        self._segment_roll_ms = segment_roll_ms
        self._index_interval = index_interval
        self._time_index_interval_ms = time_index_interval_ms
        self._log_id_limit = log_id_limit
        self._clock = clock
        self._partitions: dict[tuple[int, int], _PartitionLog] = {}
        self._log_ids: OrderedDict[str, tuple[int, int, int]] = OrderedDict()
        self._realigner = realigner
        self._probe = probe
        self._pending_alignment: dict[tuple[int, int], int] = {}
        # 02-6：进程级恢复台账（就绪检查项的只读数据源；分区懒加载时逐条写入）。
        self._ledger = RecoveryLedger()

    async def append(
        self,
        *,
        topic_id: int,
        partition_no: int,
        message_id: str,
        log_id: str,
        key: str | None,
        payload: bytes,
        headers: ConcurrentStableDict[str, str],
        trace: TraceContext,
        published_at: datetime,
        content_type: str = DEFAULT_CONTENT_TYPE,
        compression: str = COMPRESSION_NONE,
        uncompressed_len: int | None = None,
        epoch: int = 0,
    ) -> StoredMessage:
        """追加一条消息并分配 offset。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。
            message_id: 消息身份（调用方生成）。
            log_id: 记录身份。
            key: 分区键（可空）。
            payload: 业务字节。
            headers: 结构化头。
            trace: 链路字段（06-4；四个字段一物一传）。
            published_at: 发布时间（UTC）。
            content_type: 内容类型。
            compression: 压缩算法。
            uncompressed_len: 解压后字节数（可空）。
            epoch: leader 任期（02-4；follower 复制落盘时沿用 leader 值）。

        Returns:
            StoredMessage: 含分配到的 offset 的完整记录。

        Raises:
            PartitionNotBoundError: 主题目录未登记。
        """
        location = self._require_location(topic_id)
        partition = await self._open(topic_id=topic_id, partition_no=partition_no)
        async with partition.lock:
            message = partition.append(
                topic_id=int(topic_id),
                partition_no=int(partition_no),
                tenant_id=location.tenant_id,
                message_id=message_id,
                log_id=log_id,
                key=key,
                payload=payload,
                headers=headers,
                trace=trace,
                published_at=published_at,
                content_type=content_type,
                compression=compression,
                uncompressed_len=uncompressed_len,
                epoch=epoch,
            )
        self._remember_log_id(
            log_id=log_id, topic_id=int(topic_id), partition_no=int(partition_no), offset=message.offset
        )
        return message

    async def fetch(
        self, *, topic_id: int, partition_no: int, from_offset: int, max_messages: int
    ) -> ConcurrentStableList[StoredMessage]:
        """从 `from_offset` 起取至多 `max_messages` 条。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。
            from_offset: 起始位点（含；早于起点按起点）。
            max_messages: 本次上限。

        Returns:
            ConcurrentStableList[StoredMessage]: 按 offset 升序的记录。

        Raises:
            PartitionNotBoundError: 主题目录未登记。
            CorruptedRecordError: 记录损坏。
        """
        if self._locate(topic_id) is None:
            return ConcurrentStableList()
        partition = await self._open(topic_id=topic_id, partition_no=partition_no)
        return partition.read(from_offset=from_offset, max_messages=max_messages)

    async def leo(self, *, topic_id: int, partition_no: int) -> int:
        """取分区日志末端位点。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。

        Returns:
            int: LEO（空分区为 0）。

        Raises:
            PartitionNotBoundError: 主题目录未登记。
        """
        if self._locate(topic_id) is None:
            return 0
        partition = await self._open(topic_id=topic_id, partition_no=partition_no)
        return partition.leo

    async def start_offset(self, *, topic_id: int, partition_no: int) -> int:
        """取分区日志起点（保留期清理后大于 0）。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。

        Returns:
            int: 起点位点。

        Raises:
            PartitionNotBoundError: 主题目录未登记。
        """
        if self._locate(topic_id) is None:
            return 0
        partition = await self._open(topic_id=topic_id, partition_no=partition_no)
        return partition.start

    async def segments(self, *, topic_id: int, partition_no: int) -> ConcurrentStableList[SegmentMeta]:
        """按 offset 升序取段元数据（空段不暴露）。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。

        Returns:
            ConcurrentStableList[SegmentMeta]: 段元数据。

        Raises:
            PartitionNotBoundError: 主题目录未登记。
        """
        if self._locate(topic_id) is None:
            return ConcurrentStableList()
        partition = await self._open(topic_id=topic_id, partition_no=partition_no)
        return partition.segments()

    async def truncate_before(self, *, topic_id: int, partition_no: int, offset: int) -> int:
        """把可读区间起点前移到 `offset`（向下取整到段边界；整段删除）。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。
            offset: 新的起点。

        Returns:
            int: 前移后的起点。

        Raises:
            PartitionNotBoundError: 主题目录未登记。
            ParamError: `offset > leo`（10001）。
        """
        self._require_location(topic_id)
        partition = await self._open(topic_id=topic_id, partition_no=partition_no)
        async with partition.lock:
            return partition.truncate_before(offset=offset)

    async def truncate_to(self, *, topic_id: int, partition_no: int, offset: int) -> int:
        """截掉 `offset` 及之后的记录（02-5 切换截断）。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。
            offset: 保留到的位置（不含）。

        Returns:
            int: 截断后的 LEO。

        Raises:
            PartitionNotBoundError: 主题目录未登记。
            ParamError: `offset < start`（10001）。
        """
        self._require_location(topic_id)
        partition = await self._open(topic_id=topic_id, partition_no=partition_no)
        async with partition.lock:
            target = partition.truncate_to(offset=offset)
        key = (int(topic_id), int(partition_no))
        self._pending_alignment.pop(key, None)
        for log_id, (owner, part, cached_offset) in list(self._log_ids.items()):
            if owner == int(topic_id) and part == int(partition_no) and cached_offset >= target:
                self._log_ids.pop(log_id, None)
        return target

    async def locate(self, *, topic_id: int, partition_no: int, offset: int) -> FileSlice | None:
        """取一条记录的坐标（段文件 + 偏移 + 长度；不读载荷）。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。
            offset: 分区内序号。

        Returns:
            FileSlice | None: 坐标；越界返回 None。

        Raises:
            PartitionNotBoundError: 主题目录未登记。
        """
        if self._locate(topic_id) is None:
            return None
        partition = await self._open(topic_id=topic_id, partition_no=partition_no)
        return partition.locate(offset=offset)

    async def read_range(
        self, *, topic_id: int, partition_no: int, from_offset: int, max_messages: int
    ) -> ConcurrentStableList[FileSlice]:
        """取一批记录的坐标（供传输层批量 `sendfile`）。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。
            from_offset: 起始位点。
            max_messages: 本次上限。

        Returns:
            ConcurrentStableList[FileSlice]: 坐标列表（按 offset 升序）。

        Raises:
            PartitionNotBoundError: 主题目录未登记。
        """
        if self._locate(topic_id) is None:
            return ConcurrentStableList()
        partition = await self._open(topic_id=topic_id, partition_no=partition_no)
        return partition.read_range(from_offset=from_offset, max_messages=max_messages)

    async def read_bytes(self, *, topic_id: int, partition_no: int, offset: int) -> StoredMessage | None:
        """读一条记录并解码（校验 CRC）。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。
            offset: 分区内序号。

        Returns:
            StoredMessage | None: 记录；越界返回 None。

        Raises:
            PartitionNotBoundError: 主题目录未登记。
            CorruptedRecordError: 记录损坏。
        """
        if self._locate(topic_id) is None:
            return None
        partition = await self._open(topic_id=topic_id, partition_no=partition_no)
        return partition.read_bytes(offset=offset)

    async def _resolve_log_id(self, *, log_id: str) -> tuple[int, int, int] | None:
        """把记录身份解析成 `(topic_id, partition_no, offset)`（02-8-4：LRU → 分区索引 → 磁盘索引）。

        查找顺序：进程内 LRU → 已加载分区的持久化索引 → 已绑定主题在磁盘上的分区索引文件。
        哈希只筛候选，命中后**回读原记录比对完整 `log_id`**，防哈希碰撞返回错误记录。

        `locate_by_log_id()`（磁盘坐标）与 `find_by_log_id()`（只读头视图）共用同一份解析结果，
        避免两处索引口径漂移。

        Args:
            log_id: 记录标识。

        Returns:
            tuple[int, int, int] | None: 坐标三元组；不存在返回 None。
        """
        cached = self._log_ids.get(log_id)
        if cached is not None:
            self._log_ids.move_to_end(log_id)
            return cached
        for (topic_id, partition_no), partition in self._partitions.items():
            resolved = self._verify_index_candidate(partition=partition, log_id=log_id)
            if resolved is not None:
                self._remember_log_id(log_id=log_id, topic_id=topic_id, partition_no=partition_no, offset=resolved)
                return (topic_id, partition_no, resolved)
        found = await self._resolve_from_disk_index(log_id=log_id)
        if found is None:
            return None
        topic_id, partition_no, offset = found
        self._remember_log_id(log_id=log_id, topic_id=topic_id, partition_no=partition_no, offset=offset)
        return found

    def _verify_index_candidate(self, *, partition: _PartitionLog, log_id: str) -> int | None:
        """用分区索引的候选 offset 回读记录，确认 `log_id` 一致。

        Args:
            partition: 分区日志。
            log_id: 记录标识。

        Returns:
            int | None: 校验通过的 offset；否则 None。
        """
        candidate = partition.log_index_lookup(log_id=log_id)
        candidates: ConcurrentStableList[int] = (
            ConcurrentStableList() if candidate is None else ConcurrentStableList([candidate])
        )
        for offset in candidates:
            if self._record_matches(partition=partition, offset=offset, log_id=log_id):
                return offset
        if candidate is None:
            return None
        # 哈希碰撞：按文件里的全部同哈希候选再验一遍
        for offset in lookup_log_index_file(partition.log_index_path(), log_id=log_id):
            if offset not in candidates and self._record_matches(partition=partition, offset=offset, log_id=log_id):
                return offset
        return None

    @staticmethod
    def _record_matches(*, partition: _PartitionLog, offset: int, log_id: str) -> bool:
        """回读一条记录并比对 `log_id`。

        Args:
            partition: 分区日志。
            offset: 分区内序号。
            log_id: 记录标识。

        Returns:
            bool: 一致返回 True。
        """
        try:
            messages = partition.read(from_offset=offset, max_messages=1)
        except Exception:  # pragma: no cover - 损坏记录按未命中处理
            return False
        return bool(messages) and messages[0].offset == offset and messages[0].log_id == log_id

    async def _resolve_from_disk_index(self, *, log_id: str) -> tuple[int, int, int] | None:
        """扫描已绑定主题在磁盘上的分区索引文件（未加载分区也能定位）。

        Args:
            log_id: 记录标识。

        Returns:
            tuple[int, int, int] | None: 坐标；未命中返回 None。
        """
        scanned = 0
        for location in self._directory.all_locations():
            topic_dir = self._directory.path_of(location, 0).parent
            if not topic_dir.is_dir():
                continue
            for entry in sorted(topic_dir.iterdir()):
                if not entry.is_dir() or not entry.name.isdigit():
                    continue
                partition_no = int(entry.name)
                if (location.topic_id, partition_no) in self._partitions:
                    continue
                index_path = entry / LOG_INDEX_NAME
                if not index_path.exists():
                    continue
                scanned += 1
                if scanned > LOG_ID_INDEX_SCAN_LIMIT:
                    _LOGGER.warning("msg_log_index_scan_limit", limit=LOG_ID_INDEX_SCAN_LIMIT)
                    return None
                for offset in lookup_log_index_file(index_path, log_id=log_id):
                    partition = await self._open(topic_id=location.topic_id, partition_no=partition_no)
                    if self._record_matches(partition=partition, offset=offset, log_id=log_id):
                        return (location.topic_id, partition_no, offset)
        return None

    async def locate_by_log_id(self, *, log_id: str) -> FileSlice | None:
        """按记录身份定位（先查 LRU，未命中扫已加载分区的段头）。

        **边界**：只扫已加载的分区——未加载分区需先访问一次；本方法是排障 / 审计辅助，
        不是热路径（02-7 的恢复期去重走「扫 `.log` 尾部」的自有原语，不依赖本方法）。

        Args:
            log_id: 记录标识。

        Returns:
            FileSlice | None: 坐标；不存在返回 None。
        """
        resolved = await self._resolve_log_id(log_id=log_id)
        if resolved is None:
            return None
        topic_id, partition_no, offset = resolved
        partition = self._partitions.get((topic_id, partition_no))
        return None if partition is None else partition.locate(offset=offset)

    async def delete(self, *, topic_id: int, partition_no: int, log_id: str) -> bool:
        """追加墓碑标记（05-4-1；逻辑删除，位点与 LEO 不变）。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。
            log_id: 目标记录标识。

        Returns:
            bool: 首次标记 True；目标不存在 / 已删除 / 跨分区 False。
        """
        location = self._locate(topic_id)
        if location is None:
            return False
        resolved = await self._resolve_log_id(log_id=log_id)
        if resolved is None:
            return False
        resolved_topic, resolved_partition, offset = resolved
        if resolved_topic != int(topic_id) or resolved_partition != int(partition_no):
            return False
        partition = await self._open(topic_id=topic_id, partition_no=partition_no)
        async with partition.lock:
            target = partition.read_bytes(offset=offset)
            if target is None or target.log_id != log_id:
                return False
            partition.append(
                topic_id=int(topic_id),
                partition_no=int(partition_no),
                tenant_id=location.tenant_id,
                message_id=f"tombstone-{log_id}",
                log_id=f"tombstone-{uuid4().hex}",
                key=None,
                payload=b"",
                headers=ConcurrentStableDict(),
                trace=TraceContext(),
                published_at=datetime.now(UTC),
                content_type="none",
                compression=COMPRESSION_NONE,
                uncompressed_len=None,
                delete_target=log_id,
            )
        return True

    async def find_by_log_id(self, *, log_id: str) -> MessageHeaderView | None:
        """按记录身份取只读头视图（端口方法；命中即解码该条，不回退全量扫描）。

        Args:
            log_id: 记录标识。

        Returns:
            MessageHeaderView | None: 头视图；不存在或分区已卸载返回 None。
        """
        resolved = await self._resolve_log_id(log_id=log_id)
        if resolved is None:
            return None
        topic_id, partition_no, offset = resolved
        message = await self.read_bytes(topic_id=topic_id, partition_no=partition_no, offset=offset)
        return None if message is None else message_header_view(message)

    async def scan_headers(
        self, *, topic_id: int, partition_no: int, from_offset: int, max_messages: int
    ) -> ConcurrentStableList[MessageHeaderView]:
        """取一段只读头视图（复用 `fetch` 的可读区间裁剪与 CRC 校验口径）。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。
            from_offset: 起始位点（含）。
            max_messages: 本次上限。

        Returns:
            ConcurrentStableList[MessageHeaderView]: 按 offset 升序的头视图（不含正文）。
        """
        messages = await self.fetch(
            topic_id=topic_id,
            partition_no=partition_no,
            from_offset=from_offset,
            max_messages=max_messages,
        )
        return ConcurrentStableList(message_header_view(message) for message in messages)

    def _locate(self, topic_id: int) -> TopicLocation | None:
        """取主题目录身份（未登记返回 None；读路径用它判空，不建目录）。

        Args:
            topic_id: 主题主键。

        Returns:
            TopicLocation | None: 目录身份；未登记且盘上无目录为 None。
        """
        location = self._directory.locate(topic_id)
        if location is None:
            location = self._directory.recover(topic_id=int(topic_id))
        return location

    def _require_location(self, topic_id: int) -> TopicLocation:
        """取主题目录身份（未登记即抛错）。

        Args:
            topic_id: 主题主键。

        Returns:
            TopicLocation: 目录身份。

        Raises:
            PartitionNotBoundError: 未登记。
        """
        location = self._locate(topic_id)
        if location is None:
            raise PartitionNotBoundError(f"主题目录未登记：topic_id={topic_id}（须先经 TopicRouter.resolve 解析）")
        return location

    def recovery_ledger(self) -> RecoveryLedger:
        """取恢复台账（02-6 就绪检查项；只覆盖**已打开过**的分区，启动期为空）。

        Returns:
            RecoveryLedger: 进程级台账（调用方只读）。
        """
        return self._ledger

    def _partition(self, *, topic_id: int, partition_no: int) -> _PartitionLog:
        """取分区日志（懒加载；目录不存在即视为空分区）。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。

        Returns:
            _PartitionLog: 分区日志。

        Raises:
            PartitionNotBoundError: 主题目录未登记。
        """
        key = (int(topic_id), int(partition_no))
        existing = self._partitions.get(key)
        if existing is not None:
            return existing
        directory = self._directory.partition_path(topic_id=topic_id, partition_no=partition_no)
        partition = _PartitionLog(
            directory=directory,
            topic_id=key[0],
            partition_no=key[1],
            fsync_mode=self._fsync_mode,
            fsync_interval_ms=self._fsync_interval_ms,
            fsync_batch_records=FSYNC_BATCH_RECORDS,
            segment_max_bytes=self._segment_max_bytes,
            segment_roll_ms=self._segment_roll_ms,
            index_interval=self._index_interval,
            time_index_interval_ms=self._time_index_interval_ms,
            clock=self._clock,
            probe=self._probe,
        )
        self._partitions[key] = partition
        self._ledger.record(topic_id=key[0], partition_no=key[1], report=partition.report)
        if partition.needs_alignment:
            self._pending_alignment[key] = partition.leo
        return partition

    async def _open(self, *, topic_id: int, partition_no: int) -> _PartitionLog:
        """取分区日志并在首次访问时完成位点对齐。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。

        Returns:
            _PartitionLog: 分区日志。

        Raises:
            PartitionNotBoundError: 主题目录未登记。
        """
        partition = self._partition(topic_id=topic_id, partition_no=partition_no)
        await self._align(topic_id=topic_id, partition_no=partition_no)
        return partition

    async def _align(self, *, topic_id: int, partition_no: int) -> None:
        """该分区首次访问前完成位点对齐（每分区只做一次）。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。
        """
        key = (int(topic_id), int(partition_no))
        ceiling = self._pending_alignment.pop(key, None)
        if ceiling is None:
            return
        if self._realigner is None:
            # 装配缺陷要可见，但不该让服务写不进去（fail-open）
            _LOGGER.warning(
                "msg_offset_alignment_skipped",
                topic_id=key[0],
                partition_no=key[1],
                leo=ceiling,
            )
            return
        adjustments = await self._realigner.realign_partition(topic_id=key[0], partition_no=key[1], ceiling=ceiling)
        for item in adjustments:
            _LOGGER.warning(
                "msg_offset_rollback",
                group=item.group,
                topic_id=item.topic_id,
                partition_no=item.partition_no,
                previous=item.previous,
                current=item.current,
                leo=ceiling,
            )

    async def log_ids_since(self, *, topic_id: int, partition_no: int, since_us: int, candidates: set[str]) -> set[str]:
        """取该分区日志里发布时间不早于 `since_us` 且命中候选集的 `log_id`（02-7 恢复期去重）。

        **非端口方法**：`MessageStore` 端口只服务数据面读写，去重是恢复期的内部动作；
        延迟区通过 `DelayDedupeSource` 协议结构性地依赖本方法（端口签名不变）。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。
            since_us: 发布时间下界（epoch 微秒）。
            candidates: 关心的 `log_id` 集合。

        Returns:
            set[str]: 命中的 `log_id`。

        Raises:
            PartitionNotBoundError: 主题目录未登记。
        """
        self._require_location(topic_id)
        partition = await self._open(topic_id=topic_id, partition_no=partition_no)
        return partition.log_ids_since(since_us=since_us, candidates=candidates)

    async def sync(self, *, topic_id: int, partition_no: int) -> None:
        """把该分区日志刷到磁盘并写恢复点（02-7 转投的耐久点，与 fsync 档位无关）。

        **为什么必须显式刷**：转投的偏序是「日志已落盘 → 才清 `.delay`」。`os` 档下 `append` 只进
        页缓存，此时清掉延迟记录、崩溃就会同时丢掉两份（既无日志也无延迟记录）。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。

        Raises:
            PartitionNotBoundError: 主题目录未登记。
        """
        self._require_location(topic_id)
        partition = await self._open(topic_id=topic_id, partition_no=partition_no)
        async with partition.lock:
            partition.flush()

    async def corrupted_segments(self, *, topic_id: int, partition_no: int) -> tuple[int, ...]:
        """取该分区非末段对账失败的段 base（排障 / 指标出口；只读）。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。

        Returns:
            tuple[int, ...]: 段 base 升序。

        Raises:
            PartitionNotBoundError: 主题目录未登记。
        """
        if self._locate(topic_id) is None:
            return ()
        partition = self._partition(topic_id=topic_id, partition_no=partition_no)
        return partition.corrupted_segments()

    async def aclose(self) -> None:
        """优雅关闭：逐分区刷盘、写恢复点、关句柄（幂等）。"""
        closed = len(self._partitions)
        for partition in list(self._partitions.values()):
            async with partition.lock:
                partition.flush()
                partition.close()
        self._partitions.clear()
        self._pending_alignment.clear()
        _LOGGER.info("msg_store_closed", partitions=closed)

    async def close_tenant(self, *, tenant_id: int) -> int:
        """关闭某租户的全部分区句柄并移除进程内缓存（不删数据文件）。

        租户删除清理在删目录前必须先关句柄，否则 Windows 下会因打开句柄删不掉。数据文件由清扫
        服务按主题删除；本方法只释放句柄与 `log_id` LRU 缓存。

        Args:
            tenant_id: 租户主键。

        Returns:
            int: 关闭的分区数。
        """
        target = int(tenant_id)
        closed = 0
        topic_ids: ConcurrentStableSet[int] = ConcurrentStableSet()
        for key, partition in list(self._partitions.items()):
            located = self._directory.locate(key[0])
            if located is None or int(located.tenant_id) != target:
                continue
            async with partition.lock:
                partition.flush()
                partition.close()
            self._partitions.pop(key, None)
            self._pending_alignment.pop(key, None)
            topic_ids.add(int(key[0]))
            closed += 1
        for log_id, (topic_id, _partition, _offset) in list(self._log_ids.items()):
            if topic_id in topic_ids:
                self._log_ids.pop(log_id, None)
        return closed

    async def drop_topic(self, *, topic_id: int) -> int:
        """删除该主题的全部段文件与目录（06-2 删主题清理用；幂等）。

        **顺序**：先关句柄（否则 Windows 下删不掉打开中的文件）→ 清 `log_id` LRU → 删主题目录 →
        移除目录登记。目录不存在时只清内存态并返回 0，保证重复执行无副作用。

        Args:
            topic_id: 主题主键。

        Returns:
            int: 关闭并删除的分区数。
        """
        target = int(topic_id)
        closed = 0
        for key, partition in list(self._partitions.items()):
            if key[0] != target:
                continue
            async with partition.lock:
                partition.flush()
                partition.close()
            self._partitions.pop(key, None)
            self._pending_alignment.pop(key, None)
            closed += 1
        for log_id, (owner, _partition_no, _offset) in list(self._log_ids.items()):
            if owner == target:
                self._log_ids.pop(log_id, None)
        location = self._directory.locate(target)
        if location is None:
            location = self._directory.recover(topic_id=target)
        if location is not None:
            topic_dir = self._directory.path_of(location, 0).parent
            if topic_dir.is_dir():
                shutil.rmtree(topic_dir, ignore_errors=True)
        self._directory.forget(target)
        return closed

    def _remember_log_id(self, *, log_id: str, topic_id: int, partition_no: int, offset: int) -> None:
        """登记 `log_id` → 位置的 LRU 条目（超上限淘汰最久未用）。

        Args:
            log_id: 记录标识。
            topic_id: 主题主键。
            partition_no: 分区号。
            offset: 分区内序号。
        """
        self._log_ids[log_id] = (topic_id, partition_no, offset)
        self._log_ids.move_to_end(log_id)
        while len(self._log_ids) > self._log_id_limit:
            self._log_ids.popitem(last=False)
