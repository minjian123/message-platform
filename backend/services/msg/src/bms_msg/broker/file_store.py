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
import os
import time
from collections import OrderedDict
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Final

from bms_core.core.concurrent import ConcurrentStableDict, ConcurrentStableList
from bms_core.core.exceptions import ParamError
from bms_core.core.logging import get_logger
from bms_core.core.objects import BaseValueObject

from bms_msg.broker.log import MessageStore, SegmentMeta, StoredMessage
from bms_msg.broker.record import (
    ParsedRecord,
    RecordHeader,
    encode_record,
    from_epoch_us,
    to_epoch_us,
)
from bms_msg.broker.segment import SegmentFile, list_segment_bases
from bms_msg.core.config import (
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
    SEGMENT_MAX_BYTES,
    SEGMENT_ROLL_MS,
    TIME_INDEX_INTERVAL_MS,
)
from bms_msg.protocol.payload import COMPRESSION_NONE, DEFAULT_CONTENT_TYPE

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


class PartitionNotBoundError(RuntimeError):
    """分区目录未登记（`topic_id` 未经 `TopicRouter.resolve()` 解析过）。

    程序性错误：属装配 / 接线缺陷，不是客户端可纠正的输入错误，按 500 暴露。
    """


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
        fsync_mode: str,
        fsync_interval_ms: int,
        fsync_batch_records: int,
        segment_max_bytes: int,
        segment_roll_ms: int,
        index_interval: int,
        time_index_interval_ms: int,
        clock: Callable[[], float],
    ) -> None:
        """初始化并加载已有段（不做任何写入）。

        Args:
            directory: 分区目录。
            fsync_mode: fsync 档位（已校验）。
            fsync_interval_ms: 刷盘时间窗（毫秒）。
            fsync_batch_records: 按批档的条数阈值。
            segment_max_bytes: 段滚动字节阈值。
            segment_roll_ms: 段滚动时长（毫秒）。
            index_interval: 稀疏 offset 索引间隔（字节）。
            time_index_interval_ms: 时间索引间隔（毫秒）。
            clock: 单调毫秒时钟（用例注入以确定性驱动刷盘窗口）。
        """
        self._directory = directory
        self._fsync_mode = fsync_mode
        self._fsync_interval_ms = fsync_interval_ms
        self._fsync_batch_records = fsync_batch_records
        self._segment_max_bytes = segment_max_bytes
        self._segment_roll_ms = segment_roll_ms
        self._index_interval = index_interval
        self._time_index_interval_ms = time_index_interval_ms
        self._clock = clock
        self._segments: list[SegmentFile] = []
        self._ensured = False
        self._leo = 0
        self._start = 0
        self._unflushed = 0
        self._last_fsync_ms = clock()
        self.lock = asyncio.Lock()
        self._load()

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
        trace_id: str | None,
        published_at: datetime,
        content_type: str,
        compression: str,
        uncompressed_len: int | None,
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
            trace_id: 链路标识（可空）。
            published_at: 发布时间（必须带时区）。
            content_type: 内容类型。
            compression: 压缩算法。
            uncompressed_len: 解压后字节数（可空）。

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
            trace_id=trace_id,
            published_at_us=published_at_us,
            headers=tuple((str(name), str(value)) for name, value in headers.items()),
            content_type=content_type,
            compression=compression,
            uncompressed_len=uncompressed_len,
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
        return StoredMessage(
            message_id=message_id,
            log_id=log_id,
            topic_id=topic_id,
            partition_no=partition_no,
            offset=offset,
            key=key,
            payload=payload,
            headers=headers,
            trace_id=trace_id,
            published_at=published_at,
            content_type=content_type,
            compression=compression,
            uncompressed_len=uncompressed_len,
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
            segment.delete()
        self._segments = kept
        self._start = target
        if not kept:
            self._open_segment(base_offset=target)
        _LOGGER.info(
            "msg_segment_deleted",
            partition_path=str(self._directory),
            base_offset=target,
            files=len(removed) * 3,
            deleted_bytes=deleted_bytes,
        )
        return target

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

    def flush(self) -> None:
        """把当前写段刷盘并重置刷盘窗口（段轮转 / 优雅关闭用）。"""
        if self._segments:
            self._segments[-1].fsync()
        self._unflushed = 0
        self._last_fsync_ms = self._clock()

    def _load(self) -> None:
        """枚举已有段并推导 LEO 与起点（结构级恢复只扫末段尾部）。"""
        bases = list_segment_bases(self._directory)
        if not bases:
            return
        self._segments = [self._make_segment(base) for base in bases]
        last = self._segments[-1]
        scanned_bytes = last.size_bytes
        scan = last.scan_tail()
        if scan.complete_bytes < scanned_bytes:
            complete_relative = scan.from_rel + scan.count - 1
            last.truncate(size=scan.complete_bytes, max_relative_offset=max(complete_relative, -1))
            _LOGGER.warning(
                "msg_partition_recovered",
                partition_path=str(self._directory),
                scanned_bytes=scanned_bytes,
                truncated_bytes=scan.complete_bytes,
                leo=last.base_offset + scan.count + scan.from_rel,
            )
        self._start = bases[0]
        self._leo = last.base_offset + scan.from_rel + scan.count
        _LOGGER.info(
            "msg_partition_opened",
            partition_path=str(self._directory),
            segments=len(self._segments),
            leo=self._leo,
            start_offset=self._start,
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
        current.fsync()
        current.close()
        self._open_segment(base_offset=new_base)
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
            return
        now = self._clock()
        reached_batch = self._fsync_mode == FSYNC_MODE_BATCH and self._unflushed >= self._fsync_batch_records
        reached_window = now - self._last_fsync_ms >= self._fsync_interval_ms
        if not reached_batch and not reached_window:
            return
        segment.fsync()
        self._unflushed = 0
        self._last_fsync_ms = now
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
        trace_id=header.trace_id,
        published_at=from_epoch_us(header.published_at_us),
        content_type=header.content_type or DEFAULT_CONTENT_TYPE,
        compression=header.compression or COMPRESSION_NONE,
        uncompressed_len=header.uncompressed_len,
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
        trace_id: str | None,
        published_at: datetime,
        content_type: str = DEFAULT_CONTENT_TYPE,
        compression: str = COMPRESSION_NONE,
        uncompressed_len: int | None = None,
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
            trace_id: 链路标识（可空）。
            published_at: 发布时间（UTC）。
            content_type: 内容类型。
            compression: 压缩算法。
            uncompressed_len: 解压后字节数（可空）。

        Returns:
            StoredMessage: 含分配到的 offset 的完整记录。

        Raises:
            PartitionNotBoundError: 主题目录未登记。
        """
        location = self._require_location(topic_id)
        partition = self._partition(topic_id=topic_id, partition_no=partition_no)
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
                trace_id=trace_id,
                published_at=published_at,
                content_type=content_type,
                compression=compression,
                uncompressed_len=uncompressed_len,
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
        self._require_location(topic_id)
        partition = self._partition(topic_id=topic_id, partition_no=partition_no)
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
        self._require_location(topic_id)
        return self._partition(topic_id=topic_id, partition_no=partition_no).leo

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
        self._require_location(topic_id)
        return self._partition(topic_id=topic_id, partition_no=partition_no).start

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
        self._require_location(topic_id)
        partition = self._partition(topic_id=topic_id, partition_no=partition_no)
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
        partition = self._partition(topic_id=topic_id, partition_no=partition_no)
        async with partition.lock:
            return partition.truncate_before(offset=offset)

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
        self._require_location(topic_id)
        partition = self._partition(topic_id=topic_id, partition_no=partition_no)
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
        self._require_location(topic_id)
        partition = self._partition(topic_id=topic_id, partition_no=partition_no)
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
        self._require_location(topic_id)
        partition = self._partition(topic_id=topic_id, partition_no=partition_no)
        return partition.read_bytes(offset=offset)

    async def locate_by_log_id(self, *, log_id: str) -> FileSlice | None:
        """按记录身份定位（先查 LRU，未命中扫已加载分区的段头）。

        **边界**：只扫已加载的分区——未加载分区需先访问一次；本方法是排障 / 审计辅助，
        不是热路径（02-7 的恢复期去重走「扫 `.log` 尾部」的自有原语，不依赖本方法）。

        Args:
            log_id: 记录标识。

        Returns:
            FileSlice | None: 坐标；不存在返回 None。
        """
        cached = self._log_ids.get(log_id)
        if cached is not None:
            self._log_ids.move_to_end(log_id)
            topic_id, partition_no, offset = cached
            partition = self._partitions.get((topic_id, partition_no))
            return None if partition is None else partition.locate(offset=offset)
        for partition in self._partitions.values():
            for offset, _position, header in partition.iterate_headers():
                if header.log_id == log_id:
                    return partition.locate(offset=offset)
        return None

    def _require_location(self, topic_id: int) -> TopicLocation:
        """取主题目录身份（未登记即抛错）。

        Args:
            topic_id: 主题主键。

        Returns:
            TopicLocation: 目录身份。

        Raises:
            PartitionNotBoundError: 未登记。
        """
        location = self._directory.locate(topic_id)
        if location is None:
            location = self._directory.recover(topic_id=int(topic_id))
        if location is None:
            raise PartitionNotBoundError(f"主题目录未登记：topic_id={topic_id}（须先经 TopicRouter.resolve 解析）")
        return location

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
            fsync_mode=self._fsync_mode,
            fsync_interval_ms=self._fsync_interval_ms,
            fsync_batch_records=FSYNC_BATCH_RECORDS,
            segment_max_bytes=self._segment_max_bytes,
            segment_roll_ms=self._segment_roll_ms,
            index_interval=self._index_interval,
            time_index_interval_ms=self._time_index_interval_ms,
            clock=self._clock,
        )
        self._partitions[key] = partition
        return partition

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
