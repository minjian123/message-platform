"""消费位点：端口、内存实现、文件实现与写侧协议（02-2 §7 / 02-3 详细设计）。

**端口**（01-2 定，本任务一字不改）：`OffsetStore.get` / `commit`
- 粒度 `(group, topic_id, partition_no)`：与消费组语义一致（组间位点天然隔离，01-4 不改键）；
- 提交值 = 已处理完的最后一条 + 1（即下一条待消费的 offset）；只前进不后退，重复提交幂等；
- **不吞异常**：提交失败必须冒泡，由投递服务保证「位点不前移」（先落存储、再改内存状态）。

**三个写侧协议**（独立于 `OffsetStore`，与 02-2 的 `OffsetRealigner` 同法）：
- `OffsetResetter`：管理员重置（可倒退可前跳，必须留痕；端点归 06-2）；
- `OffsetEnumerator`：按组枚举已提交位点（位点查询端点归 06-2）；
- `OffsetPurger`：组 / 主题删除后的孤儿位点清理（触发归 06-2）。
把这三个方法塞进 `OffsetStore` 会让内存替身与后续入表实现都被迫实现与本任务无关的方法，故单开协议。

**文件实现的三条结构性口径**（详细设计 §5 / §6 / §7）：
1. **单文件双槽**：两槽各自 CRC + 递增 `seq`，主槽坏了用另一槽回退——「宁可重复，不可丢」需要的
   是「上一版真的存在」，只靠 rename 的单槽做不到。
2. **内存为准**：`commit` 只改内存并标脏（零 IO），后台定时刷 + 脏阈值提前刷 + 关闭强制刷；
   崩溃窗口 = 刷盘间隔内的提交，后果是重复消费而非丢消息。
3. **越界不夹取**：`< 0` 或 `> LEO` 一律 `ParamError`（`10001`），LEO 由注入的只读 `leo()` 协议提供。
"""

import asyncio
import hashlib
import json
import os
import struct
import time
import zlib
from abc import ABC, abstractmethod
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Final, Protocol

from bms_core.audit.base import RecordRef
from bms_core.audit.events import build_operation_event
from bms_core.core.concurrent import ConcurrentStableList
from bms_core.core.exceptions import ParamError
from bms_core.core.logging import get_logger
from bms_core.core.objects import BaseFrameworkObject, BaseValueObject
from bms_core.events.base import EventEnvelope

from bms_msg.broker.log import PartitionNotBoundError
from bms_msg.core.config import (
    MS_PER_SECOND,
    OFFSET_DIRTY_LIMIT,
    OFFSET_DIRTY_LIMIT_ENV,
    OFFSET_FILE_FORMAT_VERSION,
    OFFSET_FILE_MAGIC,
    OFFSET_FILE_SUFFIX,
    OFFSET_FILE_TEMP_SUFFIX,
    OFFSET_FLUSH_INTERVAL_MS,
    OFFSET_FLUSH_INTERVAL_MS_ENV,
    OFFSET_GROUP_DIR_MAX_CHARS,
    OFFSET_GROUP_HASH_CHARS,
    OFFSET_HEADER_FIXED_BYTES,
    OFFSET_HEADER_FIXED_BYTES_V1,
    OFFSET_MAX_GROUP_BYTES,
    OFFSET_SLOT_BYTES,
    OFFSET_TOPIC_ID_ASCII_BYTES,
    OFFSETS_SUBDIR,
)

__all__ = [
    "OFFSET_PURGE_ACTION",
    "OFFSET_PURGE_PATH",
    "OFFSET_REASON_ABSENT",
    "OFFSET_REASON_CRC",
    "OFFSET_REASON_HEADER",
    "OFFSET_REASON_IDENTITY",
    "OFFSET_REASON_OK",
    "OFFSET_REASON_PARTIAL",
    "OFFSET_REASON_TRUNCATED",
    "OFFSET_REASON_VERSION",
    "OFFSET_RESET_ACTION",
    "OFFSET_RESET_PATH",
    "FileOffsetStore",
    "InMemoryOffsetStore",
    "OffsetAdjustment",
    "OffsetAuditError",
    "OffsetAuditSink",
    "OffsetCeiling",
    "OffsetDirectory",
    "OffsetEntry",
    "OffsetEnumerator",
    "OffsetFileReadout",
    "OffsetKey",
    "OffsetPurger",
    "OffsetRealigner",
    "OffsetResetter",
    "OffsetSlot",
    "OffsetStore",
    "OffsetTenantLocation",
    "OffsetUnavailableError",
    "decode_offset_file",
    "encode_offset_file",
    "group_directory_name",
    "parse_offset_filename",
]

_LOGGER = get_logger("bms_msg.offset")

OffsetKey = tuple[str, int, int]
"""位点键：`(group, topic_id, partition_no)`。"""

OFFSET_RESET_ACTION = "msg:offset.reset"
"""位点重置的审计动作（与权限码同名风格，`sys_operation_log.action` 可直接检索）。"""

OFFSET_RESET_PATH = "/api/v1/msg/admin/offsets/reset"
"""位点重置的审计路径（管理端点由 06-2 挂载，路径按规划先定，端点落地后不变）。"""

OFFSET_PURGE_ACTION = "msg:offset.purge"
"""孤儿位点清理的审计动作。"""

OFFSET_PURGE_PATH = "/api/v1/msg/admin/offsets/purge"
"""孤儿位点清理的审计路径（触发端点归 06-2）。"""

OFFSET_REASON_OK = "ok"
"""两槽均可用。"""

OFFSET_REASON_ABSENT = "absent"
"""文件不存在（从未持久化，或文件被删）。"""

OFFSET_REASON_PARTIAL = "partial"
"""仅一槽可用：用该槽回退，另一槽损坏（仍可用，但要告警）。"""

OFFSET_REASON_CRC = "crc"
"""两槽 CRC 均不通过（不可用）。"""

OFFSET_REASON_TRUNCATED = "truncated"
"""文件长度不足（写了一半 / 被截断，不可用）。"""

OFFSET_REASON_HEADER = "header"
"""文件头结构非法（魔数 / 组名长度，不可用）。"""

OFFSET_REASON_VERSION = "version"
"""格式版本高于实现支持（拒绝加载，不按旧格式猜）。"""

OFFSET_REASON_IDENTITY = "identity"
"""文件内的 group / topic_id / partition 与路径不符（挪错目录，不可用）。"""

_WRITE_FLAGS: Final = os.O_WRONLY | os.O_CREAT | os.O_TRUNC | getattr(os, "O_BINARY", 0)
"""写临时文件的打开标志。

**必须带 `O_BINARY`**：Windows 上 `os.open` 默认文本模式会把 `\\n` 翻译成 `\\r\\n`，
CRC 与实际字节立刻对不上——02-1 段文件与 02-2 恢复点同一个坑。
"""

_SLOT_STRUCT: Final = struct.Struct(">IQq")
"""槽体结构：`seq` u32 + `offset` u64 + `updated_at_us` i64（大端）。"""

_CRC_STRUCT: Final = struct.Struct(">I")
"""CRC 字段结构：u32 大端。"""

_HEADER_PREFIX: Final = struct.Struct(">HB")
"""文件头前缀：`format_version` u16 + `group_len` u8。"""

_HEADER_SUFFIX: Final = struct.Struct(">qi")
"""v1 头尾：`topic_id`（int64）+ 分区号（int32）。"""

_HEADER_PARTITION: Final = struct.Struct(">i")
"""v2 头尾的分区号（`topic_id` 已改为定宽 ASCII，只剩分区号）。"""
"""文件头后缀：`topic_id` i64 + `partition_no` i32。"""

_USABLE_REASONS: Final = frozenset({OFFSET_REASON_OK, OFFSET_REASON_PARTIAL})
"""「可用」的解析结论（其余一律 fail-closed）。"""


def _monotonic_ms() -> float:
    """单调毫秒时钟（刷盘窗口判定；用例可注入）。

    Returns:
        float: 单调递增的毫秒数。
    """
    return time.monotonic() * MS_PER_SECOND


def _epoch_us() -> int:
    """当前 epoch 微秒（`updated_at_us`，排障用、不作判据）。

    Returns:
        int: epoch 微秒。
    """
    return int(time.time() * 1_000_000)


class OffsetUnavailableError(RuntimeError):
    """位点文件存在但不可用（两槽均坏 / 版本过高 / 身份不符）。

    **fail-closed 的理由**：任务 §2.5 要求「不得直接归零、不得跳 LEO」。两槽都坏时「最近已持久化位点」
    本身不存在——归零会让整组从头重消费，跳 LEO 会静默丢积压，两者都不如「停下来等人处置」。
    `rename` 是原子的，此态只可能来自磁盘损坏或外部改写，属真正的数据丢失事件。
    """


class OffsetAuditError(RuntimeError):
    """位点重置 / 清理的审计写入失败（操作 fail-closed：没留痕就不该生效）。"""


@dataclass(frozen=True)
class OffsetAdjustment(BaseValueObject):
    """一次位点回退（崩溃恢复的「数据丢失事件」凭证）。"""

    group: str
    """消费组。"""

    topic_id: int
    """主题主键。"""

    partition_no: int
    """分区号。"""

    previous: int
    """回退前的已提交位点。"""

    current: int
    """回退后的位点（= 恢复后的新 LEO）。"""


@dataclass(frozen=True)
class OffsetEntry(BaseValueObject):
    """一个已提交位点的只读视图（`list_group` 的条目）。"""

    group: str
    """消费组。"""

    topic_id: int
    """主题主键。"""

    partition_no: int
    """分区号。"""

    offset: int
    """已提交位点（下一条待消费的 offset）。"""

    updated_at_us: int
    """最近一次落盘时刻（epoch 微秒；内存替身恒 0）。"""


@dataclass(frozen=True)
class OffsetSlot(BaseValueObject):
    """位点文件里的一个槽（`seq` 大者为最新，另一个即「上一版」）。"""

    seq: int
    """槽序号（每键独立递增，从 1 开始；空槽为 0）。"""

    offset: int
    """该槽记录的已提交位点。"""

    updated_at_us: int
    """该槽的写入时刻（epoch 微秒）。"""


@dataclass(frozen=True)
class OffsetFileReadout(BaseValueObject):
    """位点文件的解析结果（`reason` 为 `OFFSET_REASON_*`）。"""

    reason: str
    """解析结论。"""

    group: str | None = None
    """文件内的组名（头可解析时给出，供「挪错目录」排障）。"""

    topic_id: int | None = None
    """文件内的主题主键。"""

    partition_no: int | None = None
    """文件内的分区号。"""

    persisted: OffsetSlot | None = None
    """最新一版（`seq` 大的可用槽）。"""

    previous: OffsetSlot | None = None
    """上一版（另一可用槽；只有一槽可用时为 None）。"""


class OffsetStore(BaseFrameworkObject, ABC):
    """消费位点端口（读写分离；只前进不后退）。"""

    @abstractmethod
    async def get(self, *, group: str, topic_id: int, partition_no: int) -> int:
        """取已提交位点（无记录即 0，表示从头消费）。

        Args:
            group: 消费组。
            topic_id: 主题主键。
            partition_no: 分区号。

        Returns:
            int: 已提交位点。
        """

    @abstractmethod
    async def commit(self, *, group: str, topic_id: int, partition_no: int, offset: int) -> None:
        """提交位点（只前进不后退；小于等于现值即幂等无操作）。

        Args:
            group: 消费组。
            topic_id: 主题主键。
            partition_no: 分区号。
            offset: 目标位点（下一条待消费的 offset）。
        """


class OffsetRealigner(Protocol):
    """崩溃恢复的位点对齐端口（独立于 `OffsetStore`，不改其签名）。"""

    async def realign_partition(
        self, *, topic_id: int, partition_no: int, ceiling: int
    ) -> ConcurrentStableList[OffsetAdjustment]:
        """把该分区超过 `ceiling` 的已提交位点夹到 `ceiling`。

        **只回退到新 LEO，不归零**：归零等于让整组从头重消费，而「宁可重复、不可丢」要求
        回退到「数据确实还在的最后一条之后」。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。
            ceiling: 恢复后的新 LEO。

        Returns:
            ConcurrentStableList[OffsetAdjustment]: 本次调整清单（无调整即空）。
        """
        ...


class OffsetResetter(Protocol):
    """位点重置端口（管理员操作；端点、二次确认与权限校验归 06-2）。"""

    async def reset(
        self,
        *,
        group: str,
        topic_id: int,
        partition_no: int,
        offset: int,
        actor_id: int | None = None,
        ip: str | None = None,
        trace_id: str | None = None,
    ) -> int:
        """把位点重置到指定值（可倒退可前跳；必须留痕）。

        Args:
            group: 消费组。
            topic_id: 主题主键。
            partition_no: 分区号。
            offset: 目标位点（`0 ≤ offset ≤ LEO`）。
            actor_id: 操作人主键（无请求上下文时为空）。
            ip: 来源 IP。
            trace_id: 链路标识。

        Returns:
            int: 重置后的位点。
        """
        ...


class OffsetEnumerator(Protocol):
    """位点枚举端口（位点查询端点归 06-2）。"""

    async def list_group(self, *, tenant_id: int, group: str) -> ConcurrentStableList[OffsetEntry]:
        """枚举该组已提交的全部位点。

        **必须带 `tenant_id`**：只有组名无法定位租户目录；扫全部租户目录等于跨租户读取。

        Args:
            tenant_id: 租户主键。
            group: 消费组。

        Returns:
            ConcurrentStableList[OffsetEntry]: 按 `(topic_id, partition_no)` 升序。
        """
        ...


class OffsetPurger(Protocol):
    """孤儿位点清理端口（组 / 主题删除后；触发归 06-2）。"""

    async def purge_group(self, *, tenant_id: int, group: str) -> int:
        """删除该组的全部位点文件。

        Args:
            tenant_id: 租户主键。
            group: 消费组。

        Returns:
            int: 删除的文件数。
        """
        ...

    async def purge_topic(self, *, topic_id: int) -> int:
        """删除该主题在所有组下的位点文件。

        Args:
            topic_id: 主题主键。

        Returns:
            int: 删除的文件数。
        """
        ...

    async def purge_tenant(self, *, tenant_id: int) -> int:
        """删除该租户的全部位点文件（租户删除清理）。

        Args:
            tenant_id: 租户主键。

        Returns:
            int: 删除的文件数。
        """
        ...


class OffsetCeiling(Protocol):
    """LEO 只读协议（越界判定用；`FileMessageStore` 结构上即满足）。"""

    async def leo(self, *, topic_id: int, partition_no: int) -> int:
        """取分区的日志末端（LEO）。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。

        Returns:
            int: 下一条待写入的 offset。
        """
        ...


class OffsetTenantLocation(Protocol):
    """目录身份的最小视图（只要 `tenant_id`；`TopicLocation` 结构上即满足）。

    **用只读 property 而非属性**：`TopicLocation` 是 frozen dataclass，其字段是只读的；
    Protocol 若声明可写属性，pyright 会判「不兼容」（结构子类型要求可变属性双向可赋值）。
    """

    @property
    def tenant_id(self) -> int:
        """租户主键。

        Returns:
            int: 租户主键。
        """
        ...


class OffsetDirectory(Protocol):
    """`topic_id → 目录身份` 的查询协议（`PartitionDirectory` 结构上即满足）。

    只声明位点需要的两个方法，避免 `offset` 反向依赖 `file_store`（那会成环：
    `file_store` 已经 `from bms_msg.broker.offset import OffsetRealigner`）。
    """

    def locate(self, topic_id: int) -> OffsetTenantLocation | None:
        """按主题主键取已登记身份。

        Args:
            topic_id: 主题主键。

        Returns:
            OffsetTenantLocation | None: 已登记返回身份，否则 None。
        """
        ...

    def recover(self, *, topic_id: int) -> OffsetTenantLocation | None:
        """从盘上反查目录身份并登记（重启后内部调用兜底）。

        Args:
            topic_id: 主题主键。

        Returns:
            OffsetTenantLocation | None: 找回的身份；盘上不存在返回 None。
        """
        ...


class OffsetAuditSink(Protocol):
    """审计出口（最小协议）：基座 `AuditEventEmitter` 结构上即满足，用例可注入替身。"""

    async def emit(self, event: EventEnvelope, *, required: bool = False) -> bool:
        """写一条审计事件。

        Args:
            event: 事件信封。
            required: 强审计（写入失败即抛，不放行业务）。

        Returns:
            bool: 写入成功 True。
        """
        ...


def group_directory_name(group: str) -> str:
    """算组目录名（`{可读名}-{sha1 前 8 位}`）。

    **为什么要哈希**：`GROUP_NAME_PATTERN` 允许大小写混用，而 Windows / macOS 大小写不敏感——
    直接用组名作目录名会让 `Audit` 与 `audit` 共享一个文件（前者位点丢失、后者跳消息）。
    哈希取自**未截断**的组名，故截断只影响可读性、不影响唯一性。

    Args:
        group: 消费组名。

    Returns:
        str: 组目录名（长度 ≤ `OFFSET_GROUP_DIR_MAX_CHARS + 1 + OFFSET_GROUP_HASH_CHARS`）。
    """
    digest = hashlib.sha1(group.encode("utf-8")).hexdigest()[:OFFSET_GROUP_HASH_CHARS]
    return f"{group[:OFFSET_GROUP_DIR_MAX_CHARS]}-{digest}"


def parse_offset_filename(name: str) -> tuple[int, int] | None:
    """解析位点文件名（`{topic_id}-{partition}.offset`）。

    Args:
        name: 文件名。

    Returns:
        tuple[int, int] | None: `(topic_id, partition_no)`；不符合命名返回 None。
    """
    if not name.endswith(OFFSET_FILE_SUFFIX):
        return None
    stem = name[: -len(OFFSET_FILE_SUFFIX)]
    topic_part, separator, partition_part = stem.rpartition("-")
    if not separator or not topic_part.isdigit() or not partition_part.isdigit():
        return None
    return int(topic_part), int(partition_part)


def encode_offset_file(*, group: str, topic_id: int, partition_no: int, slots: tuple[OffsetSlot, ...]) -> bytes:
    """把两个槽编码成位点文件的字节（头 + 槽，CRC 覆盖「头 + 本槽前 20 字节」）。

    Args:
        group: 消费组（写入文件头，用于「挪错目录」自检）。
        topic_id: 主题主键。
        partition_no: 分区号。
        slots: 槽列表（按写入顺序，通常是「新值、上一版」）。

    Returns:
        bytes: 文件字节。

    Raises:
        ValueError: 组名超出 `OFFSET_MAX_GROUP_BYTES`（写进去就读不出来，必须在写前失败）。
    """
    group_bytes = group.encode("utf-8")
    if not 0 < len(group_bytes) <= OFFSET_MAX_GROUP_BYTES:
        raise ValueError(f"组名长度非法：{len(group_bytes)} 字节（上限 {OFFSET_MAX_GROUP_BYTES}）")
    topic_ascii = str(int(topic_id)).encode("ascii").rjust(OFFSET_TOPIC_ID_ASCII_BYTES, b" ")
    header = (
        OFFSET_FILE_MAGIC
        + _HEADER_PREFIX.pack(OFFSET_FILE_FORMAT_VERSION, len(group_bytes))
        + group_bytes
        + topic_ascii
        + _HEADER_PARTITION.pack(int(partition_no))
    )
    parts = [header]
    for slot in slots:
        payload = _SLOT_STRUCT.pack(slot.seq, slot.offset, slot.updated_at_us)
        parts.append(payload + _CRC_STRUCT.pack(zlib.crc32(header + payload)))
    return b"".join(parts)


def decode_offset_file(
    raw: bytes, *, group: str | None = None, topic_id: int | None = None, partition_no: int | None = None
) -> OffsetFileReadout:
    """解析位点文件字节（两槽各自校验，取 `seq` 大者为最新）。

    Args:
        raw: 文件字节。
        group: 期望的组名（None 表示不校验身份，供 `realign` / `purge` 反查用）。
        topic_id: 期望的主题主键（None 表示不校验）。
        partition_no: 期望的分区号（None 表示不校验）。

    Returns:
        OffsetFileReadout: 解析结论（`reason` 为 `OFFSET_REASON_*`）。
    """
    if len(raw) < OFFSET_HEADER_FIXED_BYTES_V1:
        return OffsetFileReadout(reason=OFFSET_REASON_TRUNCATED)
    if raw[:4] != OFFSET_FILE_MAGIC:
        return OffsetFileReadout(reason=OFFSET_REASON_HEADER)
    version, group_len = _HEADER_PREFIX.unpack_from(raw, 4)
    if version > OFFSET_FILE_FORMAT_VERSION:
        return OffsetFileReadout(reason=OFFSET_REASON_VERSION)
    if not 0 < group_len <= OFFSET_MAX_GROUP_BYTES:
        return OffsetFileReadout(reason=OFFSET_REASON_HEADER)
    fixed = OFFSET_HEADER_FIXED_BYTES if version >= 2 else OFFSET_HEADER_FIXED_BYTES_V1
    header_len = fixed + group_len
    if len(raw) < header_len + 2 * OFFSET_SLOT_BYTES:
        return OffsetFileReadout(reason=OFFSET_REASON_TRUNCATED)
    file_group = raw[7 : 7 + group_len].decode("utf-8", errors="replace")
    if version >= 2:
        raw_topic = raw[7 + group_len : 7 + group_len + OFFSET_TOPIC_ID_ASCII_BYTES]
        file_topic = int(raw_topic.decode("ascii", errors="replace").strip() or "0")
        file_partition = _HEADER_PARTITION.unpack_from(raw, 7 + group_len + OFFSET_TOPIC_ID_ASCII_BYTES)[0]
    else:
        file_topic, file_partition = _HEADER_SUFFIX.unpack_from(raw, 7 + group_len)
    identity = OffsetFileReadout(
        reason=OFFSET_REASON_OK,
        group=file_group,
        topic_id=file_topic,
        partition_no=file_partition,
    )
    if group is not None and file_group != group:
        return OffsetFileReadout(
            reason=OFFSET_REASON_IDENTITY,
            group=file_group,
            topic_id=file_topic,
            partition_no=file_partition,
        )
    if topic_id is not None and file_topic != int(topic_id):
        return OffsetFileReadout(
            reason=OFFSET_REASON_IDENTITY,
            group=file_group,
            topic_id=file_topic,
            partition_no=file_partition,
        )
    if partition_no is not None and file_partition != int(partition_no):
        return OffsetFileReadout(
            reason=OFFSET_REASON_IDENTITY,
            group=file_group,
            topic_id=file_topic,
            partition_no=file_partition,
        )
    header = raw[:header_len]
    slots: list[OffsetSlot] = []
    for index in range(2):
        start = header_len + index * OFFSET_SLOT_BYTES
        payload = raw[start : start + OFFSET_SLOT_BYTES - 4]
        crc = _CRC_STRUCT.unpack(raw[start + OFFSET_SLOT_BYTES - 4 : start + OFFSET_SLOT_BYTES])[0]
        if zlib.crc32(header + payload) != crc:
            continue
        seq, offset, updated_at_us = _SLOT_STRUCT.unpack(payload)
        slots.append(OffsetSlot(seq=seq, offset=offset, updated_at_us=updated_at_us))
    if not slots:
        return OffsetFileReadout(
            reason=OFFSET_REASON_CRC,
            group=identity.group,
            topic_id=identity.topic_id,
            partition_no=identity.partition_no,
        )
    slots.sort(key=lambda slot: slot.seq)
    return OffsetFileReadout(
        reason=OFFSET_REASON_OK if len(slots) == 2 else OFFSET_REASON_PARTIAL,
        group=identity.group,
        topic_id=identity.topic_id,
        partition_no=identity.partition_no,
        persisted=slots[-1],
        previous=slots[0] if len(slots) == 2 else None,
    )


class InMemoryOffsetStore(OffsetStore, OffsetRealigner, OffsetResetter, OffsetEnumerator, OffsetPurger):
    """内存位点存储（轻量替身；文件实现见 `FileOffsetStore`）。

    **补齐三个写侧协议是为了端点用例可以直接用内存库**：`tenant_id` 在内存里没有维度（键仍是三元组），
    故 `list_group` / `purge_group` 忽略它；`reset` 不做 LEO 校验（没有 LEO 可读）、不写审计。
    真实语义（越界拒绝、审计 fail-closed、落盘）只在 `FileOffsetStore` 上成立。
    """

    def __init__(self) -> None:
        """初始化（无记录即空）。"""
        self._offsets: dict[OffsetKey, int] = {}

    async def get(self, *, group: str, topic_id: int, partition_no: int) -> int:
        """取已提交位点（无记录即 0）。

        Args:
            group: 消费组。
            topic_id: 主题主键。
            partition_no: 分区号。

        Returns:
            int: 已提交位点。
        """
        return self._offsets.get((group, topic_id, partition_no), 0)

    async def commit(self, *, group: str, topic_id: int, partition_no: int, offset: int) -> None:
        """提交位点（只前进不后退；重复提交幂等）。

        Args:
            group: 消费组。
            topic_id: 主题主键。
            partition_no: 分区号。
            offset: 目标位点。
        """
        key = (group, topic_id, partition_no)
        if offset > self._offsets.get(key, 0):
            self._offsets[key] = offset

    async def realign_partition(
        self, *, topic_id: int, partition_no: int, ceiling: int
    ) -> ConcurrentStableList[OffsetAdjustment]:
        """把该分区超过 `ceiling` 的位点夹到 `ceiling`（内存实现；文件实现见 `FileOffsetStore`）。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。
            ceiling: 恢复后的新 LEO。

        Returns:
            ConcurrentStableList[OffsetAdjustment]: 本次调整清单。
        """
        adjusted: ConcurrentStableList[OffsetAdjustment] = ConcurrentStableList()
        for (group, topic, partition), value in list(self._offsets.items()):
            if topic != topic_id or partition != partition_no or value <= ceiling:
                continue
            self._offsets[(group, topic, partition)] = ceiling
            adjusted.add(
                OffsetAdjustment(
                    group=group,
                    topic_id=topic,
                    partition_no=partition,
                    previous=value,
                    current=ceiling,
                )
            )
        return adjusted

    async def reset(
        self,
        *,
        group: str,
        topic_id: int,
        partition_no: int,
        offset: int,
        actor_id: int | None = None,
        ip: str | None = None,
        trace_id: str | None = None,
    ) -> int:
        """把位点重置到指定值（替身语义：不做 LEO 校验、不写审计）。

        Args:
            group: 消费组。
            topic_id: 主题主键。
            partition_no: 分区号。
            offset: 目标位点（不得为负）。
            actor_id: 操作人主键（替身忽略）。
            ip: 来源 IP（替身忽略）。
            trace_id: 链路标识（替身忽略）。

        Returns:
            int: 重置后的位点。

        Raises:
            ParamError: 位点为负（`10001`，与文件实现同口径）。
        """
        del actor_id, ip, trace_id
        candidate = int(offset)
        if candidate < 0:
            raise ParamError(f"位点不得为负：{candidate}")
        self._offsets[(group, int(topic_id), int(partition_no))] = candidate
        return candidate

    async def list_group(self, *, tenant_id: int, group: str) -> ConcurrentStableList[OffsetEntry]:
        """枚举该组已提交的位点（替身忽略 `tenant_id`，内存里没有租户维度）。

        Args:
            tenant_id: 租户主键（替身忽略）。
            group: 消费组。

        Returns:
            ConcurrentStableList[OffsetEntry]: 按 `(topic_id, partition_no)` 升序。
        """
        del tenant_id
        entries = [
            OffsetEntry(group=name, topic_id=topic, partition_no=partition, offset=value, updated_at_us=0)
            for (name, topic, partition), value in sorted(self._offsets.items())
            if name == group
        ]
        return ConcurrentStableList(entries)

    async def purge_group(self, *, tenant_id: int, group: str) -> int:
        """删除该组的全部位点（替身忽略 `tenant_id`）。

        Args:
            tenant_id: 租户主键（替身忽略）。
            group: 消费组。

        Returns:
            int: 删除的条目数。
        """
        del tenant_id
        keys = [key for key in self._offsets if key[0] == group]
        for key in keys:
            self._offsets.pop(key, None)
        return len(keys)

    async def purge_topic(self, *, topic_id: int) -> int:
        """删除该主题在所有组下的位点。

        Args:
            topic_id: 主题主键。

        Returns:
            int: 删除的条目数。
        """
        keys = [key for key in self._offsets if key[1] == int(topic_id)]
        for key in keys:
            self._offsets.pop(key, None)
        return len(keys)

    async def purge_tenant(self, *, tenant_id: int) -> int:
        """删除该租户的全部位点（内存替身没有租户维度，恒返回 0）。

        Args:
            tenant_id: 租户主键。

        Returns:
            int: 0。
        """
        del tenant_id
        return 0


@dataclass
class _Entry:
    """一个位点的进程内状态（`current` 为准，`persisted` / `previous` 供刷盘写双槽）。"""

    tenant_id: int
    """租户主键（位点文件目录的根）。"""

    group: str
    """消费组。"""

    topic_id: int
    """主题主键。"""

    partition_no: int
    """分区号。"""

    path: Path
    """位点文件路径（懒创建：只在刷盘时建目录）。"""

    current: int = 0
    """内存最新位点（`get` 的返回值）。"""

    persisted: int = 0
    """最近一次成功落盘的值（= 文件里 `seq` 大的槽）。"""

    previous: int | None = None
    """上一次成功落盘的值（= 另一槽；没有则为 None）。"""

    seq: int = 0
    """已落盘的最大槽序号。"""

    updated_at_us: int = 0
    """最近一次变更时刻（epoch 微秒）。"""

    version: int = 0
    """内存值的版本号（每次变更 +1；刷盘后比对决定能否清脏）。"""

    dirty: bool = False
    """是否有未落盘变更。"""

    unavailable: str | None = None
    """不可用原因（`OFFSET_REASON_*` 或 `read:<错误>`；None 表示可用）。"""


@dataclass
class _Snapshot:
    """一次刷盘的快照（取快照后内存可能继续前进，故记下版本号）。"""

    entry: _Entry
    """目标位点状态。"""

    version: int
    """取快照时的版本号。"""

    seq: int
    """本次写入的槽序号（= 当前最大 `seq` + 1）。"""

    offset: int
    """本次写入的位点值。"""

    previous_offset: int
    """另一槽要写的「上一版」位点值。"""

    updated_at_us: int
    """写入时刻（epoch 微秒）。"""


class FileOffsetStore(OffsetStore, OffsetRealigner, OffsetResetter, OffsetEnumerator, OffsetPurger):
    """文件位点存储（单文件双槽 + 内存为准 + 后台刷盘）。

    崩溃语义：`commit` 后未刷盘即崩 → 重启读到的位点是窗口前的值 → **重复消费这段**（不丢）。
    不会出现「位点 > 已落盘 LEO」：关闭时先刷日志（`StorageCloser`）再刷位点（`OffsetFlusher`）。
    """

    def __init__(
        self,
        *,
        directory: OffsetDirectory,
        root: Path,
        audit: OffsetAuditSink,
        flush_interval_ms: int | None = None,
        dirty_limit: int | None = None,
        clock: Callable[[], float] = _monotonic_ms,
        now_us: Callable[[], int] = _epoch_us,
    ) -> None:
        """初始化（不扫盘、不建目录——首个访问触发懒加载）。

        Args:
            directory: 目录登记表（`topic_id → tenant_id` 的唯一来源）。
            root: 数据根（位点目录为 `{root}/offsets/{tenant_id}/{group_dir}`）。
            audit: 审计出口（位点重置与清理必须留痕）。
            flush_interval_ms: 刷盘时间窗（None 取环境变量，缺省模块常量）。
            dirty_limit: 单次刷盘文件数上限（None 取环境变量，缺省模块常量）。
            clock: 单调毫秒时钟（刷盘窗口判定，用例注入）。
            now_us: 当前 epoch 微秒（`updated_at_us`，用例注入）。

        Raises:
            ValueError: 时间窗或脏阈值不是正数（启动即失败，不静默降级）。
        """
        interval = (
            flush_interval_ms
            if flush_interval_ms is not None
            else int(os.environ.get(OFFSET_FLUSH_INTERVAL_MS_ENV, OFFSET_FLUSH_INTERVAL_MS))
        )
        if interval <= 0:
            raise ValueError(f"{OFFSET_FLUSH_INTERVAL_MS_ENV} 须为正数：{interval}")
        limit = (
            dirty_limit if dirty_limit is not None else int(os.environ.get(OFFSET_DIRTY_LIMIT_ENV, OFFSET_DIRTY_LIMIT))
        )
        if limit <= 0:
            raise ValueError(f"{OFFSET_DIRTY_LIMIT_ENV} 须为正数：{limit}")
        self._directory = directory
        self._root = root
        self._audit = audit
        self._interval_ms = interval
        self._dirty_limit = limit
        self._clock = clock
        self._now_us = now_us
        self._entries: dict[OffsetKey, _Entry] = {}
        self._ceiling: OffsetCeiling | None = None
        self._ceiling_warned = False
        self._wakeup = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        self._flushing = False

    # ------------------------------------------------------------------
    # 端口：get / commit
    # ------------------------------------------------------------------

    async def get(self, *, group: str, topic_id: int, partition_no: int) -> int:
        """取已提交位点（无记录即 0；不可用时 fail-closed 抛错）。

        Args:
            group: 消费组。
            topic_id: 主题主键。
            partition_no: 分区号。

        Returns:
            int: 已提交位点。

        Raises:
            OffsetUnavailableError: 位点文件存在但两槽均坏 / 版本过高 / 身份不符。
        """
        entry = await self._resolve(group=group, topic_id=topic_id, partition_no=partition_no)
        if entry is None:
            return 0
        self._ensure_available(entry)
        return entry.current

    async def commit(self, *, group: str, topic_id: int, partition_no: int, offset: int) -> None:
        """提交位点（只前进；负数或越过 LEO 即拒绝，不夹取）。

        Args:
            group: 消费组。
            topic_id: 主题主键。
            partition_no: 分区号。
            offset: 目标位点（下一条待消费的 offset）。

        Raises:
            ParamError: 位点为负或超过 LEO（`10001`，不得默默夹取）。
            PartitionNotBoundError: 主题目录未登记（`tenant_id` 无从解析）。
            OffsetUnavailableError: 位点文件不可用（fail-closed）。
        """
        candidate = int(offset)
        if candidate < 0:
            raise ParamError(f"位点不得为负：{candidate}")
        entry = await self._resolve(group=group, topic_id=topic_id, partition_no=partition_no)
        if entry is None:
            raise PartitionNotBoundError(f"主题目录未登记：topic_id={topic_id}（须先经 TopicRouter.resolve 解析）")
        self._ensure_available(entry)
        await self._judge_ceiling(entry, candidate)
        if candidate <= entry.current:
            return
        entry.current = candidate
        entry.version += 1
        entry.updated_at_us = self._now_us()
        entry.dirty = True
        if self._dirty_count() >= self._dirty_limit:
            self._wakeup.set()

    # ------------------------------------------------------------------
    # 对齐：realign_partition
    # ------------------------------------------------------------------

    async def realign_partition(
        self, *, topic_id: int, partition_no: int, ceiling: int
    ) -> ConcurrentStableList[OffsetAdjustment]:
        """把该分区超过 `ceiling` 的位点夹到 `ceiling` 并立即落盘。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。
            ceiling: 恢复后的新 LEO。

        Returns:
            ConcurrentStableList[OffsetAdjustment]: 本次调整清单（无调整即空）。
        """
        adjusted: ConcurrentStableList[OffsetAdjustment] = ConcurrentStableList()
        location = self._location(int(topic_id))
        if location is None:
            # 没有日志分区就没有截断，无需对齐；不告警（与「装配缺陷」区分开）
            return adjusted
        tenant_id = int(location.tenant_id)
        group_root = self._root / OFFSETS_SUBDIR / str(tenant_id)
        if not group_root.is_dir():
            return adjusted
        target = f"{int(topic_id)}-{int(partition_no)}{OFFSET_FILE_SUFFIX}"
        for directory in sorted(path for path in group_root.iterdir() if path.is_dir()):
            path = directory / target
            if not path.is_file():
                continue
            entry = self._entry_for_path(tenant_id=tenant_id, path=path)
            if entry is None or entry.unavailable is not None or entry.current <= ceiling:
                continue
            previous = entry.current
            entry.current = int(ceiling)
            entry.version += 1
            entry.updated_at_us = self._now_us()
            entry.dirty = True
            adjusted.add(
                OffsetAdjustment(
                    group=entry.group,
                    topic_id=entry.topic_id,
                    partition_no=entry.partition_no,
                    previous=previous,
                    current=int(ceiling),
                )
            )
        if adjusted:
            await self.flush_once()
            _LOGGER.info(
                "msg_offset_realigned",
                topic_id=int(topic_id),
                partition_no=int(partition_no),
                count=len(adjusted),
            )
        return adjusted

    # ------------------------------------------------------------------
    # 三协议：reset / list_group / purge
    # ------------------------------------------------------------------

    async def reset(
        self,
        *,
        group: str,
        topic_id: int,
        partition_no: int,
        offset: int,
        actor_id: int | None = None,
        ip: str | None = None,
        trace_id: str | None = None,
    ) -> int:
        """把位点重置到指定值（可倒退可前跳；审计失败即拒绝）。

        Args:
            group: 消费组。
            topic_id: 主题主键。
            partition_no: 分区号。
            offset: 目标位点（`0 ≤ offset ≤ LEO`）。
            actor_id: 操作人主键。
            ip: 来源 IP。
            trace_id: 链路标识。

        Returns:
            int: 重置后的位点。

        Raises:
            ParamError: 位点为负或超过 LEO（`10001`）。
            PartitionNotBoundError: 主题目录未登记。
            OffsetUnavailableError: 位点文件不可用（fail-closed）。
            OffsetAuditError: 审计写入失败（重置 fail-closed）。
        """
        candidate = int(offset)
        if candidate < 0:
            raise ParamError(f"位点不得为负：{candidate}")
        entry = await self._resolve(group=group, topic_id=topic_id, partition_no=partition_no)
        if entry is None:
            raise PartitionNotBoundError(f"主题目录未登记：topic_id={topic_id}（须先经 TopicRouter.resolve 解析）")
        self._ensure_available(entry)
        await self._judge_ceiling(entry, candidate)
        previous = entry.current
        await self._emit_reset_audit(
            entry=entry,
            previous=previous,
            current=candidate,
            actor_id=actor_id,
            ip=ip,
            trace_id=trace_id,
        )
        entry.current = candidate
        entry.version += 1
        entry.updated_at_us = self._now_us()
        entry.dirty = True
        await self.flush_once()
        _LOGGER.info(
            "msg_offset_reset",
            group=group,
            topic_id=entry.topic_id,
            partition_no=entry.partition_no,
            previous=previous,
            current=candidate,
            actor_id=actor_id,
        )
        return candidate

    async def list_group(self, *, tenant_id: int, group: str) -> ConcurrentStableList[OffsetEntry]:
        """枚举该组已提交的全部位点（按 `(topic_id, partition_no)` 升序）。

        Args:
            tenant_id: 租户主键。
            group: 消费组。

        Returns:
            ConcurrentStableList[OffsetEntry]: 位点视图（不可用文件跳过并告警）。
        """
        entries: list[OffsetEntry] = []
        directory = self._root / OFFSETS_SUBDIR / str(int(tenant_id)) / group_directory_name(group)
        if not directory.is_dir():
            return ConcurrentStableList()
        for path in sorted(directory.glob(f"*{OFFSET_FILE_SUFFIX}")):
            if parse_offset_filename(path.name) is None:
                continue
            entry = self._entry_for_path(tenant_id=int(tenant_id), path=path)
            if entry is None:
                continue
            self._ensure_available(entry, quiet=True)
            if entry.unavailable is not None:
                continue
            entries.append(
                OffsetEntry(
                    group=entry.group,
                    topic_id=entry.topic_id,
                    partition_no=entry.partition_no,
                    offset=entry.current,
                    updated_at_us=entry.updated_at_us,
                )
            )
        entries.sort(key=lambda item: (item.topic_id, item.partition_no))
        return ConcurrentStableList(entries)

    async def purge_group(self, *, tenant_id: int, group: str) -> int:
        """删除该组的全部位点文件与进程内缓存。

        Args:
            tenant_id: 租户主键。
            group: 消费组。

        Returns:
            int: 删除的文件数。
        """
        directory = self._root / OFFSETS_SUBDIR / str(int(tenant_id)) / group_directory_name(group)
        if not directory.is_dir():
            return 0
        removed = 0
        for path in sorted(directory.glob(f"*{OFFSET_FILE_SUFFIX}")):
            parsed = parse_offset_filename(path.name)
            path.unlink(missing_ok=True)
            removed += 1
            if parsed is not None:
                self._entries.pop((group, parsed[0], parsed[1]), None)
        if not any(directory.iterdir()):
            directory.rmdir()
        await self._emit_purge_audit(scope="group", tenant_id=int(tenant_id), key=group, files=removed)
        _LOGGER.info("msg_offset_purged", scope="group", tenant_id=int(tenant_id), group=group, files=removed)
        return removed

    async def purge_topic(self, *, topic_id: int) -> int:
        """删除该主题在所有组下的位点文件（空组目录一并删）。

        Args:
            topic_id: 主题主键。

        Returns:
            int: 删除的文件数。
        """
        location = self._location(int(topic_id))
        if location is None:
            return 0
        tenant_id = int(location.tenant_id)
        group_root = self._root / OFFSETS_SUBDIR / str(tenant_id)
        if not group_root.is_dir():
            return 0
        removed = 0
        for directory in sorted(path for path in group_root.iterdir() if path.is_dir()):
            for path in sorted(directory.glob(f"{int(topic_id)}-*{OFFSET_FILE_SUFFIX}")):
                if parse_offset_filename(path.name) is None:
                    continue
                path.unlink(missing_ok=True)
                removed += 1
            if not any(directory.iterdir()):
                directory.rmdir()
        for key in [item for item in self._entries if item[1] == int(topic_id)]:
            self._entries.pop(key, None)
        await self._emit_purge_audit(scope="topic", tenant_id=tenant_id, key=str(int(topic_id)), files=removed)
        _LOGGER.info("msg_offset_purged", scope="topic", topic_id=int(topic_id), files=removed)
        return removed

    async def purge_tenant(self, *, tenant_id: int) -> int:
        """删除该租户的全部位点文件与进程内缓存（租户删除清理）。

        Args:
            tenant_id: 租户主键。

        Returns:
            int: 删除的文件数。
        """
        target = int(tenant_id)
        group_root = self._root / OFFSETS_SUBDIR / str(target)
        removed = 0
        if group_root.is_dir():
            for directory in sorted(path for path in group_root.iterdir() if path.is_dir()):
                for path in sorted(directory.glob(f"*{OFFSET_FILE_SUFFIX}")):
                    if parse_offset_filename(path.name) is None:
                        continue
                    path.unlink(missing_ok=True)
                    removed += 1
                if not any(directory.iterdir()):
                    directory.rmdir()
            if not any(group_root.iterdir()):
                group_root.rmdir()
        for key in [item for item in self._entries if int(self._entries[item].tenant_id) == target]:
            self._entries.pop(key, None)
        await self._emit_purge_audit(scope="tenant", tenant_id=target, key=str(target), files=removed)
        _LOGGER.info("msg_offset_purged", scope="tenant", tenant_id=target, files=removed)
        return removed

    # ------------------------------------------------------------------
    # 刷盘
    # ------------------------------------------------------------------

    def bind_ceiling(self, ceiling: OffsetCeiling) -> None:
        """绑定 LEO 读取端口（构造后才可用：`FileMessageStore` 与本对象互相引用，只能后绑）。

        Args:
            ceiling: LEO 只读端口。
        """
        self._ceiling = ceiling

    @property
    def running(self) -> bool:
        """刷盘循环是否在跑（`ensure_started` 幂等与用例判据）。

        Returns:
            bool: 有活跃任务即 True。
        """
        return self._task is not None

    def ensure_started(self) -> None:
        """惰性启动刷盘循环（幂等：已有活跃任务即返回）。"""
        if self._task is not None:
            return
        self._task = asyncio.create_task(self._loop())

    async def stop(self) -> None:
        """停止刷盘循环（幂等；不刷盘——刷盘由 `OffsetFlusher` 在其后负责）。"""
        task = self._task
        self._task = None
        if task is None:
            return
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task

    async def run_once(self) -> int:
        """单步刷盘（不等待，用例可精确驱动）。

        Returns:
            int: 本轮落盘的文件数。
        """
        return await self.flush_once()

    async def flush_once(self) -> int:
        """把脏条目刷到磁盘（单飞：同一时刻只有一次刷盘在飞）。

        快照取完后内存可能继续前进，故刷盘成功只更新 `persisted` / `previous`，
        **清脏要按版本比对**：版本不等说明又有新值，保留脏让下一轮补刷。

        Returns:
            int: 本次落盘的文件数（失败的不计）。
        """
        if self._flushing:
            return 0
        self._flushing = True
        try:
            snapshots = [
                _Snapshot(
                    entry=entry,
                    version=entry.version,
                    seq=entry.seq + 1,
                    offset=entry.current,
                    previous_offset=entry.persisted,
                    updated_at_us=entry.updated_at_us,
                )
                for entry in self._entries.values()
                if entry.dirty
            ][: self._dirty_limit]
            if not snapshots:
                return 0
            started = self._clock()
            written = 0
            for snapshot in snapshots:
                try:
                    self._write(snapshot)
                except (OSError, ValueError) as exc:
                    _LOGGER.warning(
                        "msg_offset_flush_failed",
                        group=snapshot.entry.group,
                        topic_id=snapshot.entry.topic_id,
                        partition_no=snapshot.entry.partition_no,
                        error=repr(exc),
                    )
                    continue
                written += 1
                snapshot.entry.previous = snapshot.entry.persisted
                snapshot.entry.persisted = snapshot.offset
                snapshot.entry.seq = snapshot.seq
                if snapshot.entry.version == snapshot.version:
                    snapshot.entry.dirty = False
            _LOGGER.info(
                "msg_offset_flushed",
                files=written,
                entries=len(snapshots),
                elapsed_ms=int(self._clock() - started),
            )
            return written
        finally:
            self._flushing = False

    async def aclose(self) -> None:
        """优雅关闭：停循环 + 无条件刷盘（幂等）。"""
        await self.stop()
        written = await self.flush_once()
        closed = len(self._entries)
        self._entries.clear()
        _LOGGER.info("msg_offset_store_closed", files=written, entries=closed)

    async def _loop(self) -> None:
        """刷盘循环：窗口到期或被唤醒即刷一轮（单轮异常不终止循环）。"""
        while True:
            with suppress(TimeoutError):
                await asyncio.wait_for(self._wakeup.wait(), timeout=self._interval_ms / MS_PER_SECOND)
            self._wakeup.clear()
            try:
                await self.run_once()
            except Exception:  # 单轮异常只记日志，下一轮继续（刷盘不得静默退出）
                _LOGGER.exception("msg_offset_flush_tick_failed")

    def _write(self, snapshot: _Snapshot) -> None:
        """把一个快照写成位点文件（临时文件 + fsync + rename）。

        Args:
            snapshot: 刷盘快照。

        Raises:
            OSError: 磁盘满 / 权限不足等（调用方保留脏并告警，不阻断提交）。
            ValueError: 组名超长（装配期不该发生，写前失败优于写坏）。
        """
        entry = snapshot.entry
        encoded = encode_offset_file(
            group=entry.group,
            topic_id=entry.topic_id,
            partition_no=entry.partition_no,
            slots=(
                OffsetSlot(seq=snapshot.seq, offset=snapshot.offset, updated_at_us=snapshot.updated_at_us),
                OffsetSlot(
                    seq=max(snapshot.seq - 1, 0),
                    offset=snapshot.previous_offset,
                    updated_at_us=snapshot.updated_at_us,
                ),
            ),
        )
        entry.path.parent.mkdir(parents=True, exist_ok=True)
        temporary = entry.path.with_name(f"{entry.path.name}{OFFSET_FILE_TEMP_SUFFIX}")
        descriptor = os.open(temporary, _WRITE_FLAGS, 0o644)
        try:
            written = 0
            while written < len(encoded):
                written += os.write(descriptor, encoded[written:])
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        temporary.replace(entry.path)

    # ------------------------------------------------------------------
    # 内部：解析、加载、判定
    # ------------------------------------------------------------------

    def _location(self, topic_id: int) -> OffsetTenantLocation | None:
        """按主题主键解析目录身份（登记表优先，未登记则扫盘反查）。

        Args:
            topic_id: 主题主键。

        Returns:
            OffsetTenantLocation | None: 目录身份；盘上不存在返回 None。
        """
        located = self._directory.locate(int(topic_id))
        if located is not None:
            return located
        return self._directory.recover(topic_id=int(topic_id))

    async def _resolve(self, *, group: str, topic_id: int, partition_no: int) -> _Entry | None:
        """取（必要时加载）一个位点的状态。

        Args:
            group: 消费组。
            topic_id: 主题主键。
            partition_no: 分区号。

        Returns:
            _Entry | None: 位点状态；租户无从解析时返回 None（读侧按 0 处理，写侧由调用方抛错）。
        """
        key = (group, int(topic_id), int(partition_no))
        cached = self._entries.get(key)
        if cached is not None:
            return cached
        location = self._location(int(topic_id))
        if location is None:
            return None
        return self._load_entry(key=key, tenant_id=int(location.tenant_id))

    def _entry_for_path(self, *, tenant_id: int, path: Path) -> _Entry | None:
        """由已知文件路径取状态（组名从文件头读：目录名是截断 + 哈希，反推不出来）。

        Args:
            tenant_id: 租户主键。
            path: 位点文件路径。

        Returns:
            _Entry | None: 位点状态；文件读不出身份（空文件 / 损坏头 / 读失败）返回 None。

        文件名合法性由调用方过滤（`list_group` 用 `parse_offset_filename` 判过，
        `realign` 用的是自己拼出来的合法文件名），这里不再重复判。
        """
        try:
            raw = path.read_bytes()
        except OSError:
            return None
        head = decode_offset_file(raw)
        if head.group is None or head.topic_id is None or head.partition_no is None:
            return None
        key = (head.group, head.topic_id, head.partition_no)
        cached = self._entries.get(key)
        if cached is not None:
            return cached
        return self._load_entry(key=key, tenant_id=int(tenant_id), raw=raw)

    def _load_entry(self, *, key: OffsetKey, tenant_id: int, raw: bytes | None = None) -> _Entry:
        """加载一个位点文件（缺失按 0；损坏按 `OFFSET_REASON_*` 处置）。

        Args:
            key: 位点键。
            tenant_id: 租户主键。
            raw: 已读出的文件字节（避免重复读盘）。

        Returns:
            _Entry: 位点状态（可能标为不可用）。
        """
        group, topic_id, partition_no = key
        path = self._path(tenant_id=int(tenant_id), group=group, topic_id=int(topic_id), partition_no=int(partition_no))
        entry = _Entry(
            tenant_id=int(tenant_id),
            group=group,
            topic_id=int(topic_id),
            partition_no=int(partition_no),
            path=path,
        )
        if raw is None:
            try:
                raw = path.read_bytes()
            except FileNotFoundError:
                _LOGGER.info("msg_offset_absent", group=group, topic_id=int(topic_id), partition_no=int(partition_no))
                self._entries[key] = entry
                return entry
            except OSError as exc:
                entry.unavailable = f"read:{exc}"
                self._entries[key] = entry
                return entry
        readout = decode_offset_file(raw, group=group, topic_id=int(topic_id), partition_no=int(partition_no))
        if readout.persisted is not None:
            entry.current = readout.persisted.offset
            entry.persisted = readout.persisted.offset
            entry.seq = readout.persisted.seq
            entry.updated_at_us = readout.persisted.updated_at_us
            entry.previous = readout.previous.offset if readout.previous is not None else None
        if readout.reason not in _USABLE_REASONS:
            entry.unavailable = readout.reason
        elif readout.reason == OFFSET_REASON_PARTIAL:
            _LOGGER.warning(
                "msg_offset_slot_recovered",
                group=group,
                topic_id=int(topic_id),
                partition_no=int(partition_no),
                seq_used=entry.seq,
            )
        self._entries[key] = entry
        return entry

    def _path(self, *, tenant_id: int, group: str, topic_id: int, partition_no: int) -> Path:
        """算一个位点文件的路径（不创建目录）。

        Args:
            tenant_id: 租户主键。
            group: 消费组。
            topic_id: 主题主键。
            partition_no: 分区号。

        Returns:
            Path: 位点文件路径。
        """
        return (
            self._root
            / OFFSETS_SUBDIR
            / str(int(tenant_id))
            / group_directory_name(group)
            / f"{int(topic_id)}-{int(partition_no)}{OFFSET_FILE_SUFFIX}"
        )

    def _ensure_available(self, entry: _Entry, *, quiet: bool = False) -> None:
        """位点不可用时按 fail-closed 处置（不归零、不跳 LEO）。

        Args:
            entry: 位点状态。
            quiet: True 表示只跳过不抛错（`realign` / `list_group` 的扫描路径）。

        Raises:
            OffsetUnavailableError: 位点文件不可用（`quiet=False` 时）。
        """
        if entry.unavailable is None:
            return
        _LOGGER.warning(
            "msg_offset_unavailable",
            group=entry.group,
            topic_id=entry.topic_id,
            partition_no=entry.partition_no,
            reason=entry.unavailable,
        )
        if quiet:
            return
        raise OffsetUnavailableError(
            f"位点文件不可用（{entry.unavailable}）：group={entry.group} "
            f"topic_id={entry.topic_id} partition={entry.partition_no}（需人工重置，不自动归零）"
        )

    async def _judge_ceiling(self, entry: _Entry, offset: int) -> None:
        """判越界（`offset > LEO` 即违约）。

        Args:
            entry: 位点状态。
            offset: 待写入的位点。

        Raises:
            ParamError: 位点超过 LEO（`10001`）。
        """
        ceiling = self._ceiling
        if ceiling is None:
            if not self._ceiling_warned:
                self._ceiling_warned = True
                _LOGGER.warning("msg_offset_ceiling_unbound")
            return
        leo = await ceiling.leo(topic_id=entry.topic_id, partition_no=entry.partition_no)
        if offset > leo:
            raise ParamError(f"位点越界：{offset} > LEO {leo}")

    def _dirty_count(self) -> int:
        """脏条目数（刷盘触发判据）。

        Returns:
            int: 脏条目数。
        """
        return sum(1 for entry in self._entries.values() if entry.dirty)

    # ------------------------------------------------------------------
    # 内部：审计
    # ------------------------------------------------------------------

    async def _emit_reset_audit(
        self,
        *,
        entry: _Entry,
        previous: int,
        current: int,
        actor_id: int | None,
        ip: str | None,
        trace_id: str | None,
    ) -> None:
        """写一条重置审计（失败即抛错：没留痕就不该生效）。

        Args:
            entry: 位点状态。
            previous: 重置前的位点。
            current: 重置后的位点。
            actor_id: 操作人主键。
            ip: 来源 IP。
            trace_id: 链路标识。

        Raises:
            OffsetAuditError: 审计写入失败或被拒。
        """
        params = json.dumps(
            {
                "group": entry.group,
                "topic_id": entry.topic_id,
                "partition_no": entry.partition_no,
                "previous": previous,
                "current": current,
            },
            ensure_ascii=False,
        )
        await self._emit_audit(
            action=OFFSET_RESET_ACTION,
            path=OFFSET_RESET_PATH,
            tenant_id=entry.tenant_id,
            params=params,
            actor_id=actor_id,
            ip=ip,
            trace_id=trace_id,
            record_ref=RecordRef(type="msg.offset", id=f"{int(entry.topic_id)}:{entry.partition_no}"),
        )

    async def _emit_purge_audit(self, *, scope: str, tenant_id: int, key: str, files: int) -> None:
        """写一条清理审计（失败即抛错，与重置同口径）。

        Args:
            scope: `group` / `topic`。
            tenant_id: 租户主键。
            key: 组名或主题主键。
            files: 删除的文件数。

        Raises:
            OffsetAuditError: 审计写入失败或被拒。
        """
        params = json.dumps({"scope": scope, "key": key, "files": files}, ensure_ascii=False)
        await self._emit_audit(
            action=OFFSET_PURGE_ACTION,
            path=OFFSET_PURGE_PATH,
            tenant_id=int(tenant_id),
            params=params,
            actor_id=None,
            ip=None,
            trace_id=None,
        )

    async def _emit_audit(
        self,
        *,
        action: str,
        path: str,
        tenant_id: int | None,
        params: str,
        actor_id: int | None,
        ip: str | None,
        trace_id: str | None,
        record_ref: RecordRef | None = None,
    ) -> None:
        """发射一条操作审计（复用 `sys.operation.logged`）。

        Args:
            action: 审计动作。
            path: 审计路径。
            tenant_id: 租户主键。
            params: JSON 载荷。
            actor_id: 操作人主键。
            ip: 来源 IP。
            trace_id: 链路标识。
            record_ref: 记录级定位（涉及具体记录的操作才给；07D 任务 03 附录 D）。

        Raises:
            OffsetAuditError: 审计写入失败或被拒。
        """
        event = build_operation_event(
            tenant_id=None if tenant_id is None else str(int(tenant_id)),
            user_id=actor_id,
            username=None,
            module="msg",
            action=action,
            method="POST",
            path=path,
            query=None,
            params=params,
            ip=ip,
            user_agent=None,
            cost_time=None,
            status=200,
            error_msg=None,
            trace_id=trace_id,
            record_ref=record_ref,
        )
        try:
            # 记录级操作按强审计写入（07D 任务 03 附录 D）；失败一律转成拒绝执行。
            accepted = await self._audit.emit(event, required=True)
        except Exception as exc:  # 审计出口异常一律转成「拒绝执行」
            _LOGGER.warning("msg_offset_audit_failed", action=action, error=repr(exc))
            raise OffsetAuditError(f"审计写入失败：{action}") from exc
        if not accepted:
            _LOGGER.warning("msg_offset_audit_rejected", action=action)
            raise OffsetAuditError(f"审计写入被拒绝：{action}")
