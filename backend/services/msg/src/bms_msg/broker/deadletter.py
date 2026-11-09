"""死信队列端口与内存实现（详细设计 §4.3、§6.3）。

- **转入时机**：① 投递次数达 `MAX_DELIVERY_ATTEMPTS` 后仍在超时重投（broker 判定，`delivery_exhausted`）；
  ② 消费端校验失败或业务拒绝后经 `nack` 上报（`schema_invalid` / `processing_failed`，03-3）。两条路径
  共用「转死信 + 位点前移」，只是触发方不同。
- **不删日志**：死信只在投递侧「不再投」，offset 仍可读——重放（01-6）与排障需要原始记录。
- **通道**：06 域的死信查询 / 处置直接消费本队列（`list_all` / `contains`），不需另建数据通道。
- 8B_02 落盘时随消息存储一并持久化，端口签名不变。
"""

from abc import ABC, abstractmethod
from collections.abc import Callable, Iterable
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import cast

from bms_core.core.concurrent import ConcurrentStableDict, ConcurrentStableList, ConcurrentStableSet
from bms_core.core.logging import get_logger
from bms_core.core.objects import BaseFrameworkObject, BaseValueObject
from bms_core.db.session import DbSession
from bms_core.db.unit_of_work import UnitOfWork

from bms_msg.models.dead_letter import DEFAULT_DLQ_STATUS, MsgDeadLetter
from bms_msg.repositories.dead_letter import DeadLetterRepository

__all__ = [
    "DEAD_LETTER_EXHAUSTED",
    "DatabaseDeadLetterQueue",
    "DeadLetter",
    "DeadLetterQueue",
    "DeadLetterReason",
    "InMemoryDeadLetterQueue",
]


class DeadLetterReason(StrEnum):
    """转入死信的原因（登记枚举：06 域按原因筛选与展示，不靠字符串匹配）。"""

    DELIVERY_EXHAUSTED = "delivery_exhausted"
    """重投次数超限（broker 判定；外部不得通过 `nack` 指定）。"""

    SCHEMA_INVALID = "schema_invalid"
    """消息体不符合 schema（消费端校验失败后经 `nack` 上报）。"""

    PROCESSING_FAILED = "processing_failed"
    """业务处理失败且确认不可重试（消费端经 `nack` 上报）。"""


DEAD_LETTER_EXHAUSTED = DeadLetterReason.DELIVERY_EXHAUSTED
"""转入原因：重投次数超限（既有调用点的常量别名）。"""

DeadLetterKey = tuple[int, int, int]
"""死信定位键：`(topic_id, partition_no, offset)`。"""


@dataclass(frozen=True)
class DeadLetter(BaseValueObject):
    """一条死信记录（定位三元组 + 失败上下文）。"""

    message_id: str
    """原消息 ID（重放时不变）。"""

    topic_id: int
    """主题主键。"""

    partition_no: int
    """分区号。"""

    offset: int
    """分区内序号。"""

    attempts: int
    """累计投递次数（达上限值；消费端上报时为首投次数）。"""

    failed_at: datetime
    """转入死信时间（UTC）。"""

    reason: DeadLetterReason
    """转入原因（枚举见 `DeadLetterReason`）。"""

    detail: ConcurrentStableDict[str, str] = field(default_factory=lambda: ConcurrentStableDict[str, str]())
    """失败详情（字段名 → 说明；消费端上报时原样保存，broker 判定时为空）。"""

    tenant_id: int | None = None
    """租户主键（持久化必填；旧调用点未传时为 None，由持久化实现拒绝）。"""

    namespace: str | None = None
    """命名空间（持久化必填；取自主题归属）。"""

    topic: str | None = None
    """主题名快照（持久化必填；旧调用点未传时回落主题主键字符串）。"""

    log_id: str | None = None
    """原记录 ID（记录级定位 / 审计 `record_ref`）。"""

    trace_id: str | None = None
    """链路标识（取自消息头；可空）。"""

    status: str = DEFAULT_DLQ_STATUS
    """处置状态（持久化实现返回真实状态；内存实现缺省待处置）。"""


class DeadLetterQueue(BaseFrameworkObject, ABC):
    """死信队列端口：转入 / 全量列出 / 判定是否已死信。"""

    @abstractmethod
    async def add(self, letter: DeadLetter) -> None:
        """转入一条死信（同定位三元组重复转入幂等）。

        Args:
            letter: 死信记录。
        """

    @abstractmethod
    async def list_all(self) -> ConcurrentStableList[DeadLetter]:
        """列出全部死信（插入序）。

        Returns:
            ConcurrentStableList[DeadLetter]: 死信列表。
        """

    @abstractmethod
    async def contains(self, *, topic_id: int, partition_no: int, offset: int) -> bool:
        """判定某条消息是否已死信。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。
            offset: 分区内序号。

        Returns:
            bool: 已死信返回 True。
        """

    @abstractmethod
    async def purge_topics(self, *, topic_ids: Iterable[int]) -> int:
        """删除指定主题的全部死信（租户删除时的易失资源清理）。

        Args:
            topic_ids: 主题主键集合。

        Returns:
            int: 删除的死信数。
        """


class InMemoryDeadLetterQueue(DeadLetterQueue):
    """内存死信队列（首期实现；06 域查询端点直接消费）。"""

    def __init__(self) -> None:
        """初始化（空队列）。"""
        self._letters: ConcurrentStableList[DeadLetter] = ConcurrentStableList()
        self._keys: ConcurrentStableSet[DeadLetterKey] = ConcurrentStableSet()

    async def add(self, letter: DeadLetter) -> None:
        """转入一条死信（同三元组重复转入幂等）。

        Args:
            letter: 死信记录。
        """
        key = (letter.topic_id, letter.partition_no, letter.offset)
        if key in self._keys:
            return
        self._keys.add(key)
        self._letters.add(letter)

    async def list_all(self) -> ConcurrentStableList[DeadLetter]:
        """列出全部死信（插入序）。

        Returns:
            ConcurrentStableList[DeadLetter]: 死信列表。
        """
        return ConcurrentStableList(self._letters)

    async def contains(self, *, topic_id: int, partition_no: int, offset: int) -> bool:
        """判定某条消息是否已死信。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。
            offset: 分区内序号。

        Returns:
            bool: 已死信返回 True。
        """
        return (topic_id, partition_no, offset) in self._keys

    async def purge_topics(self, *, topic_ids: Iterable[int]) -> int:
        """删除指定主题的全部死信（幂等）。

        Args:
            topic_ids: 主题主键集合。

        Returns:
            int: 删除的死信数。
        """
        targets = ConcurrentStableSet([int(item) for item in topic_ids])
        if not targets:
            return 0
        kept = [letter for letter in self._letters if letter.topic_id not in targets]
        removed = len(self._letters) - len(kept)
        if removed == 0:
            return 0
        self._letters = ConcurrentStableList(kept)
        self._keys = ConcurrentStableSet((letter.topic_id, letter.partition_no, letter.offset) for letter in kept)
        return removed


_LOGGER = get_logger("bms_msg.dead_letter")

UowFactory = Callable[[], AbstractAsyncContextManager[UnitOfWork]]
"""平台库工作单元工厂（队列自开短事务；用例可注入内存库会话）。"""


class DatabaseDeadLetterQueue(DeadLetterQueue):
    """持久化死信队列（`msg_dead_letter`）：端口签名不变，重启不丢。

    口径（详细设计 06-5-1 §5）：

    - `add` / `purge_topics` 自持平台库短事务，写库成功后才更新内存索引；
    - `contains` 只查内存索引（启动加载 + 写时同步），投递路径不打库；
    - `list_all` 查库全量并把三元组回填索引，供指标 / 管理面使用；
    - 同一三元组重复转入由唯一约束 + 内存索引双重幂等。
    """

    def __init__(self, *, uow_factory: UowFactory) -> None:
        """初始化（空索引；启动后由 `load()` 恢复）。

        Args:
            uow_factory: 平台库工作单元工厂。
        """
        self._uow_factory = uow_factory
        self._keys: ConcurrentStableSet[DeadLetterKey] = ConcurrentStableSet()

    async def load(self) -> None:
        """从表恢复全部定位三元组（含 `archived`）。

        Raises:
            Exception: 表不可读 / 库不可达时上抛（死信判定属投递正确性，空索引会重复投递）。
        """
        async with self._uow_factory() as uow:
            repository = DeadLetterRepository(cast("DbSession", uow.session))
            keys = await repository.list_keys()
        self._keys = ConcurrentStableSet(keys)
        _LOGGER.info("msg_dead_letter_index_loaded", count=len(self._keys))

    async def add(self, letter: DeadLetter) -> None:
        """转入一条死信（同三元组幂等）。

        Args:
            letter: 死信记录。

        Raises:
            ValueError: 缺少租户主键（持久化必填）。
        """
        key: DeadLetterKey = (letter.topic_id, letter.partition_no, letter.offset)
        if key in self._keys:
            return
        if letter.tenant_id is None:
            raise ValueError("死信持久化缺少 tenant_id")
        row = MsgDeadLetter(
            tenant_id=int(letter.tenant_id),
            namespace=letter.namespace or "",
            topic_id=letter.topic_id,
            topic=letter.topic or str(letter.topic_id),
            partition_no=letter.partition_no,
            msg_offset=letter.offset,
            message_id=letter.message_id,
            log_id=letter.log_id or "",
            trace_id=letter.trace_id,
            reason=str(letter.reason),
            attempts=letter.attempts,
            detail=letter.detail,
            status=letter.status,
            failed_at=letter.failed_at,
        )
        async with self._uow_factory() as uow, uow.begin():
            session = cast("DbSession", uow.session)
            repository = DeadLetterRepository(session)
            exists = await repository.exists_key(
                topic_id=letter.topic_id, partition_no=letter.partition_no, offset=letter.offset
            )
            if exists:
                return
            session.add(row)
        self._keys.add(key)

    async def list_all(self) -> ConcurrentStableList[DeadLetter]:
        """列出全部死信（按 `failed_at`、`id` 升序）。

        Returns:
            ConcurrentStableList[DeadLetter]: 死信列表（含状态与处置字段）。
        """
        async with self._uow_factory() as uow:
            repository = DeadLetterRepository(cast("DbSession", uow.session))
            rows = await repository.list_all()
        for row in rows:
            self._keys.add((int(row.topic_id), int(row.partition_no), int(row.msg_offset)))
        return ConcurrentStableList(_to_letter(row) for row in rows)

    async def contains(self, *, topic_id: int, partition_no: int, offset: int) -> bool:
        """判定某条消息是否已死信（内存索引）。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。
            offset: 分区内序号。

        Returns:
            bool: 已死信返回 True。
        """
        return (topic_id, partition_no, offset) in self._keys

    async def purge_topics(self, *, topic_ids: Iterable[int]) -> int:
        """按主题物理删除死信（幂等）。

        Args:
            topic_ids: 主题主键集合。

        Returns:
            int: 删除行数。
        """
        targets = ConcurrentStableSet([int(item) for item in topic_ids])
        if not targets:
            return 0
        async with self._uow_factory() as uow, uow.begin():
            repository = DeadLetterRepository(cast("DbSession", uow.session))
            removed = await repository.purge_topics(topic_ids=targets)
        if removed:
            self._keys = ConcurrentStableSet(key for key in self._keys if key[0] not in targets)
        return removed


def _to_letter(row: MsgDeadLetter) -> DeadLetter:
    """行 → 端口值对象。

    Args:
        row: 死信行。

    Returns:
        DeadLetter: 端口值对象。
    """
    return DeadLetter(
        message_id=row.message_id,
        topic_id=int(row.topic_id),
        partition_no=int(row.partition_no),
        offset=int(row.msg_offset),
        attempts=int(row.attempts),
        failed_at=row.failed_at,
        reason=DeadLetterReason(row.reason),
        detail=ConcurrentStableDict(row.detail),
        tenant_id=int(row.tenant_id),
        namespace=row.namespace,
        topic=row.topic,
        log_id=row.log_id,
        trace_id=row.trace_id,
        status=row.status,
    )
