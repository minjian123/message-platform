"""数据完整性核对（02-8）：已提交集合 vs 消费集合，区分「丢已提交」与「未提交尾部」。

**唯一判 `data_loss=true` 的条件**：`offset < 故障前 HW` 的已提交消息缺席；未提交尾部缺席只记
`retained_tail`（不伪造通过，也不把可解释回退当成丢消息）。
"""

from __future__ import annotations

from dataclasses import dataclass, field

from bms_core.core.concurrent import (
    ConcurrentStableDict,
    ConcurrentStableFrozenList,
    ConcurrentStableList,
)
from bms_core.core.objects import BaseValueObject

__all__ = ["DataVerdict", "compare_committed"]


@dataclass(frozen=True)
class DataVerdict(BaseValueObject):
    """一次数据核对结果。"""

    lost: ConcurrentStableFrozenList[str] = field(default_factory=lambda: ConcurrentStableFrozenList[str]())
    """已提交但消费不到的 ID（`offset < HW`）。"""

    retained_tail: ConcurrentStableFrozenList[str] = field(default_factory=lambda: ConcurrentStableFrozenList[str]())
    """未提交尾部缺席的 ID（`offset >= HW`；可解释回退）。"""

    @property
    def data_loss(self) -> bool:
        """是否丢已提交。

        Returns:
            bool: 存在 `offset < HW` 的缺席为 True。
        """
        return bool(self.lost)

    @property
    def ok(self) -> bool:
        """是否通过硬判据（不丢已提交）。

        Returns:
            bool: `data_loss=false` 为 True。
        """
        return not self.lost


def compare_committed(
    *,
    committed: ConcurrentStableDict[str, int],
    consumed: ConcurrentStableFrozenList[str],
    high_watermark: int,
) -> DataVerdict:
    """核对已提交集合与消费集合。

    Args:
        committed: 已确认（`acks=all`）消息 ID → offset。
        consumed: 切换后实际消费到的消息 ID。
        high_watermark: 故障前 leader HW（不含）。

    Returns:
        DataVerdict: 丢失 / 保留尾部。
    """
    seen: ConcurrentStableDict[str, bool] = ConcurrentStableDict()
    for message_id in consumed:
        seen.set(message_id, True)
    lost: ConcurrentStableList[str] = ConcurrentStableList()
    retained: ConcurrentStableList[str] = ConcurrentStableList()
    for message_id, offset in committed.items():
        if message_id in seen:
            continue
        if int(offset) < int(high_watermark):
            lost.add(message_id)
        else:
            retained.add(message_id)
    return DataVerdict(
        lost=ConcurrentStableFrozenList(lost),
        retained_tail=ConcurrentStableFrozenList(retained),
    )
