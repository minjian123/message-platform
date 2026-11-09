"""消息平台用量列清单（05-1）：端点、种子与测试共用的唯一口径。

四列固定为「主题数 / 分区数 / 消息存储 / 今日投递量」，与 `msg_tenant_usage` 的
`allowlist` 四键同名闭合；限额 / 状态 / 预警不属于本清单，归 05-2 配额任务。

`field_type` 取平台字段类型语义键 `number`（见 bms《数据库开发规范》与字段类型注册表）；
`sort` 为 1 起的展示顺序，种子写入 `sys_field.sort`，端点按同一顺序输出 `columns`。
"""

from dataclasses import dataclass
from typing import cast

from bms_core.core.concurrent import ConcurrentStableDict, ConcurrentStableFrozenList, ConcurrentStableList

__all__ = ["USAGE_COLUMNS", "UsageColumn", "column_payloads", "usage_row"]


@dataclass(frozen=True)
class UsageColumn:
    """用量投影的一列（不可变）。

    Attributes:
        key: 列键（＝ `sys_field.field_key` ＝ `allowlist` 字段名）。
        label: 展示名（＝ `sys_field.name`）。
        field_type: 平台字段类型语义键（本期统一 `number`）。
        sort: 展示顺序（1 起，升序）。
    """

    key: str
    label: str
    field_type: str = "number"
    sort: int = 0


USAGE_COLUMNS: ConcurrentStableFrozenList[UsageColumn] = ConcurrentStableFrozenList(
    (
        UsageColumn(key="topic_count", label="主题数", field_type="number", sort=1),
        UsageColumn(key="partition_count", label="分区数", field_type="number", sort=2),
        UsageColumn(key="storage_bytes", label="消息存储", field_type="number", sort=3),
        UsageColumn(key="delivered_today", label="今日投递量", field_type="number", sort=4),
    )
)
"""用量投影四列（固定顺序；与 `msg_tenant_usage` 的 `allowlist` 四键闭合）。"""


def column_payloads() -> ConcurrentStableList[ConcurrentStableDict[str, str]]:
    """端点 `columns` 字段的载荷（只暴露键与展示名）。

    Returns:
        ConcurrentStableList[ConcurrentStableDict[str, str]]: `[{key, label}, ...]`，按
        `USAGE_COLUMNS` 顺序；调用方在 JSON 边界再转普通 `dict`。
    """
    return ConcurrentStableList(
        ConcurrentStableDict({"key": column.key, "label": column.label}) for column in USAGE_COLUMNS
    )


def usage_row(snapshot: object) -> ConcurrentStableDict[str, int | None]:
    """把用量快照装配成一行四列值（键序与 `USAGE_COLUMNS` 一致）。

    这是 05-1 投影端点与 06-8 管理面端点**共用的唯一装配**：不重算、不另起口径，
    快照缺该属性时该列为 `None`（与「不可用」语义一致）。

    Args:
        snapshot: 用量快照（`UsageSnapshot`）。

    Returns:
        ConcurrentStableDict[str, int | None]: 列键 → 值。
    """
    values: ConcurrentStableDict[str, int | None] = ConcurrentStableDict()
    for column in USAGE_COLUMNS:
        values[column.key] = cast("int | None", getattr(snapshot, column.key, None))
    return values
