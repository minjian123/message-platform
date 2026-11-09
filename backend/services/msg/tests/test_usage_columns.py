"""06-8 用量列装配与契约闭合（Kiwi 2471）。

覆盖：`usage_row()` 键序与 `USAGE_COLUMNS` 闭合、缺失属性回落 `None`、
`TenantUsageRow` 字段集合等于列键集合（「列元数据与行同源」的机器判据）。
"""

from types import SimpleNamespace

import pytest

from bms_msg.schemas.usage import TenantUsageRow
from bms_msg.usage.columns import USAGE_COLUMNS, column_payloads, usage_row

pytestmark = pytest.mark.kiwi_id(2471)


def test_usage_row_keys_follow_columns() -> None:
    """行键序与 `USAGE_COLUMNS` 顺序一致（列展示顺序即取值顺序）。"""
    snapshot = SimpleNamespace(
        topic_count=1,
        partition_count=2,
        storage_bytes=3,
        delivered_today=4,
        group_count=5,
        unacked_count=6,
        connection_count=7,
    )
    row = usage_row(snapshot)
    assert list(row) == [column.key for column in USAGE_COLUMNS]
    assert list(row.values()) == [1, 2, 3, 4]


def test_usage_row_missing_attribute_is_none() -> None:
    """快照缺该属性时该列为 None（「不可用」语义，不写 0）。"""
    row = usage_row(SimpleNamespace(topic_count=1))
    assert row["topic_count"] == 1
    assert row["partition_count"] is None
    assert row["storage_bytes"] is None
    assert row["delivered_today"] is None


def test_contract_fields_match_column_keys() -> None:
    """契约行字段集合等于列键集合（新增列必须同时改两处，否则此处失败）。"""
    assert set(TenantUsageRow.model_fields) == {column.key for column in USAGE_COLUMNS}


def test_column_payloads_are_key_and_label_only() -> None:
    """列元数据只暴露键与展示名（内部 `field_type` / `sort` 不外泄）。"""
    payloads = column_payloads()
    assert [dict(item) for item in payloads] == [{"key": column.key, "label": column.label} for column in USAGE_COLUMNS]
