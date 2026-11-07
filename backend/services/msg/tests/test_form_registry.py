"""05-1 `form_key` 自持清单与应用级视图（Kiwi 2452）。

覆盖：`msg_usage` 记录字段；`form_records()` / `form_record()` / `form_keys()`；
应用工厂覆写后 `app.state.form_registry` 含 `msg_usage`；清单自校验通过。
"""

import pytest
from bms_core.core.plugin import default_plugin_registry
from bms_core.registry.forms import FormKind, FormRegistry, FormStatus

from services.form_registry import MSG_FORM_RECORDS, form_keys, form_record, form_records

pytestmark = pytest.mark.kiwi_id(2452)


def test_form_records_contains_msg_usage() -> None:
    """清单只有一条 `msg_usage`，类型 / 状态 / 数据源与声明闭合。"""
    assert len(MSG_FORM_RECORDS) == 1
    record = MSG_FORM_RECORDS[0]
    assert record.form_key == "msg_usage"
    assert record.table == "msg_usage"
    assert record.name == "消息平台用量"
    assert record.kind == FormKind.FORM.value
    assert record.status == FormStatus.ENABLED.value
    assert record.data_source_key == "msg_tenant_usage"


def test_form_helpers() -> None:
    """`form_records()` 插入序副本、`form_record(table)` 命中 / 未命中、`form_keys()` 清单。"""
    records = form_records()
    assert list(records) == list(MSG_FORM_RECORDS)
    assert form_record("msg_usage") is MSG_FORM_RECORDS[0]
    assert form_record("not_exists") is None
    assert form_keys() == ("msg_usage",)


def test_form_registry_validates() -> None:
    """清单自校验通过（标识 / 表名格式、唯一、名称、类型、数据源回填）。"""
    assert list(FormRegistry(form_records()).validate()) == []


def test_application_injects_form_record() -> None:
    """应用装配后 `app.state.form_registry` 含 `msg_usage`（产品注入通道）。"""
    from bms_msg.main import ApplicationFactory

    default_plugin_registry().reset()
    app = ApplicationFactory().create(None)
    record = app.state.form_registry.get("msg_usage")
    assert record is not None
    assert record.data_source_key == "msg_tenant_usage"
    assert [item.form_key for item in app.state.form_injected_records] == ["msg_usage"]


def test_factory_form_records_hook() -> None:
    """应用工厂 `form_records()` 钩子直接返回本产品清单。"""
    from bms_msg.main import ApplicationFactory

    assert [record.form_key for record in ApplicationFactory().form_records()] == ["msg_usage"]
