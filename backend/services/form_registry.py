"""msg 产品 **`form_key` 自持清单**（与 `bms_core.registry.forms.FormRecord` 同结构）。

- **产品自持**：本清单是 msg 侧表单标识登记的唯一出处；平台侧 `FORM_CATALOG` 不登记产品表，
  避免产品值写进基座清单。
- **装载期注入**（基座装配钩子）：应用工厂覆写 `form_records()` 返回本清单，基座经
  `merge_form_records(injected=...)` 与 `FORM_CATALOG` 拼接为**应用级视图**
  （`app.state.form_registry`）；无进程级注册表、不改常量。
- **跨服务可见性**：`GET /api/v1/form-keys` 由 bms platform 服务提供，产品注入记录只进
  产品自身应用视图；平台侧聚合产品表单需 bms「产品清单下发 / 聚合」通道（mdm 已登记同项）。
  本任务只保证 msg 应用级视图可见，缺口回 bms 登记，不在 msg 挂平台路由。
- **取值口径**：`form_key` 与 `table` 同名 `msg_usage`（只读投影无物理表，`table` 仅作逻辑
  表单标识）；`data_source_key` 与 `deploy/registry/msg.toml` 的 `msg_tenant_usage` 闭合。
"""

from bms_core.core.concurrent import ConcurrentStableList
from bms_core.registry.forms import FormKind, FormRecord, FormStatus

__all__ = ["MSG_FORM_RECORDS", "form_keys", "form_record", "form_records"]

MSG_FORM_RECORDS: tuple[FormRecord, ...] = (
    FormRecord(
        form_key="msg_usage",
        table="msg_usage",
        name="消息平台用量",
        kind=FormKind.FORM.value,
        status=FormStatus.ENABLED.value,
        data_source_key="msg_tenant_usage",
    ),
)
"""msg 用量投影表单登记（只读；无菜单入口、无按钮、无物理表）。"""


def form_records() -> ConcurrentStableList[FormRecord]:
    """本产品表单标识记录清单（插入序副本；供应用装配钩子与用例消费）。

    Returns:
        ConcurrentStableList[FormRecord]: 表单标识记录（插入序）。
    """
    return ConcurrentStableList(MSG_FORM_RECORDS)


def form_record(table: str) -> FormRecord | None:
    """按物理表名取登记记录（供校验脚本与运维核对）。

    Args:
        table: 物理表名（如 `msg_usage`）。

    Returns:
        FormRecord | None: 命中记录；未登记返回 `None`。
    """
    for record in MSG_FORM_RECORDS:
        if record.table == table:
            return record
    return None


def form_keys() -> tuple[str, ...]:
    """本产品表单标识清单（插入序；供校验脚本与用例断言）。

    Returns:
        tuple[str, ...]: 表单标识元组。
    """
    return tuple(record.form_key for record in MSG_FORM_RECORDS)
