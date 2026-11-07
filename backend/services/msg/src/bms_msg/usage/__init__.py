"""消息平台用量投影（05-1）：列口径的唯一事实源。

`usage/columns.py` 同时被三处消费：

- 取数端点（`bms_msg.api.admin.quota`）→ `columns[].key/label`；
- 表单种子（`bms_msg.seeds.forms`）→ `sys_field.field_key/name/type/sort`；
- 测试 → 断言端点列与 `sys_field` 完全一致。

列常量不放在 `backend/services/` 顶层：种子模块会在平台进程内被导入，只依赖 `bms_msg`
可避免与 bms 的顶层 `services` 包撞名。
"""

from bms_msg.usage.columns import USAGE_COLUMNS, UsageColumn, column_payloads

__all__ = ["USAGE_COLUMNS", "UsageColumn", "column_payloads"]
