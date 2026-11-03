"""msg 产品表归属自持清单（与 `bms_core.services.table_registry.TableRecord` 同结构）。

8B_00 设计阶段表尚未定稿：消息 / 主题 / 消费组 / offset / 死信等表随《8B_00 总体设计》
与数据库设计节点登记后补入；当前为空清单，避免提前占表名。
"""

from bms_core.services.table_registry import TableRecord, register_table_records

MSG_TABLE_RECORDS: tuple[TableRecord, ...] = ()
"""msg 产品表归属记录（8B_00 阶段为空；设计定稿后补）。"""


def register() -> None:
    """把本产品表归属记录注册进基座（装载期调用；空清单幂等）。"""
    register_table_records(MSG_TABLE_RECORDS)
