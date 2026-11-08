"""msg 产品表归属自持清单（与 `bms_core.services.table_registry.TableRecord` 同结构）。

- **产品自持**：本清单是 msg 侧表归属登记的唯一出处（与 msg《数据库设计》总览「已设计数据表登记」
  及各表文件一致）；平台侧 `TABLE_OWNERSHIP` 不登记产品表。
- **装载期注册**（基座 12_04 注入通道）：经 `register()` 把本清单注册进基座表归属注册表，合并视图
  （`table_ownership_view()`）供迁移清单表集派生（`series_tables`）、开发库自动建表与归属校验使用。
- **归属业务域（07C）**：业务域 `msg` 由 `deploy/registry/msg.toml` 的 `[[domains]]` 走**装载注册**
  （`register_domain_services`）；表归属的 `domain` 与该声明同值，承载服务与 `owner` 一致。
- **数据源**：消息本体不入库（文件日志），元数据表全部落 `platform`（平台库、跨租户按 `tenant_id` 列区分）。
- **同值去重、异值即拒**：与平台侧同 `table_name` 同值去重；任一字段差异即被归属校验判为
  「表名重复登记」而拒（fail-closed）。
"""

from bms_core.core.concurrent import ConcurrentStableList
from bms_core.services.table_registry import Datasource, TableRecord, register_table_records

MSG_DOMAIN = "msg"
"""msg 归属业务域（与清单 `[[domains]]` 的 `domain_key` 同值）。"""

MSG_TABLE_RECORDS: tuple[TableRecord, ...] = (
    TableRecord(
        table_name="msg_topic",
        owner="msg",
        domain=MSG_DOMAIN,
        datasource=Datasource.PLATFORM,
        note="主题定义（分区数 / 保留期 / 清理策略 / 状态）",
    ),
    TableRecord(
        table_name="msg_partition",
        owner="msg",
        domain=MSG_DOMAIN,
        datasource=Datasource.PLATFORM,
        note="分区定义与检查点（leader / 副本 / LEO / HW / epoch）",
    ),
    TableRecord(
        table_name="msg_replay_task",
        owner="msg",
        domain=MSG_DOMAIN,
        datasource=Datasource.PLATFORM,
        note="重放任务（范围 / 限速 / 进度 / 游标 / 终态）",
    ),
    TableRecord(
        table_name="msg_tenant_lifecycle",
        owner="msg",
        domain=MSG_DOMAIN,
        datasource=Datasource.PLATFORM,
        note="租户生命周期动作账本（版本戳 / 删除保留期 / 清理进度）",
    ),
    TableRecord(
        table_name="msg_quota",
        owner="msg",
        domain=MSG_DOMAIN,
        datasource=Datasource.PLATFORM,
        note="消息维度配额限额（六维 / 软硬限 / 作用域 / 来源；不存用量）",
    ),
)
"""msg 元数据四表归属记录（业务域 `msg`，平台库，迁移清单 `msg:platform`）。"""


def register() -> None:
    """把本产品表归属记录注册进基座（**装载期**调用；同值重复登记无操作，幂等）。

    ops / alembic 等**不经应用装配**的装载入口须显式调用本函数；应用装配路径由基座的
    `table_records()` 钩子在 `create()` 内自动注册（`bms_msg.main.ApplicationFactory`）。
    """
    register_table_records(ConcurrentStableList(MSG_TABLE_RECORDS))


def table_names() -> tuple[str, ...]:
    """本产品自持的表名清单（插入序；供用例与运维核对）。

    Returns:
        tuple[str, ...]: 表名元组。
    """
    return tuple(record.table_name for record in MSG_TABLE_RECORDS)
