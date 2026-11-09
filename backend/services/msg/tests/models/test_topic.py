"""主题 / 分区模型（msg 任务 01-1；Kiwi 2412）。

覆盖：表名与索引 / 唯一约束命名、缺省值（状态 / 清理策略 / 副本 / 检查点）、雪花列声明、
唯一约束生效、`MODEL_MODULES` 可解析且表已登记进 `Base.metadata`。
"""

import pytest
from bms_core.models.base import Base, SnowflakeId
from sqlalchemy import Table
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from bms_msg.models import MODEL_MODULES, MsgPartition, MsgTopic
from bms_msg.models.topic import CLEANUP_DELETE, STATUS_ACTIVE

pytestmark = pytest.mark.kiwi_id(2412)


def _table(name: str) -> Table:
    """按表名取元数据中的 `Table`（`metadata.tables` 带类型，避免 `__table__` 的 `FromClause`）。

    Args:
        name: 表名。

    Returns:
        Table: 表对象。
    """
    return Base.metadata.tables[name]


def test_model_modules_cover_all_model_files() -> None:
    """`MODEL_MODULES` 与本模块导出的模型一一对应（漏登记会让迁移链与自动建表漏表）。"""
    assert tuple(MODEL_MODULES) == (
        "bms_msg.models.topic",
        "bms_msg.models.partition",
        "bms_msg.models.quota",
        "bms_msg.models.replay",
        "bms_msg.models.tenant_lifecycle",
        "bms_msg.models.topic_acl",
        "bms_msg.models.topic_cleanup",
        "bms_msg.models.dead_letter",
    )
    assert {MsgTopic.__tablename__, MsgPartition.__tablename__} <= set(Base.metadata.tables)


def test_table_names_and_constraints() -> None:
    """表名与索引 / 唯一约束命名符合《命名规范》。"""
    assert MsgTopic.__tablename__ == "msg_topic"
    assert MsgPartition.__tablename__ == "msg_partition"
    assert {constraint.name for constraint in _table("msg_topic").constraints if constraint.name} >= {
        "uq_msg_topic_identity"
    }
    assert {constraint.name for constraint in _table("msg_partition").constraints if constraint.name} >= {
        "uq_msg_partition_no"
    }
    assert {index.name for index in _table("msg_topic").indexes} >= {
        "idx_msg_topic_status",
        "idx_msg_topic_deleted_at",
    }
    assert {index.name for index in _table("msg_partition").indexes} >= {
        "idx_msg_partition_topic",
        "idx_msg_partition_deleted_at",
    }


def test_snowflake_columns_are_declared() -> None:
    """`tenant_id` / `topic_id` 用雪花列类型（与基座租户主键同型，仓储租户条件可直接比较）。"""
    assert isinstance(_table("msg_topic").c["tenant_id"].type, SnowflakeId)
    assert isinstance(_table("msg_partition").c["topic_id"].type, SnowflakeId)


async def test_defaults_applied_on_insert(db_session: AsyncSession) -> None:
    """缺省值：状态 `active`、清理策略 `delete`、副本空列表、检查点与任期 0。"""
    topic = MsgTopic(tenant_id=7001, namespace="msg", name="audit.operation", partition_count=1, retention_ms=1000)
    db_session.add(topic)
    await db_session.flush()
    partition = MsgPartition(topic_id=int(topic.id), partition_no=0, leader="msg-0")
    db_session.add(partition)
    await db_session.flush()
    assert (topic.status, topic.cleanup_policy, topic.deleted_at) == (STATUS_ACTIVE, CLEANUP_DELETE, None)
    assert (list(partition.replicas), partition.checkpoint_leo, partition.checkpoint_hw, partition.epoch) == (
        [],
        0,
        0,
        0,
    )


async def test_identity_unique_constraint_rejects_duplicate(db_session: AsyncSession) -> None:
    """同租户 + 命名空间 + 名称重复插入：唯一约束生效。"""
    for _ in range(2):
        db_session.add(
            MsgTopic(
                tenant_id=7001,
                namespace="msg",
                name="audit.operation",
                partition_count=1,
                retention_ms=1000,
            )
        )
    with pytest.raises(IntegrityError):
        await db_session.flush()
    await db_session.rollback()


async def test_partition_number_unique_per_topic(db_session: AsyncSession) -> None:
    """同主题下分区号重复插入：复合唯一生效。"""
    topic = MsgTopic(tenant_id=7001, namespace="msg", name="dup.partition", partition_count=1, retention_ms=1000)
    db_session.add(topic)
    await db_session.flush()
    for _ in range(2):
        db_session.add(MsgPartition(topic_id=int(topic.id), partition_no=0, leader="msg-0"))
    with pytest.raises(IntegrityError):
        await db_session.flush()
    await db_session.rollback()
