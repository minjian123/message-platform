"""主题与分区仓储（`msg_topic` / `msg_partition`）：平台库同库多租户，按 `tenant_id` 列隔离。

- 主题仓储声明 `tenant_scoped`：基座给 `create` 注入 `tenant_id`、给全部查询追加
  `tenant_id = 当前租户`（平台库无库级隔离，这层条件就是隔离本身）；
- 元数据表**不做软删除**（状态列表达），故 `soft_delete_enabled = False`；
- 分区表无 `tenant_id` 列（按 `topic_id` 归属），隔离由主题查询保证；仓储只 `flush` 不 `commit`，
  事务边界在服务层工作单元。
"""

from bms_core.core.concurrent import ConcurrentStableDict, ConcurrentStableList, ConcurrentStableSet
from bms_core.repositories.base_db_repository import BaseDbRepository
from sqlalchemy import ColumnElement, delete, func, select

from bms_msg.models.partition import MsgPartition
from bms_msg.models.topic import STATUS_DELETING, MsgTopic

__all__ = ["PartitionRepository", "TopicRepository"]


class TopicRepository(BaseDbRepository[MsgTopic]):
    """主题仓储：按标识查重、分页列表（含总数）、同事务建主题 + 分区由服务层编排。"""

    model = MsgTopic
    tenant_scoped = True
    soft_delete_enabled = False
    sortable_fields = ConcurrentStableSet({"id", "name", "status", "created_at"})

    async def get_by_identity(self, *, namespace: str, name: str) -> MsgTopic | None:
        """按唯一键（当前租户 + 命名空间 + 名称）取主题。

        Args:
            namespace: 命名空间。
            name: 主题名。

        Returns:
            MsgTopic | None: 主题；不存在返回 None。
        """
        statement = self._select().where(
            self._column("namespace") == namespace,
            self._column("name") == name,
        )
        return (await self._session.execute(statement)).scalar_one_or_none()

    async def list_topics(
        self, *, namespace: str | None, keyword: str | None, offset: int, limit: int
    ) -> ConcurrentStableList[MsgTopic]:
        """分页取主题（不含分区明细；按创建时间倒序、主键兜底稳定序）。

        Args:
            namespace: 命名空间过滤；None 不限制。
            keyword: 名称模糊匹配；None / 空串不限制。
            offset: 跳过条数（由页码换算）。
            limit: 本页条数。

        Returns:
            ConcurrentStableList[MsgTopic]: 当页主题。
        """
        statement = (
            self._select()
            .where(*self._filters(namespace, keyword))
            .order_by(self._column("created_at").desc(), self._column("id").desc())
            .limit(limit)
            .offset(offset)
        )
        return ConcurrentStableList((await self._session.execute(statement)).scalars().all())

    async def count_topics(self, *, namespace: str | None, keyword: str | None) -> int:
        """取符合条件的主题总数（与 `list_topics` 同条件）。

        Args:
            namespace: 命名空间过滤；None 不限制。
            keyword: 名称模糊匹配；None / 空串不限制。

        Returns:
            int: 总数。
        """
        statement = (
            select(func.count()).select_from(self.model).where(*self._scope_where(), *self._filters(namespace, keyword))
        )
        return int((await self._session.execute(statement)).scalar_one())

    async def get_by_id_unscoped(self, topic_id: int) -> MsgTopic | None:
        """按主键取主题（**不带租户条件**；仅供后台指标采样按 `topic_id` 反查名称与租户）。

        后台采样没有请求租户上下文，`tenant_scoped` 仓储的默认查询会追加当前租户条件而查不到；
        本方法刻意绕开作用域，只返回订阅者（指标、审计补全）需要的最小事实。

        Args:
            topic_id: 主题主键（雪花）。

        Returns:
            MsgTopic | None: 主题；不存在返回 None。
        """
        return await self._session.get(MsgTopic, topic_id)

    def _filters(self, namespace: str | None, keyword: str | None) -> tuple[ColumnElement[bool], ...]:
        """构造列表过滤条件（命名空间 / 名称模糊 / 排除清理中）。

        `deleting` 状态的主题**不进列表也不进计数**（06-2 定稿）：用户已发起删除，列表再展示会让人
        以为「删了还在」；配额按计数判定，若不排除会卡住「删了建不了新的」。清理进度只能经详情端点查看。

        Args:
            namespace: 命名空间过滤；None 不限制。
            keyword: 名称模糊匹配；None / 空串不限制。

        Returns:
            tuple[ColumnElement[bool], ...]: 条件元组（非空）。
        """
        conditions: ConcurrentStableList[ColumnElement[bool]] = ConcurrentStableList()
        conditions.add(self._column("status") != STATUS_DELETING)
        if namespace is not None:
            conditions.add(self._column("namespace") == namespace)
        if keyword:
            conditions.add(self._column("name").contains(keyword, autoescape=True))
        return tuple(conditions)


class PartitionRepository(BaseDbRepository[MsgPartition]):
    """分区仓储：按主题取分区、追加分区（只追加不重排）。"""

    model = MsgPartition
    soft_delete_enabled = False
    sortable_fields = ConcurrentStableSet({"partition_no"})

    async def list_by_topic(self, topic_id: int) -> ConcurrentStableList[MsgPartition]:
        """取某主题的全部分区（按分区号升序）。

        Args:
            topic_id: 主题 ID。

        Returns:
            ConcurrentStableList[MsgPartition]: 分区列表。
        """
        statement = self._select().where(self._column("topic_id") == topic_id).order_by(self._column("partition_no"))
        return ConcurrentStableList((await self._session.execute(statement)).scalars().all())

    async def list_assignments(self) -> ConcurrentStableList[MsgPartition]:
        """取全部分区行（按 `topic_id`、`partition_no` 升序；02-10 挂载枚举用）。

        只读、无副作用：成员过滤与「本节点是否承载」的判定留在复制协调器，
        仓储不掺业务判断（避免第二套副本集解析）。

        Returns:
            ConcurrentStableList[MsgPartition]: 分区行（全量）。
        """
        statement = self._select().order_by(self._column("topic_id"), self._column("partition_no"))
        return ConcurrentStableList((await self._session.execute(statement)).scalars().all())

    async def add_partitions(
        self,
        *,
        topic_id: int,
        start_no: int,
        count: int,
        leader: str,
        replicas_by_no: ConcurrentStableDict[int, ConcurrentStableList[str]] | None = None,
        leader_by_no: ConcurrentStableDict[int, str] | None = None,
        min_insync_replicas: int = 0,
    ) -> ConcurrentStableList[MsgPartition]:
        """追加 `count` 个分区（分区号从 `start_no` 起连续，只追加不重排）。

        Args:
            topic_id: 主题 ID。
            start_no: 起始分区号（= 主题原分区数）。
            count: 追加数量（须为正）。
            leader: 缺省 leader 标识（未在 `leader_by_no` 中给出时使用）。
            replicas_by_no: 分区号 → 副本集（02-8 分配结果；None = 不写副本集）。
            leader_by_no: 分区号 → leader（02-8 分配结果；None = 用缺省 leader）。
            min_insync_replicas: 分区级最小同步副本数（0 = 继承全局）。

        Returns:
            ConcurrentStableList[MsgPartition]: 新建分区（插入序）。
        """
        created: ConcurrentStableList[MsgPartition] = ConcurrentStableList()
        for partition_no in range(start_no, start_no + count):
            replicas: ConcurrentStableList[str] = (
                replicas_by_no.get(partition_no, ConcurrentStableList[str]())
                if replicas_by_no is not None
                else ConcurrentStableList[str]()
            )
            chosen_leader = leader_by_no.get(partition_no, leader) if leader_by_no is not None else leader
            created.add(
                await self.create(
                    topic_id=topic_id,
                    partition_no=partition_no,
                    leader=chosen_leader,
                    replicas=replicas,
                    min_insync_replicas=int(min_insync_replicas),
                )
            )
        return created

    async def delete_by_topic(self, topic_id: int) -> int:
        """删除某主题的全部分区行（**物理删除**，06-2 删主题清理的最后一步）。

        分区行没有软删语义（表只表达「有哪些分区」），删除即真删；调用方负责保证该主题已停止读写。

        Args:
            topic_id: 主题 ID。

        Returns:
            int: 删除的行数（无行返回 0，幂等）。
        """
        # 先取行再删：`Result.rowcount` 在异步会话类型面不可见（pyright 判 `Result[Any]` 无该属性），
        # 而分区数上限 1024，取出再删的代价可忽略。
        targets = await self.list_by_topic(topic_id)
        await self._session.execute(delete(MsgPartition).where(MsgPartition.topic_id == int(topic_id)))
        await self._session.flush()
        return len(targets)
