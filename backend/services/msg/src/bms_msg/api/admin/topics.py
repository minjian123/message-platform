"""主题管理接口（`/api/v1/msg/admin/topics`）：创建 / 列表 / 详情（详细设计 §8）。

- **权限码**：`msg:manage`（创建）、`msg:query`（列表 / 详情），经 `require_permission` 依赖声明；
  登录态与服务身份由网关与基座中间件承担（详细设计 §8.1 鉴权口径，05 域统一接入）。
- **租户**：唯一出口 `get_current_tenant_context()`；请求体不接收 `tenant`（防自报冒充）。
- **数据源**：元数据表在**平台库**，故会话取 `get_platform_uow`（`get_uow` 会按请求租户库选引擎）。
- **契约**：端点声明 `x-error-codes`，供跨仓 `check_modules` 的错误码反向对账。
"""

from typing import Annotated, cast

from bms_core.api.base import BaseRouter
from bms_core.core.concurrent import ConcurrentStableDict, ConcurrentStableList
from bms_core.core.exceptions import ParamError
from bms_core.core.logging import get_logger
from bms_core.db.session import DbSession, get_platform_uow
from bms_core.db.unit_of_work import UnitOfWork
from bms_core.permission.base import require_permission
from bms_core.schemas.common import ApiResponse
from fastapi import Depends, Path, Query

from bms_msg.acl.keys import ACL_ACTION_DESCRIBE, ACL_ACTION_MANAGE
from bms_msg.api.deps import (
    AuditSinkDep,
    CurrentTenantDep,
    PartitionAssignerDep,
    QuotaServiceDep,
    TopicAclServiceDep,
    TopicCacheDep,
    TopicCleanupDep,
)
from bms_msg.audit.trail import (
    TOPIC_CREATE_ACTION,
    TOPIC_DELETE_ACTION,
    TOPIC_DISABLE_ACTION,
    TOPIC_ENABLE_ACTION,
    TOPIC_EXPAND_ACTION,
    TOPIC_UPDATE_ACTION,
    AuditEmitter,
    emit_sensitive,
)
from bms_msg.broker.replay import AuditSink
from bms_msg.broker.topic_cache import TopicCache
from bms_msg.errors import MsgErrorCode
from bms_msg.models.partition import MsgPartition
from bms_msg.models.topic import STATUS_ACTIVE, STATUS_DELETING, STATUS_DISABLED, MsgTopic
from bms_msg.models.topic_cleanup import MsgTopicCleanup
from bms_msg.quota.service import QuotaService
from bms_msg.repositories.topic import PartitionRepository, TopicRepository
from bms_msg.repositories.topic_cleanup import TopicCleanupRepository
from bms_msg.schemas.topic import (
    CleanupProgress,
    DeleteImpact,
    PartitionItem,
    TopicCleanupView,
    TopicCreateRequest,
    TopicDetail,
    TopicItem,
    TopicPage,
    TopicPartitionExpandRequest,
    TopicRetentionUpdate,
)
from bms_msg.services.partition_assignment import PartitionAssigner
from bms_msg.services.topic import MAX_LIST_SIZE, TopicService

__all__ = ["router"]

_LOGGER = get_logger("bms_msg.admin_topics")

_QUERY = Depends(require_permission("msg:query"))
_MANAGE = Depends(require_permission("msg:manage"))

_QUERY_ERRORS = [
    int(MsgErrorCode.TOPIC_NOT_FOUND),
    int(MsgErrorCode.NAMESPACE_INVALID),
    int(MsgErrorCode.TENANT_SUSPENDED),
    int(MsgErrorCode.TENANT_NOT_FOUND),
    int(MsgErrorCode.ACL_DENIED),
]
"""查询端点声明的错误码（`x-error-codes`）。"""

_CREATE_ERRORS = [
    int(MsgErrorCode.TOPIC_EXISTS),
    int(MsgErrorCode.PARTITION_COUNT_INVALID),
    int(MsgErrorCode.TOPIC_QUOTA_EXCEEDED),
    int(MsgErrorCode.NAMESPACE_INVALID),
    int(MsgErrorCode.TENANT_SUSPENDED),
    int(MsgErrorCode.TENANT_NOT_FOUND),
    int(MsgErrorCode.ACL_DENIED),
]
"""创建端点声明的错误码（`x-error-codes`）。"""

_WRITE_ERRORS = [
    int(MsgErrorCode.TOPIC_NOT_FOUND),
    int(MsgErrorCode.TOPIC_STATE_INVALID),
    int(MsgErrorCode.PARTITION_COUNT_INVALID),
    int(MsgErrorCode.TENANT_SUSPENDED),
    int(MsgErrorCode.TENANT_NOT_FOUND),
    int(MsgErrorCode.ACL_DENIED),
]
"""主题写操作端点声明的错误码（扩分区 / 改保留期；`10001` 属契约层，不登记）。"""

_DELETE_ERRORS = [
    int(MsgErrorCode.TOPIC_NOT_FOUND),
    int(MsgErrorCode.TENANT_SUSPENDED),
    int(MsgErrorCode.TENANT_NOT_FOUND),
    int(MsgErrorCode.ACL_DENIED),
]
"""删主题端点声明的错误码（重复删除幂等受理，故不声明 `350028`）。"""

_IMPACT_ERRORS = [
    int(MsgErrorCode.TOPIC_NOT_FOUND),
    int(MsgErrorCode.TENANT_SUSPENDED),
    int(MsgErrorCode.TENANT_NOT_FOUND),
    int(MsgErrorCode.ACL_DENIED),
]
"""影响预检端点声明的错误码。"""

router = BaseRouter(
    key="msg_admin_topics",
    prefix="/msg/admin/topics",
    tags=["msg-admin-topics"],
)

PlatformUowDep = Annotated[UnitOfWork, Depends(get_platform_uow)]
"""平台库工作单元依赖（元数据表归平台库）。"""

TopicIdPath = Annotated[int, Path(description="主题 ID（雪花）")]
"""主题 ID 路径参数。"""


def _service(
    uow: UnitOfWork,
    *,
    cache: TopicCache | None = None,
    quota: QuotaService | None = None,
    cleanups: TopicCleanupRepository | None = None,
    assigner: PartitionAssigner | None = None,
) -> TopicService:
    """组装主题服务（请求级平台库会话注入）。

    Args:
        uow: 平台库工作单元。
        cache: 进程内主题快照缓存（**只有写路径需要**：提交成功后按精确键失效；
            读路径不经缓存，管理面永远读库最新）。
        quota: 配额服务（创建主题时判主题数配额）。
        cleanups: 清理台账仓储（删主题 / 查清理进度需要）。
        assigner: 分区副本分配器（02-8；None = 不写副本集）。

    Returns:
        TopicService: 主题服务。
    """
    session = cast("DbSession", uow.session)
    return TopicService(
        TopicRepository(session),
        PartitionRepository(session),
        uow,
        cache=cache,
        quota=quota,
        cleanups=cleanups,
        assigner=assigner,
    )


def _item(topic: MsgTopic) -> TopicItem:
    """主题 → 列表 / 创建响应明细。

    Args:
        topic: 主题记录。

    Returns:
        TopicItem: 响应明细。
    """
    return TopicItem.model_validate(topic)


def _partition(partition: MsgPartition) -> PartitionItem:
    """分区记录 → 响应明细。

    Args:
        partition: 分区记录。

    Returns:
        PartitionItem: 响应明细。
    """
    return PartitionItem.model_validate(partition)


def _cleanup_progress(cleanup: MsgTopicCleanup | None) -> CleanupProgress | None:
    """清理台账 → 进度视图（无台账返回 None）。

    Args:
        cleanup: 清理台账。

    Returns:
        CleanupProgress | None: 进度视图。
    """
    if cleanup is None:
        return None
    return CleanupProgress(
        status=cleanup.status,
        done_steps=ConcurrentStableList(str(item) for item in cleanup.done_steps),
        attempts=int(cleanup.attempts),
        started_at=cleanup.started_at,
        finished_at=cleanup.finished_at,
        last_error=cleanup.last_error,
    )


def _cleanup_view(topic: MsgTopic, cleanup: MsgTopicCleanup) -> TopicCleanupView:
    """主题 + 台账 → 删主题受理结果。

    Args:
        topic: 主题记录。
        cleanup: 清理台账。

    Returns:
        TopicCleanupView: 受理结果。
    """
    return TopicCleanupView(
        topic_id=topic.id,
        namespace=topic.namespace,
        topic=topic.name,
        status=topic.status,
        cleanup=CleanupProgress(
            status=cleanup.status,
            done_steps=ConcurrentStableList(str(item) for item in cleanup.done_steps),
            attempts=int(cleanup.attempts),
            started_at=cleanup.started_at,
            finished_at=cleanup.finished_at,
            last_error=cleanup.last_error,
        ),
    )


def _detail(
    topic: MsgTopic,
    partitions: ConcurrentStableList[MsgPartition],
    cleanup: MsgTopicCleanup | None = None,
) -> TopicDetail:
    """主题 + 分区（+ 清理进度）→ 详情响应。

    Args:
        topic: 主题记录。
        partitions: 分区记录（按分区号升序）。
        cleanup: 清理台账（仅 `deleting` 状态传入）。

    Returns:
        TopicDetail: 详情响应。
    """
    payload = _item(topic).model_dump()
    metadata_version = max((int(partition.epoch or 0) for partition in partitions), default=0)
    return TopicDetail.model_validate(
        {
            **payload,
            "metadata_version": metadata_version,
            "partitions": ConcurrentStableList(_partition(partition) for partition in partitions),
            "cleanup": _cleanup_progress(cleanup),
        }
    )


def _require_confirm(confirm: bool, *, action: str, topic: str) -> None:
    """校验高危写操作的显式确认参数（缺失即拒绝并留痕）。

    Args:
        confirm: 请求携带的确认值。
        action: 被拒动作（日志分类用）。
        topic: 主题标识（日志定位用）。

    Raises:
        ParamError: `confirm` 不为 true（10001）。
    """
    if confirm:
        return
    _LOGGER.warning("msg_write_confirm_missing", action=action, topic=topic)
    raise ParamError("高危写操作须显式确认（confirm=true）")


async def _filter_topics(
    acl: TopicAclServiceDep,
    tenant: CurrentTenantDep,
    topics: ConcurrentStableList[MsgTopic],
) -> ConcurrentStableList[MsgTopic]:
    """按 `describe` ACL 过滤主题（列表接口；无权限主题不返回）。

    Args:
        acl: ACL 守卫。
        tenant: 当前租户。
        topics: 候选主题。

    Returns:
        ConcurrentStableList[MsgTopic]: 允许查看的主题（保持入参顺序）。
    """
    visible: ConcurrentStableList[MsgTopic] = ConcurrentStableList()
    for topic in topics:
        allowed = await acl.filter_allowed(
            tenant_id=int(tenant.tenant_id),
            namespace=topic.namespace,
            topics=ConcurrentStableList([topic.name]),
            action=ACL_ACTION_DESCRIBE,
        )
        if allowed:
            visible.add(topic)
    return visible


async def _audit_create(
    sink: AuditSink,
    *,
    tenant_id: int,
    namespace: str,
    topic_name: str,
    topic: MsgTopic,
    session: DbSession | None = None,
) -> None:
    """写一条建主题审计（配置变更，强审计；失败即接口失败）。

    Args:
        sink: 审计出口。
        tenant_id: 租户主键。
        namespace: 命名空间。
        topic_name: 主题名。
        topic: 新建主题。
        session: 业务会话（给出则审计与业务同事务）。
    """
    params = ConcurrentStableDict(
        {
            "topic_id": str(topic.id),
            "namespace": namespace,
            "topic": topic_name,
            "partition_count": int(topic.partition_count),
            "retention_ms": int(topic.retention_ms),
            "retention_bytes": int(topic.retention_bytes),
            "cleanup_policy": topic.cleanup_policy,
            "status": topic.status,
        }
    )
    await emit_sensitive(
        cast("AuditEmitter", sink),
        action=TOPIC_CREATE_ACTION,
        tenant_id=tenant_id,
        params=params,
        method="POST",
        path="/api/v1/msg/admin/topics",
        module_key=namespace,
        resource_key=topic_name,
        session=session,
    )


@router.post("", dependencies=[_MANAGE], openapi_extra={"x-error-codes": _CREATE_ERRORS})
async def create_topic(
    req: TopicCreateRequest,
    tenant: CurrentTenantDep,
    uow: PlatformUowDep,
    cache: TopicCacheDep,
    quota: QuotaServiceDep,
    sink: AuditSinkDep,
    acl: TopicAclServiceDep,
    assigner: PartitionAssignerDep,
) -> ApiResponse[TopicItem]:
    """创建主题（指定分区数，返回完整元数据）。

    需要 msg:manage 权限；`tenant_id` 取自基座解析链（请求体不接收）；同租户 / 命名空间下重名
    返回 350008，分区数或保留期越界返回 350009，命名空间非已启用 module_key 返回 350010，
    主题数配额已满返回 350025。
    """
    await acl.authorize(
        tenant_id=int(tenant.tenant_id),
        namespace=req.namespace,
        topic=req.name,
        action=ACL_ACTION_MANAGE,
    )
    topic = await _service(uow, cache=cache, quota=quota, assigner=assigner).create_topic(tenant=tenant, request=req)
    await _audit_create(
        sink,
        tenant_id=int(tenant.tenant_id),
        namespace=req.namespace,
        topic_name=req.name,
        topic=topic,
        session=uow.session,
    )
    return ApiResponse.ok(_item(topic))


@router.get("", dependencies=[_QUERY], openapi_extra={"x-error-codes": _QUERY_ERRORS})
async def list_topics(
    tenant: CurrentTenantDep,
    uow: PlatformUowDep,
    acl: TopicAclServiceDep,
    namespace: Annotated[str | None, Query(max_length=64, description="命名空间过滤")] = None,
    keyword: Annotated[str | None, Query(max_length=200, description="主题名模糊匹配")] = None,
    page: Annotated[int, Query(ge=1, description="页码（从 1 起）")] = 1,
    size: Annotated[int, Query(ge=1, le=MAX_LIST_SIZE, description="每页条数")] = 20,
) -> ApiResponse[TopicPage]:
    """主题列表（页码分页 + 命名空间 / 名称过滤）。

    需要 msg:query 权限；按租户隔离；命名空间过滤值非法返回 350010。
    """
    items, total = await _service(uow).list_topics(
        tenant=tenant, namespace=namespace, keyword=keyword, page=page, size=size
    )
    visible = await _filter_topics(acl, tenant, items)
    return ApiResponse.ok(
        TopicPage(
            items=ConcurrentStableList(_item(topic) for topic in visible),
            total=total,
            page=page,
            size=size,
        )
    )


@router.get("/{topic_id}", dependencies=[_QUERY], openapi_extra={"x-error-codes": _QUERY_ERRORS})
async def get_topic(
    topic_id: TopicIdPath,
    tenant: CurrentTenantDep,
    uow: PlatformUowDep,
    acl: TopicAclServiceDep,
) -> ApiResponse[TopicDetail]:
    """主题详情（含分区列表；`deleting` 时含清理进度）。

    需要 msg:query 权限；不存在或不属于当前租户返回 350001。
    """
    service = _service(uow, cleanups=TopicCleanupRepository(cast("DbSession", uow.session)))
    topic, partitions = await service.get_topic(tenant=tenant, topic_id=topic_id)
    await acl.authorize(
        tenant_id=int(tenant.tenant_id),
        namespace=topic.namespace,
        topic=topic.name,
        action=ACL_ACTION_DESCRIBE,
    )
    cleanup = await service.get_cleanup(topic_id=topic_id) if topic.status == STATUS_DELETING else None
    return ApiResponse.ok(_detail(topic, partitions, cleanup))


@router.get("/{topic_id}/delete-impact", dependencies=[_MANAGE], openapi_extra={"x-error-codes": _IMPACT_ERRORS})
async def topic_delete_impact(
    topic_id: TopicIdPath,
    tenant: CurrentTenantDep,
    uow: PlatformUowDep,
    cleanup: TopicCleanupDep,
) -> ApiResponse[DeleteImpact]:
    """删主题影响预检（二次确认弹窗的五项影响范围）。

    需要 msg:manage 权限；主题不存在 350001。各维度独立容错：单项取数失败记 0 并告警，
    不因预检失败阻断后续删除（真正的保护在后端的状态与二次确认判定）。
    """
    topic, _partitions = await _service(uow).get_topic(tenant=tenant, topic_id=topic_id)
    impact = await cleanup.impact(
        tenant_id=int(tenant.tenant_id),
        topic_id=int(topic.id),
        partition_count=int(topic.partition_count),
    )
    return ApiResponse.ok(impact)


@router.delete("/{topic_id}", dependencies=[_MANAGE], openapi_extra={"x-error-codes": _DELETE_ERRORS})
async def delete_topic(
    topic_id: TopicIdPath,
    tenant: CurrentTenantDep,
    uow: PlatformUowDep,
    cache: TopicCacheDep,
    cleanup: TopicCleanupDep,
    sink: AuditSinkDep,
    acl: TopicAclServiceDep,
    confirm: Annotated[bool, Query(description="显式确认（必须为 true；缺省 / false 一律拒绝）")] = False,
) -> ApiResponse[TopicCleanupView]:
    """删主题（**受理即返回**：置 `deleting` + 落台账 + 后台异步清理）。

    需要 msg:manage 权限；`confirm` 缺失返回 10001；主题不存在 350001；**重复删除幂等受理**
    （返回已有台账，不产生二次副作用）。审计失败即请求失败（主题不会被置 `deleting`）。
    """
    _require_confirm(confirm, action=TOPIC_DELETE_ACTION, topic=str(topic_id))
    topic, _partitions = await _service(uow).get_topic(tenant=tenant, topic_id=topic_id)
    await acl.authorize(
        tenant_id=int(tenant.tenant_id),
        namespace=topic.namespace,
        topic=topic.name,
        action=ACL_ACTION_MANAGE,
    )
    impact = await cleanup.impact(
        tenant_id=int(tenant.tenant_id),
        topic_id=int(topic.id),
        partition_count=int(topic.partition_count),
    )
    service = _service(uow, cache=cache, cleanups=TopicCleanupRepository(cast("DbSession", uow.session)))
    updated, ledger = await service.begin_delete(tenant=tenant, topic_id=topic_id)
    try:
        await _audit_write(
            sink,
            action=TOPIC_DELETE_ACTION,
            method="DELETE",
            tenant_id=int(tenant.tenant_id),
            namespace=updated.namespace,
            topic_name=updated.name,
            params=ConcurrentStableDict(
                {
                    "topic_id": str(updated.id),
                    "namespace": updated.namespace,
                    "topic": updated.name,
                    "partition_count": int(impact.partition_count),
                    "message_total": int(impact.message_total),
                    "group_count": int(impact.group_count),
                    "delayed_total": int(impact.delayed_total),
                    "dlq_total": int(impact.dlq_total),
                    "status": updated.status,
                }
            ),
            session=uow.session,
        )
    finally:
        # 审计失败也要把台账推进（否则主题停在 deleting 且无人清理）
        cleanup.kick(int(updated.id))
    return ApiResponse.ok(_cleanup_view(updated, ledger))


@router.post("/{topic_id}/partitions", dependencies=[_MANAGE], openapi_extra={"x-error-codes": _WRITE_ERRORS})
async def expand_topic_partitions(
    topic_id: TopicIdPath,
    req: TopicPartitionExpandRequest,
    tenant: CurrentTenantDep,
    uow: PlatformUowDep,
    cache: TopicCacheDep,
    sink: AuditSinkDep,
    acl: TopicAclServiceDep,
    assigner: PartitionAssignerDep,
) -> ApiResponse[TopicDetail]:
    """扩分区（**只增不减**：等于现状幂等返回 200，小于现状返回 350009）。

    需要 msg:manage 权限；主题不存在 350001，状态为 `disabled` / `deleting` 时 350028。
    占用现有 `TopicService.expand_partitions()`，不迁移既有 key 映射。
    """
    topic, _partitions = await _service(uow).get_topic(tenant=tenant, topic_id=topic_id)
    await acl.authorize(
        tenant_id=int(tenant.tenant_id),
        namespace=topic.namespace,
        topic=topic.name,
        action=ACL_ACTION_MANAGE,
    )
    before = int(topic.partition_count)
    updated, partitions = await _service(uow, cache=cache, assigner=assigner).expand_partitions(
        tenant=tenant, topic_id=topic_id, partition_count=req.partition_count
    )
    await _audit_write(
        sink,
        action=TOPIC_EXPAND_ACTION,
        method="POST",
        tenant_id=int(tenant.tenant_id),
        namespace=updated.namespace,
        topic_name=updated.name,
        params=ConcurrentStableDict(
            {
                "topic_id": str(updated.id),
                "namespace": updated.namespace,
                "topic": updated.name,
                "from_partitions": before,
                "to_partitions": int(updated.partition_count),
            }
        ),
        path=f"/api/v1/msg/admin/topics/{topic_id}/partitions",
        session=uow.session,
    )
    return ApiResponse.ok(_detail(updated, partitions))


@router.patch("/{topic_id}", dependencies=[_MANAGE], openapi_extra={"x-error-codes": _WRITE_ERRORS})
async def update_topic(
    topic_id: TopicIdPath,
    req: TopicRetentionUpdate,
    tenant: CurrentTenantDep,
    uow: PlatformUowDep,
    cache: TopicCacheDep,
    sink: AuditSinkDep,
    acl: TopicAclServiceDep,
) -> ApiResponse[TopicItem]:
    """改保留期 / 容量上限（**部分更新**：只为给出的项写库，至少一项）。

    需要 msg:manage 权限；主题不存在 350001，状态 `disabled` / `deleting` 时 350028，
    取值越界 350009。即时生效、不回溯删除已保留记录（后续清理按新值执行）。
    """
    topic, _partitions = await _service(uow).get_topic(tenant=tenant, topic_id=topic_id)
    await acl.authorize(
        tenant_id=int(tenant.tenant_id),
        namespace=topic.namespace,
        topic=topic.name,
        action=ACL_ACTION_MANAGE,
    )
    updated, before = await _service(uow, cache=cache).update_retention(
        tenant=tenant,
        topic_id=topic_id,
        retention_ms=req.retention_ms,
        retention_bytes=req.retention_bytes,
    )
    await _audit_write(
        sink,
        action=TOPIC_UPDATE_ACTION,
        method="PATCH",
        tenant_id=int(tenant.tenant_id),
        namespace=updated.namespace,
        topic_name=updated.name,
        params=ConcurrentStableDict(
            {
                "topic_id": str(updated.id),
                "namespace": updated.namespace,
                "topic": updated.name,
                "retention_ms_before": before.get("retention_ms", 0),
                "retention_ms_after": int(updated.retention_ms),
                "retention_bytes_before": before.get("retention_bytes", 0),
                "retention_bytes_after": int(updated.retention_bytes),
            }
        ),
        path=f"/api/v1/msg/admin/topics/{topic_id}",
        session=uow.session,
    )
    return ApiResponse.ok(_item(updated))


@router.post("/{topic_id}/disable", dependencies=[_MANAGE], openapi_extra={"x-error-codes": _WRITE_ERRORS})
async def disable_topic(
    topic_id: TopicIdPath,
    tenant: CurrentTenantDep,
    uow: PlatformUowDep,
    cache: TopicCacheDep,
    sink: AuditSinkDep,
    acl: TopicAclServiceDep,
    confirm: Annotated[bool, Query(description="显式确认（必须为 true）")] = False,
) -> ApiResponse[TopicItem]:
    """停用主题（**只停数据面**：发布 / 消费 350028；管理面查询与写操作照常）。

    需要 msg:manage 权限；`confirm` 缺失 10001；主题不存在 350001；`deleting` 状态 350028；
    重复停用幂等返回 200。
    """
    _require_confirm(confirm, action=TOPIC_DISABLE_ACTION, topic=str(topic_id))
    topic, _partitions = await _service(uow).get_topic(tenant=tenant, topic_id=topic_id)
    await acl.authorize(
        tenant_id=int(tenant.tenant_id),
        namespace=topic.namespace,
        topic=topic.name,
        action=ACL_ACTION_MANAGE,
    )
    updated, before = await _service(uow, cache=cache).set_status(
        tenant=tenant, topic_id=topic_id, status=STATUS_DISABLED
    )
    await _audit_write(
        sink,
        action=TOPIC_DISABLE_ACTION,
        method="POST",
        tenant_id=int(tenant.tenant_id),
        namespace=updated.namespace,
        topic_name=updated.name,
        params=ConcurrentStableDict(
            {
                "topic_id": str(updated.id),
                "namespace": updated.namespace,
                "topic": updated.name,
                "status_before": before,
                "status_after": updated.status,
            }
        ),
        path=f"/api/v1/msg/admin/topics/{topic_id}/disable",
        session=uow.session,
    )
    return ApiResponse.ok(_item(updated))


@router.post("/{topic_id}/enable", dependencies=[_MANAGE], openapi_extra={"x-error-codes": _WRITE_ERRORS})
async def enable_topic(
    topic_id: TopicIdPath,
    tenant: CurrentTenantDep,
    uow: PlatformUowDep,
    cache: TopicCacheDep,
    sink: AuditSinkDep,
    acl: TopicAclServiceDep,
) -> ApiResponse[TopicItem]:
    """启用主题（数据面恢复；重复启用幂等返回 200）。

    需要 msg:manage 权限；主题不存在 350001；`deleting` 状态 350028。
    """
    topic, _partitions = await _service(uow).get_topic(tenant=tenant, topic_id=topic_id)
    await acl.authorize(
        tenant_id=int(tenant.tenant_id),
        namespace=topic.namespace,
        topic=topic.name,
        action=ACL_ACTION_MANAGE,
    )
    updated, before = await _service(uow, cache=cache).set_status(
        tenant=tenant, topic_id=topic_id, status=STATUS_ACTIVE
    )
    await _audit_write(
        sink,
        action=TOPIC_ENABLE_ACTION,
        method="POST",
        tenant_id=int(tenant.tenant_id),
        namespace=updated.namespace,
        topic_name=updated.name,
        params=ConcurrentStableDict(
            {
                "topic_id": str(updated.id),
                "namespace": updated.namespace,
                "topic": updated.name,
                "status_before": before,
                "status_after": updated.status,
            }
        ),
        path=f"/api/v1/msg/admin/topics/{topic_id}/enable",
        session=uow.session,
    )
    return ApiResponse.ok(_item(updated))


async def _audit_write(
    sink: AuditSink,
    *,
    action: str,
    method: str,
    tenant_id: int,
    namespace: str,
    topic_name: str,
    params: ConcurrentStableDict[str, object],
    path: str = "/api/v1/msg/admin/topics",
    session: DbSession | None = None,
) -> None:
    """写一条主题写操作审计（配置变更，强审计；失败即接口失败）。

    Args:
        sink: 审计出口。
        action: 审计动作（删主题 / 扩分区 / 改保留期）。
        method: HTTP 方法。
        tenant_id: 租户主键。
        namespace: 命名空间。
        topic_name: 主题名。
        params: 载荷（含变更前后值与影响范围）。
        path: 实际请求路径。
        session: 业务会话（给出则审计与业务同事务）。
    """
    await emit_sensitive(
        cast("AuditEmitter", sink),
        action=action,
        tenant_id=tenant_id,
        params=params,
        method=method,
        path=path,
        module_key=namespace,
        resource_key=topic_name,
        session=session,
    )
