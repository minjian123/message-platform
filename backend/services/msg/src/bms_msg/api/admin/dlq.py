"""死信管理接口（`/api/v1/msg/admin/dlq`；06-5-2 详细设计 §3）。

- **权限**：查询 `msg:query`，重放 / 忽略 / 归档 `msg:manage`；主题资源 ACL 同口径（`describe` / `manage`）。
- **租户**：唯一出口 `get_current_tenant_context()`；跨租户按 `350029`（不泄露存在性）。
- **重放**：复用 01-6 任务化重放，按该死信的分区 / offset 建单条任务；原记录不存在返回 `350031`。
- **审计**：建任务审计由本端点按 01-6 口径补写（`msg:replay`），死信处置再写 `msg:dlq.*`（记录级强审计）。
"""

from datetime import datetime
from typing import Annotated, cast

from bms_core.api.base import BaseRouter
from bms_core.core.concurrent import ConcurrentStableDict, ConcurrentStableList, ConcurrentStableSet
from bms_core.core.context import get_current_client_ip, get_current_trace_id, get_current_user_id
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
    GroupStoreDep,
    MessageStoreDep,
    OffsetStoreDep,
    ReplayRunnerDep,
    TopicAclServiceDep,
)
from bms_msg.audit.trail import (
    DLQ_ARCHIVE_ACTION,
    DLQ_DETAIL_ACTION,
    DLQ_IGNORE_ACTION,
    DLQ_REPLAY_ACTION,
    AuditEmitter,
    emit_record_operation,
    emit_sensitive,
)
from bms_msg.broker.log import MessageStore
from bms_msg.broker.replay import ReplayAuditor, ReplayService
from bms_msg.broker.router import TopicRouter
from bms_msg.errors import MsgDeadLetterInvalidStateError, MsgDeadLetterNotReplayableError, MsgErrorCode
from bms_msg.models.dead_letter import MsgDeadLetter
from bms_msg.repositories.dead_letter import DeadLetterRepository
from bms_msg.repositories.replay import ReplayTaskRepository
from bms_msg.repositories.topic import TopicRepository
from bms_msg.schemas.dead_letter import (
    DlqArchiveRequest,
    DlqDetail,
    DlqDisposeResult,
    DlqIgnoreRequest,
    DlqItem,
    DlqList,
    DlqReplayRequest,
)
from bms_msg.schemas.replay import ReplayRequest
from bms_msg.services.dead_letter import DeadLetterService
from bms_msg.services.replay_impact import ReplayImpactInspector, build_impact_inspector
from bms_msg.services.topic import MAX_LIST_SIZE

__all__ = ["router"]

_LOGGER = get_logger("bms_msg.admin_dlq")

_QUERY = Depends(require_permission("msg:query"))
_MANAGE = Depends(require_permission("msg:manage"))

_BASE_PATH = "/api/v1/msg/admin/dlq"

_QUERY_ERRORS = [
    int(MsgErrorCode.DEAD_LETTER_NOT_FOUND),
    int(MsgErrorCode.TENANT_SUSPENDED),
    int(MsgErrorCode.TENANT_NOT_FOUND),
    int(MsgErrorCode.ACL_DENIED),
]
"""查询端点声明的错误码（`x-error-codes`）。"""

_MANAGE_ERRORS = [
    int(MsgErrorCode.DEAD_LETTER_NOT_FOUND),
    int(MsgErrorCode.DEAD_LETTER_INVALID_STATE),
    int(MsgErrorCode.DEAD_LETTER_NOT_REPLAYABLE),
    int(MsgErrorCode.TOPIC_NOT_FOUND),
    int(MsgErrorCode.PARTITION_COUNT_INVALID),
    int(MsgErrorCode.NAMESPACE_INVALID),
    int(MsgErrorCode.TENANT_SUSPENDED),
    int(MsgErrorCode.TENANT_NOT_FOUND),
    int(MsgErrorCode.ACL_DENIED),
]
"""处置端点声明的错误码（`x-error-codes`）。"""

router = BaseRouter(
    key="msg_admin_dlq",
    prefix="/msg/admin/dlq",
    tags=["msg-admin-dlq"],
)

PlatformUowDep = Annotated[UnitOfWork, Depends(get_platform_uow)]
"""平台库工作单元依赖（死信表归平台库）。"""

DlqIdPath = Annotated[int, Path(description="死信 ID（雪花）")]
"""死信 ID 路径参数。"""


def _require_confirm(confirm: bool, *, action: str, **scope: str) -> None:
    """校验高危写操作的显式确认参数（缺失即拒绝）。

    Args:
        confirm: 请求携带的确认值。
        action: 被拒动作（日志分类用）。
        scope: 作用域字段（主题 / 分区等，日志定位用）。

    Raises:
        ParamError: `confirm` 不为 true（10001）。
    """
    if confirm:
        return
    _LOGGER.warning("msg_dlq_confirm_missing", action=action, **scope)
    raise ParamError("高危写操作须显式确认（confirm=true）")


def _service(uow: UnitOfWork) -> DeadLetterService:
    """组装死信服务（请求级平台库会话）。

    Args:
        uow: 平台库工作单元。

    Returns:
        DeadLetterService: 死信服务。
    """
    session = cast("DbSession", uow.session)
    return DeadLetterService(
        uow=uow,
        dead_letters=DeadLetterRepository(session),
        replay_tasks=ReplayTaskRepository(session),
    )


def _replay_service(uow: UnitOfWork, store: MessageStore, impact: ReplayImpactInspector) -> ReplayService:
    """组装重放服务（复用 01-6 任务化重放 + 06-6-1 影响投影）。

    Args:
        uow: 平台库工作单元。
        store: 消息存储。
        impact: 影响投影（建任务时采集影响基线）。

    Returns:
        ReplayService: 重放任务服务。
    """
    session = cast("DbSession", uow.session)
    return ReplayService(
        tasks=ReplayTaskRepository(session),
        router=TopicRouter(TopicRepository(session)),
        store=store,
        uow=uow,
        impact=impact,
    )


def _item(row: MsgDeadLetter) -> DlqItem:
    """行 → 列表摘要。

    Args:
        row: 死信行。

    Returns:
        DlqItem: 摘要。
    """
    return DlqItem(
        id=int(row.id),
        namespace=row.namespace,
        topic=row.topic,
        partition_no=int(row.partition_no),
        offset=int(row.msg_offset),
        log_id=row.log_id,
        message_id=row.message_id,
        trace_id=row.trace_id,
        reason=row.reason,
        attempts=int(row.attempts),
        status=row.status,
        failed_at=row.failed_at,
        replay_task_id=None if row.replay_task_id is None else int(row.replay_task_id),
        disposition_reason=row.disposition_reason,
        disposed_at=row.disposed_at,
        created_at=row.created_at,
    )


def _detail(row: MsgDeadLetter) -> DlqDetail:
    """行 → 详情（含失败上下文）。

    Args:
        row: 死信行。

    Returns:
        DlqDetail: 详情。
    """
    base = _item(row)
    return DlqDetail(**base.model_dump(), detail=row.detail)


async def _visible_rows(
    acl: TopicAclServiceDep,
    tenant: CurrentTenantDep,
    rows: ConcurrentStableList[MsgDeadLetter],
) -> ConcurrentStableList[MsgDeadLetter]:
    """按 `describe` ACL 过滤死信列表（无权限主题不返回）。

    Args:
        acl: ACL 守卫。
        tenant: 当前租户。
        rows: 候选死信。

    Returns:
        ConcurrentStableList[MsgDeadLetter]: 允许查看的行（保持入参顺序）。
    """
    by_namespace: ConcurrentStableDict[str, ConcurrentStableList[str]] = ConcurrentStableDict()
    for row in rows:
        topics = by_namespace.get(row.namespace)
        if topics is None:
            topics = ConcurrentStableList[str]()
            by_namespace.set(row.namespace, topics)
        if row.topic not in topics:
            topics.add(row.topic)
    allowed: ConcurrentStableSet[tuple[str, str]] = ConcurrentStableSet()
    for namespace, topics in by_namespace.items():
        visible = await acl.filter_allowed(
            tenant_id=int(tenant.tenant_id),
            namespace=namespace,
            topics=topics,
            action=ACL_ACTION_DESCRIBE,
        )
        for topic in visible:
            allowed.add((namespace, topic))
    return ConcurrentStableList(row for row in rows if (row.namespace, row.topic) in allowed)


@router.get("", dependencies=[_QUERY], openapi_extra={"x-error-codes": _QUERY_ERRORS})
async def list_dead_letters(
    tenant: CurrentTenantDep,
    uow: PlatformUowDep,
    acl: TopicAclServiceDep,
    status: Annotated[str | None, Query(max_length=16, description="状态过滤；缺省排除 archived")] = None,
    reason: Annotated[str | None, Query(max_length=32, description="转入原因过滤")] = None,
    topic: Annotated[str | None, Query(max_length=200, description="主题名过滤")] = None,
    partition: Annotated[int | None, Query(ge=0, description="分区号过滤")] = None,
    failed_from: Annotated[datetime | None, Query(description="失败时间下限（含，UTC）")] = None,
    failed_to: Annotated[datetime | None, Query(description="失败时间上限（不含，UTC）")] = None,
    keyword: Annotated[str | None, Query(max_length=200, description="log_id / message_id 模糊匹配")] = None,
    page: Annotated[int, Query(ge=1, description="页码（从 1 起）")] = 1,
    size: Annotated[int, Query(ge=1, le=MAX_LIST_SIZE, description="每页条数")] = 20,
) -> ApiResponse[DlqList]:
    """死信列表（默认排除已归档）。

    需要 msg:query 权限；按租户隔离；无主题 `describe` 授权的行不返回。

    Raises:
        ParamError: 失败时间区间倒置（10001）。
    """
    if failed_from is not None and failed_to is not None and failed_from > failed_to:
        raise ParamError("失败时间区间倒置")
    rows, total = await _service(uow).list(
        tenant_id=int(tenant.tenant_id),
        page=page,
        size=size,
        status=status,
        reason=reason,
        topic=topic,
        partition_no=partition,
        failed_from=failed_from,
        failed_to=failed_to,
        keyword=keyword,
    )
    visible = await _visible_rows(acl, tenant, rows)
    return ApiResponse.ok(
        DlqList(items=ConcurrentStableList(_item(row) for row in visible), total=total, page=page, size=size)
    )


@router.get("/{dlq_id}", dependencies=[_QUERY], openapi_extra={"x-error-codes": _QUERY_ERRORS})
async def get_dead_letter(
    dlq_id: DlqIdPath,
    tenant: CurrentTenantDep,
    uow: PlatformUowDep,
    acl: TopicAclServiceDep,
    sink: AuditSinkDep,
) -> ApiResponse[DlqDetail]:
    """死信详情（含失败上下文）。

    需要 msg:query 权限；不存在或跨租户返回 350029；无主题 describe 授权返回 350027。
    """
    row = await _service(uow).detail(item_id=dlq_id, tenant_id=int(tenant.tenant_id))
    await acl.authorize(
        tenant_id=int(tenant.tenant_id), namespace=row.namespace, topic=row.topic, action=ACL_ACTION_DESCRIBE
    )
    await emit_sensitive(
        cast("AuditEmitter", sink),
        action=DLQ_DETAIL_ACTION,
        tenant_id=int(tenant.tenant_id),
        params=ConcurrentStableDict({"dlq_id": str(row.id), "log_id": row.log_id}),
        method="GET",
        path=f"{_BASE_PATH}/{dlq_id}",
        module_key=row.namespace,
        resource_key=row.topic,
        session=uow.session,
    )
    return ApiResponse.ok(_detail(row))


@router.post("/{dlq_id}/replay", dependencies=[_MANAGE], openapi_extra={"x-error-codes": _MANAGE_ERRORS})
async def replay_dead_letter(
    dlq_id: DlqIdPath,
    req: DlqReplayRequest,
    tenant: CurrentTenantDep,
    uow: PlatformUowDep,
    store: MessageStoreDep,
    groups: GroupStoreDep,
    offsets: OffsetStoreDep,
    runner: ReplayRunnerDep,
    sink: AuditSinkDep,
    acl: TopicAclServiceDep,
) -> ApiResponse[DlqDisposeResult]:
    """重放死信（按原分区 / offset 建单条重放任务）。

    需要 msg:manage 权限；状态非 pending 350030；原记录不存在 350031。
    """
    row = await _service(uow).detail(item_id=dlq_id, tenant_id=int(tenant.tenant_id))
    await acl.authorize(
        tenant_id=int(tenant.tenant_id), namespace=row.namespace, topic=row.topic, action=ACL_ACTION_MANAGE
    )
    _require_confirm(req.confirm, action=DLQ_REPLAY_ACTION, topic=row.topic, partition=str(row.partition_no))
    item_id = int(row.id)
    namespace = row.namespace
    topic_name = row.topic
    partition_no = int(row.partition_no)
    msg_offset = int(row.msg_offset)
    log_id = row.log_id
    batch = await store.fetch(
        topic_id=int(row.topic_id), partition_no=partition_no, from_offset=msg_offset, max_messages=1
    )
    if not batch or int(batch[0].offset) != msg_offset:
        raise MsgDeadLetterNotReplayableError("原消息已超出保留期或不存在，无法重放")
    await uow.rollback()
    task = await _replay_service(
        uow, store, build_impact_inspector(groups=groups, offsets=offsets, messages=store)
    ).create(
        tenant=tenant,
        request=ReplayRequest(
            namespace=namespace,
            topic=topic_name,
            partitions=ConcurrentStableList([partition_no]),
            offset_from=msg_offset,
            offset_to=msg_offset + 1,
            rate_per_second=req.rate_per_second,
            confirm=True,
        ),
    )
    actor_id = get_current_user_id()
    replay_task_id = int(task.id)
    await ReplayAuditor(sink=sink).record_created(
        task=task, actor_id=actor_id, ip=get_current_client_ip(), trace_id=get_current_trace_id()
    )
    service = _replay_service(uow, store, build_impact_inspector(groups=groups, offsets=offsets, messages=store))
    try:
        status = await _service(uow).mark_replaying(
            item_id=item_id, tenant_id=int(tenant.tenant_id), replay_task_id=replay_task_id
        )
    except MsgDeadLetterInvalidStateError:
        # 并发已被他人处置：取消刚建的任务，避免留下孤儿（best-effort，失败只记日志）
        try:
            await service.cancel(tenant=tenant, task_id=replay_task_id)
        except Exception as exc:  # pragma: no cover - 补偿失败不掩盖原始错误
            _LOGGER.error("msg_dlq_replay_orphan_task", task_id=str(replay_task_id), error=repr(exc))
        raise
    await emit_record_operation(
        sink,
        action=DLQ_REPLAY_ACTION,
        tenant_id=tenant.tenant_id,
        log_id=log_id,
        params=ConcurrentStableDict(
            {"reason": "manual_replay", "rate_per_second": req.rate_per_second, "replay_task_id": str(replay_task_id)}
        ),
        method="POST",
        path=f"{_BASE_PATH}/{dlq_id}/replay",
        module_key=namespace,
        resource_key=topic_name,
        user_id=actor_id,
    )
    runner.ensure_started()
    return ApiResponse.ok(DlqDisposeResult(id=item_id, status=status, replay_task_id=replay_task_id))


@router.post("/{dlq_id}/ignore", dependencies=[_MANAGE], openapi_extra={"x-error-codes": _MANAGE_ERRORS})
async def ignore_dead_letter(
    dlq_id: DlqIdPath,
    req: DlqIgnoreRequest,
    tenant: CurrentTenantDep,
    uow: PlatformUowDep,
    sink: AuditSinkDep,
    acl: TopicAclServiceDep,
) -> ApiResponse[DlqDisposeResult]:
    """忽略死信（原因必填）。

    需要 msg:manage 权限；状态非 pending 350030；原因空白 / 超长 10001。
    """
    row = await _service(uow).detail(item_id=dlq_id, tenant_id=int(tenant.tenant_id))
    await acl.authorize(
        tenant_id=int(tenant.tenant_id), namespace=row.namespace, topic=row.topic, action=ACL_ACTION_MANAGE
    )
    _require_confirm(req.confirm, action=DLQ_IGNORE_ACTION, topic=row.topic)
    item_id = int(row.id)
    log_id = row.log_id
    namespace = row.namespace
    topic_name = row.topic
    actor_id = get_current_user_id()
    await uow.rollback()
    status = await _service(uow).ignore(
        item_id=item_id, tenant_id=int(tenant.tenant_id), reason=req.reason, actor_id=actor_id
    )
    await emit_record_operation(
        sink,
        action=DLQ_IGNORE_ACTION,
        tenant_id=tenant.tenant_id,
        log_id=log_id,
        params=ConcurrentStableDict({"reason": req.reason}),
        method="POST",
        path=f"{_BASE_PATH}/{dlq_id}/ignore",
        module_key=namespace,
        resource_key=topic_name,
        user_id=actor_id,
    )
    return ApiResponse.ok(DlqDisposeResult(id=item_id, status=status))


@router.post("/{dlq_id}/archive", dependencies=[_MANAGE], openapi_extra={"x-error-codes": _MANAGE_ERRORS})
async def archive_dead_letter(
    dlq_id: DlqIdPath,
    req: DlqArchiveRequest,
    tenant: CurrentTenantDep,
    uow: PlatformUowDep,
    sink: AuditSinkDep,
    acl: TopicAclServiceDep,
) -> ApiResponse[DlqDisposeResult]:
    """归档死信（从活跃列表隐藏，原始消息保留）。

    需要 msg:manage 权限；`replaying` / `archived` 返回 350030。
    """
    row = await _service(uow).detail(item_id=dlq_id, tenant_id=int(tenant.tenant_id))
    await acl.authorize(
        tenant_id=int(tenant.tenant_id), namespace=row.namespace, topic=row.topic, action=ACL_ACTION_MANAGE
    )
    _require_confirm(req.confirm, action=DLQ_ARCHIVE_ACTION, topic=row.topic)
    item_id = int(row.id)
    log_id = row.log_id
    namespace = row.namespace
    topic_name = row.topic
    actor_id = get_current_user_id()
    await uow.rollback()
    status = await _service(uow).archive(
        item_id=item_id, tenant_id=int(tenant.tenant_id), reason=req.reason, actor_id=actor_id
    )
    await emit_record_operation(
        sink,
        action=DLQ_ARCHIVE_ACTION,
        tenant_id=tenant.tenant_id,
        log_id=log_id,
        params=ConcurrentStableDict({"reason": req.reason or ""}),
        method="POST",
        path=f"{_BASE_PATH}/{dlq_id}/archive",
        module_key=namespace,
        resource_key=topic_name,
        user_id=actor_id,
    )
    return ApiResponse.ok(DlqDisposeResult(id=item_id, status=status))
