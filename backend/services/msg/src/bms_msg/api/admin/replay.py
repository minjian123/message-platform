"""重放任务接口（`/api/v1/msg/admin/replay`；01-6 详细设计 §8、06-6-1 详细设计 §6~§7）。

- **权限码**：建任务 / 暂停 / 恢复 / 取消用 `msg:manage`，列表与进度查询用 `msg:query`（与概要 §3.3 同口径）。
- **租户**：唯一出口 `get_current_tenant_context()`；服务层再做一次显式归属校验（`350016`），
  不让「别人的任务」泄露存在性。
- **审计**：建任务与取消**在请求侧**写（有真实操作人与 IP），完成 / 失败由执行器写（§7.2）——
  「谁迁移谁记」，避免同一终态两条留痕。
- **契约**：端点声明 `x-error-codes`，供跨仓 `check_modules` 的错误码反向对账。
- **影响视图（06-6-1）**：详情 additive 返回 `affected_groups`；列表返回 null（逐行实时积压会随
  行数×组数×分区数放大，列表只回摘要）。影响只读，不改任务状态机。
"""

from datetime import datetime
from typing import Annotated, cast

from bms_core.api.base import BaseRouter
from bms_core.core.concurrent import ConcurrentStableDict, ConcurrentStableList
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
from bms_msg.audit.trail import REPLAY_DETAIL_ACTION, AuditEmitter, emit_sensitive
from bms_msg.broker.group import GroupStore
from bms_msg.broker.log import MessageStore
from bms_msg.broker.offset import OffsetStore
from bms_msg.broker.replay import AuditSink, ReplayAuditor, ReplayService
from bms_msg.broker.router import TopicRouter
from bms_msg.errors import MsgErrorCode
from bms_msg.models.replay import MsgReplayTask
from bms_msg.repositories.replay import ReplayTaskRepository
from bms_msg.repositories.topic import TopicRepository
from bms_msg.schemas.replay import AffectedGroupItem, ReplayRequest, ReplayTaskItem, ReplayTaskPage
from bms_msg.services.replay_impact import AffectedGroup, ReplayImpactInspector, build_impact_inspector
from bms_msg.services.topic import MAX_LIST_SIZE

__all__ = ["router"]

_LOGGER = get_logger("bms_msg.admin_replay")

_MANAGE = Depends(require_permission("msg:manage"))
_QUERY = Depends(require_permission("msg:query"))

_MANAGE_ERRORS = [
    int(MsgErrorCode.REPLAY_TASK_NOT_FOUND),
    int(MsgErrorCode.REPLAY_TASK_INVALID_STATE),
    int(MsgErrorCode.TOPIC_NOT_FOUND),
    int(MsgErrorCode.PARTITION_COUNT_INVALID),
    int(MsgErrorCode.NAMESPACE_INVALID),
    int(MsgErrorCode.TENANT_SUSPENDED),
    int(MsgErrorCode.TENANT_NOT_FOUND),
    int(MsgErrorCode.ACL_DENIED),
]
"""写操作端点声明的错误码（`x-error-codes`）。"""

_QUERY_ERRORS = [
    int(MsgErrorCode.REPLAY_TASK_NOT_FOUND),
    int(MsgErrorCode.TENANT_SUSPENDED),
    int(MsgErrorCode.TENANT_NOT_FOUND),
    int(MsgErrorCode.ACL_DENIED),
]
"""进度查询端点声明的错误码（`x-error-codes`）。"""

_LIST_ERRORS = [
    int(MsgErrorCode.NAMESPACE_INVALID),
    int(MsgErrorCode.TENANT_SUSPENDED),
    int(MsgErrorCode.TENANT_NOT_FOUND),
]
"""列表端点声明的错误码（`x-error-codes`；分页 / 状态非法走框架级 `10001`，不入产品码表）。"""

router = BaseRouter(
    key="msg_admin_replay",
    prefix="/msg/admin/replay",
    tags=["msg-admin-replay"],
)

PlatformUowDep = Annotated[UnitOfWork, Depends(get_platform_uow)]
"""平台库工作单元依赖（任务表归平台库）。"""

TaskIdPath = Annotated[int, Path(description="重放任务 ID（雪花）")]
"""任务 ID 路径参数。"""


def _require_confirm(confirm: bool, *, action: str, **scope: str) -> None:
    """校验高危写操作的显式确认参数（缺失即拒绝并留痕）。

    **为什么用 10001 而不是产品码**：缺参数是请求契约问题（改请求即可解），不是业务状态问题；
    留痕走结构化日志而不是审计——审计的「动作 ↔ 留痕」映射针对**已生效**的操作，被拒的请求写进去
    会与真实操作混在一起（05-4 口径）。

    Args:
        confirm: 请求携带的确认值。
        action: 被拒动作（日志分类用）。
        scope: 作用域字段（命名空间 / 主题等，日志定位用）。

    Raises:
        ParamError: `confirm` 不为 true（10001）。
    """
    if confirm:
        return
    _LOGGER.warning("msg_write_confirm_missing", action=action, **scope)
    raise ParamError("高危写操作须显式确认（confirm=true）")


def _impact(groups: GroupStore, offsets: OffsetStore, store: MessageStore) -> ReplayImpactInspector:
    """组装影响投影（三个进程级端口，无数据库会话）。

    Args:
        groups: 组存储（解析受影响组）。
        offsets: 位点存储（取已提交位点）。
        store: 消息存储（取分区 LEO）。

    Returns:
        ReplayImpactInspector: 影响投影。
    """
    return build_impact_inspector(groups=groups, offsets=offsets, messages=store)


def _service(uow: UnitOfWork, store: MessageStore, impact: ReplayImpactInspector) -> ReplayService:
    """组装重放服务（请求级平台库会话 + 进程级消息存储 + 影响投影）。

    Args:
        uow: 平台库工作单元。
        store: 消息存储。
        impact: 影响投影。

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


def _affected(item: AffectedGroup) -> AffectedGroupItem:
    """影响行 → 响应明细。

    Args:
        item: 影响视图的一行。

    Returns:
        AffectedGroupItem: 响应明细。
    """
    return AffectedGroupItem(
        group=item.group,
        baseline_lag=item.baseline_lag,
        current_lag=item.current_lag,
        delta=item.delta,
        captured_at=item.captured_at,
    )


def _item(
    task: MsgReplayTask,
    *,
    affected: ConcurrentStableList[AffectedGroup] | None = None,
    truncated: bool = False,
) -> ReplayTaskItem:
    """任务行 → 响应明细（`filter_key` 在契约里叫 `key`；影响段仅详情填充）。

    Args:
        task: 任务行。
        affected: 影响视图；None 表示本次不采集（列表 / 写操作响应）。
        truncated: 影响基线是否被组数上限截断。

    Returns:
        ReplayTaskItem: 响应明细。
    """
    return ReplayTaskItem(
        task_id=int(task.id),
        namespace=task.namespace,
        topic=task.topic,
        partitions=ConcurrentStableList(task.partitions),
        offset_from=task.offset_from,
        offset_to=task.offset_to,
        time_from=task.time_from,
        time_to=task.time_to,
        key=task.filter_key,
        rate_per_second=int(task.rate_per_second),
        status=task.status,
        total=int(task.total),
        replayed=int(task.replayed),
        log_id_first=task.log_id_first,
        log_id_last=task.log_id_last,
        created_at=task.created_at,
        finished_at=task.finished_at,
        error_msg=task.error_msg,
        affected_groups=None if affected is None else ConcurrentStableList(_affected(row) for row in affected),
        affected_truncated=truncated,
    )


async def _visible(
    acl: TopicAclServiceDep,
    tenant: CurrentTenantDep,
    rows: ConcurrentStableList[MsgReplayTask],
) -> ConcurrentStableList[MsgReplayTask]:
    """按主题 `describe` 过滤当页任务（同主题只判一次 ACL）。

    Args:
        acl: ACL 守卫。
        tenant: 当前租户。
        rows: 当页任务。

    Returns:
        ConcurrentStableList[MsgReplayTask]: 允许查看的任务行（保持入参顺序）。
    """
    granted: ConcurrentStableDict[tuple[str, str], bool] = ConcurrentStableDict()
    visible: ConcurrentStableList[MsgReplayTask] = ConcurrentStableList()
    for row in rows:
        key = (row.namespace, row.topic)
        if key not in granted:
            allowed = await acl.filter_allowed(
                tenant_id=int(tenant.tenant_id),
                namespace=row.namespace,
                topics=ConcurrentStableList([row.topic]),
                action=ACL_ACTION_DESCRIBE,
            )
            granted.set(key, bool(allowed))
        if granted[key]:
            visible.add(row)
    return visible


def _auditor(sink: AuditSink) -> ReplayAuditor:
    """组装审计记录器（只依赖一个 emit 出口）。

    Args:
        sink: 审计出口。

    Returns:
        ReplayAuditor: 审计记录器。
    """
    return ReplayAuditor(sink=sink)


@router.get("", dependencies=[_QUERY], openapi_extra={"x-error-codes": _LIST_ERRORS})
async def list_replay(
    tenant: CurrentTenantDep,
    uow: PlatformUowDep,
    store: MessageStoreDep,
    groups: GroupStoreDep,
    offsets: OffsetStoreDep,
    acl: TopicAclServiceDep,
    status: Annotated[str | None, Query(max_length=16, description="状态过滤（六态之一）")] = None,
    namespace: Annotated[str | None, Query(max_length=64, description="命名空间过滤")] = None,
    topic: Annotated[str | None, Query(max_length=200, description="主题名过滤")] = None,
    created_from: Annotated[datetime | None, Query(description="创建时间下限（含，UTC）")] = None,
    created_to: Annotated[datetime | None, Query(description="创建时间上限（不含，UTC）")] = None,
    page: Annotated[int, Query(ge=1, description="页码（从 1 起）")] = 1,
    size: Annotated[int, Query(ge=1, le=MAX_LIST_SIZE, description="每页条数")] = 20,
) -> ApiResponse[ReplayTaskPage]:
    """重放任务列表（创建时间倒序；可按状态 / 命名空间 / 主题 / 创建时间过滤）。

    需要 msg:query 权限；按租户隔离；无主题 `describe` 权限的任务不出现在 items（`total` 仍为过滤前
    计数，与 06-1 组列表同口径）。状态取值非法 / 分页越界 / 创建时间区间颠倒返回 10001。
    """
    rows, total = await _service(uow, store, _impact(groups, offsets, store)).list(
        tenant=tenant,
        page=page,
        size=size,
        status=status,
        namespace=namespace,
        topic=topic,
        created_from=created_from,
        created_to=created_to,
    )
    visible = await _visible(acl, tenant, rows)
    return ApiResponse.ok(
        ReplayTaskPage(
            items=ConcurrentStableList(_item(row) for row in visible),
            total=total,
            page=page,
            size=size,
        )
    )


@router.post("", dependencies=[_MANAGE], openapi_extra={"x-error-codes": _MANAGE_ERRORS})
async def create_replay(
    req: ReplayRequest,
    tenant: CurrentTenantDep,
    uow: PlatformUowDep,
    store: MessageStoreDep,
    groups: GroupStoreDep,
    offsets: OffsetStoreDep,
    runner: ReplayRunnerDep,
    sink: AuditSinkDep,
    acl: TopicAclServiceDep,
) -> ApiResponse[ReplayTaskItem]:
    """建重放任务（按条件把历史再写一遍，返回任务快照）。

    需要 msg:manage 权限；范围条件左闭右开、跨类相与，`partitions` 缺省为全部分区。
    命名空间非法 350010，主题不存在或未启用 350001，分区号越界 350009，区间次序非法 10001。
    任务落库前采集影响基线（受影响组 + 分区级积压）；入口先写「建任务」审计，再惰性启动执行器
    （保证留痕顺序先创建后终态）。
    """
    # 鉴权先于确认校验：无权限的调用方不该先拿到「缺 confirm」这类参数级提示。
    await acl.authorize(
        tenant_id=int(tenant.tenant_id),
        namespace=req.namespace,
        topic=req.topic,
        action=ACL_ACTION_MANAGE,
    )
    _require_confirm(req.confirm, action="msg:replay", namespace=req.namespace, topic=req.topic)
    task = await _service(uow, store, _impact(groups, offsets, store)).create(tenant=tenant, request=req)
    await _auditor(sink).record_created(
        task=task,
        actor_id=get_current_user_id(),
        ip=get_current_client_ip(),
        trace_id=get_current_trace_id(),
    )
    runner.ensure_started()
    return ApiResponse.ok(_item(task))


@router.get("/{task_id}", dependencies=[_QUERY], openapi_extra={"x-error-codes": _QUERY_ERRORS})
async def get_replay(
    task_id: TaskIdPath,
    tenant: CurrentTenantDep,
    uow: PlatformUowDep,
    store: MessageStoreDep,
    groups: GroupStoreDep,
    offsets: OffsetStoreDep,
    acl: TopicAclServiceDep,
    sink: AuditSinkDep,
) -> ApiResponse[ReplayTaskItem]:
    """查重放任务进度（已投递 / 总量 + 状态 + 首末记录标识 + 受影响组影响视图）。

    需要 msg:query 权限；任务不存在或不属于当前租户返回 350016。任务已终结且尚无影响快照时，
    本次查询顺带落一次快照（之后恒返回该快照），任务本身的状态与进度不变。
    """
    view = await _service(uow, store, _impact(groups, offsets, store)).get_detail(tenant=tenant, task_id=task_id)
    await acl.authorize(
        tenant_id=int(tenant.tenant_id),
        namespace=view.task.namespace,
        topic=view.task.topic,
        action=ACL_ACTION_DESCRIBE,
    )
    await emit_sensitive(
        cast("AuditEmitter", sink),
        action=REPLAY_DETAIL_ACTION,
        tenant_id=int(tenant.tenant_id),
        params=ConcurrentStableDict({"task_id": str(view.task.id)}),
        method="GET",
        path=f"/api/v1/msg/admin/replay/{task_id}",
        module_key=view.task.namespace,
        resource_key=view.task.topic,
        session=uow.session,
    )
    return ApiResponse.ok(_item(view.task, affected=view.affected, truncated=view.truncated))


@router.post("/{task_id}/pause", dependencies=[_MANAGE], openapi_extra={"x-error-codes": _MANAGE_ERRORS})
async def pause_replay(
    task_id: TaskIdPath,
    tenant: CurrentTenantDep,
    uow: PlatformUowDep,
    store: MessageStoreDep,
    groups: GroupStoreDep,
    offsets: OffsetStoreDep,
    acl: TopicAclServiceDep,
) -> ApiResponse[ReplayTaskItem]:
    """暂停任务（批边界生效：当前批写完才停，进度与写入始终一致）。

    需要 msg:manage 权限；任务不存在 350016，已暂停或已终态 350017。
    """
    impact = _impact(groups, offsets, store)
    current = await _service(uow, store, impact).get(tenant=tenant, task_id=task_id)
    await acl.authorize(
        tenant_id=int(tenant.tenant_id),
        namespace=current.namespace,
        topic=current.topic,
        action=ACL_ACTION_MANAGE,
    )
    task = await _service(uow, store, impact).pause(tenant=tenant, task_id=task_id)
    return ApiResponse.ok(_item(task))


@router.post("/{task_id}/resume", dependencies=[_MANAGE], openapi_extra={"x-error-codes": _MANAGE_ERRORS})
async def resume_replay(
    task_id: TaskIdPath,
    tenant: CurrentTenantDep,
    uow: PlatformUowDep,
    store: MessageStoreDep,
    groups: GroupStoreDep,
    offsets: OffsetStoreDep,
    acl: TopicAclServiceDep,
) -> ApiResponse[ReplayTaskItem]:
    """恢复任务（仅接受已暂停的任务）。

    需要 msg:manage 权限；任务不存在 350016，未处于暂停 350017。
    """
    impact = _impact(groups, offsets, store)
    current = await _service(uow, store, impact).get(tenant=tenant, task_id=task_id)
    await acl.authorize(
        tenant_id=int(tenant.tenant_id),
        namespace=current.namespace,
        topic=current.topic,
        action=ACL_ACTION_MANAGE,
    )
    task = await _service(uow, store, impact).resume(tenant=tenant, task_id=task_id)
    return ApiResponse.ok(_item(task))


@router.post("/{task_id}/cancel", dependencies=[_MANAGE], openapi_extra={"x-error-codes": _MANAGE_ERRORS})
async def cancel_replay(
    task_id: TaskIdPath,
    tenant: CurrentTenantDep,
    uow: PlatformUowDep,
    store: MessageStoreDep,
    groups: GroupStoreDep,
    offsets: OffsetStoreDep,
    sink: AuditSinkDep,
    acl: TopicAclServiceDep,
) -> ApiResponse[ReplayTaskItem]:
    """取消任务（非终态可取消；已写入的记录不回滚——重放是新增写入，消费端仍按 message_id 幂等）。

    需要 msg:manage 权限；任务不存在 350016，已是终态 350017。取消在请求侧写终态审计（操作人可查）。
    """
    impact = _impact(groups, offsets, store)
    current = await _service(uow, store, impact).get(tenant=tenant, task_id=task_id)
    await acl.authorize(
        tenant_id=int(tenant.tenant_id),
        namespace=current.namespace,
        topic=current.topic,
        action=ACL_ACTION_MANAGE,
    )
    task = await _service(uow, store, impact).cancel(tenant=tenant, task_id=task_id)
    await _auditor(sink).record_cancelled(
        task=task,
        actor_id=get_current_user_id(),
        ip=get_current_client_ip(),
        trace_id=get_current_trace_id(),
    )
    return ApiResponse.ok(_item(task))
