"""重放任务接口（`/api/v1/msg/admin/replay`；01-6 详细设计 §8）。

- **权限码**：建任务 / 暂停 / 恢复 / 取消用 `msg:manage`，进度查询用 `msg:query`（与概要 §3.3 同口径）。
- **租户**：唯一出口 `get_current_tenant_context()`；服务层再做一次显式归属校验（`350016`），
  不让「别人的任务」泄露存在性。
- **审计**：建任务与取消**在请求侧**写（有真实操作人与 IP），完成 / 失败由执行器写（§7.2）——
  「谁迁移谁记」，避免同一终态两条留痕。
- **契约**：端点声明 `x-error-codes`，供跨仓 `check_modules` 的错误码反向对账。
"""

from typing import Annotated, cast

from bms_core.api.base import BaseRouter
from bms_core.core.concurrent import ConcurrentStableList
from bms_core.core.context import get_current_client_ip, get_current_trace_id, get_current_user_id
from bms_core.db.session import DbSession, get_platform_uow
from bms_core.db.unit_of_work import UnitOfWork
from bms_core.permission.base import require_permission
from bms_core.schemas.common import ApiResponse
from fastapi import Depends, Path

from bms_msg.acl.keys import ACL_ACTION_DESCRIBE, ACL_ACTION_MANAGE
from bms_msg.api.deps import AuditSinkDep, CurrentTenantDep, MessageStoreDep, ReplayRunnerDep, TopicAclServiceDep
from bms_msg.broker.log import MessageStore
from bms_msg.broker.replay import AuditSink, ReplayAuditor, ReplayService
from bms_msg.broker.router import TopicRouter
from bms_msg.errors import MsgErrorCode
from bms_msg.models.replay import MsgReplayTask
from bms_msg.repositories.replay import ReplayTaskRepository
from bms_msg.repositories.topic import TopicRepository
from bms_msg.schemas.replay import ReplayRequest, ReplayTaskItem

__all__ = ["router"]

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
]
"""写操作端点声明的错误码（`x-error-codes`）。"""

_QUERY_ERRORS = [
    int(MsgErrorCode.REPLAY_TASK_NOT_FOUND),
    int(MsgErrorCode.TENANT_SUSPENDED),
    int(MsgErrorCode.TENANT_NOT_FOUND),
]
"""进度查询端点声明的错误码（`x-error-codes`）。"""

router = BaseRouter(
    key="msg_admin_replay",
    prefix="/msg/admin/replay",
    tags=["msg-admin-replay"],
)

PlatformUowDep = Annotated[UnitOfWork, Depends(get_platform_uow)]
"""平台库工作单元依赖（任务表归平台库）。"""

TaskIdPath = Annotated[int, Path(description="重放任务 ID（雪花）")]
"""任务 ID 路径参数。"""


def _service(uow: UnitOfWork, store: MessageStore) -> ReplayService:
    """组装重放服务（请求级平台库会话 + 进程级消息存储）。

    Args:
        uow: 平台库工作单元。
        store: 消息存储。

    Returns:
        ReplayService: 重放任务服务。
    """
    session = cast("DbSession", uow.session)
    return ReplayService(
        tasks=ReplayTaskRepository(session),
        router=TopicRouter(TopicRepository(session)),
        store=store,
        uow=uow,
    )


def _item(task: MsgReplayTask) -> ReplayTaskItem:
    """任务行 → 响应明细（`filter_key` 在契约里叫 `key`）。

    Args:
        task: 任务行。

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
    )


def _auditor(sink: AuditSink) -> ReplayAuditor:
    """组装审计记录器（只依赖一个 emit 出口）。

    Args:
        sink: 审计出口。

    Returns:
        ReplayAuditor: 审计记录器。
    """
    return ReplayAuditor(sink=sink)


@router.post("", dependencies=[_MANAGE], openapi_extra={"x-error-codes": _MANAGE_ERRORS})
async def create_replay(
    req: ReplayRequest,
    tenant: CurrentTenantDep,
    uow: PlatformUowDep,
    store: MessageStoreDep,
    runner: ReplayRunnerDep,
    sink: AuditSinkDep,
    acl: TopicAclServiceDep,
) -> ApiResponse[ReplayTaskItem]:
    """建重放任务（按条件把历史再写一遍，返回任务快照）。

    需要 msg:manage 权限；范围条件左闭右开、跨类相与，`partitions` 缺省为全部分区。
    命名空间非法 350010，主题不存在或未启用 350001，分区号越界 350009，区间次序非法 10001。
    入口先写「建任务」审计，再惰性启动执行器（保证留痕顺序先创建后终态）。
    """
    await acl.authorize(
        tenant_id=int(tenant.tenant_id),
        namespace=req.namespace,
        topic=req.topic,
        action=ACL_ACTION_MANAGE,
    )
    task = await _service(uow, store).create(tenant=tenant, request=req)
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
    acl: TopicAclServiceDep,
) -> ApiResponse[ReplayTaskItem]:
    """查重放任务进度（已投递 / 总量 + 状态 + 首末记录标识）。

    需要 msg:query 权限；任务不存在或不属于当前租户返回 350016。
    """
    task = await _service(uow, store).get(tenant=tenant, task_id=task_id)
    await acl.authorize(
        tenant_id=int(tenant.tenant_id),
        namespace=task.namespace,
        topic=task.topic,
        action=ACL_ACTION_DESCRIBE,
    )
    return ApiResponse.ok(_item(task))


@router.post("/{task_id}/pause", dependencies=[_MANAGE], openapi_extra={"x-error-codes": _MANAGE_ERRORS})
async def pause_replay(
    task_id: TaskIdPath,
    tenant: CurrentTenantDep,
    uow: PlatformUowDep,
    store: MessageStoreDep,
    acl: TopicAclServiceDep,
) -> ApiResponse[ReplayTaskItem]:
    """暂停任务（批边界生效：当前批写完才停，进度与写入始终一致）。

    需要 msg:manage 权限；任务不存在 350016，已暂停或已终态 350017。
    """
    current = await _service(uow, store).get(tenant=tenant, task_id=task_id)
    await acl.authorize(
        tenant_id=int(tenant.tenant_id),
        namespace=current.namespace,
        topic=current.topic,
        action=ACL_ACTION_MANAGE,
    )
    task = await _service(uow, store).pause(tenant=tenant, task_id=task_id)
    return ApiResponse.ok(_item(task))


@router.post("/{task_id}/resume", dependencies=[_MANAGE], openapi_extra={"x-error-codes": _MANAGE_ERRORS})
async def resume_replay(
    task_id: TaskIdPath,
    tenant: CurrentTenantDep,
    uow: PlatformUowDep,
    store: MessageStoreDep,
    acl: TopicAclServiceDep,
) -> ApiResponse[ReplayTaskItem]:
    """恢复任务（仅接受已暂停的任务）。

    需要 msg:manage 权限；任务不存在 350016，未处于暂停 350017。
    """
    current = await _service(uow, store).get(tenant=tenant, task_id=task_id)
    await acl.authorize(
        tenant_id=int(tenant.tenant_id),
        namespace=current.namespace,
        topic=current.topic,
        action=ACL_ACTION_MANAGE,
    )
    task = await _service(uow, store).resume(tenant=tenant, task_id=task_id)
    return ApiResponse.ok(_item(task))


@router.post("/{task_id}/cancel", dependencies=[_MANAGE], openapi_extra={"x-error-codes": _MANAGE_ERRORS})
async def cancel_replay(
    task_id: TaskIdPath,
    tenant: CurrentTenantDep,
    uow: PlatformUowDep,
    store: MessageStoreDep,
    sink: AuditSinkDep,
    acl: TopicAclServiceDep,
) -> ApiResponse[ReplayTaskItem]:
    """取消任务（非终态可取消；已写入的记录不回滚——重放是新增写入，消费端仍按 message_id 幂等）。

    需要 msg:manage 权限；任务不存在 350016，已是终态 350017。取消在请求侧写终态审计（操作人可查）。
    """
    current = await _service(uow, store).get(tenant=tenant, task_id=task_id)
    await acl.authorize(
        tenant_id=int(tenant.tenant_id),
        namespace=current.namespace,
        topic=current.topic,
        action=ACL_ACTION_MANAGE,
    )
    task = await _service(uow, store).cancel(tenant=tenant, task_id=task_id)
    await _auditor(sink).record_cancelled(
        task=task,
        actor_id=get_current_user_id(),
        ip=get_current_client_ip(),
        trace_id=get_current_trace_id(),
    )
    return ApiResponse.ok(_item(task))
