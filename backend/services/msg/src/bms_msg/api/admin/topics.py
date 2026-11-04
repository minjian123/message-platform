"""主题管理接口（`/api/v1/msg/admin/topics`）：创建 / 列表 / 详情（详细设计 §8）。

- **权限码**：`msg:manage`（创建）、`msg:query`（列表 / 详情），经 `require_permission` 依赖声明；
  登录态与服务身份由网关与基座中间件承担（详细设计 §8.1 鉴权口径，05 域统一接入）。
- **租户**：唯一出口 `get_current_tenant_context()`；请求体不接收 `tenant`（防自报冒充）。
- **数据源**：元数据表在**平台库**，故会话取 `get_platform_uow`（`get_uow` 会按请求租户库选引擎）。
- **契约**：端点声明 `x-error-codes`，供跨仓 `check_modules` 的错误码反向对账。
"""

from typing import Annotated, cast

from bms_core.api.base import BaseRouter
from bms_core.core.concurrent import ConcurrentStableList
from bms_core.db.session import DbSession, get_platform_uow
from bms_core.db.unit_of_work import UnitOfWork
from bms_core.permission.base import require_permission
from bms_core.schemas.common import ApiResponse
from fastapi import Depends, Path, Query

from bms_msg.api.deps import CurrentTenantDep
from bms_msg.errors import MsgErrorCode
from bms_msg.models.partition import MsgPartition
from bms_msg.models.topic import MsgTopic
from bms_msg.repositories.topic import PartitionRepository, TopicRepository
from bms_msg.schemas.topic import (
    PartitionItem,
    TopicCreateRequest,
    TopicDetail,
    TopicItem,
    TopicPage,
)
from bms_msg.services.topic import MAX_LIST_SIZE, TopicService

__all__ = ["router"]

_QUERY = Depends(require_permission("msg:query"))
_MANAGE = Depends(require_permission("msg:manage"))

_QUERY_ERRORS = [
    int(MsgErrorCode.TOPIC_NOT_FOUND),
    int(MsgErrorCode.NAMESPACE_INVALID),
    int(MsgErrorCode.TENANT_SUSPENDED),
    int(MsgErrorCode.TENANT_NOT_FOUND),
]
"""查询端点声明的错误码（`x-error-codes`）。"""

_CREATE_ERRORS = [
    int(MsgErrorCode.TOPIC_EXISTS),
    int(MsgErrorCode.PARTITION_COUNT_INVALID),
    int(MsgErrorCode.NAMESPACE_INVALID),
    int(MsgErrorCode.TENANT_SUSPENDED),
    int(MsgErrorCode.TENANT_NOT_FOUND),
]
"""创建端点声明的错误码（`x-error-codes`）。"""

router = BaseRouter(
    key="msg_admin_topics",
    prefix="/msg/admin/topics",
    tags=["msg-admin-topics"],
)

PlatformUowDep = Annotated[UnitOfWork, Depends(get_platform_uow)]
"""平台库工作单元依赖（元数据表归平台库）。"""

TopicIdPath = Annotated[int, Path(description="主题 ID（雪花）")]
"""主题 ID 路径参数。"""


def _service(uow: UnitOfWork) -> TopicService:
    """组装主题服务（请求级平台库会话注入）。

    Args:
        uow: 平台库工作单元。

    Returns:
        TopicService: 主题服务。
    """
    session = cast("DbSession", uow.session)
    return TopicService(TopicRepository(session), PartitionRepository(session), uow)


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


def _detail(topic: MsgTopic, partitions: ConcurrentStableList[MsgPartition]) -> TopicDetail:
    """主题 + 分区 → 详情响应。

    Args:
        topic: 主题记录。
        partitions: 分区记录（按分区号升序）。

    Returns:
        TopicDetail: 详情响应。
    """
    payload = _item(topic).model_dump()
    return TopicDetail.model_validate(
        {**payload, "partitions": ConcurrentStableList(_partition(partition) for partition in partitions)}
    )


@router.post("", dependencies=[_MANAGE], openapi_extra={"x-error-codes": _CREATE_ERRORS})
async def create_topic(
    req: TopicCreateRequest,
    tenant: CurrentTenantDep,
    uow: PlatformUowDep,
) -> ApiResponse[TopicItem]:
    """创建主题（指定分区数，返回完整元数据）。

    需要 msg:manage 权限；`tenant_id` 取自基座解析链（请求体不接收）；同租户 / 命名空间下重名
    返回 350008，分区数或保留期越界返回 350009，命名空间非已启用 module_key 返回 350010。
    """
    topic = await _service(uow).create_topic(tenant=tenant, request=req)
    return ApiResponse.ok(_item(topic))


@router.get("", dependencies=[_QUERY], openapi_extra={"x-error-codes": _QUERY_ERRORS})
async def list_topics(
    tenant: CurrentTenantDep,
    uow: PlatformUowDep,
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
    return ApiResponse.ok(
        TopicPage(
            items=ConcurrentStableList(_item(topic) for topic in items),
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
) -> ApiResponse[TopicDetail]:
    """主题详情（含分区列表）。

    需要 msg:query 权限；不存在或不属于当前租户返回 350001。
    """
    topic, partitions = await _service(uow).get_topic(tenant=tenant, topic_id=topic_id)
    return ApiResponse.ok(_detail(topic, partitions))
