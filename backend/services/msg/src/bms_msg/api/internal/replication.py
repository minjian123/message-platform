"""内部复制 / 控制端点（02-8）：8001 专用，仅平台网络可达。

**与主应用的关系**：本路由只挂内部 ASGI 应用，主应用 8000 看不到 `/internal/*`；
运行态直接复用主应用装配的 `ReplicationCoordinator`（同一份进程内状态）。

**错误体**：`HTTPException(status_code, detail={reason, ...})`，不新增产品错误码；
`reason` 闭集见 02-8 详细设计 §5.2。
"""

from __future__ import annotations

from typing import Annotated, NoReturn, cast

from bms_core.api.base import require_service
from bms_core.core.concurrent import ConcurrentStableDict, ConcurrentStableList
from bms_core.core.logging import get_logger
from bms_core.oauth.verify import VerifiedToken
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import Response

from bms_msg import SERVICE_NAME
from bms_msg.api.internal.schemas import (
    DemoteRequest,
    FetchRequest,
    FetchResponse,
    HealthResponse,
    InternalMessage,
    SnapshotResponse,
    SwitchApplyRequest,
    SwitchApplyResponse,
)
from bms_msg.broker.replication import (
    ReplicaFetchRequest,
    ReplicationCoordinator,
    ReplicationDivergedError,
    ReplicaUnavailableError,
    StaleLeaderError,
    SwitchApplyCommand,
)
from bms_msg.core.config import REPLICATION_INTERNAL_PROTOCOL_VERSION

__all__ = ["ServiceIdentity", "router"]

_LOGGER = get_logger("bms_msg.internal")

router = APIRouter(prefix="/internal/replication", tags=["internal-replication"])

ServiceIdentity = Annotated[VerifiedToken, Depends(require_service(SERVICE_NAME))]
"""入站服务身份依赖（`aud=service` + 调用方白名单）；healthz 之外全部要求。"""


def _coordinator(request: Request) -> ReplicationCoordinator:
    """取本节点的复制协调器（内部应用共享主应用 state）。

    Args:
        request: 当前请求。

    Returns:
        ReplicationCoordinator: 复制协调器。
    """
    return cast("ReplicationCoordinator", request.app.state.replication)


def _node_id(request: Request) -> str:
    """取本节点标识。

    Args:
        request: 当前请求。

    Returns:
        str: 节点标识（复制未装配时回 `unknown`）。
    """
    coordinator = getattr(request.app.state, "replication", None)
    if coordinator is None:
        return "unknown"
    return str(cast("ReplicationCoordinator", coordinator).config.node_id)


def _fail(status_code: int, reason: str, **extra: object) -> NoReturn:
    """抛内部错误体（`reason` 闭集 + 附加证据）。

    Args:
        status_code: HTTP 状态码。
        reason: 结构化原因。
        **extra: 附加证据（如 `leader_leo`）。

    Raises:
        HTTPException: 恒抛。
    """
    detail: ConcurrentStableDict[str, object] = ConcurrentStableDict({"reason": reason})
    detail.update(extra)
    raise HTTPException(status_code=status_code, detail=dict(detail.items()))


def _assert_protocol(version: int) -> None:
    """校验内部协议版本（未知版本按不可达处理，不做尽力解析）。

    Args:
        version: 请求携带的版本。

    Raises:
        HTTPException: 版本不匹配（400 `protocol`）。
    """
    if version != REPLICATION_INTERNAL_PROTOCOL_VERSION:
        _fail(400, "protocol", expected=REPLICATION_INTERNAL_PROTOCOL_VERSION, received=version)


@router.get("/healthz", response_model=HealthResponse)
async def healthz(request: Request) -> HealthResponse:
    """内部健康探针（唯一不鉴权端点）。

    Args:
        request: 当前请求。

    Returns:
        HealthResponse: 节点标识与协议版本。
    """
    return HealthResponse(
        status="ok",
        node_id=_node_id(request),
        protocol_version=REPLICATION_INTERNAL_PROTOCOL_VERSION,
    )


@router.post("/fetch", response_model=FetchResponse)
async def fetch(payload: FetchRequest, request: Request, _: ServiceIdentity) -> FetchResponse:
    """leader 侧服务一次 follower 拉取。

    Args:
        payload: 拉取请求。
        request: 当前请求。
        _: 服务身份（鉴权依赖）。

    Returns:
        FetchResponse: 记录与 leader 运行态。

    Raises:
        HTTPException: 409 `stale_epoch` / 410 `diverged` / 503 `not_leader` / `switching` / `unavailable`。
    """
    _assert_protocol(payload.protocol_version)
    coordinator = _coordinator(request)
    try:
        result = await coordinator.serve_fetch(
            ReplicaFetchRequest(
                leader=payload.leader,
                follower=payload.follower,
                topic_id=payload.topic_id,
                partition_no=payload.partition_no,
                from_offset=payload.from_offset,
                max_messages=payload.max_messages,
                epoch=payload.epoch,
            )
        )
    except StaleLeaderError as exc:
        _fail(409, "stale_epoch", detail=str(exc))
    except ReplicationDivergedError as exc:
        _fail(410, "diverged", detail=str(exc), leader_leo=exc.leader_leo)
    except ReplicaUnavailableError as exc:
        _fail(503, exc.reason, detail=str(exc))
    records: ConcurrentStableList[InternalMessage] = ConcurrentStableList(
        InternalMessage.from_stored(message) for message in result.records
    )
    return FetchResponse(
        leader_epoch=result.leader_epoch,
        from_offset=result.from_offset,
        records=records,
        leader_leo=result.leader_leo,
        high_watermark=result.high_watermark,
        leader_isr=ConcurrentStableList(result.leader_isr),
    )


@router.get("/partitions/{topic_id}/{partition_no}/snapshot", response_model=SnapshotResponse)
async def snapshot(topic_id: int, partition_no: int, request: Request, _: ServiceIdentity) -> SnapshotResponse:
    """读本节点分区复制运行态快照。

    Args:
        topic_id: 主题主键。
        partition_no: 分区号。
        request: 当前请求。
        _: 服务身份（鉴权依赖）。

    Returns:
        SnapshotResponse: 运行态快照。

    Raises:
        HTTPException: 404 `unavailable`（未挂载）。
    """
    state = await _coordinator(request).snapshot(topic_id=topic_id, partition_no=partition_no)
    if state is None:
        _fail(404, "unavailable", detail="分区未挂载或快照失败")
    return SnapshotResponse(
        leader=state.leader,
        epoch=state.epoch,
        leo=state.leo,
        hw=state.hw,
        isr=ConcurrentStableList(state.isr),
        replicas=ConcurrentStableList(state.replicas),
        switch_state=state.switch_state,
        writable=state.writable,
        last_known_hw=state.last_known_hw,
        last_known_isr=ConcurrentStableList(state.last_known_isr),
        last_known_at=state.last_known_at,
        start_offset=state.start_offset,
    )


@router.post("/partitions/{topic_id}/{partition_no}/demote", status_code=204)
async def demote(
    topic_id: int, partition_no: int, payload: DemoteRequest, request: Request, _: ServiceIdentity
) -> Response:
    """通知旧 leader 立即降级（不可达时由调用方靠租约兜底）。

    Args:
        topic_id: 主题主键。
        partition_no: 分区号。
        payload: 降级请求。
        request: 当前请求。
        _: 服务身份（鉴权依赖）。

    Returns:
        Response: 204 空响应。

    Raises:
        HTTPException: 503 `unavailable`（降级失败）。
    """
    try:
        await _coordinator(request).demote(
            topic_id=topic_id,
            partition_no=partition_no,
            to_leader=payload.to_leader,
            to_epoch=payload.to_epoch,
        )
    except Exception as exc:
        _LOGGER.warning(
            "msg_internal_demote_failed", topic_id=topic_id, partition=partition_no, reason=type(exc).__name__
        )
        _fail(503, "unavailable", detail=str(exc))
    return Response(status_code=204)


@router.post("/partitions/{topic_id}/{partition_no}/apply-switch", response_model=SwitchApplyResponse)
async def apply_switch(
    topic_id: int, partition_no: int, payload: SwitchApplyRequest, request: Request, _: ServiceIdentity
) -> SwitchApplyResponse:
    """让目标节点截断 / 保留尾部并激活。

    Args:
        topic_id: 主题主键。
        partition_no: 分区号。
        payload: 激活请求。
        request: 当前请求。
        _: 服务身份（鉴权依赖）。

    Returns:
        SwitchApplyResponse: 激活结果。

    Raises:
        HTTPException: 404 `unavailable`（未挂载）/ 409 `switch_conflict` / 503 `unavailable`。
    """
    _assert_protocol(payload.protocol_version)
    if payload.topic_id != topic_id or payload.partition_no != partition_no:
        _fail(409, "switch_conflict", detail="路径与命令体分区不一致")
    try:
        result = await _coordinator(request).apply_switch(
            SwitchApplyCommand(
                topic_id=payload.topic_id,
                partition_no=payload.partition_no,
                from_leader=payload.from_leader,
                to_leader=payload.to_leader,
                to_epoch=payload.to_epoch,
                watermark=payload.watermark,
                truncate=payload.truncate,
                grace_ms=payload.grace_ms,
            )
        )
    except ReplicaUnavailableError as exc:
        status = 404 if exc.reason == "unavailable" else 503
        _fail(status, exc.reason, detail=str(exc))
    except Exception as exc:
        _LOGGER.warning(
            "msg_internal_apply_switch_failed", topic_id=topic_id, partition=partition_no, reason=type(exc).__name__
        )
        _fail(503, "unavailable", detail=str(exc))
    return SwitchApplyResponse(
        to_leo=result.to_leo,
        truncated_to=result.truncated_to,
        retained_tail=result.retained_tail,
        already_applied=result.already_applied,
    )
