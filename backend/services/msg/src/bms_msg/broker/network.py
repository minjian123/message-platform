"""真实网络复制 / 控制通道（02-8）：跨容器 HTTP 实现与节点地址映射。

**职责边界**：本模块只把 `ReplicaTransport` 与 `LeaderControlTransport` 的两个协议搬到 HTTP 上，
复制协调器与切换服务的判定逻辑零改动；进程内实现保留供单测。

**为什么用 httpx**：与仓内其它部件同栈（客户端 SDK 同源），异步连接池与超时语义明确；
传输层**不重试**——重试与退避归协调器拉取循环 / 切换服务，避免双重重试放大。
"""

from __future__ import annotations

import asyncio
import os
import time
from dataclasses import dataclass, field
from typing import cast
from urllib.parse import urlsplit

import httpx
from bms_core.core.concurrent import (
    ConcurrentStableDict,
    ConcurrentStableFrozenDict,
    ConcurrentStableFrozenList,
    ConcurrentStableList,
)
from bms_core.core.exceptions import ConfigError
from bms_core.core.logging import get_logger
from bms_core.core.objects import BaseFrameworkObject, BaseValueObject
from bms_core.oauth.token import BaseServiceTokenIssuer, ServiceTokenSpec

from bms_msg import SERVICE_NAME
from bms_msg.api.internal.schemas import (
    FetchRequest,
    FetchResponse,
    SnapshotResponse,
    SwitchApplyResponse,
)
from bms_msg.broker.election import LeaderControlTransport
from bms_msg.broker.replication import (
    ReplicaFetchRequest,
    ReplicaFetchResult,
    ReplicationCoordinator,
    ReplicationDivergedError,
    ReplicationSnapshot,
    ReplicaTransport,
    ReplicaUnavailableError,
    StaleLeaderError,
    SwitchApplyCommand,
    SwitchApplyResult,
)
from bms_msg.core.config import (
    DEFAULT_REPLICATION_FETCH_MAX_BYTES,
    DEFAULT_REPLICATION_INTERNAL_CONNECT_TIMEOUT_MS,
    DEFAULT_REPLICATION_INTERNAL_HOST,
    DEFAULT_REPLICATION_INTERNAL_PORT,
    DEFAULT_REPLICATION_INTERNAL_TIMEOUT_MS,
    REPLICATION_FETCH_MAX_BYTES_ENV,
    REPLICATION_INTERNAL_CONNECT_TIMEOUT_MS_ENV,
    REPLICATION_INTERNAL_HOST_ENV,
    REPLICATION_INTERNAL_PATH_PREFIX,
    REPLICATION_INTERNAL_PORT_ENV,
    REPLICATION_INTERNAL_PROTOCOL_VERSION,
    REPLICATION_INTERNAL_TIMEOUT_MS_ENV,
    REPLICATION_NODE_ENDPOINTS_ENV,
)

__all__ = [
    "HttpLeaderControlTransport",
    "HttpReplicaTransport",
    "InternalChannelConfig",
    "ServiceTokenProvider",
    "TransportErrorMetrics",
    "parse_node_endpoints",
]

_LOGGER = get_logger("bms_msg.network")

_TOKEN_REFRESH_RATIO = 0.8
"""令牌刷新提前量（按 TTL 的 80% 刷新，避免临界过期）。"""

_TOKEN_FALLBACK_TTL_SECONDS = 60
"""签发者未回 `expires_in` 时的兜底缓存时长（秒）。"""

_TRANSPORT_LOG_WINDOW_MS = 60_000
"""传输失败日志抑制窗口（毫秒；同节点同操作）。"""


def parse_node_endpoints(raw: str) -> ConcurrentStableFrozenDict[str, str]:
    """解析并校验节点地址映射（`node_id=http://host:port,...`）。

    Args:
        raw: 环境变量原值（逗号分隔，可空）。

    Returns:
        ConcurrentStableFrozenDict[str, str]: 节点标识 → 基地址（去尾斜杠，插入序）。

    Raises:
        ConfigError: 条目缺 `=` / 节点重复 / 非 http(s) / 缺 host / 带 path 或 query。
    """
    mapping: ConcurrentStableDict[str, str] = ConcurrentStableDict()
    for item in raw.split(","):
        entry = item.strip()
        if not entry:
            continue
        node, separator, url = entry.partition("=")
        node = node.strip()
        url = url.strip().rstrip("/")
        if not separator or not node or not url:
            raise ConfigError(f"{REPLICATION_NODE_ENDPOINTS_ENV} 条目非法（须为 node_id=http://host:port）：{entry!r}")
        if node in mapping:
            raise ConfigError(f"{REPLICATION_NODE_ENDPOINTS_ENV} 节点重复：{node}")
        parts = urlsplit(url)
        if parts.scheme not in ("http", "https") or not parts.hostname:
            raise ConfigError(f"{REPLICATION_NODE_ENDPOINTS_ENV} 地址非法（须为 http(s)://host:port）：{url!r}")
        if parts.path not in ("", "/") or parts.query or parts.fragment or parts.username or parts.password:
            raise ConfigError(f"{REPLICATION_NODE_ENDPOINTS_ENV} 地址只能含 scheme / host / port：{url!r}")
        mapping.set(node, url)
    return ConcurrentStableFrozenDict(mapping)


def _env_int(name: str, default: int) -> int:
    """读取整数环境变量。

    Args:
        name: 变量名。
        default: 缺省值。

    Returns:
        int: 解析结果。

    Raises:
        ConfigError: 取值不是整数。
    """
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return int(raw)
    except (TypeError, ValueError) as exc:
        raise ConfigError(f"{name} 取值非法：{raw!r}（须为整数）") from exc


@dataclass(frozen=True)
class InternalChannelConfig(BaseValueObject):
    """内部通道配置（多实例必填；单实例整体忽略）。"""

    host: str = DEFAULT_REPLICATION_INTERNAL_HOST
    """监听地址。"""

    port: int = DEFAULT_REPLICATION_INTERNAL_PORT
    """监听端口（0 = 不启用）。"""

    endpoints: ConcurrentStableFrozenDict[str, str] = field(
        default_factory=lambda: ConcurrentStableFrozenDict[str, str]()
    )
    """节点标识 → 基地址（可省略自身）。"""

    timeout_ms: int = DEFAULT_REPLICATION_INTERNAL_TIMEOUT_MS
    """读超时（毫秒）。"""

    connect_timeout_ms: int = DEFAULT_REPLICATION_INTERNAL_CONNECT_TIMEOUT_MS
    """连接超时（毫秒）。"""

    fetch_max_bytes: int = DEFAULT_REPLICATION_FETCH_MAX_BYTES
    """单次拉取响应软上限（字节）。"""

    @classmethod
    def from_env(
        cls,
        *,
        enabled: bool,
        node_id: str,
        nodes: ConcurrentStableFrozenList[str],
        business_port: int,
    ) -> InternalChannelConfig:
        """从环境变量构造并校验内部通道配置。

        多实例（`enabled` 且节点数 > 1）下：端口必须为正、端点覆盖除自身外全部节点、
        业务端口 ≠ 内部端口、三个数值为正；单实例忽略端点与端口。

        Args:
            enabled: 复制总开关。
            node_id: 本节点标识。
            nodes: 集群节点列表。
            business_port: 业务端口（用于冲突校验）。

        Returns:
            InternalChannelConfig: 已校验配置。

        Raises:
            ConfigError: 任一约束不满足（fail-closed）。
        """
        multi = bool(enabled and len(nodes) > 1)
        host = os.environ.get(REPLICATION_INTERNAL_HOST_ENV) or DEFAULT_REPLICATION_INTERNAL_HOST
        port = _env_int(REPLICATION_INTERNAL_PORT_ENV, DEFAULT_REPLICATION_INTERNAL_PORT)
        timeout_ms = _env_int(REPLICATION_INTERNAL_TIMEOUT_MS_ENV, DEFAULT_REPLICATION_INTERNAL_TIMEOUT_MS)
        connect_timeout_ms = _env_int(
            REPLICATION_INTERNAL_CONNECT_TIMEOUT_MS_ENV, DEFAULT_REPLICATION_INTERNAL_CONNECT_TIMEOUT_MS
        )
        fetch_max_bytes = _env_int(REPLICATION_FETCH_MAX_BYTES_ENV, DEFAULT_REPLICATION_FETCH_MAX_BYTES)
        endpoints = parse_node_endpoints(os.environ.get(REPLICATION_NODE_ENDPOINTS_ENV, ""))
        if not multi:
            return cls(
                host=host,
                port=0,
                endpoints=endpoints,
                timeout_ms=timeout_ms,
                connect_timeout_ms=connect_timeout_ms,
                fetch_max_bytes=fetch_max_bytes,
            )
        if port <= 0:
            raise ConfigError(f"多实例部署必须配置 {REPLICATION_INTERNAL_PORT_ENV}（当前 {port}）")
        if port == business_port:
            raise ConfigError(f"内部端口与业务端口不得相同：{port}")
        for name, value in (
            ("timeout_ms", timeout_ms),
            ("connect_timeout_ms", connect_timeout_ms),
            ("fetch_max_bytes", fetch_max_bytes),
        ):
            if value <= 0:
                raise ConfigError(f"内部通道配置非法：{name} 必须为正（当前 {value}）")
        for node in nodes:
            if node != node_id and node not in endpoints:
                raise ConfigError(f"{REPLICATION_NODE_ENDPOINTS_ENV} 未覆盖节点 {node!r}（多实例必须可寻址）")
        return cls(
            host=host,
            port=port,
            endpoints=endpoints,
            timeout_ms=timeout_ms,
            connect_timeout_ms=connect_timeout_ms,
            fetch_max_bytes=fetch_max_bytes,
        )


class ServiceTokenProvider(BaseFrameworkObject):
    """出站服务令牌（`service=msg`；按 TTL 80% 缓存，失败不缓存）。"""

    def __init__(self, issuer: BaseServiceTokenIssuer, *, service: str = SERVICE_NAME) -> None:
        """初始化。

        Args:
            issuer: 基座服务 JWT 签发器。
            service: 签发方服务标识（入站白名单校验项）。
        """
        self._issuer = issuer
        self._service = service
        self._token = ""
        self._refresh_at = 0.0
        self._lock = asyncio.Lock()

    async def authorization(self) -> str:
        """取 `Authorization` 头值（必要时签发新令牌）。

        Returns:
            str: `Bearer <jwt>`。

        Raises:
            Exception: 签发失败原样上抛（调用方记传输错误，不重试到其他节点）。
        """
        now = time.monotonic()
        if self._token and now < self._refresh_at:
            return f"Bearer {self._token}"
        async with self._lock:
            now = time.monotonic()
            if self._token and now < self._refresh_at:
                return f"Bearer {self._token}"
            token = await self._issuer.issue(ServiceTokenSpec(service=self._service))
            ttl = int(token.expires_in or _TOKEN_FALLBACK_TTL_SECONDS)
            self._token = token.access_token
            self._refresh_at = now + max(ttl, 1) * _TOKEN_REFRESH_RATIO
            return f"Bearer {self._token}"


class TransportErrorMetrics(BaseFrameworkObject):
    """传输错误计数（`operation` / `reason` 闭集标签，节点明细只走日志）。"""

    def __init__(self) -> None:
        """初始化（全零）。"""
        self._counts: ConcurrentStableDict[str, int] = ConcurrentStableDict()

    def record(self, *, operation: str, reason: str) -> None:
        """记一次传输失败。

        Args:
            operation: `fetch` / `snapshot` / `demote` / `apply`。
            reason: `timeout` / `connect` / `http` / `auth` / `protocol`。
        """
        key = f"{operation}|{reason}"
        self._counts.set(key, self._counts.get(key, 0) + 1)

    def snapshot(self) -> ConcurrentStableDict[str, int]:
        """取计数快照（键为 `operation|reason`）。

        Returns:
            ConcurrentStableDict[str, int]: 计数。
        """
        return ConcurrentStableDict(self._counts)


class _InternalHttpClient(BaseFrameworkObject):
    """内部通道 HTTP 客户端（连接池 + 令牌 + 错误映射）。"""

    def __init__(
        self,
        *,
        endpoints: ConcurrentStableFrozenDict[str, str],
        tokens: ServiceTokenProvider,
        connect_timeout_ms: int,
        timeout_ms: int,
        metrics: TransportErrorMetrics | None,
        fetch_max_bytes: int,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        """初始化。

        Args:
            endpoints: 节点 → 基地址。
            tokens: 服务令牌提供者。
            connect_timeout_ms: 连接超时（毫秒）。
            timeout_ms: 读超时（毫秒）。
            metrics: 传输错误计数（可选）。
            fetch_max_bytes: 单次响应软上限（字节）。
            transport: 传输替身（用例注入）。
        """
        self._endpoints = endpoints
        self._tokens = tokens
        self._metrics = metrics
        self._fetch_max_bytes = int(fetch_max_bytes)
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(timeout_ms / 1000, connect=connect_timeout_ms / 1000),
            limits=httpx.Limits(
                max_connections=max(len(endpoints) + 1, 2), max_keepalive_connections=max(len(endpoints) + 1, 2)
            ),
            transport=transport,
        )
        self._logged_at: ConcurrentStableDict[str, float] = ConcurrentStableDict()

    async def aclose(self) -> None:
        """关闭连接池（幂等）。"""
        await self._client.aclose()

    async def post(self, node: str, path: str, payload: ConcurrentStableDict[str, object]) -> httpx.Response:
        """发 POST（含令牌与错误计数）。

        Args:
            node: 目标节点。
            path: 内部路径（已含前缀）。
            payload: JSON 请求体。

        Returns:
            httpx.Response: 响应（2xx / 业务错误由调用方映射）。

        Raises:
            ReplicaUnavailableError: 不可达 / 鉴权失败 / 协议错误。
        """
        return await self._request("POST", node, path, json=payload)

    async def get(self, node: str, path: str) -> httpx.Response:
        """发 GET（含令牌与错误计数）。

        Args:
            node: 目标节点。
            path: 内部路径（已含前缀）。

        Returns:
            httpx.Response: 响应。

        Raises:
            ReplicaUnavailableError: 不可达 / 鉴权失败 / 协议错误。
        """
        return await self._request("GET", node, path)

    async def _request(
        self,
        method: str,
        node: str,
        path: str,
        *,
        json: ConcurrentStableDict[str, object] | None = None,
    ) -> httpx.Response:
        """发请求并做传输级错误映射。

        Args:
            method: HTTP 方法。
            node: 目标节点。
            path: 路径。
            json: 请求体。

        Returns:
            httpx.Response: 响应。

        Raises:
            ReplicaUnavailableError: 地址缺失 / 超时 / 连接失败 / 鉴权 / 协议 / 未知版本。
        """
        operation = "snapshot" if "snapshot" in path else "fetch"
        if "demote" in path:
            operation = "demote"
        elif "apply-switch" in path:
            operation = "apply"
        base = self._endpoints.get(node)
        if base is None:
            self._record(operation=operation, reason="connect")
            raise ReplicaUnavailableError(f"节点地址未配置：{node}")
        try:
            headers = {"Authorization": await self._tokens.authorization()}
        except Exception as exc:
            self._record(operation=operation, reason="auth")
            raise ReplicaUnavailableError(f"服务令牌签发失败：{type(exc).__name__}", reason="auth") from exc
        try:
            body = dict(json.items()) if json is not None else None
            response = await self._client.request(method, f"{base}{path}", json=body, headers=headers)
        except httpx.TimeoutException as exc:
            self._record(operation=operation, reason="timeout")
            self._log_failure(node=node, operation=operation, reason="timeout")
            raise ReplicaUnavailableError(f"内部通道超时：{node}", reason="timeout") from exc
        except httpx.HTTPError as exc:
            self._record(operation=operation, reason="connect")
            self._log_failure(node=node, operation=operation, reason="connect")
            raise ReplicaUnavailableError(f"内部通道不可达：{node}", reason="connect") from exc
        if response.status_code in (401, 403):
            self._record(operation=operation, reason="auth")
            self._log_failure(node=node, operation=operation, reason="auth", status=response.status_code)
            raise ReplicaUnavailableError(f"内部通道鉴权失败：{node}", reason="auth")
        return response

    def _record(self, *, operation: str, reason: str) -> None:
        """记一次传输错误（未装配指标时空转）。

        Args:
            operation: 操作名。
            reason: 原因闭集。
        """
        if self._metrics is not None:
            self._metrics.record(operation=operation, reason=reason)

    def _log_failure(self, *, node: str, operation: str, reason: str, status: int | None = None) -> None:
        """记传输失败日志（同节点同操作 60s 抑制）。

        Args:
            node: 目标节点。
            operation: 操作名。
            reason: 原因。
            status: HTTP 状态（可空）。
        """
        key = f"{node}|{operation}"
        now = time.monotonic() * 1000
        last = self._logged_at.get(key)
        if last is not None and now - last < _TRANSPORT_LOG_WINDOW_MS:
            return
        self._logged_at.set(key, now)
        _LOGGER.warning("msg_replication_transport_error", operation=operation, peer=node, reason=reason, status=status)

    @property
    def fetch_max_bytes(self) -> int:
        """单次拉取响应软上限。

        Returns:
            int: 字节数。
        """
        return self._fetch_max_bytes


def _error_detail(response: httpx.Response) -> ConcurrentStableDict[str, object]:
    """解析内部错误体（`detail` 映射；非映射按空处理）。

    Args:
        response: 响应。

    Returns:
        ConcurrentStableDict[str, object]: 错误明细。
    """
    try:
        body = cast("dict[str, object]", response.json())
    except ValueError:
        return ConcurrentStableDict()
    detail = body.get("detail")
    if isinstance(detail, dict):
        return ConcurrentStableDict(cast("dict[str, object]", detail))
    return ConcurrentStableDict()


def _raise_for_status(response: httpx.Response, *, operation: str, metrics: TransportErrorMetrics | None) -> None:
    """把非 2xx 的内部响应映射成复制领域异常。

    Args:
        response: 响应。
        operation: 操作名（计数用）。
        metrics: 传输错误计数。

    Raises:
        StaleLeaderError: 409 `stale_epoch`。
        ReplicationDivergedError: 410 `diverged`。
        ReplicaUnavailableError: 其余（含协议 / 内部错误）。
    """
    if response.is_success:
        return
    detail = _error_detail(response)
    reason = str(detail.get("reason") or "unavailable")
    if response.status_code == 409:
        raise StaleLeaderError(str(detail.get("detail") or "leader 任期过期"))
    if response.status_code == 410:
        leader_leo = detail.get("leader_leo")
        raise ReplicationDivergedError(
            str(detail.get("detail") or "副本位点发散"),
            leader_leo=int(leader_leo) if isinstance(leader_leo, int | str) and str(leader_leo).isdigit() else None,
        )
    if metrics is not None:
        metrics.record(operation=operation, reason="protocol" if response.status_code == 400 else "http")
    raise ReplicaUnavailableError(
        f"内部端点返回 {response.status_code}：{detail.get('detail') or reason}",
        reason=reason,
    )


class HttpReplicaTransport(ReplicaTransport):
    """跨实例副本拉取（`ReplicaTransport.fetch` 的 HTTP 实现）。"""

    def __init__(
        self,
        *,
        config: InternalChannelConfig,
        tokens: ServiceTokenProvider,
        metrics: TransportErrorMetrics | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        """初始化。

        Args:
            config: 内部通道配置。
            tokens: 服务令牌提供者。
            metrics: 传输错误计数（可选）。
            transport: 传输替身（用例注入）。
        """
        self._metrics = metrics
        self._client = _InternalHttpClient(
            endpoints=config.endpoints,
            tokens=tokens,
            connect_timeout_ms=config.connect_timeout_ms,
            timeout_ms=config.timeout_ms,
            metrics=metrics,
            fetch_max_bytes=config.fetch_max_bytes,
            transport=transport,
        )

    async def aclose(self) -> None:
        """关闭连接池（幂等）。"""
        await self._client.aclose()

    async def fetch(self, request: ReplicaFetchRequest) -> ReplicaFetchResult:
        """向 leader 拉取一批记录。

        Args:
            request: 拉取请求。

        Returns:
            ReplicaFetchResult: 记录与 leader 运行态。

        Raises:
            ReplicaUnavailableError: 目标不可达 / 非 leader / 切换窗口。
            StaleLeaderError: leader 任期低于本地已知任期。
            ReplicationDivergedError: follower 位点越界。
        """
        payload = ConcurrentStableDict[str, object](
            FetchRequest(
                leader=request.leader,
                follower=request.follower,
                topic_id=request.topic_id,
                partition_no=request.partition_no,
                from_offset=request.from_offset,
                max_messages=request.max_messages,
                epoch=request.epoch,
            ).model_dump()
        )
        response = await self._client.post(request.leader, f"{REPLICATION_INTERNAL_PATH_PREFIX}/fetch", payload)
        _raise_for_status(response, operation="fetch", metrics=self._metrics)
        body = FetchResponse.model_validate(response.json())
        if body.protocol_version != REPLICATION_INTERNAL_PROTOCOL_VERSION:
            if self._metrics is not None:
                self._metrics.record(operation="fetch", reason="protocol")
            raise ReplicaUnavailableError(
                f"内部协议版本未知：{body.protocol_version}（期望 {REPLICATION_INTERNAL_PROTOCOL_VERSION}）",
                reason="protocol",
            )
        return ReplicaFetchResult(
            leader_epoch=body.leader_epoch,
            from_offset=body.from_offset,
            records=ConcurrentStableList(message.to_stored() for message in body.records),
            leader_leo=body.leader_leo,
            high_watermark=body.high_watermark,
            leader_isr=ConcurrentStableFrozenList(body.leader_isr),
        )


class HttpLeaderControlTransport(LeaderControlTransport):
    """跨实例节点控制（`snapshot` / `demote` / `apply_switch` 的 HTTP 实现）。

    本节点控制走本地协调器（不绕 HTTP）：`register_local()` 登记自身后，
    针对自身的 `snapshot` / `demote` / `apply_switch` 直接调用进程内实现。
    """

    def __init__(
        self,
        *,
        config: InternalChannelConfig,
        tokens: ServiceTokenProvider,
        metrics: TransportErrorMetrics | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        """初始化。

        Args:
            config: 内部通道配置。
            tokens: 服务令牌提供者。
            metrics: 传输错误计数（可选）。
            transport: 传输替身（用例注入）。
        """
        self._metrics = metrics
        self._client = _InternalHttpClient(
            endpoints=config.endpoints,
            tokens=tokens,
            connect_timeout_ms=config.connect_timeout_ms,
            timeout_ms=config.timeout_ms,
            metrics=metrics,
            fetch_max_bytes=config.fetch_max_bytes,
            transport=transport,
        )
        self._local: ConcurrentStableDict[str, ReplicationCoordinator] = ConcurrentStableDict()

    def register_local(self, node_id: str, coordinator: ReplicationCoordinator) -> None:
        """登记本节点协调器（针对自身的控制走本地快路径）。

        Args:
            node_id: 本节点标识。
            coordinator: 本节点复制协调器。
        """
        self._local.set(node_id, coordinator)

    async def aclose(self) -> None:
        """关闭连接池（幂等）。"""
        await self._client.aclose()

    async def snapshot(self, *, node_id: str, topic_id: int, partition_no: int) -> ReplicationSnapshot | None:
        """读目标节点分区运行态（不可达返回 None）。

        Args:
            node_id: 节点标识。
            topic_id: 主题主键。
            partition_no: 分区号。

        Returns:
            ReplicationSnapshot | None: 快照；不可达 / 未挂载返回 None。
        """
        local = self._local.get(node_id)
        if local is not None:
            return await local.snapshot(topic_id=topic_id, partition_no=partition_no)
        try:
            response = await self._client.get(
                node_id, f"{REPLICATION_INTERNAL_PATH_PREFIX}/partitions/{topic_id}/{partition_no}/snapshot"
            )
        except ReplicaUnavailableError:
            return None
        if response.status_code == 404:
            return None
        try:
            _raise_for_status(response, operation="snapshot", metrics=self._metrics)
        except ReplicaUnavailableError:
            return None
        body = SnapshotResponse.model_validate(response.json())
        if body.protocol_version != REPLICATION_INTERNAL_PROTOCOL_VERSION:
            if self._metrics is not None:
                self._metrics.record(operation="snapshot", reason="protocol")
            return None
        return ReplicationSnapshot(
            leader=body.leader,
            epoch=body.epoch,
            leo=body.leo,
            hw=body.hw,
            isr=ConcurrentStableFrozenList(body.isr),
            replicas=ConcurrentStableFrozenList(body.replicas),
            switch_state=body.switch_state,
            writable=body.writable,
            last_known_hw=body.last_known_hw,
            last_known_isr=ConcurrentStableFrozenList(body.last_known_isr),
            last_known_at=body.last_known_at,
            start_offset=body.start_offset,
        )

    async def demote(
        self,
        *,
        node_id: str,
        topic_id: int,
        partition_no: int,
        to_leader: str,
        to_epoch: int,
    ) -> None:
        """通知旧 leader 立即降级（不可达原样上抛，由切换服务忽略并靠租约兜底）。

        Args:
            node_id: 旧 leader 节点标识。
            topic_id: 主题主键。
            partition_no: 分区号。
            to_leader: 新 leader。
            to_epoch: 新任期。

        Raises:
            ReplicaUnavailableError: 目标不可达 / 端点失败。
        """
        local = self._local.get(node_id)
        if local is not None:
            await local.demote(topic_id=topic_id, partition_no=partition_no, to_leader=to_leader, to_epoch=to_epoch)
            return
        payload = ConcurrentStableDict[str, object]({"to_leader": to_leader, "to_epoch": int(to_epoch)})
        response = await self._client.post(
            node_id, f"{REPLICATION_INTERNAL_PATH_PREFIX}/partitions/{topic_id}/{partition_no}/demote", payload
        )
        _raise_for_status(response, operation="demote", metrics=self._metrics)

    async def apply_switch(self, *, node_id: str, command: SwitchApplyCommand) -> SwitchApplyResult:
        """让目标节点截断 / 保留尾部并激活。

        Args:
            node_id: 新 leader 节点标识。
            command: 切换命令。

        Returns:
            SwitchApplyResult: 激活结果。

        Raises:
            ReplicaUnavailableError: 目标不可达 / 端点失败（切换停在 switching，可重试）。
        """
        local = self._local.get(node_id)
        if local is not None:
            return await local.apply_switch(command)
        payload = ConcurrentStableDict[str, object](
            {
                "topic_id": command.topic_id,
                "partition_no": command.partition_no,
                "from_leader": command.from_leader,
                "to_leader": command.to_leader,
                "to_epoch": int(command.to_epoch),
                "watermark": command.watermark,
                "truncate": bool(command.truncate),
                "grace_ms": int(command.grace_ms),
            }
        )
        response = await self._client.post(
            node_id,
            f"{REPLICATION_INTERNAL_PATH_PREFIX}/partitions/{command.topic_id}/{command.partition_no}/apply-switch",
            payload,
        )
        _raise_for_status(response, operation="apply", metrics=self._metrics)
        body = SwitchApplyResponse.model_validate(response.json())
        if body.protocol_version != REPLICATION_INTERNAL_PROTOCOL_VERSION:
            if self._metrics is not None:
                self._metrics.record(operation="apply", reason="protocol")
            raise ReplicaUnavailableError(
                f"内部协议版本未知：{body.protocol_version}（期望 {REPLICATION_INTERNAL_PROTOCOL_VERSION}）",
                reason="protocol",
            )
        return SwitchApplyResult(
            to_leo=body.to_leo,
            truncated_to=body.truncated_to,
            retained_tail=body.retained_tail,
            already_applied=body.already_applied,
        )
