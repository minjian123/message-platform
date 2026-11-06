"""最小 RPC 客户端（任务 04-1 / 04-2 / 04-3-2 / 04-7-1）：一元调用、流式入口、帧编解码与错误映射骨架。

**为什么单独一个 `client/` 包**：`protocol/` 是 broker 侧编解码（明确「不碰连接、不碰业务语义」），
客户端要发网络请求；`rpc/` 是服务端域（注册表 / 分发）。三者分开后，04-5 的 bms 适配只 import
`bms_msg.client`，不会把 broker 侧模块拖进 bms 进程。

**发帧形态而不是 JSON**：帧是 SDK 的常态路径（省一次 base64 与 JSON 解析），且与 `/msg/*` 的
「JSON 为默认识别、帧为可选」口径一致——JSON 路径留给 curl / 网关 / 浏览器。

**传输层自持（04-7-1）**：本模块只认 `RpcTransport` 协议，不认识 `h2` / socket / `httpx`。原因见
`client/transport/base.py` 模块说明：精确取消要求拿到单条 stream 并发 `RST_STREAM`，通用 HTTP 客户端
给不了。生产实现 `H2Transport` 由构造参数缺省注入，用例注入 `AsgiRpcTransport` / `ScriptedRpcTransport`。

**做的**：一元调用（04-4 起带重试中间层：五类分类 + full jitter 退避 + 调用链预算 + 幂等键 +
熔断 + `Retry-After`）、流式入口（`stream()` 返回句柄，实现见 `client/stream.py`，**不进重试层**）、
`request_id` 关联（单 stream 一请求；重试每次尝试重新生成，幂等键全程稳定）、预算传播
（`deadline_ms` 写入帧头）、一元本地超时 = 预算 + 余量、本地超时与服务端 `350004` 统一异常、
错误帧 → `RpcCallError`、地址轮询。
**不做的**（后续任务）：实例选择策略与失败摘除（04-6）、流式错误重试（8B_12 / 后续）。

**请求帧组装只写一份**：`call()` 与 `stream()` 共用 `_request_frame()` / `_http_request()`。两份编码
会在「加一个帧头字段」时悄悄漂移，而漂移只会在跨版本联调时才暴露——那时已经很难说清是哪边旧。
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from types import TracebackType
from typing import TYPE_CHECKING, Any, Protocol, Self, cast
from uuid import uuid4

from bms_core.circuit.base import BaseCircuitBreaker
from bms_core.circuit.null import NullCircuitBreaker
from bms_core.core.concurrent import ConcurrentStableDict
from bms_core.core.logging import get_logger

from bms_msg.client.circuit import rpc_dependency
from bms_msg.client.retry import (
    Clock,
    JitterSource,
    RetryPolicy,
    SystemClock,
    SystemJitter,
    acquire_budget,
    decide_retry,
    parse_retry_after,
    release_budget,
)
from bms_msg.client.transport import (
    H2Transport,
    RpcHttpRequest,
    RpcHttpResponse,
    RpcRequestTrace,
    RpcTransport,
    RpcTransportError,
    RpcTransportProtocolError,
    RpcTransportTimeoutError,
)
from bms_msg.core.config import (
    RPC_CLIENT_TIMEOUT_SLACK_MS,
    RPC_DEADLINE_HEADER,
    RPC_DEFAULT_TIMEOUT_MS,
    RPC_TENANT_HEADER,
)
from bms_msg.errors import ErrorCategory, MsgError, MsgErrorCode
from bms_msg.protocol.frame import (
    FRAME_CONTENT_TYPE,
    PROTOCOL_VERSION,
    FrameFlags,
    FrameType,
    decode_frame,
    encode_frame,
)
from bms_msg.rpc.deadline import OUTCOME_UNKNOWN, validate_budget_ms
from bms_msg.rpc.naming import ensure_rpc_name
from bms_msg.rpc.registry import RpcReply

if TYPE_CHECKING:  # pragma: no cover - 仅类型检查期
    from bms_msg.client.stream import RpcStream

__all__ = [
    "InstanceProvider",
    "RetryPolicy",
    "RpcCallError",
    "RpcCallTimeoutError",
    "RpcClient",
    "StaticInstanceProvider",
    "error_of_frame",
    "frame_headers",
]

_LOGGER = get_logger("bms_msg.rpc.client")


class RpcCallError(Exception):
    """调用失败（承载服务端给的服务端判定，客户端不自行分类）。

    **`retryable` 与 `layer` 照搬服务端**：服务端 `classify()` 是唯一出处。客户端再维护一张码表，
    两端就会逐渐漂移——那时「为什么这个错客户端说可重试、服务端说不可重试」会变成常驻 bug。
    """

    def __init__(
        self,
        code: int,
        message: str,
        *,
        retryable: bool = False,
        layer: str = "system",
        category: ErrorCategory = ErrorCategory.SERVER_BUG,
        retry_after_ms: int | None = None,
        attempts: int = 1,
        backoff_ms: tuple[int, ...] = (),
    ) -> None:
        """初始化。

        Args:
            code: 产品码 / 平台码；无法映射时为 0。
            message: 可读提示。
            retryable: 服务端判定的可重试性。
            layer: 服务端判定的错误层（`transport` / `protocol` / `business` / `system`）。
            category: 服务端判定的错误类别（04-4；老服务端缺该字段时保守回落 `server_bug`）。
            retry_after_ms: 建议重试间隔（毫秒；限流 / 连接类错误才给）。
            attempts: 实际尝试次数（重试层回填；任务 §2.7）。
            backoff_ms: 已发生的退避序列（重试层回填）。
        """
        super().__init__(message)
        self.code = code
        self.message = message
        self.retryable = retryable
        self.layer = layer
        self.category = category
        self.retry_after_ms = retry_after_ms
        self.attempts = attempts
        self.backoff_ms = backoff_ms


class RpcCallTimeoutError(RpcCallError):
    """调用没在预算内拿到结果（客户端本地兜底超时，或服务端 `350004`）。

    **统一 `350004`**：不论谁先到点，对调用方都是「结果未知」——既不代表没生效、也不代表失败，
    所以 `retryable=False`；层判 `business` 而不是 `transport`：传输本身没坏，凭一次超时断连接
    （上面还跑着别的 stream）是错的。
    """

    def __init__(self, message: str = "RPC 调用超时", *, budget_ms: int | None = None) -> None:
        """初始化。

        Args:
            message: 可读提示。
            budget_ms: 本次预算（毫秒；用于日志与排障，可为 None）。
        """
        super().__init__(
            int(MsgErrorCode.RPC_DEADLINE_EXCEEDED),
            message,
            retryable=False,
            layer="business",
            category=ErrorCategory.UNKNOWN,
        )
        self.outcome = OUTCOME_UNKNOWN
        self.budget_ms = budget_ms


class InstanceProvider(Protocol):
    """实例地址来源（首期单地址实现，04-6 换 DNS / 服务目录实现，签名不变）。"""

    def next(self) -> str:
        """取下一个实例地址（含 scheme 与端口，不带尾斜线）。

        Returns:
            str: 实例基址，如 `http://msg:8000`。
        """
        ...


class StaticInstanceProvider:
    """静态地址列表 + 轮询（单地址即单元素列表）。

    轮询而不是随机：确定性、易断言——04-6 加「最少在途」时才需要随机化或计数。
    """

    def __init__(self, endpoints: Sequence[str]) -> None:
        """初始化。

        Args:
            endpoints: 实例基址列表（至少一个）。

        Raises:
            ValueError: 列表为空。
        """
        if not endpoints:
            raise ValueError("InstanceProvider 至少需要一个实例地址")
        self._endpoints: tuple[str, ...] = tuple(endpoint.rstrip("/") for endpoint in endpoints)
        self._index = 0

    def next(self) -> str:
        """取下一个实例地址（插入序轮询）。

        Returns:
            str: 实例基址。
        """
        endpoint = self._endpoints[self._index]
        self._index = (self._index + 1) % len(self._endpoints)
        return endpoint


def _error_of(response: RpcHttpResponse) -> RpcCallError:
    """把非 200 响应映射为调用错误（只映射有明确含义的两个码）。

    **04-4**：429 补解析 HTTP `Retry-After`（delta-seconds → 毫秒；非法忽略），与帧路径的
    `retry_after_ms` 汇成同一个字段；`category` 由本层显式给出（HTTP 响应没有帧头可照搬）。

    Args:
        response: 传输层响应。

    Returns:
        RpcCallError: 待抛出的错误。
    """
    retry_after_ms = parse_retry_after(response.headers)
    if response.status == 429:
        return RpcCallError(
            350005,
            "RPC 调用被限流（429）",
            retryable=True,
            layer="business",
            category=ErrorCategory.TRANSIENT,
            retry_after_ms=retry_after_ms,
        )
    if response.status == 413:
        return RpcCallError(
            350006,
            "RPC 参数或结果超过上限（413）",
            retryable=False,
            layer="transport",
            category=ErrorCategory.PARAM_ERROR,
        )
    return RpcCallError(0, f"RPC 端点返回 HTTP {response.status}", retryable=False, layer="transport")


def error_of_frame(header: Mapping[str, Any], *, budget_ms: int | None = None) -> RpcCallError:
    """把 `ERROR` 帧头映射为调用错误（一元与流式共用一份判定）。

    **`layer` / `retryable` / `retry_after_ms` 全部照搬帧头**：服务端 `classify()` 是唯一出处，客户端
    再维护一张码表，两端就会逐渐漂移。流式与一元的区别只在「从哪里拿到这帧」，不在「怎么解释它」。

    Args:
        header: 帧头（`code` / `message` / `retryable` / `layer` / `retry_after_ms`）。
        budget_ms: 本次预算（毫秒；`350004` 映射成 `RpcCallTimeoutError` 时带上）。

    Returns:
        RpcCallError: 待抛出的错误（`350004` 时是 `RpcCallTimeoutError` 子类实例）。

    Raises:
        RpcCallTimeoutError: 帧头 `code` 为 `350004`（预算耗尽）。
    """
    code = _int_of(header.get("code"))
    message = str(header.get("message") or "")
    if code == int(MsgErrorCode.RPC_DEADLINE_EXCEEDED):
        # 服务端预算耗尽与本地兜底超时对调用方是同一件事：没在预算内拿到结果。
        raise RpcCallTimeoutError(message or "RPC 预算耗尽", budget_ms=budget_ms)
    retry_after = header.get("retry_after_ms")
    # 老服务端没有 category / 取值不认识：保守回落 server_bug（不重试），不猜测码值。
    try:
        category = ErrorCategory(str(header.get("category")))
    except ValueError:
        category = ErrorCategory.SERVER_BUG
    return RpcCallError(
        code,
        message,
        retryable=bool(header.get("retryable")),
        layer=str(header.get("layer") or "system"),
        category=category,
        retry_after_ms=retry_after if isinstance(retry_after, int) else None,
    )


def _reply_of(content: bytes, *, budget_ms: int | None = None) -> RpcReply:
    """解析响应帧（结果帧 / 错误帧）。

    Args:
        content: 响应体字节。
        budget_ms: 本次预算（毫秒；`350004` 映射成 `RpcCallTimeoutError` 时带上）。

    Returns:
        RpcReply: 结果。

    Raises:
        RpcCallError: 响应不是合法帧 / 是错误帧 / 类型不符。
        RpcCallTimeoutError: 服务端返回 `350004`（预算耗尽）。
    """
    try:
        frame = decode_frame(content)
    except MsgError as exc:
        raise RpcCallError(exc.code, f"响应不是合法帧：{exc}", layer="protocol") from exc
    if frame.frame_type == int(FrameType.ERROR):
        raise error_of_frame(frame.header, budget_ms=budget_ms)
    if frame.frame_type != int(FrameType.RPC) or not frame.flags & FrameFlags.RESPONSE:
        raise RpcCallError(0, f"响应帧类型不符：{frame.frame_type}", layer="protocol")
    return RpcReply(payload=bytes(frame.payload), headers=frame_headers(frame.header.get("headers")))


def _int_of(value: Any) -> int:
    """把帧头里的码转整数（非法给 0，不抛——错误路径不该再抛错）。

    Args:
        value: 帧头值。

    Returns:
        int: 码值；非法为 0。
    """
    return value if isinstance(value, int) else 0


def frame_headers(raw: Any) -> ConcurrentStableDict[str, str]:
    """把帧头里的 `headers` 转结构化头（非法给空，不抛）。

    一元与流式共用：「一帧的结果头」与「整次调用的结果头」是同一种东西，转换规则不该有两份。

    Args:
        raw: 帧头值。

    Returns:
        ConcurrentStableDict[str, str]: 结果头。
    """
    headers: ConcurrentStableDict[str, str] = ConcurrentStableDict()
    if not isinstance(raw, Mapping):
        return headers
    for key, value in cast("Mapping[object, object]", raw).items():
        if isinstance(key, str) and isinstance(value, str):
            headers.set(key, value)
    return headers


def _request_header(
    *,
    request_id: str,
    service: str,
    method: str,
    headers: Mapping[str, str] | None,
    trace_id: str | None,
    caller: str | None,
    deadline_ms: int,
    idempotency_key: str | None = None,
    retry_budget: int | None = None,
) -> ConcurrentStableDict[str, Any]:
    """构造请求帧头（只放 `rpc/contract.py` 声明过的可选字段）。

    Args:
        request_id: 请求标识。
        service: 服务标识。
        method: 方法名。
        headers: 结构化头。
        trace_id: 链路标识。
        caller: 调用方服务标识。
        deadline_ms: 本次预算（毫秒；进帧头供逐跳扣减）。
        idempotency_key: 幂等键（04-4；重试全程稳定）。
        retry_budget: 调用链剩余重试次数（04-4）。

    Returns:
        ConcurrentStableDict[str, Any]: 帧头。
    """
    header: ConcurrentStableDict[str, Any] = ConcurrentStableDict()
    header.set("request_id", request_id)
    header.set("protocol_version", PROTOCOL_VERSION)
    header.set("service", service)
    header.set("method", method)
    header.set(RPC_DEADLINE_HEADER, deadline_ms)
    if headers:
        header.set("headers", dict(headers.items()))
    if trace_id is not None:
        header.set("trace_id", trace_id)
    if caller is not None:
        header.set("caller", caller)
    if idempotency_key is not None:
        header.set("idempotency_key", idempotency_key)
    if retry_budget is not None:
        header.set("retry_budget", retry_budget)
    return header


def _request_frame(
    *,
    request_id: str,
    service: str,
    method: str,
    payload: bytes,
    headers: Mapping[str, str] | None,
    trace_id: str | None,
    caller: str | None,
    deadline_ms: int,
    idempotency_key: str | None = None,
    retry_budget: int | None = None,
) -> bytes:
    """编码请求帧（`call()` 与 `stream()` 共用，避免两份编码漂移）。

    Args:
        request_id: 请求标识。
        service: 服务标识。
        method: 方法名。
        payload: 调用参数原始字节。
        headers: 结构化头。
        trace_id: 链路标识。
        caller: 调用方服务标识。
        deadline_ms: 本次预算（毫秒）。
        idempotency_key: 幂等键（04-4）。
        retry_budget: 调用链剩余重试次数（04-4）。

    Returns:
        bytes: 完整帧字节。
    """
    return encode_frame(
        frame_type=int(FrameType.RPC),
        flags=FrameFlags.NEED_RESPONSE,
        header=_request_header(
            request_id=request_id,
            service=service,
            method=method,
            headers=headers,
            trace_id=trace_id,
            caller=caller,
            deadline_ms=deadline_ms,
            idempotency_key=idempotency_key,
            retry_budget=retry_budget,
        ),
        payload=payload,
    )


class RpcClient:
    """最小 RPC 客户端（一元调用；连接复用由传输层承担）。"""

    def __init__(
        self,
        instances: InstanceProvider,
        *,
        tenant: str | None = None,
        deadline_ms: int | None = None,
        transport: RpcTransport | None = None,
        retry_policy: RetryPolicy | None = None,
        circuit: BaseCircuitBreaker | None = None,
        clock: Clock | None = None,
        jitter: JitterSource | None = None,
    ) -> None:
        """初始化。

        Args:
            instances: 实例地址来源（`StaticInstanceProvider` 或 04-6 的实现）。
            tenant: 缺省租户编码（写进 `X-Tenant-ID`；服务端仍以解析链结果为准）。跨租户调用时
                用 `call(tenant=...)` 覆盖。
            deadline_ms: 单次调用缺省预算（毫秒；None 取 `RPC_DEFAULT_TIMEOUT_MS`）。
            transport: 传输层（用例注入 `AsgiRpcTransport` / `ScriptedRpcTransport`；生产留 None 走 h2）。
            retry_policy: 重试策略（缺省 `RetryPolicy()`）。
            circuit: 熔断器（缺省 `NullCircuitBreaker()` 恒闭合，首期行为等价现状、零回归）。
            clock: 时钟（缺省 `SystemClock()`；用例注入 fake clock）。
            jitter: 抖动随机源（缺省 `SystemJitter()`；用例注入固定序列）。

        Raises:
            ParamError: 预算取值非法（`10001`）。
        """
        self._instances = instances
        self._tenant = tenant
        self._deadline_ms = validate_budget_ms(deadline_ms, default=RPC_DEFAULT_TIMEOUT_MS)
        self._transport: RpcTransport = transport if transport is not None else H2Transport()
        self._retry_policy = retry_policy if retry_policy is not None else RetryPolicy()
        self._circuit = circuit if circuit is not None else NullCircuitBreaker()
        self._clock = clock if clock is not None else SystemClock()
        self._jitter = jitter if jitter is not None else SystemJitter()

    async def _attempt(
        self,
        *,
        service: str,
        method: str,
        payload: bytes,
        headers: Mapping[str, str] | None,
        trace_id: str | None,
        caller: str | None,
        budget: int,
        tenant: str | None,
        idempotency_key: str | None,
        retry_budget: int | None,
    ) -> RpcReply:
        """发一次请求并解释响应（不含重试；`call()` 的重试循环唯一调用点）。

        Args:
            service: 服务标识。
            method: 方法名。
            payload: 调用参数原始字节。
            headers: 结构化头。
            trace_id: 链路标识。
            caller: 调用方服务标识。
            budget: 本次预算（毫秒）。
            tenant: 本次调用的租户编码。
            idempotency_key: 幂等键（重试全程稳定）。
            retry_budget: 调用链剩余重试次数。

        Returns:
            RpcReply: 结果。

        Raises:
            RpcCallTimeoutError: 本地兜底超时 / 服务端 `350004`。
            RpcCallError: 错误帧 / 非 200 / 响应不可解析。
        """
        request = self._http_request(
            request_id=uuid4().hex,
            service=service,
            method=method,
            payload=payload,
            headers=headers,
            trace_id=trace_id,
            caller=caller,
            budget=budget,
            tenant=tenant,
            suffix="",
            idempotency_key=idempotency_key,
            retry_budget=retry_budget,
        )
        try:
            response = await self._transport.send(request)
        except RpcTransportTimeoutError as exc:
            raise RpcCallTimeoutError(f"RPC 调用超时（{budget} ms）：{service}.{method}", budget_ms=budget) from exc
        except RpcTransportProtocolError as exc:
            raise RpcCallError(0, f"RPC 响应不可解析：{exc}", layer="protocol") from exc
        except RpcTransportError as exc:
            raise RpcCallError(
                0, f"RPC 连接失败：{exc}", layer="transport", retryable=True, category=ErrorCategory.TRANSIENT
            ) from exc
        if response.status != 200:
            raise _error_of(response)
        return _reply_of(response.body, budget_ms=budget)

    async def call(
        self,
        service: str,
        method: str,
        payload: bytes = b"",
        headers: Mapping[str, str] | None = None,
        *,
        trace_id: str | None = None,
        caller: str | None = None,
        deadline_ms: int | None = None,
        tenant: str | None = None,
        idempotency_key: str | None = None,
        idempotent: bool = False,
        retry_budget: int | None = None,
        policy: RetryPolicy | None = None,
    ) -> RpcReply:
        """一元调用（发帧形态，收结果帧；04-4 起带重试中间层）。

        **重试只包 `call()`**：`stream()` / `RpcStream` 完全不进这一层（决策 E1）。

        Args:
            service: 服务标识。
            method: 方法名。
            payload: 调用参数原始字节。
            headers: 结构化头（随请求帧发，服务端透传给处理器）。
            trace_id: 链路标识（进帧头）。
            caller: 调用方服务标识（进帧头；服务端仍以 HTTP 头优先）。
            deadline_ms: 本次预算（毫秒；缺省取构造时的值）。写进帧头 `deadline_ms` 供逐跳传播。
            tenant: 本次调用的租户编码（缺省取构造时的值；服务端以解析链结果为准）。
            idempotency_key: 幂等键（显式给则用之；未给且策略允许则生成 `uuid4().hex`，
                重试全程复用同一键）。
            idempotent: 显式声明方法幂等（无需键即可重试）。
            retry_budget: 上游给的剩余重试次数（缺省取调用链共享预算）。
            policy: 本次调用覆盖的重试策略（缺省取构造注入的策略）。

        Returns:
            RpcReply: 结果。

        Raises:
            ParamError: `service` / `method` 形态非法（本地先拦）或预算参数非法。
            RpcRetryBlockedError: 非幂等且无幂等键，重试被阻止。
            RpcCallTimeoutError: 本地兜底超时 / 服务端 `350004`（都带 `code=350004`）。
            RpcCallError: 错误帧 / 非 200 / 响应不可解析 / 熔断打开快速失败 / 重试耗尽。
            asyncio.CancelledError: 调用方取消（原样穿透，不重试）。
        """
        ensure_rpc_name(service, method)
        budget = validate_budget_ms(deadline_ms, default=self._deadline_ms)
        active = policy if policy is not None else self._retry_policy
        key = idempotency_key
        if key is None and active.auto_idempotency_key and not idempotent:
            key = uuid4().hex
        chain_budget, token = acquire_budget(active)
        # 上游显式给了更小的预算就收紧本跳：每跳都受同一上限约束，A→B→C 不会各重试 2 次。
        if retry_budget is not None:
            chain_budget.remaining = min(chain_budget.remaining, retry_budget)
        attempts = 0
        backoff_ms: list[int] = []
        dependency = rpc_dependency(service)
        try:
            if not await self._circuit.allow(dependency):
                raise RpcCallError(
                    0,
                    f"RPC 熔断打开，快速失败：{service}.{method}",
                    layer="system",
                    category=ErrorCategory.TRANSIENT,
                )
            while True:
                try:
                    reply = await self._attempt(
                        service=service,
                        method=method,
                        payload=payload,
                        headers=headers,
                        trace_id=trace_id,
                        caller=caller,
                        budget=budget,
                        tenant=tenant,
                        idempotency_key=key,
                        retry_budget=chain_budget.remaining,
                    )
                except RpcCallError as exc:
                    await self._circuit.record_failure(dependency)
                    exc.attempts = attempts + 1
                    exc.backoff_ms = tuple(backoff_ms)
                    decision = decide_retry(
                        exc,
                        attempts=attempts,
                        policy=active,
                        budget=chain_budget,
                        idempotency_key=key,
                        idempotent=idempotent,
                        retry_after_ms=exc.retry_after_ms,
                        jitter=self._jitter,
                    )
                    if decision is None:
                        _LOGGER.info(
                            "msg_rpc_retry_stopped",
                            service=service,
                            method=method,
                            attempts=exc.attempts,
                            category=str(exc.category),
                            code=exc.code,
                        )
                        raise
                    _LOGGER.info(
                        "msg_rpc_retry",
                        service=service,
                        method=method,
                        attempt=attempts + 1,
                        delay_ms=decision.delay_ms,
                        category=str(exc.category),
                        budget_remaining=chain_budget.remaining,
                    )
                    backoff_ms.append(decision.delay_ms)
                    chain_budget.consume()
                    await self._clock.sleep_ms(decision.delay_ms)
                    attempts += 1
                    continue
                await self._circuit.record_success(dependency)
                return reply
        finally:
            release_budget(token)

    def stream(
        self,
        service: str,
        method: str,
        payload: bytes = b"",
        headers: Mapping[str, str] | None = None,
        *,
        trace_id: str | None = None,
        caller: str | None = None,
        deadline_ms: int | None = None,
        tenant: str | None = None,
    ) -> RpcStream:
        """流式调用：返回**惰性打开**的 `RpcStream` 句柄（构造时一个字节都不发）。

        **唯一推荐写法是 `async with`**：

        ```python
        async with client.stream("demo", "ticker", b"5") as replies:
            async for reply in replies:
                handle(reply.payload)
                if enough:
                    break            # 退出 with 时发 RST_STREAM(CANCEL)
        ```

        `async for` 的 `break` 在语言层面**不通知迭代器**，所以「提前不看了」只有两条路：`async with`
        退出，或显式 `await replies.aclose()`。只写 `break` 而不退出上下文，服务端会一直把这条流推完
        （用例把这条已知行为固定下来，见 04-3-2 详细设计 §4.2）。

        Args:
            service: 服务标识。
            method: 流式方法名（须在注册表里以 `register_stream()` 登记）。
            payload: 调用参数原始字节。
            headers: 结构化头（随请求帧发，服务端透传给处理器）。
            trace_id: 链路标识（进帧头）。
            caller: 调用方服务标识（进帧头；服务端仍以 HTTP 头优先）。
            deadline_ms: 整条流的预算（毫秒；缺省取构造时的值）。**只作为请求字段发给服务端**，
                客户端不据此设本地整段超时——长流靠服务端按预算收尾，客户端只兜「90s 没数据」。
            tenant: 本次调用的租户编码（缺省取构造时的值）。

        Returns:
            RpcStream: 流句柄（`__aenter__` / 首次 `__anext__` 才发请求）。

        Raises:
            ParamError: `service` / `method` 形态非法（本地先拦，省一次往返）或预算参数非法。
        """
        from bms_msg.client.stream import RpcStream

        ensure_rpc_name(service, method)
        budget = validate_budget_ms(deadline_ms, default=self._deadline_ms)
        request_id = uuid4().hex
        request = self._http_request(
            request_id=request_id,
            service=service,
            method=method,
            payload=payload,
            headers=headers,
            trace_id=trace_id,
            caller=caller,
            budget=budget,
            tenant=tenant,
            suffix="/stream",
        )
        return RpcStream(
            transport=self._transport,
            request=request,
            request_id=request_id,
            service=service,
            method=method,
            budget_ms=budget,
        )

    def _http_request(
        self,
        *,
        request_id: str,
        service: str,
        method: str,
        payload: bytes,
        headers: Mapping[str, str] | None,
        trace_id: str | None,
        caller: str | None,
        budget: int,
        tenant: str | None,
        suffix: str,
        idempotency_key: str | None = None,
        retry_budget: int | None = None,
    ) -> RpcHttpRequest:
        """构造传输层请求（一元与流式共用；`suffix` 决定走哪个端点）。

        Args:
            request_id: 请求标识。
            service: 服务标识。
            method: 方法名。
            payload: 调用参数原始字节。
            headers: 结构化头。
            trace_id: 链路标识。
            caller: 调用方服务标识。
            budget: 本次预算（毫秒）。
            tenant: 本次调用的租户编码。
            suffix: 端点后缀（一元为空串、流式为 `/stream`）。
            idempotency_key: 幂等键（04-4；流式不传）。
            retry_budget: 调用链剩余重试次数（04-4；流式不传）。

        Returns:
            RpcHttpRequest: 传输层入参。
        """
        effective_tenant = self._tenant if tenant is None else tenant
        # `content-type` 与租户头一起给：只带帧媒体类型时服务端解析不到租户（`350012`），
        # 这条链路正是「RPC 与 `/msg/*` 共用同一中间件」的落点。
        request_headers: list[tuple[str, str]] = [("content-type", FRAME_CONTENT_TYPE)]
        if effective_tenant:
            request_headers.append((RPC_TENANT_HEADER, effective_tenant))
        return RpcHttpRequest(
            origin=self._instances.next(),
            target=f"/rpc/{service}/{method}{suffix}",
            headers=tuple(request_headers),
            body=_request_frame(
                request_id=request_id,
                service=service,
                method=method,
                payload=payload,
                headers=headers,
                trace_id=trace_id,
                caller=caller,
                deadline_ms=budget,
                idempotency_key=idempotency_key,
                retry_budget=retry_budget,
            ),
            # 一元：超时包住整次调用；流式：只约束「打开阶段」（见 `open_stream` 的说明）。
            timeout_ms=budget + RPC_CLIENT_TIMEOUT_SLACK_MS,
            trace=RpcRequestTrace(request_id=request_id, service=service, method=method),
        )

    async def aclose(self) -> None:
        """释放底层传输（连接池 / 后台任务）。"""
        await self._transport.aclose()

    async def __aenter__(self) -> Self:
        """进入上下文（便于 `async with RpcClient(...) as client`）。

        Returns:
            Self: 客户端自身。
        """
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """退出上下文（关闭传输）。

        Args:
            exc_type: 异常类型（未使用）。
            exc: 异常实例（未使用）。
            traceback: 栈（未使用）。
        """
        await self.aclose()
