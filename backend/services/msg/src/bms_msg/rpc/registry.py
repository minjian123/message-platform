"""RPC 方法注册表（任务 04-1 / 04-3-1）：装配期静态登记、按 `service + method` 分发。

**为什么是实例而不是模块级全局**：仓内进程级状态一律经 `app.state` + 依赖注入（位点 / 组 / 限流器
全部如此）。模块级注册表会在多 app 实例（monorepo 测试、同进程双服务）之间串扰，用例不得不显式
`reset()`；实例化后每个应用各持一份，用例各造一份，互不影响。

**重复即拒启**：同一 `(service, method)` 二次登记抛 `RpcRegistrationError`（`ValueError` 子类）。
装配期调用，异常直接让服务拒启——这正是「启动期静态注册 + 重复即拒启」要的效果；换成运行期
后写覆盖先写，就变成「谁最后加载谁生效」的隐式行为，最难排障。

**两种形态各一个入口**（04-3-1）：一元是 `Awaitable[RpcReply]`、流式是 `AsyncIterator[RpcReply]`，
合并成一个 `register()` 只能靠运行期判别（`isasyncgen`），登记错形态要到第一次调用才炸；两个入口在
装配期就把形态固定下来。同一 `(service, method)` **只能是一种形态**：两条路径各挂一个同名方法时，
「调哪个」取决于调用方选了哪个端点，属隐式行为，正是「冲突即拒启」要消灭的。

**边界**：本模块只管「谁处理」。限流 / 参数上限 / 结果上限在 `rpc/service.py` / `rpc/stream.py`，
编解码在 API 层。
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable
from dataclasses import dataclass
from typing import Protocol

from bms_core.core.concurrent import ConcurrentStableDict, ConcurrentStableList
from bms_core.core.roots import BaseFrameworkObject, BaseValueObject
from bms_core.msgclient.values import RpcCall, RpcReply

__all__ = [
    "RpcCall",
    "RpcDeclaration",
    "RpcHandler",
    "RpcRegistrationError",
    "RpcRegistry",
    "RpcReply",
    "RpcStreamHandler",
]

UNARY_FORM = "一元方法"
"""形态名（一元）：只用于拒启文案，让「哪一侧已占用」在异常里直接可读。"""

STREAM_FORM = "流式方法"
"""形态名（流式）：同上。"""


@dataclass(frozen=True)
class RpcDeclaration(BaseValueObject):
    """一个方法的契约声明（04-4）：幂等性 + 形态，供 `rpc_contract()` 发布。

    **单独一个类型而不是改 `methods()` 的返回**：既有用例与调用点都按 `(service, method)`
    二元组消费 `methods()`，改形状会让它们全体跟着改；声明是新增的查询面，不该动旧面。
    """

    service: str
    """服务标识。"""

    method: str
    """方法名。"""

    idempotent: bool
    """是否幂等（默认 `False` = 保守；带幂等键的非幂等调用才可安全重试）。"""

    form: str
    """形态名（一元 / 流式）。"""


class RpcHandler(Protocol):
    """一元处理器契约：一个异步可调用对象，入参 `RpcCall`、出参 `RpcReply`。"""

    def __call__(self, call: RpcCall) -> Awaitable[RpcReply]:
        """处理一次调用。

        Args:
            call: 调用入参。

        Returns:
            Awaitable[RpcReply]: 调用结果。

        Raises:
            MsgError: 业务错误（由调用方转错误响应 / ERROR 帧）。
        """
        ...


class RpcStreamHandler(Protocol):
    """流式处理器契约：入参 `RpcCall`、出参异步迭代器（逐项一个 `RpcReply`）。

    **为什么是 `AsyncIterator` 而不是「返回列表的协程」**：流式的价值在于「边算边发」——返回列表
    等于要求处理器先把整条流算完，长流会把内存与首帧时延都压到不可接受；迭代器还能让服务层在每次
    索取之间插入预算 / 空闲 / 断开三条判据，并在取消时只取消「下一项」。
    """

    def __call__(self, call: RpcCall) -> AsyncIterator[RpcReply]:
        """产出一条流的结果序列（迭代结束 = 正常结束）。

        Args:
            call: 调用入参（`deadline_ms` 为当下剩余预算）。

        Returns:
            AsyncIterator[RpcReply]: 结果序列。

        Raises:
            MsgError: 迭代中抛出的业务错误（服务层转流内 ERROR 帧收尾）。
        """
        ...


class RpcRegistrationError(ValueError):
    """装配期登记冲突（重复登记 / 同一方法两种形态）：拒启，不是业务错误。

    用 `ValueError` 而不是 `BizError`：这个异常永远不该到达调用方——它在 `configure_service()`
    里抛出，此时还没有任何请求。给它一个产品码反而会让人以为「运行期可以捕获后继续」。
    """


class RpcRegistry(BaseFrameworkObject):
    """进程内方法注册表（插入序；装配期填，之后只读；两形态各一张表）。"""

    def __init__(self) -> None:
        """初始化空注册表。"""
        self._handlers: ConcurrentStableDict[tuple[str, str], RpcHandler] = ConcurrentStableDict()
        self._stream_handlers: ConcurrentStableDict[tuple[str, str], RpcStreamHandler] = ConcurrentStableDict()
        self._idempotent: ConcurrentStableDict[tuple[str, str], bool] = ConcurrentStableDict()

    def register(self, service: str, method: str, handler: RpcHandler, *, idempotent: bool = False) -> None:
        """登记一个一元处理器（重复 / 形态冲突即拒）。

        Args:
            service: 服务标识。
            method: 方法名。
            handler: 处理器。
            idempotent: 方法是否幂等（04-4；缺省 `False` 是保守值：写操作漏声明不会被自动重试）。

        Raises:
            RpcRegistrationError: 同一 `(service, method)` 已登记（同形态重复或另一形态占用）。
        """
        key = (service, method)
        self._reject_conflict(key, STREAM_FORM if self._stream_handlers.get(key) is not None else None)
        if self._handlers.get(key) is not None:
            raise RpcRegistrationError(f"RPC 方法重复登记：{service}.{method}")
        self._handlers.set(key, handler)
        self._idempotent.set(key, idempotent)

    def register_stream(
        self, service: str, method: str, handler: RpcStreamHandler, *, idempotent: bool = False
    ) -> None:
        """登记一个流式处理器（重复 / 形态冲突即拒）。

        Args:
            service: 服务标识。
            method: 方法名。
            handler: 流式处理器（返回异步迭代器）。
            idempotent: 方法是否幂等（流式不进重试层，仅进契约供 SDK 查询）。

        Raises:
            RpcRegistrationError: 同一 `(service, method)` 已登记（同形态重复或另一形态占用）。
        """
        key = (service, method)
        self._reject_conflict(key, UNARY_FORM if self._handlers.get(key) is not None else None)
        if self._stream_handlers.get(key) is not None:
            raise RpcRegistrationError(f"RPC 流式方法重复登记：{service}.{method}")
        self._stream_handlers.set(key, handler)
        self._idempotent.set(key, idempotent)

    def _reject_conflict(self, key: tuple[str, str], occupied: str | None) -> None:
        """形态冲突即拒（同一 `(service, method)` 只能一种形态）。

        Args:
            key: `(service, method)`。
            occupied: 若已被另一形态占用，给出该形态名；未被占用为 None。

        Raises:
            RpcRegistrationError: 已被另一形态占用。
        """
        if occupied is None:
            return
        raise RpcRegistrationError(f"RPC 方法形态冲突：{key[0]}.{key[1]} 已登记为{occupied}（同一方法只能一种形态）")

    def resolve(self, service: str, method: str) -> RpcHandler | None:
        """取一元处理器（未登记返回 None，由服务层转 `350023`）。

        只查一元表：流式方法被一元端点调用时应当「不存在」——若回落到流式处理器，等于让
        `POST /rpc/{s}/{m}` 静默跑起一条流，这正是形态互斥要禁止的。

        Args:
            service: 服务标识。
            method: 方法名。

        Returns:
            RpcHandler | None: 处理器；未登记（含只登记了流式）为 None。
        """
        return self._handlers.get((service, method))

    def resolve_stream(self, service: str, method: str) -> RpcStreamHandler | None:
        """取流式处理器（未登记或只有一元方法均为 None，服务层统一转 `350023`）。

        Args:
            service: 服务标识。
            method: 方法名。

        Returns:
            RpcStreamHandler | None: 流式处理器；不可流式调用时为 None。
        """
        return self._stream_handlers.get((service, method))

    def idempotent(self, service: str, method: str) -> bool:
        """取方法的幂等声明（未登记按缺省保守值 `False`）。

        Args:
            service: 服务标识。
            method: 方法名。

        Returns:
            bool: 幂等为 True。
        """
        return bool(self._idempotent.get((service, method), False))

    def declarations(self) -> tuple[RpcDeclaration, ...]:
        """全部方法的契约声明（一元在前、流式补充，各自插入序）。

        Returns:
            tuple[RpcDeclaration, ...]: 含 `idempotent` 与形态的声明序列。
        """
        entries: ConcurrentStableList[RpcDeclaration] = ConcurrentStableList()
        for table, form in ((self._handlers, UNARY_FORM), (self._stream_handlers, STREAM_FORM)):
            for service, method in table:
                entries.add(
                    RpcDeclaration(
                        service=service,
                        method=method,
                        idempotent=self.idempotent(service, method),
                        form=form,
                    )
                )
        return tuple(entries)

    def methods(self) -> tuple[tuple[str, str], ...]:
        """全部一元方法（插入序）。

        与 `stream_methods()` 分开：契约里 `methods`（一元）与 `stream.methods`（流式）分别发布，
        混一张表会让客户端以为所有方法都能流式调。

        Returns:
            tuple[tuple[str, str], ...]: `(service, method)` 序列。
        """
        return tuple(self._handlers.keys())

    def stream_methods(self) -> tuple[tuple[str, str], ...]:
        """全部流式方法（插入序）。

        Returns:
            tuple[tuple[str, str], ...]: `(service, method)` 序列。
        """
        return tuple(self._stream_handlers.keys())

    def services(self) -> tuple[str, ...]:
        """去重后的服务标识（插入序，两形态合并；RPC 配额的键维度）。

        **为什么合并**：配额按 `(tenant, service)` 计，只登记了流式方法的服务同样会消耗配额；
        只看一元表会让这类服务在配额维度上「不存在」。

        Returns:
            tuple[str, ...]: 服务标识序列（一元表在前、流式表补充）。
        """
        ordered: ConcurrentStableList[str] = ConcurrentStableList()
        for table in (self._handlers, self._stream_handlers):
            for service, _method in table:
                if service not in ordered:
                    ordered.add(service)
        return tuple(ordered)

    def __len__(self) -> int:
        """方法总数（两形态合计）。

        Returns:
            int: 已登记的方法数。
        """
        return len(self._handlers) + len(self._stream_handlers)
