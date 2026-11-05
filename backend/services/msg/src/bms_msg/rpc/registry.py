"""RPC 方法注册表（任务 04-1）：装配期静态登记、按 `service + method` 分发。

**为什么是实例而不是模块级全局**：仓内进程级状态一律经 `app.state` + 依赖注入（位点 / 组 / 限流器
全部如此）。模块级注册表会在多 app 实例（monorepo 测试、同进程双服务）之间串扰，用例不得不显式
`reset()`；实例化后每个应用各持一份，用例各造一份，互不影响。

**重复即拒启**：同一 `(service, method)` 二次登记抛 `RpcRegistrationError`（`ValueError` 子类）。
装配期调用，异常直接让服务拒启——这正是「启动期静态注册 + 重复即拒启」要的效果；换成运行期
后写覆盖先写，就变成「谁最后加载谁生效」的隐式行为，最难排障。

**边界**：本模块只管「谁处理」。限流 / 参数上限 / 结果上限在 `rpc/service.py`，编解码在 API 层。
"""

from __future__ import annotations

from collections.abc import Awaitable
from dataclasses import dataclass, field
from typing import Protocol

from bms_core.core.concurrent import ConcurrentStableDict, ConcurrentStableList
from bms_core.core.roots import BaseFrameworkObject, BaseValueObject

__all__ = [
    "RpcCall",
    "RpcHandler",
    "RpcRegistrationError",
    "RpcRegistry",
    "RpcReply",
]


@dataclass(frozen=True)
class RpcCall(BaseValueObject):
    """一次调用的入参（handler 侧契约；冻结实例，handler 改不动它）。"""

    service: str
    """服务标识（取自路径 `{service}`）。"""

    method: str
    """方法名（取自路径 `{method}`）。"""

    payload: bytes
    """调用参数原始字节（broker 不解析、不解压）。"""

    headers: ConcurrentStableDict[str, str]
    """结构化头（透传给 handler；handler 可据此做业务分支）。"""

    tenant: str
    """租户编码（已由数据面解析链判定；本任务只透传，不做授权）。"""

    trace_id: str | None = None
    """链路标识（透传进 handler 与日志）。"""

    caller: str | None = None
    """调用方服务标识（HTTP 头优先，见详细设计 §9.2；非鉴权字段）。"""


def _empty_headers() -> ConcurrentStableDict[str, str]:
    """空结果头（dataclass 默认工厂；显式标注泛型，避免 strict 模式下退化成 Unknown）。"""

    return ConcurrentStableDict()


@dataclass(frozen=True)
class RpcReply(BaseValueObject):
    """一次调用的结果（handler 侧契约）。"""

    payload: bytes = b""
    """结果原始字节（帧形态直接进 payload 段，JSON 形态转 base64）。"""

    headers: ConcurrentStableDict[str, str] = field(default_factory=_empty_headers)
    """结果结构化头（回显给调用方）。"""


class RpcHandler(Protocol):
    """处理器契约：一个异步可调用对象，入参 `RpcCall`、出参 `RpcReply`。"""

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


class RpcRegistrationError(ValueError):
    """装配期登记冲突（同一 `(service, method)` 重复登记）：拒启，不是业务错误。

    用 `ValueError` 而不是 `BizError`：这个异常永远不该到达调用方——它在 `configure_service()`
    里抛出，此时还没有任何请求。给它一个产品码反而会让人以为「运行期可以捕获后继续」。
    """


class RpcRegistry(BaseFrameworkObject):
    """进程内方法注册表（插入序；装配期填，之后只读）。"""

    def __init__(self) -> None:
        """初始化空注册表。"""
        self._handlers: ConcurrentStableDict[tuple[str, str], RpcHandler] = ConcurrentStableDict()

    def register(self, service: str, method: str, handler: RpcHandler) -> None:
        """登记一个处理器（重复即拒）。

        Args:
            service: 服务标识。
            method: 方法名。
            handler: 处理器。

        Raises:
            RpcRegistrationError: 同一 `(service, method)` 已登记。
        """
        key = (service, method)
        if self._handlers.get(key) is not None:
            raise RpcRegistrationError(f"RPC 方法重复登记：{service}.{method}")
        self._handlers.set(key, handler)

    def resolve(self, service: str, method: str) -> RpcHandler | None:
        """取处理器（未登记返回 None，由服务层转 `350023`）。

        Args:
            service: 服务标识。
            method: 方法名。

        Returns:
            RpcHandler | None: 处理器；未登记为 None。
        """
        return self._handlers.get((service, method))

    def methods(self) -> tuple[tuple[str, str], ...]:
        """全部方法（插入序）。

        Returns:
            tuple[tuple[str, str], ...]: `(service, method)` 序列。
        """
        return tuple(self._handlers.keys())

    def services(self) -> tuple[str, ...]:
        """去重后的服务标识（插入序；RPC 配额的键维度）。

        Returns:
            tuple[str, ...]: 服务标识序列。
        """
        ordered: ConcurrentStableList[str] = ConcurrentStableList()
        for service, _method in self._handlers:
            if service not in ordered:
                ordered.add(service)
        return tuple(ordered)

    def __len__(self) -> int:
        """方法总数。

        Returns:
            int: 已登记的方法数。
        """
        return len(self._handlers)
