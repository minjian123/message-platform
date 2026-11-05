"""RPC 方法注册表与内置处理器（任务 04-1；Kiwi 2433）。

判据：

1. **装配期静态登记**：按 `(service, method)` 登记与解析，未登记返回 None（由服务层转 `350023`）；
2. **重复即拒启**：同一键二次登记抛 `RpcRegistrationError`（`ValueError` 子类）；
3. **插入序**：`methods()` / `services()` 保登记顺序，`services()` 去重；
4. **内置处理器**：`demo.echo` 原样回字节、`demo.length` 回十进制长度字节，结果头标明处理器。
"""

import pytest
from bms_core.core.concurrent import ConcurrentStableDict

from bms_msg.rpc.builtin import (
    DEMO_ECHO,
    DEMO_LENGTH,
    DEMO_SERVICE,
    default_rpc_registry,
    echo_handler,
    length_handler,
)
from bms_msg.rpc.registry import RpcCall, RpcRegistrationError, RpcRegistry, RpcReply

pytestmark = pytest.mark.kiwi_id(2433)


def _call(payload: bytes = b"abc", *, service: str = "demo", method: str = "echo") -> RpcCall:
    """构造一次调用（用例用）。

    Args:
        payload: 参数原始字节。
        service: 服务标识。
        method: 方法名。

    Returns:
        RpcCall: 调用入参。
    """
    headers: ConcurrentStableDict[str, str] = ConcurrentStableDict()
    headers.set("x-biz", "audit")
    return RpcCall(service=service, method=method, payload=payload, headers=headers, tenant="demo")


async def _noop(call: RpcCall) -> RpcReply:
    """一个返回空结果的处理器替身（只为占位，注册表不校验返回值）。

    Args:
        call: 调用入参（未使用）。

    Returns:
        RpcReply: 空结果。
    """
    del call
    return RpcReply()


async def test_register_and_resolve() -> None:
    """登记后可解析到同一处理器对象。"""
    registry = RpcRegistry()
    registry.register("demo", "echo", echo_handler)

    assert registry.resolve("demo", "echo") is echo_handler
    assert registry.resolve("demo", "missing") is None
    assert len(registry) == 1


async def test_duplicate_registration_rejected() -> None:
    """同一 `(service, method)` 二次登记即抛（装配期拒启，不是运行期覆盖）。"""
    registry = RpcRegistry()
    registry.register("demo", "echo", echo_handler)

    with pytest.raises(RpcRegistrationError, match="重复登记"):
        registry.register("demo", "echo", length_handler)
    # 同名不同服务不算重复。
    registry.register("other", "echo", length_handler)
    assert len(registry) == 2


async def test_methods_and_services_keep_insertion_order() -> None:
    """`methods()` 保插入序；`services()` 去重且保首次出现顺序。"""
    registry = RpcRegistry()
    registry.register("b", "two", _noop)
    registry.register("a", "one", echo_handler)
    registry.register("b", "three", length_handler)

    assert registry.methods() == (("b", "two"), ("a", "one"), ("b", "three"))
    assert registry.services() == ("b", "a")


async def test_default_registry_exposes_two_methods() -> None:
    """缺省注册表登记两个内置方法（装配与契约导出共用同一构造入口）。"""
    registry = default_rpc_registry()

    assert registry.methods() == ((DEMO_SERVICE, DEMO_ECHO), (DEMO_SERVICE, DEMO_LENGTH))
    assert registry.services() == (DEMO_SERVICE,)


async def test_echo_handler_returns_payload_verbatim() -> None:
    """`demo.echo` 原样回传参数字节（不假定编码）。"""
    reply = await echo_handler(_call(b"\x00\xff binary"))

    assert reply.payload == b"\x00\xff binary"
    assert dict(reply.headers.items()) == {"handler": "demo.echo"}


async def test_length_handler_returns_decimal_bytes() -> None:
    """`demo.length` 回传十进制长度字节。"""
    reply = await length_handler(_call(b"12345"))

    assert reply.payload == b"5"
    assert dict(reply.headers.items()) == {"handler": "demo.length"}
