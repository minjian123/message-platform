"""RPC 幂等上下文构造（04-4）：帧闸门与 JSON 端点共用同一份判据。

**为什么单列**：两种形态都要「有没有键 → 方法是不是幂等 → 要不要启用去重」这三步，
写两份就会出现「帧路径去重、JSON 路径不去重」这类静默分叉（04-1 的教训）。

**仅「带键 + 方法声明非幂等」启用**：幂等方法重复执行无副作用，去重表没必要留一条记录；
非幂等方法没带键则根本不该自动重试，服务端也不需要去重。
"""

from __future__ import annotations

from bms_msg.rpc.idempotency import IdempotencyContext, InProcessIdempotencyStore, scoped_idempotency_key
from bms_msg.rpc.registry import RpcRegistry

__all__ = ["idempotency_context"]


def idempotency_context(
    *,
    store: InProcessIdempotencyStore,
    registry: RpcRegistry,
    tenant: str,
    service: str,
    method: str,
    key: str | None,
) -> IdempotencyContext | None:
    """按「带键 + 方法声明非幂等」决定是否构造去重上下文。

    Args:
        store: 进程级去重存储。
        registry: 方法注册表（读幂等声明）。
        tenant: 租户编码（作用域第一维）。
        service: 服务标识。
        method: 方法名。
        key: 客户端给的幂等键业务部分（可为 None）。

    Returns:
        IdempotencyContext | None: 需要去重时的上下文；否则 None（零开销透传）。
    """
    if key is None or registry.idempotent(service, method):
        return None
    return IdempotencyContext(
        store=store,
        key=scoped_idempotency_key(tenant=tenant, service=service, method=method, key=key),
    )
