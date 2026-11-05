"""RPC 名称形态校验（任务 04-1）：服务端与客户端共用同一份判据。

**为什么单列一个模块**：`rpc/service.py` 依赖 `broker.backpressure`（限流器），而客户端
（`client/rpc.py`）不该把 broker 侧模块拖进 bms 进程；把这条纯函数（只看正则与 `ParamError`）
放在这里，两边都能用，且不会因为「客户端自己写一份正则」而漂移。

客户端先判一次是**省一次往返**，不是替代服务端判定：服务端仍会判（它不能假设调用方守规矩）。
"""

from __future__ import annotations

from bms_core.core.exceptions import ParamError

from bms_msg.core.config import RPC_METHOD_PATTERN, RPC_SERVICE_PATTERN

__all__ = ["ensure_rpc_name"]


def ensure_rpc_name(service: str, method: str) -> None:
    """校验路径参数的形态（契约层 `10001`：键在而值形态非法）。

    Args:
        service: 服务标识。
        method: 方法名。

    Raises:
        ParamError: 任一形态不符（`10001`）。
    """
    if RPC_SERVICE_PATTERN.match(service) is None:
        raise ParamError(f"RPC 服务标识形态非法：{service!r}（允许字母数字与 . _ -，1~64 字符）")
    if RPC_METHOD_PATTERN.match(method) is None:
        raise ParamError(f"RPC 方法名形态非法：{method!r}（允许字母数字与 . _ -，1~128 字符）")
