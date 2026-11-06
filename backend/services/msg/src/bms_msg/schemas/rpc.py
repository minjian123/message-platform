"""RPC 契约模型（任务 04-1）：JSON 形态的请求与结果。

**payload 用 base64 字符串**（与 `/msg/publish` 完全同口径）：能承载任意二进制（protobuf / msgpack /
文本），且响应 schema 确定非空——不撞基座「空 schema」门禁（`EMPTY_SCHEMA_ALLOWLIST` 为空集，
业务 schema 一律不得入内）。换成「`payload` 为任意 JSON 值」会让字段在 OpenAPI 里退化成空 schema，
且与帧路径（原始字节）变成两套编码规则。

**帧形态不进模型**：帧的 payload 段是原始字节，直接走 `set_raw` 之外的独立参数（见
`api/rpc/gate.py`），不做 base64 往返；本模型的 `payload` 字段只服务 JSON 路径。
"""

from __future__ import annotations

from typing import Annotated

from bms_core.core.concurrent import ConcurrentStableDict
from bms_core.schemas.base import CONTRACT_COLLECTION, CONTRACT_STABLE_DICT, BaseSchema
from pydantic import Field

from bms_msg.core.config import RPC_CALLER_MAX_LENGTH, RPC_DEADLINE_MAX_MS

__all__ = ["RpcRequest", "RpcResult"]


class RpcRequest(BaseSchema):
    """RPC 调用请求（JSON 形态）。"""

    payload: str = Field(default="", description="调用参数（base64 编码；空串表示无参数）")
    headers: Annotated[ConcurrentStableDict[str, str], CONTRACT_COLLECTION] = Field(
        default_factory=CONTRACT_STABLE_DICT, description="结构化头（透传给处理器并随结果回显）"
    )
    trace_id: str | None = Field(
        default=None, max_length=128, description="链路标识（写入日志与响应头；与 PublishRequest 同尺）"
    )
    caller: str | None = Field(
        default=None,
        max_length=RPC_CALLER_MAX_LENGTH,
        description="调用方服务标识（非鉴权字段，只透传与记录；与 HTTP 头冲突时以 HTTP 头为准）",
    )
    deadline_ms: int | None = Field(
        default=None,
        ge=0,
        le=RPC_DEADLINE_MAX_MS,
        description=(
            "剩余预算（毫秒，相对量；缺省 30000）。与帧头 `deadline_ms` 同名字段同一语义；"
            "服务端扣本跳耗时后把剩余值传给下游，耗尽返回 350004"
        ),
    )
    idempotency_key: str | None = Field(
        default=None,
        max_length=128,
        description="幂等键（04-4；带键且方法非幂等时服务端按作用域去重，重试全程复用同一键）",
    )
    retry_budget: int | None = Field(
        default=None,
        ge=0,
        description="调用链剩余可重试次数（04-4；与帧头 retry_budget 同名同义，逐跳递减防重试风暴）",
    )


class RpcResult(BaseSchema):
    """RPC 调用结果（JSON 形态）。"""

    payload: str = Field(description="结果（base64 编码；空串表示无结果体）")
    headers: Annotated[ConcurrentStableDict[str, str], CONTRACT_COLLECTION] = Field(
        default_factory=CONTRACT_STABLE_DICT, description="结果结构化头"
    )
