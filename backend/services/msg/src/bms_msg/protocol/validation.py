"""消息体校验器端口与注册表（任务 03-3 详细设计 §6）。

**为什么在协议层而不是业务层**：它描述的是「拿到某个 `content_type` 的字节后，按什么规则判断字段是否
合法」，与线格式同层；具体 schema 由调用方提供，broker 一概不知。

**为什么 broker 不默认调用**：解 payload 需要业务 schema（broker 没有），且一旦解包就破零拷贝。
端口的价值是「接口先定、调用方后到」——真实的调用方是消费端 SDK（8B_03）。

**默认空注册表 = 不校验**：未注册即放行，不让「没配校验器」变成「消息投不出去」。校验失败由消费端
转成 `nack(schema_invalid, detail)` 上报，broker 按既有死信路径处置（详细设计 §5.1）。
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Protocol, runtime_checkable

from bms_msg.protocol.payload import normalize_content_type

__all__ = [
    "PayloadValidationError",
    "PayloadValidator",
    "ValidatorRegistry",
]


class PayloadValidationError(ValueError):
    """消息体不符合 schema（消费端捕获后转 `nack`）。

    `detail` 直接作为 `nack` 的 `detail` 上报，因此约定为「字段名 → 说明」的扁平字符串映射
    （如 `{"field": "order_id", "expected": "str", "actual": "int"}`），便于 06 域直接展示与筛选。
    """

    def __init__(self, message: str, detail: Mapping[str, str] | None = None) -> None:
        """初始化。

        Args:
            message: 可读失败原因。
            detail: 字段级详情（缺省空映射）。
        """
        super().__init__(message)
        self.detail: Mapping[str, str] = dict(detail) if detail is not None else {}


@runtime_checkable
class PayloadValidator(Protocol):
    """按内容类型校验消息体的最小面（结构化类型：调用方不必继承任何基类）。"""

    content_type: str
    """本校验器负责的媒体类型（注册时会被规范化，查找时同口径）。"""

    def validate(self, payload: bytes) -> None:
        """校验消息体。

        Args:
            payload: 消息体原始字节（未解压、未解码）。

        Raises:
            PayloadValidationError: 字段缺失 / 类型错误 / 顶层不是 Map。
        """
        ...


class ValidatorRegistry:
    """按 `content_type` 索引的校验器表（进程级；同键重复注册后注册者覆盖）。"""

    def __init__(self) -> None:
        """初始化（空表 = 不校验）。"""
        self._validators: dict[str, PayloadValidator] = {}

    def register(self, validator: PayloadValidator) -> None:
        """登记一个校验器（同 `content_type` 覆盖旧值，不报错）。

        Args:
            validator: 校验器（其 `content_type` 会被规范化为小写主体）。
        """
        self._validators[normalize_content_type(validator.content_type)] = validator

    @property
    def content_types(self) -> tuple[str, ...]:
        """已登记的内容类型（排障与用例断言用）。

        Returns:
            tuple[str, ...]: 规范化后的内容类型（注册序）。
        """
        return tuple(self._validators)

    def get(self, content_type: str) -> PayloadValidator | None:
        """按内容类型取校验器。

        Args:
            content_type: 内容类型（查找前规范化，带参数也能命中）。

        Returns:
            PayloadValidator | None: 校验器；未登记返回 None（调用方据此跳过校验）。
        """
        return self._validators.get(normalize_content_type(content_type))

    def validate(self, content_type: str, payload: bytes) -> None:
        """校验消息体（未登记该类型则直接通过）。

        Args:
            content_type: 内容类型。
            payload: 消息体原始字节。

        Raises:
            PayloadValidationError: 已登记校验器判定不合法。
        """
        validator = self.get(content_type)
        if validator is None:
            return
        validator.validate(payload)
