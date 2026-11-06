"""校验器端口与注册表（任务 03-3；Kiwi 2423）：未注册跳过 / 注册生效 / 失败带详情。

端口的价值是「接口先定、调用方后到」：真实调用方是消费端 SDK（8B_03），broker 不调用、也不解 payload。
因此本文件的断言都是「注册表本身的行为」，不涉及投递链路（那条链路一行未改）。
"""

from __future__ import annotations

import pytest
from bms_core.msgclient.payload import CONTENT_TYPE_JSON, CONTENT_TYPE_MSGPACK

from bms_msg.protocol.validation import PayloadValidationError, PayloadValidator, ValidatorRegistry

pytestmark = pytest.mark.kiwi_id(2423)


class _RequireKey:
    """校验器替身：要求顶层必须出现某个键（模拟「字段缺失」判定）。"""

    def __init__(self, content_type: str, key: str) -> None:
        """初始化。

        Args:
            content_type: 负责的内容类型。
            key: 必需字段名。
        """
        self.content_type = content_type
        self._key = key

    def validate(self, payload: bytes) -> None:
        """字节里必须包含该键的 UTF-8 形式。

        Args:
            payload: 消息体。

        Raises:
            PayloadValidationError: 字段缺失（带 field / expected 详情）。
        """
        if self._key.encode() not in payload:
            raise PayloadValidationError(
                f"缺少字段 {self._key}",
                {"field": self._key, "expected": "present", "actual": "missing"},
            )


def test_empty_registry_skips_validation() -> None:
    """未注册即不校验（不阻断投递）。"""
    registry = ValidatorRegistry()

    registry.validate(CONTENT_TYPE_MSGPACK, b"anything")
    assert registry.get(CONTENT_TYPE_MSGPACK) is None
    assert registry.content_types == ()


def test_registered_validator_runs() -> None:
    """注册后按内容类型命中：合法通过、非法抛错并带详情。"""
    registry = ValidatorRegistry()
    registry.register(_RequireKey(CONTENT_TYPE_MSGPACK, "order_id"))

    registry.validate(CONTENT_TYPE_MSGPACK, b'{"order_id": 1}')

    with pytest.raises(PayloadValidationError) as excinfo:
        registry.validate(CONTENT_TYPE_MSGPACK, b'{"amount": 1}')
    assert excinfo.value.detail["field"] == "order_id"
    assert excinfo.value.detail["actual"] == "missing"


def test_other_content_type_still_skipped() -> None:
    """只校验已登记的类型：另一种类型照常放行。"""
    registry = ValidatorRegistry()
    registry.register(_RequireKey(CONTENT_TYPE_MSGPACK, "order_id"))

    registry.validate(CONTENT_TYPE_JSON, b"{}")
    assert registry.get(CONTENT_TYPE_JSON) is None


def test_lookup_normalizes_content_type() -> None:
    """查找前规范化：带参数、大写都能命中。"""
    registry = ValidatorRegistry()
    registry.register(_RequireKey(CONTENT_TYPE_MSGPACK, "order_id"))

    assert registry.get("Application/MSGPACK; charset=utf-8") is not None


def test_duplicate_registration_overrides() -> None:
    """同内容类型重复注册：后注册者覆盖（便于测试与插件式扩展），不报错。"""
    registry = ValidatorRegistry()
    registry.register(_RequireKey(CONTENT_TYPE_MSGPACK, "first"))
    registry.register(_RequireKey(CONTENT_TYPE_MSGPACK, "second"))

    assert registry.content_types == (CONTENT_TYPE_MSGPACK,)
    registry.validate(CONTENT_TYPE_MSGPACK, b"second")
    with pytest.raises(PayloadValidationError):
        registry.validate(CONTENT_TYPE_MSGPACK, b"first")


def test_error_without_detail_defaults_to_empty() -> None:
    """详情缺省为空映射（`nack` 的 detail 因此不会变成 null）。"""
    error = PayloadValidationError("坏了")

    assert error.detail == {}
    assert str(error) == "坏了"


def test_protocol_is_structural() -> None:
    """校验器是结构化类型：普通类只要属性与方法齐备即可用（不必继承）。"""
    validator = _RequireKey(CONTENT_TYPE_JSON, "x")

    assert isinstance(validator, PayloadValidator)
