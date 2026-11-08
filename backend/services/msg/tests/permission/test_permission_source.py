"""真实 RBAC 接入（Kiwi 2456）：msg 侧权限数据来源 + 注册 + 失败口径。"""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Any, cast

import pytest
from bms_core.core.concurrent import ConcurrentStableDict, ConcurrentStableList
from bms_core.core.exceptions import ServiceUnavailableError
from bms_core.core.plugin import default_plugin_registry
from bms_core.servicecall.base import BaseServiceClient, ServiceResponse
from fastapi import FastAPI

from bms_msg.main import ApplicationFactory
from bms_msg.permission import MSG_PERMISSION_DATA_SOURCE_KEY, build_msg_permission_source, register_msg_permission
from bms_msg.permission.source import (
    MsgPlatformPermissionDataSource,
    _as_int,
    _payload_data,
)

pytestmark = pytest.mark.kiwi_id(2456)


class _Client(BaseServiceClient):
    """服务间客户端替身（可配置响应 / 抛错，并记录请求）。"""

    key: str = "service_client"

    def __init__(self, *, response: ServiceResponse | None = None, error: Exception | None = None) -> None:
        """初始化。"""
        self.response = response
        self.error = error
        self.requests: ConcurrentStableList[Any] = ConcurrentStableList()

    async def call(self, request: Any) -> ServiceResponse:
        """模拟调用并记录请求。"""
        self.requests.add(request)
        if self.error is not None:
            raise self.error
        return self.response if self.response is not None else ServiceResponse(status_code=200, content=b"{}")


def _response(data: object, *, code: int = 0, status_code: int = 200) -> ServiceResponse:
    """构造统一响应体。"""
    body = {"code": code, "message": "ok", "data": data}
    return ServiceResponse(status_code=status_code, content=json.dumps(body).encode())


async def test_load_subject_maps_contract_payload() -> None:
    """`subject` 请求带 user_id；响应映射为基座数据契约（字符串主键还原）。"""
    client = _Client(
        response=_response(
            {
                "role_ids": [11, 12],
                "role_types": {"11": "custom", "12": "system"},
                "grants": [{"perm_type": "permission", "target_id": 902}],
                "data_scope_rules": [
                    {"data_source_key": "role", "policy_type": "select", "config": [{"field": "area_code"}]}
                ],
                "field_perms": [{"form_id": 901, "field_key": "salary", "visible": False, "editable": False}],
            }
        )
    )
    data = await MsgPlatformPermissionDataSource(client).load_subject(user_id=1001)
    assert sorted(data.role_ids) == [11, 12]
    assert data.role_types.get(11) == "custom"
    assert [(item.perm_type, item.target_id) for item in data.grants] == [("permission", 902)]
    assert data.data_scope_rules[0].policy_type == "select"
    assert data.field_perms[0].field_key == "salary"
    request = client.requests[0]
    assert request.service == "platform"
    assert request.method == "POST"
    assert request.path == "/api/v1/platform/internal/permissions/subject"


async def test_load_metadata_maps_contract_payload() -> None:
    """`metadata` 请求带 locale；菜单 / 权限码索引还原为整数键。"""
    client = _Client(
        response=_response({"version": 3, "menu_forms": {"10": [1, 2]}, "permission_codes": {"20": "msg:manage"}})
    )
    metadata = await MsgPlatformPermissionDataSource(client).load_metadata(locale="zh-CN")
    assert metadata.version == 3
    assert list(metadata.menu_forms.get(10) or []) == [1, 2]
    assert metadata.permission_codes.get(20) == "msg:manage"
    assert client.requests[0].method == "GET"


async def test_source_fails_closed() -> None:
    """非 2xx / 业务码非 0 / data 非对象 → 10007 / 503。"""
    for response in (
        _response({}, status_code=500),
        _response({}, code=1),
        _response(None),
    ):
        source = MsgPlatformPermissionDataSource(_Client(response=response))
        with pytest.raises(ServiceUnavailableError) as excinfo:
            await source.load_subject(user_id=1)
        assert excinfo.value.code == 10007
    with pytest.raises(ServiceUnavailableError):
        await MsgPlatformPermissionDataSource(_Client(error=RuntimeError("down"))).load_subject(user_id=1)


async def test_build_source_reads_service_client() -> None:
    """请求级工厂从 `app.state.service_client` 取客户端。"""
    app = FastAPI()
    client = _Client(response=_response({"role_ids": []}))
    app.state.service_client = client
    request = SimpleNamespace(app=app)
    source = await build_msg_permission_source(cast("Any", request))
    assert source.key == MSG_PERMISSION_DATA_SOURCE_KEY


def test_register_and_prepare_settings() -> None:
    """注册真实 RBAC：数据来源工厂可解析；`prepare_settings` 置 provider。"""
    registry = default_plugin_registry()
    registry.reset()
    try:
        settings = SimpleNamespace(
            permission=SimpleNamespace(provider="", profile="smb", options=ConcurrentStableDict()),
            resource_acl=SimpleNamespace(provider=""),
        )
        register_msg_permission(cast("Any", settings))
        factory = ApplicationFactory()
        factory.prepare_settings(cast("Any", settings))
        assert settings.permission.provider == "rbac"
        assert settings.resource_acl.provider == "msg.topic"
    finally:
        registry.reset()


def test_as_int_variants() -> None:
    """主键还原：整数 / 数字串 / 布尔 / 非法串 / 其他类型。"""
    assert _as_int(7) == 7
    assert _as_int("8") == 8
    assert _as_int(True) is None
    assert _as_int("x") is None
    assert _as_int(1.5) is None


def test_payload_data_rejects_non_mapping() -> None:
    """响应体不是对象 → 10007。"""
    response = ServiceResponse(status_code=200, content=b"[]")
    with pytest.raises(ServiceUnavailableError):
        _payload_data(response, "test")


async def test_contract_skips_non_numeric_keys() -> None:
    """契约里的非数字键在映射时跳过（不污染角色 / 菜单 / 权限码索引）。"""
    subject = _Client(
        response=_response(
            {
                "role_ids": [1],
                "role_types": {"bad": "system", "1": "custom"},
                "grants": [],
                "data_scope_rules": [],
                "field_perms": [],
            }
        )
    )
    data = await MsgPlatformPermissionDataSource(subject).load_subject(user_id=1)
    assert list(data.role_ids) == [1]
    assert data.role_types.get(1) == "custom"

    metadata = _Client(response=_response({"version": 1, "menu_forms": {"bad": [1]}, "permission_codes": {"bad": "x"}}))
    parsed = await MsgPlatformPermissionDataSource(metadata).load_metadata()
    assert list(parsed.menu_forms) == []
    assert list(parsed.permission_codes) == []
