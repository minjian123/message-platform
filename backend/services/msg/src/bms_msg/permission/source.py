"""msg 侧权限数据来源（05-3）：经 platform 内部只读契约取原始数据。

- `load_subject`：`POST /api/v1/platform/internal/permissions/subject`；
- `load_metadata`：`GET /api/v1/platform/internal/permissions/metadata`；
- 算法不在本服务：聚合 / 快照 / 缓存全部在基座 `bms_core.permission.engine`；
- 失败口径：下游不可达 / 非 2xx / 契约非法 → `ServiceUnavailableError` 10007 / 503，
  **不降级为「无权限」也不放行**（与 identity 同款实现）。
"""

from __future__ import annotations

from typing import Any, cast

from bms_core.core.concurrent import (
    ConcurrentStableDict,
    ConcurrentStableFrozenDict,
    ConcurrentStableFrozenList,
    ConcurrentStableList,
    ConcurrentStableSet,
)
from bms_core.core.exceptions import ServiceUnavailableError
from bms_core.i18n.base import DEFAULT_LOCALE
from bms_core.permission.field import FieldPermission
from bms_core.permission.sources import (
    BasePermissionDataSource,
    DataScopeRule,
    PermissionGrant,
    PermissionMetadata,
    PermissionSubjectData,
)
from bms_core.servicecall.base import BaseServiceClient, ServiceCallPolicy, ServiceRequest, ServiceResponse

from bms_msg.permission.schemas import PlatformPermissionMetadata, PlatformPermissionSubject

__all__ = ["MSG_PERMISSION_DATA_SOURCE_KEY", "MsgPlatformPermissionDataSource"]

PLATFORM_SERVICE = "platform"
"""权限数据归属服务标识。"""

MSG_PERMISSION_DATA_SOURCE_KEY = "msg_platform_contract_permission_data_source"
"""msg 侧数据来源标识（与实现类 `key` 一致）。"""

_PERMISSION_SCOPES: ConcurrentStableFrozenList[str] = ConcurrentStableFrozenList(("permission",))
"""内部权限取数所需服务 JWT scope。"""

_SUBJECT_PATH = "/api/v1/platform/internal/permissions/subject"
_METADATA_PATH = "/api/v1/platform/internal/permissions/metadata"
_PERMISSION_INTERFACE = "platform 权限数据接口"


class MsgPlatformPermissionDataSource(BasePermissionDataSource):
    """msg 侧数据来源：经 platform 内部只读契约取原始数据。"""

    key: str = MSG_PERMISSION_DATA_SOURCE_KEY

    def __init__(self, client: BaseServiceClient) -> None:
        """初始化。

        Args:
            client: 服务间调用客户端（出站附自签服务 JWT）。
        """
        self._client = client

    async def load_subject(self, *, user_id: int) -> PermissionSubjectData:
        """按用户取聚合输入（角色 / 类型 / 授权 / 数据范围 / 字段收窄）。

        Args:
            user_id: 用户主键。

        Returns:
            PermissionSubjectData: 聚合输入。

        Raises:
            ServiceUnavailableError: 下游不可达 / 非 2xx / 响应契约非法（10007 / 503）。
        """
        body: ConcurrentStableDict[str, object] = ConcurrentStableDict()
        body.set("user_id", user_id)
        data = await self._post(_SUBJECT_PATH, body)
        parsed = PlatformPermissionSubject.model_validate(data)
        role_types: ConcurrentStableDict[int, str] = ConcurrentStableDict()
        for key, value in parsed.role_types.items():
            role_id = _as_int(key)
            if role_id is not None:
                role_types.set(role_id, value)
        return PermissionSubjectData(
            role_ids=ConcurrentStableSet(parsed.role_ids),
            role_types=role_types,
            grants=ConcurrentStableList(
                PermissionGrant(perm_type=item.perm_type, target_id=item.target_id) for item in parsed.grants
            ),
            data_scope_rules=ConcurrentStableList(
                DataScopeRule(
                    data_source_key=item.data_source_key,
                    policy_type=item.policy_type,
                    config=ConcurrentStableList(item.config),
                )
                for item in parsed.data_scope_rules
            ),
            field_perms=ConcurrentStableList(
                FieldPermission(
                    form_id=item.form_id,
                    field_key=item.field_key,
                    visible=item.visible,
                    editable=item.editable,
                )
                for item in parsed.field_perms
            ),
        )

    async def load_metadata(self, *, locale: str = DEFAULT_LOCALE) -> PermissionMetadata:
        """取权限元数据（菜单 → 表单、权限码 id → 权限码）。

        Args:
            locale: 语言标识。

        Returns:
            PermissionMetadata: 元数据索引。

        Raises:
            ServiceUnavailableError: 下游不可达 / 非 2xx / 响应契约非法（10007 / 503）。
        """
        query: ConcurrentStableDict[str, str] = ConcurrentStableDict()
        query.set("locale", locale)
        data = await self._get(_METADATA_PATH, query)
        parsed = PlatformPermissionMetadata.model_validate(data)
        metadata = PermissionMetadata(version=parsed.version)
        for key, form_ids in parsed.menu_forms.items():
            menu_id = _as_int(key)
            if menu_id is not None:
                metadata.menu_forms.set(menu_id, ConcurrentStableList(form_ids))
        for key, code in parsed.permission_codes.items():
            permission_id = _as_int(key)
            if permission_id is not None:
                metadata.permission_codes.set(permission_id, code)
        return metadata

    async def _post(self, path: str, body: ConcurrentStableDict[str, object]) -> ConcurrentStableDict[str, object]:
        """发起内部 POST 并解析统一响应 `data`。

        Args:
            path: 公开契约路径。
            body: JSON 请求体。

        Returns:
            ConcurrentStableDict[str, object]: 统一响应 `data`。

        Raises:
            ServiceUnavailableError: 下游不可达 / 非 2xx / 契约非法（10007 / 503）。
        """
        response = await self._call(
            ServiceRequest(
                service=PLATFORM_SERVICE,
                method="POST",
                path=path,
                json_body=ConcurrentStableFrozenDict(body),
                policy=ServiceCallPolicy(scopes=_PERMISSION_SCOPES),
            )
        )
        return _payload_data(response, _PERMISSION_INTERFACE)

    async def _get(self, path: str, query: ConcurrentStableDict[str, str]) -> ConcurrentStableDict[str, object]:
        """发起内部 GET 并解析统一响应 `data`。

        Args:
            path: 公开契约路径。
            query: 查询参数。

        Returns:
            ConcurrentStableDict[str, object]: 统一响应 `data`。

        Raises:
            ServiceUnavailableError: 下游不可达 / 非 2xx / 契约非法（10007 / 503）。
        """
        response = await self._call(
            ServiceRequest(
                service=PLATFORM_SERVICE,
                method="GET",
                path=path,
                query=ConcurrentStableFrozenDict(query),
                policy=ServiceCallPolicy(scopes=_PERMISSION_SCOPES),
            )
        )
        return _payload_data(response, _PERMISSION_INTERFACE)

    async def _call(self, request: ServiceRequest) -> ServiceResponse:
        """发起服务间调用（异常统一转 10007 / 503，不降级为无权限 / 放行）。

        Args:
            request: 服务间调用请求。

        Returns:
            ServiceResponse: 响应。

        Raises:
            ServiceUnavailableError: 下游不可达（10007 / 503）。
        """
        try:
            return await self._client.call(request)
        except Exception as exc:
            raise ServiceUnavailableError(f"{_PERMISSION_INTERFACE}不可用") from exc


def _as_int(raw: object) -> int | None:
    """把契约里的字符串主键还原为整数（非数字返回 None）。

    Args:
        raw: 原始键。

    Returns:
        int | None: 整数主键；非法为 None。
    """
    if isinstance(raw, int) and not isinstance(raw, bool):
        return raw
    if isinstance(raw, str):
        try:
            return int(raw)
        except ValueError:
            return None
    return None


def _payload_data(response: ServiceResponse, interface: str) -> ConcurrentStableDict[str, object]:
    """解析服务响应为统一响应体并取 `data`（异常即服务不可用）。

    Args:
        response: 服务间调用响应。
        interface: 错误提示用接口名称。

    Returns:
        ConcurrentStableDict[str, object]: 统一响应 `data`。

    Raises:
        ServiceUnavailableError: 非 2xx / 业务码非 0 / data 非对象（10007 / 503）。
    """
    if response.status_code != 200:
        raise ServiceUnavailableError(f"{interface}不可用（HTTP {response.status_code}）")
    payload = response.payload()
    if not isinstance(payload, dict):
        raise ServiceUnavailableError(f"{interface}返回非法响应")
    body = ConcurrentStableDict(cast("dict[str, object]", payload))
    if body.get("code") != 0:
        raise ServiceUnavailableError(f"{interface}返回非法响应")
    data = cast("Any", body.get("data"))
    if not isinstance(data, dict):
        raise ServiceUnavailableError(f"{interface}返回契约非法")
    return ConcurrentStableDict(cast("dict[str, object]", data))
