"""审计排除清单（05-4；Kiwi 2455）：普通读写不逐条留痕，配置写端点由显式审计独占。

**清单从真实路由推导**（台账 #48）：数据面与成员面同时挂在 `/api/v1/msg/*` 与根路径 `/msg/*`，
本用例遍历两条聚合路由、按各自挂载前缀拼出最终路径，再按基座中间件的匹配口径（路径相等或前缀）
逐一核对。原先靠手写常量维护，端点新增与清单脱节时静默放行——根路径五条（`publish` / `consume` /
`ack` / `nack` / `subscribe`）与 `/api/v1/msg/nack` 就是这么漏的；改为自动推导后，新增写端点
忘登记会直接失败。
"""

from __future__ import annotations

import tomllib
from pathlib import Path

import pytest
from fastapi.routing import APIRoute

from bms_msg.api.router import api_router, frame_router

pytestmark = pytest.mark.kiwi_id(2455)

_CONFIG = Path(__file__).resolve().parents[3] / "config.toml"
"""msg 后端配置（`backend/config.toml`）。"""

_AUTH_PATHS = {
    "/api/v1/auth/login",
    "/api/v1/auth/refresh",
    "/api/v1/auth/logout",
    "/api/v1/auth/introspect",
}
"""基座默认认证排除路径（整表覆盖时必须写回）。"""

_EXPLICIT_AUDIT_PATHS = {
    "/api/v1/msg/admin/replay",
    "/api/v1/msg/admin/topics",
    "/api/v1/msg/admin/quotas",
    "/api/v1/msg/admin/delayed",
    "/api/v1/msg/admin/offsets",
}
"""已由业务显式审计的路径族（中间件不再记；配置里写前缀，覆盖其下全部子路径）。"""

_AUDITED_METHODS = {"POST", "PUT", "PATCH", "DELETE"}
"""基座中间件的 `record_methods`：只有写方法才逐条落审计，GET 默认不记。

本用例据此只核对写端点——`GET /msg/subscribe`（SSE）与 `GET /msg/connection` 本就不落审计。
"""

_PREFIX_EXCLUDED = ("/rpc",)
"""按前缀排除的路径族（参数化端点如 `/rpc/{service}/{method}` 无法逐条枚举）。"""

_ROOT_DATA_PLANE = {
    "/msg/publish",
    "/msg/consume",
    "/msg/ack",
    "/msg/nack",
    "/msg/subscribe",
}
"""台账 #48 的核心：根路径数据面五条（原先只有成员三条在册，数据面整族漏登）。"""


def _iter_routes(router: object) -> list[APIRoute]:
    """递归展开聚合路由里的真实 `APIRoute`（聚合路由可能包一层 `_IncludedRouter`）。

    Args:
        router: 待遍历的路由对象。

    Returns:
        list[APIRoute]: 全部叶子路由。
    """
    found: list[APIRoute] = []
    for item in getattr(router, "routes", []):
        if isinstance(item, APIRoute):
            found.append(item)
            continue
        inner = getattr(item, "original_router", None)
        if inner is not None:
            found.extend(_iter_routes(inner))
    return found


def _excluded() -> list[str]:
    """读取配置里的排除清单。

    Returns:
        list[str]: 排除路径（保持配置序）。
    """
    with _CONFIG.open("rb") as handle:
        settings = tomllib.load(handle)
    return list(settings["audit_log"]["exclude_paths"])


def _is_covered(path: str, excluded: list[str]) -> bool:
    """按基座中间件的匹配口径（路径相等或前缀后接 `/`）判断是否已排除。

    参数化路径（如 `/api/v1/msg/admin/topics/{topic_id}/disable`）与配置里的前缀条目
    （`/api/v1/msg/admin/topics`）比对时走「前缀」分支，故不能要求字符串相等。

    Args:
        path: 待判端点路径。
        excluded: 排除清单。

    Returns:
        bool: 已排除返回 True。
    """
    return any(path == item or path.startswith(f"{item}/") for item in excluded)


def _audited_paths() -> list[str]:
    """导出全部写端点的最终路径（按两条聚合路由各自的挂载前缀拼接）。

    Returns:
        list[str]: 写端点路径（去重、升序）。
    """
    found: set[str] = set()
    mounts: list[tuple[object, str]] = [(api_router, "/api/v1"), (frame_router, "")]
    for router, prefix in mounts:
        for route in _iter_routes(router):
            methods: set[str] = set(route.methods or ())
            if methods & _AUDITED_METHODS:
                found.add(f"{prefix}{route.path}")
    return sorted(found)


def test_all_write_endpoints_are_excluded_from_middleware_audit() -> None:
    """全部写端点（两处挂载）都已排除，不会被中间件逐条落审计。"""
    excluded = _excluded()
    missing = [
        path for path in _audited_paths() if not path.startswith(_PREFIX_EXCLUDED) and not _is_covered(path, excluded)
    ]

    assert missing == [], f"以下写端点未进 exclude_paths（会逐条落审计）：{missing}"


def test_root_path_data_plane_entries_are_explicit() -> None:
    """根路径数据面五条逐条在册（台账 #48：原先只有成员三条，数据面整族漏登）。"""
    assert set(_excluded()) >= _ROOT_DATA_PLANE


def test_exclude_paths_cover_auth_and_explicit_audit() -> None:
    """排除清单同时覆盖认证与显式审计路径族，且无重复条目。"""
    paths = _excluded()

    assert set(paths) >= _AUTH_PATHS
    assert set(paths) >= _EXPLICIT_AUDIT_PATHS
    assert len(paths) == len(set(paths))
