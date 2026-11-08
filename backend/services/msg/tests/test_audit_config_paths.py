"""审计排除清单（05-4；Kiwi 2455）：普通读写不逐条留痕，配置写端点由显式审计独占。"""

from __future__ import annotations

import tomllib
from pathlib import Path

import pytest

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

_DATA_PLANE_PATHS = {
    "/api/v1/msg/publish",
    "/api/v1/msg/consume",
    "/api/v1/msg/ack",
    "/api/v1/msg/join",
    "/api/v1/msg/heartbeat",
    "/api/v1/msg/leave",
    "/api/v1/msg/subscribe",
    "/api/v1/msg/connection/heartbeat",
    "/rpc",
}
"""普通读写 / 数据面高频路径（逐条写会洪泛）。"""

_EXPLICIT_AUDIT_PATHS = {
    "/api/v1/msg/admin/replay",
    "/api/v1/msg/admin/topics",
    "/api/v1/msg/admin/quotas",
    "/api/v1/msg/admin/delayed",
    "/api/v1/msg/admin/offsets",
}
"""已由业务显式审计的路径（中间件不再记）。"""


def test_exclude_paths_cover_data_plane_and_explicit_audit() -> None:
    """排除清单同时覆盖认证、数据面与显式审计端点，且无重复。"""
    with _CONFIG.open("rb") as handle:
        settings = tomllib.load(handle)
    paths = list(settings["audit_log"]["exclude_paths"])

    assert set(paths) >= _AUTH_PATHS
    assert set(paths) >= _DATA_PLANE_PATHS
    assert set(paths) >= _EXPLICIT_AUDIT_PATHS
    assert len(paths) == len(set(paths))
