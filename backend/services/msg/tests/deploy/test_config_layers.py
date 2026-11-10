"""配置分层与启动守卫（Kiwi 2474；任务 07-2 §4 / §5 / §8）。

覆盖策略：

- **四层优先级走真实 `Settings()`**：临时配置目录写基线 / 环境覆盖 / dotenv，进程环境变量按层撤除，
  逐层断言回落；环境选择器与非法值也在此取证；
- **生产禁用项走纯函数**：扁平映射直接喂 `validate_config_invariants`，逐条覆盖失败分支；
  另有一例走真实装配 + `BMS_APP__DEBUG=true`，证明门禁判的是**最终生效值**而非静态文件；
- **失效关闭走基座真实入口**：空密钥可装配、签发时抛 `ConfigError`；
- **启动接线用 spy**：守卫在 `configure_service()` 首行被调用，抛错即中断后续装配。
"""

from __future__ import annotations

import os
from pathlib import Path
from types import SimpleNamespace

import pytest
from bms_core.core import config as base_config
from bms_core.core.concurrent import ConcurrentStableDict, ConcurrentStableList
from bms_core.core.config import Settings, load_settings
from bms_core.core.exceptions import ConfigError
from bms_core.security.jwt import JwtTokenCodecFactory
from fastapi import FastAPI

from bms_msg.config_guard import flatten_settings, validate_config_invariants
from bms_msg.main import ApplicationFactory

pytestmark = pytest.mark.kiwi_id(2474)

_BASE = """
[app]
name = "msg 消息平台"
env = "{env}"
worker_id = {worker}

[server]
host = "0.0.0.0"
port = 8000

[log]
level = "INFO"

[database.platform]
url = "{url}"

[database.tenants]
url = "{url}"

[database.archive]
url = "{url}"
"""
"""最小可用配置（字段口径同 bms `tests/core/test_config.py` 的 `_VALID_BASE`）。"""

_SQLITE_URL = "sqlite+aiosqlite:///./app.db"
_MYSQL_URL = "mysql+aiomysql://app@db/{database}"


def _clear_bms_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """清掉宿主可能残留的 `BMS_` 变量，避免污染分层断言。"""
    for name in ConcurrentStableList(os.environ):
        if name.startswith("BMS_"):
            monkeypatch.delenv(name, raising=False)


def _write(path: Path, text: str) -> None:
    """写临时配置（统一 LF）。"""
    with path.open("w", encoding="utf-8", newline="\n") as stream:
        stream.write(text)


def _prod_mapping() -> ConcurrentStableDict[str, object]:
    """构造合规的生产扁平映射（空密钥不参与判据）。

    Returns:
        ConcurrentStableDict[str, object]: 点分键 → 生效值。
    """
    return ConcurrentStableDict(
        {
            "app.env": "prod",
            "app.debug": False,
            "app.docs_enabled": False,
            "database.platform.url": _MYSQL_URL,
            "database.tenants.url": _MYSQL_URL,
            "database.archive.url": _MYSQL_URL,
            "tenant.dev_override_tenant": "",
        }
    )


def test_four_layer_precedence(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """层 4 > 层 3 > 层 2 > 层 1，逐层撤除后依次回落；`app.env` 与 `BMS_ENV` 一致。"""
    _clear_bms_env(monkeypatch)
    monkeypatch.setenv("BMS_ENV", "test")
    monkeypatch.setattr(base_config, "_config_dir", lambda: tmp_path)
    # 基座 dotenv 源在类定义时锚定 `backend/.env`；测试里把该键改指临时目录，才能验层 3 的普通键。
    monkeypatch.setitem(Settings.model_config, "env_file", tmp_path / ".env")
    monkeypatch.chdir(tmp_path)

    _write(tmp_path / "config.toml", _BASE.format(env="dev", worker=1, url=_SQLITE_URL))
    overlay = tmp_path / "config.test.toml"
    _write(overlay, "[app]\nworker_id = 2\n")
    dotenv = tmp_path / ".env"
    _write(dotenv, "BMS_APP__WORKER_ID=3\n")
    monkeypatch.setenv("BMS_APP__WORKER_ID", "4")

    settings = load_settings()
    assert settings.app.env == "test"
    assert settings.app.worker_id == 4

    monkeypatch.delenv("BMS_APP__WORKER_ID")
    assert load_settings().app.worker_id == 3

    dotenv.unlink()
    assert load_settings().app.worker_id == 2

    overlay.unlink()
    assert load_settings().app.worker_id == 1


def test_invalid_environment_is_rejected(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """`BMS_ENV` 非法值由基座拒启，不给静默回落。"""
    _clear_bms_env(monkeypatch)
    monkeypatch.setenv("BMS_ENV", "staging")
    monkeypatch.setattr(base_config, "_config_dir", lambda: tmp_path)
    _write(tmp_path / "config.toml", _BASE.format(env="dev", worker=1, url=_SQLITE_URL))
    with pytest.raises(ConfigError, match="BMS_ENV"):
        load_settings()


def test_flatten_settings_covers_nested_and_scalar() -> None:
    """扁平化覆盖嵌套映射与标量，且真实 `Settings` 的关键键在位。"""
    nested = flatten_settings(SimpleNamespace(model_dump=lambda: {"a": {"b": 7}, "c": "x"}))
    assert nested.get("a.b") == 7 and nested.get("c") == "x"

    real = flatten_settings(Settings())
    for key in ("app.env", "app.debug", "database.platform.url", "tenant.dev_override_tenant"):
        assert key in real
    validate_config_invariants(real, str(real.get("app.env", "")))


def test_guard_dev_fast_path_and_env_mismatch() -> None:
    """非生产只判环境一致性；环境不一致即报键路径，不回显值。"""
    dev = _prod_mapping()
    dev["app.env"] = "dev"
    dev["app.debug"] = True
    validate_config_invariants(dev, "dev")

    with pytest.raises(ConfigError, match="配置环境不一致"):
        validate_config_invariants(dev, "test")


def test_guard_prod_rejects_every_forbidden_value() -> None:
    """生产禁用项逐条被拒：debug / docs / 三个 SQLite 兜底 / 开发态租户覆盖。"""
    validate_config_invariants(_prod_mapping(), "prod")

    cases: ConcurrentStableList[tuple[str, object, str]] = ConcurrentStableList(
        [
            ("app.debug", True, "app.debug"),
            ("app.docs_enabled", True, "app.docs_enabled"),
            ("database.platform.url", _SQLITE_URL, "database.platform.url"),
            ("database.tenants.url", _SQLITE_URL, "database.tenants.url"),
            ("database.archive.url", _SQLITE_URL, "database.archive.url"),
            ("tenant.dev_override_tenant", "demo", "dev_override_tenant"),
        ]
    )
    for key, value, expected in cases:
        mapping = _prod_mapping()
        mapping[key] = value
        with pytest.raises(ConfigError, match=expected):
            validate_config_invariants(mapping, "prod")


def test_prod_effective_value_overrides_static_file(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """静态 `config.prod.toml` 合规、但环境变量把 debug 改回 true 时，最终生效值仍被拒。"""
    _clear_bms_env(monkeypatch)
    monkeypatch.setenv("BMS_ENV", "prod")
    monkeypatch.setenv("BMS_APP__DEBUG", "true")
    monkeypatch.setattr(base_config, "_config_dir", lambda: tmp_path)
    monkeypatch.chdir(tmp_path)
    _write(tmp_path / "config.toml", _BASE.format(env="prod", worker=0, url=_MYSQL_URL))
    _write(tmp_path / "config.prod.toml", "[app]\ndebug = false\ndocs_enabled = false\n")

    flat = flatten_settings(load_settings())
    assert flat.get("app.debug") is True
    with pytest.raises(ConfigError, match=r"app\.debug"):
        validate_config_invariants(flat, "prod")


def test_missing_keys_are_assemblable_but_fail_closed_on_use() -> None:
    """空密钥可装配、守卫不拒；签发时才抛 `ConfigError`，不静默放行。

    prod 的库 URL 只经环境变量注入，故此处用构造的扁平映射验「守卫不判密钥」，
    不构造真实 prod `Settings()`（那会依赖宿主环境变量，在并行 worker 里不稳定）。
    """
    settings = Settings()
    validate_config_invariants(flatten_settings(settings), settings.app.env)
    assert not settings.security.keys and not settings.service_token.keys

    prod = _prod_mapping()
    prod["security.active_kid"] = ""
    prod["service_token.keys"] = ""
    validate_config_invariants(prod, "prod")

    codec = JwtTokenCodecFactory(settings).create()
    with pytest.raises(ConfigError, match="签名密钥"):
        codec.encode(ConcurrentStableDict({"sub": "smoke"}))


def test_startup_wiring_calls_guard_first(monkeypatch: pytest.MonkeyPatch) -> None:
    """`configure_service()` 首行把扁平映射与生效环境交给守卫（抛错即中断后续装配）。"""
    calls: ConcurrentStableList[tuple[ConcurrentStableDict[str, object], str]] = ConcurrentStableList()
    settings = Settings()

    def _spy(effective: ConcurrentStableDict[str, object], env: str) -> None:
        calls.add((effective, env))
        raise ConfigError("sentinel")

    monkeypatch.setattr("bms_msg.main.validate_config_invariants", _spy)
    with pytest.raises(ConfigError, match="sentinel"):
        ApplicationFactory().configure_service(FastAPI(), settings)

    assert len(calls) == 1
    assert "app.env" in calls[0][0]
    assert calls[0][1] == (os.environ.get("BMS_ENV") or settings.app.env)
