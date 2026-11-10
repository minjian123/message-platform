"""msg 配置守卫（任务 07-2）：启动期与离线自检共用的纯函数判据。

判据只覆盖「配置分层结果」与「生产禁用项」，**不判密钥是否存在**——缺密钥沿用基座
「可装配、使用时失效关闭」口径（签发 / 校验时才抛 `ConfigError`），避免启动时因凭据未注入而拒启。

三个入口共用本模块：

- 启动期：`ApplicationFactory.configure_service()` 首行调用；
- 离线：`ops.check_credentials` 的 C5 用 `load_settings()` 装配后调用同一函数；
- 用例：`tests/deploy/test_config_layers.py` 直接喂构造的扁平映射做正反断言。

违规文案只给键路径与期望，不回显任何值。
"""

from __future__ import annotations

from typing import Any, cast

from bms_core.core.concurrent import (
    ConcurrentStableDict,
    ConcurrentStableFrozenList,
    ConcurrentStableList,
)
from bms_core.core.exceptions import ConfigError

_ENV_PROD = "prod"
_SQLITE_PREFIX = "sqlite"

_PROD_BOOL_FORBIDDEN: ConcurrentStableFrozenList[str] = ConcurrentStableFrozenList(["app.debug", "app.docs_enabled"])
"""生产环境必须为 false 的布尔键。"""

_PROD_DB_URL_KEYS: ConcurrentStableFrozenList[str] = ConcurrentStableFrozenList(
    ["database.platform.url", "database.tenants.url", "database.archive.url"]
)
"""生产环境不得回落 SQLite 的三个库目标。"""


def _flatten(value: object, prefix: str, out: ConcurrentStableDict[str, object]) -> None:
    """把嵌套映射摊平成点分键（列表不再下钻，标量直接落键）。

    Args:
        value: 待摊平的值。
        prefix: 当前键前缀。
        out: 结果映射（原地写入）。
    """
    if isinstance(value, dict):
        for key, child in cast("dict[str, object]", value).items():
            path = f"{prefix}.{key}" if prefix else str(key)
            _flatten(child, path, out)
        return
    out[prefix] = value


def flatten_settings(settings: Any) -> ConcurrentStableDict[str, object]:
    """把 `Settings` 摊平成点分键映射（供纯函数消费）。

    Args:
        settings: 基座 `Settings` 实例（只需支持 `model_dump()`）。

    Returns:
        ConcurrentStableDict[str, object]: 点分键 → 生效值。
    """
    out: ConcurrentStableDict[str, object] = ConcurrentStableDict()
    _flatten(settings.model_dump(), "", out)
    return out


def validate_config_invariants(effective: ConcurrentStableDict[str, object], env: str) -> None:
    """校验配置分层结果与生产禁用项。

    Args:
        effective: 点分键 → 生效值（由 `flatten_settings` 或用例构造）。
        env: 生效环境标识（调用方取 `BMS_ENV`，未设置时回落 `app.env`）。

    Raises:
        ConfigError: 环境不一致，或生产环境出现 debug / docs / SQLite 兜底 / 开发态租户覆盖。
    """
    actual = str(effective.get("app.env", ""))
    if actual != env:
        raise ConfigError(f"配置环境不一致：app.env={actual}、生效环境={env}")
    if env != _ENV_PROD:
        return

    violations: ConcurrentStableList[str] = ConcurrentStableList()
    for key in _PROD_BOOL_FORBIDDEN:
        if effective.get(key) is True:
            violations.add(f"{key} 在生产环境必须为 false")
    for key in _PROD_DB_URL_KEYS:
        if str(effective.get(key, "")).startswith(_SQLITE_PREFIX):
            violations.add(f"{key} 不得使用 SQLite 兜底")
    if str(effective.get("tenant.dev_override_tenant", "")).strip():
        violations.add("tenant.dev_override_tenant 在生产环境必须为空")
    if violations:
        raise ConfigError("生产配置校验失败：" + "；".join(violations))
