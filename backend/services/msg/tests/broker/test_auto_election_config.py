# pyright: reportPrivateUsage=false
"""自动选举配置与校验（msg 任务 02-9；Kiwi 2488）。

覆盖详细设计 §11.1 / §11.2：缺省值、环境变量覆盖、非法回落，以及五条拒启条件；
其中「节拍 × 阈值 + 宽限 ≤ 视图新鲜窗口」只在开关开启时校验（关闭时不应拦住单实例）。
"""

from __future__ import annotations

from datetime import UTC

import pytest

from bms_msg.broker.auto_election import AutoElectionConfig, _env_bool, _env_int, _monotonic_ms, _now_utc

pytestmark = pytest.mark.kiwi_id(2488)


def test_defaults_are_off_and_safe(monkeypatch: pytest.MonkeyPatch) -> None:
    """无环境变量时：开关关闭、节拍 1s、阈值 3 次、冷却 60s、并发 4。"""
    for name in (
        "MSG_AUTO_ELECTION_ENABLED",
        "MSG_AUTO_ELECTION_DETECT_INTERVAL_MS",
        "MSG_AUTO_ELECTION_PROBE_TIMEOUT_MS",
        "MSG_AUTO_ELECTION_FAILURE_THRESHOLD",
        "MSG_AUTO_ELECTION_MAX_ATTEMPTS",
        "MSG_AUTO_ELECTION_BACKOFF_BASE_MS",
        "MSG_AUTO_ELECTION_BACKOFF_CAP_MS",
        "MSG_AUTO_ELECTION_COOLDOWN_MS",
        "MSG_AUTO_ELECTION_CONCURRENCY",
        "MSG_AUTO_ELECTION_STAGGER_MS",
    ):
        monkeypatch.delenv(name, raising=False)
    config = AutoElectionConfig.from_env()
    assert config.enabled is False
    assert config.detect_interval_ms == 1_000
    assert config.probe_timeout_ms == 800
    assert config.failure_threshold == 3
    assert config.max_attempts == 5
    assert config.backoff_base_ms == 2_000
    assert config.backoff_cap_ms == 60_000
    assert config.cooldown_ms == 60_000
    assert config.concurrency == 4
    assert config.stagger_ms == 200


def test_from_env_overrides(monkeypatch: pytest.MonkeyPatch) -> None:
    """环境变量逐项覆盖（含布尔真值写法）。"""
    monkeypatch.setenv("MSG_AUTO_ELECTION_ENABLED", "yes")
    monkeypatch.setenv("MSG_AUTO_ELECTION_DETECT_INTERVAL_MS", "500")
    monkeypatch.setenv("MSG_AUTO_ELECTION_PROBE_TIMEOUT_MS", "300")
    monkeypatch.setenv("MSG_AUTO_ELECTION_FAILURE_THRESHOLD", "2")
    monkeypatch.setenv("MSG_AUTO_ELECTION_MAX_ATTEMPTS", "3")
    monkeypatch.setenv("MSG_AUTO_ELECTION_BACKOFF_BASE_MS", "100")
    monkeypatch.setenv("MSG_AUTO_ELECTION_BACKOFF_CAP_MS", "900")
    monkeypatch.setenv("MSG_AUTO_ELECTION_COOLDOWN_MS", "5_000")
    monkeypatch.setenv("MSG_AUTO_ELECTION_CONCURRENCY", "2")
    monkeypatch.setenv("MSG_AUTO_ELECTION_STAGGER_MS", "50")
    config = AutoElectionConfig.from_env()
    assert config.enabled is True
    assert config.detect_interval_ms == 500
    assert config.probe_timeout_ms == 300
    assert config.failure_threshold == 2
    assert config.max_attempts == 3
    assert config.backoff_base_ms == 100
    assert config.backoff_cap_ms == 900
    assert config.cooldown_ms == 5_000
    assert config.concurrency == 2
    assert config.stagger_ms == 50
    # 整数字符串允许下划线（`int("5_000")` 合法），非法字母才回落缺省
    monkeypatch.setenv("MSG_AUTO_ELECTION_COOLDOWN_MS", "abc")
    assert AutoElectionConfig.from_env().cooldown_ms == 60_000


def test_env_helpers_tolerate_bad_values(monkeypatch: pytest.MonkeyPatch) -> None:
    """布尔 / 整数解析：空值取缺省、非法值回落、真值写法齐全。"""
    monkeypatch.delenv("SAMPLE_BOOL", raising=False)
    monkeypatch.delenv("SAMPLE_INT", raising=False)
    assert _env_bool("SAMPLE_BOOL", True) is True
    assert _env_bool("SAMPLE_BOOL", False) is False
    assert _env_int("SAMPLE_INT", 7) == 7
    for raw, expected in (("1", True), ("true", True), ("on", True), ("0", False), ("no", False)):
        monkeypatch.setenv("SAMPLE_BOOL", raw)
        assert _env_bool("SAMPLE_BOOL", False) is expected
    monkeypatch.setenv("SAMPLE_INT", "abc")
    assert _env_int("SAMPLE_INT", 7) == 7
    monkeypatch.setenv("SAMPLE_INT", "12")
    assert _env_int("SAMPLE_INT", 7) == 12


def test_validate_accepts_defaults_when_enabled() -> None:
    """缺省组合满足「节拍 × 阈值 + 宽限 ≤ 新鲜窗口」（1s×3 + 5s ≤ 10s）。"""
    AutoElectionConfig(enabled=True).validate(switch_grace_ms=5_000, freshness_ms=10_000)


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"detect_interval_ms": 0}, "detect_interval_ms"),
        ({"probe_timeout_ms": 0}, "probe_timeout_ms"),
        ({"failure_threshold": 0}, "failure_threshold"),
        ({"max_attempts": 0}, "max_attempts"),
        ({"backoff_base_ms": 0}, "backoff_base_ms"),
        ({"backoff_cap_ms": 0}, "backoff_cap_ms"),
        ({"cooldown_ms": 0}, "cooldown_ms"),
        ({"concurrency": 0}, "concurrency"),
        ({"stagger_ms": 0}, "stagger_ms"),
    ],
)
def test_validate_rejects_non_positive(overrides: dict[str, int], message: str) -> None:
    """任一数值键必须为正（逐项拒启）。

    Args:
        overrides: 被破坏的字段。
        message: 期望出现在报错里的字段名。
    """
    config = AutoElectionConfig(**overrides)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match=message):
        config.validate(switch_grace_ms=5_000, freshness_ms=10_000)


def test_validate_rejects_probe_not_smaller_than_interval() -> None:
    """探活超时必须小于节拍（否则失败计数跨轮抖动）。"""
    config = AutoElectionConfig(enabled=True, detect_interval_ms=800, probe_timeout_ms=800)
    with pytest.raises(ValueError, match="probe_timeout_ms"):
        config.validate(switch_grace_ms=5_000, freshness_ms=10_000)


def test_validate_rejects_backoff_base_above_cap() -> None:
    """退避基数不得大于上限。"""
    config = AutoElectionConfig(enabled=True, backoff_base_ms=2_000, backoff_cap_ms=1_000)
    with pytest.raises(ValueError, match="backoff_base_ms"):
        config.validate(switch_grace_ms=5_000, freshness_ms=10_000)


def test_validate_rejects_cooldown_below_grace() -> None:
    """冷却不得小于切主宽限（否则等于没有冷却）。"""
    config = AutoElectionConfig(enabled=True, cooldown_ms=1_000)
    with pytest.raises(ValueError, match="cooldown_ms"):
        config.validate(switch_grace_ms=5_000, freshness_ms=10_000)


def test_validate_rejects_budget_over_freshness_window() -> None:
    """判死 + 宽限预算超出新鲜窗口：拒启（否则自动路径必然 isr_unverified）。"""
    config = AutoElectionConfig(enabled=True)
    with pytest.raises(ValueError, match="isr_unverified"):
        config.validate(switch_grace_ms=5_000, freshness_ms=7_000)


def test_validate_skips_budget_when_disabled() -> None:
    """开关关闭时不做预算校验（单实例 / 灰度期不应被拦住）。"""
    AutoElectionConfig(enabled=False).validate(switch_grace_ms=5_000, freshness_ms=1_000)


def test_default_clocks() -> None:
    """缺省时钟：单调毫秒递增、墙钟带 UTC 时区。"""
    first = _monotonic_ms()
    second = _monotonic_ms()
    assert second >= first
    assert _now_utc().tzinfo is UTC
