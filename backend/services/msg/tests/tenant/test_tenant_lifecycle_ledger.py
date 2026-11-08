"""05-5 生命周期账本（Kiwi 2453）：版本幂等、删除排期、兼容读与失败计数。"""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path

import pytest
from bms_core.core.exceptions import ConfigError

from bms_msg.tenant.ledger import (
    LEDGER_NAME,
    DbTenantLifecycleLedger,
    FileTenantLifecycleLedger,
)

pytestmark = pytest.mark.kiwi_id(2453)

_NOW = datetime(2026, 11, 8, 12, 0, 0)
"""测试基准时刻（UTC naive）。"""


async def test_upsert_applied_advances_only_newer_version(ledger: DbTenantLifecycleLedger) -> None:
    """版本只前进：同版本重复不重写，新版本推进。"""
    assert await ledger.applied_version(tenant_id=7001, action="created") == -1
    await ledger.mark_applied(tenant_id=7001, action="created", version=1, event_id="e1")
    await ledger.mark_applied(tenant_id=7001, action="created", version=1, event_id="e1-dup")
    assert await ledger.applied_version(tenant_id=7001, action="created") == 1
    await ledger.mark_applied(tenant_id=7001, action="created", version=2, event_id="e2")
    assert await ledger.applied_version(tenant_id=7001, action="created") == 2


async def test_mark_deleted_preserves_version_and_is_idempotent(
    ledger: DbTenantLifecycleLedger,
) -> None:
    """删除排期不提前推进 applied_version；同版本重试不延长保留期，新版本重设。"""
    first = _NOW + timedelta(days=30)
    await ledger.mark_deleted(tenant_id=7001, version=3, deleted_event_at=_NOW, purge_after=first)
    state = await ledger.get_deleted(tenant_id=7001)
    assert state is not None
    assert state.applied_version == -1
    assert state.purge_after == first
    assert state.purged_at is None

    await ledger.mark_applied(tenant_id=7001, action="deleted", version=3, event_id="e3")
    state = await ledger.get_deleted(tenant_id=7001)
    assert state is not None and state.applied_version == 3

    later = _NOW + timedelta(days=60)
    await ledger.mark_deleted(tenant_id=7001, version=3, deleted_event_at=_NOW, purge_after=later)
    state = await ledger.get_deleted(tenant_id=7001)
    assert state is not None and state.purge_after == first, "同版本重试不得延长保留期"

    await ledger.mark_deleted(tenant_id=7001, version=4, deleted_event_at=_NOW + timedelta(days=1), purge_after=later)
    state = await ledger.get_deleted(tenant_id=7001)
    assert state is not None and state.purge_after == later


async def test_get_deleted_missing_and_set_purge_after(ledger: DbTenantLifecycleLedger) -> None:
    """无删除行返回 None；set_purge_after 对已存在行生效、对缺失行无副作用。"""
    assert await ledger.get_deleted(tenant_id=9999) is None
    await ledger.set_purge_after(tenant_id=9999, at=_NOW)
    await ledger.mark_deleted(tenant_id=7001, version=1, deleted_event_at=_NOW, purge_after=_NOW + timedelta(days=1))
    await ledger.set_purge_after(tenant_id=7001, at=_NOW + timedelta(days=2))
    state = await ledger.get_deleted(tenant_id=7001)
    assert state is not None and state.purge_after == _NOW + timedelta(days=2)
    assert await ledger.delete_topics(tenant_id=7001, topic_ids=()) == 0
    await ledger.mark_purged(tenant_id=9999, at=_NOW)
    await ledger.record_purge_failure(tenant_id=9999, error="ignored")


async def test_cancel_purge_and_activate(ledger: DbTenantLifecycleLedger) -> None:
    """启用取消待清理，保留删除事件时刻供审计。"""
    await ledger.mark_deleted(tenant_id=7001, version=1, deleted_event_at=_NOW, purge_after=_NOW + timedelta(days=30))
    assert await ledger.cancel_purge(tenant_id=7001, event_id="activate-1") is True
    state = await ledger.get_deleted(tenant_id=7001)
    assert state is not None
    assert state.purge_after is None
    assert state.deleted_event_at == _NOW
    assert await ledger.cancel_purge(tenant_id=8002, event_id="missing") is False


async def test_list_due_purges_only_returns_due_rows(ledger: DbTenantLifecycleLedger) -> None:
    """只返回到期且未完成的删除行。"""
    await ledger.mark_deleted(tenant_id=7001, version=1, deleted_event_at=_NOW, purge_after=_NOW - timedelta(seconds=1))
    await ledger.mark_deleted(tenant_id=8002, version=1, deleted_event_at=_NOW, purge_after=_NOW + timedelta(days=1))
    due = await ledger.list_due_purges(now=_NOW)
    assert [item.tenant_id for item in due] == [7001]


async def test_failure_count_and_mark_purged(ledger: DbTenantLifecycleLedger) -> None:
    """清扫失败累计并记录原因；成功后清零。"""
    await ledger.mark_deleted(tenant_id=7001, version=1, deleted_event_at=_NOW, purge_after=_NOW)
    await ledger.record_purge_failure(tenant_id=7001, error="disk full")
    await ledger.record_purge_failure(tenant_id=7001, error="retry")
    state = await ledger.get_deleted(tenant_id=7001)
    assert state is not None
    assert state.failure_count == 2
    assert state.last_error == "retry"

    await ledger.mark_purged(tenant_id=7001, at=_NOW)
    state = await ledger.get_deleted(tenant_id=7001)
    assert state is not None
    assert state.purged_at == _NOW
    assert state.failure_count == 0
    assert state.last_error is None


async def test_file_ledger_reads_legacy_and_rejects_purge(tmp_path: Path) -> None:
    """05-1 文件账本可兼容读；删除排期 / 清扫方法必须拒绝（生产用 DB）。"""
    tenant_dir = tmp_path / "7001"
    tenant_dir.mkdir()
    (tenant_dir / LEDGER_NAME).write_text(json.dumps({"created": 2}), encoding="utf-8")
    ledger = FileTenantLifecycleLedger(root=tmp_path)
    assert await ledger.applied_version(tenant_id=7001, action="created") == 2

    await ledger.mark_applied(tenant_id=7001, action="suspended", version=1, event_id=None)
    assert await ledger.applied_version(tenant_id=7001, action="suspended") == 1

    with pytest.raises(ConfigError):
        await ledger.mark_deleted(tenant_id=7001, version=1, deleted_event_at=_NOW, purge_after=_NOW)
    with pytest.raises(ConfigError):
        await ledger.cancel_purge(tenant_id=7001, event_id=None)
    with pytest.raises(ConfigError):
        await ledger.mark_purged(tenant_id=7001, at=_NOW)
    with pytest.raises(ConfigError):
        await ledger.record_purge_failure(tenant_id=7001, error="x")
    with pytest.raises(ConfigError):
        await ledger.set_purge_after(tenant_id=7001, at=_NOW)
    assert await ledger.get_deleted(tenant_id=7001) is None
    assert list(await ledger.list_due_purges(now=_NOW)) == []
    assert list(await ledger.list_topics(tenant_id=7001)) == []
    assert list(await ledger.list_replay_tasks(tenant_id=7001)) == []
    assert await ledger.cancel_active_replay_tasks(tenant_id=7001, at=_NOW) == 0
    assert await ledger.delete_topics(tenant_id=7001, topic_ids=(1,)) == 0
    assert await ledger.count_topics(tenant_id=7001) == 0


async def test_file_ledger_corrupt_and_non_dict(tmp_path: Path) -> None:
    """文件账本损坏 / 非对象 / 布尔版本均按 -1。"""
    tenant_dir = tmp_path / "7001"
    tenant_dir.mkdir()
    path = tenant_dir / LEDGER_NAME
    path.write_text("{not-json", encoding="utf-8")
    ledger = FileTenantLifecycleLedger(root=tmp_path)
    assert await ledger.applied_version(tenant_id=7001, action="created") == -1
    path.write_text("[1, 2]", encoding="utf-8")
    assert await ledger.applied_version(tenant_id=7001, action="created") == -1
    path.write_text(json.dumps({"created": True, "suspended": 3}), encoding="utf-8")
    assert await ledger.applied_version(tenant_id=7001, action="created") == -1
    assert await ledger.applied_version(tenant_id=7001, action="suspended") == 3
