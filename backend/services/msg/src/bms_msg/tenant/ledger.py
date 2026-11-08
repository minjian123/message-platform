"""租户生命周期幂等账本：DB 实现（生产）+ 文件实现（05-1 兼容 / 测试替身）。

- **生产写路径是 DB**：`msg_tenant_lifecycle` 按 `(tenant_id, action)` 保存已处理版本与删除进度；
  账本独立于租户数据目录，删除资源后仍保留幂等证据。
- **文件只兼容读**：05-1 的 `data/{tenant_id}/.lifecycle.json` 仍可读出旧版本（DB 无行时回退），
  但不再写回；首次新事件后自然迁移到 DB。
- **不是租户状态表**：只服务生命周期消费与到期清扫，不参与数据面租户状态判定。
"""

from __future__ import annotations

import json
import os
from collections.abc import Callable, Iterable
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Protocol, cast

from bms_core.core.concurrent import ConcurrentStableDict, ConcurrentStableList
from bms_core.core.exceptions import ConfigError
from bms_core.db.session import DbSession

from bms_msg.broker.file_store import data_root
from bms_msg.core.config import DATA_ROOT_SUBDIR
from bms_msg.models.tenant_lifecycle import ACTION_DELETED, MsgTenantLifecycle
from bms_msg.repositories.tenant_lifecycle import (
    TenantLifecycleRepository,
    TenantPurgeDue,
    TenantReplayRef,
    TenantTopicRef,
)

__all__ = [
    "LEDGER_NAME",
    "DbTenantLifecycleLedger",
    "FileTenantLifecycleLedger",
    "SessionScope",
    "TenantDeletedState",
    "TenantLifecycleLedger",
]

LEDGER_NAME = ".lifecycle.json"
"""05-1 租户目录内的文件账本名（仅兼容读）。"""

SessionScope = Callable[[], AbstractAsyncContextManager[DbSession]]
"""一次短会话入口（生产为 `session_scope(platform)`；测试注入内存库）。"""


@dataclass(frozen=True)
class TenantDeletedState:
    """`deleted` 行对外快照（避免会话关闭后读 ORM 属性）。"""

    applied_version: int
    """已处理版本。"""

    deleted_event_at: datetime | None
    """删除事件生效时刻（UTC naive）。"""

    purge_after: datetime | None
    """存量清理排期（UTC naive；两步口径下新行恒 None）。"""

    purged_at: datetime | None
    """物理清理完成时刻（UTC naive；收到 `tenant.purged` 后写入）。"""

    failure_count: int
    """连续失败次数。"""

    last_error: str | None
    """最近失败原因。"""


class TenantLifecycleLedger(Protocol):
    """生命周期账本端口（DB 与文件两实现；文件实现只支持版本读写）。"""

    async def applied_version(self, *, tenant_id: int, action: str) -> int:
        """读已处理版本；无记录返回 -1。"""
        ...

    async def mark_applied(self, *, tenant_id: int, action: str, version: int, event_id: str | None) -> None:
        """推进已处理版本。"""
        ...

    async def mark_deleted(
        self,
        *,
        tenant_id: int,
        version: int,
        deleted_event_at: datetime,
        purge_after: datetime | None = None,
    ) -> None:
        """写删除事件时刻（不推进 applied_version；`purge_after` 仅存量兼容）。"""
        ...

    async def get_deleted(self, *, tenant_id: int) -> TenantDeletedState | None:
        """取删除行快照。"""
        ...

    async def cancel_purge(self, *, tenant_id: int, event_id: str | None) -> bool:
        """取消待清理。"""
        ...

    async def set_purge_after(self, *, tenant_id: int, at: datetime) -> None:
        """推进下一次清理时刻。"""
        ...

    async def mark_purged(self, *, tenant_id: int, at: datetime) -> None:
        """标记清理完成。"""
        ...

    async def record_purge_failure(self, *, tenant_id: int, error: str) -> None:
        """记录清理失败。"""
        ...

    async def list_due_purges(self, *, now: datetime, limit: int = 100) -> ConcurrentStableList[TenantPurgeDue]:
        """取到期待清理租户。"""
        ...

    async def list_topics(self, *, tenant_id: int) -> ConcurrentStableList[TenantTopicRef]:
        """列主题清单。"""
        ...

    async def list_replay_tasks(self, *, tenant_id: int) -> ConcurrentStableList[TenantReplayRef]:
        """列重放任务。"""
        ...

    async def cancel_active_replay_tasks(self, *, tenant_id: int, at: datetime) -> int:
        """取消非终态重放任务。"""
        ...

    async def delete_topics(self, *, tenant_id: int, topic_ids: Iterable[int]) -> int:
        """删除到期主题元数据。"""
        ...

    async def count_topics(self, *, tenant_id: int) -> int:
        """统计主题数。"""
        ...


class DbTenantLifecycleLedger:
    """DB 账本：每次方法开一个平台库短会话（后台消费无请求上下文）。"""

    def __init__(self, *, session_scope: SessionScope) -> None:
        """初始化。

        Args:
            session_scope: 平台库短会话入口。
        """
        self._session_scope = session_scope

    async def applied_version(self, *, tenant_id: int, action: str) -> int:
        """读已处理版本；无行返回 -1。

        Args:
            tenant_id: 租户主键。
            action: 动作名。

        Returns:
            int: 已处理版本。
        """
        async with self._session_scope() as session:
            version = await TenantLifecycleRepository(session).get_version(tenant_id=tenant_id, action=action)
        return -1 if version is None else int(version)

    async def mark_applied(self, *, tenant_id: int, action: str, version: int, event_id: str | None) -> None:
        """推进已处理版本（不覆盖删除排期）。

        Args:
            tenant_id: 租户主键。
            action: 动作名。
            version: 事件版本。
            event_id: 事件 ID。
        """
        async with self._session_scope() as session, session.begin():
            await TenantLifecycleRepository(session).upsert_applied(
                tenant_id=tenant_id, action=action, version=version, event_id=event_id
            )

    async def mark_deleted(
        self,
        *,
        tenant_id: int,
        version: int,
        deleted_event_at: datetime,
        purge_after: datetime | None = None,
    ) -> None:
        """写删除事件时刻（同版本重试不改变已记时刻）。

        Args:
            tenant_id: 租户主键。
            version: 事件版本。
            deleted_event_at: 删除事件生效时刻。
            purge_after: 存量排期（两步口径下生产路径传 None）。
        """
        async with self._session_scope() as session, session.begin():
            await TenantLifecycleRepository(session).mark_deleted(
                tenant_id=tenant_id,
                version=version,
                deleted_event_at=deleted_event_at,
                purge_after=purge_after,
            )

    async def get_deleted(self, *, tenant_id: int) -> TenantDeletedState | None:
        """取删除行快照。

        Args:
            tenant_id: 租户主键。

        Returns:
            TenantDeletedState | None: 删除行快照；无行返回 None。
        """
        async with self._session_scope() as session:
            row = await TenantLifecycleRepository(session).get_by_action(tenant_id=tenant_id, action=ACTION_DELETED)
            if row is None:
                return None
            return _deleted_state(row)

    async def cancel_purge(self, *, tenant_id: int, event_id: str | None) -> bool:
        """取消待清理（保留删除事件时刻供审计）。

        Args:
            tenant_id: 租户主键。
            event_id: 启用事件 ID。

        Returns:
            bool: 是否确有行被取消。
        """
        async with self._session_scope() as session, session.begin():
            return await TenantLifecycleRepository(session).cancel_purge(tenant_id=tenant_id, event_id=event_id)

    async def set_purge_after(self, *, tenant_id: int, at: datetime) -> None:
        """推进下一次清理时刻。

        Args:
            tenant_id: 租户主键。
            at: 下一次到期点。
        """
        async with self._session_scope() as session, session.begin():
            await TenantLifecycleRepository(session).set_purge_after(tenant_id=tenant_id, at=at)

    async def mark_purged(self, *, tenant_id: int, at: datetime) -> None:
        """标记清理完成。

        Args:
            tenant_id: 租户主键。
            at: 完成时刻。
        """
        async with self._session_scope() as session, session.begin():
            await TenantLifecycleRepository(session).mark_purged(tenant_id=tenant_id, at=at)

    async def record_purge_failure(self, *, tenant_id: int, error: str) -> None:
        """记录清理失败。

        Args:
            tenant_id: 租户主键。
            error: 失败原因。
        """
        async with self._session_scope() as session, session.begin():
            await TenantLifecycleRepository(session).record_purge_failure(tenant_id=tenant_id, error=error)

    async def list_due_purges(self, *, now: datetime, limit: int = 100) -> ConcurrentStableList[TenantPurgeDue]:
        """取到期待清理租户。

        Args:
            now: 当前时刻。
            limit: 单轮上限。

        Returns:
            ConcurrentStableList[TenantPurgeDue]: 到期待清理行。
        """
        async with self._session_scope() as session:
            return await TenantLifecycleRepository(session).list_due_purges(now=now, limit=limit)

    async def list_topics(self, *, tenant_id: int) -> ConcurrentStableList[TenantTopicRef]:
        """列主题清单。

        Args:
            tenant_id: 租户主键。

        Returns:
            ConcurrentStableList[TenantTopicRef]: 主题清单。
        """
        async with self._session_scope() as session:
            return await TenantLifecycleRepository(session).list_topics(tenant_id=tenant_id)

    async def list_replay_tasks(self, *, tenant_id: int) -> ConcurrentStableList[TenantReplayRef]:
        """列重放任务。

        Args:
            tenant_id: 租户主键。

        Returns:
            ConcurrentStableList[TenantReplayRef]: 任务清单。
        """
        async with self._session_scope() as session:
            return await TenantLifecycleRepository(session).list_replay_tasks(tenant_id=tenant_id)

    async def cancel_active_replay_tasks(self, *, tenant_id: int, at: datetime) -> int:
        """取消非终态重放任务。

        Args:
            tenant_id: 租户主键。
            at: 终态时刻。

        Returns:
            int: 受影响行数。
        """
        async with self._session_scope() as session, session.begin():
            return await TenantLifecycleRepository(session).cancel_active_replay_tasks(tenant_id=tenant_id, at=at)

    async def delete_topics(self, *, tenant_id: int, topic_ids: Iterable[int]) -> int:
        """删除到期主题元数据。

        Args:
            tenant_id: 租户主键。
            topic_ids: 到期主题主键。

        Returns:
            int: 删除行数。
        """
        async with self._session_scope() as session, session.begin():
            return await TenantLifecycleRepository(session).delete_topics(tenant_id=tenant_id, topic_ids=topic_ids)

    async def count_topics(self, *, tenant_id: int) -> int:
        """统计主题数。

        Args:
            tenant_id: 租户主键。

        Returns:
            int: 主题数。
        """
        async with self._session_scope() as session:
            return await TenantLifecycleRepository(session).count_topics(tenant_id=tenant_id)


class FileTenantLifecycleLedger:
    """05-1 文件账本（仅兼容读 / 测试替身；不支持删除排期与清扫）。"""

    def __init__(self, *, root: Path | None = None) -> None:
        """初始化。

        Args:
            root: 数据根（缺省 `data_root()`）。
        """
        self._root = Path(root) if root is not None else data_root() / DATA_ROOT_SUBDIR

    async def applied_version(self, *, tenant_id: int, action: str) -> int:
        """读文件账本中的动作版本。

        Args:
            tenant_id: 租户主键。
            action: 动作名。

        Returns:
            int: 已处理版本；缺失 / 损坏 / 非整数返回 -1。
        """
        value = self._read(tenant_id).get(action)
        if isinstance(value, int) and not isinstance(value, bool):
            return value
        return -1

    async def mark_applied(self, *, tenant_id: int, action: str, version: int, event_id: str | None) -> None:
        """写文件账本（仅测试 / 兼容路径）。

        Args:
            tenant_id: 租户主键。
            action: 动作名。
            version: 事件版本。
            event_id: 事件 ID（文件账本不保存）。
        """
        del event_id
        directory = self._tenant_dir(tenant_id)
        directory.mkdir(parents=True, exist_ok=True)
        payload = self._read(tenant_id)
        payload[action] = int(version)
        path = directory / LEDGER_NAME
        temporary = path.with_name(f"{LEDGER_NAME}.tmp")
        temporary.write_text(json.dumps(dict(payload), ensure_ascii=False, sort_keys=True), encoding="utf-8")
        os.replace(temporary, path)

    async def mark_deleted(
        self,
        *,
        tenant_id: int,
        version: int,
        deleted_event_at: datetime,
        purge_after: datetime | None = None,
    ) -> None:
        """文件账本不支持删除事件时刻。

        Raises:
            ConfigError: 恒抛（生产必须装配 DB 账本）。
        """
        del tenant_id, version, deleted_event_at, purge_after
        raise ConfigError("文件账本不支持删除事件时刻；生产须装配 DbTenantLifecycleLedger")

    async def get_deleted(self, *, tenant_id: int) -> TenantDeletedState | None:
        """文件账本没有删除排期。

        Args:
            tenant_id: 租户主键。

        Returns:
            None: 恒 None。
        """
        del tenant_id
        return None

    async def cancel_purge(self, *, tenant_id: int, event_id: str | None) -> bool:
        """文件账本不支持取消清理。

        Raises:
            ConfigError: 恒抛。
        """
        del tenant_id, event_id
        raise ConfigError("文件账本不支持取消清理；生产须装配 DbTenantLifecycleLedger")

    async def set_purge_after(self, *, tenant_id: int, at: datetime) -> None:
        """文件账本不支持推进清理时刻。

        Raises:
            ConfigError: 恒抛。
        """
        del tenant_id, at
        raise ConfigError("文件账本不支持推进清理时刻；生产须装配 DbTenantLifecycleLedger")

    async def mark_purged(self, *, tenant_id: int, at: datetime) -> None:
        """文件账本不支持清理完成标记。

        Raises:
            ConfigError: 恒抛。
        """
        del tenant_id, at
        raise ConfigError("文件账本不支持清理完成标记；生产须装配 DbTenantLifecycleLedger")

    async def record_purge_failure(self, *, tenant_id: int, error: str) -> None:
        """文件账本不支持清理失败记录。

        Raises:
            ConfigError: 恒抛。
        """
        del tenant_id, error
        raise ConfigError("文件账本不支持清理失败记录；生产须装配 DbTenantLifecycleLedger")

    async def list_due_purges(self, *, now: datetime, limit: int = 100) -> ConcurrentStableList[TenantPurgeDue]:
        """文件账本没有待清理队列。

        Args:
            now: 当前时刻。
            limit: 单轮上限。

        Returns:
            ConcurrentStableList[TenantPurgeDue]: 空清单。
        """
        del now, limit
        return ConcurrentStableList()

    async def list_topics(self, *, tenant_id: int) -> ConcurrentStableList[TenantTopicRef]:
        """文件账本不查主题表。

        Args:
            tenant_id: 租户主键。

        Returns:
            ConcurrentStableList[TenantTopicRef]: 空清单。
        """
        del tenant_id
        return ConcurrentStableList()

    async def list_replay_tasks(self, *, tenant_id: int) -> ConcurrentStableList[TenantReplayRef]:
        """文件账本不查重放任务。

        Args:
            tenant_id: 租户主键。

        Returns:
            ConcurrentStableList[TenantReplayRef]: 空清单。
        """
        del tenant_id
        return ConcurrentStableList()

    async def cancel_active_replay_tasks(self, *, tenant_id: int, at: datetime) -> int:
        """文件账本不处理重放任务。

        Args:
            tenant_id: 租户主键。
            at: 终态时刻。

        Returns:
            int: 0。
        """
        del tenant_id, at
        return 0

    async def delete_topics(self, *, tenant_id: int, topic_ids: Iterable[int]) -> int:
        """文件账本不删元数据。

        Args:
            tenant_id: 租户主键。
            topic_ids: 到期主题主键。

        Returns:
            int: 0。
        """
        del tenant_id, topic_ids
        return 0

    async def count_topics(self, *, tenant_id: int) -> int:
        """文件账本不统计主题。

        Args:
            tenant_id: 租户主键。

        Returns:
            int: 0。
        """
        del tenant_id
        return 0

    def _tenant_dir(self, tenant_id: int) -> Path:
        """租户数据目录。

        Args:
            tenant_id: 租户主键。

        Returns:
            Path: `{root}/{tenant_id}`。
        """
        return self._root / str(tenant_id)

    def _read(self, tenant_id: int) -> ConcurrentStableDict[str, int]:
        """读文件账本（缺失 / 损坏 / 非对象按空处理）。

        Args:
            tenant_id: 租户主键。

        Returns:
            ConcurrentStableDict[str, int]: 动作 → 版本。
        """
        path = self._tenant_dir(tenant_id) / LEDGER_NAME
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return ConcurrentStableDict()
        except OSError, ValueError:
            return ConcurrentStableDict()
        if not isinstance(raw, dict):
            return ConcurrentStableDict()
        payload: ConcurrentStableDict[str, int] = ConcurrentStableDict()
        for key, value in cast("dict[str, object]", raw).items():
            if isinstance(value, int) and not isinstance(value, bool):
                payload[key] = value
        return payload


def _deleted_state(row: MsgTenantLifecycle) -> TenantDeletedState:
    """把 ORM 行转成删除状态快照。

    Args:
        row: `deleted` 账本行。

    Returns:
        TenantDeletedState: 快照。
    """
    return TenantDeletedState(
        applied_version=int(row.applied_version),
        deleted_event_at=row.deleted_event_at,
        purge_after=row.purge_after,
        purged_at=row.purged_at,
        failure_count=int(row.failure_count),
        last_error=row.last_error,
    )
