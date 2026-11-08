"""权限码种子（Kiwi 2456）：三表落库、幂等、冲突与清单声明。"""

from __future__ import annotations

import importlib
import tomllib
from collections.abc import AsyncIterator
from pathlib import Path
from typing import cast

import pytest
from bms_core.core.concurrent import ConcurrentStableDict, ConcurrentStableFrozenDict
from bms_core.core.exceptions import ParamError
from bms_core.db.session import DbSession
from bms_core.models.base import Base
from bms_core.models.platform.permission_meta import SysAction, SysBusiness, SysPermission
from bms_core.registry.products import load_product_manifest
from sqlalchemy import Table, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from bms_msg.seeds.permissions import (
    ACTION_CODES,
    BUSINESS_CODE,
    BUSINESS_FORM_KEY,
    BUSINESS_NAME,
    PERMISSION_ACTIONS,
    expected_rows,
    seed_permissions,
)

pytestmark = pytest.mark.kiwi_id(2456)

_MANIFEST = Path(__file__).resolve().parents[5] / "deploy" / "registry" / "msg.toml"
"""产品自持声明（资源类型 / 种子唯一事实源）。"""

_TABLES: list[Table] = [
    cast("Table", SysBusiness.__table__),
    cast("Table", SysAction.__table__),
    cast("Table", SysPermission.__table__),
]
"""种子涉及的三张平台服务库表。"""


@pytest.fixture
async def platform_session(tmp_path: Path) -> AsyncIterator[AsyncSession]:
    """建三张平台表并返回空会话（每用例独立文件库）。"""
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'permissions.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(lambda conn: Base.metadata.create_all(conn, tables=_TABLES))
    factory = async_sessionmaker(engine, expire_on_commit=False)
    async with factory() as session:
        try:
            yield session
        finally:
            await engine.dispose()


class _NoCommitSession:
    """记录型会话包装：转发真实会话，单独计数 commit 调用。"""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session
        self.commits = 0

    def __getattr__(self, name: str) -> object:
        return getattr(self._session, name)

    async def commit(self) -> None:
        self.commits += 1
        await self._session.commit()


def test_action_definitions() -> None:
    """动作码与权限码动作清单闭合。"""
    assert [spec.code for spec in ACTION_CODES] == ["query", "manage", "publish", "consume", "rpc"]
    assert list(PERMISSION_ACTIONS) == ["query", "manage", "publish", "consume", "rpc"]


def test_expected_rows_is_pure() -> None:
    """`expected_rows()` 只列业务码 `msg` 一条，两次调用一致。"""
    first = expected_rows()
    second = expected_rows()
    assert list(first) == list(second)
    assert len(first) == 1
    row = first[0]
    assert row.table == "sys_business"
    assert row.key == ConcurrentStableFrozenDict({"code": BUSINESS_CODE})
    assert row.values == ConcurrentStableFrozenDict(
        {"name": BUSINESS_NAME, "form_key": BUSINESS_FORM_KEY, "status": "enabled"}
    )


async def test_seed_writes_three_tables(platform_session: AsyncSession) -> None:
    """空库首次播种：业务码 + 五个动作 + 五个权限码。"""
    await seed_permissions(platform_session, ConcurrentStableDict())
    await platform_session.commit()

    business = (await platform_session.execute(select(SysBusiness))).scalar_one()
    assert (business.code, business.name, business.form_key, business.status) == (
        BUSINESS_CODE,
        BUSINESS_NAME,
        BUSINESS_FORM_KEY,
        "enabled",
    )
    actions = (await platform_session.execute(select(SysAction))).scalars().all()
    assert sorted(action.code for action in actions) == ["consume", "manage", "publish", "query", "rpc"]
    permissions = (await platform_session.execute(select(SysPermission))).scalars().all()
    assert len(permissions) == 5
    assert {int(item.business_id) for item in permissions} == {int(business.id)}
    assert {int(item.action_id) for item in permissions} == {int(action.id) for action in actions}


async def test_seed_is_idempotent(platform_session: AsyncSession) -> None:
    """重复执行：三表行数不变。"""
    await seed_permissions(platform_session, ConcurrentStableDict())
    await platform_session.commit()
    first = await _counts(platform_session)
    await seed_permissions(platform_session, ConcurrentStableDict())
    await platform_session.commit()
    assert await _counts(platform_session) == first == (1, 5, 5)


async def test_existing_action_name_is_kept(platform_session: AsyncSession) -> None:
    """已存在的全局动作码不覆盖名称（平台维护）。"""
    platform_session.add(SysAction(code="query", name="平台查询", form_key="action", status="enabled"))
    await platform_session.commit()
    await seed_permissions(platform_session, ConcurrentStableDict())
    await platform_session.commit()
    action = (
        await platform_session.execute(
            select(SysAction).where(SysAction.code == "query", SysAction.deleted_at.is_(None))
        )
    ).scalar_one()
    assert action.name == "平台查询"
    assert await _counts(platform_session) == (1, 5, 5)


async def test_conflicting_business_rejected(platform_session: AsyncSession) -> None:
    """业务码已存在且名称不同 → `ParamError`。"""
    platform_session.add(SysBusiness(code=BUSINESS_CODE, name="被改过的名字", form_key=BUSINESS_FORM_KEY))
    await platform_session.commit()
    with pytest.raises(ParamError, match=r"业务码 msg 已存在且与声明不一致"):
        await seed_permissions(platform_session, ConcurrentStableDict())


async def test_seed_does_not_commit(platform_session: AsyncSession) -> None:
    """入口只 flush、不自行 commit（XA 分支由编排器统一提交）。"""
    recorder = _NoCommitSession(platform_session)
    await seed_permissions(cast("DbSession", recorder), ConcurrentStableDict())
    assert recorder.commits == 0
    assert await _counts(platform_session) == (1, 5, 5)


def test_manifest_declares_permissions_seed_and_resource_type() -> None:
    """`msg.toml` 声明权限码种子与 `msg.topic` 资源类型。"""
    payload = tomllib.loads(_MANIFEST.read_text(encoding="utf-8"))
    seeds = {str(item["name"]): item for item in payload["seeds"]}
    assert seeds["msg-permissions"]["module"] == "bms_msg.seeds.permissions"
    assert seeds["msg-permissions"]["entry"] == "seed_permissions"
    assert seeds["msg-permissions"]["tables"] == {
        "sys_business": "code",
        "sys_action": "code",
        "sys_permission": "business_id,action_id",
    }
    types = {str(item["key"]): item for item in payload["resource_types"]}
    assert types["msg.topic"]["name_pattern"].startswith("^[a-zA-Z0-9._-]")
    manifest = load_product_manifest(_MANIFEST)
    assert [seed.name for seed in manifest.seeds] == ["msg-usage-forms", "msg-permissions"]
    assert [item.resource_type_key for item in manifest.resource_types] == ["msg.topic"]
    module = importlib.import_module("bms_msg.seeds.permissions")
    assert callable(module.__dict__.get("seed_permissions"))


async def _counts(session: AsyncSession) -> tuple[int, int, int]:
    """统计三表行数。"""
    business = len((await session.execute(select(SysBusiness))).all())
    action = len((await session.execute(select(SysAction))).all())
    permission = len((await session.execute(select(SysPermission))).all())
    return business, action, permission
