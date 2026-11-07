"""05-1 用量表单与字段种子（Kiwi 2452）。

覆盖：四列常量与 `allowlist` 同源；`expected_rows()` 纯函数；三表落库；
重复执行幂等；业务码 / 表单 / 字段异值抛 `ParamError`；入口不自行 commit（XA 语义）；
`msg.toml` 种子声明与入口可导入；端点列与 `sys_field` 同源。
"""

import importlib
import json
import tomllib
from collections.abc import AsyncIterator
from pathlib import Path
from typing import cast

import pytest
from bms_core.core.concurrent import ConcurrentStableDict, ConcurrentStableFrozenDict
from bms_core.core.exceptions import ParamError
from bms_core.db.session import DbSession
from bms_core.models.base import Base
from bms_core.models.platform.menu_form import SysField, SysForm
from bms_core.models.platform.permission_meta import SysBusiness
from bms_core.registry.products import load_product_manifest
from sqlalchemy import Table, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from bms_msg.api.admin.quota import tenant_usage
from bms_msg.seeds.forms import (
    BUSINESS_CODE,
    BUSINESS_FORM_KEY,
    BUSINESS_NAME,
    expected_rows,
    seed_usage_forms,
)
from bms_msg.usage.columns import USAGE_COLUMNS

pytestmark = pytest.mark.kiwi_id(2452)

_MANIFEST = Path(__file__).resolve().parents[4] / "deploy" / "registry" / "msg.toml"
"""产品自持声明（数据源 / 种子 / 错误码的唯一事实源）。"""

_TABLES: list[Table] = [
    cast("Table", SysBusiness.__table__),
    cast("Table", SysForm.__table__),
    cast("Table", SysField.__table__),
]
"""种子涉及的三张平台服务库表。"""


@pytest.fixture
async def platform_session(tmp_path: Path) -> AsyncIterator[AsyncSession]:
    """建三张平台表并返回空会话（每用例独立文件库）。

    Args:
        tmp_path: 临时目录。

    Yields:
        AsyncSession: 空库会话。
    """
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'platform.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(lambda conn: Base.metadata.create_all(conn, tables=_TABLES))
    factory = async_sessionmaker(engine, expire_on_commit=False)
    async with factory() as session:
        try:
            yield session
        finally:
            await engine.dispose()


class _NoCommitSession:
    """记录型会话包装：转发真实会话，单独计数 `commit` 调用（XA 语义断言用）。"""

    def __init__(self, session: AsyncSession) -> None:
        """初始化。

        Args:
            session: 真实会话。
        """
        self._session = session
        self.commits = 0

    def __getattr__(self, name: str) -> object:
        """把未定义属性转发给真实会话。

        Args:
            name: 属性名。

        Returns:
            object: 真实会话属性。
        """
        return getattr(self._session, name)

    async def commit(self) -> None:
        """计数并转发提交（种子若调用即失败）。"""
        self.commits += 1
        await self._session.commit()


def test_usage_columns_are_fixed() -> None:
    """四列键 / 标签 / 类型 / 排序固定，且与 `allowlist` 同名闭合。"""
    assert [column.key for column in USAGE_COLUMNS] == [
        "topic_count",
        "partition_count",
        "storage_bytes",
        "delivered_today",
    ]
    assert [column.label for column in USAGE_COLUMNS] == ["主题数", "分区数", "消息存储", "今日投递量"]
    assert [column.field_type for column in USAGE_COLUMNS] == ["number"] * 4
    assert [column.sort for column in USAGE_COLUMNS] == [1, 2, 3, 4]


def test_expected_rows_is_pure_function() -> None:
    """`expected_rows()` 返回业务码期望行，连续两次调用一致（纯函数）。"""
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
    """空库首次播种：业务码 + 1:1 表单 + 四个字段落库。"""
    await seed_usage_forms(platform_session, ConcurrentStableDict())
    await platform_session.commit()

    business = (await platform_session.execute(select(SysBusiness))).scalar_one()
    assert business.code == BUSINESS_CODE
    assert business.name == BUSINESS_NAME
    assert business.form_key == BUSINESS_FORM_KEY
    assert business.status == "enabled"

    form = (await platform_session.execute(select(SysForm))).scalar_one()
    assert int(form.business_id) == int(business.id)
    assert form.component is None
    assert form.status == "enabled"

    fields = (await platform_session.execute(select(SysField).order_by(SysField.sort))).scalars().all()
    assert [(field.field_key, field.name, field.type, field.sort, field.status) for field in fields] == [
        (column.key, column.label, column.field_type, column.sort, "enabled") for column in USAGE_COLUMNS
    ]


async def test_seed_is_idempotent(platform_session: AsyncSession) -> None:
    """重复执行：三表行数不变、字段行不重复。"""
    await seed_usage_forms(platform_session, ConcurrentStableDict())
    await platform_session.commit()
    first = await _counts(platform_session)

    await seed_usage_forms(platform_session, ConcurrentStableDict())
    await platform_session.commit()

    assert await _counts(platform_session) == first == (1, 1, 4)


async def test_seed_rejects_conflicting_business(platform_session: AsyncSession) -> None:
    """业务码已存在且名称不同 → `ParamError`，不静默覆盖。"""
    platform_session.add(SysBusiness(code=BUSINESS_CODE, name="被改过的名字", form_key=BUSINESS_FORM_KEY))
    await platform_session.commit()
    with pytest.raises(ParamError, match=r"业务码 .* 已存在且与声明不一致"):
        await seed_usage_forms(platform_session, ConcurrentStableDict())


async def test_seed_rejects_conflicting_form(platform_session: AsyncSession) -> None:
    """表单已存在且 `component` 不同 → `ParamError`。"""
    await seed_usage_forms(platform_session, ConcurrentStableDict())
    await platform_session.commit()
    form = (await platform_session.execute(select(SysForm))).scalar_one()
    form.component = "legacy"
    await platform_session.commit()
    with pytest.raises(ParamError, match=r"表单 .* 已存在且与声明不一致"):
        await seed_usage_forms(platform_session, ConcurrentStableDict())


async def test_seed_rejects_conflicting_field(platform_session: AsyncSession) -> None:
    """字段已存在且名称不同 → `ParamError`。"""
    await seed_usage_forms(platform_session, ConcurrentStableDict())
    await platform_session.commit()
    field = (await platform_session.execute(select(SysField).where(SysField.field_key == "topic_count"))).scalar_one()
    field.name = "旧名字"
    await platform_session.commit()
    with pytest.raises(ParamError, match="字段 topic_count 已存在且与声明不一致"):
        await seed_usage_forms(platform_session, ConcurrentStableDict())


async def test_seed_does_not_commit(platform_session: AsyncSession) -> None:
    """入口只 flush、不自行 commit（XA 分支由编排器统一提交）。"""
    recorder = _NoCommitSession(platform_session)
    await seed_usage_forms(cast("DbSession", recorder), ConcurrentStableDict())
    assert recorder.commits == 0
    assert await _counts(platform_session) == (1, 1, 4)


def test_manifest_declares_seed_and_allowlist() -> None:
    """`msg.toml` 种子声明字段完整，`allowlist` 与四列常量同名闭合。"""
    payload = tomllib.loads(_MANIFEST.read_text(encoding="utf-8"))
    sources = {str(item["data_source_key"]): item for item in payload["data_sources"]}
    assert sources["msg_tenant_usage"]["allowlist"] == [column.key for column in USAGE_COLUMNS]
    assert sources["msg_quota_projection"]["path"] == "/api/v1/msg/tenant-usage/quota"

    seeds = payload["seeds"]
    assert len(seeds) == 1
    seed = seeds[0]
    assert seed["name"] == "msg-usage-forms"
    assert seed["service"] == "msg"
    assert seed["db_key"] == "platform_platform"
    assert seed["module"] == "bms_msg.seeds.forms"
    assert seed["entry"] == "seed_usage_forms"
    assert seed["tables"] == {
        "sys_business": "code",
        "sys_form": "business_id",
        "sys_field": "form_id,field_key",
    }


def test_manifest_loads_and_seed_entry_is_importable() -> None:
    """装载期校验通过：清单可解析、种子入口可导入、`[seeds.tables]` 自然键可解析。"""
    manifest = load_product_manifest(_MANIFEST)
    assert [seed.name for seed in manifest.seeds] == ["msg-usage-forms"]
    module = importlib.import_module("bms_msg.seeds.forms")
    assert callable(module.__dict__.get("seed_usage_forms"))


async def test_endpoint_columns_match_seeded_fields(platform_session: AsyncSession) -> None:
    """端点 `columns` 与种子写入的 `sys_field` 完全一致（同一常量派生）。"""
    await seed_usage_forms(platform_session, ConcurrentStableDict())
    await platform_session.commit()
    fields = (await platform_session.execute(select(SysField).order_by(SysField.sort))).scalars().all()
    response = await tenant_usage()
    payload = json.loads(bytes(response.body))
    assert payload["columns"] == [{"key": field.field_key, "label": field.name} for field in fields]


async def _counts(session: AsyncSession) -> tuple[int, int, int]:
    """统计三表行数。

    Args:
        session: 会话。

    Returns:
        tuple[int, int, int]: `(sys_business, sys_form, sys_field)` 行数。
    """
    business = len((await session.execute(select(SysBusiness))).all())
    form = len((await session.execute(select(SysForm))).all())
    field = len((await session.execute(select(SysField))).all())
    return business, form, field
