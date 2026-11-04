"""Alembic 迁移环境（msg · 08B 00-1 工程骨架）：按「业务域 × 数据源」清单执行，离线 / 在线两模式。

- **清单选择**：配置段名即清单名——`alembic -n alembic:msg:platform upgrade head`（`[alembic]` 段为
  缺省清单，供裸命令使用）；版本目录由配置段的 `version_locations` 指定，目标元数据取自清单注册表
  `bms_core/db/migration.py`（**唯一来源**，本仓不重复登记表集）。
- **表归属**：装载期调用 `services.table_registry.register()`（空清单幂等），首批表登记后迁移链自动可用。
- **URL 解析优先级**：`-x url=`（显式）> `BMS_MIGRATION_URL` 环境变量 > 配置（清单对应库）；
  可加 `-x db_key=tenant_{code}` 经 `url_template` 解析租户库连接串。
- **在线模式**：达梦 `dm` 为同步驱动（无异步方言）→ 同步引擎执行；其余方言异步引擎
  （`create_async_engine`）+ `connection.run_sync(context.run_migrations)`。
- **达梦模式切换**：`-x schema=`（仅达梦生效）→ 迁移前于同一连接执行 `SET SCHEMA`，
  不被支持时回落 `ALTER SESSION SET CURRENT_SCHEMA`。
- **空清单**：无迁移脚本时打印「无脚本（跳过）」并正常退出（8B_00 阶段即此态）。
- 四库（SQLite / MySQL / PostgreSQL / 达梦）共用本环境；迁移脚本禁写方言 SQL。
"""

import asyncio
import os
import re
from collections.abc import Mapping
from functools import partial

from alembic.ddl.base import ColumnType, alter_table, format_column_name, format_type
from alembic.ddl.impl import DefaultImpl
from bms_core.core.config import get_settings
from bms_core.core.exceptions import ConfigError
from bms_core.db.migration import (
    MigrationSeries,
    apply_session_schema,
    has_revisions,
    resolve_series_from_section,
    series_metadata,
    series_url,
)
from sqlalchemy import Connection, Engine, create_engine, pool
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.ext.compiler import compiles

from alembic import context
from services.table_registry import register as register_product_tables

config = context.config

# 表归属**产品注入**（bms 12_04）：迁移期链派生（`series_tables`）与元数据子集需先注册本产品表归属
register_product_tables()

_SYNC_ONLY_DIALECTS = frozenset({"dm"})
"""仅同步驱动的方言（达梦；无异步方言实现）。"""


@compiles(ColumnType, "dm")
def _dm_visit_column_type(element: ColumnType, compiler: object, **kw: object) -> str:
    """达梦列类型变更 DDL（`ALTER TABLE … MODIFY <列> <类型>`）。

    Alembic 默认模板为 PostgreSQL 式 `ALTER TABLE … ALTER COLUMN … TYPE …`，达梦不支持
    （10_04 真库实测：第 1 行 `[TYPE]` 语法分析出错）；达梦支持 MySQL 式 `MODIFY`。

    Args:
        element: Alembic 列类型变更构造。
        compiler: DDL 编译器。
        kw: 编译扩展参数。

    Returns:
        str: 达梦 `ALTER TABLE … MODIFY …` 语句。
    """
    table = alter_table(compiler, element.table_name, element.schema)  # type: ignore[arg-type]
    column = format_column_name(compiler, element.column_name)  # type: ignore[arg-type]
    type_sql = format_type(compiler, element.type_)  # type: ignore[arg-type]
    return f"{table} MODIFY {column} {type_sql}"


class DMImpl(DefaultImpl):
    """达梦 DDL 实现（Alembic 未内建 `dm` 方言的 DDL impl，缺注册时 `MigrationContext` 直接 KeyError）。

    达梦与 Oracle 同源、DDL 语法兼容通用形态；本类只做注册与方言声明，
    实际 DDL 生成沿用 `DefaultImpl` 的通用实现（差异项实测见 01_05）。
    """

    __dialect__ = "dm"


_SCHEMA_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
"""模式名合法形态（防拼接注入）。"""


def _x_args() -> Mapping[str, str]:
    """取 Alembic 命令行扩展参数（`-x k=v`）。

    Returns:
        Mapping[str, str]: 扩展参数字典。
    """
    x_args: Mapping[str, str] = context.get_x_argument(as_dictionary=True)
    return x_args


def _series() -> MigrationSeries:
    """取当前迁移链（配置段名派生：`[alembic]` → 缺省租户链；`[alembic:<链名>]` → 该链）。

    Returns:
        MigrationSeries: 链定义。

    Raises:
        ConfigError: 配置段未登记。
    """
    return resolve_series_from_section(config.config_ini_section)


def _database_url(series: MigrationSeries) -> str:
    """取迁移目标连接串（显式参数 > 环境变量 > 配置）。

    Args:
        series: 链定义。

    Returns:
        str: 数据库 URL。
    """
    x_args = _x_args()
    explicit = x_args.get("url")
    if explicit:
        return explicit
    env_url = os.environ.get("BMS_MIGRATION_URL", "")
    if env_url:
        return env_url
    db_key = x_args.get("db_key") or None
    # 命令行显式指定库键 = 运维通道（允许跨服务键，如 platform_tenant / tenant_org_acme）
    return series_url(series, get_settings(), db_key=db_key, allow_cross_service=bool(db_key))


def _schema_name() -> str:
    """取目标模式名（`-x schema=`；仅达梦生效）。

    Returns:
        str: 模式名（未指定为空串）。

    Raises:
        ConfigError: 模式名形态非法。
    """
    schema = (_x_args().get("schema") or "").strip()
    if schema and not _SCHEMA_PATTERN.match(schema):
        raise ConfigError(f"模式名形态非法：{schema}（允许字母 / 数字 / 下划线且不以数字开头）")
    return schema


def run_migrations_offline() -> None:
    """离线模式：仅生成 SQL，不连接数据库。"""
    series = _series()
    if not has_revisions(series):
        print(f"[alembic] 链 {series.name} 暂无迁移脚本（跳过）")
        return
    context.configure(
        url=_database_url(series),
        target_metadata=series_metadata(series),
        version_table=series.version_table,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def _do_run_migrations(connection: Connection, series: MigrationSeries, schema: str) -> None:
    """在线迁移主体（同步回调，由 `run_sync` 调用）。

    Args:
        connection: 同步连接（异步连接的同步封装）。
        series: 链定义。
        schema: 目标模式名（仅达梦生效）。
    """
    apply_session_schema(connection, schema)
    # 达梦：显式声明版本表模式——达梦方言 `has_table` 不认会话 `SET SCHEMA` 后的当前模式，
    # 增量迁移会重复建 `alembic_version` 报「对象已存在」（10_04 真库实测）；显式模式后
    # 版本表检查 / 建表 / 读写均限定在目标模式内。
    context.configure(
        connection=connection,
        target_metadata=series_metadata(series),
        version_table=series.version_table,
        version_table_schema=schema or None,
    )
    with context.begin_transaction():
        context.run_migrations()


async def _run_async_migrations(url: str, series: MigrationSeries, schema: str) -> None:
    """在线模式（异步引擎）：连接并执行迁移。

    Args:
        url: 数据库 URL。
        series: 链定义。
        schema: 目标模式名（仅达梦生效）。
    """
    engine = create_async_engine(url, poolclass=pool.NullPool)
    async with engine.connect() as connection:
        await connection.run_sync(partial(_do_run_migrations, series=series, schema=schema))
    await engine.dispose()


def _run_sync_migrations(url: str, series: MigrationSeries, schema: str) -> None:
    """在线模式（同步引擎，达梦等无异步方言的驱动）：连接并执行迁移。

    Args:
        url: 数据库 URL。
        series: 链定义。
        schema: 目标模式名（仅达梦生效）。
    """
    engine: Engine = create_engine(url, poolclass=pool.NullPool)
    try:
        with engine.connect() as connection:
            _do_run_migrations(connection, series, schema)
    finally:
        engine.dispose()


def run_migrations_online() -> None:
    """在线模式入口（按方言分支：达梦同步、其余异步）。"""
    series = _series()
    if not has_revisions(series):
        print(f"[alembic] 链 {series.name} 暂无迁移脚本（跳过）")
        return
    url = _database_url(series)
    schema = _schema_name()
    dialect = make_url(url).get_backend_name()
    if dialect in _SYNC_ONLY_DIALECTS:
        _run_sync_migrations(url, series, schema)
        return
    asyncio.run(_run_async_migrations(url, series, schema))


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
