"""消息平台用量表单与字段种子（05-1；平台库三表）。

- **声明与执行分离**：产品自持清单 `deploy/registry/msg.toml` 的 `[[seeds]]` 声明
  「模块路径 + 入口名 + 目标库 + 自然键」；执行由平台**统一种子编排**调度（一个 XA 全局
  事务内串行执行、统一提交或整体回滚）。
- **入口契约**：沿用基座分支处理器范式 `async def entry(session, args) -> None`——会话由
  执行器传入且**已绑定 XA 分支连接**，写入自动并入分支事务，本模块**不自行 commit**。
- **写入内容**：业务码 `msg_usage`（`sys_business`）+ 1:1 表单（`sys_form`）+ 四字段
  （`sys_field`）。三表归**平台服务库**，故种子声明 `db_key = platform_platform`；
  服务身份仍是 `msg`，体现「产品声明、平台调度、产品执行」。
- **幂等**：按自然键判存——业务码按 `code`、表单按 `business_id`、字段按
  `(form_id, field_key)`；存在且**全字段同值**即跳过，存在且异值抛 `ParamError`
  （由编排器事务前预检 / 分支执行拦截）。
- **不建**：`sys_menu_form`（无菜单入口）、`sys_button` / `sys_permission`（只读投影无按钮）、
  `msg_usage` 物理表（数据由取数端点按 `data_source_key` 提供）。
"""

from bms_core.core.concurrent import ConcurrentStableDict, ConcurrentStableFrozenDict, ConcurrentStableList
from bms_core.core.exceptions import ParamError
from bms_core.db.session import DbSession
from bms_core.models.platform.menu_form import SysField, SysForm
from bms_core.models.platform.permission_meta import SysBusiness
from bms_core.tenant.seed.contract import ExpectedRow
from sqlalchemy import select

from bms_msg.usage.columns import USAGE_COLUMNS, UsageColumn

__all__ = [
    "BUSINESS_CODE",
    "BUSINESS_FORM_KEY",
    "BUSINESS_NAME",
    "expected_rows",
    "seed_usage_forms",
]

BUSINESS_CODE = "msg_usage"
"""用量表单的业务码（`sys_business.code`）。"""

BUSINESS_NAME = "消息平台用量"
"""业务码显示名（冲突判定用）。"""

BUSINESS_FORM_KEY = "business"
"""业务码行的 `form_key`（平台业务码字典约定；`msg_usage` 只作 `code` 与 `FormRecord.form_key`）。"""

_FORM_COMPONENT: str | None = None
"""表单视图组件（只读投影无前端组件）。"""

_STATUS_ENABLED = "enabled"
"""启用状态（三表统一取 `enabled`）。"""


def expected_rows() -> ConcurrentStableList[ExpectedRow]:
    """用量表单种子的期望行（冲突预检用）。

    **纯函数**：只读模块常量，不连库、不写库。`sys_form` / `sys_field` 依赖自增 ID，
    不列入期望行，靠唯一约束与入口内全字段比对兜底。

    Returns:
        ConcurrentStableList[ExpectedRow]: 期望行（业务码 `msg_usage` 一条）。
    """
    return ConcurrentStableList(
        (
            ExpectedRow(
                table="sys_business",
                key=ConcurrentStableFrozenDict({"code": BUSINESS_CODE}),
                values=ConcurrentStableFrozenDict(
                    {"name": BUSINESS_NAME, "form_key": BUSINESS_FORM_KEY, "status": _STATUS_ENABLED}
                ),
            ),
        )
    )


async def seed_usage_forms(session: DbSession, args: ConcurrentStableDict[str, object]) -> None:
    """写入用量表单与四字段（在 XA 分支事务内执行，不自行提交）。

    Args:
        session: 已绑定 XA 分支连接的会话（写入自动并入分支事务）。
        args: 分支参数（本种子不消费）。

    Raises:
        ParamError: 业务码 / 表单 / 字段已存在且任一字段值与声明不一致。
    """
    del args
    business = await _ensure_business(session)
    form = await _ensure_form(session, business_id=int(business.id))
    for column in USAGE_COLUMNS:
        await _ensure_field(session, form_id=int(form.id), column=column)


async def _ensure_business(session: DbSession) -> SysBusiness:
    """按 `code` 幂等写业务码。

    Args:
        session: XA 分支会话。

    Returns:
        SysBusiness: 已存在或新建的业务码行。

    Raises:
        ParamError: 已存在且 `name` / `form_key` / `status` 与声明不一致。
    """
    existing = (
        await session.execute(
            select(SysBusiness).where(SysBusiness.code == BUSINESS_CODE, SysBusiness.deleted_at.is_(None))
        )
    ).scalar_one_or_none()
    if existing is None:
        created = SysBusiness(
            code=BUSINESS_CODE,
            name=BUSINESS_NAME,
            form_key=BUSINESS_FORM_KEY,
            status=_STATUS_ENABLED,
        )
        session.add(created)
        await session.flush()
        return created
    _ensure_same(
        label=f"业务码 {BUSINESS_CODE}",
        values=ConcurrentStableFrozenDict(
            {"name": BUSINESS_NAME, "form_key": BUSINESS_FORM_KEY, "status": _STATUS_ENABLED}
        ),
        existing=existing,
    )
    return existing


async def _ensure_form(session: DbSession, *, business_id: int) -> SysForm:
    """按 `business_id` 幂等写表单。

    Args:
        session: XA 分支会话。
        business_id: 业务码主键。

    Returns:
        SysForm: 已存在或新建的表单行。

    Raises:
        ParamError: 已存在且 `component` / `status` 与声明不一致。
    """
    existing = (
        await session.execute(select(SysForm).where(SysForm.business_id == business_id, SysForm.deleted_at.is_(None)))
    ).scalar_one_or_none()
    if existing is None:
        created = SysForm(business_id=business_id, component=_FORM_COMPONENT, status=_STATUS_ENABLED)
        session.add(created)
        await session.flush()
        return created
    _ensure_same(
        label=f"表单 business_id={business_id}",
        values=ConcurrentStableFrozenDict({"component": _FORM_COMPONENT, "status": _STATUS_ENABLED}),
        existing=existing,
    )
    return existing


async def _ensure_field(session: DbSession, *, form_id: int, column: UsageColumn) -> SysField:
    """按 `(form_id, field_key)` 幂等写字段。

    Args:
        session: XA 分支会话。
        form_id: 表单主键。
        column: 用量列（`UsageColumn`）。

    Returns:
        SysField: 已存在或新建的字段行。

    Raises:
        ParamError: 已存在且 `name` / `type` / `sort` / `status` 与声明不一致。
    """
    key = column.key
    name = column.label
    field_type = column.field_type
    sort = column.sort
    existing = (
        await session.execute(
            select(SysField).where(
                SysField.form_id == form_id,
                SysField.field_key == key,
                SysField.deleted_at.is_(None),
            )
        )
    ).scalar_one_or_none()
    if existing is None:
        created = SysField(
            form_id=form_id,
            field_key=key,
            name=name,
            type=field_type,
            sort=sort,
            status=_STATUS_ENABLED,
        )
        session.add(created)
        await session.flush()
        return created
    _ensure_same(
        label=f"字段 {key}",
        values=ConcurrentStableFrozenDict({"name": name, "type": field_type, "sort": sort, "status": _STATUS_ENABLED}),
        existing=existing,
    )
    return existing


def _ensure_same(*, label: str, values: ConcurrentStableFrozenDict[str, object], existing: object) -> None:
    """比对已存在行与声明值（同值通过、异值抛 `ParamError`）。

    Args:
        label: 错误定位文本（业务码 / 表单 / 字段）。
        values: 声明值（列名 → 期望值）。
        existing: 已存在的 ORM 行。

    Raises:
        ParamError: 任一列值与期望值不同。
    """
    diffs = [
        f"{column}：现有 {getattr(existing, column, None)!r} / 期望 {expected!r}"
        for column, expected in values.items()
        if getattr(existing, column, None) != expected
    ]
    if diffs:
        raise ParamError(f"{label} 已存在且与声明不一致（{'；'.join(diffs)}）")
