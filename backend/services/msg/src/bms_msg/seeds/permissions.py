"""消息平台权限码种子（05-3；平台库三表）。

- 写入业务码 `msg`、全局动作码 `query` / `manage` / `publish` / `consume` / `rpc`，
  以及五组 `sys_permission`（`msg:query` / `msg:manage` / `msg:publish` / `msg:consume` / `msg:rpc`）；
- **已存在的业务码 / 动作码原样保留**：全局动作字典由平台维护，名称差异不视为冲突；
  只补齐缺失行与 `sys_permission` 组合；
- 入口契约与 05-1 表单种子一致：`async def seed_permissions(session, args)`，只 flush、不 commit；
- `expected_rows()` 只列本产品新业务码（动作码名称归平台，不做值比对）。
"""

from dataclasses import dataclass

from bms_core.core.concurrent import (
    ConcurrentStableDict,
    ConcurrentStableFrozenDict,
    ConcurrentStableFrozenList,
    ConcurrentStableList,
)
from bms_core.core.exceptions import ParamError
from bms_core.db.session import DbSession
from bms_core.models.platform.permission_meta import SysAction, SysBusiness, SysPermission
from bms_core.tenant.seed.contract import ExpectedRow
from sqlalchemy import select

__all__ = [
    "ACTION_CODES",
    "BUSINESS_CODE",
    "BUSINESS_FORM_KEY",
    "BUSINESS_NAME",
    "PERMISSION_ACTIONS",
    "ActionSpec",
    "expected_rows",
    "seed_permissions",
]

BUSINESS_CODE = "msg"
"""消息平台业务码（`sys_business.code`）。"""

BUSINESS_NAME = "消息平台"
"""业务码显示名。"""

BUSINESS_FORM_KEY = "business"
"""业务码行的 `form_key`（平台业务码字典约定）。"""


@dataclass(frozen=True)
class ActionSpec:
    """一个全局动作码与默认中文名。"""

    code: str
    """动作码（`sys_action.code`）。"""

    label: str
    """新建时的中文名（已存在动作码不覆盖名称）。"""


ACTION_CODES: ConcurrentStableFrozenList[ActionSpec] = ConcurrentStableFrozenList(
    (
        ActionSpec("query", "查询"),
        ActionSpec("manage", "管理"),
        ActionSpec("publish", "发布"),
        ActionSpec("consume", "消费"),
        ActionSpec("rpc", "RPC 调用"),
    )
)
"""本产品用到的全局动作码与默认中文名。"""

PERMISSION_ACTIONS: ConcurrentStableFrozenList[str] = ConcurrentStableFrozenList(spec.code for spec in ACTION_CODES)
"""`msg` 业务码下需要保证存在的权限码动作（`msg:<action>`）。"""

_STATUS_ENABLED = "enabled"
"""启用状态。"""


def expected_rows() -> ConcurrentStableList[ExpectedRow]:
    """权限码种子的期望行（冲突预检用）。

    **纯函数**：只列本产品新业务码；全局动作码名称归平台维护，`sys_permission` 依赖自增 ID，
    两者靠唯一约束与入口内判存兜底。

    Returns:
        ConcurrentStableList[ExpectedRow]: 期望行（业务码 `msg` 一条）。
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


async def seed_permissions(session: DbSession, args: ConcurrentStableDict[str, object]) -> None:
    """写入消息平台权限码（在 XA 分支事务内执行，不自行提交）。

    Args:
        session: 已绑定 XA 分支连接的会话。
        args: 分支参数（本种子不消费）。

    Raises:
        ParamError: 业务码已存在且名称 / form_key / 状态与声明不一致。
    """
    del args
    business = await _ensure_business(session)
    for spec in ACTION_CODES:
        action = await _ensure_action(session, code=spec.code, name=spec.label)
        await _ensure_permission(session, business_id=int(business.id), action_id=int(action.id))


async def _ensure_business(session: DbSession) -> SysBusiness:
    """按 `code` 幂等写业务码。

    Args:
        session: XA 分支会话。

    Returns:
        SysBusiness: 已存在或新建的业务码行。

    Raises:
        ParamError: 已存在且名称 / form_key / 状态与声明不一致。
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
    diffs = [
        f"{column}：现有 {getattr(existing, column, None)!r} / 期望 {expected!r}"
        for column, expected in (
            ("name", BUSINESS_NAME),
            ("form_key", BUSINESS_FORM_KEY),
            ("status", _STATUS_ENABLED),
        )
        if getattr(existing, column, None) != expected
    ]
    if diffs:
        raise ParamError(f"业务码 {BUSINESS_CODE} 已存在且与声明不一致（{'；'.join(diffs)}）")
    return existing


async def _ensure_action(session: DbSession, *, code: str, name: str) -> SysAction:
    """按 `code` 幂等取 / 建全局动作码（已存在不覆盖名称）。

    Args:
        session: XA 分支会话。
        code: 动作码。
        name: 新建时的中文名。

    Returns:
        SysAction: 已存在或新建的动作码行。
    """
    existing = (
        await session.execute(select(SysAction).where(SysAction.code == code, SysAction.deleted_at.is_(None)))
    ).scalar_one_or_none()
    if existing is not None:
        return existing
    created = SysAction(code=code, name=name, form_key="action", status=_STATUS_ENABLED)
    session.add(created)
    await session.flush()
    return created


async def _ensure_permission(session: DbSession, *, business_id: int, action_id: int) -> SysPermission:
    """按 `(business_id, action_id)` 幂等写权限码组合。

    Args:
        session: XA 分支会话。
        business_id: 业务码主键。
        action_id: 动作码主键。

    Returns:
        SysPermission: 已存在或新建的权限码行。
    """
    existing = (
        await session.execute(
            select(SysPermission).where(
                SysPermission.business_id == business_id,
                SysPermission.action_id == action_id,
                SysPermission.deleted_at.is_(None),
            )
        )
    ).scalar_one_or_none()
    if existing is not None:
        return existing
    created = SysPermission(business_id=business_id, action_id=action_id, status=_STATUS_ENABLED)
    session.add(created)
    await session.flush()
    return created
