"""${message}

Revision ID: ${up_revision}
Revises: ${down_revision | comma,n}
Create Date: ${create_date}

- 归属链见 `alembic.ini` 的 `[alembic:<链名>]` 配置段与 `app/db/migration.py` 链注册；
- 公共字段块对齐 `BaseModel`（雪花 ID / 审计 / 软删除 / 乐观锁）；
- 索引命名对齐 `idx_{表}_{列}`；四库兼容编写（禁方言 SQL）；
- 只建表不写种子（种子走 `ops/seed_*.py` 幂等脚本）。
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
${imports if imports else ""}

revision: str = ${repr(up_revision)}
down_revision: str | None = ${repr(down_revision)}
branch_labels: str | Sequence[str] | None = ${repr(branch_labels)}
depends_on: str | Sequence[str] | None = ${repr(depends_on)}


def upgrade() -> None:
    """升级。"""
    ${upgrades if upgrades else "pass"}


def downgrade() -> None:
    """回滚。"""
    ${downgrades if downgrades else "pass"}
