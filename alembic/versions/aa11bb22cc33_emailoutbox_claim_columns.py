"""emailoutbox claim columns (claimed_by, claimed_at)

Добавляет поля для атомарного «захвата» письма воркером-релеем, чтобы
несколько машин не отправили одно письмо дважды.

Revision ID: aa11bb22cc33
Revises: 50a408eab54e
Create Date: 2026-07-09 09:00:00.000000
"""
from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

revision: str = "aa11bb22cc33"
down_revision: Union[str, Sequence[str], None] = "50a408eab54e"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "emailoutbox",
        sa.Column("claimed_by", sa.String(length=128), nullable=True),
    )
    op.add_column(
        "emailoutbox",
        sa.Column(
            "claimed_at",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
    )
    op.create_index(
        "ix_emailoutbox_claimed_at",
        "emailoutbox",
        ["claimed_at"],
    )


def downgrade() -> None:
    op.drop_index("ix_emailoutbox_claimed_at", table_name="emailoutbox")
    op.drop_column("emailoutbox", "claimed_at")
    op.drop_column("emailoutbox", "claimed_by")
