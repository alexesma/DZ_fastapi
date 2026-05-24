"""add raw brand name support to invalid crosses

Revision ID: 9ab0c1d2e3f4
Revises: 8d9e0f1a2b3c
Create Date: 2026-05-24 16:40:00.000000

"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "9ab0c1d2e3f4"
down_revision: Union[str, Sequence[str], None] = "8d9e0f1a2b3c"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "autopartinvalidcross",
        sa.Column("invalid_brand_name_raw", sa.String(length=120), nullable=True),
    )
    op.create_index(
        op.f("ix_autopartinvalidcross_invalid_brand_name_raw"),
        "autopartinvalidcross",
        ["invalid_brand_name_raw"],
        unique=False,
    )
    op.alter_column(
        "autopartinvalidcross",
        "invalid_brand_id",
        existing_type=sa.Integer(),
        nullable=True,
    )


def downgrade() -> None:
    op.alter_column(
        "autopartinvalidcross",
        "invalid_brand_id",
        existing_type=sa.Integer(),
        nullable=False,
    )
    op.drop_index(
        op.f("ix_autopartinvalidcross_invalid_brand_name_raw"),
        table_name="autopartinvalidcross",
    )
    op.drop_column("autopartinvalidcross", "invalid_brand_name_raw")
