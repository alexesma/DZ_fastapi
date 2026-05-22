"""add invalid cross table

Revision ID: 7c8d9e0f1a2b
Revises: 6b7c8d9e0f1a
Create Date: 2026-05-22 15:10:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


revision: str = "7c8d9e0f1a2b"
down_revision: Union[str, Sequence[str], None] = "5ab1c2d3e4f5"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "autopartinvalidcross",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("source_autopart_id", sa.Integer(), nullable=False),
        sa.Column("invalid_brand_id", sa.Integer(), nullable=False),
        sa.Column("invalid_oem_number", sa.String(length=50), nullable=False),
        sa.Column("invalid_autopart_id", sa.Integer(), nullable=True),
        sa.Column("comment", sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(
            ["invalid_autopart_id"],
            ["autopart.id"],
            ondelete="SET NULL",
        ),
        sa.ForeignKeyConstraint(
            ["invalid_brand_id"],
            ["brand.id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["source_autopart_id"],
            ["autopart.id"],
            ondelete="CASCADE",
        ),
        sa.CheckConstraint(
            "source_autopart_id != invalid_autopart_id",
            name="check_not_self_invalid_cross",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "source_autopart_id",
            "invalid_brand_id",
            "invalid_oem_number",
            name="uq_invalid_cross",
        ),
    )
    op.create_index(
        op.f("ix_autopartinvalidcross_source_autopart_id"),
        "autopartinvalidcross",
        ["source_autopart_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_autopartinvalidcross_invalid_brand_id"),
        "autopartinvalidcross",
        ["invalid_brand_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_autopartinvalidcross_invalid_oem_number"),
        "autopartinvalidcross",
        ["invalid_oem_number"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        op.f("ix_autopartinvalidcross_invalid_oem_number"),
        table_name="autopartinvalidcross",
    )
    op.drop_index(
        op.f("ix_autopartinvalidcross_invalid_brand_id"),
        table_name="autopartinvalidcross",
    )
    op.drop_index(
        op.f("ix_autopartinvalidcross_source_autopart_id"),
        table_name="autopartinvalidcross",
    )
    op.drop_table("autopartinvalidcross")
