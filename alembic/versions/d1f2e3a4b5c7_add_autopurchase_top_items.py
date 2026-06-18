"""add autopurchase top items

Revision ID: d1f2e3a4b5c7
Revises: c7e8f9a0b1d2
Create Date: 2026-06-18 12:00:00.000000

"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

revision: str = "d1f2e3a4b5c7"
down_revision: Union[str, Sequence[str], None] = "c7e8f9a0b1d2"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "autopurchasetopitem",
        sa.Column("source", sa.String(length=32), nullable=False),
        sa.Column("autopart_id", sa.Integer(), nullable=True),
        sa.Column("oem_number", sa.String(length=64), nullable=False),
        sa.Column("brand_name", sa.String(length=255), nullable=True),
        sa.Column("autopart_name", sa.String(length=255), nullable=True),
        sa.Column("rank", sa.Integer(), nullable=False),
        sa.Column("sold_qty", sa.Integer(), nullable=False),
        sa.Column("target_stock_qty", sa.Integer(), nullable=False),
        sa.Column("is_active", sa.Boolean(), nullable=False),
        sa.Column("note", sa.Text(), nullable=True),
        sa.Column("imported_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("raw_payload", sa.JSON(), nullable=False),
        sa.Column("id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(["autopart_id"], ["autopart.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "source",
            "oem_number",
            "brand_name",
            name="uq_autopurchasetopitem_source_oem_brand",
        ),
    )
    op.create_index(
        op.f("ix_autopurchasetopitem_autopart_id"),
        "autopurchasetopitem",
        ["autopart_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_autopurchasetopitem_brand_name"),
        "autopurchasetopitem",
        ["brand_name"],
        unique=False,
    )
    op.create_index(
        op.f("ix_autopurchasetopitem_is_active"),
        "autopurchasetopitem",
        ["is_active"],
        unique=False,
    )
    op.create_index(
        op.f("ix_autopurchasetopitem_oem_number"),
        "autopurchasetopitem",
        ["oem_number"],
        unique=False,
    )
    op.create_index(
        op.f("ix_autopurchasetopitem_rank"),
        "autopurchasetopitem",
        ["rank"],
        unique=False,
    )
    op.create_index(
        op.f("ix_autopurchasetopitem_source"),
        "autopurchasetopitem",
        ["source"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(op.f("ix_autopurchasetopitem_source"), table_name="autopurchasetopitem")
    op.drop_index(op.f("ix_autopurchasetopitem_rank"), table_name="autopurchasetopitem")
    op.drop_index(
        op.f("ix_autopurchasetopitem_oem_number"),
        table_name="autopurchasetopitem",
    )
    op.drop_index(
        op.f("ix_autopurchasetopitem_is_active"),
        table_name="autopurchasetopitem",
    )
    op.drop_index(
        op.f("ix_autopurchasetopitem_brand_name"),
        table_name="autopurchasetopitem",
    )
    op.drop_index(
        op.f("ix_autopurchasetopitem_autopart_id"),
        table_name="autopurchasetopitem",
    )
    op.drop_table("autopurchasetopitem")
