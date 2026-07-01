"""add autopurchase exclusions and supplier blocks

Revision ID: a1b2c3d4e5f7
Revises: f4a5b6c7d8e9
Create Date: 2026-07-01 00:00:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

revision: str = "a1b2c3d4e5f7"
down_revision: Union[str, Sequence[str], None] = "f4a5b6c7d8e9"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "provider",
        sa.Column(
            "autopurchase_blocked",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        ),
    )
    op.add_column(
        "provider",
        sa.Column("autopurchase_block_reason", sa.Text(), nullable=True),
    )
    op.add_column(
        "providerpricelistconfig",
        sa.Column(
            "autopurchase_blocked",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        ),
    )
    op.add_column(
        "providerpricelistconfig",
        sa.Column("autopurchase_block_reason", sa.Text(), nullable=True),
    )
    op.create_table(
        "autopurchaseexcludeditem",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("autopart_id", sa.Integer(), nullable=True),
        sa.Column("oem_number", sa.String(length=64), nullable=False),
        sa.Column(
            "brand_name",
            sa.String(length=256),
            nullable=False,
            server_default="",
        ),
        sa.Column("autopart_name", sa.String(length=64), nullable=True),
        sa.Column("reason", sa.Text(), nullable=True),
        sa.Column(
            "is_active",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("true"),
        ),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["autopart_id"], ["autopart.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "oem_number",
            "brand_name",
            name="uq_autopurchaseexcludeditem_oem_brand",
        ),
    )
    op.create_index(
        "ix_autopurchaseexcludeditem_autopart_id",
        "autopurchaseexcludeditem",
        ["autopart_id"],
    )
    op.create_index(
        "ix_autopurchaseexcludeditem_brand_name",
        "autopurchaseexcludeditem",
        ["brand_name"],
    )
    op.create_index(
        "ix_autopurchaseexcludeditem_is_active",
        "autopurchaseexcludeditem",
        ["is_active"],
    )
    op.create_index(
        "ix_autopurchaseexcludeditem_oem_number",
        "autopurchaseexcludeditem",
        ["oem_number"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_autopurchaseexcludeditem_oem_number",
        table_name="autopurchaseexcludeditem",
    )
    op.drop_index(
        "ix_autopurchaseexcludeditem_is_active",
        table_name="autopurchaseexcludeditem",
    )
    op.drop_index(
        "ix_autopurchaseexcludeditem_brand_name",
        table_name="autopurchaseexcludeditem",
    )
    op.drop_index(
        "ix_autopurchaseexcludeditem_autopart_id",
        table_name="autopurchaseexcludeditem",
    )
    op.drop_table("autopurchaseexcludeditem")
    op.drop_column("providerpricelistconfig", "autopurchase_block_reason")
    op.drop_column("providerpricelistconfig", "autopurchase_blocked")
    op.drop_column("provider", "autopurchase_block_reason")
    op.drop_column("provider", "autopurchase_blocked")
