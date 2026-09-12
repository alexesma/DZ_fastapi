"""add Parts-Soft product data

Revision ID: f8d3b7c1a5e9
Revises: e7c2a9f4b1d3
Create Date: 2026-09-12
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "f8d3b7c1a5e9"
down_revision: Union[str, None] = "e7c2a9f4b1d3"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "autopart",
        sa.Column("partssoft_product_id", sa.Integer(), nullable=True),
    )
    op.add_column(
        "autopart",
        sa.Column(
            "partssoft_product_updated_at",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
    )
    op.add_column(
        "autopart",
        sa.Column("partssoft_synced_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "autopart",
        sa.Column(
            "partssoft_payload",
            sa.JSON(),
            nullable=False,
            server_default=sa.text("'{}'::json"),
        ),
    )
    op.create_index(
        "ix_autopart_partssoft_product_id",
        "autopart",
        ["partssoft_product_id"],
        unique=True,
    )


def downgrade() -> None:
    op.drop_index("ix_autopart_partssoft_product_id", table_name="autopart")
    op.drop_column("autopart", "partssoft_payload")
    op.drop_column("autopart", "partssoft_synced_at")
    op.drop_column("autopart", "partssoft_product_updated_at")
    op.drop_column("autopart", "partssoft_product_id")
