"""add use_for_order_insights to provider pricelist config

Revision ID: 6b7c8d9e0f1a
Revises: 5ab1c2d3e4f5
Create Date: 2026-05-22 11:40:00.000000
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "6b7c8d9e0f1a"
down_revision = "5ab1c2d3e4f5"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "providerpricelistconfig",
        sa.Column(
            "use_for_order_insights",
            sa.Boolean(),
            nullable=False,
            server_default=sa.false(),
        ),
    )
    op.execute(
        "UPDATE providerpricelistconfig "
        "SET use_for_order_insights = FALSE "
        "WHERE use_for_order_insights IS NULL"
    )
    op.alter_column(
        "providerpricelistconfig",
        "use_for_order_insights",
        server_default=None,
    )


def downgrade() -> None:
    op.drop_column(
        "providerpricelistconfig",
        "use_for_order_insights",
    )
