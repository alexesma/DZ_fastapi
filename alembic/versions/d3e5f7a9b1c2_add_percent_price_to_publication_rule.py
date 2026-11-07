"""Add percent-of-source price mode to customer pricelist publication rules.

Revision ID: d3e5f7a9b1c2
Revises: c4d6e8f0a2b3
Create Date: 2026-11-06 12:00:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

revision: str = "d3e5f7a9b1c2"
down_revision: Union[str, Sequence[str], None] = "c4d6e8f0a2b3"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

TABLE = "customerpricelistpublicationrule"


def upgrade() -> None:
    op.add_column(
        TABLE,
        sa.Column(
            "price_mode",
            sa.String(length=16),
            nullable=False,
            server_default="default",
        ),
    )
    op.add_column(
        TABLE,
        sa.Column("price_percent", sa.Numeric(precision=7, scale=2), nullable=True),
    )
    op.add_column(TABLE, sa.Column("price_source_id", sa.Integer(), nullable=True))
    op.create_foreign_key(
        "fk_publication_rule_price_source",
        TABLE,
        "customerpricelistsource",
        ["price_source_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.execute(f"UPDATE {TABLE} SET price_mode = 'fixed' WHERE fixed_price IS NOT NULL")


def downgrade() -> None:
    op.drop_constraint("fk_publication_rule_price_source", TABLE, type_="foreignkey")
    op.drop_column(TABLE, "price_source_id")
    op.drop_column(TABLE, "price_percent")
    op.drop_column(TABLE, "price_mode")
