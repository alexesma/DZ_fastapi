"""add top_site_offers to autopurchase run item

Revision ID: c7e8f9a0b1d2
Revises: eb1c2d3e4f5a
Create Date: 2026-06-11 10:00:00.000000

"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "c7e8f9a0b1d2"
down_revision: Union[str, Sequence[str], None] = "eb1c2d3e4f5a"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "autopurchaserunitem",
        sa.Column("top_site_offers", sa.JSON(), nullable=True),
    )
    op.add_column(
        "autopurchaserunitem",
        sa.Column("cross_group", sa.JSON(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("autopurchaserunitem", "cross_group")
    op.drop_column("autopurchaserunitem", "top_site_offers")
