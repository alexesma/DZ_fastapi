"""add site offer snapshot to watchlist

Revision ID: e2a3b4c5d6e7
Revises: d1f2e3a4b5c7
Create Date: 2026-06-20 12:00:00.000000

"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

revision: str = "e2a3b4c5d6e7"
down_revision: Union[str, Sequence[str], None] = "d1f2e3a4b5c7"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "pricewatchitem",
        sa.Column("last_seen_site_offers", sa.JSON(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("pricewatchitem", "last_seen_site_offers")
