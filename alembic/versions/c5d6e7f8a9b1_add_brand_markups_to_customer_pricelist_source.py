"""add brand_markups to customerpricelistsource

Revision ID: c5d6e7f8a9b1
Revises: b2c3d4e5f6a1
Create Date: 2026-04-13 18:45:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "c5d6e7f8a9b1"
down_revision: Union[str, Sequence[str], None] = "b2c3d4e5f6a1"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "customerpricelistsource",
        sa.Column("brand_markups", sa.JSON(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("customerpricelistsource", "brand_markups")
