"""Add watchlist site qty

Revision ID: 2f6d9b1c0a8e
Revises: 9d2c7b1e4f6a
Create Date: 2026-02-26 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '2f6d9b1c0a8e'
down_revision: Union[str, None] = '9d2c7b1e4f6a'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        'pricewatchitem', sa.Column('last_seen_site_qty', sa.Integer(), nullable=True)
    )


def downgrade() -> None:
    op.drop_column('pricewatchitem', 'last_seen_site_qty')
