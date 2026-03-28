"""add ship price col to customer order config

Revision ID: b8c9d0e1f2a3
Revises: a7b8c9d0e1f2
Create Date: 2026-03-28 19:10:00.000000
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'b8c9d0e1f2a3'
down_revision = 'a7b8c9d0e1f2'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        'customerorderconfig',
        sa.Column('ship_price_col', sa.Integer(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column('customerorderconfig', 'ship_price_col')
