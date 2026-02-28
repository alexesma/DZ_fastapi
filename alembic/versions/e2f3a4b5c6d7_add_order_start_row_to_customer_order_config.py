"""add order start row to customer order config

Revision ID: e2f3a4b5c6d7
Revises: d7e4f5a6b7c8
Create Date: 2026-02-28 00:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'e2f3a4b5c6d7'
down_revision = 'd7e4f5a6b7c8'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        'customerorderconfig',
        sa.Column('order_start_row', sa.Integer(), server_default='1'),
    )


def downgrade():
    op.drop_column('customerorderconfig', 'order_start_row')
