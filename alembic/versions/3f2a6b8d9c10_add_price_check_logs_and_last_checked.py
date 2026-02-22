"""add_price_check_logs_and_last_checked

Revision ID: 3f2a6b8d9c10
Revises: 9c1d7f0a5b2e
Create Date: 2026-02-21 00:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '3f2a6b8d9c10'
down_revision = '9c1d7f0a5b2e'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        'pricecheckschedule',
        sa.Column('last_checked_at', sa.DateTime(timezone=True), nullable=True),
    )
    op.create_table(
        'pricechecklog',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('status', sa.String(length=32), nullable=False),
        sa.Column('message', sa.String(length=255), nullable=True),
        sa.Column('checked_at', sa.DateTime(timezone=True), nullable=True),
    )


def downgrade():
    op.drop_table('pricechecklog')
    op.drop_column('pricecheckschedule', 'last_checked_at')
