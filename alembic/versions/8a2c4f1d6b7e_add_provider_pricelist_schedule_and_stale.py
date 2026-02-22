"""add_provider_pricelist_stale_tracking

Revision ID: 8a2c4f1d6b7e
Revises: 2b6e1d3c7a9f
Create Date: 2026-02-21 00:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '8a2c4f1d6b7e'
down_revision = '2b6e1d3c7a9f'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        'providerpricelistconfig',
        sa.Column('max_days_without_update', sa.Integer(), nullable=True),
    )
    op.add_column(
        'providerpricelistconfig',
        sa.Column('last_stale_alert_at', sa.DateTime(timezone=True), nullable=True),
    )
    op.execute(
        "UPDATE providerpricelistconfig SET max_days_without_update = 3 "
        "WHERE max_days_without_update IS NULL"
    )


def downgrade():
    op.drop_column('providerpricelistconfig', 'last_stale_alert_at')
    op.drop_column('providerpricelistconfig', 'max_days_without_update')
