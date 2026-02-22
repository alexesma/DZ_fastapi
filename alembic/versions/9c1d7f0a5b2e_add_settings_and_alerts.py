"""add_settings_and_alerts

Revision ID: 9c1d7f0a5b2e
Revises: 8a2c4f1d6b7e
Create Date: 2026-02-21 00:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '9c1d7f0a5b2e'
down_revision = '8a2c4f1d6b7e'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'pricecheckschedule',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('enabled', sa.Boolean(), nullable=False),
        sa.Column('days', sa.JSON(), nullable=False),
        sa.Column('times', sa.JSON(), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=True),
    )
    op.execute(
        "INSERT INTO pricecheckschedule (enabled, days, times) "
        "VALUES (true, '[]'::json, '[]'::json)"
    )
    op.create_table(
        'priceliststalealert',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('provider_id', sa.Integer(), nullable=False),
        sa.Column('provider_config_id', sa.Integer(), nullable=False),
        sa.Column('days_diff', sa.Integer(), nullable=False),
        sa.Column('last_price_date', sa.Date(), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(['provider_id'], ['provider.id']),
        sa.ForeignKeyConstraint(
            ['provider_config_id'], ['providerpricelistconfig.id']
        ),
    )


def downgrade():
    op.drop_table('priceliststalealert')
    op.drop_table('pricecheckschedule')
