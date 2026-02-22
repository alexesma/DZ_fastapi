"""add_provider_pricelist_filters

Revision ID: 2b6e1d3c7a9f
Revises: 7d4f0b6a2c3f
Create Date: 2026-02-21 00:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '2b6e1d3c7a9f'
down_revision = '7d4f0b6a2c3f'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        'providerpricelistconfig', sa.Column('min_price', sa.Float(), nullable=True)
    )
    op.add_column(
        'providerpricelistconfig', sa.Column('max_price', sa.Float(), nullable=True)
    )
    op.add_column(
        'providerpricelistconfig', sa.Column('min_quantity', sa.Integer(), nullable=True)
    )
    op.add_column(
        'providerpricelistconfig', sa.Column('max_quantity', sa.Integer(), nullable=True)
    )
    op.add_column(
        'providerpricelistconfig',
        sa.Column(
            'exclude_positions',
            sa.JSON(),
            nullable=False,
            server_default=sa.text("'[]'::json"),
        ),
    )


def downgrade():
    op.drop_column('providerpricelistconfig', 'exclude_positions')
    op.drop_column('providerpricelistconfig', 'max_quantity')
    op.drop_column('providerpricelistconfig', 'min_quantity')
    op.drop_column('providerpricelistconfig', 'max_price')
    op.drop_column('providerpricelistconfig', 'min_price')
