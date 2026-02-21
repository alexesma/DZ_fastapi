"""add_customer_pricelist_filters

Revision ID: 7d4f0b6a2c3f
Revises: 1c8b9d2a4e01
Create Date: 2026-02-21 00:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '7d4f0b6a2c3f'
down_revision = '1c8b9d2a4e01'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        'customerpricelistconfig',
        sa.Column(
            'default_filters',
            sa.JSON(),
            server_default=sa.text("'{}'::json"),
            nullable=False,
        ),
    )
    op.add_column(
        'customerpricelistconfig',
        sa.Column(
            'own_filters',
            sa.JSON(),
            server_default=sa.text("'{}'::json"),
            nullable=False,
        ),
    )
    op.add_column(
        'customerpricelistconfig',
        sa.Column(
            'other_filters',
            sa.JSON(),
            server_default=sa.text("'{}'::json"),
            nullable=False,
        ),
    )
    op.add_column(
        'customerpricelistconfig',
        sa.Column(
            'supplier_filters',
            sa.JSON(),
            server_default=sa.text("'{}'::json"),
            nullable=False,
        ),
    )


def downgrade():
    op.drop_column('customerpricelistconfig', 'supplier_filters')
    op.drop_column('customerpricelistconfig', 'other_filters')
    op.drop_column('customerpricelistconfig', 'own_filters')
    op.drop_column('customerpricelistconfig', 'default_filters')
