"""Add pricelist missing brand table

Revision ID: 6b1e4d9a2f7c
Revises: 3c2a9d1f5b7e, c9d8e7f6a5b4
Create Date: 2026-03-13 00:00:00.000000

"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = '6b1e4d9a2f7c'
down_revision: Union[str, Sequence[str], None] = (
    '3c2a9d1f5b7e',
    'c9d8e7f6a5b4',
)
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'pricelistmissingbrand',
        sa.Column('pricelist_id', sa.Integer(), nullable=False),
        sa.Column('provider_config_id', sa.Integer(), nullable=False),
        sa.Column('brand_name', sa.String(length=255), nullable=False),
        sa.Column('positions_count', sa.Integer(), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('id', sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(
            ['pricelist_id'], ['pricelist.id'], ondelete='CASCADE'
        ),
        sa.ForeignKeyConstraint(
            ['provider_config_id'],
            ['providerpricelistconfig.id'],
            ondelete='CASCADE',
        ),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index(
        'ix_pricelistmissingbrand_brand_name',
        'pricelistmissingbrand',
        ['brand_name'],
        unique=False,
    )
    op.create_index(
        'ix_pricelistmissingbrand_pricelist_id',
        'pricelistmissingbrand',
        ['pricelist_id'],
        unique=False,
    )
    op.create_index(
        'ix_pricelistmissingbrand_provider_config_id',
        'pricelistmissingbrand',
        ['provider_config_id'],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        'ix_pricelistmissingbrand_provider_config_id',
        table_name='pricelistmissingbrand',
    )
    op.drop_index(
        'ix_pricelistmissingbrand_pricelist_id',
        table_name='pricelistmissingbrand',
    )
    op.drop_index(
        'ix_pricelistmissingbrand_brand_name',
        table_name='pricelistmissingbrand',
    )
    op.drop_table('pricelistmissingbrand')
