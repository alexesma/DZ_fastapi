"""Add customer pricelist sources and scheduling

Revision ID: 3f8b2c1d9a7e
Revises: 2179bca37b49
Create Date: 2026-02-16 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '3f8b2c1d9a7e'
down_revision: Union[str, None] = '2179bca37b49'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        'provider',
        sa.Column('is_own_price', sa.Boolean(), server_default=sa.text('false'), nullable=False),
    )

    op.add_column(
        'customerpricelistconfig',
        sa.Column(
            'schedule_days',
            sa.JSON(),
            server_default=sa.text("'[]'::json"),
            nullable=False,
        ),
    )
    op.add_column(
        'customerpricelistconfig',
        sa.Column(
            'schedule_times',
            sa.JSON(),
            server_default=sa.text("'[]'::json"),
            nullable=False,
        ),
    )
    op.add_column(
        'customerpricelistconfig',
        sa.Column(
            'emails',
            sa.JSON(),
            server_default=sa.text("'[]'::json"),
            nullable=False,
        ),
    )
    op.add_column(
        'customerpricelistconfig',
        sa.Column('is_active', sa.Boolean(), server_default=sa.text('true'), nullable=False),
    )
    op.add_column(
        'customerpricelistconfig',
        sa.Column('last_sent_at', sa.DateTime(timezone=True), nullable=True),
    )

    op.add_column(
        'autopartpricehistory',
        sa.Column('provider_config_id', sa.Integer(), nullable=True),
    )
    op.create_foreign_key(
        'autopartpricehistory_provider_config_id_fkey',
        'autopartpricehistory',
        'providerpricelistconfig',
        ['provider_config_id'],
        ['id'],
    )

    op.execute(
        """
        UPDATE autopartpricehistory aph
        SET provider_config_id = pl.provider_config_id
        FROM pricelist pl
        WHERE aph.pricelist_id = pl.id
        """
    )

    op.drop_index(
        'idx_autopart_price_history_autopart_provider_created_at',
        table_name='autopartpricehistory',
    )
    op.create_index(
        'idx_autopart_price_history_autopart_provider_created_at',
        'autopartpricehistory',
        ['autopart_id', 'provider_id', 'provider_config_id', 'created_at'],
        unique=False,
    )

    op.create_table(
        'customerpricelistsource',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('customer_config_id', sa.Integer(), nullable=False),
        sa.Column('provider_config_id', sa.Integer(), nullable=False),
        sa.Column('enabled', sa.Boolean(), server_default=sa.text('true'), nullable=False),
        sa.Column('markup', sa.Float(), server_default=sa.text('1.0'), nullable=False),
        sa.Column('brand_filters', sa.JSON(), server_default=sa.text("'{}'::json"), nullable=False),
        sa.Column('position_filters', sa.JSON(), server_default=sa.text("'{}'::json"), nullable=False),
        sa.Column('min_price', sa.DECIMAL(10, 2), nullable=True),
        sa.Column('max_price', sa.DECIMAL(10, 2), nullable=True),
        sa.Column('min_quantity', sa.Integer(), nullable=True),
        sa.Column('max_quantity', sa.Integer(), nullable=True),
        sa.Column('additional_filters', sa.JSON(), server_default=sa.text("'{}'::json"), nullable=False),
        sa.ForeignKeyConstraint(
            ['customer_config_id'],
            ['customerpricelistconfig.id'],
            ondelete='CASCADE',
        ),
        sa.ForeignKeyConstraint(
            ['provider_config_id'],
            ['providerpricelistconfig.id'],
        ),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index(
        'ix_customer_pricelist_source_config',
        'customerpricelistsource',
        ['customer_config_id'],
        unique=False,
    )
    op.create_index(
        'ix_customer_pricelist_source_provider_config',
        'customerpricelistsource',
        ['provider_config_id'],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index('ix_customer_pricelist_source_provider_config', table_name='customerpricelistsource')
    op.drop_index('ix_customer_pricelist_source_config', table_name='customerpricelistsource')
    op.drop_table('customerpricelistsource')

    op.drop_index(
        'idx_autopart_price_history_autopart_provider_created_at',
        table_name='autopartpricehistory',
    )
    op.create_index(
        'idx_autopart_price_history_autopart_provider_created_at',
        'autopartpricehistory',
        ['autopart_id', 'provider_id', 'created_at'],
        unique=False,
    )

    op.drop_constraint(
        'autopartpricehistory_provider_config_id_fkey',
        'autopartpricehistory',
        type_='foreignkey',
    )
    op.drop_column('autopartpricehistory', 'provider_config_id')

    op.drop_column('customerpricelistconfig', 'last_sent_at')
    op.drop_column('customerpricelistconfig', 'is_active')
    op.drop_column('customerpricelistconfig', 'emails')
    op.drop_column('customerpricelistconfig', 'schedule_times')
    op.drop_column('customerpricelistconfig', 'schedule_days')

    op.drop_column('provider', 'is_own_price')
