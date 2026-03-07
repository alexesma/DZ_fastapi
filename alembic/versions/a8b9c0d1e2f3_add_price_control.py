"""add price control tables

Revision ID: a8b9c0d1e2f3
Revises: f1e2d3c4b5a6
Create Date: 2026-03-03 00:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'a8b9c0d1e2f3'
down_revision = 'f1e2d3c4b5a6'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'customerpricelistoverride',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column(
            'config_id',
            sa.Integer(),
            sa.ForeignKey('customerpricelistconfig.id'),
            nullable=False,
        ),
        sa.Column(
            'autopart_id',
            sa.Integer(),
            sa.ForeignKey('autopart.id'),
            nullable=False,
        ),
        sa.Column('price', sa.Float(), nullable=False),
        sa.Column('is_active', sa.Boolean(), server_default='true'),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=True),
        sa.UniqueConstraint(
            'config_id', 'autopart_id', name='uq_pricelist_override_item'
        ),
    )

    op.create_table(
        'pricecontrolconfig',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column(
            'customer_id',
            sa.Integer(),
            sa.ForeignKey('customer.id'),
            nullable=False,
        ),
        sa.Column(
            'pricelist_config_id',
            sa.Integer(),
            sa.ForeignKey('customerpricelistconfig.id'),
            nullable=False,
        ),
        sa.Column('is_active', sa.Boolean(), server_default='true'),
        sa.Column('total_daily_count', sa.Integer(), server_default='100'),
        sa.Column('schedule_days', sa.JSON(), nullable=True),
        sa.Column('schedule_times', sa.JSON(), nullable=True),
        sa.Column('min_stock', sa.Integer(), nullable=True),
        sa.Column('max_delivery_days', sa.Integer(), nullable=True),
        sa.Column('delta_pct', sa.Float(), server_default='0.2'),
        sa.Column('target_cheapest_pct', sa.Float(), server_default='60'),
        sa.Column('our_offer_field', sa.String(length=64), nullable=True),
        sa.Column('our_offer_match', sa.String(length=255), nullable=True),
        sa.Column(
            'own_cost_markup_default', sa.Float(), server_default='20'
        ),
        sa.Column('own_cost_markup_by_brand', sa.JSON(), nullable=True),
        sa.Column('last_run_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=True),
    )

    op.create_table(
        'pricecontrolsource',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column(
            'config_id',
            sa.Integer(),
            sa.ForeignKey('pricecontrolconfig.id'),
            nullable=False,
        ),
        sa.Column(
            'provider_config_id',
            sa.Integer(),
            sa.ForeignKey('providerpricelistconfig.id'),
            nullable=False,
        ),
        sa.Column('weight_pct', sa.Float(), server_default='0'),
        sa.Column('min_markup_pct', sa.Float(), server_default='0'),
        sa.Column('locked', sa.Boolean(), server_default='false'),
        sa.UniqueConstraint(
            'config_id',
            'provider_config_id',
            name='uq_pricecontrol_source',
        ),
    )

    op.create_table(
        'pricecontrolmanualitem',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column(
            'config_id',
            sa.Integer(),
            sa.ForeignKey('pricecontrolconfig.id'),
            nullable=False,
        ),
        sa.Column('oem', sa.String(length=255), nullable=False),
        sa.Column('brand', sa.String(length=255), nullable=False),
        sa.UniqueConstraint(
            'config_id', 'oem', 'brand', name='uq_pricecontrol_manual_item'
        ),
    )

    op.create_table(
        'pricecontrolrun',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column(
            'config_id',
            sa.Integer(),
            sa.ForeignKey('pricecontrolconfig.id'),
            nullable=False,
        ),
        sa.Column('run_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('status', sa.String(length=32), nullable=True),
        sa.Column('total_items', sa.Integer(), server_default='0'),
    )

    op.create_table(
        'pricecontrolrecommendation',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column(
            'run_id',
            sa.Integer(),
            sa.ForeignKey('pricecontrolrun.id'),
            nullable=False,
        ),
        sa.Column('provider_config_id', sa.Integer(), nullable=True),
        sa.Column(
            'autopart_id',
            sa.Integer(),
            sa.ForeignKey('autopart.id'),
            nullable=True,
        ),
        sa.Column('oem', sa.String(length=255), nullable=False),
        sa.Column('brand', sa.String(length=255), nullable=False),
        sa.Column('name', sa.String(length=255), nullable=True),
        sa.Column('our_price', sa.Float(), nullable=True),
        sa.Column('competitor_price', sa.Float(), nullable=True),
        sa.Column('competitor_qty', sa.Integer(), nullable=True),
        sa.Column('competitor_supplier', sa.String(length=255), nullable=True),
        sa.Column('competitor_min_delivery', sa.Integer(), nullable=True),
        sa.Column('competitor_max_delivery', sa.Integer(), nullable=True),
        sa.Column('target_price', sa.Float(), nullable=True),
        sa.Column('cost_price', sa.Float(), nullable=True),
        sa.Column('min_allowed_price', sa.Float(), nullable=True),
        sa.Column('is_cheapest', sa.Boolean(), server_default='false'),
        sa.Column('below_min_markup', sa.Boolean(), server_default='false'),
        sa.Column('below_cost', sa.Boolean(), server_default='false'),
        sa.Column('missing_competitor', sa.Boolean(), server_default='false'),
        sa.Column('missing_in_pricelist', sa.Boolean(), server_default='false'),
        sa.Column('suggested_action', sa.String(length=32), nullable=True),
    )

    op.create_table(
        'pricecontrolsource_reco',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column(
            'run_id',
            sa.Integer(),
            sa.ForeignKey('pricecontrolrun.id'),
            nullable=False,
        ),
        sa.Column(
            'provider_config_id',
            sa.Integer(),
            sa.ForeignKey('providerpricelistconfig.id'),
            nullable=False,
        ),
        sa.Column('current_markup_pct', sa.Float(), nullable=True),
        sa.Column('suggested_markup_pct', sa.Float(), nullable=True),
        sa.Column('coverage_pct', sa.Float(), nullable=True),
        sa.Column('sample_size', sa.Integer(), server_default='0'),
        sa.Column('note', sa.String(length=255), nullable=True),
        sa.UniqueConstraint(
            'run_id',
            'provider_config_id',
            name='uq_pricecontrol_source_reco',
        ),
    )


def downgrade():
    op.drop_table('pricecontrolsource_reco')
    op.drop_table('pricecontrolrecommendation')
    op.drop_table('pricecontrolrun')
    op.drop_table('pricecontrolmanualitem')
    op.drop_table('pricecontrolsource')
    op.drop_table('pricecontrolconfig')
    op.drop_table('customerpricelistoverride')
