"""add state profiles for price control coefficients

Revision ID: f7a8b9c0d1e2
Revises: e6f7a8b9c0d1
Create Date: 2026-03-07 11:10:00.000000
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'f7a8b9c0d1e2'
down_revision = 'e6f7a8b9c0d1'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'pricecontrolstateprofile',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column(
            'config_id',
            sa.Integer(),
            sa.ForeignKey('pricecontrolconfig.id'),
            nullable=False,
        ),
        sa.Column('site_api_key_env', sa.String(length=128), nullable=False, server_default=''),
        sa.Column('our_offer_field', sa.String(length=64), nullable=False, server_default=''),
        sa.Column('our_offer_match', sa.String(length=255), nullable=False, server_default=''),
        sa.Column('client_markup_coef', sa.Float(), nullable=True, server_default='1.0'),
        sa.Column('client_markup_sample_size', sa.Integer(), nullable=True, server_default='0'),
        sa.Column('client_markup_recent_coef', sa.JSON(), nullable=True, server_default='[]'),
        sa.Column('cooldown_hours', sa.Integer(), nullable=True, server_default='0'),
        sa.Column('cooldown_reset_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=True),
        sa.UniqueConstraint(
            'config_id',
            'site_api_key_env',
            'our_offer_field',
            'our_offer_match',
            name='uq_pricecontrol_state_profile',
        ),
    )

    op.execute(
        """
        INSERT INTO pricecontrolstateprofile
            (config_id, site_api_key_env, our_offer_field, our_offer_match,
             client_markup_coef, client_markup_sample_size, client_markup_recent_coef,
             cooldown_hours, cooldown_reset_at, created_at, updated_at)
        SELECT
            id,
            COALESCE(site_api_key_env, ''),
            COALESCE(our_offer_field, ''),
            COALESCE(our_offer_match, ''),
            COALESCE(client_markup_coef, 1.0),
            COALESCE(client_markup_sample_size, 0),
            COALESCE(client_markup_recent_coef, '[]'::json),
            COALESCE(cooldown_hours, 0),
            cooldown_reset_at,
            NOW(),
            NOW()
        FROM pricecontrolconfig;
        """
    )


def downgrade():
    op.drop_table('pricecontrolstateprofile')
