"""add site key env and dragonzap toggle to price control config

Revision ID: e6f7a8b9c0d1
Revises: d5e6f7a8b9c0
Create Date: 2026-03-07 10:25:00.000000
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'e6f7a8b9c0d1'
down_revision = 'd5e6f7a8b9c0'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        'pricecontrolconfig',
        sa.Column('site_api_key_env', sa.String(length=128), nullable=True),
    )
    op.add_column(
        'pricecontrolconfig',
        sa.Column(
            'exclude_dragonzap_non_dz',
            sa.Boolean(),
            nullable=True,
            server_default='false',
        ),
    )


def downgrade():
    op.drop_column('pricecontrolconfig', 'exclude_dragonzap_non_dz')
    op.drop_column('pricecontrolconfig', 'site_api_key_env')
