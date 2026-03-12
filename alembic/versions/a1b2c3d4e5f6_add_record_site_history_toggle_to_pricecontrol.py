"""add toggle for saving site history in price control

Revision ID: a1b2c3d4e5f6
Revises: f7a8b9c0d1e2
Create Date: 2026-03-12 16:10:00.000000
"""

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = 'a1b2c3d4e5f6'
down_revision = 'f7a8b9c0d1e2'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        'pricecontrolconfig',
        sa.Column(
            'record_site_history_for_dz',
            sa.Boolean(),
            nullable=True,
            server_default='false',
        ),
    )


def downgrade():
    op.drop_column('pricecontrolconfig', 'record_site_history_for_dz')
