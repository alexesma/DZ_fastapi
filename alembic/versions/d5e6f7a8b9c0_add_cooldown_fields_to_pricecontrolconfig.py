"""add cooldown fields to price control config

Revision ID: d5e6f7a8b9c0
Revises: c4d5e6f7a8b9
Create Date: 2026-03-06 19:45:00.000000
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'd5e6f7a8b9c0'
down_revision = 'c4d5e6f7a8b9'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        'pricecontrolconfig',
        sa.Column(
            'cooldown_hours',
            sa.Integer(),
            nullable=True,
            server_default='0',
        ),
    )
    op.add_column(
        'pricecontrolconfig',
        sa.Column('cooldown_reset_at', sa.DateTime(timezone=True), nullable=True),
    )


def downgrade():
    op.drop_column('pricecontrolconfig', 'cooldown_reset_at')
    op.drop_column('pricecontrolconfig', 'cooldown_hours')
