"""add customer order inbox settings

Revision ID: c5d1a2b3c4d5
Revises: b4c6e7a1d2f3
Create Date: 2026-02-28 00:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'c5d1a2b3c4d5'
down_revision = 'b4c6e7a1d2f3'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'customerorderinboxsettings',
        sa.Column('lookback_days', sa.Integer(), server_default='1'),
        sa.Column(
            'updated_at', sa.DateTime(timezone=True), nullable=True
        ),
        sa.Column('id', sa.Integer(), primary_key=True),
    )


def downgrade():
    op.drop_table('customerorderinboxsettings')
