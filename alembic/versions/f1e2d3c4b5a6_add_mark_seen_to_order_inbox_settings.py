"""add mark seen to order inbox settings

Revision ID: f1e2d3c4b5a6
Revises: e2f3a4b5c6d7
Create Date: 2026-02-28 00:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'f1e2d3c4b5a6'
down_revision = 'e2f3a4b5c6d7'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        'customerorderinboxsettings',
        sa.Column('mark_seen', sa.Boolean(), server_default='false'),
    )


def downgrade():
    op.drop_column('customerorderinboxsettings', 'mark_seen')
