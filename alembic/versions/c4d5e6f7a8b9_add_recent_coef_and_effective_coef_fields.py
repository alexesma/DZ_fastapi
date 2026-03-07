"""add recent client coef and effective coef fields for price control

Revision ID: c4d5e6f7a8b9
Revises: bc3f4d5e6a71
Create Date: 2026-03-06 17:10:00.000000
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'c4d5e6f7a8b9'
down_revision = 'bc3f4d5e6a71'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        'pricecontrolconfig',
        sa.Column('client_markup_recent_coef', sa.JSON(), nullable=True, server_default='[]'),
    )
    op.add_column(
        'pricecontrolrecommendation',
        sa.Column('effective_client_coef', sa.Float(), nullable=True),
    )
    op.add_column(
        'pricecontrolrecommendation',
        sa.Column('effective_client_pct', sa.Float(), nullable=True),
    )


def downgrade():
    op.drop_column('pricecontrolrecommendation', 'effective_client_pct')
    op.drop_column('pricecontrolrecommendation', 'effective_client_coef')
    op.drop_column('pricecontrolconfig', 'client_markup_recent_coef')
