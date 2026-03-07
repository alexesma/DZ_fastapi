"""add adaptive client markup coefficient fields to price control

Revision ID: bc3f4d5e6a71
Revises: a8b9c0d1e2f3
Create Date: 2026-03-06 12:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'bc3f4d5e6a71'
down_revision = 'a8b9c0d1e2f3'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        'pricecontrolconfig',
        sa.Column('client_markup_coef', sa.Float(), nullable=True, server_default='1.0'),
    )
    op.add_column(
        'pricecontrolconfig',
        sa.Column('client_markup_sample_size', sa.Integer(), nullable=True, server_default='0'),
    )


def downgrade():
    op.drop_column('pricecontrolconfig', 'client_markup_sample_size')
    op.drop_column('pricecontrolconfig', 'client_markup_coef')
