"""add_watchlist_items

Revision ID: 1a7f2b9d4c5e
Revises: 3f2a6b8d9c10
Create Date: 2026-02-21 00:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '1a7f2b9d4c5e'
down_revision = '3f2a6b8d9c10'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'pricewatchitem',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('brand', sa.String(length=255), nullable=False),
        sa.Column('oem', sa.String(length=255), nullable=False),
        sa.Column('max_price', sa.Float(), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('last_seen_provider_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('last_seen_provider_price', sa.Float(), nullable=True),
        sa.Column('last_seen_provider_id', sa.Integer(), nullable=True),
        sa.Column('last_seen_provider_config_id', sa.Integer(), nullable=True),
        sa.Column('last_seen_provider_pricelist_id', sa.Integer(), nullable=True),
        sa.Column('last_seen_site_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('last_seen_site_price', sa.Float(), nullable=True),
        sa.Column('last_notified_provider_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('last_notified_site_at', sa.DateTime(timezone=True), nullable=True),
    )


def downgrade():
    op.drop_table('pricewatchitem')
