"""add supplier response file format fields

Revision ID: cb8f1a2d3e45
Revises: ca7d9e1b2f34
Create Date: 2026-04-08 13:40:00.000000
"""

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = 'cb8f1a2d3e45'
down_revision = 'ca7d9e1b2f34'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        'provider',
        sa.Column('supplier_response_filename_pattern', sa.String(length=255), nullable=True),
    )
    op.add_column(
        'provider',
        sa.Column(
            'supplier_shipping_doc_filename_pattern',
            sa.String(length=255),
            nullable=True,
        ),
    )
    op.add_column(
        'provider',
        sa.Column(
            'supplier_response_start_row',
            sa.Integer(),
            nullable=False,
            server_default='1',
        ),
    )
    op.add_column(
        'provider',
        sa.Column('supplier_response_oem_col', sa.Integer(), nullable=True),
    )
    op.add_column(
        'provider',
        sa.Column('supplier_response_brand_col', sa.Integer(), nullable=True),
    )
    op.add_column(
        'provider',
        sa.Column('supplier_response_qty_col', sa.Integer(), nullable=True),
    )
    op.add_column(
        'provider',
        sa.Column('supplier_response_price_col', sa.Integer(), nullable=True),
    )
    op.add_column(
        'provider',
        sa.Column('supplier_response_comment_col', sa.Integer(), nullable=True),
    )
    op.add_column(
        'provider',
        sa.Column('supplier_response_status_col', sa.Integer(), nullable=True),
    )
    op.alter_column(
        'provider',
        'supplier_response_start_row',
        server_default=None,
    )


def downgrade() -> None:
    op.drop_column('provider', 'supplier_response_status_col')
    op.drop_column('provider', 'supplier_response_comment_col')
    op.drop_column('provider', 'supplier_response_price_col')
    op.drop_column('provider', 'supplier_response_qty_col')
    op.drop_column('provider', 'supplier_response_brand_col')
    op.drop_column('provider', 'supplier_response_oem_col')
    op.drop_column('provider', 'supplier_response_start_row')
    op.drop_column('provider', 'supplier_shipping_doc_filename_pattern')
    op.drop_column('provider', 'supplier_response_filename_pattern')
