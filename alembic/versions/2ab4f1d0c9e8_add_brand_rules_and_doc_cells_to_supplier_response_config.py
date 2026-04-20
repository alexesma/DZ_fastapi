"""Add brand rules and document cell mapping to supplier response config

Revision ID: 2ab4f1d0c9e8
Revises: 1fd35aab7c9d
Create Date: 2026-04-20 11:30:00.000000

"""
from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = '2ab4f1d0c9e8'
down_revision: Union[str, None] = '1fd35aab7c9d'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        'supplierresponseconfig',
        sa.Column('name_col', sa.Integer(), nullable=True),
    )
    op.add_column(
        'supplierresponseconfig',
        sa.Column('fixed_brand_name', sa.String(length=120), nullable=True),
    )
    op.add_column(
        'supplierresponseconfig',
        sa.Column('brand_priority_list', sa.JSON(), nullable=True),
    )
    op.add_column(
        'supplierresponseconfig',
        sa.Column(
            'brand_from_name_regex',
            sa.String(length=255),
            nullable=True,
        ),
    )
    op.add_column(
        'supplierresponseconfig',
        sa.Column('document_number_cell', sa.String(length=16), nullable=True),
    )
    op.add_column(
        'supplierresponseconfig',
        sa.Column('document_date_cell', sa.String(length=16), nullable=True),
    )
    op.add_column(
        'supplierresponseconfig',
        sa.Column('document_meta_cell', sa.String(length=16), nullable=True),
    )


def downgrade() -> None:
    op.drop_column('supplierresponseconfig', 'document_meta_cell')
    op.drop_column('supplierresponseconfig', 'document_date_cell')
    op.drop_column('supplierresponseconfig', 'document_number_cell')
    op.drop_column('supplierresponseconfig', 'brand_from_name_regex')
    op.drop_column('supplierresponseconfig', 'brand_priority_list')
    op.drop_column('supplierresponseconfig', 'fixed_brand_name')
    op.drop_column('supplierresponseconfig', 'name_col')
