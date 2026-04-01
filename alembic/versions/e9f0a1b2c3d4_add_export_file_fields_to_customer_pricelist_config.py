"""add export file fields to customer pricelist config

Revision ID: e9f0a1b2c3d4
Revises: b1c2d3e4f5a6
Create Date: 2026-04-01 15:30:00.000000

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = 'e9f0a1b2c3d4'
down_revision: Union[str, None] = 'b1c2d3e4f5a6'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        'customerpricelistconfig',
        sa.Column('export_file_name', sa.String(length=255), nullable=True),
    )
    op.add_column(
        'customerpricelistconfig',
        sa.Column(
            'export_file_format',
            sa.String(length=16),
            nullable=False,
            server_default='xlsx',
        ),
    )
    op.add_column(
        'customerpricelistconfig',
        sa.Column(
            'export_file_extension', sa.String(length=16), nullable=True
        ),
    )


def downgrade() -> None:
    op.drop_column('customerpricelistconfig', 'export_file_extension')
    op.drop_column('customerpricelistconfig', 'export_file_format')
    op.drop_column('customerpricelistconfig', 'export_file_name')
