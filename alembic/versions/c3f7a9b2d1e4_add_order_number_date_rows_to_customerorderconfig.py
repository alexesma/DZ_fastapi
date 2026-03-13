"""Add order number/date row fields to customerorderconfig

Revision ID: c3f7a9b2d1e4
Revises: b4e2d1c9f7aa
Create Date: 2026-03-13 00:10:00.000000

"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = 'c3f7a9b2d1e4'
down_revision: Union[str, None] = 'b4e2d1c9f7aa'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        'customerorderconfig',
        sa.Column('order_number_row', sa.Integer(), nullable=True),
    )
    op.add_column(
        'customerorderconfig',
        sa.Column('order_date_row', sa.Integer(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column('customerorderconfig', 'order_date_row')
    op.drop_column('customerorderconfig', 'order_number_row')
