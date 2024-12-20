"""Add for name CustomerPriceListConfig get True

Revision ID: 85607d9739ca
Revises: 6f63ef2b3a28
Create Date: 2024-11-19 17:06:28.374813

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '85607d9739ca'
down_revision: Union[str, None] = '6f63ef2b3a28'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_unique_constraint(None, 'customerpricelistconfig', ['name'])
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_constraint(None, 'customerpricelistconfig', type_='unique')
    # ### end Alembic commands ###