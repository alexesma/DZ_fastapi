"""Add default values to CustomerPriceListConfig

Revision ID: 6f63ef2b3a28
Revises: 51e6113c540c
Create Date: 2024-11-19 12:11:48.561307

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '6f63ef2b3a28'
down_revision: Union[str, None] = '51e6113c540c'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    # Добавляем значения по умолчанию для существующих столбцов
    op.alter_column('customerpricelistconfig', 'general_markup', server_default='0.0')
    op.alter_column('customerpricelistconfig', 'own_price_list_markup', server_default='0.0')
    op.alter_column('customerpricelistconfig', 'third_party_markup', server_default='0.0')
    op.alter_column('customerpricelistconfig', 'individual_markups', server_default="{}")
    op.alter_column('customerpricelistconfig', 'brand_filters', server_default="[]")
    op.alter_column('customerpricelistconfig', 'category_filter', server_default="[]")
    op.alter_column('customerpricelistconfig', 'price_intervals', server_default="[]")
    op.alter_column('customerpricelistconfig', 'position_filters', server_default="[]")
    op.alter_column('customerpricelistconfig', 'supplier_quantity_filters', server_default="[]")
    op.alter_column('customerpricelistconfig', 'additional_filters', server_default="{}")
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.alter_column('customerpricelistconfig', 'general_markup', server_default=None)
    op.alter_column('customerpricelistconfig', 'own_price_list_markup', server_default=None)
    op.alter_column('customerpricelistconfig', 'third_party_markup', server_default=None)
    op.alter_column('customerpricelistconfig', 'individual_markups', server_default=None)
    op.alter_column('customerpricelistconfig', 'brand_filters', server_default=None)
    op.alter_column('customerpricelistconfig', 'category_filter', server_default=None)
    op.alter_column('customerpricelistconfig', 'price_intervals', server_default=None)
    op.alter_column('customerpricelistconfig', 'position_filters', server_default=None)
    op.alter_column('customerpricelistconfig', 'supplier_quantity_filters', server_default=None)
    op.alter_column('customerpricelistconfig', 'additional_filters', server_default=None)
    # ### end Alembic commands ###
