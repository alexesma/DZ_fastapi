"""Chenge function delete to delete-orhan

Revision ID: 04da60905cf7
Revises: 102fc3bf7eb3
Create Date: 2024-08-15 21:17:01.110523

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '04da60905cf7'
down_revision: Union[str, None] = '102fc3bf7eb3'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_constraint('unique_brand_synonyms_v3', 'brand_synonyms', type_='unique')
    op.create_unique_constraint('unique_brand_synonyms_v4', 'brand_synonyms', ['brand_id', 'synonym_id'])
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_constraint('unique_brand_synonyms_v4', 'brand_synonyms', type_='unique')
    op.create_unique_constraint('unique_brand_synonyms_v3', 'brand_synonyms', ['brand_id', 'synonym_id'])
    # ### end Alembic commands ###
