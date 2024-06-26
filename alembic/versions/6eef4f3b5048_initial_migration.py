"""Initial migration

Revision ID: 6eef4f3b5048
Revises: 197a50163aef
Create Date: 2024-05-14 18:11:55.468347

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '6eef4f3b5048'
down_revision: Union[str, None] = '197a50163aef'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_constraint('unique_brand_synonyms_2', 'brand_synonyms', type_='unique')
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_constraint('unique_brand_synonyms', 'brand_synonyms', type_='unique')
    # ### end Alembic commands ###
