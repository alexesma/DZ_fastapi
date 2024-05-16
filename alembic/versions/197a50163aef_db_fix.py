"""db fix

Revision ID: 197a50163aef
Revises: b125d65c004a
Create Date: 2024-05-08 12:22:48.730419

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '197a50163aef'
down_revision: Union[str, None] = 'b125d65c004a'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_unique_constraint('unique_brand_synonyms_2', 'brand_synonyms', ['brand_id', 'synonym_id'])
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_constraint('unique_brand_synonyms_2', 'brand_synonyms', type_='unique')
    # ### end Alembic commands ###