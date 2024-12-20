"""Change description for Client

Revision ID: 4a5508391712
Revises: 016d634e658a
Create Date: 2024-11-01 19:04:07.093647

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '4a5508391712'
down_revision: Union[str, None] = '016d634e658a'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_unique_constraint(None, 'customer', ['id'])
    op.create_unique_constraint(None, 'provider', ['id'])
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_constraint(None, 'provider', type_='unique')
    op.drop_constraint(None, 'customer', type_='unique')
    # ### end Alembic commands ###
