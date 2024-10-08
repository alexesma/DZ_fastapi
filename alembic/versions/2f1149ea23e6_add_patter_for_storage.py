"""add patter for storage

Revision ID: 2f1149ea23e6
Revises: 87c207d08bc0
Create Date: 2024-10-02 15:55:54.965685

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '2f1149ea23e6'
down_revision: Union[str, None] = '87c207d08bc0'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_constraint('latin_characters_only', 'storagelocation', type_='check')

    op.create_check_constraint(
        'latin_characters_only',
        'storagelocation',
        "name ~ '^[A-Z0-9 ]+$'"
    )
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_constraint('latin_characters_only', 'storagelocation', type_='check')
    op.create_check_constraint(
        'latin_characters_only',
        'storagelocation',
        "name ~ '^[A-Z0-9]+$'"
    )
    # ### end Alembic commands ###
