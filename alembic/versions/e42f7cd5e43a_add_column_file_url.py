"""Add column file_url

Revision ID: e42f7cd5e43a
Revises: 22d3e2b9aac9
Create Date: 2025-02-07 16:34:38.193196

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'e42f7cd5e43a'
down_revision: Union[str, None] = '22d3e2b9aac9'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('providerpricelistconfig', sa.Column('file_url', sa.String(), nullable=True))
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('providerpricelistconfig', 'file_url')
    # ### end Alembic commands ###
