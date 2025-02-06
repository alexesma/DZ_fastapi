"""Change connecrion Provider and Pricelist for one-to-many

Revision ID: 2731df8a4738
Revises: 2ea0706f43ec
Create Date: 2025-02-02 16:05:31.761142

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '2731df8a4738'
down_revision: Union[str, None] = '2ea0706f43ec'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_constraint('providerpricelistconfig_provider_id_key', 'providerpricelistconfig', type_='unique')
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_unique_constraint('providerpricelistconfig_provider_id_key', 'providerpricelistconfig', ['provider_id'])
    # ### end Alembic commands ###
