"""Add incoming_email_account_id to providerpricelistconfig

Revision ID: b4e2d1c9f7aa
Revises: 9d24c7f1b2a3
Create Date: 2026-03-13 00:00:02.000000

"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = 'b4e2d1c9f7aa'
down_revision: Union[str, None] = '9d24c7f1b2a3'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        'providerpricelistconfig',
        sa.Column('incoming_email_account_id', sa.Integer(), nullable=True),
    )
    op.create_foreign_key(
        'fk_providerpricelistconfig_incoming_email_account_id',
        'providerpricelistconfig',
        'emailaccount',
        ['incoming_email_account_id'],
        ['id'],
    )


def downgrade() -> None:
    op.drop_constraint(
        'fk_providerpricelistconfig_incoming_email_account_id',
        'providerpricelistconfig',
        type_='foreignkey',
    )
    op.drop_column('providerpricelistconfig', 'incoming_email_account_id')
