"""Customer contract for UNF sales documents.

Revision ID: b3d5f7a9c268
Revises: a2c4e6f8b157
Create Date: 2026-11-14 12:00:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

revision: str = "b3d5f7a9c268"
down_revision: Union[str, Sequence[str], None] = "a2c4e6f8b157"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "customer",
        sa.Column("one_c_contract_name", sa.String(length=255), nullable=True),
    )
    op.add_column(
        "customer",
        sa.Column("one_c_contract_number", sa.String(length=100), nullable=True),
    )
    op.add_column(
        "customer", sa.Column("one_c_contract_date", sa.Date(), nullable=True)
    )
    op.add_column(
        "customer",
        sa.Column("one_c_contract_guid", sa.String(length=36), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("customer", "one_c_contract_guid")
    op.drop_column("customer", "one_c_contract_date")
    op.drop_column("customer", "one_c_contract_number")
    op.drop_column("customer", "one_c_contract_name")
