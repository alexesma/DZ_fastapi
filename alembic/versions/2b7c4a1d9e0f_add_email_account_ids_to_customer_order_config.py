"""Add email_account_ids to customer order config

Revision ID: 2b7c4a1d9e0f
Revises: 2ab4f1d0c9e8
Create Date: 2026-04-21 17:10:00.000000

"""
from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "2b7c4a1d9e0f"
down_revision: Union[str, None] = "2ab4f1d0c9e8"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "customerorderconfig",
        sa.Column("email_account_ids", sa.JSON(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("customerorderconfig", "email_account_ids")
