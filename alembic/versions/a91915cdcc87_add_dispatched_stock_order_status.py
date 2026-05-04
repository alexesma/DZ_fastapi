"""Add DISPATCHED status to stockorderstatus enum

Revision ID: a91915cdcc87
Revises: a9b0c1d2e3f4
Create Date: 2026-04-30 12:30:00.000000

"""
from typing import Sequence, Union

from alembic import op

revision: str = 'a91915cdcc87'
down_revision: Union[str, None] = 'a9b0c1d2e3f4'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        "ALTER TYPE stockorderstatus ADD VALUE IF NOT EXISTS 'DISPATCHED'"
    )


def downgrade() -> None:
    # PostgreSQL does not support removing enum values
    pass
