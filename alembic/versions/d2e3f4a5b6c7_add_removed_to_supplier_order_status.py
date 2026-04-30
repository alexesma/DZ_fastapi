"""Add REMOVED to supplierorderstatus enum

Revision ID: d2e3f4a5b6c7
Revises: c1d2e3f4a5b6
Create Date: 2026-04-30 12:00:00.000000

"""
from typing import Sequence, Union

from alembic import op

revision: str = 'd2e3f4a5b6c7'
down_revision: Union[str, None] = 'c1d2e3f4a5b6'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # PostgreSQL requires COMMIT before ALTER TYPE ADD VALUE
    op.execute("COMMIT")
    op.execute("ALTER TYPE supplierorderstatus ADD VALUE IF NOT EXISTS 'REMOVED'")


def downgrade() -> None:
    # PostgreSQL does not support removing enum values;
    # downgrade is a no-op (value stays but is unused).
    pass
