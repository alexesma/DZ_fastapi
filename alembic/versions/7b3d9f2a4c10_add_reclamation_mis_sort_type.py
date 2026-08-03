"""add reclamation mis-sort type

Revision ID: 7b3d9f2a4c10
Revises: 6a2c8e4f1b90
Create Date: 2026-08-02 19:45:00.000000
"""

from typing import Sequence, Union

from alembic import op

revision: str = "7b3d9f2a4c10"
down_revision: Union[str, Sequence[str], None] = "6a2c8e4f1b90"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        "ALTER TYPE reclamationtype "
        "ADD VALUE IF NOT EXISTS 'mis_sort'"
    )


def downgrade() -> None:
    # PostgreSQL enum values cannot be removed safely while rows may use them.
    pass
