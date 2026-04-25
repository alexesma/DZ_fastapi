"""merge supplier response cleanup and autopart metadata branches

Revision ID: b2c3d4e5f6a7
Revises: f4e5d6c7b8a9, a2b3c4d5e6f8
Create Date: 2026-04-25 20:15:00.000000
"""

from typing import Sequence, Union

# revision identifiers, used by Alembic.
revision: str = "b2c3d4e5f6a7"
down_revision: Union[str, Sequence[str], None] = (
    "f4e5d6c7b8a9",
    "a2b3c4d5e6f8",
)
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
