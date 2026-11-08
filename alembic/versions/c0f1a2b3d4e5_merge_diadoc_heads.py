"""Merge Diadoc migration heads.

Revision ID: c0f1a2b3d4e5
Revises: b9d2e4f6a8c1, b9d2e5f8a031
Create Date: 2026-11-08 18:00:00.000000
"""

from typing import Sequence, Union

revision: str = "c0f1a2b3d4e5"
down_revision: Union[str, Sequence[str], None] = (
    "b9d2e4f6a8c1",
    "b9d2e5f8a031",
)
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
