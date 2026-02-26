"""Merge heads 3f8b2c1d9a7e and 6c9d8b7a2e1f

Revision ID: 7a3c9d2b1e4f
Revises: 3f8b2c1d9a7e, 6c9d8b7a2e1f
Create Date: 2026-02-26 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = '7a3c9d2b1e4f'
down_revision: Union[str, None] = ('3f8b2c1d9a7e', '6c9d8b7a2e1f')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
