"""Merge heads 1a7f2b9d4c5e and 7a3c9d2b1e4f

Revision ID: 9d2c7b1e4f6a
Revises: 1a7f2b9d4c5e, 7a3c9d2b1e4f
Create Date: 2026-02-26 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = '9d2c7b1e4f6a'
down_revision: Union[str, None] = ('1a7f2b9d4c5e', '7a3c9d2b1e4f')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
