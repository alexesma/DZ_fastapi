"""merge return/diadoc and finance heads

Revision ID: 4ee5ff6aa7bb
Revises: 3dd4ee5ff6aa, 2cc3dd4ee5ff
Create Date: 2026-05-13 10:30:00.000000
"""

from typing import Sequence, Union


revision: str = '4ee5ff6aa7bb'
down_revision: Union[str, Sequence[str], None] = (
    '3dd4ee5ff6aa',
    '2cc3dd4ee5ff',
)
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
