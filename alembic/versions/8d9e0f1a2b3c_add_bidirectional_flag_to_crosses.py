"""add bidirectional flag to crosses

Revision ID: 8d9e0f1a2b3c
Revises: 7c8d9e0f1a2b
Create Date: 2026-05-23 11:10:00.000000

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "8d9e0f1a2b3c"
down_revision: Union[str, Sequence[str], None] = "7c8d9e0f1a2b"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "autopartcross",
        sa.Column(
            "is_bidirectional",
            sa.Boolean(),
            nullable=False,
            server_default=sa.true(),
        ),
    )


def downgrade() -> None:
    op.drop_column("autopartcross", "is_bidirectional")
