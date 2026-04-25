"""add honest_sign_category and applicability to autopart

Revision ID: f1a2b3c4d5e6
Revises: f4e5d6c7b8a9
Create Date: 2026-04-25 12:00:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

revision: str = "f1a2b3c4d5e6"
down_revision: Union[str, Sequence[str], None] = "f4e5d6c7b8a9"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "autopart",
        sa.Column("honest_sign_category", sa.String(100), nullable=True),
    )
    op.add_column(
        "autopart",
        sa.Column("applicability", sa.Text(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("autopart", "applicability")
    op.drop_column("autopart", "honest_sign_category")
