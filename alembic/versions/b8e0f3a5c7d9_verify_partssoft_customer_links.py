"""Distinguish verified Parts-Soft customer links.

Revision ID: b8e0f3a5c7d9
Revises: a7d9e2f4b6c8
Create Date: 2026-09-10 10:00:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

revision: str = "b8e0f3a5c7d9"
down_revision: Union[str, Sequence[str], None] = "a7d9e2f4b6c8"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "customerexternalreference",
        sa.Column(
            "is_verified",
            sa.Boolean(),
            server_default=sa.false(),
            nullable=False,
        ),
    )
    op.add_column(
        "customerexternalreference",
        sa.Column("match_basis", sa.String(64), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("customerexternalreference", "match_basis")
    op.drop_column("customerexternalreference", "is_verified")
