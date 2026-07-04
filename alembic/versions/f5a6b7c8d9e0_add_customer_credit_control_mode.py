"""add customer credit control mode

Revision ID: f5a6b7c8d9e0
Revises: f4a5b6c7d8e9
Create Date: 2026-07-04 12:00:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

revision: str = "f5a6b7c8d9e0"
down_revision: Union[str, Sequence[str], None] = "f4a5b6c7d8e9"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "customer",
        sa.Column(
            "credit_control_mode",
            sa.String(length=16),
            nullable=False,
            server_default=sa.text("'off'"),
        ),
    )


def downgrade() -> None:
    op.drop_column("customer", "credit_control_mode")
