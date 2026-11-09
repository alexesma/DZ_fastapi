"""Add safe 1C activation mode and reversible event quarantine.

Revision ID: f0b1c2d3e4a5
Revises: d1a2b3c4e5f6
Create Date: 2026-11-08 18:30:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "f0b1c2d3e4a5"
down_revision: Union[str, Sequence[str], None] = "d1a2b3c4e5f6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "onecexchangeevent",
        sa.Column("ed_excluded_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "onecexchangeevent",
        sa.Column("ed_exclusion_reason", sa.Text(), nullable=True),
    )
    op.create_index(
        op.f("ix_onecexchangeevent_ed_excluded_at"),
        "onecexchangeevent",
        ["ed_excluded_at"],
        unique=False,
    )
    op.add_column(
        "oncenterpriseconfig",
        sa.Column(
            "activation_mode",
            sa.String(length=20),
            nullable=False,
            server_default="new",
        ),
    )


def downgrade() -> None:
    op.drop_column("oncenterpriseconfig", "activation_mode")
    op.drop_index(
        op.f("ix_onecexchangeevent_ed_excluded_at"),
        table_name="onecexchangeevent",
    )
    op.drop_column("onecexchangeevent", "ed_exclusion_reason")
    op.drop_column("onecexchangeevent", "ed_excluded_at")
