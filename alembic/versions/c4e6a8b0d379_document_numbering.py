"""Own sequential document numbers.

Revision ID: c4e6a8b0d379
Revises: b3d5f7a9c268
Create Date: 2026-11-15 16:00:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

revision: str = "c4e6a8b0d379"
down_revision: Union[str, Sequence[str], None] = "b3d5f7a9c268"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "documentnumbering",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("kind", sa.String(length=40), nullable=False),
        sa.Column(
            "enabled", sa.Boolean(), nullable=False, server_default=sa.false()
        ),
        sa.Column("prefix", sa.String(length=8), nullable=False, server_default=""),
        sa.Column("digits", sa.Integer(), nullable=False, server_default="6"),
        sa.Column("year", sa.Integer(), nullable=False),
        sa.Column("last_value", sa.Integer(), nullable=False, server_default="0"),
        sa.UniqueConstraint("kind", name="uq_documentnumbering_kind"),
    )
    op.add_column(
        "shipmentdocument",
        sa.Column("number_assigned_at", sa.DateTime(timezone=True), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("shipmentdocument", "number_assigned_at")
    op.drop_table("documentnumbering")
