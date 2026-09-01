"""Enforce product multiplicity and keep it in customer pricelist previews.

Revision ID: c8e1f4a6b920
Revises: b7d2e93a5c18
Create Date: 2026-09-01 12:00:00.000000

"""
from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

revision: str = "c8e1f4a6b920"
down_revision: Union[str, None] = "b7d2e93a5c18"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        "UPDATE autopart SET multiplicity = 1 "
        "WHERE multiplicity IS NULL OR multiplicity < 1"
    )
    op.alter_column(
        "autopart",
        "multiplicity",
        existing_type=sa.Integer(),
        nullable=False,
        server_default="1",
    )
    op.add_column(
        "customerpricelistexportrow",
        sa.Column(
            "multiplicity",
            sa.Integer(),
            nullable=False,
            server_default="1",
        ),
    )


def downgrade() -> None:
    op.drop_column("customerpricelistexportrow", "multiplicity")
    op.alter_column(
        "autopart",
        "multiplicity",
        existing_type=sa.Integer(),
        nullable=True,
        server_default=None,
    )
