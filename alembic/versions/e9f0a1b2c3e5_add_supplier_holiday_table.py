"""add supplier_holiday table

Revision ID: e9f0a1b2c3e5
Revises: d8e9f0a1b2c3
Create Date: 2026-04-23 11:00:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "e9f0a1b2c3e5"
down_revision: Union[str, Sequence[str], None] = "d8e9f0a1b2c3"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "supplierholiday",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("date", sa.Date(), nullable=False),
        sa.Column("description", sa.String(255), nullable=True),
        sa.Column(
            "is_working_day",
            sa.Boolean(),
            nullable=False,
            server_default="false",
        ),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_supplierholiday_date",
        "supplierholiday",
        ["date"],
        unique=True,
    )


def downgrade() -> None:
    op.drop_index("ix_supplierholiday_date", table_name="supplierholiday")
    op.drop_table("supplierholiday")
