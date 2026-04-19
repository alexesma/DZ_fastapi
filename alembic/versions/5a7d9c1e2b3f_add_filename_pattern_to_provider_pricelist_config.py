"""add filename_pattern to provider pricelist config

Revision ID: 5a7d9c1e2b3f
Revises: 2b4d6f8a9c1e, 2d7e9f1a3b4c
Create Date: 2026-04-19 18:40:00.000000
"""
from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "5a7d9c1e2b3f"
down_revision: Union[str, Sequence[str], None] = (
    "2b4d6f8a9c1e",
    "2d7e9f1a3b4c",
)
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "providerpricelistconfig",
        sa.Column("filename_pattern", sa.String(length=255), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("providerpricelistconfig", "filename_pattern")
