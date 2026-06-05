"""expand autopurchase run item text columns

Revision ID: da7e8c9f0b1a
Revises: 6b7c8d9e0f1a, b2d3e4f5a6b7
Create Date: 2026-06-05 11:20:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "da7e8c9f0b1a"
down_revision: Union[str, Sequence[str], None] = (
    "6b7c8d9e0f1a",
    "b2d3e4f5a6b7",
)
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.alter_column(
        "autopurchaserunitem",
        "oem_number",
        existing_type=sa.String(length=64),
        type_=sa.String(length=256),
        existing_nullable=False,
    )
    op.alter_column(
        "autopurchaserunitem",
        "autopart_name",
        existing_type=sa.String(length=64),
        type_=sa.String(length=256),
        existing_nullable=True,
    )


def downgrade() -> None:
    op.alter_column(
        "autopurchaserunitem",
        "autopart_name",
        existing_type=sa.String(length=256),
        type_=sa.String(length=64),
        existing_nullable=True,
    )
    op.alter_column(
        "autopurchaserunitem",
        "oem_number",
        existing_type=sa.String(length=256),
        type_=sa.String(length=64),
        existing_nullable=False,
    )
