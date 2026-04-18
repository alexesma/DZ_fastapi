"""Add subject_pattern to supplier response config

Revision ID: 2d7e9f1a3b4c
Revises: 1fd35aab7c9d
Create Date: 2026-04-18 17:20:00.000000
"""
from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "2d7e9f1a3b4c"
down_revision: Union[str, None] = "1fd35aab7c9d"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "supplierresponseconfig",
        sa.Column("subject_pattern", sa.String(length=255), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("supplierresponseconfig", "subject_pattern")
