"""add auto confirm timeout minutes to supplier response config

Revision ID: c7d8e9f0a1b2
Revises: b6c7d8e9f1a2
Create Date: 2026-04-11 16:45:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "c7d8e9f0a1b2"
down_revision: Union[str, Sequence[str], None] = "b6c7d8e9f1a2"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "supplierresponseconfig",
        sa.Column("auto_confirm_after_minutes", sa.Integer(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("supplierresponseconfig", "auto_confirm_after_minutes")
