"""add auto_refused_at to supplier order item

Revision ID: d8e9f0a1b2c3
Revises: 6c4d2e1f9a8b
Create Date: 2026-04-23 10:00:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "d8e9f0a1b2c3"
down_revision: Union[str, Sequence[str], None] = "6c4d2e1f9a8b"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "supplierorderitem",
        sa.Column("auto_refused_at", sa.DateTime(timezone=True), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("supplierorderitem", "auto_refused_at")
