"""supplier order send attempts and alert marker

Revision ID: a7c9e1f3b5d7
Revises: e2a4b6c8d0f1
Create Date: 2026-11-26 12:00:00.000000
"""

import sqlalchemy as sa
from alembic import op

revision = "a7c9e1f3b5d7"
down_revision = "e2a4b6c8d0f1"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "supplierorder",
        sa.Column("send_attempts", sa.Integer(), nullable=False, server_default="0"),
    )
    op.add_column(
        "supplierorder",
        sa.Column("send_alerted_at", sa.DateTime(timezone=True), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("supplierorder", "send_alerted_at")
    op.drop_column("supplierorder", "send_attempts")
