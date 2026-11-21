"""indexes on inbox email dates

Revision ID: a8c0d2e4f6b7
Revises: f6a8b0c2d4e5
Create Date: 2026-11-21 23:00:00.000000
"""

from alembic import op

revision = "a8c0d2e4f6b7"
down_revision = "f6a8b0c2d4e5"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_index(
        "ix_inboxemail_received_at", "inboxemail", ["received_at"], unique=False
    )
    op.create_index(
        "ix_inboxemail_fetched_at", "inboxemail", ["fetched_at"], unique=False
    )


def downgrade() -> None:
    op.drop_index("ix_inboxemail_fetched_at", table_name="inboxemail")
    op.drop_index("ix_inboxemail_received_at", table_name="inboxemail")
