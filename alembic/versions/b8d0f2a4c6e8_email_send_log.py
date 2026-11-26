"""email send log

Revision ID: b8d0f2a4c6e8
Revises: a7c9e1f3b5d7
Create Date: 2026-11-26 15:00:00.000000
"""

import sqlalchemy as sa
from alembic import op

revision = "b8d0f2a4c6e8"
down_revision = "a7c9e1f3b5d7"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "emailsendlog",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("to_email", sa.String(length=1000), nullable=False),
        sa.Column("from_email", sa.String(length=255), nullable=True),
        sa.Column("subject", sa.String(length=998), nullable=True),
        sa.Column("transport", sa.String(length=32), nullable=True),
        sa.Column("source", sa.String(length=64), nullable=True),
        sa.Column("source_id", sa.Integer(), nullable=True),
        sa.Column("attachment_name", sa.String(length=255), nullable=True),
        sa.Column("attachment_size", sa.Integer(), nullable=True),
        sa.Column("status", sa.String(length=16), nullable=False, server_default="sent"),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("next_retry_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("sent_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("events", sa.JSON(), nullable=True),
        sa.Column("retry_allowed", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("auto_retry", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("is_html", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("body", sa.Text(), nullable=True),
        sa.Column("attachment_path", sa.String(length=1000), nullable=True),
        sa.Column("duration_ms", sa.Integer(), nullable=True),
    )
    op.create_index("ix_emailsendlog_created_at", "emailsendlog", ["created_at"])
    op.create_index("ix_emailsendlog_source", "emailsendlog", ["source"])
    op.create_index("ix_emailsendlog_source_id", "emailsendlog", ["source_id"])
    op.create_index("ix_emailsendlog_status", "emailsendlog", ["status"])
    op.create_index("ix_emailsendlog_next_retry_at", "emailsendlog", ["next_retry_at"])


def downgrade() -> None:
    op.drop_table("emailsendlog")
