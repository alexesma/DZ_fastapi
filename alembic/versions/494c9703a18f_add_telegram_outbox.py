"""add telegram outbox

Revision ID: 494c9703a18f
Revises: aa77bb88cc99
Create Date: 2026-08-02 12:04:09.376439

"""
from typing import Sequence, Union

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "494c9703a18f"
down_revision: Union[str, None] = "aa77bb88cc99"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


telegram_outbox_status = postgresql.ENUM(
    "pending",
    "sent",
    "error",
    "cancelled",
    name="telegramoutboxstatus",
    create_type=False,
)


def upgrade() -> None:
    bind = op.get_bind()
    telegram_outbox_status.create(bind, checkfirst=True)
    op.create_table(
        "telegramoutbox",
        sa.Column("status", telegram_outbox_status, nullable=False),
        sa.Column("chat_id", sa.String(length=255), nullable=False),
        sa.Column("text", sa.Text(), nullable=True),
        sa.Column("parse_mode", sa.String(length=32), nullable=True),
        sa.Column("document_name", sa.String(length=512), nullable=True),
        sa.Column("document_path", sa.String(length=1024), nullable=True),
        sa.Column(
            "document_content_type",
            sa.String(length=255),
            nullable=True,
        ),
        sa.Column("caption", sa.Text(), nullable=True),
        sa.Column("source_type", sa.String(length=64), nullable=True),
        sa.Column("source_id", sa.Integer(), nullable=True),
        sa.Column(
            "attempts",
            sa.Integer(),
            nullable=False,
            server_default="0",
        ),
        sa.Column("last_error", sa.String(length=2000), nullable=True),
        sa.Column("sent_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("claimed_by", sa.String(length=128), nullable=True),
        sa.Column("claimed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("id", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_telegramoutbox_status",
        "telegramoutbox",
        ["status"],
    )
    op.create_index(
        "ix_telegramoutbox_source_type",
        "telegramoutbox",
        ["source_type"],
    )
    op.create_index(
        "ix_telegramoutbox_source_id",
        "telegramoutbox",
        ["source_id"],
    )
    op.create_index(
        "ix_telegramoutbox_claimed_at",
        "telegramoutbox",
        ["claimed_at"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_telegramoutbox_claimed_at",
        table_name="telegramoutbox",
    )
    op.drop_index(
        "ix_telegramoutbox_source_id",
        table_name="telegramoutbox",
    )
    op.drop_index(
        "ix_telegramoutbox_source_type",
        table_name="telegramoutbox",
    )
    op.drop_index(
        "ix_telegramoutbox_status",
        table_name="telegramoutbox",
    )
    op.drop_table("telegramoutbox")
    telegram_outbox_status.drop(op.get_bind(), checkfirst=True)
