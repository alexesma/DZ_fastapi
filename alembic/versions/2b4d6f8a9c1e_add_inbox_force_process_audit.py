"""add inbox force process audit table

Revision ID: 2b4d6f8a9c1e
Revises: 1fd35aab7c9d
Create Date: 2026-04-18 12:30:00.000000

"""
from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "2b4d6f8a9c1e"
down_revision: Union[str, None] = "1fd35aab7c9d"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "inbox_force_process_audit",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("inbox_email_id", sa.Integer(), nullable=True),
        sa.Column("requested_by_user_id", sa.Integer(), nullable=True),
        sa.Column("rule_type", sa.String(length=64), nullable=False),
        sa.Column("mode", sa.String(length=16), nullable=False),
        sa.Column("allow_reprocess", sa.Boolean(), nullable=False),
        sa.Column("status", sa.String(length=64), nullable=False),
        sa.Column("reason_code", sa.String(length=128), nullable=True),
        sa.Column("reason_text", sa.String(length=1000), nullable=True),
        sa.Column("details", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(
            ["inbox_email_id"],
            ["inboxemail.id"],
            ondelete="SET NULL",
        ),
        sa.ForeignKeyConstraint(
            ["requested_by_user_id"],
            ["app_user.id"],
            ondelete="SET NULL",
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        op.f("ix_inbox_force_process_audit_created_at"),
        "inbox_force_process_audit",
        ["created_at"],
        unique=False,
    )
    op.create_index(
        op.f("ix_inbox_force_process_audit_inbox_email_id"),
        "inbox_force_process_audit",
        ["inbox_email_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_inbox_force_process_audit_requested_by_user_id"),
        "inbox_force_process_audit",
        ["requested_by_user_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_inbox_force_process_audit_status"),
        "inbox_force_process_audit",
        ["status"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        op.f("ix_inbox_force_process_audit_status"),
        table_name="inbox_force_process_audit",
    )
    op.drop_index(
        op.f("ix_inbox_force_process_audit_requested_by_user_id"),
        table_name="inbox_force_process_audit",
    )
    op.drop_index(
        op.f("ix_inbox_force_process_audit_inbox_email_id"),
        table_name="inbox_force_process_audit",
    )
    op.drop_index(
        op.f("ix_inbox_force_process_audit_created_at"),
        table_name="inbox_force_process_audit",
    )
    op.drop_table("inbox_force_process_audit")
