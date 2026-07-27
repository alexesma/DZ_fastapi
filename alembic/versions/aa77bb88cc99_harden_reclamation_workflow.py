"""Harden reclamation mail processing and add audit history.

Revision ID: aa77bb88cc99
Revises: ff66aa77bb88
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "aa77bb88cc99"
down_revision: Union[str, Sequence[str], None] = "ff66aa77bb88"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("ALTER TYPE userrole ADD VALUE IF NOT EXISTS 'reclamation'")

    op.create_table(
        "reclamationevent",
        sa.Column("reclamation_id", sa.Integer(), nullable=False),
        sa.Column("event_type", sa.String(length=64), nullable=False),
        sa.Column("actor_user_id", sa.Integer(), nullable=True),
        sa.Column("details", sa.JSON(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
        ),
        sa.Column("id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(
            ["actor_user_id"],
            ["app_user.id"],
            ondelete="SET NULL",
        ),
        sa.ForeignKeyConstraint(
            ["reclamation_id"],
            ["reclamation.id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_reclamationevent_reclamation_id",
        "reclamationevent",
        ["reclamation_id"],
    )
    op.create_index(
        "ix_reclamationevent_event_type",
        "reclamationevent",
        ["event_type"],
    )
    op.create_index(
        "ix_reclamationevent_actor_user_id",
        "reclamationevent",
        ["actor_user_id"],
    )
    op.create_index(
        "ix_reclamationevent_created_at",
        "reclamationevent",
        ["created_at"],
    )

    op.create_table(
        "reclamationmailboxstate",
        sa.Column("email_account_id", sa.Integer(), nullable=False),
        sa.Column(
            "folder",
            sa.String(length=255),
            nullable=False,
            server_default="INBOX",
        ),
        sa.Column(
            "last_uid",
            sa.BigInteger(),
            nullable=False,
            server_default="0",
        ),
        sa.Column(
            "last_checked_at",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
        sa.Column("last_error", sa.String(length=2000), nullable=True),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
        ),
        sa.Column("id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(
            ["email_account_id"],
            ["emailaccount.id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "email_account_id",
            "folder",
            name="uq_reclamation_mailbox_state_account_folder",
        ),
    )
    op.create_index(
        "ix_reclamationmailboxstate_email_account_id",
        "reclamationmailboxstate",
        ["email_account_id"],
    )

    op.create_table(
        "reclamationmailmessage",
        sa.Column("email_account_id", sa.Integer(), nullable=False),
        sa.Column(
            "folder",
            sa.String(length=255),
            nullable=False,
            server_default="INBOX",
        ),
        sa.Column("uid", sa.String(length=64), nullable=False),
        sa.Column("message_id", sa.String(length=512), nullable=True),
        sa.Column("reclamation_id", sa.Integer(), nullable=True),
        sa.Column("sender_email", sa.String(length=255), nullable=True),
        sa.Column("subject", sa.String(length=998), nullable=True),
        sa.Column(
            "received_at",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
        sa.Column("body_text", sa.Text(), nullable=True),
        sa.Column("body_html", sa.Text(), nullable=True),
        sa.Column(
            "processing_status",
            sa.String(length=32),
            nullable=False,
            server_default="pending",
        ),
        sa.Column("processing_error", sa.String(length=4000), nullable=True),
        sa.Column(
            "attempts",
            sa.Integer(),
            nullable=False,
            server_default="0",
        ),
        sa.Column(
            "parser_version",
            sa.String(length=32),
            nullable=False,
            server_default="rules-v2",
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
        ),
        sa.Column(
            "processed_at",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
        sa.Column("id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(
            ["email_account_id"],
            ["emailaccount.id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["reclamation_id"],
            ["reclamation.id"],
            ondelete="SET NULL",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "email_account_id",
            "folder",
            "uid",
            name="uq_reclamation_mail_message_account_folder_uid",
        ),
    )
    op.create_index(
        "ix_reclamationmailmessage_email_account_id",
        "reclamationmailmessage",
        ["email_account_id"],
    )
    op.create_index(
        "ix_reclamationmailmessage_message_id",
        "reclamationmailmessage",
        ["message_id"],
    )
    op.create_index(
        "ix_reclamationmailmessage_reclamation_id",
        "reclamationmailmessage",
        ["reclamation_id"],
    )
    op.create_index(
        "ix_reclamationmailmessage_sender_email",
        "reclamationmailmessage",
        ["sender_email"],
    )
    op.create_index(
        "ix_reclamationmailmessage_processing_status",
        "reclamationmailmessage",
        ["processing_status"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_reclamationmailmessage_processing_status",
        table_name="reclamationmailmessage",
    )
    op.drop_index(
        "ix_reclamationmailmessage_sender_email",
        table_name="reclamationmailmessage",
    )
    op.drop_index(
        "ix_reclamationmailmessage_reclamation_id",
        table_name="reclamationmailmessage",
    )
    op.drop_index(
        "ix_reclamationmailmessage_message_id",
        table_name="reclamationmailmessage",
    )
    op.drop_index(
        "ix_reclamationmailmessage_email_account_id",
        table_name="reclamationmailmessage",
    )
    op.drop_table("reclamationmailmessage")
    op.drop_index(
        "ix_reclamationmailboxstate_email_account_id",
        table_name="reclamationmailboxstate",
    )
    op.drop_table("reclamationmailboxstate")
    op.drop_index(
        "ix_reclamationevent_created_at",
        table_name="reclamationevent",
    )
    op.drop_index(
        "ix_reclamationevent_actor_user_id",
        table_name="reclamationevent",
    )
    op.drop_index(
        "ix_reclamationevent_event_type",
        table_name="reclamationevent",
    )
    op.drop_index(
        "ix_reclamationevent_reclamation_id",
        table_name="reclamationevent",
    )
    op.drop_table("reclamationevent")
    # PostgreSQL enum values cannot be removed safely in a regular downgrade.
