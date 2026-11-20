"""add source and inbox email to bank statements

Revision ID: c3d5e7f9a1b2
Revises: da180ce6f4b1
Create Date: 2026-11-20 20:30:00.000000
"""

import sqlalchemy as sa

from alembic import op

revision = "c3d5e7f9a1b2"
down_revision = "da180ce6f4b1"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "bankstatement",
        sa.Column(
            "source",
            sa.String(length=32),
            nullable=False,
            server_default="upload",
        ),
    )
    op.add_column(
        "bankstatement",
        sa.Column("inbox_email_id", sa.Integer(), nullable=True),
    )
    op.create_index(
        op.f("ix_bankstatement_inbox_email_id"),
        "bankstatement",
        ["inbox_email_id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        op.f("ix_bankstatement_inbox_email_id"), table_name="bankstatement"
    )
    op.drop_column("bankstatement", "inbox_email_id")
    op.drop_column("bankstatement", "source")
