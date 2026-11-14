"""Journal of UNF documents that have no DragonZap event.

Revision ID: a2c4e6f8b157
Revises: a4c7e2d9f105
Create Date: 2026-11-13 14:00:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

revision: str = "a2c4e6f8b157"
down_revision: Union[str, Sequence[str], None] = "a4c7e2d9f105"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "onecforeigndocument",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("message_no", sa.Integer(), nullable=False),
        sa.Column("object_type", sa.String(length=160), nullable=False),
        sa.Column("reason", sa.String(length=32), nullable=False),
        sa.Column("ref", sa.String(length=64), nullable=True),
        sa.Column("number", sa.String(length=64), nullable=True),
        sa.Column("document_date", sa.String(length=40), nullable=True),
        sa.Column("posted", sa.Boolean(), nullable=True),
        sa.Column("deleted", sa.Boolean(), nullable=True),
        sa.Column("comment", sa.Text(), nullable=True),
        sa.Column("archive_file", sa.String(length=255), nullable=True),
        sa.Column("received_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("reviewed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "reviewed_by_user_id",
            sa.Integer(),
            sa.ForeignKey("app_user.id", ondelete="SET NULL"),
            nullable=True,
        ),
    )
    op.create_index(
        "ix_onecforeigndocument_message_no",
        "onecforeigndocument",
        ["message_no"],
    )
    op.create_index(
        "ix_onecforeigndocument_reviewed_at",
        "onecforeigndocument",
        ["reviewed_at"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_onecforeigndocument_reviewed_at", table_name="onecforeigndocument"
    )
    op.drop_index(
        "ix_onecforeigndocument_message_no", table_name="onecforeigndocument"
    )
    op.drop_table("onecforeigndocument")
