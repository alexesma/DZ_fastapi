"""add shared process architecture annotations

Revision ID: 9d7a2e4c1b60
Revises: 8c4e1a7b2d30
Create Date: 2026-08-09 12:00:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

revision: str = "9d7a2e4c1b60"
down_revision: Union[str, Sequence[str], None] = "8c4e1a7b2d30"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "processarchitectureannotation",
        sa.Column("page_key", sa.String(length=64), nullable=False),
        sa.Column("section_key", sa.String(length=96), nullable=False),
        sa.Column("kind", sa.String(length=16), nullable=False),
        sa.Column("anchor_x", sa.Float(), nullable=True),
        sa.Column("anchor_y", sa.Float(), nullable=True),
        sa.Column("content", sa.Text(), nullable=True),
        sa.Column("drawing_data", sa.JSON(), nullable=True),
        sa.Column("parent_id", sa.Integer(), nullable=True),
        sa.Column("created_by_id", sa.Integer(), nullable=False),
        sa.Column("is_resolved", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("resolved_by_id", sa.Integer(), nullable=True),
        sa.Column("resolved_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(["created_by_id"], ["app_user.id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(
            ["parent_id"], ["processarchitectureannotation.id"], ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(["resolved_by_id"], ["app_user.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
    )
    for column in ("page_key", "section_key", "kind", "parent_id", "created_by_id"):
        op.create_index(
            f"ix_processarchitectureannotation_{column}",
            "processarchitectureannotation",
            [column],
        )


def downgrade() -> None:
    op.drop_table("processarchitectureannotation")
