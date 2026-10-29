"""Add scan log.

Revision ID: d5e7f9a1b3c4
Revises: c3f5a7b9d1e2
"""

import sqlalchemy as sa

from alembic import op

revision = "d5e7f9a1b3c4"
down_revision = "c3f5a7b9d1e2"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "scanlog",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("code", sa.String(128), nullable=False),
        sa.Column("kind", sa.String(40), nullable=True),
        sa.Column("found", sa.Boolean(), nullable=False,
                  server_default=sa.false()),
        sa.Column("source", sa.String(20), nullable=False,
                  server_default="server"),
        sa.Column("context", sa.String(60), nullable=True),
        sa.Column("user_id", sa.Integer(), sa.ForeignKey(
            "app_user.id", ondelete="SET NULL"), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_scanlog_code", "scanlog", ["code"])
    op.create_index("ix_scanlog_user_id", "scanlog", ["user_id"])
    op.create_index("ix_scanlog_created_at", "scanlog", ["created_at"])


def downgrade():
    op.drop_index("ix_scanlog_created_at", table_name="scanlog")
    op.drop_index("ix_scanlog_user_id", table_name="scanlog")
    op.drop_index("ix_scanlog_code", table_name="scanlog")
    op.drop_table("scanlog")
