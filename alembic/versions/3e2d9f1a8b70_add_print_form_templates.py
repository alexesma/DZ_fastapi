"""Store visual label layouts.

Revision ID: 3e2d9f1a8b70
Revises: fb7a4d21c901
"""

import sqlalchemy as sa

from alembic import op

revision = "3e2d9f1a8b70"
down_revision = "fb7a4d21c901"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "printformtemplate",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("key", sa.String(64), nullable=False),
        sa.Column("width_mm", sa.Float(), nullable=False),
        sa.Column("height_mm", sa.Float(), nullable=False),
        sa.Column("blocks", sa.JSON(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index(
        "ix_printformtemplate_key", "printformtemplate", ["key"], unique=True,
    )


def downgrade():
    op.drop_index("ix_printformtemplate_key", table_name="printformtemplate")
    op.drop_table("printformtemplate")
