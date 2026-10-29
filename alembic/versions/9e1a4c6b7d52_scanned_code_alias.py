"""Add manually bound scan codes for original spare-part barcodes.

Revision ID: 9e1a4c6b7d52
Revises: b8d1c2e3f4a5
"""

import sqlalchemy as sa

from alembic import op

revision = "9e1a4c6b7d52"
down_revision = "b8d1c2e3f4a5"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "scannedcodealias",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("code", sa.String(128), nullable=False),
        sa.Column("autopart_id", sa.Integer(), sa.ForeignKey(
            "autopart.id", ondelete="CASCADE"), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_by_user_id", sa.Integer(), sa.ForeignKey(
            "app_user.id", ondelete="SET NULL"), nullable=True),
    )
    op.create_index("ix_scannedcodealias_code", "scannedcodealias",
                    ["code"], unique=True)
    op.create_index("ix_scannedcodealias_autopart_id", "scannedcodealias",
                    ["autopart_id"])


def downgrade():
    op.drop_index("ix_scannedcodealias_autopart_id", table_name="scannedcodealias")
    op.drop_index("ix_scannedcodealias_code", table_name="scannedcodealias")
    op.drop_table("scannedcodealias")
