"""Add per-line scan counts for supplier receipt acceptance.

Revision ID: c3f5a7b9d1e2
Revises: 9e1a4c6b7d52
"""

import sqlalchemy as sa

from alembic import op

revision = "c3f5a7b9d1e2"
down_revision = "9e1a4c6b7d52"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "receiptscancount",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("receipt_item_id", sa.Integer(), sa.ForeignKey(
            "supplierreceiptitem.id", ondelete="CASCADE"), nullable=False,
            unique=True),
        sa.Column("accepted_qty", sa.Integer(), nullable=False,
                  server_default="0"),
        sa.Column("placed_qty", sa.Integer(), nullable=False,
                  server_default="0"),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint("accepted_qty >= 0", name="ck_receipt_scan_accepted"),
        sa.CheckConstraint("placed_qty >= 0", name="ck_receipt_scan_placed"),
    )


def downgrade():
    op.drop_table("receiptscancount")
