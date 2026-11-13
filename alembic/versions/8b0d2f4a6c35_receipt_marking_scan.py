"""Marking codes scanned while counting a supplier receipt.

Revision ID: 8b0d2f4a6c35
Revises: 7a9c1e3b5d24
Create Date: 2026-11-12 18:00:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

revision: str = "8b0d2f4a6c35"
down_revision: Union[str, Sequence[str], None] = "7a9c1e3b5d24"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "receiptmarkingscan",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column(
            "receipt_id",
            sa.Integer(),
            sa.ForeignKey("supplierreceipt.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "receipt_item_id",
            sa.Integer(),
            sa.ForeignKey("supplierreceiptitem.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column(
            "autopart_id",
            sa.Integer(),
            sa.ForeignKey("autopart.id"),
            nullable=True,
        ),
        sa.Column("code", sa.String(length=255), nullable=False),
        sa.Column("full_code", sa.Text(), nullable=True),
        sa.Column("scanned_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "scanned_by_user_id",
            sa.Integer(),
            sa.ForeignKey("app_user.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.UniqueConstraint(
            "receipt_id", "code", name="uq_receipt_marking_scan"
        ),
    )
    op.create_index(
        "ix_receiptmarkingscan_receipt_id",
        "receiptmarkingscan",
        ["receipt_id"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_receiptmarkingscan_receipt_id", table_name="receiptmarkingscan"
    )
    op.drop_table("receiptmarkingscan")
