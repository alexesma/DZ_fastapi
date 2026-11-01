"""Mismatch acknowledgement and receipt price alerts.

Revision ID: 9b2d4f6a8c13
Revises: 8a1c3e5f7b92
"""

import sqlalchemy as sa

from alembic import op

revision = "9b2d4f6a8c13"
down_revision = "8a1c3e5f7b92"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "diadocincomingdocument",
        sa.Column("mismatch_acknowledged_at", sa.DateTime(timezone=True)),
    )
    op.add_column("supplierreceiptitem", sa.Column("order_price", sa.DECIMAL(10, 2)))
    op.add_column(
        "supplierreceiptitem",
        sa.Column(
            "price_exceeds_order",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        ),
    )


def downgrade():
    op.drop_column("supplierreceiptitem", "price_exceeds_order")
    op.drop_column("supplierreceiptitem", "order_price")
    op.drop_column("diadocincomingdocument", "mismatch_acknowledged_at")
