"""Direction (supplier/customer) of incoming Diadoc documents.

Revision ID: b3c5d7e9f1a2
Revises: a1e7c9d3b5f2
"""

import sqlalchemy as sa

from alembic import op

revision = "b3c5d7e9f1a2"
down_revision = "a1e7c9d3b5f2"
branch_labels = None
depends_on = None


def upgrade():
    t = "diadocincomingdocument"
    op.add_column(t, sa.Column("direction", sa.String(16)))
    op.add_column(
        t,
        sa.Column(
            "customer_id",
            sa.Integer(),
            sa.ForeignKey("customer.id", ondelete="SET NULL"),
        ),
    )
    op.create_index("ix_diadocincomingdocument_direction", t, ["direction"])
    op.create_index("ix_diadocincomingdocument_customer_id", t, ["customer_id"])


def downgrade():
    t = "diadocincomingdocument"
    op.drop_index("ix_diadocincomingdocument_customer_id", table_name=t)
    op.drop_index("ix_diadocincomingdocument_direction", table_name=t)
    op.drop_column(t, "customer_id")
    op.drop_column(t, "direction")
