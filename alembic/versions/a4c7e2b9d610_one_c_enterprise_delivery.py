"""Track EnterpriseData transport separately from other 1C channels.

Revision ID: a4c7e2b9d610
Revises: 9a2f4d6e8b10
"""

import sqlalchemy as sa

from alembic import op

revision = "a4c7e2b9d610"
down_revision = "9a2f4d6e8b10"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "onecexchangeevent",
        sa.Column("ed_only", sa.Boolean(), nullable=False, server_default=sa.false()),
    )
    op.add_column("onecexchangeevent", sa.Column("ed_message_no", sa.Integer()))
    op.add_column("onecexchangeevent", sa.Column("ed_sent_at", sa.DateTime(timezone=True)))
    op.add_column(
        "onecexchangeevent", sa.Column("ed_transport_confirmed_at", sa.DateTime(timezone=True))
    )
    op.add_column("onecexchangeevent", sa.Column("ed_last_error", sa.Text()))
    op.create_index("ix_onecexchangeevent_ed_message_no", "onecexchangeevent", ["ed_message_no"])
    op.create_table(
        "oncenterpriseconfig",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("enabled", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("send_receipts", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("send_shipments", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("first_event_id", sa.Integer()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )


def downgrade():
    op.drop_table("oncenterpriseconfig")
    op.drop_index("ix_onecexchangeevent_ed_message_no", table_name="onecexchangeevent")
    op.drop_column("onecexchangeevent", "ed_last_error")
    op.drop_column("onecexchangeevent", "ed_transport_confirmed_at")
    op.drop_column("onecexchangeevent", "ed_sent_at")
    op.drop_column("onecexchangeevent", "ed_message_no")
    op.drop_column("onecexchangeevent", "ed_only")
