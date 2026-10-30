"""Store UNF number, date and posting flag returned for sent documents.

Revision ID: 7f3a9c2e5d18
Revises: d5e7f9a1b3c4
"""

import sqlalchemy as sa

from alembic import op

revision = "7f3a9c2e5d18"
down_revision = "d5e7f9a1b3c4"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("onecexchangeevent", sa.Column("ed_unf_ref", sa.String(36)))
    op.add_column("onecexchangeevent", sa.Column("ed_unf_number", sa.String(50)))
    op.add_column("onecexchangeevent", sa.Column("ed_unf_date", sa.DateTime(timezone=True)))
    op.add_column("onecexchangeevent", sa.Column("ed_unf_posted", sa.Boolean()))
    op.add_column(
        "onecexchangeevent", sa.Column("ed_unf_received_at", sa.DateTime(timezone=True))
    )
    op.create_index("ix_onecexchangeevent_ed_unf_number", "onecexchangeevent", ["ed_unf_number"])


def downgrade():
    op.drop_index("ix_onecexchangeevent_ed_unf_number", table_name="onecexchangeevent")
    for column in (
        "ed_unf_received_at", "ed_unf_posted", "ed_unf_date", "ed_unf_number", "ed_unf_ref",
    ):
        op.drop_column("onecexchangeevent", column)
