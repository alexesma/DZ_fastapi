"""Add printer registration, user assignments and print jobs.

Revision ID: 4f1b0c2d3e50
Revises: 3e2d9f1a8b70
"""

import sqlalchemy as sa

from alembic import op

revision = "4f1b0c2d3e50"
down_revision = "3e2d9f1a8b70"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "printdevice",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("machine_id", sa.String(128), nullable=False),
        sa.Column("system_name", sa.String(255), nullable=False),
        sa.Column("display_name", sa.String(255), nullable=False),
        sa.Column("active", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("machine_id", "system_name", name="uq_printdevice_machine_name"),
    )
    op.create_index("ix_printdevice_machine_id", "printdevice", ["machine_id"])
    op.create_table(
        "printassignment",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("user_id", sa.Integer(),
                  sa.ForeignKey("app_user.id", ondelete="CASCADE"), nullable=False),
        sa.Column("device_id", sa.Integer(),
                  sa.ForeignKey("printdevice.id", ondelete="CASCADE"), nullable=False),
        sa.Column("kind", sa.String(16), nullable=False),
        sa.Column("is_default", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.UniqueConstraint(
            "user_id", "device_id", "kind",
            name="uq_printassignment_user_device_kind",
        ),
    )
    op.create_index("ix_printassignment_user_kind", "printassignment", ["user_id", "kind"])
    op.create_table(
        "printjob",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("request_key", sa.String(64), nullable=False, unique=True),
        sa.Column("user_id", sa.Integer(),
                  sa.ForeignKey("app_user.id", ondelete="SET NULL")),
        sa.Column("device_id", sa.Integer(),
                  sa.ForeignKey("printdevice.id", ondelete="RESTRICT"), nullable=False),
        sa.Column("kind", sa.String(16), nullable=False),
        sa.Column("title", sa.String(160), nullable=False),
        sa.Column("html", sa.Text(), nullable=False),
        sa.Column("status", sa.String(16), nullable=False, server_default="pending"),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("claimed_by", sa.String(128)),
        sa.Column("error", sa.Text()),
        sa.Column("width_mm", sa.Float()),
        sa.Column("height_mm", sa.Float()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("claimed_at", sa.DateTime(timezone=True)),
        sa.Column("printed_at", sa.DateTime(timezone=True)),
    )
    op.create_index("ix_printjob_device_status", "printjob", ["device_id", "status", "id"])
    op.create_index("ix_printjob_status", "printjob", ["status"])


def downgrade():
    op.drop_index("ix_printjob_status", table_name="printjob")
    op.drop_index("ix_printjob_device_status", table_name="printjob")
    op.drop_table("printjob")
    op.drop_index("ix_printassignment_user_kind", table_name="printassignment")
    op.drop_table("printassignment")
    op.drop_index("ix_printdevice_machine_id", table_name="printdevice")
    op.drop_table("printdevice")
