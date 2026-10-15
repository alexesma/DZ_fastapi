"""Persistent customer pricelist jobs and progress.

Revision ID: c5e7f9a1b3d2
Revises: b4d6e8f0a2c1
"""

import sqlalchemy as sa

from alembic import op

revision = "c5e7f9a1b3d2"
down_revision = "b4d6e8f0a2c1"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "customerpricelistjob",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column(
            "customer_id",
            sa.Integer(),
            sa.ForeignKey("customer.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "config_id",
            sa.Integer(),
            sa.ForeignKey("customerpricelistconfig.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "requested_by_id", sa.Integer(), sa.ForeignKey("app_user.id", ondelete="SET NULL")
        ),
        sa.Column("action", sa.String(16), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("progress", sa.Integer(), nullable=False),
        sa.Column("stage", sa.String(255), nullable=False),
        sa.Column(
            "pricelist_id", sa.Integer(), sa.ForeignKey("customerpricelist.id", ondelete="SET NULL")
        ),
        sa.Column("use_saved_file", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("delivery_status", sa.String(32)),
        sa.Column("error", sa.Text()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True)),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True)),
    )
    op.create_index("ix_customerpricelistjob_config_id", "customerpricelistjob", ["config_id"])
    op.create_index(
        "uq_customerpricelistjob_active_config",
        "customerpricelistjob",
        ["config_id"],
        unique=True,
        postgresql_where=sa.text("status IN ('queued', 'running')"),
    )


def downgrade():
    op.drop_table("customerpricelistjob")
