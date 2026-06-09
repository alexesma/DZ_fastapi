"""add execution trace table

Revision ID: eb1c2d3e4f5a
Revises: da7e8c9f0b1a
Create Date: 2026-06-09 19:10:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "eb1c2d3e4f5a"
down_revision: Union[str, Sequence[str], None] = "da7e8c9f0b1a"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "executiontrace",
        sa.Column("trace_type", sa.String(length=32), nullable=False),
        sa.Column("job_key", sa.String(length=64), nullable=False),
        sa.Column("job_name", sa.String(length=255), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("provider_id", sa.Integer(), nullable=True),
        sa.Column("provider_config_id", sa.Integer(), nullable=True),
        sa.Column("source_filename", sa.String(length=255), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("duration_ms", sa.Integer(), nullable=True),
        sa.Column("rss_before_mb", sa.Float(), nullable=True),
        sa.Column("rss_after_mb", sa.Float(), nullable=True),
        sa.Column("memory_delta_mb", sa.Float(), nullable=True),
        sa.Column(
            "details",
            sa.JSON(),
            nullable=False,
            server_default=sa.text("'{}'::json"),
        ),
        sa.Column("id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(["provider_config_id"], ["providerpricelistconfig.id"]),
        sa.ForeignKeyConstraint(["provider_id"], ["provider.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(op.f("ix_executiontrace_trace_type"), "executiontrace", ["trace_type"], unique=False)
    op.create_index(op.f("ix_executiontrace_job_key"), "executiontrace", ["job_key"], unique=False)
    op.create_index(op.f("ix_executiontrace_status"), "executiontrace", ["status"], unique=False)
    op.create_index(op.f("ix_executiontrace_provider_id"), "executiontrace", ["provider_id"], unique=False)
    op.create_index(op.f("ix_executiontrace_provider_config_id"), "executiontrace", ["provider_config_id"], unique=False)
    op.create_index(op.f("ix_executiontrace_started_at"), "executiontrace", ["started_at"], unique=False)


def downgrade() -> None:
    op.drop_index(op.f("ix_executiontrace_started_at"), table_name="executiontrace")
    op.drop_index(op.f("ix_executiontrace_provider_config_id"), table_name="executiontrace")
    op.drop_index(op.f("ix_executiontrace_provider_id"), table_name="executiontrace")
    op.drop_index(op.f("ix_executiontrace_status"), table_name="executiontrace")
    op.drop_index(op.f("ix_executiontrace_job_key"), table_name="executiontrace")
    op.drop_index(op.f("ix_executiontrace_trace_type"), table_name="executiontrace")
    op.drop_table("executiontrace")
