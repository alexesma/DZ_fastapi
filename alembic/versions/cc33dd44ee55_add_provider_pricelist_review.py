"""add provider pricelist review queue

Revision ID: cc33dd44ee55
Revises: bb22cc33dd44
Create Date: 2026-07-26 12:00:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

revision: str = "cc33dd44ee55"
down_revision: Union[str, Sequence[str], None] = "bb22cc33dd44"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "providerpricelistreview",
        sa.Column("provider_id", sa.Integer(), nullable=False),
        sa.Column("provider_config_id", sa.Integer(), nullable=False),
        sa.Column("previous_pricelist_id", sa.Integer(), nullable=True),
        sa.Column("published_pricelist_id", sa.Integer(), nullable=True),
        sa.Column("source_filename", sa.String(length=512), nullable=False),
        sa.Column("file_path", sa.Text(), nullable=False),
        sa.Column("file_extension", sa.String(length=32), nullable=False),
        sa.Column("file_sha256", sa.String(length=64), nullable=False),
        sa.Column(
            "status",
            sa.String(length=24),
            server_default="pending",
            nullable=False,
        ),
        sa.Column("reasons", sa.JSON(), nullable=False),
        sa.Column("metrics", sa.JSON(), nullable=False),
        sa.Column("examples", sa.JSON(), nullable=False),
        sa.Column("decision_reason", sa.Text(), nullable=True),
        sa.Column("processing_error", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column("decided_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("decided_by_user_id", sa.Integer(), nullable=True),
        sa.Column("id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(
            ["decided_by_user_id"],
            ["app_user.id"],
            ondelete="SET NULL",
        ),
        sa.ForeignKeyConstraint(
            ["previous_pricelist_id"],
            ["pricelist.id"],
            ondelete="SET NULL",
        ),
        sa.ForeignKeyConstraint(
            ["provider_config_id"],
            ["providerpricelistconfig.id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["provider_id"],
            ["provider.id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["published_pricelist_id"],
            ["pricelist.id"],
            ondelete="SET NULL",
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_providerpricelistreview_provider_id",
        "providerpricelistreview",
        ["provider_id"],
    )
    op.create_index(
        "ix_providerpricelistreview_provider_config_id",
        "providerpricelistreview",
        ["provider_config_id"],
    )
    op.create_index(
        "ix_providerpricelistreview_status",
        "providerpricelistreview",
        ["status"],
    )
    op.create_index(
        "ix_provider_pricelist_review_provider_created",
        "providerpricelistreview",
        ["provider_id", "created_at"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_provider_pricelist_review_provider_created",
        table_name="providerpricelistreview",
    )
    op.drop_index(
        "ix_providerpricelistreview_status",
        table_name="providerpricelistreview",
    )
    op.drop_index(
        "ix_providerpricelistreview_provider_config_id",
        table_name="providerpricelistreview",
    )
    op.drop_index(
        "ix_providerpricelistreview_provider_id",
        table_name="providerpricelistreview",
    )
    op.drop_table("providerpricelistreview")
