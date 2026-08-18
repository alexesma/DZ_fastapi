"""add customer pricelist drafts, export rows and publication rules

Revision ID: b6d4e8f2a901
Revises: a4c9e7f2b610
Create Date: 2026-08-18 12:00:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

revision: str = "b6d4e8f2a901"
down_revision: Union[str, Sequence[str], None] = "a4c9e7f2b610"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    for column in (
        sa.Column(
            "generation_status",
            sa.String(length=24),
            nullable=False,
            server_default="generated",
        ),
        sa.Column("generated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("artifact_path", sa.Text(), nullable=True),
        sa.Column("artifact_filename", sa.String(length=512), nullable=True),
        sa.Column("artifact_content_type", sa.String(length=128), nullable=True),
        sa.Column("positions_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column(
            "generation_summary",
            sa.JSON(),
            nullable=False,
            server_default=sa.text("'{}'::json"),
        ),
        sa.Column("approved_by_user_id", sa.Integer(), nullable=True),
        sa.Column("approved_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("rejected_by_user_id", sa.Integer(), nullable=True),
        sa.Column("rejected_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("decision_reason", sa.Text(), nullable=True),
        sa.Column("send_error", sa.Text(), nullable=True),
    ):
        op.add_column("customerpricelist", column)

    op.create_foreign_key(
        "fk_customerpricelist_approved_by_user_id",
        "customerpricelist",
        "app_user",
        ["approved_by_user_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_foreign_key(
        "fk_customerpricelist_rejected_by_user_id",
        "customerpricelist",
        "app_user",
        ["rejected_by_user_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_index(
        "ix_customerpricelist_generation_status",
        "customerpricelist",
        ["generation_status"],
    )
    op.create_index(
        "ix_customerpricelist_generated_at",
        "customerpricelist",
        ["generated_at"],
    )

    op.execute(
        "UPDATE customerpricelist SET generation_status = "
        "CASE WHEN sent_at IS NOT NULL THEN 'sent' ELSE 'generated' END, "
        "generated_at = COALESCE(sent_at, now())"
    )
    # Existing ZZap configurations used a hidden flag and were sent
    # immediately. Move them to the safer draft-first workflow while keeping
    # any explicitly configured values for the new controls.
    op.execute(
        """
        UPDATE customerpricelistconfig
        SET additional_filters = (
            '{
                "REQUIRE_DRAFT_APPROVAL": true,
                "PUBLISH_CONFIRMED_DZ_CROSSES": true,
                "ZZAP_MIN_PRICE_MULTIPLIER": 1.2,
                "ZZAP_ROUNDING_STEP": 10,
                "ZZAP_LABEL_PRODUCTS": true
            }'::jsonb
            || COALESCE(additional_filters, '{}'::json)::jsonb
        )::json
        WHERE lower(COALESCE(additional_filters->>'ZZAP', 'false'))
            IN ('1', 'true', 'yes', 'on')
        """
    )

    op.create_table(
        "customerpricelistpublicationrule",
        sa.Column("config_id", sa.Integer(), nullable=False),
        sa.Column("source_autopart_id", sa.Integer(), nullable=False),
        sa.Column("target_autopart_id", sa.Integer(), nullable=True),
        sa.Column("mode", sa.String(length=24), nullable=False, server_default="only_cross"),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("created_by_user_id", sa.Integer(), nullable=True),
        sa.Column("updated_by_user_id", sa.Integer(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.ForeignKeyConstraint(["config_id"], ["customerpricelistconfig.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["source_autopart_id"], ["autopart.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["target_autopart_id"], ["autopart.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["created_by_user_id"], ["app_user.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["updated_by_user_id"], ["app_user.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "config_id",
            "source_autopart_id",
            name="uq_customer_pricelist_publication_rule_source",
        ),
    )
    for column in ("config_id", "source_autopart_id", "target_autopart_id"):
        op.create_index(
            f"ix_customerpricelistpublicationrule_{column}",
            "customerpricelistpublicationrule",
            [column],
        )

    op.create_table(
        "customerpricelistexportrow",
        sa.Column("customer_pricelist_id", sa.Integer(), nullable=False),
        sa.Column("source_autopart_id", sa.Integer(), nullable=True),
        sa.Column("advertised_brand", sa.String(length=255), nullable=False),
        sa.Column("advertised_oem", sa.String(length=255), nullable=False),
        sa.Column("advertised_name", sa.String(length=512), nullable=True),
        sa.Column("normalized_brand", sa.String(length=255), nullable=False),
        sa.Column("normalized_oem", sa.String(length=255), nullable=False),
        sa.Column("quantity", sa.Integer(), nullable=False),
        sa.Column("price", sa.DECIMAL(precision=10, scale=2), nullable=False),
        sa.Column("row_type", sa.String(length=32), nullable=False, server_default="direct"),
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.ForeignKeyConstraint(
            ["customer_pricelist_id"],
            ["customerpricelist.id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(["source_autopart_id"], ["autopart.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_customerpricelistexportrow_customer_pricelist_id",
        "customerpricelistexportrow",
        ["customer_pricelist_id"],
    )
    op.create_index(
        "ix_customerpricelistexportrow_source_autopart_id",
        "customerpricelistexportrow",
        ["source_autopart_id"],
    )
    op.create_index(
        "ix_customer_pricelist_export_row_lookup",
        "customerpricelistexportrow",
        ["customer_pricelist_id", "normalized_brand", "normalized_oem"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_customer_pricelist_export_row_lookup",
        table_name="customerpricelistexportrow",
    )
    op.drop_index(
        "ix_customerpricelistexportrow_source_autopart_id",
        table_name="customerpricelistexportrow",
    )
    op.drop_index(
        "ix_customerpricelistexportrow_customer_pricelist_id",
        table_name="customerpricelistexportrow",
    )
    op.drop_table("customerpricelistexportrow")

    for column in ("target_autopart_id", "source_autopart_id", "config_id"):
        op.drop_index(
            f"ix_customerpricelistpublicationrule_{column}",
            table_name="customerpricelistpublicationrule",
        )
    op.drop_table("customerpricelistpublicationrule")

    op.drop_index("ix_customerpricelist_generated_at", table_name="customerpricelist")
    op.drop_index("ix_customerpricelist_generation_status", table_name="customerpricelist")
    op.drop_constraint(
        "fk_customerpricelist_rejected_by_user_id",
        "customerpricelist",
        type_="foreignkey",
    )
    op.drop_constraint(
        "fk_customerpricelist_approved_by_user_id",
        "customerpricelist",
        type_="foreignkey",
    )
    for column in (
        "send_error",
        "decision_reason",
        "rejected_at",
        "rejected_by_user_id",
        "approved_at",
        "approved_by_user_id",
        "generation_summary",
        "positions_count",
        "artifact_content_type",
        "artifact_filename",
        "artifact_path",
        "generated_at",
        "generation_status",
    ):
        op.drop_column("customerpricelist", column)
