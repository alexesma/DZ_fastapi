"""add customer pricelist Dragonzap alias tracking

Revision ID: 8c4e1a7b2d30
Revises: 7b3d9f2a4c10
Create Date: 2026-08-03 12:00:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

revision: str = "8c4e1a7b2d30"
down_revision: Union[str, Sequence[str], None] = "7b3d9f2a4c10"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "customerpricelist",
        sa.Column("customer_config_id", sa.Integer(), nullable=True),
    )
    op.add_column(
        "customerpricelist",
        sa.Column("sent_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_foreign_key(
        "fk_customerpricelist_customer_config_id",
        "customerpricelist",
        "customerpricelistconfig",
        ["customer_config_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_index(
        "ix_customerpricelist_customer_config_id",
        "customerpricelist",
        ["customer_config_id"],
    )
    op.create_index(
        "ix_customerpricelist_sent_at",
        "customerpricelist",
        ["sent_at"],
    )

    op.create_table(
        "customerpricelistpublishedalias",
        sa.Column("customer_pricelist_id", sa.Integer(), nullable=False),
        sa.Column("source_autopart_id", sa.Integer(), nullable=False),
        sa.Column("advertised_oem", sa.String(length=255), nullable=False),
        sa.Column("advertised_brand", sa.String(length=255), nullable=False),
        sa.Column("advertised_name", sa.String(length=512), nullable=True),
        sa.Column("normalized_oem", sa.String(length=255), nullable=False),
        sa.Column("normalized_brand", sa.String(length=255), nullable=False),
        sa.Column("quantity", sa.Integer(), nullable=False),
        sa.Column("price", sa.DECIMAL(precision=10, scale=2), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.ForeignKeyConstraint(
            ["customer_pricelist_id"],
            ["customerpricelist.id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["source_autopart_id"],
            ["autopart.id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "customer_pricelist_id",
            "normalized_oem",
            "normalized_brand",
            name="uq_customer_pricelist_published_alias_key",
        ),
    )
    op.create_index(
        "ix_customer_pricelist_published_alias_lookup",
        "customerpricelistpublishedalias",
        ["customer_pricelist_id", "normalized_oem", "normalized_brand"],
    )
    op.create_index(
        "ix_customerpricelistpublishedalias_customer_pricelist_id",
        "customerpricelistpublishedalias",
        ["customer_pricelist_id"],
    )
    op.create_index(
        "ix_customerpricelistpublishedalias_source_autopart_id",
        "customerpricelistpublishedalias",
        ["source_autopart_id"],
    )

    for name, length in (
        ("match_type", 32),
        ("actual_oem", 255),
        ("actual_brand", 255),
        ("actual_name", 512),
    ):
        op.add_column(
            "customerorderitem",
            sa.Column(name, sa.String(length=length), nullable=True),
        )

    op.add_column(
        "shipmentdocumentitem",
        sa.Column("customer_order_item_id", sa.Integer(), nullable=True),
    )
    op.add_column(
        "shipmentdocumentitem",
        sa.Column("customer_oem", sa.String(length=255), nullable=True),
    )
    op.add_column(
        "shipmentdocumentitem",
        sa.Column("customer_brand", sa.String(length=255), nullable=True),
    )
    op.add_column(
        "shipmentdocumentitem",
        sa.Column("customer_name", sa.String(length=512), nullable=True),
    )
    op.create_foreign_key(
        "fk_shipmentdocumentitem_customer_order_item_id",
        "shipmentdocumentitem",
        "customerorderitem",
        ["customer_order_item_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_index(
        "ix_shipmentdocumentitem_customer_order_item_id",
        "shipmentdocumentitem",
        ["customer_order_item_id"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_shipmentdocumentitem_customer_order_item_id",
        table_name="shipmentdocumentitem",
    )
    op.drop_constraint(
        "fk_shipmentdocumentitem_customer_order_item_id",
        "shipmentdocumentitem",
        type_="foreignkey",
    )
    for name in (
        "customer_name",
        "customer_brand",
        "customer_oem",
        "customer_order_item_id",
    ):
        op.drop_column("shipmentdocumentitem", name)
    for name in ("actual_name", "actual_brand", "actual_oem", "match_type"):
        op.drop_column("customerorderitem", name)
    op.drop_table("customerpricelistpublishedalias")
    op.drop_index("ix_customerpricelist_sent_at", table_name="customerpricelist")
    op.drop_index(
        "ix_customerpricelist_customer_config_id",
        table_name="customerpricelist",
    )
    op.drop_constraint(
        "fk_customerpricelist_customer_config_id",
        "customerpricelist",
        type_="foreignkey",
    )
    op.drop_column("customerpricelist", "sent_at")
    op.drop_column("customerpricelist", "customer_config_id")
