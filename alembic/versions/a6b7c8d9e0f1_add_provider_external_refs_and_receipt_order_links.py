"""add provider external refs and receipt order links

Revision ID: a6b7c8d9e0f1
Revises: d4e5f6a7b8c9
Create Date: 2026-04-28 18:30:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "a6b7c8d9e0f1"
down_revision: Union[str, Sequence[str], None] = "d4e5f6a7b8c9"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "providerexternalreference",
        sa.Column("id", sa.Integer(), primary_key=True, nullable=False),
        sa.Column(
            "provider_id",
            sa.Integer(),
            sa.ForeignKey("provider.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("source_system", sa.String(length=32), nullable=False),
        sa.Column("external_supplier_id", sa.Integer(), nullable=True),
        sa.Column("external_supplier_name", sa.String(length=255), nullable=True),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
        sa.UniqueConstraint(
            "source_system",
            "external_supplier_id",
            name="uq_provider_external_reference_source_supplier",
        ),
    )
    op.create_index(
        "ix_providerexternalreference_provider_id",
        "providerexternalreference",
        ["provider_id"],
        unique=False,
    )
    op.create_index(
        "ix_providerexternalreference_source_system",
        "providerexternalreference",
        ["source_system"],
        unique=False,
    )
    op.create_index(
        "ix_providerexternalreference_external_supplier_id",
        "providerexternalreference",
        ["external_supplier_id"],
        unique=False,
    )

    op.add_column(
        "orderitem",
        sa.Column("external_supplier_id", sa.Integer(), nullable=True),
    )
    op.add_column(
        "orderitem",
        sa.Column("external_supplier_name", sa.String(length=255), nullable=True),
    )
    op.add_column(
        "orderitem",
        sa.Column("external_price_name", sa.String(length=255), nullable=True),
    )
    op.add_column(
        "orderitem",
        sa.Column("external_sup_logo", sa.String(length=255), nullable=True),
    )
    op.create_index(
        "ix_orderitem_external_supplier_id",
        "orderitem",
        ["external_supplier_id"],
        unique=False,
    )

    op.add_column(
        "supplierreceiptitem",
        sa.Column("order_item_id", sa.Integer(), nullable=True),
    )
    op.create_index(
        "ix_supplierreceiptitem_order_item_id",
        "supplierreceiptitem",
        ["order_item_id"],
        unique=False,
    )
    op.create_foreign_key(
        "fk_supplierreceiptitem_order_item_id_orderitem",
        "supplierreceiptitem",
        "orderitem",
        ["order_item_id"],
        ["id"],
    )


def downgrade() -> None:
    op.drop_constraint(
        "fk_supplierreceiptitem_order_item_id_orderitem",
        "supplierreceiptitem",
        type_="foreignkey",
    )
    op.drop_index(
        "ix_supplierreceiptitem_order_item_id",
        table_name="supplierreceiptitem",
    )
    op.drop_column("supplierreceiptitem", "order_item_id")

    op.drop_index("ix_orderitem_external_supplier_id", table_name="orderitem")
    op.drop_column("orderitem", "external_sup_logo")
    op.drop_column("orderitem", "external_price_name")
    op.drop_column("orderitem", "external_supplier_name")
    op.drop_column("orderitem", "external_supplier_id")

    op.drop_index(
        "ix_providerexternalreference_external_supplier_id",
        table_name="providerexternalreference",
    )
    op.drop_index(
        "ix_providerexternalreference_source_system",
        table_name="providerexternalreference",
    )
    op.drop_index(
        "ix_providerexternalreference_provider_id",
        table_name="providerexternalreference",
    )
    op.drop_table("providerexternalreference")
