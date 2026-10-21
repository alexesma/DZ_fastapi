"""Customer order picking labels and realization boxes.

Revision ID: fb7a4d21c901
Revises: fa2d4e6b8c30
"""

import sqlalchemy as sa

from alembic import op

revision = "fb7a4d21c901"
down_revision = "fa2d4e6b8c30"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "shipmentdocument",
        sa.Column("assembly_managed", sa.Boolean(), nullable=False, server_default=sa.false()),
    )
    op.create_table(
        "customerorderpicklabel",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column(
            "stock_order_item_id",
            sa.Integer(),
            sa.ForeignKey("stockorderitem.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "storage_location_id",
            sa.Integer(),
            sa.ForeignKey("storagelocation.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column(
            "reserve_id", sa.Integer(), sa.ForeignKey("stockreserve.id", ondelete="SET NULL")
        ),
        sa.Column("quantity", sa.Integer(), nullable=False),
        sa.Column("barcode", sa.String(128), nullable=False, unique=True),
        sa.Column("print_count", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("printed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "printed_by_user_id", sa.Integer(), sa.ForeignKey("app_user.id", ondelete="SET NULL")
        ),
        sa.Column("cancelled_at", sa.DateTime(timezone=True)),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint("quantity > 0", name="ck_customer_order_pick_label_qty"),
    )
    op.create_index(
        "ix_customerorderpicklabel_stock_order_item_id",
        "customerorderpicklabel",
        ["stock_order_item_id"],
    )
    op.add_column(
        "shipmentdocumentitem",
        sa.Column(
            "assembly_pick_label_id",
            sa.Integer(),
            sa.ForeignKey("customerorderpicklabel.id", ondelete="SET NULL"),
        ),
    )
    op.add_column(
        "shipmentdocumentitem",
        sa.Column(
            "cross_docking_label_id",
            sa.Integer(),
            sa.ForeignKey("crossdockinglabel.id", ondelete="SET NULL"),
        ),
    )
    op.create_index(
        "ix_shipmentdocumentitem_assembly_pick_label_id",
        "shipmentdocumentitem",
        ["assembly_pick_label_id"],
    )
    op.create_index(
        "ix_shipmentdocumentitem_cross_docking_label_id",
        "shipmentdocumentitem",
        ["cross_docking_label_id"],
    )
    op.create_table(
        "shipmentpackage",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column(
            "document_id",
            sa.Integer(),
            sa.ForeignKey("shipmentdocument.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("number", sa.Integer(), nullable=False),
        sa.Column("sealed_at", sa.DateTime(timezone=True)),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("document_id", "number", name="uq_shipment_package_number"),
    )
    op.create_index("ix_shipmentpackage_document_id", "shipmentpackage", ["document_id"])
    op.create_table(
        "shipmentpackageitem",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column(
            "package_id",
            sa.Integer(),
            sa.ForeignKey("shipmentpackage.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "shipment_item_id",
            sa.Integer(),
            sa.ForeignKey("shipmentdocumentitem.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("quantity", sa.Integer(), nullable=False),
        sa.UniqueConstraint("package_id", "shipment_item_id", name="uq_shipment_package_item"),
        sa.CheckConstraint("quantity > 0", name="ck_shipment_package_item_qty"),
    )
    op.create_index("ix_shipmentpackageitem_package_id", "shipmentpackageitem", ["package_id"])
    op.create_index(
        "ix_shipmentpackageitem_shipment_item_id", "shipmentpackageitem", ["shipment_item_id"]
    )


def downgrade():
    op.drop_table("shipmentpackageitem")
    op.drop_table("shipmentpackage")
    op.drop_index("ix_shipmentdocumentitem_cross_docking_label_id", "shipmentdocumentitem")
    op.drop_index("ix_shipmentdocumentitem_assembly_pick_label_id", "shipmentdocumentitem")
    op.drop_column("shipmentdocumentitem", "cross_docking_label_id")
    op.drop_column("shipmentdocumentitem", "assembly_pick_label_id")
    op.drop_table("customerorderpicklabel")
    op.drop_column("shipmentdocument", "assembly_managed")
