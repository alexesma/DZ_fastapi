"""ETrN via Diadoc: directories and waybills.

Revision ID: a1e7c9d3b5f2
Revises: 9b2d4f6a8c13
"""

import sqlalchemy as sa

from alembic import op

revision = "a1e7c9d3b5f2"
down_revision = "9b2d4f6a8c13"
branch_labels = None
depends_on = None


def _ts():
    return sa.Column("created_at", sa.DateTime(timezone=True))


def upgrade():
    op.add_column(
        "diadocintegrationsettings",
        sa.Column("etrn_enabled", sa.Boolean(), nullable=False, server_default=sa.text("false")),
    )
    op.add_column(
        "diadocintegrationsettings",
        sa.Column("etrn_t6_enabled", sa.Boolean(), nullable=False, server_default=sa.text("false")),
    )
    op.create_table(
        "transportcarrier",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("name", sa.String(500), nullable=False),
        sa.Column("inn", sa.String(32)),
        sa.Column("kpp", sa.String(32)),
        sa.Column("address", sa.String(1000)),
        sa.Column("phone", sa.String(64)),
        sa.Column("diadoc_box_id", sa.String(64)),
        sa.Column("comment", sa.String(1000)),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        _ts(),
    )
    op.create_index("ix_transportcarrier_inn", "transportcarrier", ["inn"])
    op.create_index("ix_transportcarrier_diadoc_box_id", "transportcarrier", ["diadoc_box_id"])
    op.create_table(
        "transportdriver",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("carrier_id", sa.Integer(), sa.ForeignKey("transportcarrier.id", ondelete="SET NULL")),
        sa.Column("last_name", sa.String(120), nullable=False),
        sa.Column("first_name", sa.String(120), nullable=False),
        sa.Column("middle_name", sa.String(120)),
        sa.Column("license_series", sa.String(16)),
        sa.Column("license_number", sa.String(32)),
        sa.Column("license_date", sa.Date()),
        sa.Column("phone", sa.String(64)),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        _ts(),
    )
    op.create_index("ix_transportdriver_carrier_id", "transportdriver", ["carrier_id"])
    op.create_table(
        "transportvehicle",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("carrier_id", sa.Integer(), sa.ForeignKey("transportcarrier.id", ondelete="SET NULL")),
        sa.Column("plate_number", sa.String(32), nullable=False),
        sa.Column("brand", sa.String(120)),
        sa.Column("vehicle_type", sa.String(120)),
        sa.Column("max_weight", sa.Numeric(10, 3)),
        sa.Column("capacity", sa.Numeric(10, 3)),
        sa.Column("trailer_number", sa.String(32)),
        sa.Column("ownership", sa.String(2), nullable=False, server_default="1"),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        _ts(),
    )
    op.create_index("ix_transportvehicle_carrier_id", "transportvehicle", ["carrier_id"])
    op.create_table(
        "transportaddress",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("kind", sa.String(16), nullable=False, server_default="other"),
        sa.Column("label", sa.String(255), nullable=False),
        sa.Column("customer_id", sa.Integer(), sa.ForeignKey("customer.id", ondelete="SET NULL")),
        sa.Column("zip_code", sa.String(6), nullable=False),
        sa.Column("region_code", sa.String(2), nullable=False),
        sa.Column("city", sa.String(255)),
        sa.Column("locality", sa.String(255)),
        sa.Column("street", sa.String(255)),
        sa.Column("building", sa.String(20)),
        sa.Column("block", sa.String(20)),
        sa.Column("apartment", sa.String(20)),
        sa.Column("other_info", sa.String(1000)),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        _ts(),
    )
    op.create_index("ix_transportaddress_customer_id", "transportaddress", ["customer_id"])
    op.create_table(
        "etrnwaybill",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("environment", sa.String(32), nullable=False, server_default="staging"),
        sa.Column("direction", sa.String(16), nullable=False),
        sa.Column("status", sa.String(24), nullable=False, server_default="draft"),
        sa.Column("box_id_guid", sa.String(64)),
        sa.Column("counteragent_box_id", sa.String(64)),
        sa.Column("number", sa.String(120)),
        sa.Column("waybill_date", sa.Date()),
        sa.Column("shipment_id", sa.Integer(), sa.ForeignKey("shipmentdocument.id", ondelete="SET NULL")),
        sa.Column("customer_id", sa.Integer(), sa.ForeignKey("customer.id", ondelete="SET NULL")),
        sa.Column("carrier_id", sa.Integer(), sa.ForeignKey("transportcarrier.id", ondelete="SET NULL")),
        sa.Column("driver_id", sa.Integer(), sa.ForeignKey("transportdriver.id", ondelete="SET NULL")),
        sa.Column("vehicle_id", sa.Integer(), sa.ForeignKey("transportvehicle.id", ondelete="SET NULL")),
        sa.Column(
            "incoming_document_id",
            sa.Integer(),
            sa.ForeignKey("diadocincomingdocument.id", ondelete="SET NULL"),
        ),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("t1_message_id", sa.String(255)),
        sa.Column("t1_entity_id", sa.String(255)),
        sa.Column("mt_id", sa.String(120)),
        sa.Column("kl_status", sa.String(64)),
        sa.Column("gis_status_type", sa.String(32)),
        sa.Column("gis_status_text", sa.String(500)),
        sa.Column("t3_state", sa.String(24), nullable=False, server_default="none"),
        sa.Column("t3_result", sa.String(16)),
        sa.Column("t3_message_id", sa.String(255)),
        sa.Column("t3_data", sa.JSON(), nullable=False),
        sa.Column("last_error", sa.String(2000)),
        sa.Column("problem_notified_key", sa.String(255)),
        sa.Column("last_synced_at", sa.DateTime(timezone=True)),
        sa.Column("sent_at", sa.DateTime(timezone=True)),
        _ts(),
        sa.Column("updated_at", sa.DateTime(timezone=True)),
        sa.UniqueConstraint("environment", "direction", "t1_message_id", name="uq_etrn_waybill_t1_message"),
    )
    for col in ("direction", "status", "number", "shipment_id", "customer_id", "incoming_document_id", "t1_message_id", "mt_id"):
        op.create_index(f"ix_etrnwaybill_{col}", "etrnwaybill", [col])


def downgrade():
    op.drop_table("etrnwaybill")
    op.drop_table("transportaddress")
    op.drop_table("transportvehicle")
    op.drop_table("transportdriver")
    op.drop_table("transportcarrier")
    op.drop_column("diadocintegrationsettings", "etrn_t6_enabled")
    op.drop_column("diadocintegrationsettings", "etrn_enabled")
