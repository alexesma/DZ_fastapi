"""reclamations, return rules, customer reclamation emails, email outbox

Revision ID: 50a408eab54e
Revises: d366020e6a82
Create Date: 2026-07-05 10:00:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "50a408eab54e"
down_revision: Union[str, Sequence[str], None] = "d366020e6a82"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


reclamation_source = postgresql.ENUM(
    "email", "link", "manual",
    name="reclamationsource",
    create_type=False,
)
reclamation_status = postgresql.ENUM(
    "new", "recognized", "checked", "waiting_docs", "waiting_supplier",
    "approved", "rejected", "closed",
    name="reclamationstatus",
    create_type=False,
)
reclamation_type = postgresql.ENUM(
    "customer_refusal", "defect", "other",
    name="reclamationtype",
    create_type=False,
)
reclamation_item_source = postgresql.ENUM(
    "unknown", "our_stock", "supplier_transit",
    name="reclamationitemsource",
    create_type=False,
)
reclamation_attachment_kind = postgresql.ENUM(
    "removal_order", "installation_order", "defect_report", "photo",
    "other",
    name="reclamationattachmentkind",
    create_type=False,
)
email_outbox_status = postgresql.ENUM(
    "pending", "sent", "error", "cancelled",
    name="emailoutboxstatus",
    create_type=False,
)


def upgrade() -> None:
    bind = op.get_bind()
    for enum in (
        reclamation_source, reclamation_status, reclamation_type,
        reclamation_item_source, reclamation_attachment_kind,
        email_outbox_status,
    ):
        enum.create(bind, checkfirst=True)

    # ── Поля правил возврата ────────────────────────────────────────────
    op.add_column(
        "customer",
        sa.Column("return_window_days", sa.Integer(), nullable=True),
    )
    op.add_column(
        "provider",
        sa.Column(
            "return_allowed",
            sa.Boolean(),
            nullable=False,
            server_default=sa.true(),
        ),
    )
    op.add_column(
        "provider",
        sa.Column("return_window_days", sa.Integer(), nullable=True),
    )
    op.add_column(
        "provider",
        sa.Column("return_blocked_brands", sa.JSON(), nullable=True),
    )
    op.add_column(
        "provider",
        sa.Column("return_request_email", sa.String(255), nullable=True),
    )

    # ── Почты клиента для рекламаций ────────────────────────────────────
    op.create_table(
        "customerreclamationemail",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column(
            "customer_id",
            sa.Integer(),
            sa.ForeignKey("customer.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("email", sa.String(255), nullable=False),
        sa.Column("comment", sa.String(255), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
        sa.UniqueConstraint(
            "email", name="uq_customer_reclamation_email"
        ),
    )
    op.create_index(
        "ix_customerreclamationemail_customer_id",
        "customerreclamationemail",
        ["customer_id"],
    )
    op.create_index(
        "ix_customerreclamationemail_email",
        "customerreclamationemail",
        ["email"],
    )

    # ── Рекламации ──────────────────────────────────────────────────────
    op.create_table(
        "reclamation",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("source", reclamation_source, nullable=False),
        sa.Column("status", reclamation_status, nullable=False),
        sa.Column("reclamation_type", reclamation_type, nullable=True),
        sa.Column(
            "customer_id",
            sa.Integer(),
            sa.ForeignKey("customer.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("sender_email", sa.String(255), nullable=True),
        sa.Column("source_link", sa.String(1024), nullable=True),
        sa.Column("email_message_id", sa.String(512), nullable=True),
        sa.Column("email_subject", sa.String(998), nullable=True),
        sa.Column(
            "email_received_at", sa.DateTime(timezone=True), nullable=True
        ),
        sa.Column("email_body", sa.Text(), nullable=True),
        sa.Column("stated_document_number", sa.String(120), nullable=True),
        sa.Column("stated_document_date", sa.Date(), nullable=True),
        sa.Column("stated_reason", sa.Text(), nullable=True),
        sa.Column("extracted_data", sa.JSON(), nullable=True),
        sa.Column("check_result", sa.JSON(), nullable=True),
        sa.Column("recommendation", sa.String(64), nullable=True),
        sa.Column("resolution", sa.String(64), nullable=True),
        sa.Column("resolution_comment", sa.Text(), nullable=True),
        sa.Column(
            "resolved_by_user_id",
            sa.Integer(),
            sa.ForeignKey("app_user.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("resolved_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "return_from_customer_id",
            sa.Integer(),
            sa.ForeignKey("returnfromcustomer.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
    )
    for col in (
        "status", "customer_id", "sender_email", "email_message_id",
        "return_from_customer_id",
    ):
        op.create_index(
            f"ix_reclamation_{col}", "reclamation", [col]
        )

    op.create_table(
        "reclamationitem",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column(
            "reclamation_id",
            sa.Integer(),
            sa.ForeignKey("reclamation.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("oem_number", sa.String(120), nullable=True),
        sa.Column("brand_name", sa.String(120), nullable=True),
        sa.Column("autopart_name", sa.String(512), nullable=True),
        sa.Column(
            "quantity", sa.Integer(), nullable=False, server_default="1"
        ),
        sa.Column("reason", sa.Text(), nullable=True),
        sa.Column(
            "item_source", reclamation_item_source, nullable=False
        ),
        sa.Column(
            "autopart_id",
            sa.Integer(),
            sa.ForeignKey("autopart.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column(
            "shipment_item_id",
            sa.Integer(),
            sa.ForeignKey("shipmentdocumentitem.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column(
            "stock_lot_id",
            sa.Integer(),
            sa.ForeignKey("stocklot.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column(
            "source_provider_id",
            sa.Integer(),
            sa.ForeignKey("provider.id", ondelete="SET NULL"),
            nullable=True,
        ),
    )
    for col in (
        "reclamation_id", "oem_number", "shipment_item_id",
        "source_provider_id",
    ):
        op.create_index(
            f"ix_reclamationitem_{col}", "reclamationitem", [col]
        )

    op.create_table(
        "reclamationattachment",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column(
            "reclamation_id",
            sa.Integer(),
            sa.ForeignKey("reclamation.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "kind", reclamation_attachment_kind, nullable=False
        ),
        sa.Column("file_name", sa.String(512), nullable=True),
        sa.Column("content_type", sa.String(255), nullable=True),
        sa.Column("local_file_path", sa.String(1024), nullable=True),
        sa.Column("size_bytes", sa.Integer(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index(
        "ix_reclamationattachment_reclamation_id",
        "reclamationattachment",
        ["reclamation_id"],
    )

    # ── Очередь исходящих писем ─────────────────────────────────────────
    op.create_table(
        "emailoutbox",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("status", email_outbox_status, nullable=False),
        sa.Column("from_email", sa.String(255), nullable=True),
        sa.Column("to_email", sa.String(255), nullable=False),
        sa.Column("subject", sa.String(998), nullable=True),
        sa.Column("body_text", sa.Text(), nullable=True),
        sa.Column("body_html", sa.Text(), nullable=True),
        sa.Column("in_reply_to", sa.String(512), nullable=True),
        sa.Column("references", sa.Text(), nullable=True),
        sa.Column("reply_to", sa.String(255), nullable=True),
        sa.Column("attachments", sa.JSON(), nullable=True),
        sa.Column("source_type", sa.String(64), nullable=True),
        sa.Column("source_id", sa.Integer(), nullable=True),
        sa.Column(
            "attempts", sa.Integer(), nullable=False, server_default="0"
        ),
        sa.Column("last_error", sa.String(2000), nullable=True),
        sa.Column("sent_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
    )
    for col in ("status", "source_type", "source_id"):
        op.create_index(
            f"ix_emailoutbox_{col}", "emailoutbox", [col]
        )


def downgrade() -> None:
    op.drop_table("emailoutbox")
    op.drop_table("reclamationattachment")
    op.drop_table("reclamationitem")
    op.drop_table("reclamation")
    op.drop_table("customerreclamationemail")

    op.drop_column("provider", "return_request_email")
    op.drop_column("provider", "return_blocked_brands")
    op.drop_column("provider", "return_window_days")
    op.drop_column("provider", "return_allowed")
    op.drop_column("customer", "return_window_days")

    bind = op.get_bind()
    for enum in (
        email_outbox_status, reclamation_attachment_kind,
        reclamation_item_source, reclamation_type, reclamation_status,
        reclamation_source,
    ):
        enum.drop(bind, checkfirst=True)
