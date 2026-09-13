"""Add Parts-Soft product outbox and document snapshots.

Revision ID: d6f9a2c4e7b1
Revises: c5e8b2a71d43
"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

revision: str = "d6f9a2c4e7b1"
down_revision: Union[str, None] = "c5e8b2a71d43"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "partssoftproductoutbox",
        sa.Column("autopart_id", sa.Integer(), nullable=False),
        sa.Column("external_product_id", sa.BigInteger(), nullable=True),
        sa.Column("operation", sa.String(length=16), nullable=False, server_default="upsert"),
        sa.Column("status", sa.String(length=16), nullable=False, server_default="pending"),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column(
            "available_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.Column("locked_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.Column("sent_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.UniqueConstraint("autopart_id", name="uq_partssoft_product_outbox_autopart"),
    )
    op.create_index("ix_partssoftproductoutbox_status", "partssoftproductoutbox", ["status"])
    op.create_index(
        "ix_partssoftproductoutbox_autopart_id", "partssoftproductoutbox", ["autopart_id"]
    )
    op.create_index(
        "ix_partssoftproductoutbox_external_product_id",
        "partssoftproductoutbox",
        ["external_product_id"],
    )

    op.create_table(
        "partssoftdocumentsnapshot",
        sa.Column("document_type", sa.String(length=32), nullable=False),
        sa.Column("external_document_id", sa.BigInteger(), nullable=False),
        sa.Column("external_counterparty_id", sa.BigInteger(), nullable=True),
        sa.Column("document_number", sa.String(length=120), nullable=True),
        sa.Column("document_date", sa.Date(), nullable=True),
        sa.Column("external_status", sa.String(length=64), nullable=True),
        sa.Column("total_amount", sa.String(length=64), nullable=True),
        sa.Column("vat_rate", sa.String(length=32), nullable=True),
        sa.Column("payload", sa.JSON(), nullable=False, server_default=sa.text("'{}'::json")),
        sa.Column("import_status", sa.String(length=32), nullable=False, server_default="pending"),
        sa.Column("import_error", sa.Text(), nullable=True),
        sa.Column("local_payment_invoice_id", sa.Integer(), nullable=True),
        sa.Column("local_supplier_receipt_id", sa.Integer(), nullable=True),
        sa.Column(
            "first_synced_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "last_synced_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.ForeignKeyConstraint(
            ["local_payment_invoice_id"], ["paymentinvoice.id"], ondelete="SET NULL"
        ),
        sa.ForeignKeyConstraint(
            ["local_supplier_receipt_id"], ["supplierreceipt.id"], ondelete="SET NULL"
        ),
        sa.UniqueConstraint(
            "document_type", "external_document_id", name="uq_partssoft_document_type_external_id"
        ),
    )
    for column in (
        "document_type",
        "external_document_id",
        "external_counterparty_id",
        "document_number",
        "document_date",
        "import_status",
        "local_payment_invoice_id",
        "local_supplier_receipt_id",
    ):
        op.create_index(
            f"ix_partssoftdocumentsnapshot_{column}",
            "partssoftdocumentsnapshot",
            [column],
        )

    op.execute(
        """
        CREATE FUNCTION queue_partssoft_product_change() RETURNS trigger AS $$
        DECLARE
            target_id integer;
            external_id bigint;
            target_operation varchar(16);
        BEGIN
            target_id := COALESCE(NEW.id, OLD.id);
            external_id := COALESCE(NEW.partssoft_product_id, OLD.partssoft_product_id);
            target_operation := CASE WHEN TG_OP = 'DELETE' THEN 'delete' ELSE 'upsert' END;
            INSERT INTO partssoftproductoutbox (
                autopart_id, external_product_id, operation, status, attempts,
                available_at, created_at, updated_at
            ) VALUES (
                target_id, external_id, target_operation, 'pending', 0,
                now(), now(), now()
            )
            ON CONFLICT (autopart_id) DO UPDATE SET
                external_product_id = COALESCE(
                    EXCLUDED.external_product_id,
                    partssoftproductoutbox.external_product_id
                ),
                operation = EXCLUDED.operation,
                status = 'pending',
                attempts = 0,
                last_error = NULL,
                available_at = now(),
                locked_at = NULL,
                sent_at = NULL,
                updated_at = now();
            RETURN COALESCE(NEW, OLD);
        END;
        $$ LANGUAGE plpgsql
        """
    )
    op.execute(
        """
        CREATE TRIGGER trg_partssoft_product_insert
        AFTER INSERT ON autopart
        FOR EACH ROW EXECUTE FUNCTION queue_partssoft_product_change()
        """
    )
    op.execute(
        """
        CREATE TRIGGER trg_partssoft_product_update
        AFTER UPDATE OF brand_id, oem_number, name, description, width, height,
            length, weight, comment, barcode, honest_sign_category,
            applicability, tnved_code, okpd2_code, certification_required,
            eac_cert_number, eac_cert_url, eac_cert_valid_until,
            regulatory_source
        ON autopart
        FOR EACH ROW EXECUTE FUNCTION queue_partssoft_product_change()
        """
    )
    op.execute(
        """
        CREATE TRIGGER trg_partssoft_product_delete
        AFTER DELETE ON autopart
        FOR EACH ROW EXECUTE FUNCTION queue_partssoft_product_change()
        """
    )
    op.execute(
        """
        CREATE FUNCTION queue_partssoft_photo_change() RETURNS trigger AS $$
        DECLARE
            target_autopart_id integer;
            external_id bigint;
        BEGIN
            target_autopart_id := CASE
                WHEN TG_OP = 'DELETE' THEN OLD.autopart_id
                ELSE NEW.autopart_id
            END;
            SELECT partssoft_product_id INTO external_id
            FROM autopart WHERE id = target_autopart_id;
            INSERT INTO partssoftproductoutbox (
                autopart_id, external_product_id, operation, status, attempts,
                available_at, created_at, updated_at
            ) VALUES (
                target_autopart_id, external_id, 'upsert', 'pending', 0,
                now(), now(), now()
            )
            ON CONFLICT (autopart_id) DO UPDATE SET
                external_product_id = COALESCE(
                    EXCLUDED.external_product_id,
                    partssoftproductoutbox.external_product_id
                ),
                operation = 'upsert',
                status = 'pending',
                attempts = 0,
                last_error = NULL,
                available_at = now(),
                locked_at = NULL,
                sent_at = NULL,
                updated_at = now();
            RETURN COALESCE(NEW, OLD);
        END;
        $$ LANGUAGE plpgsql
        """
    )
    op.execute(
        """
        CREATE TRIGGER trg_partssoft_photo_change
        AFTER INSERT OR UPDATE OR DELETE ON photo
        FOR EACH ROW EXECUTE FUNCTION queue_partssoft_photo_change()
        """
    )


def downgrade() -> None:
    op.execute("DROP TRIGGER IF EXISTS trg_partssoft_photo_change ON photo")
    op.execute("DROP FUNCTION IF EXISTS queue_partssoft_photo_change()")
    op.execute("DROP TRIGGER IF EXISTS trg_partssoft_product_delete ON autopart")
    op.execute("DROP TRIGGER IF EXISTS trg_partssoft_product_update ON autopart")
    op.execute("DROP TRIGGER IF EXISTS trg_partssoft_product_insert ON autopart")
    op.execute("DROP FUNCTION IF EXISTS queue_partssoft_product_change()")
    op.drop_table("partssoftdocumentsnapshot")
    op.drop_table("partssoftproductoutbox")
