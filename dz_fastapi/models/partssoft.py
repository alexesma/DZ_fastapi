"""Persistent exchange state for the Parts-Soft integration."""

from sqlalchemy import (
    JSON,
    BigInteger,
    Column,
    Date,
    DateTime,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
)

from dz_fastapi.core.db import Base
from dz_fastapi.core.time import now_moscow


class PartsSoftProductOutbox(Base):
    """Coalesced local product change waiting for delivery to Parts-Soft."""

    autopart_id = Column(Integer, nullable=False, unique=True, index=True)
    external_product_id = Column(BigInteger, nullable=True, index=True)
    operation = Column(String(16), nullable=False, default="upsert")
    status = Column(String(16), nullable=False, default="pending", index=True)
    attempts = Column(Integer, nullable=False, default=0)
    last_error = Column(Text, nullable=True)
    available_at = Column(DateTime(timezone=True), default=now_moscow, nullable=False)
    locked_at = Column(DateTime(timezone=True), nullable=True)
    created_at = Column(DateTime(timezone=True), default=now_moscow, nullable=False)
    updated_at = Column(
        DateTime(timezone=True),
        default=now_moscow,
        onupdate=now_moscow,
        nullable=False,
    )
    sent_at = Column(DateTime(timezone=True), nullable=True)


class PartsSoftDocumentSnapshot(Base):
    """Idempotent local copy of a customer or supplier invoice."""

    document_type = Column(String(32), nullable=False, index=True)
    external_document_id = Column(BigInteger, nullable=False, index=True)
    external_counterparty_id = Column(BigInteger, nullable=True, index=True)
    document_number = Column(String(120), nullable=True, index=True)
    document_date = Column(Date, nullable=True, index=True)
    external_status = Column(String(64), nullable=True)
    total_amount = Column(String(64), nullable=True)
    vat_rate = Column(String(32), nullable=True)
    payload = Column(JSON, nullable=False, default=dict)
    import_status = Column(String(32), nullable=False, default="pending", index=True)
    import_error = Column(Text, nullable=True)
    local_payment_invoice_id = Column(
        Integer,
        ForeignKey("paymentinvoice.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    local_supplier_receipt_id = Column(
        Integer,
        ForeignKey("supplierreceipt.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    first_synced_at = Column(DateTime(timezone=True), default=now_moscow, nullable=False)
    last_synced_at = Column(DateTime(timezone=True), default=now_moscow, nullable=False)

    __table_args__ = (
        UniqueConstraint(
            "document_type",
            "external_document_id",
            name="uq_partssoft_document_type_external_id",
        ),
    )
