from sqlalchemy import (
    JSON,
    BigInteger,
    Boolean,
    Column,
    Date,
    DateTime,
    ForeignKey,
    Integer,
    String,
    UniqueConstraint,
)
from sqlalchemy.orm import relationship

from dz_fastapi.core.db import Base
from dz_fastapi.core.time import now_moscow


class DiadocIncomingDocument(Base):
    __tablename__ = "diadocincomingdocument"

    environment = Column(String(32), nullable=False, default="staging")
    box_id_guid = Column(String(64), nullable=False, index=True)
    message_id = Column(String(255), nullable=False)
    entity_id = Column(String(255), nullable=False)
    index_key = Column(String(255), nullable=True)
    counteragent_box_id = Column(String(255), nullable=True, index=True)
    file_name = Column(String(500), nullable=True)
    document_number = Column(String(120), nullable=True, index=True)
    document_date = Column(Date, nullable=True, index=True)
    delivery_timestamp_ticks = Column(BigInteger, nullable=True)
    send_timestamp_ticks = Column(BigInteger, nullable=True)
    delivery_at = Column(DateTime(timezone=True), nullable=True)
    sent_at = Column(DateTime(timezone=True), nullable=True)
    provider_id = Column(
        Integer,
        ForeignKey("provider.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    supplier_order_message_id = Column(
        Integer,
        ForeignKey("supplierordermessage.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    local_file_path = Column(String(1024), nullable=True)
    content_sha256 = Column(String(64), nullable=True, index=True)
    status = Column(String(32), nullable=False, default="synced")
    import_error_details = Column(String(2000), nullable=True)
    raw_metadata = Column(JSON, default=dict)
    synced_at = Column(DateTime(timezone=True), default=now_moscow)
    registered_at = Column(DateTime(timezone=True), nullable=True)
    created_at = Column(DateTime(timezone=True), default=now_moscow)
    updated_at = Column(
        DateTime(timezone=True),
        default=now_moscow,
        onupdate=now_moscow,
    )

    provider = relationship("Provider", lazy="selectin")
    supplier_order_message = relationship(
        "SupplierOrderMessage",
        lazy="selectin",
    )

    __table_args__ = (
        UniqueConstraint(
            "environment",
            "box_id_guid",
            "message_id",
            "entity_id",
            name="uq_diadoc_incoming_document_source",
        ),
    )


class DiadocOutgoingDocument(Base):
    __tablename__ = "diadocoutgoingdocument"

    environment = Column(String(32), nullable=False, default="staging")
    from_box_id_guid = Column(String(64), nullable=False, index=True)
    to_box_id_guid = Column(String(64), nullable=False, index=True)
    customer_id = Column(
        Integer,
        ForeignKey("customer.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    provider_id = Column(
        Integer,
        ForeignKey("provider.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    source_type = Column(String(64), nullable=True, index=True)
    source_id = Column(Integer, nullable=True, index=True)
    type_named_id = Column(
        String(120), nullable=False, default="Nonformalized"
    )
    document_function = Column(String(120), nullable=True)
    document_version = Column(String(120), nullable=True)
    file_name = Column(String(500), nullable=False)
    document_number = Column(String(120), nullable=True, index=True)
    document_date = Column(Date, nullable=True, index=True)
    local_file_path = Column(String(1024), nullable=False)
    content_sha256 = Column(String(64), nullable=True, index=True)
    comment = Column(String(5000), nullable=True)
    need_recipient_signature = Column(Boolean, default=False, nullable=False)
    need_receipt = Column(Boolean, default=True, nullable=False)
    is_draft = Column(Boolean, default=True, nullable=False)
    message_id = Column(String(255), nullable=True, index=True)
    entity_id = Column(String(255), nullable=True, index=True)
    status = Column(String(32), nullable=False, default="draft")
    error_details = Column(String(2000), nullable=True)
    metadata_json = Column("metadata", JSON, default=dict)
    raw_response = Column(JSON, default=dict)
    created_at = Column(DateTime(timezone=True), default=now_moscow)
    sent_at = Column(DateTime(timezone=True), nullable=True)
    updated_at = Column(
        DateTime(timezone=True),
        default=now_moscow,
        onupdate=now_moscow,
    )

    customer = relationship("Customer", lazy="selectin")
    provider = relationship("Provider", lazy="selectin")
