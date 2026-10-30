"""Reliable outbox and delivery batches for 1C integration."""

from enum import StrEnum, unique
from uuid import uuid4

from sqlalchemy import JSON, Boolean, Column, DateTime
from sqlalchemy import Enum as SAEnum
from sqlalchemy import ForeignKey, Index, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import relationship

from dz_fastapi.core.db import Base
from dz_fastapi.core.time import now_moscow


@unique
class OneCExchangeEventStatus(StrEnum):
    PENDING = "pending"
    IN_FLIGHT = "in_flight"
    SUCCEEDED = "succeeded"
    ERROR = "error"


@unique
class OneCExchangeBatchStatus(StrEnum):
    SENT = "sent"
    SUCCEEDED = "succeeded"
    ERROR = "error"


class OneCExchangeEvent(Base):
    """Immutable business-event snapshot waiting for acknowledgement from 1C."""

    __tablename__ = "onecexchangeevent"

    event_uid = Column(
        String(36),
        nullable=False,
        unique=True,
        index=True,
        default=lambda: str(uuid4()),
    )
    entity_type = Column(String(50), nullable=False, index=True)
    entity_id = Column(Integer, nullable=False, index=True)
    event_type = Column(String(50), nullable=False, index=True)
    payload_version = Column(Integer, nullable=False, default=1)
    payload = Column(JSON, nullable=False)
    idempotency_key = Column(String(160), nullable=False, unique=True, index=True)
    ed_only = Column(Boolean, nullable=False, default=False)
    status = Column(
        SAEnum(
            OneCExchangeEventStatus,
            name="onecexchangeeventstatus",
            values_callable=lambda enum: [item.value for item in enum],
        ),
        nullable=False,
        default=OneCExchangeEventStatus.PENDING,
        index=True,
    )
    attempt_count = Column(Integer, nullable=False, default=0)
    next_attempt_at = Column(DateTime(timezone=True), nullable=True, index=True)
    last_attempt_at = Column(DateTime(timezone=True), nullable=True)
    confirmed_at = Column(DateTime(timezone=True), nullable=True)
    external_id = Column(String(100), nullable=True, index=True)
    last_error = Column(Text, nullable=True)
    # EnterpriseData is a separate transport from CommerceML/JSON.  A read
    # receipt from UNF is not proof that the document was posted there.
    ed_message_no = Column(Integer, nullable=True, index=True)
    ed_sent_at = Column(DateTime(timezone=True), nullable=True)
    ed_transport_confirmed_at = Column(DateTime(timezone=True), nullable=True)
    ed_last_error = Column(Text, nullable=True)
    # Что УНФ прислала обратно о нашем документе: её номер, дата и признак
    # проведения (None — в сообщении этого признака не было).
    ed_unf_ref = Column(String(36), nullable=True)
    ed_unf_number = Column(String(50), nullable=True, index=True)
    ed_unf_date = Column(DateTime(timezone=True), nullable=True)
    ed_unf_posted = Column(Boolean, nullable=True)
    ed_unf_received_at = Column(DateTime(timezone=True), nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False, default=now_moscow, index=True)
    updated_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=now_moscow,
        onupdate=now_moscow,
    )

    batch_items = relationship(
        "OneCExchangeBatchItem",
        back_populates="event",
        cascade="all, delete-orphan",
        lazy="selectin",
    )

    __table_args__ = (
        Index(
            "idx_onec_event_queue",
            "status",
            "next_attempt_at",
            "created_at",
        ),
        Index(
            "idx_onec_event_entity",
            "entity_type",
            "entity_id",
            "created_at",
        ),
    )


class OneCEnterpriseConfig(Base):
    """Activation boundary for the native UNF FTP exchange."""

    __tablename__ = "oncenterpriseconfig"

    enabled = Column(Boolean, nullable=False, default=False)
    send_receipts = Column(Boolean, nullable=False, default=True)
    send_shipments = Column(Boolean, nullable=False, default=False)
    batch_size = Column(Integer, nullable=False, default=1)
    first_event_id = Column(Integer, nullable=True)
    updated_at = Column(DateTime(timezone=True), nullable=False, default=now_moscow)


class OneCExchangeBatch(Base):
    """One deterministic delivery attempt acknowledged as a whole by 1C."""

    __tablename__ = "onecexchangebatch"

    batch_uid = Column(
        String(36),
        nullable=False,
        unique=True,
        index=True,
        default=lambda: str(uuid4()),
    )
    channel = Column(String(32), nullable=False, index=True)
    status = Column(
        SAEnum(
            OneCExchangeBatchStatus,
            name="onecexchangebatchstatus",
            values_callable=lambda enum: [item.value for item in enum],
        ),
        nullable=False,
        default=OneCExchangeBatchStatus.SENT,
        index=True,
    )
    content_hash = Column(String(64), nullable=False)
    attempt_count = Column(Integer, nullable=False, default=1)
    sent_at = Column(DateTime(timezone=True), nullable=False, default=now_moscow)
    last_sent_at = Column(DateTime(timezone=True), nullable=False, default=now_moscow)
    confirmed_at = Column(DateTime(timezone=True), nullable=True)
    last_error = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False, default=now_moscow, index=True)

    items = relationship(
        "OneCExchangeBatchItem",
        back_populates="batch",
        cascade="all, delete-orphan",
        lazy="selectin",
        order_by="OneCExchangeBatchItem.id",
    )

    __table_args__ = (Index("idx_onec_batch_channel_status", "channel", "status", "created_at"),)


class OneCExchangeBatchItem(Base):
    __tablename__ = "onecexchangebatchitem"

    batch_id = Column(
        Integer,
        ForeignKey("onecexchangebatch.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    event_id = Column(
        Integer,
        ForeignKey("onecexchangeevent.id", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )
    created_at = Column(DateTime(timezone=True), nullable=False, default=now_moscow)

    batch = relationship("OneCExchangeBatch", back_populates="items")
    event = relationship("OneCExchangeEvent", back_populates="batch_items", lazy="joined")

    __table_args__ = (
        UniqueConstraint("batch_id", "event_id", name="uq_onec_exchange_batch_event"),
    )
