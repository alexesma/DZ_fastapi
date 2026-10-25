"""Windows printer registration, employee access and durable print queue."""

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import relationship

from dz_fastapi.core.db import Base
from dz_fastapi.core.time import now_moscow


class PrintDevice(Base):
    __tablename__ = "printdevice"
    __table_args__ = (
        UniqueConstraint("machine_id", "system_name", name="uq_printdevice_machine_name"),
    )

    machine_id = Column(String(128), nullable=False, index=True)
    system_name = Column(String(255), nullable=False)
    display_name = Column(String(255), nullable=False)
    active = Column(Boolean, nullable=False, default=True)
    last_seen_at = Column(DateTime(timezone=True), nullable=False, default=now_moscow)
    created_at = Column(DateTime(timezone=True), nullable=False, default=now_moscow)


class PrintAssignment(Base):
    __tablename__ = "printassignment"
    __table_args__ = (
        UniqueConstraint(
            "user_id", "device_id", "kind",
            name="uq_printassignment_user_device_kind",
        ),
        Index("ix_printassignment_user_kind", "user_id", "kind"),
    )

    user_id = Column(Integer, ForeignKey("app_user.id", ondelete="CASCADE"), nullable=False)
    device_id = Column(Integer, ForeignKey("printdevice.id", ondelete="CASCADE"), nullable=False)
    kind = Column(String(16), nullable=False)
    is_default = Column(Boolean, nullable=False, default=False)
    device = relationship("PrintDevice", lazy="joined")


class PrintJob(Base):
    __tablename__ = "printjob"
    __table_args__ = (
        Index("ix_printjob_device_status", "device_id", "status", "id"),
    )

    request_key = Column(String(64), nullable=False, unique=True)
    user_id = Column(Integer, ForeignKey("app_user.id", ondelete="SET NULL"), nullable=True)
    device_id = Column(Integer, ForeignKey("printdevice.id", ondelete="RESTRICT"), nullable=False)
    kind = Column(String(16), nullable=False)
    title = Column(String(160), nullable=False)
    html = Column(Text, nullable=False)
    status = Column(String(16), nullable=False, default="pending", index=True)
    attempts = Column(Integer, nullable=False, default=0)
    claimed_by = Column(String(128), nullable=True)
    error = Column(Text, nullable=True)
    width_mm = Column(Float, nullable=True)
    height_mm = Column(Float, nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False, default=now_moscow)
    claimed_at = Column(DateTime(timezone=True), nullable=True)
    printed_at = Column(DateTime(timezone=True), nullable=True)
    device = relationship("PrintDevice", lazy="joined")
