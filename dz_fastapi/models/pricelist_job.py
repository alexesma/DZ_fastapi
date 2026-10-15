"""Durable manual pricelist jobs, executed by the scheduler, never an HTTP request."""

from sqlalchemy import Boolean, Column, DateTime, ForeignKey, Index, Integer, String, Text, text

from dz_fastapi.core.db import Base
from dz_fastapi.core.time import now_moscow


class CustomerPricelistJob(Base):
    customer_id = Column(Integer, ForeignKey("customer.id", ondelete="CASCADE"), nullable=False)
    config_id = Column(
        Integer, ForeignKey("customerpricelistconfig.id", ondelete="CASCADE"), nullable=False
    )
    requested_by_id = Column(Integer, ForeignKey("app_user.id", ondelete="SET NULL"))
    action = Column(String(16), nullable=False)  # build / send
    status = Column(String(16), nullable=False, default="queued")
    progress = Column(Integer, nullable=False, default=0)
    stage = Column(String(255), nullable=False, default="Ожидает начала обработки")
    pricelist_id = Column(Integer, ForeignKey("customerpricelist.id", ondelete="SET NULL"))
    use_saved_file = Column(Boolean, nullable=False, default=False, server_default="false")
    delivery_status = Column(String(32))
    error = Column(Text)
    created_at = Column(DateTime(timezone=True), nullable=False, default=now_moscow)
    started_at = Column(DateTime(timezone=True))
    updated_at = Column(DateTime(timezone=True), nullable=False, default=now_moscow)
    finished_at = Column(DateTime(timezone=True))

    __table_args__ = (
        Index("ix_customerpricelistjob_config_id", "config_id"),
        Index(
            "uq_customerpricelistjob_active_config",
            "config_id",
            unique=True,
            postgresql_where=text("status IN ('queued', 'running')"),
        ),
    )
