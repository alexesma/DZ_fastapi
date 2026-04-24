from sqlalchemy import (JSON, BigInteger, Boolean, Column, Date, DateTime,
                        ForeignKey, Integer, String)
from sqlalchemy.orm import relationship

from dz_fastapi.core.db import Base
from dz_fastapi.core.time import now_moscow


class PriceCheckSchedule(Base):
    enabled = Column(Boolean, default=True)
    days = Column(JSON, default=[])
    times = Column(JSON, default=[])
    last_checked_at = Column(
        DateTime(timezone=True),
        nullable=True,
    )
    updated_at = Column(
        DateTime(timezone=True),
        default=now_moscow,
        onupdate=now_moscow,
    )


class SchedulerSetting(Base):
    key = Column(String(64), unique=True, nullable=False, index=True)
    enabled = Column(Boolean, default=True)
    days = Column(JSON, default=[])
    times = Column(JSON, default=[])
    last_run_at = Column(DateTime(timezone=True), nullable=True)
    updated_at = Column(
        DateTime(timezone=True),
        default=now_moscow,
        onupdate=now_moscow,
    )


class CustomerOrderInboxSettings(Base):
    lookback_days = Column(Integer, default=1)
    mark_seen = Column(Boolean, default=False)
    error_file_retention_days = Column(Integer, default=5)
    supplier_response_lookback_days = Column(Integer, default=14)
    supplier_response_auto_close_stale_enabled = Column(
        Boolean,
        default=True,
    )
    supplier_response_stale_days = Column(Integer, default=7)
    supplier_order_stub_enabled = Column(Boolean, default=True)
    supplier_order_stub_email = Column(
        String(255),
        default='info@dragonzap.ru',
    )
    updated_at = Column(
        DateTime(timezone=True),
        default=now_moscow,
        onupdate=now_moscow,
    )


class SystemMetricSnapshot(Base):
    created_at = Column(
        DateTime(timezone=True), default=now_moscow, nullable=False
    )
    db_size_bytes = Column(BigInteger, nullable=True)
    disk_total_bytes = Column(BigInteger, nullable=True)
    disk_free_bytes = Column(BigInteger, nullable=True)
    mem_total_bytes = Column(BigInteger, nullable=True)
    mem_available_bytes = Column(BigInteger, nullable=True)


class PriceListStaleAlert(Base):
    provider_id = Column(Integer, ForeignKey('provider.id'), nullable=False)
    provider_config_id = Column(
        Integer, ForeignKey('providerpricelistconfig.id'), nullable=False
    )
    days_diff = Column(Integer, nullable=False)
    last_price_date = Column(Date, nullable=False)
    created_at = Column(
        DateTime(timezone=True), default=now_moscow
    )

    provider = relationship('Provider')


class PriceCheckLog(Base):
    status = Column(String(32), nullable=False)
    message = Column(String(255), nullable=True)
    checked_at = Column(DateTime(timezone=True), default=now_moscow)


class SupplierHoliday(Base):
    """Manual holiday calendar for supplier auto-refusal logic.

    Records here either:
      - Add a non-working day (is_working_day=False, default) — e.g. a public
        holiday that ``python-holidays`` does not know about, or a custom
        company day-off.
      - Override an auto-detected holiday as a working day
        (is_working_day=True)
        — e.g. when a public holiday is moved to another day and this day
        becomes a normal working day.
      - Mark a weekend as a working day (is_working_day=True) — перенос
        рабочего дня на субботу.
    """

    date = Column(Date, nullable=False, unique=True, index=True)
    description = Column(String(255), nullable=True)
    # False = non-working day (holiday); True = forced working day (override)
    is_working_day = Column(Boolean, default=False, nullable=False)
    created_at = Column(DateTime(timezone=True), default=now_moscow)
