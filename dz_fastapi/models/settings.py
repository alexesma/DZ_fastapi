from datetime import datetime, timezone

from sqlalchemy import (JSON, Boolean, Column, Date, DateTime, ForeignKey,
                        Integer, String)
from sqlalchemy.orm import relationship

from dz_fastapi.core.db import Base


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
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )


class PriceListStaleAlert(Base):
    provider_id = Column(Integer, ForeignKey('provider.id'), nullable=False)
    provider_config_id = Column(
        Integer, ForeignKey('providerpricelistconfig.id'), nullable=False
    )
    days_diff = Column(Integer, nullable=False)
    last_price_date = Column(Date, nullable=False)
    created_at = Column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )

    provider = relationship('Provider')


class PriceCheckLog(Base):
    status = Column(String(32), nullable=False)
    message = Column(String(255), nullable=True)
    checked_at = Column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )
