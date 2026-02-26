from sqlalchemy import Column, DateTime, Float, Integer, String

from dz_fastapi.core.db import Base
from dz_fastapi.core.time import now_moscow


class PriceWatchItem(Base):
    brand = Column(String(255), nullable=False)
    oem = Column(String(255), nullable=False)
    max_price = Column(Float, nullable=True)
    created_at = Column(DateTime(timezone=True), default=now_moscow)

    last_seen_provider_at = Column(DateTime(timezone=True), nullable=True)
    last_seen_provider_price = Column(Float, nullable=True)
    last_seen_provider_id = Column(Integer, nullable=True)
    last_seen_provider_config_id = Column(Integer, nullable=True)
    last_seen_provider_pricelist_id = Column(Integer, nullable=True)

    last_seen_site_at = Column(DateTime(timezone=True), nullable=True)
    last_seen_site_price = Column(Float, nullable=True)
    last_seen_site_qty = Column(Integer, nullable=True)

    last_notified_provider_at = Column(DateTime(timezone=True), nullable=True)
    last_notified_site_at = Column(DateTime(timezone=True), nullable=True)
