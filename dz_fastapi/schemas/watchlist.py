from datetime import datetime
from typing import Optional

from pydantic import BaseModel, ConfigDict


class PriceWatchItemBase(BaseModel):
    brand: str
    oem: str
    max_price: Optional[float] = None


class PriceWatchItemCreate(PriceWatchItemBase):
    pass


class PriceWatchItemUpdate(BaseModel):
    max_price: Optional[float] = None


class PriceWatchItemOut(PriceWatchItemBase):
    id: int
    created_at: datetime
    last_seen_provider_at: Optional[datetime] = None
    last_seen_provider_price: Optional[float] = None
    last_seen_provider_id: Optional[int] = None
    last_seen_provider_config_id: Optional[int] = None
    last_seen_provider_pricelist_id: Optional[int] = None
    last_seen_site_at: Optional[datetime] = None
    last_seen_site_price: Optional[float] = None

    model_config = ConfigDict(from_attributes=True)


class PriceWatchListPage(BaseModel):
    items: list[PriceWatchItemOut]
    page: int
    page_size: int
    total: int
