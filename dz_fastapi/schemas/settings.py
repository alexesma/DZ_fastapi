from datetime import date, datetime
from typing import List, Optional

from pydantic import BaseModel, ConfigDict, Field


class PriceCheckScheduleBase(BaseModel):
    enabled: bool = True
    days: List[str] = Field(default_factory=list)
    times: List[str] = Field(default_factory=list)


class PriceCheckScheduleUpdate(PriceCheckScheduleBase):
    pass


class PriceCheckScheduleOut(PriceCheckScheduleBase):
    id: int
    last_checked_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    model_config = ConfigDict(from_attributes=True)


class PriceListStaleAlertOut(BaseModel):
    id: int
    provider_id: int
    provider_config_id: int
    days_diff: int
    last_price_date: date
    created_at: datetime
    model_config = ConfigDict(from_attributes=True)


class PriceCheckLogOut(BaseModel):
    id: int
    status: str
    message: Optional[str] = None
    checked_at: datetime
    model_config = ConfigDict(from_attributes=True)
