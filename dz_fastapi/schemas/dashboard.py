from datetime import date, datetime
from typing import Optional

from pydantic import BaseModel, Field


class SupplierPriceTrendPoint(BaseModel):
    pricelist_id: int
    date: date
    sku_count: int = 0
    stock_total_qty: int = 0
    avg_price: Optional[float] = None
    step_index_pct: Optional[float] = None
    step_index_smooth_pct: Optional[float] = None
    coverage_pct: Optional[float] = None
    overlap_count: Optional[int] = None


class SupplierPriceTrendSeries(BaseModel):
    provider_config_id: int
    provider_id: Optional[int] = None
    provider_name: Optional[str] = None
    provider_config_name: Optional[str] = None
    points: list[SupplierPriceTrendPoint] = Field(default_factory=list)


class SupplierPriceTrendResponse(BaseModel):
    generated_at: datetime
    days: int
    points_limit: int
    smooth_window: int
    series: list[SupplierPriceTrendSeries] = Field(default_factory=list)
