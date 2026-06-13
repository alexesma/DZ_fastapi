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


class InventoryDashboardSummary(BaseModel):
    total_skus: int = 0
    in_stock_skus: int = 0
    out_of_stock_skus: int = 0
    out_of_stock_with_demand_skus: int = 0
    urgent_count: int = 0
    dead_stock_skus: int = 0
    slow_stock_skus: int = 0
    healthy_skus: int = 0
    stock_value: float = 0.0
    dead_stock_value: float = 0.0
    service_level_pct: Optional[float] = None
    inventory_turnover: Optional[float] = None


class InventoryAbcXyzCell(BaseModel):
    abc_class: str
    xyz_class: str
    sku_count: int = 0
    stock_value: float = 0.0
    annual_sales_value: float = 0.0


class InventoryDashboardRow(BaseModel):
    oem_number: str
    brand_name: Optional[str] = None
    autopart_name: Optional[str] = None
    autopart_id: Optional[int] = None
    state: str
    current_quantity: int = 0
    in_transit_qty: int = 0
    avg_daily: Optional[float] = None
    estimated_days_left: Optional[int] = None
    sold_last_30_days: int = 0
    sold_last_90_days: int = 0
    sold_last_365_days: int = 0
    unit_cost: Optional[float] = None
    frozen_value: Optional[float] = None
    sale_price: Optional[float] = None
    abc_class: Optional[str] = None
    xyz_class: Optional[str] = None


class InventoryDashboardResponse(BaseModel):
    generated_at: datetime
    provider_config_id: int
    provider_name: Optional[str] = None
    summary: InventoryDashboardSummary
    abc_xyz_matrix: list[InventoryAbcXyzCell] = Field(default_factory=list)
    urgent_to_order: list[InventoryDashboardRow] = Field(default_factory=list)
    dead_stock: list[InventoryDashboardRow] = Field(default_factory=list)
    slow_movers: list[InventoryDashboardRow] = Field(default_factory=list)
    out_of_stock_with_demand: list[InventoryDashboardRow] = Field(
        default_factory=list
    )
    history_pending_note: Optional[str] = None
