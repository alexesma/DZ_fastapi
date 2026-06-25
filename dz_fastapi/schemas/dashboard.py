from datetime import date, datetime
from typing import Optional

from pydantic import BaseModel, Field


class SupplierPriceTrendPoint(BaseModel):
    pricelist_id: int
    date: date
    uploaded_at: Optional[datetime] = None
    # Артикулы (позиции), а не сумма количеств:
    total_sku_count: int = 0   # всего артикулов в прайсе
    sku_count: int = 0         # из них в наличии (qty > 0)
    stock_total_qty: int = 0   # сумма количеств (оставлено для совместимости)
    avg_price: Optional[float] = None
    step_index_pct: Optional[float] = None        # изменение цены к пред. загрузке
    step_index_smooth_pct: Optional[float] = None
    cumulative_index_pct: Optional[float] = None  # нетто к началу периода
    coverage_pct: Optional[float] = None
    overlap_count: Optional[int] = None
    new_positions: Optional[int] = None           # появилось позиций
    removed_positions: Optional[int] = None        # ушло позиций
    changed_share_pct: Optional[float] = None      # доля изменивших цену, %


class SupplierPriceTrendSeries(BaseModel):
    provider_config_id: int
    provider_id: Optional[int] = None
    provider_name: Optional[str] = None
    provider_config_name: Optional[str] = None
    latest_uploaded_at: Optional[datetime] = None
    net_price_change_pct: Optional[float] = None   # последняя загрузка vs первая
    points: list[SupplierPriceTrendPoint] = Field(default_factory=list)


class SupplierPriceTrendResponse(BaseModel):
    generated_at: datetime
    days: int
    points_limit: int
    smooth_window: int
    series: list[SupplierPriceTrendSeries] = Field(default_factory=list)


class DashboardDailyOrderRow(BaseModel):
    date: date
    customer_order_count: int = 0
    customer_position_count: int = 0
    customer_qty: int = 0
    customer_sum: float = 0.0
    supplier_order_count: int = 0
    supplier_position_count: int = 0
    supplier_qty: int = 0
    supplier_sum: float = 0.0


class DashboardPartnerOrderRow(BaseModel):
    partner_id: int
    partner_name: str
    order_count: int = 0
    position_count: int = 0
    quantity: int = 0
    total_sum: float = 0.0


class DashboardOrderDynamicsSummary(BaseModel):
    customer_order_count: int = 0
    customer_qty: int = 0
    customer_sum: float = 0.0
    supplier_order_count: int = 0
    supplier_qty: int = 0
    supplier_sum: float = 0.0
    purchase_coverage_pct: Optional[float] = None


class DashboardOrderDynamicsResponse(BaseModel):
    generated_at: datetime
    days: int
    summary: DashboardOrderDynamicsSummary
    daily: list[DashboardDailyOrderRow] = Field(default_factory=list)
    customers: list[DashboardPartnerOrderRow] = Field(default_factory=list)
    suppliers: list[DashboardPartnerOrderRow] = Field(default_factory=list)
    suppliers_warehouse: list[DashboardPartnerOrderRow] = Field(
        default_factory=list
    )
    suppliers_cross_docking: list[DashboardPartnerOrderRow] = Field(
        default_factory=list
    )


class DashboardOrderMarginRow(BaseModel):
    period_start: datetime
    customer_id: int
    customer_name: str
    quantity: int = 0
    revenue_total: float = 0.0
    cost_total: float = 0.0
    gross_profit: Optional[float] = None
    margin_percent: Optional[float] = None
    costed_quantity: int = 0
    uncosted_quantity: int = 0


class DashboardOrderMarginResponse(BaseModel):
    generated_at: datetime
    source: str = "customer_orders_estimate"
    note: str
    rows: list[DashboardOrderMarginRow] = Field(default_factory=list)


class DashboardSupplierReliabilityRow(BaseModel):
    provider_id: int
    provider_name: str
    order_count: int = 0
    line_count: int = 0
    evaluated_line_count: int = 0
    ordered_qty: int = 0
    evaluated_qty: int = 0
    received_qty: int = 0
    pending_qty: int = 0
    ordered_sum: float = 0.0
    evaluated_sum: float = 0.0
    received_sum: float = 0.0
    pending_sum: float = 0.0
    fill_rate_pct: Optional[float] = None
    on_time_pct: Optional[float] = None
    late_line_count: int = 0
    avg_lead_days: Optional[float] = None


class DashboardSupplierReliabilityResponse(BaseModel):
    generated_at: datetime
    days: int
    suppliers: list[DashboardSupplierReliabilityRow] = Field(
        default_factory=list
    )


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
    valuation_fallback_skus: int = 0


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
    in_stock_days_30: int = 0
    in_stock_days_90: int = 0
    in_stock_days_180: int = 0
    in_stock_days_365: int = 0
    observed_in_stock_days: int = 0
    unit_cost: Optional[float] = None
    unit_cost_source: Optional[str] = None
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
