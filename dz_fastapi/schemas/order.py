from datetime import date, datetime
from decimal import Decimal
from typing import Any, List, Optional

from pydantic import BaseModel, Field

from dz_fastapi.models.autopart import TYPE_SEND_METHOD, TYPE_SUPPLIER_DECISION_STATUS
from dz_fastapi.models.partner import TYPE_ORDER_ITEM_STATUS, TYPE_STATUS_ORDER
from dz_fastapi.schemas.autopart import AutopartOfferRow


class SupplierOfferOut(BaseModel):
    autopart_id: int = Field(..., description="ID автозапчасти")
    oem_number: str = Field(..., description="OEM номер детали")
    autopart_name: str = Field(..., description="Название детали")
    supplier_id: int = Field(..., description="ID поставщика")
    supplier_name: str = Field(..., description="Имя поставщика")
    price: float = Field(..., description="Цена за штуку")
    quantity: int = Field(..., description="Количество к заказу")
    total_cost: float = Field(..., description="Общая стоимость")
    qnt: int = Field(..., description="Количество на остатках у поставщика")
    min_qnt: int = Field(..., description="Минимальная кратность заказа")
    min_delivery_day: int = Field(
        ..., description="Минимальный срок доставки в днях"
    )
    max_delivery_day: int = Field(
        ..., description="Максимальный срок доставки в днях"
    )
    historical_min_price: float = Field(
        ..., description="Исторически минимальная цена"
    )
    sup_logo: str = Field(..., description="Абривиатура поставщика")
    brand_name: str = Field(..., description="Имя бренда")
    hash_key: Optional[str] = Field(None, description="Hash ключ, если есть")
    system_hash: Optional[str] = Field(
        None, description="System hash, если есть"
    )


class SupplierOffersResponse(BaseModel):
    offers: list[SupplierOfferOut]


class ConfirmedOfferOut(BaseModel):
    autopart_id: int = Field(..., description="ID автозапчасти")
    supplier_id: int = Field(..., description="ID поставщика")
    quantity: int = Field(..., description="Количество к заказу")
    confirmed_price: float = Field(..., description="Цена за штуку")
    status: TYPE_SUPPLIER_DECISION_STATUS = Field(
        ..., description="Статус подтверждения"
    )
    brand_name: Optional[str] = Field(..., description="Имя бренда для заказа")
    min_delivery_day: int = Field(..., description="Минимальный срок доставки")
    max_delivery_day: int = Field(
        ..., description="Максимальный срок доставки"
    )
    send_method: Optional[TYPE_SEND_METHOD] = Field(
        None, description="Способ отправки"
    )
    model_config = {"from_attributes": True, "use_enum_values": True}


class ConfirmedOffersResponse(BaseModel):
    confirmed_offers: List[ConfirmedOfferOut]
    total_items: int


class OrderPositionOut(BaseModel):
    autopart_id: Optional[int] = Field(None, description="ID автозапчасти")
    oem_number: str = Field(..., description="OEM номер детали")
    brand_name: str = Field(..., description="Имя бренда")
    autopart_name: Optional[str] = Field(None, description="Название детали")
    supplier_id: Optional[int] = Field(None, description="ID поставщика")
    supplier_name: Optional[str] = Field(None, description="Имя поставщика")
    price_name: Optional[str] = Field(
        None, description="Название витрины/прайса поставщика на сайте"
    )
    sup_logo: Optional[str] = Field(
        None, description="Маркер/логотип поставщика на сайте"
    )
    quantity: int = Field(..., description="Количество к заказу")
    confirmed_price: float = Field(..., description="Цена за штуку")
    min_delivery_day: Optional[int] = Field(
        None, description="Минимальный срок доставки в днях"
    )
    max_delivery_day: Optional[int] = Field(
        None, description="Максимальный срок доставки в днях"
    )
    status: TYPE_SUPPLIER_DECISION_STATUS = Field(
        ..., description="Статус подтверждения"
    )
    created_at: Optional[datetime] = Field(
        None, description="Время создания заказа"
    )
    updated_at: Optional[datetime] = Field(
        None, description="Время изменения заказа"
    )
    tracking_uuid: Optional[str] = Field(None, description="Уникальный индекс")
    hash_key: Optional[str] = Field(None, description="Hash ключ")
    system_hash: Optional[str] = Field(None, description="System hash ключ")
    model_config = {"use_enum_values": True}


class SendApiResponse(BaseModel):
    total_items: int = Field(..., description="Общее количество позиций")
    successful_items: int = Field(
        ..., description="Успешно отправленных позиций"
    )
    failed_items: int = Field(..., description="Неудачных позиций")
    results: List[dict] = Field(..., description="Детали по каждой позиции")
    order_id: Optional[int] = Field(None, description="Order ID")
    order_number: Optional[str] = Field(None, description="Order Numer")


class SupplierOrderOut(BaseModel):
    supplier_id: int = Field(..., description="ID поставщика")
    supplier_name: str = Field(..., description="Имя поставщика")
    total_sum: float = Field(..., description="Общая стоимость")
    min_delivery_day: Optional[int] = Field(
        None, description="Минимальный срок доставки в днях"
    )
    max_delivery_day: Optional[int] = Field(
        None, description="Максимальный срок доставки в днях"
    )
    order_status: TYPE_SUPPLIER_DECISION_STATUS = Field(
        ..., description="Статус подтверждения всего заказ"
    )
    positions: List[OrderPositionOut]
    send_method: TYPE_SEND_METHOD = Field(..., description="Метод заказа")
    model_config = {"from_attributes": True, "use_enum_values": True}


class UpdatedItemInfo(BaseModel):
    tracking_uuid: str = Field(..., description="tracking UUID для обновления")
    old_status: TYPE_SUPPLIER_DECISION_STATUS = Field(
        ..., description="Старый статус позиции для заказа"
    )
    new_status: TYPE_SUPPLIER_DECISION_STATUS = Field(
        ..., description="Новый статус"
    )


class UpdatePositionStatusResponse(BaseModel):
    message: str
    updated_count: int
    updated_items: List[UpdatedItemInfo]


class UpdatePositionStatusRequest(BaseModel):
    tracking_uuids: list[str] = Field(
        ..., description="Список tracking UUID для обновления"
    )
    status: TYPE_SUPPLIER_DECISION_STATUS = Field(
        ..., description="Новый статус"
    )
    model_config = {"use_enum_values": True}


class OrderItemIn(BaseModel):
    order_id: int
    autopart_id: int
    quantity: int
    price: Decimal
    comments: Optional[str] = None
    hash_key: Optional[str] = None
    system_hash: Optional[str] = None
    restock_supplier_id: Optional[int] = None
    model_config = {"from_attributes": True}


class OrderItemUpdate(BaseModel):
    quantity: Optional[int] = None
    price: Optional[Decimal] = None
    comments: Optional[str] = None
    status: Optional[TYPE_ORDER_ITEM_STATUS] = None
    restock_supplier_id: Optional[int] = None
    model_config = {"from_attributes": True, "use_enum_values": True}


class OrderIn(BaseModel):
    provider_id: int
    customer_id: int
    comment: Optional[str] = None


class OrderUpdate(BaseModel):
    status: Optional[TYPE_STATUS_ORDER] = None
    comment: Optional[str] = None
    model_config = {"use_enum_values": True}


class OrderItemOut(BaseModel):
    id: int
    order_id: int
    autopart_id: Optional[int] = None
    oem_number: Optional[str] = None
    brand_name: Optional[str] = None
    autopart_name: Optional[str] = None
    quantity: int
    price: Decimal
    min_delivery_day: Optional[int] = None
    max_delivery_day: Optional[int] = None
    received_quantity: Optional[int] = None
    received_at: Optional[datetime] = None
    tracking_uuid: str
    status: TYPE_ORDER_ITEM_STATUS
    comments: Optional[str] = None
    external_supplier_id: Optional[int] = None
    external_supplier_name: Optional[str] = None
    external_price_name: Optional[str] = None
    external_sup_logo: Optional[str] = None
    hash_key: Optional[str] = None
    system_hash: Optional[str] = None
    created_at: datetime
    updated_at: datetime
    restock_supplier_id: Optional[int] = None
    model_config = {"from_attributes": True, "use_enum_values": True}


class OrderOut(BaseModel):
    id: int
    order_number: str
    provider_id: int
    customer_id: int
    status: TYPE_STATUS_ORDER
    comment: Optional[str] = None
    created_at: datetime
    updated_at: datetime
    order_items: Optional[List[OrderItemOut]] = None
    model_config = {"from_attributes": True, "use_enum_values": True}


class PlacedOrderHistoryRow(BaseModel):
    source_type: str
    source_label: str
    order_id: int
    item_id: int
    provider_id: Optional[int] = None
    provider_name: Optional[str] = None
    customer_id: Optional[int] = None
    customer_name: Optional[str] = None
    ordered_by_user_id: Optional[int] = None
    ordered_by_email: Optional[str] = None
    oem_number: Optional[str] = None
    brand_name: Optional[str] = None
    autopart_name: Optional[str] = None
    ordered_quantity: int
    received_quantity: Optional[int] = None
    price: Optional[Decimal] = None
    min_delivery_day: Optional[int] = None
    max_delivery_day: Optional[int] = None
    created_at: datetime
    received_at: Optional[datetime] = None
    current_status: str
    order_status: Optional[str] = None
    item_status: Optional[str] = None
    external_status_source: Optional[str] = None
    external_status_raw: Optional[str] = None
    needs_status_mapping: bool = False
    actual_lead_days: Optional[int] = None
    link: Optional[str] = None


class PlacedOrderHistoryUpdate(BaseModel):
    status: Optional[str] = None
    received_quantity: Optional[int] = Field(default=None, ge=0)


class TrackingInsightOwnPriceConfigOption(BaseModel):
    id: int
    provider_id: int
    provider_name: str
    name_price: Optional[str] = None
    latest_pricelist_date: Optional[date] = None
    use_for_order_insights: bool = False


class TrackingInsightOwnPriceOemQuantity(BaseModel):
    oem_number: str
    quantity: int = 0


class TrackingInsightOwnPriceAnalysis(BaseModel):
    provider_config_id: int
    provider_id: int
    provider_name: str
    provider_config_name: Optional[str] = None
    latest_pricelist_date: Optional[date] = None
    latest_price: Optional[Decimal] = None
    current_quantity: int = 0
    current_quantity_breakdown: List[TrackingInsightOwnPriceOemQuantity] = Field(
        default_factory=list
    )
    arrivals_last_30_days: int = 0
    arrivals_last_90_days: int = 0
    arrivals_last_365_days: int = 0
    sold_last_30_days: int = 0
    sold_last_90_days: int = 0
    sold_last_365_days: int = 0
    average_daily_decrease_30_days: Optional[float] = None
    estimated_days_left_30_days: Optional[int] = None


class TrackingInsightCrossItem(BaseModel):
    autopart_id: Optional[int] = None
    oem_number: str
    brand_name: Optional[str] = None
    name: Optional[str] = None


class TrackingInsightSeasonalityMonth(BaseModel):
    month: str
    month_name: str
    count: int = 0
    qty: int = 0


class TrackingInsightInvalidCrossItem(BaseModel):
    id: int
    invalid_brand_name: Optional[str] = None
    invalid_oem_number: str
    invalid_autopart_name: Optional[str] = None
    comment: Optional[str] = None


class TrackingInsightSupplierStat(BaseModel):
    provider_id: Optional[int] = None
    provider_name: Optional[str] = None
    order_count: int = 0
    fill_rate: Optional[float] = None
    avg_lead_days: Optional[float] = None
    effective_lead_days: Optional[float] = None
    avg_price: Optional[float] = None
    last_ordered_at: Optional[datetime] = None
    current_price: Optional[float] = None
    current_qty: Optional[int] = None
    current_min_delivery: Optional[int] = None
    current_max_delivery: Optional[int] = None
    current_oem_number: Optional[str] = None
    current_brand_name: Optional[str] = None
    current_autopart_name: Optional[str] = None
    current_autopart_id: Optional[int] = None
    current_provider_config_id: Optional[int] = None
    current_provider_config_name: Optional[str] = None
    source_type: Optional[str] = None
    sup_logo: Optional[str] = None
    hash_key: Optional[str] = None
    system_hash: Optional[str] = None
    current_min_qnt: Optional[int] = None
    is_own_price: bool = False
    score: Optional[float] = None


class TrackingInsightExceptionItem(BaseModel):
    code: str
    severity: str
    title: str
    description: str


class TrackingInsightDraftPurchaseOrder(BaseModel):
    provider_id: Optional[int] = None
    provider_name: str
    provider_config_id: Optional[int] = None
    provider_config_name: Optional[str] = None
    autopart_id: Optional[int] = None
    oem_number: str
    brand_name: Optional[str] = None
    autopart_name: Optional[str] = None
    price: Optional[float] = None
    available_qty: int = 0
    in_transit_qty: int = 0
    target_qty: Optional[int] = None
    recommended_qty: int = 0
    supplier_available_qty: int = 0
    proposed_order_qty: int = 0
    remaining_gap_qty: int = 0
    lead_days_used: Optional[float] = None
    reason: Optional[str] = None
    source_type: Optional[str] = None
    sup_logo: Optional[str] = None
    hash_key: Optional[str] = None
    system_hash: Optional[str] = None
    min_qnt: Optional[int] = None
    min_delivery_day: Optional[int] = None
    max_delivery_day: Optional[int] = None


class TrackingInsightAbcXyz(BaseModel):
    abc_class: Optional[str] = None
    xyz_class: Optional[str] = None
    annual_ordered_qty: int = 0
    monthly_cv: Optional[float] = None
    active_months: int = 0
    cumulative_share_pct: Optional[float] = None


class TrackingExceptionsQueueRow(BaseModel):
    oem_number: str
    brand_name: Optional[str] = None
    autopart_name: Optional[str] = None
    autopart_id: Optional[int] = None
    current_quantity: int = 0
    latest_price: Optional[float] = None
    in_transit_qty: int = 0
    sold_last_30_days: int = 0
    average_daily_decrease_30_days: Optional[float] = None
    estimated_days_left_30_days: Optional[int] = None
    reorder_point: Optional[float] = None
    optimal_order_qty: Optional[float] = None
    recommended_order_qty: Optional[int] = None
    severity: str
    exception_codes: List[str] = Field(default_factory=list)
    exception_titles: List[str] = Field(default_factory=list)
    best_supplier_by_price: Optional[TrackingInsightSupplierStat] = None
    best_supplier_by_lead_time: Optional[TrackingInsightSupplierStat] = None
    recommended_supplier: Optional[TrackingInsightSupplierStat] = None


class TrackingExceptionsQueueResponse(BaseModel):
    provider_config_id: int
    provider_id: int
    provider_name: str
    provider_config_name: Optional[str] = None
    generated_at: datetime
    total_items: int = 0
    critical_count: int = 0
    warning_count: int = 0
    info_count: int = 0
    rows: List[TrackingExceptionsQueueRow] = Field(default_factory=list)


class AutoPurchasePreviewRow(BaseModel):
    oem_number: str
    brand_name: Optional[str] = None
    autopart_name: Optional[str] = None
    autopart_id: Optional[int] = None
    current_quantity: int = 0
    latest_price: Optional[float] = None
    minimum_balance: int = 0
    multiplicity: int = 1
    in_transit_qty: int = 0
    sold_last_30_days: int = 0
    sold_last_90_days: int = 0
    avg_daily_30: Optional[float] = None
    avg_daily_90: Optional[float] = None
    avg_daily_blended: Optional[float] = None
    estimated_days_left_30_days: Optional[int] = None
    average_actual_lead_days: Optional[float] = None
    lead_time_days_used: Optional[float] = None
    safety_stock_days: Optional[int] = None
    safety_stock_qty: Optional[float] = None
    reorder_point: Optional[float] = None
    target_stock: Optional[int] = None
    recommended_order_qty: int = 0
    decision_status: str
    autopurchase_mode: str
    missing_in_latest_pricelist: bool = False
    reason_codes: List[str] = Field(default_factory=list)
    reason_titles: List[str] = Field(default_factory=list)
    reasons: List[TrackingInsightExceptionItem] = Field(default_factory=list)
    abc_xyz: Optional[TrackingInsightAbcXyz] = None
    best_supplier_by_price: Optional[TrackingInsightSupplierStat] = None
    best_supplier_by_lead_time: Optional[TrackingInsightSupplierStat] = None
    recommended_supplier: Optional[TrackingInsightSupplierStat] = None
    draft_purchase_order: Optional[TrackingInsightDraftPurchaseOrder] = None


class AutoPurchaseDiagnosticMetric(BaseModel):
    code: str
    title: str
    value: int = 0
    description: Optional[str] = None


class AutoPurchasePreviewResponse(BaseModel):
    provider_config_id: int
    provider_id: int
    provider_name: str
    provider_config_name: Optional[str] = None
    generated_at: datetime
    mode: str
    supplier_source: str = "site"
    total_items: int = 0
    auto_approved_count: int = 0
    needs_review_count: int = 0
    blocked_count: int = 0
    diagnostics: List[AutoPurchaseDiagnosticMetric] = Field(default_factory=list)
    rows: List[AutoPurchasePreviewRow] = Field(default_factory=list)


class AutoPurchaseRunOut(BaseModel):
    id: int
    provider_config_id: int
    provider_id: int
    provider_name: str
    provider_config_name: Optional[str] = None
    initiated_by_user_id: Optional[int] = None
    started_at: datetime
    finished_at: Optional[datetime] = None
    status: str
    mode: str
    trigger_source: str
    supplier_source: str = "site"
    settings_snapshot: dict[str, Any] = Field(default_factory=dict)
    summary_snapshot: dict[str, Any] = Field(default_factory=dict)
    total_items: int = 0
    auto_approved_count: int = 0
    needs_review_count: int = 0
    blocked_count: int = 0
    sent_count: int = 0


class AutoPurchaseRunItemOut(AutoPurchasePreviewRow):
    id: int
    run_id: int
    selected_supplier_id: Optional[int] = None
    sent_to_site_at: Optional[datetime] = None
    sent_order_id: Optional[int] = None
    sent_order_number: Optional[str] = None
    sent_customer_id: Optional[int] = None
    send_result_snapshot: dict[str, Any] = Field(default_factory=dict)


class AutoPurchaseRunItemsResponse(BaseModel):
    run: AutoPurchaseRunOut
    total_items: int = 0
    rows: List[AutoPurchaseRunItemOut] = Field(default_factory=list)


class AutoPurchaseRunItemStatusUpdateRequest(BaseModel):
    decision_status: str
    comment: Optional[str] = None


class AutoPurchaseRunItemStatusUpdateResponse(BaseModel):
    run: AutoPurchaseRunOut
    item: AutoPurchaseRunItemOut


class AutoPurchaseRunItemsStatusUpdateRequest(BaseModel):
    item_ids: List[int] = Field(default_factory=list)
    decision_status: str
    comment: Optional[str] = None


class AutoPurchaseRunItemsStatusUpdateResponse(BaseModel):
    run: AutoPurchaseRunOut
    updated_items: List[AutoPurchaseRunItemOut] = Field(default_factory=list)


class AutoPurchaseDraftOrderLineOut(BaseModel):
    item_id: int
    autopart_id: Optional[int] = None
    oem_number: str
    brand_name: Optional[str] = None
    autopart_name: Optional[str] = None
    decision_status: str
    recommended_order_qty: int = 0
    proposed_order_qty: int = 0
    remaining_gap_qty: int = 0
    supplier_available_qty: int = 0
    price: Optional[float] = None
    line_total: Optional[float] = None
    min_qnt: Optional[int] = None
    min_delivery_day: Optional[int] = None
    max_delivery_day: Optional[int] = None
    hash_key: Optional[str] = None
    system_hash: Optional[str] = None
    reason: Optional[str] = None


class AutoPurchaseDraftOrderGroupOut(BaseModel):
    supplier_key: str
    provider_name: str
    provider_config_name: Optional[str] = None
    source_type: Optional[str] = None
    sup_logo: Optional[str] = None
    total_items: int = 0
    total_quantity: int = 0
    total_sum: Optional[float] = None
    items: List[AutoPurchaseDraftOrderLineOut] = Field(default_factory=list)


class AutoPurchaseSkippedDraftItemOut(BaseModel):
    item_id: int
    oem_number: str
    brand_name: Optional[str] = None
    reason: str


class AutoPurchaseRunDraftOrdersResponse(BaseModel):
    run: AutoPurchaseRunOut
    total_groups: int = 0
    total_items: int = 0
    total_quantity: int = 0
    total_sum: Optional[float] = None
    applied_budget_limit: Optional[float] = None
    applied_position_limit: Optional[int] = None
    groups: List[AutoPurchaseDraftOrderGroupOut] = Field(default_factory=list)
    skipped_items: List[AutoPurchaseSkippedDraftItemOut] = Field(
        default_factory=list
    )


class AutoPurchaseMarkSentRequest(BaseModel):
    item_ids: List[int] = Field(default_factory=list)
    order_id: Optional[int] = None
    order_number: Optional[str] = None
    customer_id: Optional[int] = None
    send_result_snapshot: dict[str, Any] = Field(default_factory=dict)


class AutoPurchaseMarkSentResponse(BaseModel):
    run: AutoPurchaseRunOut
    updated_items: List[AutoPurchaseRunItemOut] = Field(default_factory=list)


class AutoPurchaseAiExplanationOut(BaseModel):
    run_id: int
    item_id: int
    model: str
    generated_at: datetime
    source: str = "ai"
    warning_code: Optional[str] = None
    warning_message: Optional[str] = None
    human_explanation: str
    risk_summary: str
    manager_note: str
    supplier_message_draft: Optional[str] = None
    confidence: float = 0.0
    requires_human_review: bool = True


class AutoPurchaseDraftGroupAiExplanationOut(BaseModel):
    run_id: int
    supplier_key: str
    provider_name: str
    total_items: int = 0
    total_quantity: int = 0
    total_sum: Optional[float] = None
    model: str
    generated_at: datetime
    source: str = "ai"
    warning_code: Optional[str] = None
    warning_message: Optional[str] = None
    human_explanation: str
    risk_summary: str
    manager_note: str
    supplier_message_draft: Optional[str] = None
    confidence: float = 0.0
    requires_human_review: bool = True


class TrackingHistoryInsightSummary(BaseModel):
    oem_number: str
    resolved_oem_numbers: List[str] = Field(default_factory=list)
    cross_oem_numbers: List[str] = Field(default_factory=list)
    site_cross_oem_numbers: List[str] = Field(default_factory=list)
    cross_items: List[TrackingInsightCrossItem] = Field(default_factory=list)
    exact_min_offer: Optional[AutopartOfferRow] = None
    min_offer_with_crosses: Optional[AutopartOfferRow] = None
    cross_offer_rows: List[AutopartOfferRow] = Field(default_factory=list)
    order_count_last_year: int = 0
    total_ordered_quantity_last_year: int = 0
    total_received_quantity_last_year: int = 0
    unique_suppliers_last_year: int = 0
    fill_rate_percent: Optional[float] = None
    historical_min_price_exact: Optional[Decimal] = None
    historical_min_price_with_crosses: Optional[Decimal] = None
    average_actual_lead_days: Optional[float] = None
    last_ordered_at: Optional[datetime] = None
    last_received_at: Optional[datetime] = None
    own_price_configs: List[TrackingInsightOwnPriceConfigOption] = Field(
        default_factory=list
    )
    own_price_analysis: Optional[TrackingInsightOwnPriceAnalysis] = None
    # ── new analytics fields ──
    avg_purchase_price: Optional[float] = None
    last_purchase_price: Optional[float] = None
    price_trend: Optional[str] = None
    price_trend_pct: Optional[float] = None
    markup_percent: Optional[float] = None
    margin_percent: Optional[float] = None
    in_transit_qty: int = 0
    reorder_point: Optional[float] = None
    optimal_order_qty: Optional[float] = None
    seasonality: List[TrackingInsightSeasonalityMonth] = Field(
        default_factory=list
    )
    peak_months: List[TrackingInsightSeasonalityMonth] = Field(
        default_factory=list
    )
    supplier_stats: List[TrackingInsightSupplierStat] = Field(
        default_factory=list
    )
    best_supplier: Optional[TrackingInsightSupplierStat] = None
    best_supplier_by_price: Optional[TrackingInsightSupplierStat] = None
    best_supplier_by_lead_time: Optional[TrackingInsightSupplierStat] = None
    recommended_supplier: Optional[TrackingInsightSupplierStat] = None
    draft_purchase_order: Optional[TrackingInsightDraftPurchaseOrder] = None
    exceptions: List[TrackingInsightExceptionItem] = Field(default_factory=list)
    abc_xyz: Optional[TrackingInsightAbcXyz] = None
    invalid_cross_items: List[TrackingInsightInvalidCrossItem] = Field(
        default_factory=list
    )
