from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, ConfigDict, EmailStr, Field, field_serializer, field_validator

from dz_fastapi.models.inventory import (
    CrossDockingItemStatus,
    CrossDockingLabelStatus,
    StockOrderPackageEventType,
    StockOrderPackageStatus,
)
from dz_fastapi.models.partner import (
    CUSTOMER_ORDER_ITEM_STATUS,
    CUSTOMER_ORDER_SHIP_MODE,
    CUSTOMER_ORDER_STATUS,
    STOCK_ORDER_STATUS,
    SUPPLIER_ORDER_STATUS,
)
from dz_fastapi.schemas.autopart import AutoPartResponse

ORDER_CONFIG_COLUMN_FIELDS = (
    "order_number_column",
    "order_date_column",
    "oem_col",
    "brand_col",
    "name_col",
    "qty_col",
    "price_col",
    "ship_qty_col",
    "ship_price_col",
    "reject_qty_col",
)


def _to_zero_based_column(value: int | str | None) -> int | None:
    if value is None or value == "":
        return None
    parsed = int(value)
    if parsed < 1:
        raise ValueError("Column numbers must start from 1")
    return parsed - 1


def _to_one_based_column(value: int | None) -> int | None:
    if value is None:
        return None
    return int(value) + 1


def _normalize_email_account_ids(
    value: list[int] | tuple[int, ...] | set[int] | int | str | None,
) -> list[int]:
    if value in (None, ""):
        return []
    if isinstance(value, str):
        raw_values = value.split(",")
    elif isinstance(value, (list, tuple, set)):
        raw_values = list(value)
    else:
        raw_values = [value]

    result: list[int] = []
    seen: set[int] = set()
    for raw in raw_values:
        if raw in (None, ""):
            continue
        parsed = int(raw)
        if parsed < 1:
            raise ValueError("email_account_ids must contain positive ids")
        if parsed in seen:
            continue
        seen.add(parsed)
        result.append(parsed)
    return result


class CustomerOrderConfigBase(BaseModel):
    order_email: Optional[EmailStr] = None
    order_emails: List[EmailStr] = Field(default_factory=list)
    order_subject_pattern: Optional[str] = None
    order_filename_pattern: Optional[str] = None
    order_reply_emails: List[EmailStr] = Field(default_factory=list)
    email_account_id: Optional[int] = None
    email_account_ids: List[int] = Field(default_factory=list)
    forward_customer_order_enabled: bool = False
    forward_customer_order_email: Optional[EmailStr] = None
    forward_customer_order_email_account_id: Optional[int] = None
    pricelist_config_id: Optional[int] = None

    order_start_row: int = 1
    order_number_column: Optional[int] = None
    order_number_row: Optional[int] = Field(default=None, ge=1)
    order_date_column: Optional[int] = None
    order_date_row: Optional[int] = Field(default=None, ge=1)
    order_number_regex_subject: Optional[str] = None
    order_number_regex_filename: Optional[str] = None
    order_number_regex_body: Optional[str] = None
    order_number_prefix: Optional[str] = None
    order_number_suffix: Optional[str] = None
    order_number_source: Optional[str] = None

    oem_col: int
    brand_col: int
    name_col: Optional[int] = None
    qty_col: int
    price_col: Optional[int] = None
    ship_qty_col: Optional[int] = None
    ship_price_col: Optional[int] = None
    reject_qty_col: Optional[int] = None
    ship_mode: CUSTOMER_ORDER_SHIP_MODE = CUSTOMER_ORDER_SHIP_MODE.REPLACE_QTY

    price_tolerance_pct: float = 2.0
    price_warning_pct: float = 5.0

    is_active: bool = True

    @field_validator(
        "order_subject_pattern",
        "order_filename_pattern",
        "forward_customer_order_email",
        "order_number_regex_subject",
        "order_number_regex_filename",
        mode="before",
    )
    def empty_to_none(cls, v):
        if v == "":
            return None
        return v

    @field_validator("email_account_ids", mode="before")
    def normalize_email_account_ids(cls, value):
        return _normalize_email_account_ids(value)

    model_config = ConfigDict(from_attributes=True)


class CustomerOrderConfigCreate(CustomerOrderConfigBase):
    customer_id: int

    @field_validator(*ORDER_CONFIG_COLUMN_FIELDS, mode="before")
    def columns_to_zero_based(cls, value):
        return _to_zero_based_column(value)


class CustomerOrderConfigUpdate(BaseModel):
    order_email: Optional[EmailStr] = None
    order_emails: Optional[List[EmailStr]] = None
    order_subject_pattern: Optional[str] = None
    order_filename_pattern: Optional[str] = None
    order_reply_emails: Optional[List[EmailStr]] = None
    email_account_id: Optional[int] = None
    email_account_ids: Optional[List[int]] = None
    forward_customer_order_enabled: Optional[bool] = None
    forward_customer_order_email: Optional[EmailStr] = None
    forward_customer_order_email_account_id: Optional[int] = None
    pricelist_config_id: Optional[int] = None

    order_start_row: Optional[int] = None
    order_number_column: Optional[int] = None
    order_number_row: Optional[int] = Field(default=None, ge=1)
    order_date_column: Optional[int] = None
    order_date_row: Optional[int] = Field(default=None, ge=1)
    order_number_regex_subject: Optional[str] = None
    order_number_regex_filename: Optional[str] = None
    order_number_regex_body: Optional[str] = None
    order_number_prefix: Optional[str] = None
    order_number_suffix: Optional[str] = None
    order_number_source: Optional[str] = None

    oem_col: Optional[int] = None
    brand_col: Optional[int] = None
    name_col: Optional[int] = None
    qty_col: Optional[int] = None
    price_col: Optional[int] = None
    ship_qty_col: Optional[int] = None
    ship_price_col: Optional[int] = None
    reject_qty_col: Optional[int] = None
    ship_mode: Optional[CUSTOMER_ORDER_SHIP_MODE] = None

    price_tolerance_pct: Optional[float] = None
    price_warning_pct: Optional[float] = None

    is_active: Optional[bool] = None

    @field_validator(
        "order_subject_pattern",
        "order_filename_pattern",
        "forward_customer_order_email",
        "order_number_regex_subject",
        "order_number_regex_filename",
        "order_number_regex_body",
        "order_number_prefix",
        "order_number_suffix",
        "order_number_source",
        mode="before",
    )
    def empty_to_none(cls, v):
        if v == "":
            return None
        return v

    @field_validator("email_account_ids", mode="before")
    def normalize_email_account_ids(cls, value):
        return _normalize_email_account_ids(value)

    @field_validator(*ORDER_CONFIG_COLUMN_FIELDS, mode="before")
    def columns_to_zero_based(cls, value):
        return _to_zero_based_column(value)

    model_config = ConfigDict(from_attributes=True)


class CustomerOrderConfigResponse(CustomerOrderConfigBase):
    id: int
    customer_id: int
    last_uid: int = 0
    pricelist_config_name: Optional[str] = None

    @field_serializer(*ORDER_CONFIG_COLUMN_FIELDS)
    def columns_to_one_based(self, value):
        return _to_one_based_column(value)


class CustomerOrderItemResponse(BaseModel):
    id: int
    oem: str
    brand: str
    name: Optional[str]
    requested_qty: int
    requested_price: Optional[Decimal]
    ship_qty: Optional[int]
    reject_qty: Optional[int]
    status: CUSTOMER_ORDER_ITEM_STATUS
    supplier_id: Optional[int]
    autopart_id: Optional[int]
    match_type: Optional[str] = None
    actual_oem: Optional[str] = None
    actual_brand: Optional[str] = None
    actual_name: Optional[str] = None
    matched_price: Optional[Decimal]
    price_diff_pct: Optional[float]
    reject_reason_code: Optional[str] = None
    reject_reason_text: Optional[str] = None
    credit_warning: Optional[dict] = None

    model_config = ConfigDict(from_attributes=True)


class CustomerOrderItemUpdate(BaseModel):
    status: Optional[CUSTOMER_ORDER_ITEM_STATUS] = None
    supplier_id: Optional[int] = None


class CustomerOrderManualItemCreate(BaseModel):
    oem: str
    brand: str
    name: Optional[str] = None
    quantity: int = Field(gt=0)
    price: Optional[Decimal] = None


class CustomerOrderManualCreate(BaseModel):
    customer_id: int
    order_number: Optional[str] = None
    order_date: Optional[date] = None
    auto_process: bool = True
    order_config_id: Optional[int] = None
    items: List[CustomerOrderManualItemCreate] = Field(default_factory=list)


class SupplierOrderManualItemCreate(BaseModel):
    autopart_id: Optional[int] = None
    oem: str
    brand: str
    name: Optional[str] = None
    quantity: int = Field(gt=0)
    price: Optional[Decimal] = None
    min_delivery_day: Optional[int] = None
    max_delivery_day: Optional[int] = None


class SupplierOrderManualCreate(BaseModel):
    provider_id: int
    items: List[SupplierOrderManualItemCreate] = Field(default_factory=list)


class CustomerOrderResponse(BaseModel):
    id: int
    customer_id: int
    order_config_id: Optional[int] = None
    status: CUSTOMER_ORDER_STATUS
    received_at: datetime
    processed_at: Optional[datetime]

    source_email: Optional[str]
    source_uid: Optional[int]
    source_subject: Optional[str]
    source_filename: Optional[str]

    order_number: Optional[str]
    order_date: Optional[date]

    response_file_path: Optional[str]
    response_file_name: Optional[str]
    error_details: Optional[str] = None

    items: List[CustomerOrderItemResponse] = Field(default_factory=list)
    credit_warning: Optional[dict] = None

    model_config = ConfigDict(from_attributes=True)


class CustomerOrderStatsMonthlyBucket(BaseModel):
    month: date
    orders_count: int = 0
    rows_count: int = 0
    total_requested_qty: int = 0
    total_ship_qty: int = 0
    avg_price: Optional[Decimal] = None
    min_price: Optional[Decimal] = None
    max_price: Optional[Decimal] = None


class CustomerOrderStatsRecentRow(BaseModel):
    order_id: int
    customer_id: int
    customer_name: Optional[str] = None
    order_number: Optional[str] = None
    received_at: datetime
    requested_qty: int
    requested_price: Optional[Decimal] = None
    ship_qty: Optional[int] = None
    reject_qty: Optional[int] = None
    status: CUSTOMER_ORDER_ITEM_STATUS


class CustomerOrderStatsSummary(BaseModel):
    orders_count: int = 0
    rows_count: int = 0
    total_requested_qty: int = 0
    total_ship_qty: int = 0
    avg_price: Optional[Decimal] = None
    min_price: Optional[Decimal] = None
    max_price: Optional[Decimal] = None
    last_price: Optional[Decimal] = None
    previous_price: Optional[Decimal] = None
    price_change_pct: Optional[float] = None
    last_order_at: Optional[datetime] = None


class CustomerOrderItemStatsResponse(BaseModel):
    kind: Literal["oem", "brand"]
    value: str
    period_months: int
    current_customer_id: int
    current_customer_name: Optional[str] = None
    current_customer_summary: CustomerOrderStatsSummary
    all_customers_summary: CustomerOrderStatsSummary
    current_customer_monthly: List[CustomerOrderStatsMonthlyBucket] = Field(
        default_factory=list
    )
    all_customers_monthly: List[CustomerOrderStatsMonthlyBucket] = Field(
        default_factory=list
    )
    current_customer_recent: List[CustomerOrderStatsRecentRow] = Field(
        default_factory=list
    )
    all_customers_recent: List[CustomerOrderStatsRecentRow] = Field(
        default_factory=list
    )


class CustomerOrderSummaryResponse(BaseModel):
    id: int
    customer_id: int
    customer_name: Optional[str] = None
    order_number: Optional[str] = None
    received_at: datetime
    status: CUSTOMER_ORDER_STATUS
    total_sum: float = 0.0
    stock_sum: float = 0.0
    supplier_sum: float = 0.0
    rejected_sum: float = 0.0
    rejected_pct: float = 0.0

    model_config = ConfigDict(from_attributes=True)


class SupplierOrderItemResponse(BaseModel):
    id: int
    autopart_id: Optional[int]
    customer_order_item_id: Optional[int]
    quantity: int
    price: Optional[Decimal]

    model_config = ConfigDict(from_attributes=True)


class SupplierOrderResponse(BaseModel):
    id: int
    provider_id: int
    status: SUPPLIER_ORDER_STATUS
    created_at: datetime
    scheduled_at: Optional[datetime]
    sent_at: Optional[datetime]
    items: List[SupplierOrderItemResponse] = Field(default_factory=list)

    model_config = ConfigDict(from_attributes=True)


class SupplierOrderItemDetailResponse(BaseModel):
    id: int
    customer_order_item_id: Optional[int]
    quantity: int
    price: Optional[Decimal]
    confirmed_quantity: Optional[int] = None
    response_price: Optional[Decimal] = None
    response_comment: Optional[str] = None
    response_status_raw: Optional[str] = None
    response_status_normalized: Optional[str] = None
    response_status_synced_at: Optional[datetime] = None
    oem: Optional[str] = None
    brand: Optional[str] = None
    name: Optional[str] = None
    min_delivery_day: Optional[int] = None
    max_delivery_day: Optional[int] = None
    received_quantity: Optional[int] = None
    received_at: Optional[datetime] = None
    auto_refused_at: Optional[datetime] = None
    requested_qty: Optional[int] = None
    ship_qty: Optional[int] = None
    reject_qty: Optional[int] = None

    model_config = ConfigDict(from_attributes=True)


class SupplierOrderDetailResponse(BaseModel):
    id: int
    provider_id: int
    provider_name: Optional[str] = None
    status: SUPPLIER_ORDER_STATUS
    created_at: datetime
    scheduled_at: Optional[datetime]
    sent_at: Optional[datetime]
    response_status_raw: Optional[str] = None
    response_status_normalized: Optional[str] = None
    response_status_synced_at: Optional[datetime] = None
    items: List[SupplierOrderItemDetailResponse] = Field(default_factory=list)

    model_config = ConfigDict(from_attributes=True)


class SupplierOrderSummaryResponse(BaseModel):
    id: int
    provider_id: int
    status: SUPPLIER_ORDER_STATUS
    created_at: datetime
    customer_order_id: Optional[int] = None
    customer_name: Optional[str] = None
    customer_order_number: Optional[str] = None
    customer_received_at: Optional[datetime] = None
    customer_status: Optional[CUSTOMER_ORDER_STATUS] = None
    customer_orders_count: int = 0
    total_sum: float = 0.0
    stock_sum: float = 0.0
    supplier_sum: float = 0.0
    rejected_sum: float = 0.0
    rejected_pct: float = 0.0

    model_config = ConfigDict(from_attributes=True)


class StockOrderItemResponse(BaseModel):
    id: int
    autopart_id: Optional[int]
    customer_order_item_id: Optional[int]
    supplier_receipt_item_id: Optional[int] = None
    preferred_stock_lot_id: Optional[int] = None
    source_type: str = "own_stock"
    provider_name: Optional[str] = None
    quantity: int
    picked_quantity: int = 0
    picked_at: Optional[datetime] = None
    picked_by_user_id: Optional[int] = None
    picked_by_email: Optional[str] = None
    pick_comment: Optional[str] = None
    pick_last_scan_code: Optional[str] = None
    autopart: Optional[AutoPartResponse] = None

    model_config = ConfigDict(from_attributes=True)

    @field_validator("picked_by_email", mode="before")
    def get_picked_by_email(cls, value):
        if value:
            return value
        if hasattr(value, "email"):
            return value.email
        return None


class StockOrderResponse(BaseModel):
    id: int
    customer_id: Optional[int]
    customer_name: Optional[str] = None
    shipment_document_id: Optional[int] = None
    packing_required: bool = False
    package_count: int = 0
    packing_ready: bool = False
    status: STOCK_ORDER_STATUS
    created_at: datetime
    items: List[StockOrderItemResponse] = Field(default_factory=list)

    model_config = ConfigDict(from_attributes=True)


class StockOrderItemPickUpdate(BaseModel):
    picked_quantity: Optional[int] = Field(default=None, ge=0)
    increment: Optional[int] = Field(default=None, ge=1, le=1000)
    pick_comment: Optional[str] = Field(default=None, max_length=500)
    scan_code: Optional[str] = Field(default=None, max_length=255)


class StockOrderItemPickResponse(BaseModel):
    id: int
    stock_order_id: int
    quantity: int
    picked_quantity: int
    picked_at: Optional[datetime] = None
    picked_by_user_id: Optional[int] = None
    picked_by_email: Optional[str] = None
    pick_comment: Optional[str] = None
    pick_last_scan_code: Optional[str] = None
    stock_order_status: STOCK_ORDER_STATUS


class StockOrderPackageCreate(BaseModel):
    comment: Optional[str] = Field(default=None, max_length=500)
    pack_all_unallocated: bool = False


class StockOrderPackageAllocation(BaseModel):
    stock_order_item_id: int
    quantity: int = Field(ge=0)


class StockOrderPackageContentsUpdate(BaseModel):
    items: List[StockOrderPackageAllocation] = Field(default_factory=list)


class StockOrderPackageScanRequest(BaseModel):
    scan_code: str = Field(min_length=1, max_length=255)


class StockOrderPackageReasonRequest(BaseModel):
    reason: str = Field(min_length=1, max_length=500)


class StockOrderPackagePrintRequest(BaseModel):
    reason: Optional[str] = Field(default=None, max_length=500)


class StockOrderPackingItemResponse(BaseModel):
    stock_order_item_id: int
    autopart_id: Optional[int] = None
    actual_oem: Optional[str] = None
    actual_brand: Optional[str] = None
    customer_oem: Optional[str] = None
    customer_brand: Optional[str] = None
    name: Optional[str] = None
    barcode: Optional[str] = None
    quantity: int
    picked_quantity: int
    allocated_quantity: int
    unallocated_quantity: int


class StockOrderPackageItemResponse(BaseModel):
    id: int
    stock_order_item_id: int
    quantity: int
    verified_quantity: int
    actual_oem: Optional[str] = None
    customer_oem: Optional[str] = None
    customer_brand: Optional[str] = None
    name: Optional[str] = None
    last_scan_code: Optional[str] = None


class StockOrderPackageEventResponse(BaseModel):
    id: int
    event_type: StockOrderPackageEventType
    created_at: datetime
    user_name: Optional[str] = None
    reason: Optional[str] = None
    details: Optional[Dict[str, Any]] = None


class StockOrderPackageResponse(BaseModel):
    id: int
    sequence_number: int
    total_packages: int
    barcode: str
    status: StockOrderPackageStatus
    comment: Optional[str] = None
    created_at: datetime
    created_by_name: Optional[str] = None
    sealed_at: Optional[datetime] = None
    sealed_by_name: Optional[str] = None
    verified_at: Optional[datetime] = None
    verified_by_name: Optional[str] = None
    print_count: int
    last_printed_at: Optional[datetime] = None
    last_printed_by_name: Optional[str] = None
    last_print_reason: Optional[str] = None
    total_quantity: int
    verified_quantity: int
    items: List[StockOrderPackageItemResponse] = Field(default_factory=list)
    events: List[StockOrderPackageEventResponse] = Field(default_factory=list)


class StockOrderPackingResponse(BaseModel):
    stock_order_id: int
    customer_id: Optional[int] = None
    customer_name: Optional[str] = None
    stock_order_status: STOCK_ORDER_STATUS
    packing_required: bool
    packing_ready: bool
    items: List[StockOrderPackingItemResponse] = Field(default_factory=list)
    packages: List[StockOrderPackageResponse] = Field(default_factory=list)


class SupplierReceiptCandidateRow(BaseModel):
    supplier_order_item_id: int
    supplier_order_id: int
    provider_id: int
    provider_name: Optional[str] = None
    supplier_order_created_at: datetime
    supplier_order_sent_at: Optional[datetime] = None
    supplier_order_status: SUPPLIER_ORDER_STATUS
    customer_order_id: Optional[int] = None
    customer_order_number: Optional[str] = None
    customer_name: Optional[str] = None
    barcode: Optional[str] = None
    oem_number: Optional[str] = None
    brand_name: Optional[str] = None
    autopart_name: Optional[str] = None
    ordered_quantity: int
    confirmed_quantity: Optional[int] = None
    already_received_quantity: int = 0
    pending_quantity: int = 0
    price: Optional[Decimal] = None
    response_price: Optional[Decimal] = None
    response_comment: Optional[str] = None
    response_status_raw: Optional[str] = None
    response_status_normalized: Optional[str] = None
    min_delivery_day: Optional[int] = None
    max_delivery_day: Optional[int] = None
    last_receipt_at: Optional[datetime] = None
    last_receipt_number: Optional[str] = None


class SupplierReceiptProviderOption(BaseModel):
    provider_id: int
    provider_name: Optional[str] = None
    orders_count: int = 0


class SupplierReceiptCreateItem(BaseModel):
    supplier_order_item_id: int
    received_quantity: int = Field(ge=0)
    comment: Optional[str] = Field(default=None, max_length=500)


class SupplierReceiptCreate(BaseModel):
    provider_id: int
    warehouse_id: Optional[int] = None
    post_now: bool = Field(default=False)
    document_number: Optional[str] = Field(default=None, max_length=120)
    document_date: Optional[date] = None
    comment: Optional[str] = None
    items: List[SupplierReceiptCreateItem] = Field(default_factory=list)


class SupplierReceiptUpdate(BaseModel):
    warehouse_id: Optional[int] = None
    document_number: Optional[str] = Field(default=None, max_length=120)
    document_date: Optional[date] = None
    comment: Optional[str] = None


class SupplierReceiptManualItem(BaseModel):
    supplier_order_item_id: Optional[int] = None
    order_item_id: Optional[int] = None
    autopart_id: Optional[int] = None
    oem_number: Optional[str] = Field(default=None, max_length=120)
    brand_name: Optional[str] = Field(default=None, max_length=120)
    autopart_name: Optional[str] = Field(default=None, max_length=512)
    received_quantity: int = Field(ge=0)
    price: Optional[Decimal] = None
    total_price_with_vat: Optional[Decimal] = None
    gtd_code: Optional[str] = Field(default=None, max_length=64)
    country_code: Optional[str] = Field(default=None, max_length=16)
    country_name: Optional[str] = Field(default=None, max_length=120)
    comment: Optional[str] = Field(default=None, max_length=500)
    warehouse_id: Optional[int] = None


class SupplierReceiptManualCreate(BaseModel):
    provider_id: int
    warehouse_id: Optional[int] = None
    post_now: bool = Field(default=False)
    document_number: Optional[str] = Field(default=None, max_length=120)
    document_date: Optional[date] = None
    comment: Optional[str] = None
    items: List[SupplierReceiptManualItem] = Field(default_factory=list)


class SupplierReceiptItemUpdate(BaseModel):
    autopart_id: Optional[int] = None
    oem_number: Optional[str] = Field(default=None, max_length=120)
    brand_name: Optional[str] = Field(default=None, max_length=120)
    autopart_name: Optional[str] = Field(default=None, max_length=512)
    received_quantity: Optional[int] = Field(default=None, ge=0)
    price: Optional[Decimal] = None
    total_price_with_vat: Optional[Decimal] = None
    gtd_code: Optional[str] = Field(default=None, max_length=64)
    country_code: Optional[str] = Field(default=None, max_length=16)
    country_name: Optional[str] = Field(default=None, max_length=120)
    comment: Optional[str] = Field(default=None, max_length=500)
    warehouse_id: Optional[int] = None


class SupplierReceiptItemResponse(BaseModel):
    id: int
    supplier_order_id: Optional[int] = None
    supplier_order_item_id: Optional[int] = None
    customer_order_item_id: Optional[int] = None
    order_item_id: Optional[int] = None
    autopart_id: Optional[int] = None
    oem_number: Optional[str] = None
    brand_name: Optional[str] = None
    autopart_name: Optional[str] = None
    ordered_quantity: Optional[int] = None
    confirmed_quantity: Optional[int] = None
    received_quantity: int
    price: Optional[Decimal] = None
    total_price_with_vat: Optional[Decimal] = None
    marking_codes: Optional[List[str]] = None
    gtd_code: Optional[str] = None
    country_code: Optional[str] = None
    country_name: Optional[str] = None
    comment: Optional[str] = None
    warehouse_id: Optional[int] = None
    warehouse_name: Optional[str] = None
    # Customer info (populated in detail view)
    customer_name: Optional[str] = None
    customer_order_number: Optional[str] = None
    cross_docking_status: Optional[CrossDockingItemStatus] = None
    document_pending: bool = False
    accepted_at: Optional[datetime] = None
    accepted_by_user_id: Optional[int] = None
    ready_at: Optional[datetime] = None

    model_config = ConfigDict(from_attributes=True)


class SupplierReceiptResponse(BaseModel):
    id: int
    provider_id: int
    provider_name: Optional[str] = None
    provider_is_vat_payer: bool = False
    warehouse_id: Optional[int] = None
    warehouse_name: Optional[str] = None
    supplier_order_id: Optional[int] = None
    source_message_id: Optional[int] = None
    document_number: Optional[str] = None
    document_date: Optional[date] = None
    created_by_user_id: Optional[int] = None
    created_by_email: Optional[str] = None
    created_at: datetime
    posted_at: Optional[datetime] = None
    comment: Optional[str] = None
    items: List[SupplierReceiptItemResponse] = Field(default_factory=list)

    model_config = ConfigDict(from_attributes=True)


class CrossDockingLabelPrintEventResponse(BaseModel):
    id: int
    print_number: int
    printed_at: datetime
    printed_by_name: Optional[str] = None
    reason: Optional[str] = None


class CrossDockingLabelResponse(BaseModel):
    id: int
    supplier_receipt_item_id: int
    stock_order_item_id: Optional[int] = None
    customer_order_item_id: Optional[int] = None
    quantity: int
    requested_brand: str
    requested_oem: str
    requested_name: Optional[str] = None
    customer_name: Optional[str] = None
    order_number: Optional[str] = None
    order_date: Optional[date] = None
    barcode: str
    status: CrossDockingLabelStatus
    print_count: int = 0
    last_printed_at: Optional[datetime] = None
    last_printed_by_name: Optional[str] = None
    last_print_reason: Optional[str] = None
    print_history: List[CrossDockingLabelPrintEventResponse] = Field(
        default_factory=list
    )


class CrossDockingLabelPrintRequest(BaseModel):
    label_ids: List[int] = Field(default_factory=list)
    reason: Optional[str] = Field(default=None, max_length=500)


class CrossDockingDocumentStatusUpdate(BaseModel):
    document_pending: bool
    document_number: Optional[str] = Field(default=None, max_length=120)
    document_date: Optional[date] = None


class SupplierResponseProcessResult(BaseModel):
    fetched_messages: int = 0
    processed_messages: int = 0
    matched_orders: int = 0
    stored_attachments: int = 0
    parsed_response_files: int = 0
    parsed_text_positions: int = 0
    recognized_positions: int = 0
    unresolved_positions: int = 0
    unresolved_examples: List[str] = Field(default_factory=list)
    updated_items: int = 0
    updated_orders: int = 0
    unmapped_statuses: int = 0
    skipped_messages: int = 0
    created_receipts: int = 0
    updated_receipts: int = 0
    posted_receipts: int = 0
    draft_receipts: int = 0
    receipt_items_added: int = 0
    timeout_auto_confirmed_orders: int = 0


class SupplierResponseImportErrorItem(BaseModel):
    id: int
    received_at: datetime
    sender_email: Optional[str] = None
    subject: Optional[str] = None
    subject_raw: Optional[str] = None
    body_preview: Optional[str] = None
    message_type: Optional[str] = None
    import_error_details: Optional[str] = None
    import_error_reasons: List[str] = Field(default_factory=list)
    config_expectations: List[str] = Field(default_factory=list)
    source_uid: Optional[str] = None
    source_message_id: Optional[str] = None
    account_id: Optional[int] = None
    account_name: Optional[str] = None
    account_email: Optional[str] = None
    source_folder: Optional[str] = None
    source_message_uid: Optional[str] = None
    attachment_filenames: List[str] = Field(default_factory=list)
    attachment_details: List[str] = Field(default_factory=list)
    manager_hints: List[str] = Field(default_factory=list)


class SupplierResponseRetryErrorsResult(SupplierResponseProcessResult):
    config_id: int
    total: int = 0
    queued: int = 0
    unretryable: int = 0


class SupplierResponseInboxMessageItem(BaseModel):
    id: int
    received_at: datetime
    sender_email: Optional[str] = None
    subject: Optional[str] = None
    subject_raw: Optional[str] = None
    body_preview: Optional[str] = None
    message_type: Optional[str] = None
    import_error_details: Optional[str] = None
    source_uid: Optional[str] = None
    source_message_id: Optional[str] = None
    account_id: Optional[int] = None
    account_name: Optional[str] = None
    account_email: Optional[str] = None
    source_folder: Optional[str] = None
    source_message_uid: Optional[str] = None
    attachment_details: List[str] = Field(default_factory=list)
    suggested_message_type: Optional[str] = None
    suggested_confidence: Optional[float] = None
    suggested_explanation: Optional[str] = None
    suggested_source: Optional[str] = None
    can_retry: bool = False


class SupplierResponseMessageClassifyIn(BaseModel):
    message_type: Literal[
        "UNKNOWN",
        "IMPORT_ERROR",
        "RESPONSE_FILE",
        "TEXT_RESPONSE",
        "SHIPPING_DOC",
        "STATUS",
        "IGNORED",
        "RETRY_PENDING",
    ]


class SupplierResponseMessageActionResult(BaseModel):
    id: int
    message_type: str
    detail: Optional[str] = None


class SupplierResponseRetryMessageResult(SupplierResponseProcessResult):
    config_id: int
    message_id: int
    queued: int = 0
    unretryable: int = 0
