from datetime import date, datetime
from decimal import Decimal
from typing import List, Literal, Optional

from pydantic import (BaseModel, ConfigDict, EmailStr, Field, field_serializer,
                      field_validator)

from dz_fastapi.models.partner import (CUSTOMER_ORDER_ITEM_STATUS,
                                       CUSTOMER_ORDER_SHIP_MODE,
                                       CUSTOMER_ORDER_STATUS,
                                       STOCK_ORDER_STATUS,
                                       SUPPLIER_ORDER_STATUS)
from dz_fastapi.schemas.autopart import AutoPartResponse

ORDER_CONFIG_COLUMN_FIELDS = (
    'order_number_column',
    'order_date_column',
    'oem_col',
    'brand_col',
    'name_col',
    'qty_col',
    'price_col',
    'ship_qty_col',
    'ship_price_col',
    'reject_qty_col',
)


def _to_zero_based_column(value: int | str | None) -> int | None:
    if value is None or value == '':
        return None
    parsed = int(value)
    if parsed < 1:
        raise ValueError('Column numbers must start from 1')
    return parsed - 1


def _to_one_based_column(value: int | None) -> int | None:
    if value is None:
        return None
    return int(value) + 1


class CustomerOrderConfigBase(BaseModel):
    order_email: Optional[EmailStr] = None
    order_emails: List[EmailStr] = Field(default_factory=list)
    order_subject_pattern: Optional[str] = None
    order_filename_pattern: Optional[str] = None
    order_reply_emails: List[EmailStr] = Field(default_factory=list)
    email_account_id: Optional[int] = None
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
        'order_subject_pattern',
        'order_filename_pattern',
        'order_number_regex_subject',
        'order_number_regex_filename',
        mode='before',
    )
    def empty_to_none(cls, v):
        if v == '':
            return None
        return v

    model_config = ConfigDict(from_attributes=True)


class CustomerOrderConfigCreate(CustomerOrderConfigBase):
    customer_id: int

    @field_validator(*ORDER_CONFIG_COLUMN_FIELDS, mode='before')
    def columns_to_zero_based(cls, value):
        return _to_zero_based_column(value)


class CustomerOrderConfigUpdate(BaseModel):
    order_email: Optional[EmailStr] = None
    order_emails: Optional[List[EmailStr]] = None
    order_subject_pattern: Optional[str] = None
    order_filename_pattern: Optional[str] = None
    order_reply_emails: Optional[List[EmailStr]] = None
    email_account_id: Optional[int] = None
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
        'order_subject_pattern',
        'order_filename_pattern',
        'order_number_regex_subject',
        'order_number_regex_filename',
        'order_number_regex_body',
        'order_number_prefix',
        'order_number_suffix',
        'order_number_source',
        mode='before',
    )
    def empty_to_none(cls, v):
        if v == '':
            return None
        return v

    @field_validator(*ORDER_CONFIG_COLUMN_FIELDS, mode='before')
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
    matched_price: Optional[Decimal]
    price_diff_pct: Optional[float]
    reject_reason_code: Optional[str] = None
    reject_reason_text: Optional[str] = None

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
    kind: Literal['oem', 'brand']
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
    oem: Optional[str] = None
    brand: Optional[str] = None
    name: Optional[str] = None
    min_delivery_day: Optional[int] = None
    max_delivery_day: Optional[int] = None
    received_quantity: Optional[int] = None
    received_at: Optional[datetime] = None
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
    quantity: int
    autopart: Optional[AutoPartResponse] = None

    model_config = ConfigDict(from_attributes=True)


class StockOrderResponse(BaseModel):
    id: int
    customer_id: Optional[int]
    status: STOCK_ORDER_STATUS
    created_at: datetime
    items: List[StockOrderItemResponse] = Field(default_factory=list)

    model_config = ConfigDict(from_attributes=True)
