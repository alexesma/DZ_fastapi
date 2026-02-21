from datetime import date, datetime
from decimal import Decimal
from typing import List, Optional

from pydantic import BaseModel, ConfigDict, EmailStr, Field, field_validator

from dz_fastapi.models.partner import (CUSTOMER_ORDER_ITEM_STATUS,
                                       CUSTOMER_ORDER_SHIP_MODE,
                                       CUSTOMER_ORDER_STATUS,
                                       STOCK_ORDER_STATUS,
                                       SUPPLIER_ORDER_STATUS)
from dz_fastapi.schemas.autopart import AutoPartResponse


class CustomerOrderConfigBase(BaseModel):
    order_email: Optional[EmailStr] = None
    order_emails: List[EmailStr] = Field(default_factory=list)
    order_subject_pattern: Optional[str] = None
    order_filename_pattern: Optional[str] = None
    order_reply_emails: List[EmailStr] = Field(default_factory=list)
    pricelist_config_id: Optional[int] = None

    order_number_column: Optional[int] = None
    order_date_column: Optional[int] = None
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


class CustomerOrderConfigUpdate(BaseModel):
    order_email: Optional[EmailStr] = None
    order_emails: Optional[List[EmailStr]] = None
    order_subject_pattern: Optional[str] = None
    order_filename_pattern: Optional[str] = None
    order_reply_emails: Optional[List[EmailStr]] = None
    pricelist_config_id: Optional[int] = None

    order_number_column: Optional[int] = None
    order_date_column: Optional[int] = None
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

    model_config = ConfigDict(from_attributes=True)


class CustomerOrderConfigResponse(CustomerOrderConfigBase):
    id: int
    customer_id: int
    last_uid: int = 0


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

    model_config = ConfigDict(from_attributes=True)


class CustomerOrderResponse(BaseModel):
    id: int
    customer_id: int
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

    items: List[CustomerOrderItemResponse] = Field(default_factory=list)

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
