import re
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import (BaseModel, ConfigDict, EmailStr, Field, field_validator,
                      model_validator)

from dz_fastapi.schemas.autopart import AutoPartPricelist, AutoPartResponse


class TypePrices(str, Enum):
    WHOLESALE = 'Wholesale'
    RETAIL = 'Retail'
    CASH = 'Cash'


class ProviderDeliveryMethod(str, Enum):
    DELIVERED = 'Delivered'
    SELF_PICKUP = 'Self pickup'
    COURIER_FOOT = 'Courier foot'
    COURIER_CAR = 'Courier car'


class SupplierResponseType(str, Enum):
    FILE = 'file'
    TEXT = 'text'


class SupplierResponseFileFormat(str, Enum):
    EXCEL = 'excel'
    CSV = 'csv'


class SupplierResponseFilePayloadType(str, Enum):
    RESPONSE = 'response'
    DOCUMENT = 'document'


class SupplierResponseValueAfterArticleType(str, Enum):
    NUMBER = 'number'
    TEXT = 'text'
    BOTH = 'both'


class ClientBase(BaseModel):
    name: str
    type_prices: TypePrices = TypePrices.WHOLESALE
    email_contact: Optional[EmailStr] = None
    description: Optional[str] = None
    comment: Optional[str] = None

    @field_validator('name', mode='before')
    def name_must_not_be_empty(cls, v):
        if not v.strip():
            raise ValueError('Name must not be empty')
        return v

    @field_validator('email_contact', mode='before')
    def validate_email_contact(cls, v):
        if v == '':
            return None
        return v


class ProviderBase(ClientBase):
    email_incoming_price: Optional[EmailStr] = None
    is_own_price: Optional[bool] = False
    is_vat_payer: Optional[bool] = False
    order_schedule_days: Optional[List[str]] = None
    order_schedule_times: Optional[List[str]] = None
    order_schedule_enabled: Optional[bool] = None
    supplier_response_allow_shipping_docs: Optional[bool] = True
    supplier_response_allow_response_files: Optional[bool] = True
    supplier_response_allow_text_status: Optional[bool] = True
    supplier_response_filename_pattern: Optional[str] = None
    supplier_shipping_doc_filename_pattern: Optional[str] = None
    supplier_response_start_row: int = Field(default=1, ge=1)
    supplier_response_oem_col: Optional[int] = Field(default=None, ge=1)
    supplier_response_brand_col: Optional[int] = Field(default=None, ge=1)
    supplier_response_qty_col: Optional[int] = Field(default=None, ge=1)
    supplier_response_price_col: Optional[int] = Field(default=None, ge=1)
    supplier_response_comment_col: Optional[int] = Field(default=None, ge=1)
    supplier_response_status_col: Optional[int] = Field(default=None, ge=1)
    default_delivery_method: Optional[
        ProviderDeliveryMethod
    ] = ProviderDeliveryMethod.DELIVERED

    @field_validator('email_incoming_price', mode='before')
    def validate_email_incoming_price(cls, v):
        if v == '':
            return None
        return v

    @field_validator(
        'supplier_response_filename_pattern',
        'supplier_shipping_doc_filename_pattern',
        mode='before',
    )
    def normalize_supplier_response_patterns(cls, v):
        if v is None:
            return None
        value = str(v).strip()
        return value or None

    model_config = ConfigDict(from_attributes=True, validate_assignment=True)


class ProviderCreate(ProviderBase):
    pass


class ProviderUpdate(BaseModel):
    name: Optional[str] = None
    type_prices: Optional[TypePrices] = None
    email_contact: Optional[EmailStr] = None
    description: Optional[str] = None
    comment: Optional[str] = None
    email_incoming_price: Optional[EmailStr] = None
    is_virtual: Optional[bool] = None
    is_own_price: Optional[bool] = None
    is_vat_payer: Optional[bool] = None
    order_schedule_days: Optional[List[str]] = None
    order_schedule_times: Optional[List[str]] = None
    order_schedule_enabled: Optional[bool] = None
    supplier_response_allow_shipping_docs: Optional[bool] = None
    supplier_response_allow_response_files: Optional[bool] = None
    supplier_response_allow_text_status: Optional[bool] = None
    supplier_response_filename_pattern: Optional[str] = None
    supplier_shipping_doc_filename_pattern: Optional[str] = None
    supplier_response_start_row: Optional[int] = Field(default=None, ge=1)
    supplier_response_oem_col: Optional[int] = Field(default=None, ge=1)
    supplier_response_brand_col: Optional[int] = Field(default=None, ge=1)
    supplier_response_qty_col: Optional[int] = Field(default=None, ge=1)
    supplier_response_price_col: Optional[int] = Field(default=None, ge=1)
    supplier_response_comment_col: Optional[int] = Field(default=None, ge=1)
    supplier_response_status_col: Optional[int] = Field(default=None, ge=1)
    default_delivery_method: Optional[ProviderDeliveryMethod] = None

    @field_validator('email_contact', 'email_incoming_price', mode='before')
    def empty_to_none(cls, v):
        if v == '':
            return None
        return v

    @field_validator(
        'supplier_response_filename_pattern',
        'supplier_shipping_doc_filename_pattern',
        mode='before',
    )
    def normalize_update_supplier_response_patterns(cls, v):
        if v is None:
            return None
        value = str(v).strip()
        return value or None


class CustomerBase(ClientBase):
    email_outgoing_price: Optional[EmailStr] = None

    @field_validator('email_outgoing_price', mode='before')
    def validate_email_outgoing_price(cls, v):
        if v == '':
            return None
        return v

    model_config = ConfigDict(from_attributes=True, validate_assignment=True)


class CustomerCreate(CustomerBase):
    pass


class CustomerUpdate(CustomerBase):
    pass


class PriceListAutoPartAssociationResponse(BaseModel):
    autopart: AutoPartResponse
    quantity: int
    price: float
    multiplicity: int = 1
    model_config = ConfigDict(from_attributes=True)


class PriceListAutoPartAssociationCreate(BaseModel):
    autopart: AutoPartPricelist
    quantity: int
    price: float
    multiplicity: int = 1
    model_config = ConfigDict(from_attributes=True)


class CustomerPriceListAutoPartAssociationResponse(
    PriceListAutoPartAssociationResponse
):
    pass


class PriceListBase(BaseModel):
    date: Optional[date] = None
    provider_id: int
    provider_config_id: Optional[int] = None
    is_active: bool = True


class PriceListCreate(PriceListBase):
    provider_id: int
    provider_config_id: int
    autoparts: List[PriceListAutoPartAssociationCreate] = []


class PriceListUpdate(PriceListBase):
    provider_id: Optional[int] = None
    provider_config_id: Optional[int] = None
    autoparts: Optional[List[PriceListAutoPartAssociationCreate]] = None


class PriceListDeleteRequest(BaseModel):
    pricelist_ids: List[int]


class ProviderMinimalResponse(BaseModel):
    id: int
    name: str
    model_config = ConfigDict(from_attributes=True)


class PriceListResponse(BaseModel):
    id: int
    date: Optional[date]
    provider: ProviderMinimalResponse
    provider_config_id: Optional[int]
    autoparts: List[PriceListAutoPartAssociationResponse] = Field(
        default_factory=list
    )
    stats: Optional['PriceListProcessStats'] = None
    model_config = ConfigDict(from_attributes=True)


class PriceListProcessStats(BaseModel):
    rows_total: int
    rows_clean: int
    rows_deduplicated: int
    rows_removed: int
    rows_dedup_removed: int


class PriceListSummary(BaseModel):
    id: int
    date: date
    num_positions: int
    provider_config_id: Optional[int] = None
    model_config = ConfigDict(from_attributes=True)


class PriceListPaginationResponse(BaseModel):
    total_count: int
    skip: int
    limit: int
    pricelists: List[PriceListSummary]


class CustomerPriceListBase(BaseModel):
    date: Optional[date] = None
    customer_id: int
    is_active: bool = True


class CustomerPriceListUpdate(BaseModel):
    date: Optional[date] = None
    is_active: Optional[bool] = None
    autoparts: Optional[List[CustomerPriceListAutoPartAssociationResponse]] = (
        None
    )


class CustomerMinimalResponse(BaseModel):
    id: int
    name: str
    model_config = ConfigDict(from_attributes=True)


class CustomerPriceListResponse(CustomerPriceListBase):
    id: int
    date: date
    customer_id: int
    autoparts: List['AutoPartInPricelist']
    model_config = ConfigDict(from_attributes=True)


class ProviderResponse(ProviderBase):
    id: int
    price_lists: List[PriceListResponse] = []
    model_config = ConfigDict(from_attributes=True)


class CustomerResponse(CustomerBase):
    id: int
    customer_price_lists: List[CustomerPriceListResponse] = []
    pricelist_configs: List['CustomerPriceListConfigSummary'] = Field(
        default_factory=list
    )
    model_config = ConfigDict(from_attributes=True)

    @field_validator('pricelist_configs', mode='before')
    def normalize_pricelist_configs(cls, v):
        if v is None:
            return []
        return v


class CustomerPriceListResponseShort(BaseModel):
    id: int
    date: date
    autoparts_count: int
    model_config = ConfigDict(from_attributes=True)


class CustomerResponseShort(BaseModel):
    id: int
    name: str
    email_outgoing_price: Optional[EmailStr] = None
    type_prices: TypePrices = TypePrices.WHOLESALE
    email_contact: Optional[EmailStr] = None
    description: Optional[str] = None
    comment: Optional[str] = None
    customer_price_lists: List[CustomerPriceListResponseShort] = []

    @field_validator('name', mode='before')
    def name_must_not_be_empty(cls, v):
        if not v.strip():
            raise ValueError('Name must not be empty')
        return v

    @field_validator('email_contact', mode='before')
    def validate_email_contact(cls, v):
        if v == '':
            return None
        return v

    @field_validator('email_outgoing_price', mode='before')
    def validate_email_outgoing_price(cls, v):
        if v == '':
            return None
        return v

    model_config = ConfigDict(from_attributes=True)


class CustomerListSummary(BaseModel):
    id: int
    name: str
    email_outgoing_price: Optional[EmailStr] = None
    type_prices: TypePrices = TypePrices.WHOLESALE
    email_contact: Optional[EmailStr] = None
    price_lists_count: int = 0
    pricelist_configs_count: int = 0
    pricelist_sources_count: int = 0

    model_config = ConfigDict(from_attributes=True)


class PaginatedCustomersResponse(BaseModel):
    items: List[CustomerListSummary]
    page: int
    page_size: int
    total: int
    pages: int


class ProviderPriceListConfigBase(BaseModel):
    incoming_email_account_id: Optional[int] = Field(default=None, ge=1)
    start_row: int
    oem_col: int
    name_col: Optional[int] = None
    brand_col: Optional[int] = None
    multiplicity_col: Optional[int] = None
    qty_col: int
    price_col: int
    filename_pattern: Optional[str] = None
    name_price: Optional[str] = None
    name_mail: Optional[str] = None
    file_url: Optional[str] = None
    min_price: Optional[float] = None
    max_price: Optional[float] = None
    min_quantity: Optional[int] = None
    max_quantity: Optional[int] = None
    exclude_positions: List[Dict[str, str]] = Field(default_factory=list)
    max_days_without_update: Optional[int] = Field(default=3, ge=0)
    min_delivery_day: Optional[int] = Field(default=1, ge=0)
    max_delivery_day: Optional[int] = Field(default=3, ge=0)
    is_active: bool = True


class ProviderPriceListConfigCreate(ProviderPriceListConfigBase):
    pass


class ProviderPriceListConfigUpdate(BaseModel):
    incoming_email_account_id: Optional[int] = Field(default=None, ge=1)
    start_row: Optional[int] = Field(default=None, ge=0)
    oem_col: Optional[int] = Field(default=None, ge=0)
    name_col: Optional[int] = Field(default=None, ge=0)
    brand_col: Optional[int] = Field(default=None, ge=0)
    multiplicity_col: Optional[int] = Field(default=None, ge=0)
    qty_col: Optional[int] = Field(default=None, ge=0)
    price_col: Optional[int] = Field(default=None, ge=0)
    filename_pattern: Optional[str] = None
    name_price: Optional[str] = None
    name_mail: Optional[str] = None
    file_url: Optional[str] = None
    min_price: Optional[float] = None
    max_price: Optional[float] = None
    min_quantity: Optional[int] = None
    max_quantity: Optional[int] = None
    exclude_positions: Optional[List[Dict[str, str]]] = None
    max_days_without_update: Optional[int] = Field(default=None, ge=0)
    min_delivery_day: Optional[int] = Field(default=None, ge=0)
    max_delivery_day: Optional[int] = Field(default=None, ge=0)
    is_active: Optional[bool] = None


class ProviderPriceListConfigResponse(ProviderPriceListConfigBase):
    id: int
    provider_id: int
    model_config = ConfigDict(from_attributes=True)


def _normalize_email_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        raw_values = [value]
    elif isinstance(value, (list, tuple, set)):
        raw_values = list(value)
    else:
        raw_values = [value]
    result: list[str] = []
    seen: set[str] = set()
    for raw in raw_values:
        for item in str(raw or '').split(','):
            cleaned = item.strip().lower()
            if not cleaned or cleaned in seen:
                continue
            seen.add(cleaned)
            result.append(cleaned)
    return result


def _normalize_string_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        raw_values = [value]
    elif isinstance(value, (list, tuple, set)):
        raw_values = list(value)
    else:
        raw_values = [value]
    result: list[str] = []
    seen: set[str] = set()
    for raw in raw_values:
        for item in str(raw or '').split(','):
            cleaned = item.strip()
            if not cleaned:
                continue
            marker = cleaned.casefold()
            if marker in seen:
                continue
            seen.add(marker)
            result.append(cleaned)
    return result


_CELL_REF_RE = re.compile(r'^\s*([A-Za-z]+)\s*([0-9]+)\s*$')


def _normalize_cell_reference(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip().upper()
    if not text:
        return None
    match = _CELL_REF_RE.fullmatch(text)
    if match is None:
        raise ValueError(
            'Cell reference must be in A1 format (for example: A1, B3, AA12)'
        )
    return f'{match.group(1)}{int(match.group(2))}'


class SupplierResponseConfigBase(BaseModel):
    name: str = Field(min_length=1, max_length=255)
    is_active: bool = True
    inbox_email_account_id: Optional[int] = Field(default=None, ge=1)
    sender_emails: List[str] = Field(default_factory=list)
    response_type: SupplierResponseType = SupplierResponseType.FILE
    process_shipping_docs: bool = True
    auto_confirm_unmentioned_items: bool = False
    auto_confirm_after_minutes: Optional[int] = Field(default=None, ge=1)
    file_format: Optional[SupplierResponseFileFormat] = (
        SupplierResponseFileFormat.EXCEL
    )
    file_payload_type: SupplierResponseFilePayloadType = (
        SupplierResponseFilePayloadType.RESPONSE
    )
    subject_pattern: Optional[str] = None
    filename_pattern: Optional[str] = None
    shipping_doc_filename_pattern: Optional[str] = None
    start_row: int = Field(default=1, ge=1)
    oem_col: Optional[int] = Field(default=None, ge=1)
    brand_col: Optional[int] = Field(default=None, ge=1)
    name_col: Optional[int] = Field(default=None, ge=1)
    fixed_brand_name: Optional[str] = None
    brand_priority_list: List[str] = Field(default_factory=list)
    brand_from_name_regex: Optional[str] = None
    qty_col: Optional[int] = Field(default=None, ge=1)
    status_col: Optional[int] = Field(default=None, ge=1)
    comment_col: Optional[int] = Field(default=None, ge=1)
    price_col: Optional[int] = Field(default=None, ge=1)
    document_number_col: Optional[int] = Field(default=None, ge=1)
    document_date_col: Optional[int] = Field(default=None, ge=1)
    document_number_cell: Optional[str] = None
    document_date_cell: Optional[str] = None
    document_meta_cell: Optional[str] = None
    gtd_col: Optional[int] = Field(default=None, ge=1)
    country_code_col: Optional[int] = Field(default=None, ge=1)
    country_name_col: Optional[int] = Field(default=None, ge=1)
    total_price_with_vat_col: Optional[int] = Field(default=None, ge=1)
    confirm_keywords: List[str] = Field(
        default_factory=lambda: [
            'в наличии',
            'есть',
            'отгружаем',
            'собрали',
            'да',
        ]
    )
    reject_keywords: List[str] = Field(
        default_factory=lambda: [
            'нет',
            '0',
            'отсутствует',
            'не можем',
            'снято с производства',
        ]
    )
    value_after_article_type: SupplierResponseValueAfterArticleType = (
        SupplierResponseValueAfterArticleType.BOTH
    )

    @field_validator(
        'subject_pattern',
        'filename_pattern',
        'shipping_doc_filename_pattern',
        'brand_from_name_regex',
        mode='before',
    )
    def normalize_patterns(cls, v):
        if v is None:
            return None
        value = str(v).strip()
        return value or None

    @field_validator('name', mode='before')
    def normalize_name(cls, v):
        return str(v or '').strip()

    @field_validator('sender_emails', mode='before')
    def normalize_sender_emails(cls, v):
        return _normalize_email_list(v)

    @field_validator('confirm_keywords', 'reject_keywords', mode='before')
    def normalize_keyword_lists(cls, v):
        return _normalize_string_list(v)

    @field_validator('brand_priority_list', mode='before')
    def normalize_brand_priority_list(cls, v):
        return _normalize_string_list(v)

    @field_validator('fixed_brand_name', mode='before')
    def normalize_fixed_brand_name(cls, v):
        if v is None:
            return None
        value = str(v).strip()
        return value or None

    @field_validator(
        'document_number_cell',
        'document_date_cell',
        'document_meta_cell',
        mode='before',
    )
    def normalize_document_cell_refs(cls, v):
        return _normalize_cell_reference(v)

    @field_validator('brand_from_name_regex')
    def validate_brand_from_name_regex(cls, v):
        if v is None:
            return None
        try:
            re.compile(v)
        except re.error as exc:
            raise ValueError(f'Invalid brand_from_name_regex: {exc}') from exc
        return v


class SupplierResponseConfigCreate(SupplierResponseConfigBase):
    pass


class SupplierResponseConfigUpdate(BaseModel):
    name: Optional[str] = Field(default=None, min_length=1, max_length=255)
    is_active: Optional[bool] = None
    inbox_email_account_id: Optional[int] = Field(default=None, ge=1)
    sender_emails: Optional[List[str]] = None
    response_type: Optional[SupplierResponseType] = None
    process_shipping_docs: Optional[bool] = None
    auto_confirm_unmentioned_items: Optional[bool] = None
    auto_confirm_after_minutes: Optional[int] = Field(default=None, ge=1)
    file_format: Optional[SupplierResponseFileFormat] = None
    file_payload_type: Optional[SupplierResponseFilePayloadType] = None
    subject_pattern: Optional[str] = None
    filename_pattern: Optional[str] = None
    shipping_doc_filename_pattern: Optional[str] = None
    start_row: Optional[int] = Field(default=None, ge=1)
    oem_col: Optional[int] = Field(default=None, ge=1)
    brand_col: Optional[int] = Field(default=None, ge=1)
    name_col: Optional[int] = Field(default=None, ge=1)
    fixed_brand_name: Optional[str] = None
    brand_priority_list: Optional[List[str]] = None
    brand_from_name_regex: Optional[str] = None
    qty_col: Optional[int] = Field(default=None, ge=1)
    status_col: Optional[int] = Field(default=None, ge=1)
    comment_col: Optional[int] = Field(default=None, ge=1)
    price_col: Optional[int] = Field(default=None, ge=1)
    document_number_col: Optional[int] = Field(default=None, ge=1)
    document_date_col: Optional[int] = Field(default=None, ge=1)
    document_number_cell: Optional[str] = None
    document_date_cell: Optional[str] = None
    document_meta_cell: Optional[str] = None
    gtd_col: Optional[int] = Field(default=None, ge=1)
    country_code_col: Optional[int] = Field(default=None, ge=1)
    country_name_col: Optional[int] = Field(default=None, ge=1)
    total_price_with_vat_col: Optional[int] = Field(default=None, ge=1)
    confirm_keywords: Optional[List[str]] = None
    reject_keywords: Optional[List[str]] = None
    value_after_article_type: Optional[
        SupplierResponseValueAfterArticleType
    ] = (
        None
    )

    @field_validator('name', mode='before')
    def normalize_update_name(cls, v):
        if v is None:
            return None
        value = str(v).strip()
        return value or None

    @field_validator(
        'subject_pattern',
        'filename_pattern',
        'shipping_doc_filename_pattern',
        'brand_from_name_regex',
        mode='before',
    )
    def normalize_update_patterns(cls, v):
        if v is None:
            return None
        value = str(v).strip()
        return value or None

    @field_validator('sender_emails', mode='before')
    def normalize_update_sender_emails(cls, v):
        if v is None:
            return None
        return _normalize_email_list(v)

    @field_validator('confirm_keywords', 'reject_keywords', mode='before')
    def normalize_update_keyword_lists(cls, v):
        if v is None:
            return None
        return _normalize_string_list(v)

    @field_validator('brand_priority_list', mode='before')
    def normalize_update_brand_priority_list(cls, v):
        if v is None:
            return None
        return _normalize_string_list(v)

    @field_validator('fixed_brand_name', mode='before')
    def normalize_update_fixed_brand_name(cls, v):
        if v is None:
            return None
        value = str(v).strip()
        return value or None

    @field_validator(
        'document_number_cell',
        'document_date_cell',
        'document_meta_cell',
        mode='before',
    )
    def normalize_update_document_cell_refs(cls, v):
        return _normalize_cell_reference(v)

    @field_validator('brand_from_name_regex')
    def validate_update_brand_from_name_regex(cls, v):
        if v is None:
            return None
        try:
            re.compile(v)
        except re.error as exc:
            raise ValueError(f'Invalid brand_from_name_regex: {exc}') from exc
        return v


class SupplierResponseConfigOut(BaseModel):
    id: int
    provider_id: int
    name: str
    is_active: bool = True
    inbox_email_account_id: Optional[int] = None
    inbox_email_account_name: Optional[str] = None
    inbox_email_account_email: Optional[str] = None
    sender_emails: List[str] = Field(default_factory=list)
    response_type: SupplierResponseType = SupplierResponseType.FILE
    process_shipping_docs: bool = True
    auto_confirm_unmentioned_items: bool = False
    auto_confirm_after_minutes: Optional[int] = None
    file_format: Optional[SupplierResponseFileFormat] = (
        SupplierResponseFileFormat.EXCEL
    )
    file_payload_type: SupplierResponseFilePayloadType = (
        SupplierResponseFilePayloadType.RESPONSE
    )
    subject_pattern: Optional[str] = None
    filename_pattern: Optional[str] = None
    shipping_doc_filename_pattern: Optional[str] = None
    start_row: int = 1
    oem_col: Optional[int] = None
    brand_col: Optional[int] = None
    name_col: Optional[int] = None
    fixed_brand_name: Optional[str] = None
    brand_priority_list: List[str] = Field(default_factory=list)
    brand_from_name_regex: Optional[str] = None
    qty_col: Optional[int] = None
    status_col: Optional[int] = None
    comment_col: Optional[int] = None
    price_col: Optional[int] = None
    document_number_col: Optional[int] = None
    document_date_col: Optional[int] = None
    document_number_cell: Optional[str] = None
    document_date_cell: Optional[str] = None
    document_meta_cell: Optional[str] = None
    gtd_col: Optional[int] = None
    country_code_col: Optional[int] = None
    country_name_col: Optional[int] = None
    total_price_with_vat_col: Optional[int] = None
    confirm_keywords: List[str] = Field(default_factory=list)
    reject_keywords: List[str] = Field(default_factory=list)
    value_after_article_type: SupplierResponseValueAfterArticleType = (
        SupplierResponseValueAfterArticleType.BOTH
    )
    model_config = ConfigDict(from_attributes=True)

    @field_validator(
        'sender_emails',
        'brand_priority_list',
        'confirm_keywords',
        'reject_keywords',
        mode='before',
    )
    def normalize_out_lists(cls, v):
        if v is None:
            return []
        return _normalize_string_list(v)


class CustomerPriceListItem(BaseModel):
    autopart: AutoPartResponse
    quantity: int
    price: float


class CustomerPriceListCreate(BaseModel):
    customer_id: int
    config_id: int
    items: List[int] = Field(default_factory=list)
    excluded_own_positions: Optional[List[int]] = Field(default_factory=list)
    excluded_supplier_positions: Optional[
        Dict[int, List[int]] | List[int]
    ] = Field(default_factory=dict)
    date: Optional[date]

    @model_validator(mode="before")
    def set_default_date(cls, values):
        if 'date' not in values or values['date'] is None:
            values['date'] = date.today()
        return values

    @field_validator('excluded_supplier_positions', mode='before')
    def normalize_excluded_supplier_positions(cls, v):
        if v is None:
            return {}
        if isinstance(v, list):
            return {}
        return v


class PriceIntervalMarkup(BaseModel):
    min_price: Decimal
    max_price: Decimal
    coefficient: float


class SupplierQuantityFilter(BaseModel):
    provider_id: int
    min_quantity: int
    max_quantity: int


class CustomerPriceListConfigBase(BaseModel):
    name: str = Field(
        ..., description='Name or identifier for the configuration'
    )
    general_markup: float = Field(0.0, description='General markup percentage')
    own_price_list_markup: float = Field(
        0.0, description='Markup percentage for own price lists'
    )
    third_party_markup: float = Field(
        0.0, description='Markup percentage for third-party price lists'
    )
    individual_markups: Optional[Dict[int, float]] = Field(
        default_factory=dict,
        description='Individual markups per supplier (provider_id: markup)',
    )
    brand_filters: Optional[List[int]] = Field(
        default_factory=list,
        description='List of brand IDs to include/exclude',
    )
    category_filter: Optional[List[int]] = Field(
        default_factory=list,
        description='List of category IDs to include/exclude',
    )
    price_intervals: Optional[List[PriceIntervalMarkup]] = Field(
        default_factory=list,
        description='List of price intervals with associated coefficients',
    )
    position_filters: Optional[List[int]] = Field(
        default_factory=list,
        description='List of position IDs (autopart IDs) to include/exclude',
    )
    supplier_quantity_filters: Optional[List[SupplierQuantityFilter]] = Field(
        default_factory=list, description='Supplier-specific quantity filters'
    )
    additional_filters: Optional[Dict[str, Any]] = Field(
        default_factory=dict, description='Other custom filters'
    )
    default_filters: Optional[Dict[str, Any]] = Field(
        default_factory=dict,
        description='Default filters (fallback for all suppliers)',
    )
    own_filters: Optional[Dict[str, Any]] = Field(
        default_factory=dict,
        description='Filters for own price list',
    )
    other_filters: Optional[Dict[str, Any]] = Field(
        default_factory=dict,
        description='Filters for all other suppliers',
    )
    supplier_filters: Optional[Dict[str, Any]] = Field(
        default_factory=dict,
        description='Per-supplier filters (provider_id -> filters)',
    )
    export_file_name: Optional[str] = Field(
        default=None, description='Base file name for exported pricelist'
    )
    export_file_format: str = Field(
        default='xlsx', description='Export format for sent pricelist file'
    )
    export_file_extension: Optional[str] = Field(
        default=None, description='File extension for exported pricelist'
    )
    schedule_days: Optional[List[str]] = Field(default_factory=list)
    schedule_times: Optional[List[str]] = Field(default_factory=list)
    emails: Optional[List[EmailStr]] = Field(default_factory=list)
    outgoing_email_account_id: Optional[int] = Field(default=None, ge=1)
    is_active: Optional[bool] = True

    @field_validator('export_file_name', mode='before')
    def normalize_export_file_name(cls, value):
        if value in (None, ''):
            return None
        return str(value).strip() or None

    @field_validator('export_file_format', mode='before')
    def normalize_export_file_format(cls, value):
        normalized = str(value or 'xlsx').strip().lower()
        if normalized not in {'xlsx', 'csv'}:
            raise ValueError('export_file_format must be xlsx or csv')
        return normalized

    @field_validator('export_file_extension', mode='before')
    def normalize_export_file_extension(cls, value):
        if value in (None, ''):
            return None
        normalized = str(value).strip().lstrip('.').lower()
        if not normalized:
            return None
        if not normalized.replace('_', '').isalnum():
            raise ValueError(
                'export_file_extension must contain only letters, '
                'digits or underscore'
            )
        return normalized


class CustomerPriceListConfigCreate(CustomerPriceListConfigBase):
    general_markup: float = Field(
        default=1.0, description='Коэффициент по умолчанию равен 1'
    )
    own_price_list_markup: float = Field(
        default=1.0, description='Коэффициент по умолчанию равен 1'
    )
    third_party_markup: float = Field(
        default=1.0, description='Коэффициент по умолчанию равен 1'
    )


class CustomerPriceListConfigUpdate(BaseModel):
    name: str | None = None
    general_markup: float | None = None
    own_price_list_markup: float | None = None
    third_party_markup: float | None = None
    individual_markups: Optional[Dict[int, float]] = None
    brand_filters: Optional[List[int]] = None
    category_filter: Optional[List[int]] = None
    price_intervals: Optional[List[PriceIntervalMarkup]] = None
    position_filters: Optional[List[int]] = None
    supplier_quantity_filters: Optional[List[SupplierQuantityFilter]] = None
    additional_filters: Optional[Dict[str, Any]] = None
    default_filters: Optional[Dict[str, Any]] = None
    own_filters: Optional[Dict[str, Any]] = None
    other_filters: Optional[Dict[str, Any]] = None
    supplier_filters: Optional[Dict[str, Any]] = None
    export_file_name: Optional[str] = None
    export_file_format: Optional[str] = None
    export_file_extension: Optional[str] = None
    schedule_days: Optional[List[str]] = None
    schedule_times: Optional[List[str]] = None
    emails: Optional[List[EmailStr]] = None
    outgoing_email_account_id: Optional[int] = Field(default=None, ge=1)
    is_active: Optional[bool] = None

    @field_validator('export_file_name', mode='before')
    def normalize_export_file_name(cls, value):
        if value in (None, ''):
            return None
        return str(value).strip() or None

    @field_validator('export_file_format', mode='before')
    def normalize_export_file_format(cls, value):
        if value in (None, ''):
            return None
        normalized = str(value).strip().lower()
        if normalized not in {'xlsx', 'csv'}:
            raise ValueError('export_file_format must be xlsx or csv')
        return normalized

    @field_validator('export_file_extension', mode='before')
    def normalize_export_file_extension(cls, value):
        if value in (None, ''):
            return None
        normalized = str(value).strip().lstrip('.').lower()
        if not normalized:
            return None
        if not normalized.replace('_', '').isalnum():
            raise ValueError(
                'export_file_extension must contain only letters, '
                'digits or underscore'
            )
        return normalized


class CustomerPriceListSourceBase(BaseModel):
    provider_config_id: int
    enabled: bool = True
    markup: float = 1.0
    brand_markups: Optional[Dict[str, float]] = Field(default_factory=dict)
    brand_filters: Optional[Dict[str, Any]] = Field(default_factory=dict)
    position_filters: Optional[Dict[str, Any]] = Field(default_factory=dict)
    min_price: Optional[Decimal] = None
    max_price: Optional[Decimal] = None
    min_quantity: Optional[int] = None
    max_quantity: Optional[int] = None
    additional_filters: Optional[Dict[str, Any]] = Field(default_factory=dict)

    @field_validator('min_price', 'max_price', mode='before')
    def normalize_optional_positive_decimal(cls, value):
        if value in (None, ''):
            return None
        try:
            numeric = Decimal(str(value))
        except Exception:
            return value
        if numeric <= 0:
            return None
        return numeric

    @field_validator('min_quantity', 'max_quantity', mode='before')
    def normalize_optional_positive_int(cls, value):
        if value in (None, ''):
            return None
        try:
            numeric = int(value)
        except Exception:
            return value
        if numeric <= 0:
            return None
        return numeric

    @field_validator('brand_markups', mode='before')
    def normalize_brand_markups(cls, value):
        if value in (None, ''):
            return {}
        if not isinstance(value, dict):
            return {}
        normalized: Dict[str, float] = {}
        for raw_brand, raw_markup in value.items():
            brand_name = str(raw_brand or '').strip()
            if not brand_name:
                continue
            try:
                markup_value = float(raw_markup)
            except (TypeError, ValueError):
                continue
            if markup_value <= 0:
                continue
            normalized[brand_name.upper()] = markup_value
        return normalized


class CustomerPriceListSourceCreate(CustomerPriceListSourceBase):
    pass


class CustomerPriceListSourceUpdate(BaseModel):
    provider_config_id: Optional[int] = None
    enabled: Optional[bool] = None
    markup: Optional[float] = None
    brand_markups: Optional[Dict[str, float]] = None
    brand_filters: Optional[Dict[str, Any]] = None
    position_filters: Optional[Dict[str, Any]] = None
    min_price: Optional[Decimal] = None
    max_price: Optional[Decimal] = None
    min_quantity: Optional[int] = None
    max_quantity: Optional[int] = None
    additional_filters: Optional[Dict[str, Any]] = None

    @field_validator('min_price', 'max_price', mode='before')
    def normalize_optional_positive_decimal(cls, value):
        if value in (None, ''):
            return None
        try:
            numeric = Decimal(str(value))
        except Exception:
            return value
        if numeric <= 0:
            return None
        return numeric

    @field_validator('min_quantity', 'max_quantity', mode='before')
    def normalize_optional_positive_int(cls, value):
        if value in (None, ''):
            return None
        try:
            numeric = int(value)
        except Exception:
            return value
        if numeric <= 0:
            return None
        return numeric

    @field_validator('brand_markups', mode='before')
    def normalize_brand_markups(cls, value):
        if value in (None, ''):
            return {}
        if not isinstance(value, dict):
            return {}
        normalized: Dict[str, float] = {}
        for raw_brand, raw_markup in value.items():
            brand_name = str(raw_brand or '').strip()
            if not brand_name:
                continue
            try:
                markup_value = float(raw_markup)
            except (TypeError, ValueError):
                continue
            if markup_value <= 0:
                continue
            normalized[brand_name.upper()] = markup_value
        return normalized


class CustomerPriceListSourceResponse(CustomerPriceListSourceBase):
    id: int
    provider_id: Optional[int] = None
    provider_name: Optional[str] = None
    provider_config_name: Optional[str] = None
    is_own_price: bool = False
    model_config = ConfigDict(from_attributes=True)


class CustomerPriceListConfigResponse(CustomerPriceListConfigBase):
    id: int
    customer_id: int
    last_sent_at: Optional[datetime] = None
    sources: List[CustomerPriceListSourceResponse] = []
    model_config = ConfigDict(from_attributes=True)


class CustomerPriceListConfigSummary(BaseModel):
    id: int
    name: str
    sources_count: int = 0
    schedule_days: Optional[List[str]] = Field(default_factory=list)
    schedule_times: Optional[List[str]] = Field(default_factory=list)
    is_active: Optional[bool] = True
    model_config = ConfigDict(from_attributes=True)


class AutoPartInPricelist(BaseModel):
    autopart_id: int
    quantity: int
    price: float
    autopart: Optional[AutoPartResponse]
    model_config = {'from_attributes': True}


class CustomerAllPriceListResponse(BaseModel):
    id: int
    date: date
    customer_id: int
    items: List[CustomerPriceListItem]


class PaginatedProvidersResponse(BaseModel):
    items: List[Dict[str, Any]]
    page: int
    page_size: int
    total: int
    pages: int


class ProviderLastUIDOut(BaseModel):
    uid: int
    updated_at: Optional[datetime]
    model_config = ConfigDict(from_attributes=True)


class ProviderAbbreviationOut(BaseModel):
    id: int
    abbreviation: str
    model_config = ConfigDict(from_attributes=True)


class ProviderAbbreviationCreate(BaseModel):
    provider_id: int
    abbreviation: str


class ProviderAbbreviationUpdate(BaseModel):
    abbreviation: Optional[str] = None


class ProviderCoreOut(ProviderBase):
    id: int
    is_virtual: Optional[bool] = False
    is_vat_payer: Optional[bool] = False
    last_email_uid: Optional[ProviderLastUIDOut] = None
    model_config = ConfigDict(from_attributes=True)


class PriceListShort(BaseModel):
    id: int
    date: Optional[date]
    is_active: bool
    model_config = ConfigDict(from_attributes=True)


class PricelistTurnoverItem(BaseModel):
    autopart_id: int
    oem_number: Optional[str] = None
    brand: Optional[str] = None
    name: Optional[str] = None
    old_quantity: int
    new_quantity: int
    quantity_drop: int
    old_price: float
    new_price: float


class PricelistPriceChangeItem(BaseModel):
    autopart_id: int
    oem_number: Optional[str] = None
    brand: Optional[str] = None
    name: Optional[str] = None
    old_price: float
    new_price: float
    price_diff: float
    price_diff_pct: float
    old_quantity: int
    new_quantity: int


class ProviderPricelistAnalysisResponse(BaseModel):
    config_id: int
    config_name: Optional[str] = None
    ready: bool
    note: Optional[str] = None
    latest_pricelist_id: Optional[int] = None
    latest_pricelist_date: Optional[date] = None
    previous_pricelist_id: Optional[int] = None
    previous_pricelist_date: Optional[date] = None
    latest_positions_count: int = 0
    previous_positions_count: int = 0
    new_positions_count: int = 0
    removed_positions_count: int = 0
    changed_price_count: int = 0
    changed_quantity_count: int = 0
    top_turnover_positions: List[PricelistTurnoverItem] = Field(
        default_factory=list
    )
    sharpest_price_changes: List[PricelistPriceChangeItem] = Field(
        default_factory=list
    )


class ProviderPriceListConfigOut(BaseModel):
    id: int
    incoming_email_account_id: int | None = None
    filename_pattern: str | None = None
    name_price: str | None = None
    name_mail: str | None = None
    file_url: str | None = None
    start_row: int
    oem_col: int
    name_col: int | None = None
    brand_col: int | None = None
    multiplicity_col: int | None = None
    qty_col: int
    price_col: int
    min_price: float | None = None
    max_price: float | None = None
    min_quantity: int | None = None
    max_quantity: int | None = None
    exclude_positions: List[Dict[str, str]] = Field(default_factory=list)
    max_days_without_update: int | None = 3
    min_delivery_day: int | None = 1
    max_delivery_day: int | None = 3
    is_active: bool = True
    latest_pricelist: Optional[PriceListShort] = None

    model_config = ConfigDict(from_attributes=True)


class ProviderPriceListConfigOption(BaseModel):
    id: int
    provider_id: int
    provider_name: str
    name_price: Optional[str] = None
    is_own_price: bool = False


class ProviderCustomerPriceListSourceUsageOut(BaseModel):
    source_id: int
    customer_id: int
    customer_name: Optional[str] = None
    customer_config_id: int
    customer_config_name: Optional[str] = None
    provider_config_id: int
    provider_config_name: Optional[str] = None
    enabled: bool = True
    markup: float = 1.0
    brand_markups: Optional[Dict[str, float]] = Field(default_factory=dict)
    brand_filters: Optional[Dict[str, Any]] = Field(default_factory=dict)
    position_filters: Optional[Dict[str, Any]] = Field(default_factory=dict)
    min_price: Optional[Decimal] = None
    max_price: Optional[Decimal] = None
    min_quantity: Optional[int] = None
    max_quantity: Optional[int] = None
    additional_filters: Optional[Dict[str, Any]] = Field(default_factory=dict)


CustomerResponse.model_rebuild()
PriceListResponse.model_rebuild()


class ProviderPageResponse(BaseModel):
    provider: ProviderCoreOut
    abbreviations: List[ProviderAbbreviationOut] = Field(default_factory=list)
    pricelist_configs: List[ProviderPriceListConfigOut] = Field(
        default_factory=list
    )
    supplier_response_configs: List[SupplierResponseConfigOut] = Field(
        default_factory=list
    )
    customer_pricelist_sources_usage: List[
        ProviderCustomerPriceListSourceUsageOut
    ] = Field(default_factory=list)
