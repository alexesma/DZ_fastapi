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

    @field_validator('email_incoming_price', mode='before')
    def validate_email_incoming_price(cls, v):
        if v == '':
            return None
        return v

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

    @field_validator('email_contact', 'email_incoming_price', mode='before')
    def empty_to_none(cls, v):
        if v == '':
            return None
        return v


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
    model_config = ConfigDict(from_attributes=True)


class PriceListAutoPartAssociationCreate(BaseModel):
    autopart: AutoPartPricelist
    quantity: int
    price: float
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


class ProviderPriceListConfigBase(BaseModel):
    start_row: int
    oem_col: int
    name_col: Optional[int] = None
    brand_col: Optional[int] = None
    qty_col: int
    price_col: int
    name_price: Optional[str] = None
    name_mail: Optional[str] = None
    file_url: Optional[str] = None
    min_delivery_day: Optional[int] = Field(default=1, ge=0)
    max_delivery_day: Optional[int] = Field(default=3, ge=0)


class ProviderPriceListConfigCreate(ProviderPriceListConfigBase):
    pass


class ProviderPriceListConfigUpdate(BaseModel):
    start_row: Optional[int] = Field(default=None, ge=0)
    oem_col: Optional[int] = Field(default=None, ge=0)
    name_col: Optional[int] = Field(default=None, ge=0)
    brand_col: Optional[int] = Field(default=None, ge=0)
    qty_col: Optional[int] = Field(default=None, ge=0)
    price_col: Optional[int] = Field(default=None, ge=0)
    name_price: Optional[str] = None
    name_mail: Optional[str] = None
    file_url: Optional[str] = None
    min_delivery_day: Optional[int] = Field(default=None, ge=0)
    max_delivery_day: Optional[int] = Field(default=None, ge=0)


class ProviderPriceListConfigResponse(ProviderPriceListConfigBase):
    id: int
    provider_id: int
    model_config = ConfigDict(from_attributes=True)


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
    schedule_days: Optional[List[str]] = Field(default_factory=list)
    schedule_times: Optional[List[str]] = Field(default_factory=list)
    emails: Optional[List[EmailStr]] = Field(default_factory=list)
    is_active: Optional[bool] = True


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
    category_filters: Optional[List[int]] = None
    price_intervals: Optional[List[PriceIntervalMarkup]] = None
    position_filters: Optional[List[int]] = None
    supplier_quantity_filters: Optional[List[SupplierQuantityFilter]] = None
    additional_filters: Optional[Dict[str, Any]] = None
    schedule_days: Optional[List[str]] = None
    schedule_times: Optional[List[str]] = None
    emails: Optional[List[EmailStr]] = None
    is_active: Optional[bool] = None


class CustomerPriceListSourceBase(BaseModel):
    provider_config_id: int
    enabled: bool = True
    markup: float = 1.0
    brand_filters: Optional[Dict[str, Any]] = Field(default_factory=dict)
    position_filters: Optional[Dict[str, Any]] = Field(default_factory=dict)
    min_price: Optional[Decimal] = None
    max_price: Optional[Decimal] = None
    min_quantity: Optional[int] = None
    max_quantity: Optional[int] = None
    additional_filters: Optional[Dict[str, Any]] = Field(default_factory=dict)


class CustomerPriceListSourceCreate(CustomerPriceListSourceBase):
    pass


class CustomerPriceListSourceUpdate(BaseModel):
    provider_config_id: Optional[int] = None
    enabled: Optional[bool] = None
    markup: Optional[float] = None
    brand_filters: Optional[Dict[str, Any]] = None
    position_filters: Optional[Dict[str, Any]] = None
    min_price: Optional[Decimal] = None
    max_price: Optional[Decimal] = None
    min_quantity: Optional[int] = None
    max_quantity: Optional[int] = None
    additional_filters: Optional[Dict[str, Any]] = None


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
    last_email_uid: Optional[ProviderLastUIDOut] = None
    model_config = ConfigDict(from_attributes=True)


class PriceListShort(BaseModel):
    id: int
    date: Optional[date]
    is_active: bool
    model_config = ConfigDict(from_attributes=True)


class ProviderPriceListConfigOut(BaseModel):
    id: int
    name_price: str | None = None
    name_mail: str | None = None
    file_url: str | None = None
    start_row: int
    oem_col: int
    name_col: int | None = None
    brand_col: int | None = None
    qty_col: int
    price_col: int
    min_delivery_day: int | None = 1
    max_delivery_day: int | None = 3
    latest_pricelist: Optional[PriceListShort] = None

    model_config = ConfigDict(from_attributes=True)


class ProviderPriceListConfigOption(BaseModel):
    id: int
    provider_id: int
    provider_name: str
    name_price: Optional[str] = None
    is_own_price: bool = False


CustomerResponse.model_rebuild()
PriceListResponse.model_rebuild()


class ProviderPageResponse(BaseModel):
    provider: ProviderCoreOut
    abbreviations: List[ProviderAbbreviationOut] = Field(default_factory=list)
    pricelist_configs: List[ProviderPriceListConfigOut] = Field(
        default_factory=list
    )
