from pydantic import BaseModel, EmailStr, field_validator, ConfigDict, Field, root_validator, model_validator
from typing import List, Optional, Dict, Any
from datetime import date
from decimal import Decimal
from enum import Enum
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

    @field_validator('email_incoming_price', mode='before')
    def validate_email_incoming_price(cls, v):
        if v == '':
            return None
        return v

    model_config = ConfigDict(from_attributes=True)


class ProviderCreate(ProviderBase):
    pass


class ProviderUpdate(ProviderBase):
    pass


class CustomerBase(ClientBase):
    email_outgoing_price: Optional[EmailStr] = None

    @field_validator('email_outgoing_price', mode='before')
    def validate_email_outgoing_price(cls, v):
        if v == '':
            return None
        return v

    model_config = ConfigDict(from_attributes=True)


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
    is_active: bool = True


class PriceListCreate(PriceListBase):
    provider_id: int
    autoparts: List[PriceListAutoPartAssociationCreate] = []


class PriceListUpdate(PriceListBase):
    provider_id: Optional[int] = None
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
    autoparts: List[PriceListAutoPartAssociationResponse] = []
    model_config = ConfigDict(from_attributes=True)


class PriceListSummary(BaseModel):
    id: int
    date: date
    num_positions: int
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
    autoparts: Optional[
        List[CustomerPriceListAutoPartAssociationResponse]
    ] = None


class CustomerMinimalResponse(BaseModel):
    id: int
    name: str
    model_config = ConfigDict(from_attributes=True)


class CustomerPriceListResponse(CustomerPriceListBase):
    id: int
    date: date
    customer_id: int
    autoparts: List['AutoPartInPricelist']


class ProviderResponse(ProviderBase):
    id: int
    price_lists: List[PriceListResponse] = []
    model_config = ConfigDict(from_attributes=True)


class CustomerResponse(CustomerBase):
    id: int
    customer_price_lists: List[CustomerPriceListResponse] = []
    model_config = ConfigDict(from_attributes=True)


class ProviderPriceListConfigBase(BaseModel):
    start_row: int
    oem_col: int
    name_col: Optional[int] = None
    brand_col: Optional[int] = None
    qty_col: int
    price_col: int


class ProviderPriceListConfigCreate(ProviderPriceListConfigBase):
    pass


class ProviderPriceListConfigUpdate(ProviderPriceListConfigBase):
    pass


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
    items: List[int]
    excluded_own_positions: Optional[List[int]] = Field(
        default_factory=list
    )
    excluded_supplier_positions: Optional[List[int]] = Field(
        default_factory=list
    )
    date: Optional[date]

    @model_validator(mode="before")
    def set_default_date(cls, values):
        if 'date' not in values or values['date'] is None:
            values['date'] = date.today()
        return values


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
        ...,
        description='Name or identifier for the configuration'
    )
    general_markup: float = Field(
        0.0,
        description='General markup percentage'
    )
    own_price_list_markup: float = Field(
        0.0,
        description='Markup percentage for own price lists'
    )
    third_party_markup: float = Field(
        0.0,
        description='Markup percentage for third-party price lists'
    )
    individual_markups: Optional[Dict[int, float]] = Field(
        default_factory=dict,
        description='Individual markups per supplier (provider_id: markup)'
    )
    brand_filters: Optional[List[int]] = Field(
        default_factory=list,
        description='List of brand IDs to include/exclude'
    )
    category_filter: Optional[List[int]] = Field(
        default_factory=list,
        description='List of category IDs to include/exclude'
    )
    price_intervals: Optional[List[PriceIntervalMarkup]] = Field(
        default_factory=list,
        description='List of price intervals with associated coefficients'
    )
    position_filters: Optional[List[int]] = Field(
        default_factory=list,
        description='List of position IDs (autopart IDs) to include/exclude'
    )
    supplier_quantity_filters: Optional[List[SupplierQuantityFilter]] = Field(
        default_factory=list,
        description='Supplier-specific quantity filters'
    )
    additional_filters: Optional[Dict[str, Any]] = Field(
        default_factory=dict,
        description='Other custom filters'
    )


class CustomerPriceListConfigCreate(CustomerPriceListConfigBase):
    general_markup: float = Field(
        default=1.0,
        description='Коэффициент по умолчанию равен 1'
    )
    own_price_list_markup: float = Field(
        default=1.0,
        description='Коэффициент по умолчанию равен 1'
    )
    third_party_markup: float = Field(
        default=1.0,
        description='Коэффициент по умолчанию равен 1'
    )


class CustomerPriceListConfigUpdate(BaseModel):
    name: Optional[str] = None
    general_markup: Optional[float] = None
    own_price_list_markup: Optional[float] = None
    third_party_markup: Optional[float] = None
    individual_markups: Optional[Dict[int, float]] = None
    brand_filters: Optional[List[int]] = None
    category_filters: Optional[List[int]] = None
    price_intervals: Optional[List[PriceIntervalMarkup]] = None
    position_filters: Optional[List[int]] = None
    supplier_quantity_filters: Optional[List[SupplierQuantityFilter]] = None
    additional_filters: Optional[Dict[str, Any]] = None


class CustomerPriceListConfigResponse(CustomerPriceListConfigBase):
    id: int
    customer_id: int
    model_config = {'from_attributes': True}


class AutoPartInPricelist(BaseModel):
    autopart_id: int
    quantity: int
    price: float

class CustomerAllPriceListResponse(BaseModel):
    id: int
    date: date
    customer_id: int
    items: List[CustomerPriceListItem]