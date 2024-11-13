from pydantic import BaseModel, EmailStr, field_validator, ConfigDict
from typing import List, Optional
from datetime import date
from decimal import Decimal
from enum import Enum
from dz_fastapi.schemas.autopart import AutoPartPricelist


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
    autopart: AutoPartPricelist
    quantity: int
    price: Decimal
    model_config = ConfigDict(from_attributes=True)


class PriceListAutoPartAssociationCreate(BaseModel):
    autopart: AutoPartPricelist
    quantity: int
    price: Decimal
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
    autoparts: List[PriceListAutoPartAssociationCreate] = []


class PriceListUpdate(BaseModel):
    date: Optional[date] = None
    is_active: Optional[bool] = None
    autoparts: Optional[List[PriceListAutoPartAssociationCreate]] = None


class ProviderMinimalResponse(BaseModel):
    id: int
    name: str
    model_config = ConfigDict(from_attributes=True)



class PriceListResponse(PriceListBase):
    id: int
    date: Optional[date] = None
    provider: ProviderMinimalResponse
    autoparts: List[PriceListAutoPartAssociationResponse] = []
    model_config = ConfigDict(from_attributes=True)


class CustomerPriceListBase(BaseModel):
    date: Optional[date] = None
    customer_id: int
    is_active: bool = True


class CustomerPriceListCreate(CustomerPriceListBase):
    autoparts: List[CustomerPriceListAutoPartAssociationResponse] = []


class CustomerPriceListUpdate(BaseModel):
    date: Optional[date] = None
    is_active: Optional[bool] = None
    autoparts: Optional[List[CustomerPriceListAutoPartAssociationResponse]] = None


class CustomerMinimalResponse(BaseModel):
    id: int
    name: str
    model_config = ConfigDict(from_attributes=True)


class CustomerPriceListResponse(CustomerPriceListBase):
    id: int
    date: Optional[date] = None
    customer: CustomerMinimalResponse
    autoparts: List[CustomerPriceListAutoPartAssociationResponse] = []
    model_config = ConfigDict(from_attributes=True)


class ProviderResponse(ProviderBase):
    id: int
    price_lists: List[PriceListResponse] = []
    model_config = ConfigDict(from_attributes=True)


class CustomerResponse(CustomerBase):
    id: int
    customer_price_lists: List[CustomerPriceListResponse] = []
    model_config = ConfigDict(from_attributes=True)
