import os
from datetime import date
from typing import Annotated, Dict, List, Optional, Tuple

from pydantic import (BaseModel, ConfigDict, EmailStr, Field,
                      StringConstraints, field_validator)

from dz_fastapi.core.constants import (DEPTH_MONTHS_HISTORY_PRICE_FOR_ORDER,
                                       LIMIT_ORDER, MAX_LIGHT_NAME_LOCATION,
                                       MAX_NAME_CATEGORY,
                                       PERCENT_MIN_BALANS_FOR_ORDER)

EMAIL_NAME_ORDER = os.getenv('EMAIL_NAME_ANALYTIC')
TELEGRAM_TO = os.getenv('TELEGRAM_TO')


class AutoPartBase(BaseModel):
    brand_id: int
    oem_number: str
    name: str
    description: Optional[str] = None
    width: Optional[float] = None
    height: Optional[float] = None
    length: Optional[float] = None
    weight: Optional[float] = None
    purchase_price: Optional[float] = None
    retail_price: Optional[float] = None
    wholesale_price: Optional[float] = None
    multiplicity: Optional[int] = None
    minimum_balance: Optional[int] = None
    min_balance_auto: Optional[bool] = None
    min_balance_user: Optional[bool] = None
    comment: Optional[str] = None
    barcode: Optional[str] = None


class AutoPartResponse(BaseModel):
    id: int
    brand_id: int
    oem_number: Optional[str]
    name: Optional[str]
    description: Optional[str] = None
    width: Optional[float] = None
    height: Optional[float] = None
    length: Optional[float] = None
    weight: Optional[float] = None
    purchase_price: Optional[float] = None
    retail_price: Optional[float] = None
    wholesale_price: Optional[float] = None
    multiplicity: Optional[int] = None
    minimum_balance: Optional[int] = None
    min_balance_auto: Optional[bool] = None
    min_balance_user: Optional[bool] = None
    comment: Optional[str] = None
    barcode: Optional[str] = None
    categories: List[str] = Field(default_factory=list)
    storage_locations: List[str] = Field(default_factory=list)

    model_config = ConfigDict(from_attributes=True)

    @field_validator('categories', mode='before')
    def get_category_names(cls, v):
        if v:
            return [category.name for category in v]
        return []

    @field_validator('storage_locations', mode='before')
    def get_storage_location_names(cls, v):
        if v:
            return [storage_location.name for storage_location in v]
        return []


class AutoPartCreate(BaseModel):
    brand_id: int
    oem_number: str
    name: str
    description: Optional[str] = None
    width: Optional[float] = None
    height: Optional[float] = None
    length: Optional[float] = None
    weight: Optional[float] = None
    purchase_price: Optional[float] = None
    retail_price: Optional[float] = None
    wholesale_price: Optional[float] = None
    multiplicity: Optional[int] = None
    minimum_balance: Optional[int] = None
    min_balance_auto: Optional[bool] = None
    min_balance_user: Optional[bool] = None
    comment: Optional[str] = None
    barcode: Optional[str] = None
    category_name: Optional[str] = None
    storage_location_name: Optional[str] = None


class AutoPartCreatePriceList(BaseModel):
    oem_number: str
    brand: Optional[str] = None
    name: Optional[str] = None
    multiplicity: Optional[int] = None
    purchase_price: Optional[float] = None
    retail_price: Optional[float] = None
    wholesale_price: Optional[float] = None
    comment: Optional[str] = None
    model_config = ConfigDict(from_attributes=True)


class AutoPartCreateInDB(AutoPartBase):
    pass


class AutoPartUpdate(BaseModel):
    brand_id: Optional[int] = None
    name: Optional[str] = None
    oem_number: Optional[str] = None
    description: Optional[str] = None
    width: Optional[float] = None
    height: Optional[float] = None
    length: Optional[float] = None
    weight: Optional[float] = None
    purchase_price: Optional[float] = None
    retail_price: Optional[float] = None
    wholesale_price: Optional[float] = None
    multiplicity: Optional[int] = None
    minimum_balance: Optional[int] = None
    min_balance_auto: Optional[bool] = None
    min_balance_user: Optional[bool] = None
    comment: Optional[str] = None
    barcode: Optional[str] = None
    category_name: Optional[str] = None
    storage_location_name: Optional[str] = None


class AutoPartUpdateInDB(AutoPartBase):
    pass


class AutoPartPricelist(BaseModel):
    brand: Optional[str] = None
    oem_number: str
    name: Optional[str] = None
    multiplicity: Optional[int] = None
    purchase_price: Optional[float] = None
    retail_price: Optional[float] = None
    wholesale_price: Optional[float] = None
    comment: Optional[str] = None
    model_config = ConfigDict(from_attributes=True)


class NotFoundPartResponse(BaseModel):
    record: Dict[str, str]
    error: str


class BulkUpdateResponse(BaseModel):
    updated_count: int
    not_found_parts: List[NotFoundPartResponse]


class AutopartOfferRow(BaseModel):
    autopart_id: int
    oem_number: str
    brand_name: Optional[str] = None
    name: Optional[str] = None
    provider_id: int
    provider_name: str
    provider_config_id: Optional[int] = None
    provider_config_name: Optional[str] = None
    price: float
    quantity: int
    min_delivery_day: Optional[int] = None
    max_delivery_day: Optional[int] = None
    pricelist_id: int
    pricelist_date: Optional[date] = None
    is_own_price: bool = False


class AutopartOffersResponse(BaseModel):
    oem_number: str
    offers: List[AutopartOfferRow] = Field(default_factory=list)


# Base schema with shared fields
class CategoryBase(BaseModel):
    name: str = Field(..., max_length=MAX_NAME_CATEGORY)
    comment: Optional[str] = None


# Schema for creating a new category
class CategoryCreate(CategoryBase):
    parent_id: Optional[int] = None


# Schema for updating an existing category
class CategoryUpdate(BaseModel):
    name: Optional[str] = Field(
        None, min_length=1, max_length=MAX_NAME_CATEGORY
    )
    comment: Optional[str] = None
    parent_id: Optional[int] = None


# Response schema
class CategoryResponse(CategoryBase):
    id: int
    name: str
    parent_id: Optional[int] = None
    children: Optional[List['CategoryResponse']] = Field(default_factory=list)
    # autoparts: List['AutoPartResponse'] = Field(default_factory=list)

    model_config = ConfigDict(from_attributes=True)

    @field_validator('children', mode='before')
    def set_children(cls, v):
        if v is None:
            return []
        elif isinstance(v, list):
            return v
        else:
            return [v]


CategoryResponse.model_rebuild()

# StorageLocation Schemas


class StorageLocationBase(BaseModel):
    name: Annotated[
        str,
        StringConstraints(
            pattern='^[A-Z0-9 /]+$', max_length=MAX_LIGHT_NAME_LOCATION
        ),
    ]


class StorageLocationCreate(StorageLocationBase):
    pass


class StorageLocationUpdate(BaseModel):
    name: Annotated[
        Optional[str],
        StringConstraints(
            pattern='^[A-Z0-9 /]+$', max_length=MAX_LIGHT_NAME_LOCATION
        ),
    ] = None


class StorageLocationResponse(StorageLocationBase):
    id: int
    name: str
    autoparts: List[AutoPartResponse] = Field(default_factory=list)

    model_config = ConfigDict(from_attributes=True)


StorageLocationResponse.model_rebuild()
AutoPartResponse.model_rebuild()


class AutopartOrderRequest(BaseModel):
    budget_limit: int = Field(
        default=LIMIT_ORDER, gt=0, description='Максимальный бюджет для заказа'
    )
    months_back: int = Field(
        default=DEPTH_MONTHS_HISTORY_PRICE_FOR_ORDER,
        ge=1,
        description='Глубина поиска минимальной цены (в месяцах)',
    )
    email_to: EmailStr = Field(
        default=EMAIL_NAME_ORDER, description='Email получателя отчета'
    )
    telegram_chat_id: str = Field(
        default=TELEGRAM_TO, description='Telegram чат для отправки отчета'
    )
    autoparts: Optional[Dict[int, Tuple[float, float]]] = Field(
        None,
        description='Автозапчасти с минимальным '
        'балансом и количеством для заказа',
    )
    threshold_percent: float = Field(
        default=PERCENT_MIN_BALANS_FOR_ORDER,
        gt=0,
        lt=1,
        description='Пороговый процент остатка для формирования заказа',
    )


class ConfirmedOffer(BaseModel):
    autoparts_id: int = Field(..., description='ID автозапчасти')
    supplier_id: int = Field(..., description='ID поставщика')
    supplier_name: str = Field(..., description='Название постащика')
    quantity: int = Field(..., gt=0, description='Заказываемое количество')
    price: float = Field(..., gt=0, description='Цена за единицу товара')
    total_cost: float = Field(..., gt=0, description='Общая стоимость позиции')
    historical_min_price: int = Field(
        ..., gt=0, description='Исторически минимальная цена'
    )
