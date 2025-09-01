from datetime import datetime
from decimal import Decimal
from typing import List, Optional

from pydantic import BaseModel, Field

from dz_fastapi.models.autopart import (TYPE_SEND_METHOD,
                                        TYPE_SUPPLIER_DECISION_STATUS)
from dz_fastapi.models.partner import TYPE_ORDER_ITEM_STATUS, TYPE_STATUS_ORDER


class SupplierOfferOut(BaseModel):
    autopart_id: int = Field(..., description='ID автозапчасти')
    oem_number: str = Field(..., description='OEM номер детали')
    autopart_name: str = Field(..., description='Название детали')
    supplier_id: int = Field(..., description='ID поставщика')
    supplier_name: str = Field(..., description='Имя поставщика')
    price: float = Field(..., description='Цена за штуку')
    quantity: int = Field(..., description='Количество к заказу')
    total_cost: float = Field(..., description='Общая стоимость')
    qnt: int = Field(..., description='Количество на остатках у поставщика')
    min_qnt: int = Field(..., description='Минимальная кратность заказа')
    min_delivery_day: int = Field(
        ..., description='Минимальный срок доставки в днях'
    )
    max_delivery_day: int = Field(
        ..., description='Максимальный срок доставки в днях'
    )
    historical_min_price: float = Field(
        ..., description='Исторически минимальная цена'
    )
    sup_logo: str = Field(..., description='Абривиатура поставщика')
    brand_name: str = Field(..., description='Имя бренда')
    hash_key: Optional[str] = Field(None, description='Hash ключ, если есть')
    system_hash: Optional[str] = Field(
        None, description='System hash, если есть'
    )


class SupplierOffersResponse(BaseModel):
    offers: list[SupplierOfferOut]


class ConfirmedOfferOut(BaseModel):
    autopart_id: int = Field(..., description='ID автозапчасти')
    supplier_id: int = Field(..., description='ID поставщика')
    quantity: int = Field(..., description='Количество к заказу')
    confirmed_price: float = Field(..., description='Цена за штуку')
    status: TYPE_SUPPLIER_DECISION_STATUS = Field(
        ..., description='Статус подтверждения'
    )
    brand_name: Optional[str] = Field(..., description='Имя бренда для заказа')
    min_delivery_day: int = Field(..., description='Минимальный срок доставки')
    max_delivery_day: int = Field(
        ..., description='Максимальный срок доставки'
    )
    send_method: Optional[TYPE_SEND_METHOD] = Field(
        None, description='Способ отправки'
    )
    model_config = {'from_attributes': True, 'use_enum_values': True}


class ConfirmedOffersResponse(BaseModel):
    confirmed_offers: List[ConfirmedOfferOut]
    total_items: int


class OrderPositionOut(BaseModel):
    autopart_id: int = Field(..., description='ID автозапчасти')
    oem_number: str = Field(..., description='OEM номер детали')
    brand_name: str = Field(..., description='Имя бренда')
    autopart_name: Optional[str] = Field(None, description='Название детали')
    supplier_id: Optional[int] = Field(None, description='ID поставщика')
    quantity: int = Field(..., description='Количество к заказу')
    confirmed_price: float = Field(..., description='Цена за штуку')
    min_delivery_day: Optional[int] = Field(
        None, description='Минимальный срок доставки в днях'
    )
    max_delivery_day: Optional[int] = Field(
        None, description='Максимальный срок доставки в днях'
    )
    status: TYPE_SUPPLIER_DECISION_STATUS = Field(
        ..., description='Статус подтверждения'
    )
    created_at: Optional[datetime] = Field(
        None, description='Время создания заказа'
    )
    updated_at: Optional[datetime] = Field(
        None, description='Время изменения заказа'
    )
    tracking_uuid: Optional[str] = Field(None, description='Уникальный индекс')
    hash_key: Optional[str] = Field(None, description='Hash ключ')
    system_hash: Optional[str] = Field(None, description='System hash ключ')
    model_config = {'use_enum_values': True}


class SendApiResponse(BaseModel):
    total_items: int = Field(..., description='Общее количество позиций')
    successful_items: int = Field(
        ..., description='Успешно отправленных позиций'
    )
    failed_items: int = Field(..., description='Неудачных позиций')
    results: List[dict] = Field(..., description='Детали по каждой позиции')
    order_id: Optional[int] = Field(None, description='Order ID')
    order_number: Optional[str] = Field(None, description='Order Numer')


class SupplierOrderOut(BaseModel):
    supplier_id: int = Field(..., description='ID поставщика')
    supplier_name: str = Field(..., description='Имя поставщика')
    total_sum: float = Field(..., description='Общая стоимость')
    min_delivery_day: Optional[int] = Field(
        None, description='Минимальный срок доставки в днях'
    )
    max_delivery_day: Optional[int] = Field(
        None, description='Максимальный срок доставки в днях'
    )
    order_status: TYPE_SUPPLIER_DECISION_STATUS = Field(
        ..., description='Статус подтверждения всего заказ'
    )
    positions: List[OrderPositionOut]
    send_method: TYPE_SEND_METHOD = Field(..., description='Метод заказа')
    model_config = {'from_attributes': True, 'use_enum_values': True}


class UpdatedItemInfo(BaseModel):
    tracking_uuid: str = Field(..., description='tracking UUID для обновления')
    old_status: TYPE_SUPPLIER_DECISION_STATUS = Field(
        ..., description='Старый статус позиции для заказа'
    )
    new_status: TYPE_SUPPLIER_DECISION_STATUS = Field(
        ..., description='Новый статус'
    )


class UpdatePositionStatusResponse(BaseModel):
    message: str
    updated_count: int
    updated_items: List[UpdatedItemInfo]


class UpdatePositionStatusRequest(BaseModel):
    tracking_uuids: list[str] = Field(
        ..., description='Список tracking UUID для обновления'
    )
    status: TYPE_SUPPLIER_DECISION_STATUS = Field(
        ..., description='Новый статус'
    )
    model_config = {'use_enum_values': True}


class OrderItemIn(BaseModel):
    order_id: int
    autopart_id: int
    quantity: int
    price: Decimal
    comments: Optional[str] = None
    hash_key: Optional[str] = None
    system_hash: Optional[str] = None
    restock_supplier_id: Optional[int] = None
    model_config = {'from_attributes': True}


class OrderItemUpdate(BaseModel):
    quantity: Optional[int] = None
    price: Optional[Decimal] = None
    comments: Optional[str] = None
    status: Optional[TYPE_ORDER_ITEM_STATUS] = None
    restock_supplier_id: Optional[int] = None
    model_config = {'from_attributes': True, 'use_enum_values': True}


class OrderIn(BaseModel):
    provider_id: int
    customer_id: int
    comment: Optional[str] = None


class OrderUpdate(BaseModel):
    status: Optional[TYPE_STATUS_ORDER] = None
    comment: Optional[str] = None
    model_config = {'use_enum_values': True}


class OrderItemOut(BaseModel):
    id: int
    order_id: int
    autopart_id: int
    quantity: int
    price: Decimal
    tracking_uuid: str
    status: TYPE_ORDER_ITEM_STATUS
    comments: Optional[str] = None
    hash_key: Optional[str] = None
    system_hash: Optional[str] = None
    created_at: datetime
    updated_at: datetime
    restock_supplier_id: Optional[int] = None
    model_config = {'from_attributes': True, 'use_enum_values': True}


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
    model_config = {'from_attributes': True, 'use_enum_values': True}
