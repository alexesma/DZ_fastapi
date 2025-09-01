import logging
from typing import Optional, List

from sqlalchemy.future import select
from sqlalchemy.orm import selectinload

from dz_fastapi.core.db import AsyncSession
from dz_fastapi.crud.base import CRUDBase
from dz_fastapi.models.partner import (
    Order,
    OrderItem,
    TYPE_ORDER_ITEM_STATUS,
    TYPE_STATUS_ORDER
)
from dz_fastapi.schemas.order import (
    OrderItemIn,
    OrderItemUpdate,
    OrderIn,
    OrderUpdate,
    OrderPositionOut
)

logger = logging.getLogger('dz_fastapi')


class CRUDOrderItem(
    CRUDBase[
        OrderItem, OrderItemIn, OrderItemUpdate
    ]
):
    async def get_order_item_by_uuid(
            self,
            tracking_uuid: str,
            session: AsyncSession,
    ) -> OrderItem:
        '''Получение OrderItem по tracking_uuid'''
        result = await session.execute(
            select(OrderItem).
            where(OrderItem.tracking_uuid == tracking_uuid)
        )
        return result.scalar_one_or_none()

    async def get_order_item_by_hash(
            self,
            hash_key: str,
            session: AsyncSession,
    ) -> OrderItem:
        '''Получение OrderItem по hash_key'''
        result = await session.execute(
            select(OrderItem).
            where(OrderItem.hash_key == hash_key)
        )
        return result.scalar_one_or_none()

    async def create_order(
            self,
            order_item_in: OrderItemIn,
            session: AsyncSession,
    ) -> OrderItem:
        '''Создание нового OrderItem'''
        exist_order_tracking_uuid = await self.get_order_item_by_hash(
            hash_key=order_item_in.hash_key,
            session=session
        )
        if exist_order_tracking_uuid:
            raise ValueError(
                f'The order hash_key = '
                f'{order_item_in.hash_key} already exists.'
            )
        new_order_item = OrderItem(
            order_id=order_item_in.order_id,
            autopart_id=order_item_in.autopart_id,
            quantity=order_item_in.quantity,
            price=order_item_in.price,
            comments=order_item_in.comments,
            status=TYPE_ORDER_ITEM_STATUS.NEW,
            hash_key=order_item_in.hash_key,
            system_hash=order_item_in.system_hash,
            restock_supplier_id=order_item_in.restock_supplier_id,
        )

        session.add(new_order_item)
        await session.commit()
        await session.refresh(new_order_item)
        return new_order_item

    async def get_order_items_by_order_id(
            self,
            order_id: int,
            session: AsyncSession,
    ):
        '''Получение всех OrderItem для конкретного заказа'''
        result = await session.execute(
            select(OrderItem).where(OrderItem.order_id == order_id)
        )
        return result.scalars().all()

    async def update_order_item_status(
            self,
            tracking_uuid: str,
            new_status: TYPE_ORDER_ITEM_STATUS,
            session: AsyncSession,
    ) -> Optional[OrderItem]:
        '''Обновление статуса OrderItem'''
        order_item = await self.get_order_item_by_uuid(
            tracking_uuid=tracking_uuid,
            session=session
        )
        if not order_item:
            return None

        order_item.status = new_status
        await session.commit()
        await session.refresh(order_item)
        return order_item


crud_order_item = CRUDOrderItem(
    OrderItem
)


class CRUDOrder(
    CRUDBase[
        Order, OrderIn, OrderUpdate
    ]
):
    async def create_order_with_items(
            self,
            provider_id: int,
            customer_id: int,
            items: List[OrderPositionOut],
            session: AsyncSession,
            comment: str = None
    ):
        '''Создание заказа с позициями'''
        new_order = Order(
            provider_id=provider_id,
            customer_id=customer_id,
            status=TYPE_STATUS_ORDER.ORDERED,
            comment=comment
        )
        session.add(new_order)
        await session.flush()

        order_items = []
        for item in items:
            order_item = OrderItem(
                order_id=new_order.id,
                autopart_id=item.autopart_id,
                quantity=item.quantity,
                price=item.confirmed_price,
                comments=None,
                status=TYPE_ORDER_ITEM_STATUS.NEW,
                hash_key=item.hash_key,
                system_hash=item.system_hash
            )
            session.add(order_item)
            order_items.append(order_item)

        await session.commit()
        await session.refresh(new_order)
        return new_order

    async def get_orders_by_provider(
            self,
            provider_id: int,
            session: AsyncSession
    ) -> List[Order]:
        '''Получение заказов по поставщику'''
        result = await session.execute(
            select(Order).where(Order.provider_id == provider_id)
        )
        return result.scalars().all()

    async def get_all_orders(self, session: AsyncSession) -> List[Order]:
        '''Получение заказов'''
        result = await session.execute(
            select(Order)
            .options(selectinload(Order.order_items))
        )
        return result.scalars().all()


crud_order = CRUDOrder(Order)
