import logging
from datetime import date, datetime
from typing import List, Optional

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload, selectinload

from dz_fastapi.models.autopart import AutoPart
from dz_fastapi.models.brand import Brand
from dz_fastapi.models.partner import (Customer, CustomerOrder,
                                       CustomerOrderConfig, CustomerOrderItem,
                                       StockOrder, StockOrderItem,
                                       SupplierOrder, SupplierOrderItem,
                                       SupplierReceiptItem)

logger = logging.getLogger('dz_fastapi')


class CRUDCustomerOrderConfig:
    async def get_by_id(
        self, session: AsyncSession, config_id: int
    ) -> Optional[CustomerOrderConfig]:
        result = await session.execute(
            select(CustomerOrderConfig).where(
                CustomerOrderConfig.id == config_id
            )
        )
        return result.scalars().first()

    async def get_by_customer_and_pricelist(
        self,
        session: AsyncSession,
        customer_id: int,
        pricelist_config_id: int,
    ) -> Optional[CustomerOrderConfig]:
        result = await session.execute(
            select(CustomerOrderConfig).where(
                CustomerOrderConfig.customer_id == customer_id,
                CustomerOrderConfig.pricelist_config_id
                == pricelist_config_id,
            )
        )
        return result.scalars().first()

    async def get_by_customer_id(
        self, session: AsyncSession, customer_id: int
    ) -> Optional[CustomerOrderConfig]:
        result = await session.execute(
            select(CustomerOrderConfig)
            .where(CustomerOrderConfig.customer_id == customer_id)
            .order_by(CustomerOrderConfig.id.desc())
        )
        return result.scalars().first()

    async def list_by_customer_id(
        self, session: AsyncSession, customer_id: int
    ) -> List[CustomerOrderConfig]:
        result = await session.execute(
            select(CustomerOrderConfig).where(
                CustomerOrderConfig.customer_id == customer_id
            )
        )
        return result.scalars().all()

    async def create(
        self,
        session: AsyncSession,
        customer_id: int,
        data: dict,
    ) -> CustomerOrderConfig:
        config = CustomerOrderConfig(customer_id=customer_id, **data)
        session.add(config)
        await session.commit()
        await session.refresh(config)
        return config

    async def update(
        self,
        session: AsyncSession,
        config: CustomerOrderConfig,
        data: dict,
    ) -> CustomerOrderConfig:
        should_reset_last_uid = (
            'email_account_id' in data
            and data.get('email_account_id') != config.email_account_id
        )
        for key, value in data.items():
            setattr(config, key, value)
        if should_reset_last_uid:
            config.last_uid = 0
        await session.commit()
        await session.refresh(config)
        return config

    async def delete(
        self, session: AsyncSession, config: CustomerOrderConfig
    ) -> None:
        await session.delete(config)
        await session.commit()

    async def upsert(
        self,
        session: AsyncSession,
        customer_id: int,
        data: dict,
    ) -> CustomerOrderConfig:
        config = await self.get_by_customer_id(
            session=session, customer_id=customer_id
        )
        if config is None:
            config = CustomerOrderConfig(customer_id=customer_id, **data)
            session.add(config)
        else:
            should_reset_last_uid = (
                'email_account_id' in data
                and data.get('email_account_id') != config.email_account_id
            )
            for key, value in data.items():
                setattr(config, key, value)
            if should_reset_last_uid:
                config.last_uid = 0
        await session.commit()
        await session.refresh(config)
        return config


class CRUDCustomerOrder:
    async def get_by_id(
        self, session: AsyncSession, order_id: int
    ) -> Optional[CustomerOrder]:
        result = await session.execute(
            select(CustomerOrder)
            .options(joinedload(CustomerOrder.items))
            .where(CustomerOrder.id == order_id)
        )
        return result.scalars().first()

    async def list_orders(
        self,
        session: AsyncSession,
        customer_id: Optional[int] = None,
        status: Optional[str] = None,
        date_from: Optional[date] = None,
        date_to: Optional[date] = None,
        skip: int = 0,
        limit: int = 100,
    ) -> List[CustomerOrder]:
        stmt = select(CustomerOrder).options(
            joinedload(CustomerOrder.items),
            joinedload(CustomerOrder.customer),
        )
        if customer_id is not None:
            stmt = stmt.where(CustomerOrder.customer_id == customer_id)
        if status is not None:
            stmt = stmt.where(CustomerOrder.status == status)
        if date_from is not None:
            stmt = stmt.where(CustomerOrder.received_at >= datetime.combine(
                date_from, datetime.min.time()
            ))
        if date_to is not None:
            stmt = stmt.where(CustomerOrder.received_at <= datetime.combine(
                date_to, datetime.max.time()
            ))
        stmt = stmt.order_by(
            CustomerOrder.received_at.desc()
        ).offset(skip).limit(limit)
        result = await session.execute(stmt)
        return result.unique().scalars().all()

    async def get_stats_rows(
        self,
        session: AsyncSession,
        *,
        kind: str,
        value: str,
        date_from: datetime,
    ):
        normalized = str(value or '').strip().lower()
        if not normalized:
            return []

        stmt = (
            select(
                CustomerOrderItem.id.label('item_id'),
                CustomerOrder.id.label('order_id'),
                CustomerOrder.customer_id.label('customer_id'),
                Customer.name.label('customer_name'),
                CustomerOrder.order_number.label('order_number'),
                CustomerOrder.received_at.label('received_at'),
                CustomerOrderItem.requested_qty.label('requested_qty'),
                CustomerOrderItem.requested_price.label('requested_price'),
                CustomerOrderItem.ship_qty.label('ship_qty'),
                CustomerOrderItem.reject_qty.label('reject_qty'),
                CustomerOrderItem.status.label('status'),
            )
            .join(
                CustomerOrder,
                CustomerOrderItem.order_id == CustomerOrder.id,
            )
            .outerjoin(Customer, Customer.id == CustomerOrder.customer_id)
            .where(CustomerOrder.received_at >= date_from)
            .order_by(
                CustomerOrder.received_at.desc(),
                CustomerOrder.id.desc(),
                CustomerOrderItem.id.desc(),
            )
        )

        if kind == 'brand':
            stmt = stmt.where(
                func.lower(CustomerOrderItem.brand) == normalized
            )
        else:
            stmt = stmt.where(func.lower(CustomerOrderItem.oem) == normalized)

        result = await session.execute(stmt)
        return result.all()


class CRUDStockOrder:
    async def list_stock_orders(
        self,
        session: AsyncSession,
        date_from: Optional[date] = None,
        date_to: Optional[date] = None,
        brand_id: Optional[int] = None,
        customer_id: Optional[int] = None,
        storage_location_id: Optional[int] = None,
        skip: int = 0,
        limit: int = 100,
    ) -> List[StockOrder]:
        stmt = (
            select(StockOrder)
            .options(
                joinedload(StockOrder.customer),
                joinedload(
                    StockOrder.items
                )
                .joinedload(StockOrderItem.autopart)
                .selectinload(AutoPart.storage_locations),
                joinedload(StockOrder.items).joinedload(
                    StockOrderItem.picked_by_user
                ),
            )
            .order_by(StockOrder.created_at.desc())
        )
        if customer_id is not None:
            stmt = stmt.where(StockOrder.customer_id == customer_id)
        if date_from is not None:
            stmt = stmt.where(StockOrder.created_at >= datetime.combine(
                date_from, datetime.min.time()
            ))
        if date_to is not None:
            stmt = stmt.where(StockOrder.created_at <= datetime.combine(
                date_to, datetime.max.time()
            ))
        if brand_id is not None or storage_location_id is not None:
            stmt = stmt.join(StockOrderItem).join(AutoPart)
            if brand_id is not None:
                stmt = stmt.join(Brand).where(Brand.id == brand_id)
            if storage_location_id is not None:
                stmt = stmt.where(
                    AutoPart.storage_locations.any(id=storage_location_id)
                )
        stmt = stmt.offset(skip).limit(limit)
        result = await session.execute(stmt)
        return result.scalars().unique().all()


class CRUDSupplierOrder:
    async def get_by_id(
        self,
        session: AsyncSession,
        order_id: int,
    ) -> Optional[SupplierOrder]:
        stmt = (
            select(SupplierOrder)
            .options(
                selectinload(SupplierOrder.items)
                .selectinload(SupplierOrderItem.customer_order_item),
                selectinload(SupplierOrder.items)
                .selectinload(SupplierOrderItem.autopart)
                .selectinload(AutoPart.brand),
                selectinload(SupplierOrder.provider),
            )
            .where(SupplierOrder.id == order_id)
        )
        result = await session.execute(stmt)
        return result.scalars().first()

    async def list_supplier_orders(
        self,
        session: AsyncSession,
        provider_id: Optional[int] = None,
        status: Optional[str] = None,
        date_from: Optional[date] = None,
        date_to: Optional[date] = None,
        use_sent_at_for_period: bool = False,
        skip: int = 0,
        limit: int = 100,
    ) -> List[SupplierOrder]:
        period_column = (
            func.coalesce(SupplierOrder.sent_at, SupplierOrder.created_at)
            if use_sent_at_for_period
            else SupplierOrder.created_at
        )
        stmt = select(SupplierOrder).options(
            selectinload(SupplierOrder.provider),
            selectinload(SupplierOrder.items)
            .selectinload(SupplierOrderItem.customer_order_item)
            .selectinload(CustomerOrderItem.order)
            .selectinload(CustomerOrder.customer),
            selectinload(SupplierOrder.items)
            .selectinload(SupplierOrderItem.customer_order_item)
            .selectinload(CustomerOrderItem.order)
            .selectinload(CustomerOrder.items),
            selectinload(SupplierOrder.items)
            .selectinload(SupplierOrderItem.autopart)
            .selectinload(AutoPart.brand),
            selectinload(SupplierOrder.items)
            .selectinload(SupplierOrderItem.receipt_items)
            .selectinload(SupplierReceiptItem.receipt),
        )
        if provider_id is not None:
            stmt = stmt.where(SupplierOrder.provider_id == provider_id)
        if status is not None:
            stmt = stmt.where(SupplierOrder.status == status)
        if date_from is not None:
            stmt = stmt.where(
                period_column
                >= datetime.combine(date_from, datetime.min.time())
            )
        if date_to is not None:
            stmt = stmt.where(
                period_column
                <= datetime.combine(date_to, datetime.max.time())
            )
        if use_sent_at_for_period:
            stmt = stmt.order_by(
                period_column.desc(),
                SupplierOrder.id.desc(),
            )
        else:
            stmt = stmt.order_by(SupplierOrder.created_at.desc())
        stmt = stmt.offset(skip).limit(limit)
        result = await session.execute(stmt)
        return result.scalars().unique().all()


crud_customer_order_config = CRUDCustomerOrderConfig()
crud_customer_order = CRUDCustomerOrder()
crud_stock_order = CRUDStockOrder()
crud_supplier_order = CRUDSupplierOrder()
