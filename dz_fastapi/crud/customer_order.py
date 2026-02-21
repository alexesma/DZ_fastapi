import logging
from datetime import date, datetime
from typing import List, Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload

from dz_fastapi.models.autopart import AutoPart
from dz_fastapi.models.brand import Brand
from dz_fastapi.models.partner import (CustomerOrder, CustomerOrderConfig,
                                       StockOrder, StockOrderItem,
                                       SupplierOrder)

logger = logging.getLogger('dz_fastapi')


class CRUDCustomerOrderConfig:
    async def get_by_customer_id(
        self, session: AsyncSession, customer_id: int
    ) -> Optional[CustomerOrderConfig]:
        result = await session.execute(
            select(CustomerOrderConfig).where(
                CustomerOrderConfig.customer_id == customer_id
            )
        )
        return result.scalars().first()

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
            for key, value in data.items():
                setattr(config, key, value)
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
        stmt = select(CustomerOrder).options(joinedload(CustomerOrder.items))
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
        return result.scalars().all()


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
                joinedload(
                    StockOrder.items
                ).joinedload(StockOrderItem.autopart)
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
    async def list_supplier_orders(
        self,
        session: AsyncSession,
        provider_id: Optional[int] = None,
        status: Optional[str] = None,
        skip: int = 0,
        limit: int = 100,
    ) -> List[SupplierOrder]:
        stmt = select(SupplierOrder).options(joinedload(SupplierOrder.items))
        if provider_id is not None:
            stmt = stmt.where(SupplierOrder.provider_id == provider_id)
        if status is not None:
            stmt = stmt.where(SupplierOrder.status == status)
        stmt = stmt.order_by(
            SupplierOrder.created_at.desc()
        ).offset(skip).limit(limit)
        result = await session.execute(stmt)
        return result.scalars().all()


crud_customer_order_config = CRUDCustomerOrderConfig()
crud_customer_order = CRUDCustomerOrder()
crud_stock_order = CRUDStockOrder()
crud_supplier_order = CRUDSupplierOrder()
