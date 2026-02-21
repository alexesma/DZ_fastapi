import logging
from datetime import date
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.api.deps import get_current_user
from dz_fastapi.core.db import get_session
from dz_fastapi.crud.customer_order import (crud_customer_order,
                                            crud_customer_order_config,
                                            crud_stock_order,
                                            crud_supplier_order)
from dz_fastapi.models.user import User
from dz_fastapi.schemas.customer_order import (CustomerOrderConfigCreate,
                                               CustomerOrderConfigResponse,
                                               CustomerOrderConfigUpdate,
                                               CustomerOrderResponse,
                                               StockOrderResponse,
                                               SupplierOrderResponse)
from dz_fastapi.services.customer_orders import (
    process_customer_orders, send_scheduled_supplier_orders,
    send_supplier_orders)

logger = logging.getLogger('dz_fastapi')

router = APIRouter(prefix='/customer-orders', tags=['customer-orders'])


@router.post(
    '/config',
    response_model=CustomerOrderConfigResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_order_config(
    payload: CustomerOrderConfigCreate,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    config = await crud_customer_order_config.upsert(
        session=session,
        customer_id=payload.customer_id,
        data=payload.model_dump(exclude={'customer_id'}),
    )
    return CustomerOrderConfigResponse.model_validate(config)


@router.get(
    '/config/{customer_id}',
    response_model=CustomerOrderConfigResponse,
    status_code=status.HTTP_200_OK,
)
async def get_order_config(
    customer_id: int,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    config = await crud_customer_order_config.get_by_customer_id(
        session=session, customer_id=customer_id
    )
    if not config:
        raise HTTPException(status_code=404, detail='Config not found')
    return CustomerOrderConfigResponse.model_validate(config)


@router.put(
    '/config/{customer_id}',
    response_model=CustomerOrderConfigResponse,
    status_code=status.HTTP_200_OK,
)
async def update_order_config(
    customer_id: int,
    payload: CustomerOrderConfigUpdate,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    config = await crud_customer_order_config.upsert(
        session=session,
        customer_id=customer_id,
        data=payload.model_dump(exclude_unset=True),
    )
    return CustomerOrderConfigResponse.model_validate(config)


@router.get(
    '/',
    response_model=List[CustomerOrderResponse],
    status_code=status.HTTP_200_OK,
)
async def list_customer_orders(
    customer_id: Optional[int] = None,
    status: Optional[str] = None,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
    skip: int = 0,
    limit: int = 100,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    orders = await crud_customer_order.list_orders(
        session=session,
        customer_id=customer_id,
        status=status,
        date_from=date_from,
        date_to=date_to,
        skip=skip,
        limit=limit,
    )
    return [CustomerOrderResponse.model_validate(o) for o in orders]


@router.get(
    '/{order_id}',
    response_model=CustomerOrderResponse,
    status_code=status.HTTP_200_OK,
)
async def get_customer_order(
    order_id: int,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    order = await crud_customer_order.get_by_id(
        session=session, order_id=order_id
    )
    if not order:
        raise HTTPException(status_code=404, detail='Order not found')
    return CustomerOrderResponse.model_validate(order)


@router.post(
    '/process',
    status_code=status.HTTP_200_OK,
)
async def process_orders(
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    await process_customer_orders(session)
    return {'status': 'ok'}


@router.get(
    '/stock/list',
    response_model=List[StockOrderResponse],
    status_code=status.HTTP_200_OK,
)
async def list_stock_orders(
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
    brand_id: Optional[int] = None,
    customer_id: Optional[int] = None,
    storage_location_id: Optional[int] = None,
    skip: int = 0,
    limit: int = 100,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    orders = await crud_stock_order.list_stock_orders(
        session=session,
        date_from=date_from,
        date_to=date_to,
        brand_id=brand_id,
        customer_id=customer_id,
        storage_location_id=storage_location_id,
        skip=skip,
        limit=limit,
    )
    return [StockOrderResponse.model_validate(o) for o in orders]


@router.get(
    '/supplier/list',
    response_model=List[SupplierOrderResponse],
    status_code=status.HTTP_200_OK,
)
async def list_supplier_orders(
    provider_id: Optional[int] = None,
    status: Optional[str] = None,
    skip: int = 0,
    limit: int = 100,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    orders = await crud_supplier_order.list_supplier_orders(
        session=session,
        provider_id=provider_id,
        status=status,
        skip=skip,
        limit=limit,
    )
    return [SupplierOrderResponse.model_validate(o) for o in orders]


@router.post(
    '/supplier/send',
    status_code=status.HTTP_200_OK,
)
async def send_supplier_orders_endpoint(
    supplier_order_ids: List[int],
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    result = await send_supplier_orders(
        session=session, supplier_order_ids=supplier_order_ids
    )
    return result


@router.post(
    '/supplier/send-scheduled',
    status_code=status.HTTP_200_OK,
)
async def send_scheduled_supplier_orders_endpoint(
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    result = await send_scheduled_supplier_orders(session)
    return result
