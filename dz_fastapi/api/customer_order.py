import logging
from datetime import date
from decimal import Decimal
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.api.deps import get_current_user
from dz_fastapi.core.db import get_session
from dz_fastapi.crud.customer_order import (crud_customer_order,
                                            crud_customer_order_config,
                                            crud_stock_order,
                                            crud_supplier_order)
from dz_fastapi.models.partner import CUSTOMER_ORDER_ITEM_STATUS
from dz_fastapi.models.user import User
from dz_fastapi.schemas.customer_order import (CustomerOrderConfigCreate,
                                               CustomerOrderConfigResponse,
                                               CustomerOrderConfigUpdate,
                                               CustomerOrderResponse,
                                               StockOrderResponse,
                                               SupplierOrderSummaryResponse)
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
    response_model=List[SupplierOrderSummaryResponse],
    status_code=status.HTTP_200_OK,
)
async def list_supplier_orders(
    provider_id: Optional[int] = None,
    status: Optional[str] = None,
    customer_id: Optional[int] = None,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
    rejected_pct_min: Optional[float] = None,
    rejected_pct_max: Optional[float] = None,
    total_sum_min: Optional[float] = None,
    total_sum_max: Optional[float] = None,
    stock_sum_min: Optional[float] = None,
    stock_sum_max: Optional[float] = None,
    supplier_sum_min: Optional[float] = None,
    supplier_sum_max: Optional[float] = None,
    rejected_sum_min: Optional[float] = None,
    rejected_sum_max: Optional[float] = None,
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
    results: List[SupplierOrderSummaryResponse] = []

    def _money(value) -> Decimal:
        if value is None:
            return Decimal('0')
        return Decimal(str(value))

    def _in_range(value: float, min_value, max_value) -> bool:
        if min_value is not None and value < min_value:
            return False
        if max_value is not None and value > max_value:
            return False
        return True

    for order in orders:
        customer_order = None
        if order.items:
            first_item = order.items[0]
            if first_item.customer_order_item:
                customer_order = first_item.customer_order_item.order

        total_sum = Decimal('0')
        stock_sum = Decimal('0')
        supplier_sum = Decimal('0')
        rejected_sum = Decimal('0')

        if customer_order:
            for item in customer_order.items or []:
                ship_qty = int(item.ship_qty or 0)
                reject_qty = int(item.reject_qty or 0)

                requested_price = _money(item.requested_price)
                matched_price = _money(item.matched_price)
                base_price = (
                    requested_price
                    if requested_price > 0
                    else matched_price
                )
                ship_price = (
                    matched_price
                    if matched_price > 0
                    else requested_price
                )

                if ship_qty > 0 and ship_price > 0:
                    ship_sum = Decimal(ship_qty) * ship_price
                    if item.status == CUSTOMER_ORDER_ITEM_STATUS.OWN_STOCK:
                        stock_sum += ship_sum
                    elif item.status == CUSTOMER_ORDER_ITEM_STATUS.SUPPLIER:
                        supplier_sum += ship_sum
                    else:
                        supplier_sum += ship_sum

                if reject_qty > 0 and base_price > 0:
                    rejected_sum += Decimal(reject_qty) * base_price

        total_sum = stock_sum + supplier_sum
        reject_denominator = total_sum + rejected_sum
        rejected_pct = (
            float((rejected_sum / reject_denominator) * 100)
            if reject_denominator > 0
            else 0.0
        )

        if customer_id is not None:
            if not customer_order or customer_order.customer_id != customer_id:
                continue

        if date_from or date_to:
            if not customer_order or not customer_order.received_at:
                continue
            received_date = customer_order.received_at.date()
            if date_from and received_date < date_from:
                continue
            if date_to and received_date > date_to:
                continue

        if not _in_range(rejected_pct, rejected_pct_min, rejected_pct_max):
            continue
        if not _in_range(float(total_sum), total_sum_min, total_sum_max):
            continue
        if not _in_range(float(stock_sum), stock_sum_min, stock_sum_max):
            continue
        if not _in_range(
            float(supplier_sum), supplier_sum_min, supplier_sum_max
        ):
            continue
        if not _in_range(
            float(rejected_sum), rejected_sum_min, rejected_sum_max
        ):
            continue

        results.append(
            SupplierOrderSummaryResponse(
                id=order.id,
                provider_id=order.provider_id,
                status=order.status,
                created_at=order.created_at,
                customer_order_id=(
                    customer_order.id if customer_order else None
                ),
                customer_name=(
                    customer_order.customer.name
                    if customer_order and customer_order.customer
                    else None
                ),
                customer_order_number=(
                    customer_order.order_number if customer_order else None
                ),
                customer_received_at=(
                    customer_order.received_at if customer_order else None
                ),
                customer_status=(
                    customer_order.status if customer_order else None
                ),
                total_sum=float(total_sum),
                stock_sum=float(stock_sum),
                supplier_sum=float(supplier_sum),
                rejected_sum=float(rejected_sum),
                rejected_pct=rejected_pct,
            )
        )

    results.sort(
        key=lambda item: item.customer_received_at or item.created_at,
        reverse=True,
    )
    return results


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
