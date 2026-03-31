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
from dz_fastapi.crud.partner import crud_customer_pricelist_config
from dz_fastapi.models.notification import AppNotificationLevel
from dz_fastapi.models.partner import CUSTOMER_ORDER_ITEM_STATUS
from dz_fastapi.models.user import User, UserRole
from dz_fastapi.schemas.customer_order import (CustomerOrderConfigCreate,
                                               CustomerOrderConfigResponse,
                                               CustomerOrderConfigUpdate,
                                               CustomerOrderItemResponse,
                                               CustomerOrderItemUpdate,
                                               CustomerOrderManualCreate,
                                               CustomerOrderResponse,
                                               CustomerOrderSummaryResponse,
                                               StockOrderResponse,
                                               SupplierOrderDetailResponse,
                                               SupplierOrderManualCreate,
                                               SupplierOrderSummaryResponse)
from dz_fastapi.services.customer_orders import (
    create_manual_customer_order, create_manual_supplier_order,
    process_customer_orders, process_manual_customer_order,
    retry_customer_order, retry_customer_order_errors_for_config,
    send_scheduled_supplier_orders, send_supplier_orders,
    update_customer_order_item_manual)
from dz_fastapi.services.notifications import create_notification

logger = logging.getLogger('dz_fastapi')

router = APIRouter(prefix='/customer-orders', tags=['customer-orders'])


async def _notify_current_user(
    session: AsyncSession,
    current_user: User,
    *,
    title: str,
    message: str,
    level: str = AppNotificationLevel.INFO,
    link: str | None = None,
) -> None:
    try:
        await create_notification(
            session=session,
            user_id=current_user.id,
            title=title,
            message=message,
            level=level,
            link=link,
        )
    except Exception:
        await session.rollback()
        logger.exception(
            'Failed to create app notification for user %s',
            current_user.id,
        )


def _serialize_customer_order_item_for_user(
    item,
    current_user: User,
) -> CustomerOrderItemResponse:
    model = CustomerOrderItemResponse.model_validate(item)
    if current_user.role != UserRole.ADMIN:
        return model.model_copy(
            update={
                'reject_reason_code': None,
                'reject_reason_text': None,
            }
        )
    return model


def _serialize_customer_order_for_user(
    order,
    current_user: User,
) -> CustomerOrderResponse:
    model = CustomerOrderResponse.model_validate(order)
    items = [
        _serialize_customer_order_item_for_user(item, current_user)
        for item in (order.items or [])
    ]
    return model.model_copy(update={'items': items})


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


@router.get(
    '/configs',
    response_model=List[CustomerOrderConfigResponse],
    status_code=status.HTTP_200_OK,
)
async def list_order_configs(
    customer_id: int,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    configs = await crud_customer_order_config.list_by_customer_id(
        session=session, customer_id=customer_id
    )
    pricelist_configs = (
        await crud_customer_pricelist_config.get_by_customer_id(
            session=session,
            customer_id=customer_id,
        )
    )
    pricelist_map = {
        cfg.id: cfg.name for cfg in pricelist_configs
    }
    response = []
    for config in configs:
        model = CustomerOrderConfigResponse.model_validate(config)
        response.append(
            model.model_copy(
                update={
                    'pricelist_config_name': pricelist_map.get(
                        config.pricelist_config_id
                    )
                }
            )
        )
    return response


@router.get(
    '/configs/{config_id}',
    response_model=CustomerOrderConfigResponse,
    status_code=status.HTTP_200_OK,
)
async def get_order_config_by_id(
    config_id: int,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    config = await crud_customer_order_config.get_by_id(
        session=session, config_id=config_id
    )
    if not config:
        raise HTTPException(status_code=404, detail='Config not found')
    model = CustomerOrderConfigResponse.model_validate(config)
    return model


@router.post(
    '/configs',
    response_model=CustomerOrderConfigResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_order_config_v2(
    payload: CustomerOrderConfigCreate,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    if payload.pricelist_config_id is None:
        raise HTTPException(
            status_code=400,
            detail='pricelist_config_id is required',
        )
    existing = await crud_customer_order_config.get_by_customer_and_pricelist(
        session=session,
        customer_id=payload.customer_id,
        pricelist_config_id=payload.pricelist_config_id,
    )
    if existing:
        raise HTTPException(
            status_code=409,
            detail='Order config already exists for this pricelist',
        )
    config = await crud_customer_order_config.create(
        session=session,
        customer_id=payload.customer_id,
        data=payload.model_dump(exclude={'customer_id'}),
    )
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
    config = await crud_customer_order_config.get_by_customer_id(
        session=session, customer_id=customer_id
    )
    if not config:
        raise HTTPException(status_code=404, detail='Config not found')
    config = await crud_customer_order_config.update(
        session=session,
        config=config,
        data=payload.model_dump(exclude_unset=True),
    )
    return CustomerOrderConfigResponse.model_validate(config)


@router.patch(
    '/configs/{config_id}',
    response_model=CustomerOrderConfigResponse,
    status_code=status.HTTP_200_OK,
)
async def update_order_config_by_id(
    config_id: int,
    payload: CustomerOrderConfigUpdate,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    config = await crud_customer_order_config.get_by_id(
        session=session, config_id=config_id
    )
    if not config:
        raise HTTPException(status_code=404, detail='Config not found')
    if payload.pricelist_config_id is not None:
        existing = (
            await crud_customer_order_config.get_by_customer_and_pricelist(
                session=session,
                customer_id=config.customer_id,
                pricelist_config_id=payload.pricelist_config_id,
            )
        )
        if existing and existing.id != config.id:
            raise HTTPException(
                status_code=409,
                detail='Order config already exists for this pricelist',
            )
    config = await crud_customer_order_config.update(
        session=session,
        config=config,
        data=payload.model_dump(exclude_unset=True),
    )
    return CustomerOrderConfigResponse.model_validate(config)


@router.delete(
    '/configs/{config_id}',
    status_code=status.HTTP_204_NO_CONTENT,
)
async def delete_order_config(
    config_id: int,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    config = await crud_customer_order_config.get_by_id(
        session=session, config_id=config_id
    )
    if not config:
        raise HTTPException(status_code=404, detail='Config not found')
    await crud_customer_order_config.delete(session=session, config=config)
    return None


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
    return [
        _serialize_customer_order_for_user(order, current_user)
        for order in orders
    ]


@router.get(
    '/summary',
    response_model=List[CustomerOrderSummaryResponse],
    status_code=status.HTTP_200_OK,
)
async def list_customer_order_summary(
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
    results: List[CustomerOrderSummaryResponse] = []

    def _money(value) -> Decimal:
        if value is None:
            return Decimal('0')
        return Decimal(str(value))

    for order in orders:
        total_sum = Decimal('0')
        stock_sum = Decimal('0')
        supplier_sum = Decimal('0')
        rejected_sum = Decimal('0')
        for item in order.items or []:
            price = (
                item.requested_price
                if item.requested_price is not None
                else item.matched_price
            )
            price_value = _money(price)
            ship_qty = item.ship_qty or item.requested_qty or 0
            reject_qty = item.reject_qty or 0
            if item.status == CUSTOMER_ORDER_ITEM_STATUS.REJECTED:
                if reject_qty == 0:
                    reject_qty = item.requested_qty or 0
                rejected_sum += Decimal(reject_qty) * price_value
            elif item.status == CUSTOMER_ORDER_ITEM_STATUS.OWN_STOCK:
                stock_sum += Decimal(ship_qty) * price_value
            elif item.status == CUSTOMER_ORDER_ITEM_STATUS.SUPPLIER:
                supplier_sum += Decimal(ship_qty) * price_value
        total_sum = stock_sum + supplier_sum + rejected_sum
        rejected_pct = float(
            (rejected_sum / total_sum) * 100
        ) if total_sum > 0 else 0.0
        results.append(
            CustomerOrderSummaryResponse(
                id=order.id,
                customer_id=order.customer_id,
                customer_name=(
                    order.customer.name if order.customer else None
                ),
                order_number=order.order_number,
                received_at=order.received_at,
                status=order.status,
                total_sum=float(total_sum),
                stock_sum=float(stock_sum),
                supplier_sum=float(supplier_sum),
                rejected_sum=float(rejected_sum),
                rejected_pct=rejected_pct,
            )
        )
    return results


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
    return _serialize_customer_order_for_user(order, current_user)


@router.post(
    '/manual',
    response_model=CustomerOrderResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_manual_order(
    payload: CustomerOrderManualCreate,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    try:
        order = await create_manual_customer_order(
            session=session,
            customer_id=payload.customer_id,
            order_number=payload.order_number,
            order_date=payload.order_date,
            items=[item.model_dump() for item in payload.items],
            auto_process=payload.auto_process,
            order_config_id=payload.order_config_id,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    order = await crud_customer_order.get_by_id(
        session=session,
        order_id=order.id
    )
    if not order:
        raise HTTPException(status_code=404, detail='Order not found')
    return _serialize_customer_order_for_user(order, current_user)


@router.post(
    '/{order_id}/process-manual',
    response_model=CustomerOrderResponse,
    status_code=status.HTTP_200_OK,
)
async def process_manual_order_endpoint(
    order_id: int,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    try:
        order = await process_manual_customer_order(
            session=session, order_id=order_id
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    order = await crud_customer_order.get_by_id(
        session=session,
        order_id=order.id
    )
    if not order:
        raise HTTPException(status_code=404, detail='Order not found')
    return _serialize_customer_order_for_user(order, current_user)


@router.patch(
    '/items/{item_id}',
    response_model=CustomerOrderItemResponse,
    status_code=status.HTTP_200_OK,
)
async def update_customer_order_item(
    item_id: int,
    payload: CustomerOrderItemUpdate,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    try:
        item = await update_customer_order_item_manual(
            session=session,
            item_id=item_id,
            status=payload.status,
            supplier_id=payload.supplier_id,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return _serialize_customer_order_item_for_user(item, current_user)


@router.post(
    '/process',
    status_code=status.HTTP_200_OK,
)
async def process_orders(
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    await process_customer_orders(session)
    await _notify_current_user(
        session,
        current_user,
        title='Проверка почты завершена',
        message=(
            'Импорт заказов клиентов завершен для всех активных'
            ' конфигураций.'
        ),
        level=AppNotificationLevel.SUCCESS,
        link='/customer-orders',
    )
    return {'status': 'ok'}


@router.post(
    '/configs/{config_id}/process',
    status_code=status.HTTP_200_OK,
)
async def process_orders_for_config(
    config_id: int,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    config = await crud_customer_order_config.get_by_id(
        session=session, config_id=config_id
    )
    if not config:
        raise HTTPException(status_code=404, detail='Config not found')
    await process_customer_orders(
        session,
        customer_id=config.customer_id,
        config_id=config_id,
    )
    await _notify_current_user(
        session,
        current_user,
        title='Проверка почты завершена',
        message=(
            f'Импорт заказов завершен для конфигурации #{config_id}'
            f' клиента #{config.customer_id}.'
        ),
        level=AppNotificationLevel.SUCCESS,
        link='/customer-orders',
    )
    return {'status': 'ok', 'config_id': config_id}


@router.post(
    '/configs/{config_id}/retry-errors',
    status_code=status.HTTP_200_OK,
)
async def retry_config_errors(
    config_id: int,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    try:
        result = await retry_customer_order_errors_for_config(
            session=session,
            config_id=config_id,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    retried_count = result.get('retried', 0)
    await _notify_current_user(
        session,
        current_user,
        title='Повторная обработка ошибок завершена',
        message=(
            f'Для конфигурации #{config_id} повторно обработано'
            f' {retried_count} заказов с ошибками.'
        ),
        level=AppNotificationLevel.SUCCESS,
        link='/customer-orders',
    )
    return result


@router.post(
    '/{order_id}/retry',
    response_model=CustomerOrderResponse,
    status_code=status.HTTP_200_OK,
)
async def retry_order(
    order_id: int,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    try:
        order = await retry_customer_order(session=session, order_id=order_id)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    order = await crud_customer_order.get_by_id(
        session=session,
        order_id=order.id,
    )
    if not order:
        raise HTTPException(status_code=404, detail='Order not found')
    await _notify_current_user(
        session,
        current_user,
        title='Заказ перепроверен',
        message=f'Заказ клиента #{order.id} был перепроверен.',
        level=AppNotificationLevel.SUCCESS,
        link=f'/customer-orders/{order.id}',
    )
    return _serialize_customer_order_for_user(order, current_user)


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
        customer_orders = {}
        for item in order.items or []:
            order_item = item.customer_order_item
            if order_item and order_item.order:
                customer_orders[order_item.order.id] = order_item.order

        if customer_id is not None:
            if not any(
                o.customer_id == customer_id
                for o in customer_orders.values()
            ):
                continue

        if date_from or date_to:
            matches_date = False
            for o in customer_orders.values():
                if not o.received_at:
                    continue
                received_date = o.received_at.date()
                if date_from and received_date < date_from:
                    continue
                if date_to and received_date > date_to:
                    continue
                matches_date = True
                break
            if not matches_date:
                continue

        supplier_sum = Decimal('0')
        for item in order.items or []:
            order_item = item.customer_order_item
            if item.price is not None:
                price_value = _money(item.price)
            else:
                price_value = _money(
                    order_item.requested_price
                    if order_item and order_item.requested_price is not None
                    else (
                        order_item.matched_price
                        if order_item
                        else None
                    )
                )
            supplier_sum += Decimal(item.quantity) * price_value

        rejected_sum = Decimal('0')
        for customer_order in customer_orders.values():
            for item in customer_order.items or []:
                if item.status != CUSTOMER_ORDER_ITEM_STATUS.REJECTED:
                    continue
                price = (
                    item.requested_price
                    if item.requested_price is not None
                    else item.matched_price
                )
                price_value = _money(price)
                reject_qty = int(item.reject_qty or 0)
                if reject_qty == 0:
                    reject_qty = int(item.requested_qty or 0)
                rejected_sum += Decimal(reject_qty) * price_value

        total_sum = supplier_sum
        rejected_pct = (
            float((rejected_sum / (total_sum + rejected_sum)) * 100)
            if (total_sum + rejected_sum) > 0
            else 0.0
        )

        customer_order = None
        customer_name = None
        customer_order_number = None
        customer_received_at = None
        customer_status = None
        customer_orders_count = len(customer_orders)
        if len(customer_orders) == 1:
            customer_order = next(iter(customer_orders.values()))
            customer_name = (
                customer_order.customer.name
                if customer_order.customer
                else None
            )
            customer_order_number = customer_order.order_number
            customer_received_at = customer_order.received_at
            customer_status = customer_order.status
        elif len(customer_orders) > 1:
            customer_name = 'Несколько'
            customer_order_number = 'Несколько'
            received_list = [
                o.received_at for o in customer_orders.values()
                if o.received_at
            ]
            if received_list:
                customer_received_at = min(received_list)

        if not _in_range(rejected_pct, rejected_pct_min, rejected_pct_max):
            continue
        if not _in_range(float(total_sum), total_sum_min, total_sum_max):
            continue
        if not _in_range(0.0, stock_sum_min, stock_sum_max):
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
                customer_name=customer_name,
                customer_order_number=customer_order_number,
                customer_received_at=customer_received_at,
                customer_status=customer_status,
                customer_orders_count=customer_orders_count,
                total_sum=float(total_sum),
                stock_sum=0.0,
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


@router.get(
    '/supplier/{order_id}',
    response_model=SupplierOrderDetailResponse,
    status_code=status.HTTP_200_OK,
)
async def get_supplier_order_detail(
    order_id: int,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    order = await crud_supplier_order.get_by_id(
        session=session, order_id=order_id
    )
    if not order:
        raise HTTPException(status_code=404, detail='Order not found')
    items = []
    for item in order.items or []:
        order_item = item.customer_order_item
        autopart = item.autopart
        brand_name = (
            autopart.brand.name
            if autopart and autopart.brand
            else None
        )
        items.append(
            {
                'id': item.id,
                'customer_order_item_id': item.customer_order_item_id,
                'quantity': item.quantity,
                'price': item.price,
                'oem': (
                    order_item.oem
                    if order_item
                    else (autopart.oem_number if autopart else None)
                ),
                'brand': (
                    order_item.brand
                    if order_item
                    else brand_name
                ),
                'name': (
                    order_item.name
                    if order_item
                    else (autopart.name if autopart else None)
                ),
                'requested_qty': (
                    order_item.requested_qty if order_item else None
                ),
                'ship_qty': order_item.ship_qty if order_item else None,
                'reject_qty': order_item.reject_qty if order_item else None,
            }
        )
    return SupplierOrderDetailResponse(
        id=order.id,
        provider_id=order.provider_id,
        provider_name=order.provider.name if order.provider else None,
        status=order.status,
        created_at=order.created_at,
        scheduled_at=order.scheduled_at,
        sent_at=order.sent_at,
        items=items,
    )


@router.post(
    '/supplier/manual',
    response_model=SupplierOrderDetailResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_manual_supplier_order_endpoint(
    payload: SupplierOrderManualCreate,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    try:
        created = await create_manual_supplier_order(
            session=session,
            provider_id=payload.provider_id,
            items=[item.model_dump() for item in payload.items],
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    order = await crud_supplier_order.get_by_id(
        session=session, order_id=created.id
    )
    if not order:
        raise HTTPException(status_code=404, detail='Order not found')
    await _notify_current_user(
        session,
        current_user,
        title='Создан заказ поставщику',
        message=(
            f'Создан заказ поставщику #{order.id}'
            f' на {len(order.items or [])} поз.'
        ),
        level=AppNotificationLevel.SUCCESS,
        link=f'/customer-orders/suppliers/{order.id}',
    )

    items = []
    for item in order.items or []:
        autopart = item.autopart
        brand_name = (
            autopart.brand.name
            if autopart and autopart.brand
            else None
        )
        items.append(
            {
                'id': item.id,
                'customer_order_item_id': item.customer_order_item_id,
                'quantity': item.quantity,
                'price': item.price,
                'oem': autopart.oem_number if autopart else None,
                'brand': brand_name,
                'name': autopart.name if autopart else None,
                'requested_qty': None,
                'ship_qty': None,
                'reject_qty': None,
            }
        )
    return SupplierOrderDetailResponse(
        id=order.id,
        provider_id=order.provider_id,
        provider_name=order.provider.name if order.provider else None,
        status=order.status,
        created_at=order.created_at,
        scheduled_at=order.scheduled_at,
        sent_at=order.sent_at,
        items=items,
    )


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
    sent_count = result.get('sent', 0)
    error_count = result.get('failed', 0)
    await _notify_current_user(
        session,
        current_user,
        title='Отправка заказов поставщикам завершена',
        message=(
            f'Успешно отправлено: {sent_count}.'
            f' С ошибкой: {error_count}.'
        ),
        level=(
            AppNotificationLevel.WARNING
            if error_count
            else AppNotificationLevel.SUCCESS
        ),
        link='/customer-orders/suppliers',
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
    sent_count = result.get('sent', 0)
    error_count = result.get('failed', 0)
    await _notify_current_user(
        session,
        current_user,
        title='Плановая отправка заказов завершена',
        message=(
            f'Успешно отправлено: {sent_count}.'
            f' С ошибкой: {error_count}.'
        ),
        level=(
            AppNotificationLevel.WARNING
            if error_count
            else AppNotificationLevel.SUCCESS
        ),
        link='/customer-orders/suppliers',
    )
    return result
