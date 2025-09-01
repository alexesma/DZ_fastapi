import logging
import os
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, status

from dz_fastapi.analytics.restock_logic import (
    evaluate_supplier_offers, fetch_supplier_offers,
    get_autoparts_below_min_balance, get_historical_min_price,
    save_restock_decision)
from dz_fastapi.core.constants import (DEPTH_MONTHS_HISTORY_PRICE_FOR_ORDER,
                                       LIMIT_ORDER, URL_DZ_SEARCH)
from dz_fastapi.core.db import AsyncSession, get_session
from dz_fastapi.crud.autopart import crud_autopart_restock_decision
from dz_fastapi.crud.order import crud_order, crud_order_item
from dz_fastapi.http.dz_site_client import DZSiteClient
from dz_fastapi.models.autopart import TYPE_SUPPLIER_DECISION_STATUS
from dz_fastapi.models.partner import TYPE_ORDER_ITEM_STATUS, TYPE_STATUS_ORDER
from dz_fastapi.schemas.order import (ConfirmedOfferOut,
                                      ConfirmedOffersResponse, OrderItemOut,
                                      OrderOut, OrderPositionOut,
                                      SendApiResponse, SupplierOfferOut,
                                      SupplierOffersResponse, SupplierOrderOut,
                                      UpdatePositionStatusRequest,
                                      UpdatePositionStatusResponse)

KEY = os.getenv('KEY_FOR_WEBSITE')

logger = logging.getLogger('dz_fastapi')


router = APIRouter(prefix='/order')


@router.get(
    '/get_offers_by_oem_and_make_name',
    tags=['offer'],
    status_code=status.HTTP_200_OK,
    summary='Получение предложений с сайта dragonzap по oem и brand name',
)
async def get_offers_by_oem_and_make_name(
    oem: str, make_name: str, without_cross: bool = True
):
    async with DZSiteClient(
        base_url=URL_DZ_SEARCH, api_key=KEY, verify_ssl=False
    ) as dz_site_client:
        offers = await dz_site_client.get_offers(
            oem=oem, brand=make_name, without_cross=without_cross
        )
        return offers


@router.get(
    '/generate_restock_offers',
    tags=['offer'],
    status_code=status.HTTP_200_OK,
    summary='Получение предложения для заказа недостающих позиций',
)
async def generate_restock_offers(
    session: AsyncSession = Depends(get_session),
    budget_limit: Optional[int] = None,
    months_back: Optional[int] = None,
    threshold_percent: Optional[float] = None,
):
    logger.debug('Зашли в generate_restock_offers')
    autoparts = await get_autoparts_below_min_balance(
        threshold_percent=threshold_percent or 0.5, session=session
    )
    logger.debug(f'Словарь autoparts для заказа = {autoparts}')
    supplier_prices = await fetch_supplier_offers(
        autopart_ids=list(autoparts.keys()), session=session
    )
    logger.debug(f'Словарь предложений из прайс листов {supplier_prices}')
    historical_min_prices = await get_historical_min_price(
        months_back=months_back or DEPTH_MONTHS_HISTORY_PRICE_FOR_ORDER,
        autopart_ids=list(autoparts.keys()),
        session=session,
    )
    logger.debug(f'Словарь historical_min_prices = {historical_min_prices}')
    supplier_offers = await evaluate_supplier_offers(
        ids_autoparts_for_order=autoparts,
        autoparts_in_prices=supplier_prices,
        historical_min_prices=historical_min_prices,
        budget_limit=budget_limit or LIMIT_ORDER,
        session=session,
    )
    logger.debug(f'Словарь supplier_offers = {supplier_offers}')

    result = []
    for autopart_id, data in supplier_offers.items():
        result.append(
            SupplierOfferOut(
                autopart_id=autopart_id,
                oem_number=data['oem_number'],
                autopart_name=data['detail_name'],
                supplier_id=data['supplier_id'],
                supplier_name=data['supplier_name'],
                price=data['price'],
                quantity=data['quantity'],
                total_cost=data['total_cost'],
                qnt=data['qnt'],
                min_delivery_day=data['min_delivery_day'],
                max_delivery_day=data['max_delivery_day'],
                sup_logo=data['sup_logo'],
                brand_name=data['make_name'],
                historical_min_price=data['historical_min_price'],
                min_qnt=data.get('min_qnt', 1),
                hash_key=data.get('hash_key'),
                system_hash=data.get('system_hash'),
            )
        )
    logger.debug('Вышли из generate_restock_offers')
    return SupplierOffersResponse(offers=result)


@router.post(
    '/confirm',
    tags=['offer'],
    status_code=status.HTTP_201_CREATED,
    summary='Подтверждение предложения для заказа недостающих позиций',
)
async def confirm_order(
    request: SupplierOffersResponse,
    session: AsyncSession = Depends(get_session),
):
    offers_dict = {offer.autopart_id: offer.dict() for offer in request.offers}
    logger.debug(f'Offers dict for CRUD: {offers_dict}')
    await save_restock_decision(offers=offers_dict, session=session)
    response = [
        ConfirmedOfferOut(
            autopart_id=offer.autopart_id,
            supplier_id=offer.supplier_id,
            quantity=offer.quantity,
            confirmed_price=offer.price,
            status=TYPE_SUPPLIER_DECISION_STATUS.CONFIRMED,
            send_method=getattr(offer, 'send_method', None),
            brand_name=getattr(offer, 'brand_name', None),
            min_delivery_day=getattr(offer, 'min_delivery_day', 1),
            max_delivery_day=getattr(offer, 'max_delivery_day', 3),
        )
        for offer in request.offers
    ]
    return ConfirmedOffersResponse(
        confirmed_offers=response, total_items=len(response)
    )


@router.get(
    '/confirmed',
    tags=['offer'],
    status_code=status.HTTP_200_OK,
    summary='Получение предложения для заказов поставщикам',
    response_model=list[SupplierOrderOut],
)
async def confirmed_orders_supplier(
    session: AsyncSession = Depends(get_session),
):
    logger.debug('Get запрос confirmed')
    return await crud_autopart_restock_decision.get_new_supplier_orders(
        session=session
    )


@router.patch(
    '/update_position_status',
    tags=['offer', 'status'],
    status_code=status.HTTP_200_OK,
    summary='Изменение статусы заказа',
    response_model=UpdatePositionStatusResponse,
)
async def update_position_status(
    request: UpdatePositionStatusRequest,
    session: AsyncSession = Depends(get_session),
):
    item = await crud_autopart_restock_decision.update_positions_status(
        tracking_uuids=request.tracking_uuids,
        status=request.status.value,
        session=session,
    )
    return item


@router.post(
    '/send_api',
    tags=['offer', 'order', 'api'],
    status_code=status.HTTP_201_CREATED,
    summary='Отправка заказов поставщику через api',
    response_model=SendApiResponse,
)
async def send_api(
    request: list[OrderPositionOut],
    customer_id: int = 2,
    session: AsyncSession = Depends(get_session),
):
    if not request:
        raise HTTPException(
            status_code=400, detail='Список позиций не может быть пустым'
        )
    # 1) Определяем поставщика из позиций
    provider_ids = {
        item.supplier_id for item in request if item.supplier_id is not None
    }
    if not provider_ids:
        raise HTTPException(
            status_code=400, detail='У позиций отсутствует supplier_id'
        )
    if len(provider_ids) > 1:
        raise HTTPException(
            status_code=400, detail='Позиции содержат разных поставщиков'
        )
    provider_id = provider_ids.pop()
    results = []
    successful_count = 0
    failed_count = 0
    try:
        '''ЭТАП 1: Создаем заказ в нашей БД'''
        order = await crud_order.create_order_with_items(
            provider_id=provider_id,
            customer_id=customer_id,
            items=request,
            session=session,
            comment=f"Заказ из {len(request)} позиций",
        )

        '''ЭТАП 2: Отправляем позиции в корзину поставщика'''
        async with DZSiteClient(
            base_url=URL_DZ_SEARCH, api_key=KEY, verify_ssl=False
        ) as dz_site_client:
            for item in request:
                try:
                    if not item.hash_key:
                        results.append(
                            {
                                'tracking_uuid': item.tracking_uuid,
                                'status': 'error',
                                'message': 'Отсутствует hash_key',
                            }
                        )
                        failed_count += 1
                        continue
                    success = await dz_site_client.add_autopart_in_basket(
                        oem=item.oem_number,
                        make_name=item.brand_name,
                        detail_name=item.autopart_name,
                        qnt=item.quantity,
                        comment=item.tracking_uuid,
                        min_delivery_day=item.min_delivery_day or 1,
                        max_delivery_day=item.max_delivery_day or 3,
                        api_hash=item.hash_key,
                        api_key=KEY,
                        use_form=False,
                    )
                    if success:
                        '''ЭТАП 3: Обновляем статус позиции заказа'''
                        await crud_order_item.update_order_item_status(
                            tracking_uuid=item.tracking_uuid,
                            new_status=TYPE_ORDER_ITEM_STATUS.SENT,
                            session=session,
                        )

                        await crud_autopart_restock_decision.update_positions_status(   # noqa: E501
                            tracking_uuids=[item.tracking_uuid],
                            status=TYPE_SUPPLIER_DECISION_STATUS.SEND,
                            session=session,
                        )
                        verify = await dz_site_client.get_basket(api_key=KEY)
                        items = (
                            verify.get('data')
                            if isinstance(verify, dict)
                            else verify
                        ) or []
                        in_cart = any(
                            i.get('comment') == item.tracking_uuid
                            for i in items
                        )
                        logger.debug(
                            f'Basket contains {item.tracking_uuid}: '
                            f'{in_cart}. Raw: {items[:3]}'
                        )
                        results.append(
                            {
                                'tracking_uuid': item.tracking_uuid,
                                'status': 'success',
                                'message': 'Успешно добавлено в корзину',
                                'verify': verify,
                            }
                        )
                        successful_count += 1
                    else:
                        await crud_order_item.update_order_item_status(
                            tracking_uuid=item.tracking_uuid,
                            new_status=TYPE_ORDER_ITEM_STATUS.FAILED,
                            session=session,
                        )
                        results.append(
                            {
                                'tracking_uuid': item.tracking_uuid,
                                'status': 'error',
                                'message': 'Ошибка при добавлении в корзину',
                            }
                        )
                        failed_count += 1
                except Exception as e:
                    logger.error(
                        f'Ошибка при отправке позиции '
                        f'{item.tracking_uuid}: {e}'
                    )
                    results.append(
                        {
                            'tracking_uuid': item.tracking_uuid,
                            'status': 'error',
                            'message': f'Внутренняя ошибка: {str(e)}',
                        }
                    )
                    failed_count += 1
            placed = False
            if successful_count > 0:
                try:
                    placed = await dz_site_client.order_basket(
                        api_key=KEY,
                        comment=(
                            f'АвтоЗаказ #{order.id} ({order.order_number})'
                            if order and getattr(order, "order_number", None)
                            else None
                        ),
                    )
                    if not placed:
                        logger.warning(
                            'Оформление корзины (baskets/order) вернуло не OK'
                        )
                except Exception as e:
                    logger.error(f'Ошибка при оформлении корзины в заказ: {e}')
        '''ЭТАП 4: Обновляем статус основного заказа'''
        if successful_count > 0:
            if failed_count == 0:
                order.status = TYPE_STATUS_ORDER.ORDERED
            elif placed:
                order.status = TYPE_STATUS_ORDER.ORDERED
            else:
                order.status = TYPE_STATUS_ORDER.PROCESSING
        else:
            order.status = TYPE_STATUS_ORDER.ERROR

        await session.commit()
        return SendApiResponse(
            total_items=len(request),
            successful_items=successful_count,
            failed_items=failed_count,
            results=results,
            order_id=order.id,
            order_number=order.order_number,
        )
    except Exception as e:
        await session.rollback()
        logger.error(f'Ошибка при создании заказа: {e}')
        raise HTTPException(
            status_code=500, detail=f'Ошибка при создании заказа: {str(e)}'
        )


@router.get('/{order_id}', response_model=OrderOut)
async def get_order(
    order_id: int, session: AsyncSession = Depends(get_session)
):
    '''Получение заказа по ID'''
    order = await crud_order.get(obj_id=order_id, session=session)
    if not order:
        raise HTTPException(status_code=404, detail='Заказ не найден')
    return order


@router.get('/{order_id}/items', response_model=List[OrderItemOut])
async def get_order_items(
    order_id: int, session: AsyncSession = Depends(get_session)
):
    '''Получение позиций заказа'''
    items = await crud_order_item.get_order_items_by_order_id(
        order_id=order_id, session=session
    )
    return items


@router.patch('/{order_id}/status')
async def update_order_status(
    order_id: int,
    status: TYPE_STATUS_ORDER,
    session: AsyncSession = Depends(get_session),
):
    '''Обновление статуса заказа'''
    order = await crud_order.get(order_id, session)
    if not order:
        raise HTTPException(status_code=404, detail='Заказ не найден')

    order.status = status
    await session.commit()
    return {'message': 'Статус заказа обновлен'}


@router.get('', response_model=List[OrderOut], summary='Список заказов')
async def list_orders(session: AsyncSession = Depends(get_session)):
    orders = await crud_order.get_all_orders(session=session)
    return orders


@router.get('/debug/basket')
async def debug_basket():
    async with DZSiteClient(api_key=KEY, verify_ssl=False) as dz:
        return await dz.get_basket(api_key=KEY)


#
# router.post(
#     '/send_mail',
#     tags=['offer', 'order', 'mail'],
#     status_code=status.HTTP_201_CREATED,
#     summary='Отправка заказов поставщику через mail',
#     response_model=,
# )
