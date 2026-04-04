import logging
import os
from datetime import date
from typing import List, Optional
from uuid import uuid4

from fastapi import APIRouter, Depends, HTTPException, Query, status

from dz_fastapi.analytics.restock_logic import (
    evaluate_supplier_offers, fetch_supplier_offers,
    get_autoparts_below_min_balance, get_historical_min_price,
    save_restock_decision)
from dz_fastapi.api.deps import get_current_user
from dz_fastapi.core.constants import (DEPTH_MONTHS_HISTORY_PRICE_FOR_ORDER,
                                       LIMIT_ORDER, URL_DZ_SEARCH)
from dz_fastapi.core.db import AsyncSession, get_session
from dz_fastapi.crud.autopart import crud_autopart_restock_decision
from dz_fastapi.crud.brand import brand_crud
from dz_fastapi.crud.order import crud_order, crud_order_item
from dz_fastapi.crud.partner import crud_customer, crud_provider
from dz_fastapi.http.dz_site_client import DZSiteClient
from dz_fastapi.models.autopart import TYPE_SUPPLIER_DECISION_STATUS
from dz_fastapi.models.notification import AppNotificationLevel
from dz_fastapi.models.partner import (TYPE_ORDER_ITEM_STATUS, TYPE_PRICES,
                                       TYPE_STATUS_ORDER, Provider)
from dz_fastapi.models.user import User
from dz_fastapi.schemas.order import (ConfirmedOfferOut,
                                      ConfirmedOffersResponse, OrderItemOut,
                                      OrderOut, OrderPositionOut,
                                      PlacedOrderHistoryRow,
                                      PlacedOrderHistoryUpdate,
                                      SendApiResponse, SupplierOfferOut,
                                      SupplierOffersResponse, SupplierOrderOut,
                                      UpdatePositionStatusRequest,
                                      UpdatePositionStatusResponse)
from dz_fastapi.services.notifications import create_notification
from dz_fastapi.services.placed_orders import (list_tracking_history,
                                               sync_site_tracking_statuses,
                                               update_tracking_item)

KEY = os.getenv('KEY_FOR_WEBSITE')

logger = logging.getLogger('dz_fastapi')

MAX_LOCAL_PROVIDER_ID = 2_147_483_647


router = APIRouter(prefix='/order')


async def _resolve_site_provider_id(
    session: AsyncSession,
    item: OrderPositionOut,
    provider_cache: dict[str, int],
) -> int:
    supplier_name = (item.supplier_name or '').strip()
    cache_key = supplier_name.casefold() if supplier_name else None
    if cache_key:
        cached_provider_id = provider_cache.get(cache_key)
        if cached_provider_id is not None:
            return cached_provider_id

        provider = await crud_provider.get_provider_or_none(
            supplier_name, session
        )
        if provider is not None:
            provider_cache[cache_key] = provider.id
            return provider.id

    if (
        item.supplier_id is not None
        and 0 < item.supplier_id <= MAX_LOCAL_PROVIDER_ID
    ):
        provider_by_id = await crud_provider.get_by_id(
            item.supplier_id, session
        )
        if provider_by_id is not None:
            if cache_key:
                provider_cache[cache_key] = provider_by_id.id
            return provider_by_id.id

        logger.warning(
            'Dragonzap supplier_id=%s is not a local provider id; '
            'fallback to supplier_name=%r',
            item.supplier_id,
            supplier_name,
        )
    elif item.supplier_id is not None:
        logger.warning(
            'Dragonzap supplier_id=%s is outside local provider id range; '
            'fallback to supplier_name=%r',
            item.supplier_id,
            supplier_name,
        )

    if not supplier_name:
        raise HTTPException(
            status_code=400,
            detail=(
                'У позиции отсутствуют supplier_id и supplier_name. '
                'Невозможно определить поставщика для заказа на сайт.'
            ),
        )

    provider = Provider(
        name=supplier_name,
        is_virtual=True,
        type_prices=TYPE_PRICES.WHOLESALE,
        description='Created automatically from Dragonzap site order',
        comment='Automatically created provider from site basket send',
    )
    session.add(provider)
    await session.flush()
    provider_cache[cache_key] = provider.id
    return provider.id


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


def _merge_site_offers(offers_by_brand: list[list[dict]]) -> list[dict]:
    merged: list[dict] = []
    seen = set()
    for offers in offers_by_brand:
        for raw in offers or []:
            key = (
                raw.get('system_hash')
                or raw.get('hash_key')
                or (
                    raw.get('oem'),
                    raw.get('make_name'),
                    raw.get('cost'),
                    raw.get('qnt'),
                    raw.get('price_name'),
                    raw.get('sup_logo'),
                    raw.get('min_delivery_day'),
                    raw.get('max_delivery_day'),
                )
            )
            if key in seen:
                continue
            seen.add(key)
            merged.append(raw)
    return merged


def _extract_basket_items(payload: object) -> list[dict]:
    if isinstance(payload, dict):
        items = payload.get('data')
    else:
        items = payload
    if not isinstance(items, list):
        return []
    return [item for item in items if isinstance(item, dict)]


def _build_basket_conflict_detail(basket_items: list[dict]) -> str:
    preview = ', '.join(
        str(item.get('oem') or item.get('detail_name') or item.get('id'))
        for item in basket_items[:3]
        if item.get('oem') or item.get('detail_name') or item.get('id')
    )
    detail = (
        'Корзина Dragonzap уже не пуста. '
        'Чтобы не смешивать заказы, оформление из программы остановлено.'
    )
    if basket_items:
        detail += f' Сейчас в корзине {len(basket_items)} поз.'
    if preview:
        detail += f' Примеры: {preview}.'
    detail += (
        ' Очистите корзину Dragonzap на сайте или кнопкой '
        '«Очистить корзину Dragonzap», затем повторите отправку.'
    )
    return detail


def _normalize_tracking_uuid(raw_value: str | None) -> str:
    value = (raw_value or '').strip()
    if value and len(value) <= 36:
        return value
    return str(uuid4())


async def _expand_query_brands(
    make_name: str, session: AsyncSession
) -> list[str]:
    normalized_input = str(make_name or '').strip().upper()
    if not normalized_input:
        return []

    expanded = [normalized_input]
    try:
        main_brand = await brand_crud.get_brand_by_name_or_none(
            brand_name=normalized_input,
            session=session,
        )
        if not main_brand:
            return expanded

        related = await brand_crud.get_all_synonyms_bi_directional(
            brand=main_brand,
            session=session,
        )
        candidates = [str(main_brand.name).strip().upper()]
        candidates.extend(
            str(item.name).strip().upper()
            for item in related
            if str(getattr(item, 'name', '')).strip()
        )
        candidates.append(normalized_input)
        unique = []
        seen: set[str] = set()
        for candidate in candidates:
            if not candidate or candidate in seen:
                continue
            seen.add(candidate)
            unique.append(candidate)
        return unique
    except Exception as exc:
        logger.warning(
            'Failed to expand brand synonyms for %s: %s',
            normalized_input,
            exc,
        )
        return expanded


@router.get(
    '/get_offers_by_oem_and_make_name',
    tags=['offer'],
    status_code=status.HTTP_200_OK,
    summary='Получение предложений с сайта dragonzap по oem и brand name',
)
async def get_offers_by_oem_and_make_name(
    oem: str,
    make_name: str,
    without_cross: bool = True,
    session: AsyncSession = Depends(get_session),
):
    query_brands = await _expand_query_brands(
        make_name=make_name,
        session=session,
    )
    offers_by_brand: list[list[dict]] = []
    async with DZSiteClient(
        base_url=URL_DZ_SEARCH, api_key=KEY, verify_ssl=False
    ) as dz_site_client:
        for brand_name in query_brands:
            offers = await dz_site_client.get_offers(
                oem=oem,
                brand=brand_name,
                without_cross=without_cross,
            )
            if not offers:
                continue
            for item in offers:
                if isinstance(item, dict):
                    item.setdefault('query_brand', brand_name)
            offers_by_brand.append(offers)

    merged = _merge_site_offers(offers_by_brand)
    return {'data': merged, 'query_brands': query_brands}


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
    current_user: User = Depends(get_current_user),
):
    if not request:
        raise HTTPException(
            status_code=400, detail='Список позиций не может быть пустым'
        )
    customer = await crud_customer.get_by_id(customer_id, session)
    if customer is None:
        raise HTTPException(
            status_code=400,
            detail=(
                f'Клиент с id={customer_id} не найден. '
                'Выберите корректного клиента для заказа.'
            ),
        )
    prepared_request: list[tuple[OrderPositionOut, str]] = []
    for item in request:
        request_tracking_uuid = (item.tracking_uuid or '').strip()
        normalized_tracking_uuid = _normalize_tracking_uuid(
            request_tracking_uuid
        )
        if normalized_tracking_uuid != request_tracking_uuid:
            logger.debug(
                'Replacing incoming tracking_uuid=%r with normalized '
                'tracking_uuid=%s',
                request_tracking_uuid,
                normalized_tracking_uuid,
            )
            item = item.model_copy(
                update={'tracking_uuid': normalized_tracking_uuid}
            )
        prepared_request.append(
            (item, request_tracking_uuid or normalized_tracking_uuid)
        )
    # 1) Определяем поставщика из позиций
    provider_cache: dict[str, int] = {}
    provider_ids: set[int] = set()
    for item, _request_tracking_uuid in prepared_request:
        provider_ids.add(
            await _resolve_site_provider_id(session, item, provider_cache)
        )
    if not provider_ids:
        raise HTTPException(
            status_code=400,
            detail='У позиций отсутствует supplier_id или supplier_name',
        )
    if len(provider_ids) > 1:
        raise HTTPException(
            status_code=400, detail='Позиции содержат разных поставщиков'
        )
    provider_id = provider_ids.pop()
    results = []
    failed_count = 0
    try:
        staged_success: list[tuple[OrderPositionOut, str]] = []
        basket_started_empty = True
        async with DZSiteClient(
            base_url=URL_DZ_SEARCH, api_key=KEY, verify_ssl=False
        ) as dz_site_client:
            current_basket = await dz_site_client.get_basket(api_key=KEY)
            current_basket_items = _extract_basket_items(current_basket)
            basket_started_empty = len(current_basket_items) == 0
            if not basket_started_empty:
                raise HTTPException(
                    status_code=409,
                    detail=_build_basket_conflict_detail(
                        current_basket_items
                    ),
                )
            for item, request_tracking_uuid in prepared_request:
                try:
                    if not item.hash_key:
                        results.append(
                            {
                                'tracking_uuid': item.tracking_uuid,
                                'request_tracking_uuid': (
                                    request_tracking_uuid
                                ),
                                'status': 'error',
                                'message': 'Отсутствует hash_key',
                            }
                        )
                        failed_count += 1
                        continue
                    added_to_basket = (
                        await dz_site_client.add_autopart_in_basket(
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
                    )
                    if added_to_basket:
                        staged_success.append(
                            (item, request_tracking_uuid)
                        )
                    else:
                        await crud_order_item.update_order_item_status(
                            tracking_uuid=item.tracking_uuid,
                            new_status=TYPE_ORDER_ITEM_STATUS.FAILED,
                            session=session,
                        )
                        results.append(
                            {
                                'tracking_uuid': item.tracking_uuid,
                                'request_tracking_uuid': (
                                    request_tracking_uuid
                                ),
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
                            'request_tracking_uuid': request_tracking_uuid,
                            'status': 'error',
                            'message': f'Внутренняя ошибка: {str(e)}',
                        }
                    )
                    failed_count += 1
            if not staged_success:
                await session.rollback()
                await _notify_current_user(
                    session,
                    current_user,
                    title='Dragonzap: заказ не оформлен',
                    message=(
                        'Ни одна позиция не была добавлена в корзину сайта. '
                        'Локальный заказ не создан.'
                    ),
                    level=AppNotificationLevel.WARNING,
                    link='/autoparts/offers',
                )
                return SendApiResponse(
                    total_items=len(request),
                    successful_items=0,
                    failed_items=failed_count,
                    results=results,
                    order_id=None,
                    order_number=None,
                )

            placed = False
            try:
                placed = await dz_site_client.order_basket(
                    api_key=KEY,
                    comment='АвтоЗаказ из поиска по артикулу',
                )
                if not placed:
                    logger.warning(
                        'Оформление корзины (baskets/order) вернуло не OK'
                    )
            except Exception as exc:
                logger.error(
                    'Ошибка при оформлении корзины в заказ: %s',
                    exc,
                )

            if not placed:
                basket_cleaned = False
                if basket_started_empty:
                    basket_cleaned = await dz_site_client.clean_basket(
                        api_key=KEY
                    )
                failure_message = (
                    'Корзина на Dragonzap не была оформлена в заказ. '
                    'Локальная запись не создана.'
                )
                if basket_cleaned:
                    failure_message += ' Временная корзина очищена.'
                else:
                    failure_message += (
                        ' Проверьте корзину Dragonzap вручную.'
                    )
                for item, request_tracking_uuid in staged_success:
                    results.append(
                        {
                            'tracking_uuid': item.tracking_uuid,
                            'request_tracking_uuid': request_tracking_uuid,
                            'status': 'error',
                            'message': failure_message,
                        }
                    )
                failed_count += len(staged_success)
                await session.rollback()
                await _notify_current_user(
                    session,
                    current_user,
                    title='Dragonzap: заказ не оформлен',
                    message=failure_message,
                    level=AppNotificationLevel.WARNING,
                    link='/autoparts/offers',
                )
                return SendApiResponse(
                    total_items=len(request),
                    successful_items=0,
                    failed_items=failed_count,
                    results=results,
                    order_id=None,
                    order_number=None,
                )

        order = await crud_order.create_order_with_items(
            provider_id=provider_id,
            customer_id=customer.id,
            items=[item for item, _request_tracking_uuid in staged_success],
            session=session,
            comment=f"Заказ из {len(staged_success)} позиций",
            created_by_user_id=current_user.id,
            initial_item_status=TYPE_ORDER_ITEM_STATUS.SENT,
        )

        for item, request_tracking_uuid in staged_success:
            try:
                await crud_autopart_restock_decision.update_positions_status(
                    tracking_uuids=[item.tracking_uuid],
                    status=TYPE_SUPPLIER_DECISION_STATUS.SEND,
                    session=session,
                )
            except HTTPException as exc:
                if exc.status_code != 404:
                    raise
                logger.debug(
                    'No AutoPartRestockDecisionSupplier for '
                    'tracking_uuid=%s; skip restock status update',
                    item.tracking_uuid,
                )
            results.append(
                {
                    'tracking_uuid': item.tracking_uuid,
                    'request_tracking_uuid': request_tracking_uuid,
                    'status': 'success',
                    'message': 'Заказ оформлен на сайте Dragonzap',
                }
            )

        successful_count = len(staged_success)
        await _notify_current_user(
            session,
            current_user,
            title='Заказ на Dragonzap оформлен',
            message=(
                f'Создан заказ #{order.id}'
                f' на {successful_count} поз.'
                f' Успешно: {successful_count}, ошибок: {failed_count}.'
            ),
            level=(
                AppNotificationLevel.WARNING
                if failed_count
                else AppNotificationLevel.SUCCESS
            ),
            link='/orders/tracking',
        )
        return SendApiResponse(
            total_items=len(request),
            successful_items=successful_count,
            failed_items=failed_count,
            results=results,
            order_id=order.id,
            order_number=order.order_number,
        )
    except HTTPException:
        await session.rollback()
        raise
    except Exception as e:
        await session.rollback()
        logger.error(f'Ошибка при создании заказа: {e}')
        raise HTTPException(
            status_code=500, detail=f'Ошибка при создании заказа: {str(e)}'
        )


@router.get(
    '/tracking-items',
    response_model=list[PlacedOrderHistoryRow],
    summary='История заказов из поиска по артикулу',
)
async def get_tracking_items(
    oem: Optional[str] = None,
    brand: Optional[str] = None,
    provider_id: Optional[int] = None,
    customer_id: Optional[int] = None,
    status: Optional[str] = None,
    sync_site: bool = Query(default=False),
    date_from: Optional[date] = Query(default=None),
    date_to: Optional[date] = Query(default=None),
    limit: int = Query(default=300, ge=1, le=1000),
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    return await list_tracking_history(
        session=session,
        oem_number=oem,
        brand_name=brand,
        provider_id=provider_id,
        customer_id=customer_id,
        status=status,
        date_from=date_from,
        date_to=date_to,
        limit=limit,
        sync_site=sync_site,
    )


@router.post(
    '/tracking-items/sync-site',
    response_model=dict,
    summary='Синхронизировать статусы заказов с сайта Dragonzap',
)
async def sync_tracking_items_with_site(
    oem: Optional[str] = None,
    brand: Optional[str] = None,
    provider_id: Optional[int] = None,
    customer_id: Optional[int] = None,
    limit: int = Query(default=100, ge=1, le=500),
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    return await sync_site_tracking_statuses(
        session=session,
        oem_number=oem,
        brand_name=brand,
        provider_id=provider_id,
        customer_id=customer_id,
        limit=limit,
    )


@router.patch(
    '/tracking-items/{source_type}/{item_id}',
    response_model=dict,
    summary='Обновить статус и получение по позиции заказа',
)
async def update_tracking_order_item(
    source_type: str,
    item_id: int,
    payload: PlacedOrderHistoryUpdate,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    try:
        return await update_tracking_item(
            session=session,
            source_type=source_type,
            item_id=item_id,
            status=payload.status,
            received_quantity=payload.received_quantity,
        )
    except KeyError as exc:
        raise HTTPException(
            status_code=400,
            detail='Неизвестный статус',
        ) from exc
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


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


@router.post(
    '/dragonzap/basket/clear',
    tags=['offer', 'order', 'api'],
    summary='Очистить корзину Dragonzap',
)
async def clear_dragonzap_basket(
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    async with DZSiteClient(api_key=KEY, verify_ssl=False) as dz:
        current_basket = await dz.get_basket(api_key=KEY)
        current_basket_items = _extract_basket_items(current_basket)
        if current_basket is not None and not current_basket_items:
            return {
                'cleared': False,
                'cleared_items': 0,
                'message': 'Корзина Dragonzap уже пуста.',
            }

        cleaned = await dz.clean_basket(api_key=KEY)
        if not cleaned:
            raise HTTPException(
                status_code=502,
                detail=(
                    'Не удалось очистить корзину Dragonzap. '
                    'Проверьте сайт вручную и повторите попытку.'
                ),
            )

    cleared_items = len(current_basket_items)
    if cleared_items:
        success_message = (
            f'Корзина Dragonzap очищена. Удалено позиций: {cleared_items}.'
        )
    else:
        success_message = 'Корзина Dragonzap очищена.'
    await _notify_current_user(
        session,
        current_user,
        title='Корзина Dragonzap очищена',
        message=success_message,
        level=AppNotificationLevel.INFO,
        link='/autoparts/offers',
    )
    return {
        'cleared': True,
        'cleared_items': cleared_items,
        'message': success_message,
    }


#
# router.post(
#     '/send_mail',
#     tags=['offer', 'order', 'mail'],
#     status_code=status.HTTP_201_CREATED,
#     summary='Отправка заказов поставщику через mail',
#     response_model=,
# )
