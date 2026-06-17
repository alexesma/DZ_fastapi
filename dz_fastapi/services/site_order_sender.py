import logging
import os
from typing import Optional
from uuid import uuid4

from fastapi import HTTPException

from dz_fastapi.core.constants import URL_DZ_SEARCH
from dz_fastapi.core.db import AsyncSession
from dz_fastapi.crud.autopart import crud_autopart_restock_decision
from dz_fastapi.crud.order import crud_order, crud_order_item
from dz_fastapi.crud.partner import crud_customer, crud_provider
from dz_fastapi.http.dz_site_client import DZSiteClient
from dz_fastapi.models.autopart import TYPE_SUPPLIER_DECISION_STATUS
from dz_fastapi.models.notification import AppNotificationLevel
from dz_fastapi.models.partner import TYPE_ORDER_ITEM_STATUS, TYPE_PRICES, Provider
from dz_fastapi.models.user import User
from dz_fastapi.schemas.order import OrderPositionOut, SendApiResponse
from dz_fastapi.schemas.partner import ProviderExternalReferenceCreate
from dz_fastapi.services.inventory_stock import ensure_default_warehouse
from dz_fastapi.services.notifications import create_notification

KEY = os.getenv("KEY_FOR_WEBSITE")
DRAGONZAP_EXTERNAL_SOURCE = "DRAGONZAP"

logger = logging.getLogger("dz_fastapi")


async def _resolve_site_provider_id(
    session: AsyncSession,
    item: OrderPositionOut,
    provider_cache: dict[str, int],
) -> int:
    supplier_name = (item.supplier_name or "").strip()
    supplier_id = item.supplier_id
    id_cache_key = (
        f"{DRAGONZAP_EXTERNAL_SOURCE}:id:{int(supplier_id)}"
        if supplier_id is not None
        else None
    )
    if id_cache_key:
        cached_provider_id = provider_cache.get(id_cache_key)
        if cached_provider_id is not None:
            return cached_provider_id

        reference = await crud_provider.get_external_reference_by_source_supplier(
            source_system=DRAGONZAP_EXTERNAL_SOURCE,
            external_supplier_id=int(supplier_id),
            session=session,
        )
        if reference is not None and reference.is_active:
            provider_cache[id_cache_key] = int(reference.provider_id)
            if supplier_name:
                provider_cache[
                    f"{DRAGONZAP_EXTERNAL_SOURCE}:name:{supplier_name.casefold()}"
                ] = int(reference.provider_id)
            return int(reference.provider_id)

    name_cache_key = (
        f"{DRAGONZAP_EXTERNAL_SOURCE}:name:{supplier_name.casefold()}"
        if supplier_name
        else None
    )
    if name_cache_key:
        cached_provider_id = provider_cache.get(name_cache_key)
        if cached_provider_id is not None:
            return cached_provider_id

        provider = await crud_provider.get_provider_or_none(
            supplier_name, session
        )
        if provider is not None:
            if supplier_id is not None:
                await crud_provider.upsert_external_reference(
                    provider_id=provider.id,
                    obj_in=ProviderExternalReferenceCreate(
                        source_system=DRAGONZAP_EXTERNAL_SOURCE,
                        external_supplier_id=int(supplier_id),
                        external_supplier_name=supplier_name or None,
                        is_active=True,
                    ),
                    session=session,
                )
                provider_cache[id_cache_key] = int(provider.id)
            provider_cache[name_cache_key] = int(provider.id)
            return int(provider.id)

    if not supplier_name and supplier_id is None:
        raise HTTPException(
            status_code=400,
            detail=(
                "У позиции отсутствуют supplier_id и supplier_name. "
                "Невозможно определить поставщика для заказа на сайт."
            ),
        )

    provider = Provider(
        name=supplier_name or f"Dragonzap supplier #{supplier_id}",
        is_virtual=True,
        type_prices=TYPE_PRICES.WHOLESALE,
        description="Created automatically from Dragonzap site order",
        comment="Automatically created provider from site basket send",
        default_warehouse_id=(await ensure_default_warehouse(session)).id,
    )
    session.add(provider)
    await session.flush()

    if supplier_id is not None or supplier_name:
        await crud_provider.upsert_external_reference(
            provider_id=provider.id,
            obj_in=ProviderExternalReferenceCreate(
                source_system=DRAGONZAP_EXTERNAL_SOURCE,
                external_supplier_id=(
                    int(supplier_id) if supplier_id is not None else None
                ),
                external_supplier_name=supplier_name or None,
                is_active=True,
            ),
            session=session,
        )

    if id_cache_key:
        provider_cache[id_cache_key] = int(provider.id)
    if name_cache_key:
        provider_cache[name_cache_key] = int(provider.id)
    return int(provider.id)


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
            "Failed to create app notification for user %s",
            current_user.id,
        )


def _extract_basket_items(payload: object) -> list[dict]:
    if isinstance(payload, dict):
        items = payload.get("data")
    else:
        items = payload
    if not isinstance(items, list):
        return []
    return [item for item in items if isinstance(item, dict)]


def _build_basket_conflict_detail(basket_items: list[dict]) -> str:
    preview = ", ".join(
        str(item.get("oem") or item.get("detail_name") or item.get("id"))
        for item in basket_items[:3]
        if item.get("oem") or item.get("detail_name") or item.get("id")
    )
    detail = (
        "В корзине Dragonzap остались старые позиции. "
        "Программа попыталась очистить корзину автоматически, "
        "но не смогла."
    )
    if basket_items:
        detail += f" Сейчас в корзине {len(basket_items)} поз."
    if preview:
        detail += f" Примеры: {preview}."
    detail += (
        " Очистите корзину Dragonzap на сайте вручную и повторите отправку."
    )
    return detail


def _site_client_error_detail(client: object) -> str | None:
    detail = getattr(client, "last_error_detail", None)
    normalized = str(detail or "").strip()
    return normalized or None


def _normalize_tracking_uuid(raw_value: str | None) -> str:
    value = (raw_value or "").strip()
    if value and len(value) <= 36:
        return value
    return str(uuid4())


async def send_dragonzap_site_order(
    *,
    session: AsyncSession,
    current_user: User,
    request: list[OrderPositionOut],
    customer_id: int,
    order_comment: Optional[str] = None,
) -> SendApiResponse:
    if not request:
        raise HTTPException(
            status_code=400, detail="Список позиций не может быть пустым"
        )
    customer = await crud_customer.get_by_id(customer_id, session)
    if customer is None:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Клиент с id={customer_id} не найден. "
                "Выберите корректного клиента для заказа."
            ),
        )

    prepared_request: list[tuple[OrderPositionOut, str]] = []
    for item in request:
        request_tracking_uuid = (item.tracking_uuid or "").strip()
        normalized_tracking_uuid = _normalize_tracking_uuid(
            request_tracking_uuid
        )
        if normalized_tracking_uuid != request_tracking_uuid:
            item = item.model_copy(
                update={"tracking_uuid": normalized_tracking_uuid}
            )
        prepared_request.append(
            (item, request_tracking_uuid or normalized_tracking_uuid)
        )

    provider_cache: dict[str, int] = {}
    provider_ids: set[int] = set()
    for item, _request_tracking_uuid in prepared_request:
        provider_ids.add(
            await _resolve_site_provider_id(session, item, provider_cache)
        )
    if not provider_ids:
        raise HTTPException(
            status_code=400,
            detail="У позиций отсутствует supplier_id или supplier_name",
        )
    if len(provider_ids) > 1:
        raise HTTPException(
            status_code=400,
            detail=(
                "Позиции содержат разных поставщиков: "
                + ", ".join(str(provider_id) for provider_id in sorted(provider_ids))
            ),
        )
    provider_id = provider_ids.pop()
    results = []
    failed_count = 0
    try:
        staged_success: list[tuple[OrderPositionOut, str]] = []
        basket_started_empty = True
        basket_auto_cleaned = False
        async with DZSiteClient(
            base_url=URL_DZ_SEARCH, api_key=KEY, verify_ssl=False
        ) as dz_site_client:
            current_basket = await dz_site_client.get_basket(api_key=KEY)
            current_basket_items = _extract_basket_items(current_basket)
            basket_started_empty = len(current_basket_items) == 0
            if not basket_started_empty:
                logger.warning(
                    "Dragonzap basket contains %s stale positions; "
                    "trying to auto-clean before sending",
                    len(current_basket_items),
                )
                basket_cleaned = await dz_site_client.clean_basket(api_key=KEY)
                if not basket_cleaned:
                    raise HTTPException(
                        status_code=409,
                        detail=_build_basket_conflict_detail(
                            current_basket_items
                        ),
                    )
                basket_started_empty = True
                basket_auto_cleaned = True

            for item, request_tracking_uuid in prepared_request:
                try:
                    if not item.hash_key:
                        results.append(
                            {
                                "tracking_uuid": item.tracking_uuid,
                                "request_tracking_uuid": request_tracking_uuid,
                                "status": "error",
                                "message": "Отсутствует hash_key",
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
                        staged_success.append((item, request_tracking_uuid))
                    else:
                        failure_detail = (
                            _site_client_error_detail(dz_site_client)
                            or "Ошибка при добавлении в корзину"
                        )
                        await crud_order_item.update_order_item_status(
                            tracking_uuid=item.tracking_uuid,
                            new_status=TYPE_ORDER_ITEM_STATUS.FAILED,
                            session=session,
                        )
                        results.append(
                            {
                                "tracking_uuid": item.tracking_uuid,
                                "request_tracking_uuid": request_tracking_uuid,
                                "status": "error",
                                "message": failure_detail,
                            }
                        )
                        failed_count += 1
                except Exception as exc:
                    logger.error(
                        "Ошибка при отправке позиции %s: %s",
                        item.tracking_uuid,
                        exc,
                    )
                    results.append(
                        {
                            "tracking_uuid": item.tracking_uuid,
                            "request_tracking_uuid": request_tracking_uuid,
                            "status": "error",
                            "message": f"Внутренняя ошибка: {str(exc)}",
                        }
                    )
                    failed_count += 1

            if not staged_success:
                await session.rollback()
                await _notify_current_user(
                    session,
                    current_user,
                    title="Dragonzap: заказ не оформлен",
                    message=(
                        "Ни одна позиция не была добавлена в корзину сайта. "
                        "Локальный заказ не создан."
                    ),
                    level=AppNotificationLevel.WARNING,
                    link="/autoparts/offers",
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
                    comment=(
                        str(order_comment).strip()
                        if str(order_comment or "").strip()
                        else "АвтоЗаказ из модуля автопополнения"
                    ),
                )
                if not placed:
                    logger.warning(
                        "Оформление корзины (baskets/order) вернуло не OK"
                    )
            except Exception as exc:
                logger.error("Ошибка при оформлении корзины в заказ: %s", exc)

            if not placed:
                basket_cleaned = False
                if basket_started_empty:
                    basket_cleaned = await dz_site_client.clean_basket(
                        api_key=KEY
                    )
                failure_reason = _site_client_error_detail(dz_site_client)
                failure_message = (
                    "Корзина на Dragonzap не была оформлена в заказ. "
                    "Локальная запись не создана."
                )
                if failure_reason:
                    failure_message += f" Причина: {failure_reason}."
                if basket_cleaned:
                    failure_message += " Временная корзина очищена."
                else:
                    failure_message += " Проверьте корзину Dragonzap вручную."
                for item, request_tracking_uuid in staged_success:
                    results.append(
                        {
                            "tracking_uuid": item.tracking_uuid,
                            "request_tracking_uuid": request_tracking_uuid,
                            "status": "error",
                            "message": failure_message,
                        }
                    )
                failed_count += len(staged_success)
                await session.rollback()
                await _notify_current_user(
                    session,
                    current_user,
                    title="Dragonzap: заказ не оформлен",
                    message=failure_message,
                    level=AppNotificationLevel.WARNING,
                    link="/autoparts/offers",
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
                    "No AutoPartRestockDecisionSupplier for "
                    "tracking_uuid=%s; skip restock status update",
                    item.tracking_uuid,
                )
            results.append(
                {
                    "tracking_uuid": item.tracking_uuid,
                    "request_tracking_uuid": request_tracking_uuid,
                    "status": "success",
                    "message": "Заказ оформлен на сайте Dragonzap",
                }
            )

        successful_count = len(staged_success)
        await _notify_current_user(
            session,
            current_user,
            title="Заказ на Dragonzap оформлен",
            message=(
                f"Создан заказ #{order.id}"
                f" на {successful_count} поз."
                f" Успешно: {successful_count}, ошибок: {failed_count}."
                + (
                    " Перед отправкой программа автоматически очистила "
                    "старую корзину Dragonzap."
                    if basket_auto_cleaned
                    else ""
                )
            ),
            level=(
                AppNotificationLevel.WARNING
                if failed_count
                else AppNotificationLevel.SUCCESS
            ),
            link="/orders/tracking",
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
    except Exception as exc:
        await session.rollback()
        logger.error("Ошибка при создании заказа: %s", exc)
        raise HTTPException(
            status_code=500, detail=f"Ошибка при создании заказа: {str(exc)}"
        ) from exc
