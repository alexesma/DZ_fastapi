import html
import logging
import os
from decimal import ROUND_HALF_UP, Decimal

from sqlalchemy import select

from dz_fastapi.core.constants import URL_DZ_SEARCH
from dz_fastapi.core.time import now_moscow
from dz_fastapi.crud.autopart import crud_autopart
from dz_fastapi.crud.brand import brand_crud
from dz_fastapi.crud.partner import crud_provider
from dz_fastapi.crud.watchlist import crud_price_watch_item
from dz_fastapi.http.dz_site_client import DZSiteClient
from dz_fastapi.models.autopart import AutoPartPriceHistory
from dz_fastapi.models.notification import AppNotificationLevel
from dz_fastapi.models.partner import Provider
from dz_fastapi.services.notifications import create_admin_notifications

logger = logging.getLogger('dz_fastapi')
SITE_PROVIDER_NAME = 'Сайт Dragonzap'
SITE_PRICELIST_ID = 0
PRICE_STEP = Decimal('0.01')
TOP_SITE_OFFERS_LIMIT = 3


def _notify_immediately() -> bool:
    return os.getenv('WATCHLIST_NOTIFY_MODE', 'immediate').lower() != 'daily'


def _norm(value: str) -> str:
    return str(value or '').strip().upper()


def _normalize_price(value) -> Decimal:
    return Decimal(str(value)).quantize(PRICE_STEP, rounding=ROUND_HALF_UP)


def _pick_first(offer: dict, keys: tuple[str, ...]):
    for key in keys:
        value = offer.get(key)
        if value not in (None, ''):
            return value
    return None


def _to_float(value):
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_int(value):
    try:
        if value is None:
            return None
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _normalize_offer(offer: dict) -> dict | None:
    price_raw = _pick_first(
        offer,
        (
            'price',
            'price_rub',
            'price_total',
            'price_total_rub',
            'price_with_markup',
            'cost',
        ),
    )
    qty_raw = _pick_first(
        offer, ('qnt', 'quantity', 'qty', 'balance', 'stock')
    )
    price = _to_float(price_raw)
    qty = _to_int(qty_raw)
    if price is None or qty is None:
        return None
    supplier_name = _pick_first(
        offer,
        (
            'supplier_name',
            'supplier',
            'supplier_title',
            'supplier_company',
            'provider',
            'seller_name',
            'price_name',
            'sup_logo',
        ),
    )
    min_delivery = _to_int(
        _pick_first(
            offer, ('min_delivery_day', 'min_delivery', 'min_delivery_days')
        )
    )
    max_delivery = _to_int(
        _pick_first(
            offer, ('max_delivery_day', 'max_delivery', 'max_delivery_days')
        )
    )
    return {
        'price': price,
        'qty': qty,
        'supplier_name': supplier_name,
        'min_delivery_day': min_delivery,
        'max_delivery_day': max_delivery,
    }


async def _get_site_provider(session) -> Provider:
    provider = await crud_provider.get_provider_or_none(
        SITE_PROVIDER_NAME, session
    )
    if provider:
        return provider
    provider = Provider(name=SITE_PROVIDER_NAME, is_virtual=True)
    session.add(provider)
    await session.flush()
    return provider


async def _get_autopart_id(item, session) -> int | None:
    brand = await brand_crud.get_brand_by_name_or_none(
        item.brand, session
    )
    if not brand:
        return None
    autopart = await crud_autopart.get_autopart_by_oem_brand_or_none(
        item.oem, brand.id, session
    )
    if not autopart:
        return None
    return autopart.id


async def _get_last_site_price(
    session, autopart_id: int, provider_id: int
) -> Decimal | None:
    stmt = (
        select(AutoPartPriceHistory.price)
        .where(
            AutoPartPriceHistory.autopart_id == autopart_id,
            AutoPartPriceHistory.provider_id == provider_id,
        )
        .order_by(
            AutoPartPriceHistory.created_at.desc(),
            AutoPartPriceHistory.id.desc(),
        )
        .limit(1)
    )
    result = await session.execute(stmt)
    return result.scalar_one_or_none()


async def _record_site_price_history(
    session,
    item,
    provider: Provider,
    best_price: float,
    best_qty: int,
    created_at,
):
    autopart_id = await _get_autopart_id(item, session)
    if not autopart_id:
        return
    price = _normalize_price(best_price)
    last_price = await _get_last_site_price(
        session, autopart_id, provider.id
    )
    if last_price is not None:
        if _normalize_price(last_price) == price:
            return
    history = AutoPartPriceHistory(
        autopart_id=autopart_id,
        provider_id=provider.id,
        provider_config_id=None,
        pricelist_id=SITE_PRICELIST_ID,
        created_at=created_at,
        price=price,
        quantity=int(best_qty),
    )
    session.add(history)


def _collect_top_offers(
    offers: list[dict],
    max_price: float | None,
    limit: int = TOP_SITE_OFFERS_LIMIT,
) -> list[dict]:
    normalized: list[dict] = []
    for offer in offers:
        normalized_offer = _normalize_offer(offer)
        if not normalized_offer:
            continue
        if normalized_offer['qty'] <= 0:
            continue
        if max_price is not None and normalized_offer['price'] > max_price:
            continue
        normalized.append(normalized_offer)
    normalized.sort(
        key=lambda item: (item['price'], -item['qty'])
    )
    return normalized[:limit]


def _format_delivery(
    min_delivery: int | None, max_delivery: int | None
) -> str:
    if min_delivery is None and max_delivery is None:
        return '—'
    min_label = '?' if min_delivery is None else str(min_delivery)
    max_label = '?' if max_delivery is None else str(max_delivery)
    return f'{min_label} - {max_label}'


def format_top_offer_lines(
    offers: list[dict], html_mode: bool = False
) -> list[str]:
    lines = []
    for idx, offer in enumerate(offers, start=1):
        supplier = offer.get('supplier_name') or '—'
        if html_mode:
            supplier = html.escape(str(supplier))
        delivery = _format_delivery(
            offer.get('min_delivery_day'), offer.get('max_delivery_day')
        )
        lines.append(
            f'{idx}. {supplier} | Цена {offer["price"]:.2f} | '
            f'Кол-во {offer["qty"]} | Срок {delivery}'
        )
    return lines


async def check_watchlist_site(session):
    key = os.getenv('KEY_FOR_WEBSITE')
    if not key:
        logger.warning(
            'KEY_FOR_WEBSITE not set; skipping watchlist site check'
        )
        return
    watch_items = await crud_price_watch_item.get_all(session)
    if not watch_items:
        return
    now = now_moscow()
    site_provider = None
    async with DZSiteClient(
        base_url=URL_DZ_SEARCH, api_key=key, verify_ssl=False
    ) as client:
        for item in watch_items:
            try:
                offers = await client.get_offers(
                    oem=item.oem, brand=item.brand, without_cross=True
                )
            except Exception as e:
                logger.error(f'DZ search failed for {item.oem}: {e}')
                continue
            if not offers:
                continue

            best_offer = _collect_top_offers(offers, None, limit=1)
            if best_offer:
                try:
                    if site_provider is None:
                        site_provider = await _get_site_provider(session)
                    await _record_site_price_history(
                        session=session,
                        item=item,
                        provider=site_provider,
                        best_price=best_offer[0]['price'],
                        best_qty=best_offer[0]['qty'],
                        created_at=now,
                    )
                except Exception as e:
                    logger.error(
                        f'Failed to save site price history for '
                        f'{item.oem}: {e}'
                    )

            top_offers = _collect_top_offers(
                offers,
                item.max_price,
                limit=TOP_SITE_OFFERS_LIMIT,
            )
            if not top_offers:
                continue
            best_price = top_offers[0]['price']
            best_qty = top_offers[0]['qty']

            item.last_seen_site_at = now
            item.last_seen_site_price = best_price
            item.last_seen_site_qty = best_qty
            if _notify_immediately():
                should_notify = (
                    not item.last_notified_site_at
                    or item.last_notified_site_at.date() != now.date()
                )
                if should_notify:
                    message_lines = [
                        'Позиция найдена на сайте:',
                        f'{_norm(item.brand)} {_norm(item.oem)}',
                        f'Топ {TOP_SITE_OFFERS_LIMIT} предложения:',
                    ]
                    message_lines.extend(
                        format_top_offer_lines(top_offers)
                    )
                    message = '\n'.join(message_lines)
                    try:
                        await create_admin_notifications(
                            session=session,
                            title='Watchlist: позиция найдена на сайте',
                            message=message,
                            level=AppNotificationLevel.INFO,
                            link='/watchlist',
                            commit=False,
                        )
                        item.last_notified_site_at = now
                    except Exception as e:
                        logger.error(
                            'Failed to create site watch app notification: %s',
                            e,
                        )
            session.add(item)
        await session.commit()
