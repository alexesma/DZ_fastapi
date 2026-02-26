import logging
import os

from dz_fastapi.core.constants import URL_DZ_SEARCH
from dz_fastapi.core.time import now_moscow
from dz_fastapi.crud.watchlist import crud_price_watch_item
from dz_fastapi.http.dz_site_client import DZSiteClient
from dz_fastapi.services.telegram import send_message_to_telegram

logger = logging.getLogger('dz_fastapi')


def _notify_immediately() -> bool:
    return os.getenv('WATCHLIST_NOTIFY_MODE', 'immediate').lower() != 'daily'


def _norm(value: str) -> str:
    return str(value or '').strip().upper()


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


def _collect_top_offers(
    offers: list[dict], max_price: float | None, limit: int = 5
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


def format_top_offer_lines(offers: list[dict]) -> list[str]:
    lines = []
    for idx, offer in enumerate(offers, start=1):
        supplier = offer.get('supplier_name') or '—'
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

            top_offers = _collect_top_offers(
                offers, item.max_price, limit=5
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
                        f'Позиция найдена на сайте: '
                        f'{_norm(item.brand)} {_norm(item.oem)}',
                        'Топ 5 предложений:',
                    ]
                    message_lines.extend(format_top_offer_lines(top_offers))
                    message = '\n'.join(message_lines)
                    try:
                        await send_message_to_telegram(message)
                        item.last_notified_site_at = now
                    except Exception as e:
                        logger.error(
                            f'Failed to send site watch telegram: {e}'
                        )
            session.add(item)
        await session.commit()
