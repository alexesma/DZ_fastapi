import logging
import os
from datetime import datetime, timezone

from dz_fastapi.core.constants import URL_DZ_SEARCH
from dz_fastapi.crud.watchlist import crud_price_watch_item
from dz_fastapi.http.dz_site_client import DZSiteClient
from dz_fastapi.services.telegram import send_message_to_telegram

logger = logging.getLogger('dz_fastapi')
KEY = os.getenv('KEY_FOR_WEBSITE')


def _norm(value: str) -> str:
    return str(value or '').strip().upper()


async def check_watchlist_site(session):
    watch_items = await crud_price_watch_item.get_all(session)
    if not watch_items:
        return
    now = datetime.now(timezone.utc)
    async with DZSiteClient(
        base_url=URL_DZ_SEARCH, api_key=KEY, verify_ssl=False
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

            best = None
            for offer in offers:
                try:
                    price = float(offer.get('cost', 0))
                    qty = int(offer.get('qnt', 0))
                except Exception:
                    continue
                if qty <= 0:
                    continue
                if item.max_price is not None and price > item.max_price:
                    continue
                if best is None or price < best:
                    best = price
            if best is None:
                continue

            should_notify = (
                not item.last_notified_site_at
                or item.last_notified_site_at.date() != now.date()
            )
            item.last_seen_site_at = now
            item.last_seen_site_price = best
            if should_notify:
                message = (
                    f'Позиция найдена на сайте: '
                    f'{_norm(item.brand)} {_norm(item.oem)} | '
                    f'Цена {best}'
                )
                try:
                    await send_message_to_telegram(message)
                    item.last_notified_site_at = now
                except Exception as e:
                    logger.error(f'Failed to send site watch telegram: {e}')
            session.add(item)
        await session.commit()
