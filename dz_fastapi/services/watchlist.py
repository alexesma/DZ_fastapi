import logging
import os
from datetime import datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.core.time import now_moscow
from dz_fastapi.crud.watchlist import crud_price_watch_item
from dz_fastapi.models.partner import Client, Provider, ProviderPriceListConfig
from dz_fastapi.services.telegram import send_message_to_telegram

logger = logging.getLogger('dz_fastapi')


def _notify_immediately() -> bool:
    return os.getenv('WATCHLIST_NOTIFY_MODE', 'immediate').lower() != 'daily'


def _norm(value: str) -> str:
    return str(value or '').strip().upper()


async def handle_provider_pricelist_watch(
    session: AsyncSession,
    provider: Provider,
    provider_config: ProviderPriceListConfig,
    pricelist_id: int,
    items: list[dict],
):
    watch_items = await crud_price_watch_item.get_all(session)
    if not watch_items:
        return

    watch_map = {
        (_norm(item.brand), _norm(item.oem)): item for item in watch_items
    }
    now = now_moscow()
    for row in items:
        brand = _norm(row.get('brand'))
        oem = _norm(row.get('oem_number'))
        key = (brand, oem)
        item = watch_map.get(key)
        if not item:
            continue
        try:
            price = float(row.get('price', 0))
            quantity = int(row.get('quantity', 0))
        except Exception:
            continue
        if quantity <= 0:
            continue
        if item.max_price is not None and price > item.max_price:
            continue

        item.last_seen_provider_at = now
        item.last_seen_provider_price = price
        item.last_seen_provider_id = provider.id
        item.last_seen_provider_config_id = provider_config.id
        item.last_seen_provider_pricelist_id = pricelist_id
        if _notify_immediately():
            should_notify = (
                not item.last_notified_provider_at
                or item.last_notified_provider_at.date() != now.date()
            )
            if should_notify:
                message = (
                    f'Позиция найдена в прайсе: {brand} {oem} | '
                    f'Цена {price} | Поставщик {provider.name}'
                )
                try:
                    await send_message_to_telegram(message)
                    item.last_notified_provider_at = now
                except Exception as e:
                    logger.error(f'Failed to send watchlist telegram: {e}')
        session.add(item)
    await session.commit()


def _should_notify(
    last_seen: datetime | None, last_notified: datetime | None
) -> bool:
    if not last_seen:
        return False
    if not last_notified:
        return True
    return last_notified < last_seen


async def send_watchlist_daily_notifications(session: AsyncSession):
    watch_items = await crud_price_watch_item.get_all(session)
    if not watch_items:
        return
    now = now_moscow()

    provider_ids = {
        item.last_seen_provider_id
        for item in watch_items
        if item.last_seen_provider_id
    }
    provider_map: dict[int, str] = {}
    if provider_ids:
        rows = await session.execute(
            select(Client.id, Client.name).where(Client.id.in_(provider_ids))
        )
        provider_map = {row[0]: row[1] for row in rows.all()}

    for item in watch_items:
        if _should_notify(
            item.last_seen_provider_at, item.last_notified_provider_at
        ):
            provider_label = provider_map.get(
                item.last_seen_provider_id, ''
            ) or str(item.last_seen_provider_id)
            message = (
                f'Позиция найдена в прайсе: '
                f'{_norm(item.brand)} {_norm(item.oem)} | '
                f'Цена {item.last_seen_provider_price} | '
                f'Поставщик {provider_label}'
            )
            try:
                await send_message_to_telegram(message)
                item.last_notified_provider_at = now
            except Exception as e:
                logger.error(f'Failed to send watchlist telegram: {e}')

        if _should_notify(
            item.last_seen_site_at, item.last_notified_site_at
        ):
            message = (
                f'Позиция найдена на сайте: '
                f'{_norm(item.brand)} {_norm(item.oem)} | '
                f'Цена {item.last_seen_site_price}'
            )
            try:
                await send_message_to_telegram(message)
                item.last_notified_site_at = now
            except Exception as e:
                logger.error(f'Failed to send site watch telegram: {e}')

        session.add(item)
    await session.commit()
