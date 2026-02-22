import logging
from datetime import datetime, timezone

from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.crud.watchlist import crud_price_watch_item
from dz_fastapi.models.partner import Provider, ProviderPriceListConfig
from dz_fastapi.services.telegram import send_message_to_telegram

logger = logging.getLogger('dz_fastapi')


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
    now = datetime.now(timezone.utc)
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

        should_notify = (
            not item.last_notified_provider_at
            or item.last_notified_provider_at.date() != now.date()
        )
        item.last_seen_provider_at = now
        item.last_seen_provider_price = price
        item.last_seen_provider_id = provider.id
        item.last_seen_provider_config_id = provider_config.id
        item.last_seen_provider_pricelist_id = pricelist_id
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
