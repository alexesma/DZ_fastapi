import asyncio
import html
import logging
import os
from datetime import datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.core.constants import URL_DZ_SEARCH
from dz_fastapi.core.time import now_moscow
from dz_fastapi.crud.watchlist import crud_price_watch_item
from dz_fastapi.http.dz_site_client import DZSiteClient
from dz_fastapi.models.partner import Client, Provider, ProviderPriceListConfig
from dz_fastapi.services.email import send_email_message
from dz_fastapi.services.telegram import send_message_to_telegram
from dz_fastapi.services.watchlist_site import (TOP_SITE_OFFERS_LIMIT,
                                                _collect_top_offers,
                                                format_top_offer_lines)

logger = logging.getLogger('dz_fastapi')
SITE_ITEM_SEPARATOR = '--------------------'
WATCHLIST_EMAIL_SUBJECT = 'Отчет по отслеживаемым позициям'


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
    if provider.is_own_price:
        return
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
                    f'Позиция найдена в прайсе: '
                    f'{brand} {oem} | '
                    f'Цена {price} | '
                    f'Поставщик {provider.name}'
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

    provider_items = [
        item
        for item in watch_items
        if _should_notify(
            item.last_seen_provider_at, item.last_notified_provider_at
        )
    ]
    site_items = [
        item
        for item in watch_items
        if _should_notify(item.last_seen_site_at, item.last_notified_site_at)
    ]

    if not provider_items and not site_items:
        return

    provider_ids = {
        item.last_seen_provider_id
        for item in provider_items
        if item.last_seen_provider_id
    }
    provider_map: dict[int, str] = {}
    if provider_ids:
        rows = await session.execute(
            select(Client.id, Client.name).where(Client.id.in_(provider_ids))
        )
        provider_map = {row[0]: row[1] for row in rows.all()}

    provider_by_id = {item.id: item for item in provider_items}
    site_by_id = {item.id: item for item in site_items}
    if site_items:
        site_offer_map: dict[int, list[dict]] = {}
        key = os.getenv('KEY_FOR_WEBSITE')
        if key:
            async with DZSiteClient(
                base_url=URL_DZ_SEARCH, api_key=key, verify_ssl=False
            ) as client:
                for item in site_items:
                    try:
                        offers = await client.get_offers(
                            oem=item.oem, brand=item.brand, without_cross=True
                        )
                    except Exception as e:
                        logger.error(
                            f'DZ search failed for {item.oem}: {e}'
                        )
                        continue
                    if not offers:
                        continue
                    top_offers = _collect_top_offers(
                        offers,
                        item.max_price,
                        limit=TOP_SITE_OFFERS_LIMIT,
                    )
                    if top_offers:
                        site_offer_map[item.id] = top_offers

    notify_items = [
        item
        for item in watch_items
        if item.id in provider_by_id or item.id in site_by_id
    ]
    lines: list[str] = ['<b>Отслеживаемые позиции:</b>']
    for idx, item in enumerate(notify_items):
        if idx > 0:
            lines.append('')
            lines.append(SITE_ITEM_SEPARATOR)
            lines.append('')
        lines.append(
            f'<b>{html.escape(_norm(item.brand))} '
            f'{html.escape(_norm(item.oem))}</b>'
        )

        provider_item = provider_by_id.get(item.id)
        if provider_item:
            provider_label = provider_map.get(
                provider_item.last_seen_provider_id, ''
            ) or str(provider_item.last_seen_provider_id)
            lines.append(
                f'  <b>Прайс:</b> Цена '
                f'{provider_item.last_seen_provider_price} | '
                f'Поставщик {html.escape(str(provider_label))}'
            )

        site_item = site_by_id.get(item.id)
        if site_item:
            lines.append('  <b>Сайт:</b>')
            top_offers = site_offer_map.get(item.id)
            if top_offers:
                for line in format_top_offer_lines(
                    top_offers, html_mode=True
                ):
                    lines.append(f'    {line}')
            else:
                qty = site_item.last_seen_site_qty
                qty_part = f' | Кол-во {qty}' if qty is not None else ''
                lines.append(
                    f'    Цена {site_item.last_seen_site_price}{qty_part}'
                )

    message_html = '\n'.join(lines)
    delivered = False
    try:
        await send_message_to_telegram(message_html, parse_mode='HTML')
        delivered = True
    except Exception as e:
        logger.error(f'Failed to send watchlist telegram: {e}')

    analytics_email = os.getenv('EMAIL_NAME_ANALYTIC')
    if analytics_email:
        try:
            email_sent = await asyncio.to_thread(
                send_email_message,
                to_email=analytics_email,
                subject=WATCHLIST_EMAIL_SUBJECT,
                body=message_html,
                is_html=True,
            )
            if email_sent:
                delivered = True
            else:
                logger.error(
                    'Failed to send watchlist email report to %s',
                    analytics_email,
                )
        except Exception as e:
            logger.error(f'Failed to send watchlist email report: {e}')
    else:
        logger.warning(
            'EMAIL_NAME_ANALYTIC not set; watchlist email copy skipped'
        )

    if not delivered:
        return

    for item in provider_items:
        item.last_notified_provider_at = now
        session.add(item)
    for item in site_items:
        item.last_notified_site_at = now
        session.add(item)

    await session.commit()
