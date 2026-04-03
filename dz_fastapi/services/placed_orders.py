from __future__ import annotations

import logging
import os
from datetime import date, datetime, timedelta
from typing import Any, Optional

from sqlalchemy import delete, literal, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased

from dz_fastapi.core.constants import URL_DZ_SEARCH
from dz_fastapi.core.time import now_moscow
from dz_fastapi.http.dz_site_client import DZSiteClient
from dz_fastapi.models.partner import (ORDER_TRACKING_SOURCE,
                                       SUPPLIER_ORDER_STATUS,
                                       TYPE_ORDER_ITEM_STATUS,
                                       TYPE_STATUS_ORDER, Customer, Order,
                                       OrderItem, Provider, SupplierOrder,
                                       SupplierOrderItem)
from dz_fastapi.models.user import User
from dz_fastapi.services.order_status_mapping import (
    EXTERNAL_STATUS_SOURCE_DRAGONZAP, apply_status_mapping_to_order_item,
    build_external_status_normalized, build_external_status_raw,
    get_active_status_mappings, record_unmapped_external_status,
    resolve_internal_order_status, select_best_mapping)

logger = logging.getLogger('dz_fastapi')

TRACKING_HISTORY_DAYS = 365
SITE_TERMINAL_STATUSES = {
    TYPE_STATUS_ORDER.SHIPPED,
    TYPE_STATUS_ORDER.REFUSAL,
    TYPE_STATUS_ORDER.RETURNED,
    TYPE_STATUS_ORDER.REMOVED,
    TYPE_STATUS_ORDER.ERROR,
}
SITE_AUTO_RECEIVED_STATUSES = {
    TYPE_STATUS_ORDER.ARRIVED,
    TYPE_STATUS_ORDER.SHIPPED,
}
SITE_STATUS_SYNC_LIMIT = int(
    os.getenv('TRACKING_SITE_SYNC_LIMIT', '200')
)
SITE_API_KEY = os.getenv('KEY_FOR_WEBSITE')


def tracking_history_cutoff(days: int = TRACKING_HISTORY_DAYS) -> datetime:
    return now_moscow() - timedelta(days=days)


def _normalize_oem(value: Optional[str]) -> Optional[str]:
    normalized = str(value or '').strip().upper()
    return normalized or None


def _normalize_brand(value: Optional[str]) -> Optional[str]:
    normalized = str(value or '').strip()
    return normalized or None


def _actual_lead_days(
    created_at: Optional[datetime], received_at: Optional[datetime]
) -> Optional[int]:
    if not created_at or not received_at:
        return None
    delta = received_at - created_at
    return max(delta.days, 0)


def _resolve_site_row_status(
    order_status: Optional[TYPE_STATUS_ORDER],
    item_status: Optional[TYPE_ORDER_ITEM_STATUS],
) -> str:
    if order_status is not None:
        return order_status.name
    if item_status in {
        TYPE_ORDER_ITEM_STATUS.DELIVERED,
        TYPE_ORDER_ITEM_STATUS.CANCELLED,
        TYPE_ORDER_ITEM_STATUS.FAILED,
        TYPE_ORDER_ITEM_STATUS.ERROR,
    }:
        return item_status.name
    if item_status is not None:
        return item_status.name
    return 'UNKNOWN'


def _has_tracking_identity(
    oem_number: Optional[str],
    brand_name: Optional[str],
    autopart_name: Optional[str],
) -> bool:
    return any(
        str(value or '').strip()
        for value in (oem_number, brand_name, autopart_name)
    )


def _extract_site_items(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if not isinstance(payload, dict):
        return []

    data = payload.get('data')
    if isinstance(data, list):
        return [item for item in data if isinstance(item, dict)]
    if isinstance(data, dict):
        for key in ('items', 'results', 'records'):
            value = data.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]

    for key in ('items', 'results', 'records'):
        value = payload.get(key)
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]
    return []


def _safe_int(value: Any) -> Optional[int]:
    if value in (None, ''):
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _extract_received_quantity_from_site(
    item: dict[str, Any],
    *,
    ordered_quantity: int,
    mapped_status: TYPE_STATUS_ORDER,
) -> Optional[int]:
    sys_info = item.get('sys_info')
    sources = (
        item,
        sys_info if isinstance(sys_info, dict) else None,
    )
    for source in sources:
        if not isinstance(source, dict):
            continue
        for key in (
            'received_quantity',
            'received_qty',
            'delivered_quantity',
            'delivered_qty',
            'issued_quantity',
            'issued_qty',
            'shipped_quantity',
            'shipped_qty',
            'fact_quantity',
            'fact_qnt',
        ):
            parsed = _safe_int(source.get(key))
            if parsed is not None:
                return parsed
    if mapped_status in SITE_AUTO_RECEIVED_STATUSES:
        return ordered_quantity
    return None


async def _apply_site_sync_payload(
    session: AsyncSession,
    order: Order,
    item: OrderItem,
    site_item: dict[str, Any],
    mappings: list,
) -> bool:
    changed = False
    raw_status = build_external_status_raw(site_item)
    normalized_status = build_external_status_normalized(site_item)
    next_mapping_id = None
    mapping = None

    if item.external_status_source != EXTERNAL_STATUS_SOURCE_DRAGONZAP:
        item.external_status_source = EXTERNAL_STATUS_SOURCE_DRAGONZAP
        changed = True
    if item.external_status_raw != raw_status:
        item.external_status_raw = raw_status
        changed = True
    if item.external_status_normalized != (normalized_status or None):
        item.external_status_normalized = normalized_status or None
        changed = True

    if normalized_status:
        mapping = select_best_mapping(
            mappings,
            normalized_status=normalized_status,
            provider_id=order.provider_id,
        )

    if mapping is not None:
        next_mapping_id = mapping.id
        if apply_status_mapping_to_order_item(
            order=order,
            item=item,
            mapping=mapping,
        ):
            changed = True
    else:
        if item.external_status_mapping_id is not None:
            item.external_status_mapping_id = None
            changed = True
        if normalized_status and raw_status:
            await record_unmapped_external_status(
                session,
                source_key=EXTERNAL_STATUS_SOURCE_DRAGONZAP,
                provider_id=order.provider_id,
                raw_status=raw_status,
                normalized_status=normalized_status,
                sample_order_id=order.id,
                sample_item_id=item.id,
                sample_payload=site_item,
            )
            changed = True

    if item.external_status_mapping_id != next_mapping_id:
        item.external_status_mapping_id = next_mapping_id
        changed = True

    mapped_order_status = (
        resolve_internal_order_status(mapping.internal_order_status)
        if mapping is not None
        else None
    )
    if mapped_order_status:
        received_quantity = _extract_received_quantity_from_site(
            site_item,
            ordered_quantity=item.quantity,
            mapped_status=mapped_order_status,
        )
        if received_quantity is not None:
            next_qty, next_received_at = _set_received_metadata(
                received_quantity=received_quantity,
                received_at=item.received_at,
            )
            if item.received_quantity != next_qty:
                item.received_quantity = next_qty
                changed = True
            if item.received_at != next_received_at:
                item.received_at = next_received_at
                changed = True

    if changed or item.external_status_synced_at is None:
        item.external_status_synced_at = now_moscow()
        changed = True

    return changed


async def sync_site_tracking_statuses(
    session: AsyncSession,
    *,
    oem_number: Optional[str] = None,
    brand_name: Optional[str] = None,
    provider_id: Optional[int] = None,
    customer_id: Optional[int] = None,
    limit: int = SITE_STATUS_SYNC_LIMIT,
) -> dict[str, int]:
    if not SITE_API_KEY:
        logger.debug(
            'Skip tracking status sync: KEY_FOR_WEBSITE is not configured'
        )
        return {
            'checked': 0,
            'updated': 0,
            'not_found': 0,
            'errors': 0,
        }

    normalized_oem = _normalize_oem(oem_number)
    normalized_brand = _normalize_brand(brand_name)
    stmt = (
        select(OrderItem, Order)
        .join(Order, Order.id == OrderItem.order_id)
        .where(
            Order.source_type
            == ORDER_TRACKING_SOURCE.DRAGONZAP_SEARCH.value,
            Order.created_at >= tracking_history_cutoff(),
            OrderItem.tracking_uuid.is_not(None),
        )
        .order_by(
            Order.created_at.desc(),
            Order.id.desc(),
            OrderItem.id.desc(),
        )
    )
    if normalized_oem:
        stmt = stmt.where(OrderItem.oem_number == normalized_oem)
    if normalized_brand:
        stmt = stmt.where(OrderItem.brand_name.ilike(normalized_brand))
    if provider_id is not None:
        stmt = stmt.where(Order.provider_id == provider_id)
    if customer_id is not None:
        stmt = stmt.where(Order.customer_id == customer_id)

    rows = (await session.execute(stmt)).all()
    candidates: list[tuple[OrderItem, Order]] = []
    for item, order in rows:
        if not _has_tracking_identity(
            item.oem_number,
            item.brand_name,
            item.autopart_name,
        ):
            continue
        if (
            order.status in SITE_TERMINAL_STATUSES
            and item.received_quantity is not None
        ):
            continue
        candidates.append((item, order))
        if len(candidates) >= limit:
            break

    if not candidates:
        return {
            'checked': 0,
            'updated': 0,
            'not_found': 0,
            'errors': 0,
        }

    checked = 0
    updated = 0
    not_found = 0
    errors = 0
    mappings_cache: dict[Optional[int], list] = {}

    async with DZSiteClient(
        base_url=URL_DZ_SEARCH,
        api_key=SITE_API_KEY,
        verify_ssl=False,
    ) as site_client:
        for item, order in candidates:
            checked += 1
            try:
                payload = await site_client.get_order_items(
                    api_key=SITE_API_KEY,
                    page=1,
                    per_page=10,
                    search_comment_eq=item.tracking_uuid,
                )
                remote_items = _extract_site_items(payload)
                remote_item = next(
                    (
                        remote
                        for remote in remote_items
                        if str(remote.get('comment') or '').strip()
                        == item.tracking_uuid
                    ),
                    remote_items[0] if len(remote_items) == 1 else None,
                )
                if remote_item is None:
                    not_found += 1
                    continue
                provider_mappings = mappings_cache.get(order.provider_id)
                if provider_mappings is None:
                    provider_mappings = await get_active_status_mappings(
                        session,
                        source_key=EXTERNAL_STATUS_SOURCE_DRAGONZAP,
                        provider_id=order.provider_id,
                    )
                    mappings_cache[order.provider_id] = provider_mappings
                if await _apply_site_sync_payload(
                    session,
                    order,
                    item,
                    remote_item,
                    provider_mappings,
                ):
                    updated += 1
            except Exception:
                errors += 1
                logger.exception(
                    'Failed to sync Dragonzap tracking status '
                    'for tracking_uuid=%s',
                    item.tracking_uuid,
                )

    if updated:
        await session.commit()
    else:
        await session.rollback()

    return {
        'checked': checked,
        'updated': updated,
        'not_found': not_found,
        'errors': errors,
    }


async def list_tracking_history(
    session: AsyncSession,
    *,
    oem_number: Optional[str] = None,
    brand_name: Optional[str] = None,
    provider_id: Optional[int] = None,
    customer_id: Optional[int] = None,
    status: Optional[str] = None,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
    limit: int = 300,
    sync_site: bool = False,
) -> list[dict[str, Any]]:
    if sync_site:
        await sync_site_tracking_statuses(
            session,
            oem_number=oem_number,
            brand_name=brand_name,
            provider_id=provider_id,
            customer_id=customer_id,
            limit=min(limit, SITE_STATUS_SYNC_LIMIT),
        )

    normalized_oem = _normalize_oem(oem_number)
    normalized_brand = _normalize_brand(brand_name)
    status_filter = str(status or '').strip().upper() or None
    provider_alias = aliased(Provider, flat=True)
    customer_alias = aliased(Customer, flat=True)

    range_start = datetime.combine(
        date_from or tracking_history_cutoff().date(),
        datetime.min.time(),
        tzinfo=now_moscow().tzinfo,
    )
    range_end = datetime.combine(
        date_to or now_moscow().date(),
        datetime.max.time(),
        tzinfo=now_moscow().tzinfo,
    )

    supplier_stmt = (
        select(
            literal('supplier').label('source_type'),
            literal('Прайсы поставщиков').label('source_label'),
            SupplierOrder.id.label('order_id'),
            SupplierOrderItem.id.label('item_id'),
            SupplierOrder.provider_id.label('provider_id'),
            provider_alias.name.label('provider_name'),
            literal(None).label('customer_id'),
            literal(None).label('customer_name'),
            SupplierOrder.created_by_user_id.label('ordered_by_user_id'),
            User.email.label('ordered_by_email'),
            SupplierOrderItem.oem_number.label('oem_number'),
            SupplierOrderItem.brand_name.label('brand_name'),
            SupplierOrderItem.autopart_name.label('autopart_name'),
            SupplierOrderItem.quantity.label('ordered_quantity'),
            SupplierOrderItem.received_quantity.label('received_quantity'),
            SupplierOrderItem.price.label('price'),
            SupplierOrderItem.min_delivery_day.label('min_delivery_day'),
            SupplierOrderItem.max_delivery_day.label('max_delivery_day'),
            SupplierOrder.created_at.label('created_at'),
            SupplierOrderItem.received_at.label('received_at'),
            SupplierOrder.status.label('order_status'),
        )
        .join(
            SupplierOrderItem,
            SupplierOrderItem.supplier_order_id == SupplierOrder.id,
        )
        .join(provider_alias, provider_alias.id == SupplierOrder.provider_id)
        .outerjoin(User, User.id == SupplierOrder.created_by_user_id)
        .where(
            SupplierOrder.source_type
            == ORDER_TRACKING_SOURCE.SEARCH_OFFERS.value,
            SupplierOrder.created_at >= range_start,
            SupplierOrder.created_at <= range_end,
        )
    )
    if normalized_oem:
        supplier_stmt = supplier_stmt.where(
            SupplierOrderItem.oem_number == normalized_oem
        )
    if normalized_brand:
        supplier_stmt = supplier_stmt.where(
            SupplierOrderItem.brand_name.ilike(normalized_brand)
        )
    if provider_id is not None:
        supplier_stmt = supplier_stmt.where(
            SupplierOrder.provider_id == provider_id
        )

    site_stmt = (
        select(
            literal('site').label('source_type'),
            literal('Dragonzap').label('source_label'),
            Order.id.label('order_id'),
            OrderItem.id.label('item_id'),
            Order.provider_id.label('provider_id'),
            provider_alias.name.label('provider_name'),
            Order.customer_id.label('customer_id'),
            customer_alias.name.label('customer_name'),
            Order.created_by_user_id.label('ordered_by_user_id'),
            User.email.label('ordered_by_email'),
            OrderItem.oem_number.label('oem_number'),
            OrderItem.brand_name.label('brand_name'),
            OrderItem.autopart_name.label('autopart_name'),
            OrderItem.quantity.label('ordered_quantity'),
            OrderItem.received_quantity.label('received_quantity'),
            OrderItem.price.label('price'),
            OrderItem.min_delivery_day.label('min_delivery_day'),
            OrderItem.max_delivery_day.label('max_delivery_day'),
            Order.created_at.label('created_at'),
            OrderItem.received_at.label('received_at'),
            Order.status.label('order_status'),
            OrderItem.status.label('item_status'),
            OrderItem.external_status_source.label('external_status_source'),
            OrderItem.external_status_raw.label('external_status_raw'),
            OrderItem.external_status_mapping_id.label(
                'external_status_mapping_id'
            ),
        )
        .join(OrderItem, OrderItem.order_id == Order.id)
        .join(provider_alias, provider_alias.id == Order.provider_id)
        .join(customer_alias, customer_alias.id == Order.customer_id)
        .outerjoin(User, User.id == Order.created_by_user_id)
        .where(
            Order.source_type
            == ORDER_TRACKING_SOURCE.DRAGONZAP_SEARCH.value,
            Order.created_at >= range_start,
            Order.created_at <= range_end,
        )
    )
    if normalized_oem:
        site_stmt = site_stmt.where(OrderItem.oem_number == normalized_oem)
    if normalized_brand:
        site_stmt = site_stmt.where(
            OrderItem.brand_name.ilike(normalized_brand)
        )
    if provider_id is not None:
        site_stmt = site_stmt.where(Order.provider_id == provider_id)
    if customer_id is not None:
        site_stmt = site_stmt.where(Order.customer_id == customer_id)

    supplier_rows = (await session.execute(supplier_stmt)).all()
    site_rows = (await session.execute(site_stmt)).all()

    results: list[dict[str, Any]] = []
    for row in supplier_rows:
        if not _has_tracking_identity(
            row.oem_number,
            row.brand_name,
            row.autopart_name,
        ):
            continue
        current_status = (
            row.order_status.name if row.order_status else 'UNKNOWN'
        )
        if status_filter and current_status != status_filter:
            continue
        results.append(
            {
                'source_type': row.source_type,
                'source_label': row.source_label,
                'order_id': row.order_id,
                'item_id': row.item_id,
                'provider_id': row.provider_id,
                'provider_name': row.provider_name,
                'customer_id': row.customer_id,
                'customer_name': row.customer_name,
                'ordered_by_user_id': row.ordered_by_user_id,
                'ordered_by_email': row.ordered_by_email,
                'oem_number': row.oem_number,
                'brand_name': row.brand_name,
                'autopart_name': row.autopart_name,
                'ordered_quantity': row.ordered_quantity,
                'received_quantity': row.received_quantity,
                'price': row.price,
                'min_delivery_day': row.min_delivery_day,
                'max_delivery_day': row.max_delivery_day,
                'created_at': row.created_at,
                'received_at': row.received_at,
                'current_status': current_status,
                'order_status': current_status,
                'item_status': None,
                'external_status_source': None,
                'external_status_raw': None,
                'needs_status_mapping': False,
                'actual_lead_days': _actual_lead_days(
                    row.created_at, row.received_at
                ),
                'link': f'/customer-orders/suppliers/{row.order_id}',
            }
        )

    for row in site_rows:
        if not _has_tracking_identity(
            row.oem_number,
            row.brand_name,
            row.autopart_name,
        ):
            continue
        current_status = _resolve_site_row_status(
            row.order_status, row.item_status
        )
        if status_filter and current_status != status_filter:
            continue
        results.append(
            {
                'source_type': row.source_type,
                'source_label': row.source_label,
                'order_id': row.order_id,
                'item_id': row.item_id,
                'provider_id': row.provider_id,
                'provider_name': row.provider_name,
                'customer_id': row.customer_id,
                'customer_name': row.customer_name,
                'ordered_by_user_id': row.ordered_by_user_id,
                'ordered_by_email': row.ordered_by_email,
                'oem_number': row.oem_number,
                'brand_name': row.brand_name,
                'autopart_name': row.autopart_name,
                'ordered_quantity': row.ordered_quantity,
                'received_quantity': row.received_quantity,
                'price': row.price,
                'min_delivery_day': row.min_delivery_day,
                'max_delivery_day': row.max_delivery_day,
                'created_at': row.created_at,
                'received_at': row.received_at,
                'current_status': current_status,
                'order_status': (
                    row.order_status.name if row.order_status else None
                ),
                'item_status': (
                    row.item_status.name if row.item_status else None
                ),
                'external_status_source': row.external_status_source,
                'external_status_raw': row.external_status_raw,
                'needs_status_mapping': bool(
                    row.external_status_source
                    and row.external_status_raw
                    and row.external_status_mapping_id is None
                ),
                'actual_lead_days': _actual_lead_days(
                    row.created_at, row.received_at
                ),
                'link': '/orders/tracking',
            }
        )

    results.sort(
        key=lambda item: (
            item['created_at'],
            item['order_id'],
            item['item_id'],
        ),
        reverse=True,
    )
    return results[:limit]


def _set_received_metadata(
    *,
    received_quantity: Optional[int],
    received_at: Optional[datetime],
) -> tuple[Optional[int], Optional[datetime]]:
    if received_quantity is None:
        return None, received_at
    if received_quantity > 0:
        return received_quantity, received_at or now_moscow()
    return received_quantity, None


async def update_tracking_item(
    session: AsyncSession,
    *,
    source_type: str,
    item_id: int,
    status: Optional[str] = None,
    received_quantity: Optional[int] = None,
) -> dict[str, Any]:
    source_key = str(source_type or '').strip().lower()
    status_key = str(status or '').strip().upper() or None

    if source_key == 'supplier':
        stmt = (
            select(SupplierOrderItem, SupplierOrder)
            .join(
                SupplierOrder,
                SupplierOrder.id == SupplierOrderItem.supplier_order_id,
            )
            .where(
                SupplierOrderItem.id == item_id,
                SupplierOrder.source_type
                == ORDER_TRACKING_SOURCE.SEARCH_OFFERS.value,
            )
        )
        item, order = (
            (await session.execute(stmt)).one_or_none() or (None, None)
        )
        if item is None or order is None:
            raise ValueError('Tracking item not found')
        if status_key:
            order.status = SUPPLIER_ORDER_STATUS[status_key]
        if received_quantity is not None:
            item.received_quantity, item.received_at = _set_received_metadata(
                received_quantity=received_quantity,
                received_at=item.received_at,
            )
        await session.commit()
        return {
            'source_type': 'supplier',
            'item_id': item.id,
            'order_id': order.id,
            'status': order.status.name,
            'received_quantity': item.received_quantity,
            'received_at': item.received_at,
        }

    if source_key == 'site':
        stmt = (
            select(OrderItem, Order)
            .join(Order, Order.id == OrderItem.order_id)
            .where(
                OrderItem.id == item_id,
                Order.source_type
                == ORDER_TRACKING_SOURCE.DRAGONZAP_SEARCH.value,
            )
        )
        item, order = (
            (await session.execute(stmt)).one_or_none() or (None, None)
        )
        if item is None or order is None:
            raise ValueError('Tracking item not found')
        if status_key:
            order.status = TYPE_STATUS_ORDER[status_key]
            if status_key in {'ARRIVED', 'SHIPPED'}:
                item.status = TYPE_ORDER_ITEM_STATUS.DELIVERED
            elif status_key in {'ERROR', 'REFUSAL'}:
                item.status = TYPE_ORDER_ITEM_STATUS.ERROR
        if received_quantity is not None:
            item.received_quantity, item.received_at = _set_received_metadata(
                received_quantity=received_quantity,
                received_at=item.received_at,
            )
        await session.commit()
        return {
            'source_type': 'site',
            'item_id': item.id,
            'order_id': order.id,
            'status': (
                order.status.name if order.status else 'UNKNOWN'
            ),
            'received_quantity': item.received_quantity,
            'received_at': item.received_at,
        }

    raise ValueError('Unsupported source type')


async def cleanup_old_tracking_history(
    session: AsyncSession,
    *,
    older_than_days: int = TRACKING_HISTORY_DAYS,
) -> dict[str, int]:
    cutoff = tracking_history_cutoff(older_than_days)

    supplier_ids = (
        select(SupplierOrder.id)
        .where(
            SupplierOrder.source_type
            == ORDER_TRACKING_SOURCE.SEARCH_OFFERS.value,
            SupplierOrder.created_at < cutoff,
        )
    )
    order_ids = (
        select(Order.id)
        .where(
            Order.source_type
            == ORDER_TRACKING_SOURCE.DRAGONZAP_SEARCH.value,
            Order.created_at < cutoff,
        )
    )

    supplier_items_deleted = (
        await session.execute(
            delete(SupplierOrderItem).where(
                SupplierOrderItem.supplier_order_id.in_(supplier_ids)
            )
        )
    ).rowcount or 0
    supplier_orders_deleted = (
        await session.execute(
            delete(SupplierOrder).where(SupplierOrder.id.in_(supplier_ids))
        )
    ).rowcount or 0
    order_items_deleted = (
        await session.execute(
            delete(OrderItem).where(OrderItem.order_id.in_(order_ids))
        )
    ).rowcount or 0
    orders_deleted = (
        await session.execute(delete(Order).where(Order.id.in_(order_ids)))
    ).rowcount or 0
    await session.commit()
    return {
        'supplier_items_deleted': supplier_items_deleted,
        'supplier_orders_deleted': supplier_orders_deleted,
        'order_items_deleted': order_items_deleted,
        'orders_deleted': orders_deleted,
    }
