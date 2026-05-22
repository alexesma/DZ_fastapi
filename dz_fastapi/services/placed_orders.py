from __future__ import annotations

import logging
import os
from datetime import date, datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal
from typing import Any, Optional

from sqlalchemy import delete, func, literal, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased

from dz_fastapi.core.constants import URL_DZ_SEARCH
from dz_fastapi.core.time import now_moscow
from dz_fastapi.http.dz_site_client import DZSiteClient
from dz_fastapi.models.autopart import AutoPart
from dz_fastapi.models.brand import Brand
from dz_fastapi.models.cross import AutoPartCross
from dz_fastapi.models.partner import (
    ORDER_TRACKING_SOURCE,
    SUPPLIER_ORDER_STATUS,
    TYPE_ORDER_ITEM_STATUS,
    TYPE_STATUS_ORDER,
    Customer,
    Order,
    OrderItem,
    PriceList,
    PriceListAutoPartAssociation,
    Provider,
    ProviderPriceListConfig,
    SupplierOrder,
    SupplierOrderItem,
)
from dz_fastapi.models.user import User
from dz_fastapi.services.order_status_mapping import (
    EXTERNAL_STATUS_SOURCE_DRAGONZAP,
    apply_status_mapping_to_order_item,
    build_external_status_normalized,
    build_external_status_raw,
    get_active_status_mappings,
    record_unmapped_external_status,
    resolve_internal_order_status,
    select_best_mapping,
)

logger = logging.getLogger("dz_fastapi")

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
SITE_STATUS_SYNC_LIMIT = int(os.getenv("TRACKING_SITE_SYNC_LIMIT", "200"))
SITE_API_KEY = os.getenv("KEY_FOR_WEBSITE")


def tracking_history_cutoff(days: int = TRACKING_HISTORY_DAYS) -> datetime:
    return now_moscow() - timedelta(days=days)


def _normalize_oem(value: Optional[str]) -> Optional[str]:
    normalized = str(value or "").strip().upper()
    return normalized or None


def _normalize_brand(value: Optional[str]) -> Optional[str]:
    normalized = str(value or "").strip()
    return normalized or None


async def _resolve_tracking_oem_numbers(
    session: AsyncSession,
    *,
    oem_number: Optional[str] = None,
    brand_name: Optional[str] = None,
    include_crosses: bool = False,
) -> list[str]:
    normalized_oem = _normalize_oem(oem_number)
    if not normalized_oem:
        return []

    resolved_oems: set[str] = {normalized_oem}
    if not include_crosses:
        return [normalized_oem]

    normalized_brand = _normalize_brand(brand_name)

    async def _load_source_autopart_ids(
        restrict_brand: bool,
    ) -> list[int]:
        stmt = select(AutoPart.id).where(AutoPart.oem_number == normalized_oem)
        if restrict_brand and normalized_brand:
            stmt = (
                stmt.join(Brand, Brand.id == AutoPart.brand_id)
                .where(Brand.name.ilike(normalized_brand))
            )
        result = await session.execute(stmt)
        return list(result.scalars().all())

    source_autopart_ids = await _load_source_autopart_ids(
        restrict_brand=True
    )
    if not source_autopart_ids and normalized_brand:
        source_autopart_ids = await _load_source_autopart_ids(
            restrict_brand=False
        )

    if source_autopart_ids:
        direct_crosses_stmt = select(
            AutoPartCross.cross_oem_number,
            AutoPartCross.cross_autopart_id,
        ).where(AutoPartCross.source_autopart_id.in_(source_autopart_ids))
        direct_cross_rows = (
            await session.execute(direct_crosses_stmt)
        ).all()
        for cross_oem_number, _ in direct_cross_rows:
            normalized_cross_oem = _normalize_oem(cross_oem_number)
            if normalized_cross_oem:
                resolved_oems.add(normalized_cross_oem)

        direct_cross_autopart_ids = [
            cross_autopart_id
            for _, cross_autopart_id in direct_cross_rows
            if cross_autopart_id is not None
        ]
        if direct_cross_autopart_ids:
            direct_cross_oems_stmt = select(AutoPart.oem_number).where(
                AutoPart.id.in_(direct_cross_autopart_ids)
            )
            direct_cross_oems = (
                await session.execute(direct_cross_oems_stmt)
            ).scalars()
            for cross_oem_number in direct_cross_oems:
                normalized_cross_oem = _normalize_oem(cross_oem_number)
                if normalized_cross_oem:
                    resolved_oems.add(normalized_cross_oem)

        reverse_cross_stmt = (
            select(AutoPart.oem_number)
            .join(
                AutoPartCross,
                AutoPartCross.source_autopart_id == AutoPart.id,
            )
            .where(
                or_(
                    AutoPartCross.cross_oem_number == normalized_oem,
                    AutoPartCross.cross_autopart_id.in_(source_autopart_ids),
                )
            )
        )
        reverse_cross_oems = (
            await session.execute(reverse_cross_stmt)
        ).scalars()
        for reverse_oem_number in reverse_cross_oems:
            normalized_reverse_oem = _normalize_oem(reverse_oem_number)
            if normalized_reverse_oem:
                resolved_oems.add(normalized_reverse_oem)

    return sorted(
        resolved_oems,
        key=lambda item: (item != normalized_oem, item),
    )


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
    return "UNKNOWN"


def _has_tracking_identity(
    oem_number: Optional[str],
    brand_name: Optional[str],
    autopart_name: Optional[str],
) -> bool:
    return any(
        str(value or "").strip()
        for value in (oem_number, brand_name, autopart_name)
    )


def _extract_site_items(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if not isinstance(payload, dict):
        return []

    data = payload.get("data")
    if isinstance(data, list):
        return [item for item in data if isinstance(item, dict)]
    if isinstance(data, dict):
        for key in ("items", "results", "records"):
            value = data.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]

    for key in ("items", "results", "records"):
        value = payload.get(key)
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]
    return []


def _safe_int(value: Any) -> Optional[int]:
    if value in (None, ""):
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
    sys_info = item.get("sys_info")
    sources = (
        item,
        sys_info if isinstance(sys_info, dict) else None,
    )
    for source in sources:
        if not isinstance(source, dict):
            continue
        for key in (
            "received_quantity",
            "received_qty",
            "delivered_quantity",
            "delivered_qty",
            "issued_quantity",
            "issued_qty",
            "shipped_quantity",
            "shipped_qty",
            "fact_quantity",
            "fact_qnt",
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
    oem_numbers: Optional[list[str]] = None,
    brand_name: Optional[str] = None,
    provider_id: Optional[int] = None,
    customer_id: Optional[int] = None,
    limit: int = SITE_STATUS_SYNC_LIMIT,
) -> dict[str, int]:
    if not SITE_API_KEY:
        logger.debug(
            "Skip tracking status sync: KEY_FOR_WEBSITE is not configured"
        )
        return {
            "checked": 0,
            "updated": 0,
            "not_found": 0,
            "errors": 0,
        }

    normalized_oems: list[str] = []
    for raw_oem in oem_numbers or []:
        normalized_oem = _normalize_oem(raw_oem)
        if normalized_oem and normalized_oem not in normalized_oems:
            normalized_oems.append(normalized_oem)
    normalized_oem = _normalize_oem(oem_number)
    if normalized_oem and normalized_oem not in normalized_oems:
        normalized_oems.append(normalized_oem)
    normalized_brand = _normalize_brand(brand_name)
    stmt = (
        select(OrderItem, Order)
        .join(Order, Order.id == OrderItem.order_id)
        .where(
            Order.source_type == ORDER_TRACKING_SOURCE.DRAGONZAP_SEARCH.value,
            Order.created_at >= tracking_history_cutoff(),
            OrderItem.tracking_uuid.is_not(None),
        )
        .order_by(
            Order.created_at.desc(),
            Order.id.desc(),
            OrderItem.id.desc(),
        )
    )
    if normalized_oems:
        stmt = stmt.where(OrderItem.oem_number.in_(normalized_oems))
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
            "checked": 0,
            "updated": 0,
            "not_found": 0,
            "errors": 0,
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
                        if str(remote.get("comment") or "").strip()
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
                    "Failed to sync Dragonzap tracking status "
                    "for tracking_uuid=%s",
                    item.tracking_uuid,
                )

    if updated:
        await session.commit()
    else:
        await session.rollback()

    return {
        "checked": checked,
        "updated": updated,
        "not_found": not_found,
        "errors": errors,
    }


async def list_tracking_history(
    session: AsyncSession,
    *,
    oem_number: Optional[str] = None,
    brand_name: Optional[str] = None,
    extra_oem_numbers: Optional[list[str]] = None,
    provider_id: Optional[int] = None,
    customer_id: Optional[int] = None,
    status: Optional[str] = None,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
    limit: int = 300,
    sync_site: bool = False,
    include_crosses: bool = False,
) -> list[dict[str, Any]]:
    normalized_oem_numbers = await _resolve_tracking_oem_numbers(
        session,
        oem_number=oem_number,
        brand_name=brand_name,
        include_crosses=include_crosses,
    )
    extra_normalized_oems = [
        normalized
        for normalized in (
            _normalize_oem(value) for value in (extra_oem_numbers or [])
        )
        if normalized
    ]
    if extra_normalized_oems:
        normalized_oem_numbers = list(
            dict.fromkeys((normalized_oem_numbers or []) + extra_normalized_oems)
        )
    if sync_site:
        await sync_site_tracking_statuses(
            session,
            oem_number=oem_number,
            oem_numbers=normalized_oem_numbers,
            brand_name=brand_name,
            provider_id=provider_id,
            customer_id=customer_id,
            limit=min(limit, SITE_STATUS_SYNC_LIMIT),
        )

    normalized_oem = _normalize_oem(oem_number)
    normalized_brand = _normalize_brand(brand_name)
    status_filter = str(status or "").strip().upper() or None
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
            literal("supplier").label("source_type"),
            literal("Прайсы поставщиков").label("source_label"),
            SupplierOrder.id.label("order_id"),
            SupplierOrderItem.id.label("item_id"),
            SupplierOrder.provider_id.label("provider_id"),
            provider_alias.name.label("provider_name"),
            literal(None).label("customer_id"),
            literal(None).label("customer_name"),
            SupplierOrder.created_by_user_id.label("ordered_by_user_id"),
            User.email.label("ordered_by_email"),
            SupplierOrderItem.oem_number.label("oem_number"),
            SupplierOrderItem.brand_name.label("brand_name"),
            SupplierOrderItem.autopart_name.label("autopart_name"),
            SupplierOrderItem.quantity.label("ordered_quantity"),
            SupplierOrderItem.received_quantity.label("received_quantity"),
            SupplierOrderItem.price.label("price"),
            SupplierOrderItem.min_delivery_day.label("min_delivery_day"),
            SupplierOrderItem.max_delivery_day.label("max_delivery_day"),
            SupplierOrder.created_at.label("created_at"),
            SupplierOrderItem.received_at.label("received_at"),
            SupplierOrder.status.label("order_status"),
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
    if normalized_oem_numbers:
        supplier_stmt = supplier_stmt.where(
            SupplierOrderItem.oem_number.in_(normalized_oem_numbers)
        )
    elif normalized_oem:
        supplier_stmt = supplier_stmt.where(
            SupplierOrderItem.oem_number == normalized_oem
        )
    if normalized_brand and not include_crosses:
        supplier_stmt = supplier_stmt.where(
            SupplierOrderItem.brand_name.ilike(normalized_brand)
        )
    if provider_id is not None:
        supplier_stmt = supplier_stmt.where(
            SupplierOrder.provider_id == provider_id
        )

    site_stmt = (
        select(
            literal("site").label("source_type"),
            literal("Dragonzap").label("source_label"),
            Order.id.label("order_id"),
            OrderItem.id.label("item_id"),
            Order.provider_id.label("provider_id"),
            provider_alias.name.label("provider_name"),
            Order.customer_id.label("customer_id"),
            customer_alias.name.label("customer_name"),
            Order.created_by_user_id.label("ordered_by_user_id"),
            User.email.label("ordered_by_email"),
            OrderItem.oem_number.label("oem_number"),
            OrderItem.brand_name.label("brand_name"),
            OrderItem.autopart_name.label("autopart_name"),
            OrderItem.quantity.label("ordered_quantity"),
            OrderItem.received_quantity.label("received_quantity"),
            OrderItem.price.label("price"),
            OrderItem.min_delivery_day.label("min_delivery_day"),
            OrderItem.max_delivery_day.label("max_delivery_day"),
            Order.created_at.label("created_at"),
            OrderItem.received_at.label("received_at"),
            Order.status.label("order_status"),
            OrderItem.status.label("item_status"),
            OrderItem.external_status_source.label("external_status_source"),
            OrderItem.external_status_raw.label("external_status_raw"),
            OrderItem.external_status_mapping_id.label(
                "external_status_mapping_id"
            ),
        )
        .join(OrderItem, OrderItem.order_id == Order.id)
        .join(provider_alias, provider_alias.id == Order.provider_id)
        .join(customer_alias, customer_alias.id == Order.customer_id)
        .outerjoin(User, User.id == Order.created_by_user_id)
        .where(
            Order.source_type == ORDER_TRACKING_SOURCE.DRAGONZAP_SEARCH.value,
            Order.created_at >= range_start,
            Order.created_at <= range_end,
        )
    )
    if normalized_oem_numbers:
        site_stmt = site_stmt.where(
            OrderItem.oem_number.in_(normalized_oem_numbers)
        )
    elif normalized_oem:
        site_stmt = site_stmt.where(OrderItem.oem_number == normalized_oem)
    if normalized_brand and not include_crosses:
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
            row.order_status.name if row.order_status else "UNKNOWN"
        )
        if status_filter and current_status != status_filter:
            continue
        results.append(
            {
                "source_type": row.source_type,
                "source_label": row.source_label,
                "order_id": row.order_id,
                "item_id": row.item_id,
                "provider_id": row.provider_id,
                "provider_name": row.provider_name,
                "customer_id": row.customer_id,
                "customer_name": row.customer_name,
                "ordered_by_user_id": row.ordered_by_user_id,
                "ordered_by_email": row.ordered_by_email,
                "oem_number": row.oem_number,
                "brand_name": row.brand_name,
                "autopart_name": row.autopart_name,
                "ordered_quantity": row.ordered_quantity,
                "received_quantity": row.received_quantity,
                "price": row.price,
                "min_delivery_day": row.min_delivery_day,
                "max_delivery_day": row.max_delivery_day,
                "created_at": row.created_at,
                "received_at": row.received_at,
                "current_status": current_status,
                "order_status": current_status,
                "item_status": None,
                "external_status_source": None,
                "external_status_raw": None,
                "needs_status_mapping": False,
                "actual_lead_days": _actual_lead_days(
                    row.created_at, row.received_at
                ),
                "link": f"/customer-orders/suppliers/{row.order_id}",
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
                "source_type": row.source_type,
                "source_label": row.source_label,
                "order_id": row.order_id,
                "item_id": row.item_id,
                "provider_id": row.provider_id,
                "provider_name": row.provider_name,
                "customer_id": row.customer_id,
                "customer_name": row.customer_name,
                "ordered_by_user_id": row.ordered_by_user_id,
                "ordered_by_email": row.ordered_by_email,
                "oem_number": row.oem_number,
                "brand_name": row.brand_name,
                "autopart_name": row.autopart_name,
                "ordered_quantity": row.ordered_quantity,
                "received_quantity": row.received_quantity,
                "price": row.price,
                "min_delivery_day": row.min_delivery_day,
                "max_delivery_day": row.max_delivery_day,
                "created_at": row.created_at,
                "received_at": row.received_at,
                "current_status": current_status,
                "order_status": (
                    row.order_status.name if row.order_status else None
                ),
                "item_status": (
                    row.item_status.name if row.item_status else None
                ),
                "external_status_source": row.external_status_source,
                "external_status_raw": row.external_status_raw,
                "needs_status_mapping": bool(
                    row.external_status_source
                    and row.external_status_raw
                    and row.external_status_mapping_id is None
                ),
                "actual_lead_days": _actual_lead_days(
                    row.created_at, row.received_at
                ),
                "link": "/orders/tracking",
            }
        )

    results.sort(
        key=lambda item: (
            item["created_at"],
            item["order_id"],
            item["item_id"],
        ),
        reverse=True,
    )
    return results[:limit]


def _to_decimal(value: Any) -> Optional[Decimal]:
    if value is None or value == "":
        return None
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except Exception:
        return None


def _round_stat(value: Optional[float], digits: int = 1) -> Optional[float]:
    if value is None:
        return None
    return round(float(value), digits)


def _offer_sort_key(row: dict[str, Any]) -> tuple[Decimal, int, int]:
    price = _to_decimal(row.get("price")) or Decimal("999999999")
    max_delivery = row.get("max_delivery_day")
    quantity = int(row.get("quantity") or 0)
    return (
        price,
        int(max_delivery) if max_delivery is not None else 999999,
        -quantity,
    )


def _build_offer_payload(row: dict[str, Any]) -> dict[str, Any]:
    price_value = _to_decimal(row.get("price"))
    return {
        "autopart_id": row.get("autopart_id"),
        "oem_number": row.get("oem_number"),
        "brand_name": row.get("brand_name"),
        "name": row.get("autopart_name"),
        "provider_id": row.get("provider_id"),
        "provider_name": row.get("provider_name"),
        "provider_config_id": row.get("provider_config_id"),
        "provider_config_name": row.get("provider_config_name"),
        "price": price_value,
        "quantity": int(row.get("quantity") or 0),
        "min_delivery_day": row.get("min_delivery_day"),
        "max_delivery_day": row.get("max_delivery_day"),
        "pricelist_id": row.get("pricelist_id"),
        "pricelist_date": row.get("pricelist_date"),
        "is_own_price": bool(row.get("is_own_price")),
    }


async def _load_current_offer_candidates(
    session: AsyncSession,
    *,
    normalized_oem_numbers: list[str],
) -> list[dict[str, Any]]:
    if not normalized_oem_numbers:
        return []

    partition_key = func.coalesce(
        PriceList.provider_config_id, PriceList.provider_id
    ).label("partition_key")
    latest_pricelist_rank = (
        func.row_number()
        .over(
            partition_by=partition_key,
            order_by=(PriceList.date.desc(), PriceList.id.desc()),
        )
        .label("latest_rn")
    )

    latest_pricelists = (
        select(
            PriceList.id.label("pricelist_id"),
            latest_pricelist_rank,
        )
        .select_from(PriceList)
        .where(PriceList.is_active.is_(True))
        .subquery()
    )

    stmt = (
        select(
            AutoPart.id.label("autopart_id"),
            AutoPart.oem_number.label("oem_number"),
            AutoPart.name.label("autopart_name"),
            Brand.name.label("brand_name"),
            Provider.id.label("provider_id"),
            Provider.name.label("provider_name"),
            Provider.is_own_price.label("is_own_price"),
            ProviderPriceListConfig.id.label("provider_config_id"),
            ProviderPriceListConfig.name_price.label("provider_config_name"),
            PriceListAutoPartAssociation.price.label("price"),
            PriceListAutoPartAssociation.quantity.label("quantity"),
            ProviderPriceListConfig.min_delivery_day.label("min_delivery_day"),
            ProviderPriceListConfig.max_delivery_day.label("max_delivery_day"),
            PriceList.id.label("pricelist_id"),
            PriceList.date.label("pricelist_date"),
        )
        .select_from(latest_pricelists)
        .join(PriceList, PriceList.id == latest_pricelists.c.pricelist_id)
        .join(
            PriceListAutoPartAssociation,
            PriceListAutoPartAssociation.pricelist_id == PriceList.id,
        )
        .join(
            AutoPart,
            AutoPart.id == PriceListAutoPartAssociation.autopart_id,
        )
        .join(Brand, Brand.id == AutoPart.brand_id)
        .join(Provider, Provider.id == PriceList.provider_id)
        .outerjoin(
            ProviderPriceListConfig,
            ProviderPriceListConfig.id == PriceList.provider_config_id,
        )
        .where(latest_pricelists.c.latest_rn == 1)
        .where(AutoPart.oem_number.in_(normalized_oem_numbers))
        .order_by(
            AutoPart.oem_number.asc(),
            Provider.name.asc(),
            ProviderPriceListConfig.name_price.asc().nullslast(),
            PriceListAutoPartAssociation.price.asc(),
        )
    )
    return list((await session.execute(stmt)).mappings().all())


async def _load_own_price_config_options(
    session: AsyncSession,
    *,
    normalized_oem_numbers: list[str],
) -> list[dict[str, Any]]:
    if not normalized_oem_numbers:
        return []

    recent_cutoff = now_moscow().date() - timedelta(days=TRACKING_HISTORY_DAYS)
    stmt = (
        select(
            ProviderPriceListConfig.id.label("id"),
            Provider.id.label("provider_id"),
            Provider.name.label("provider_name"),
            ProviderPriceListConfig.name_price.label("name_price"),
            func.max(PriceList.date).label("latest_pricelist_date"),
            ProviderPriceListConfig.use_for_order_insights.label(
                "use_for_order_insights"
            ),
        )
        .select_from(PriceListAutoPartAssociation)
        .join(
            PriceList,
            PriceList.id == PriceListAutoPartAssociation.pricelist_id,
        )
        .join(
            ProviderPriceListConfig,
            ProviderPriceListConfig.id == PriceList.provider_config_id,
        )
        .join(Provider, Provider.id == PriceList.provider_id)
        .join(
            AutoPart,
            AutoPart.id == PriceListAutoPartAssociation.autopart_id,
        )
        .where(
            Provider.is_own_price.is_(True),
            PriceList.date >= recent_cutoff,
            AutoPart.oem_number.in_(normalized_oem_numbers),
        )
        .group_by(
            ProviderPriceListConfig.id,
            Provider.id,
            Provider.name,
            ProviderPriceListConfig.name_price,
            ProviderPriceListConfig.use_for_order_insights,
        )
        .order_by(Provider.name.asc(), ProviderPriceListConfig.id.asc())
    )
    return [
        dict(row._mapping)
        for row in (await session.execute(stmt)).all()
    ]


async def _build_own_price_analysis(
    session: AsyncSession,
    *,
    normalized_oem: Optional[str],
    normalized_oem_numbers: list[str],
    provider_config_id: int,
    history_rows: list[dict[str, Any]],
) -> Optional[dict[str, Any]]:
    if not normalized_oem_numbers:
        return None

    stmt = (
        select(
            PriceList.id.label("pricelist_id"),
            PriceList.date.label("pricelist_date"),
            AutoPart.oem_number.label("oem_number"),
            PriceListAutoPartAssociation.quantity.label("quantity"),
            PriceListAutoPartAssociation.price.label("price"),
            Provider.id.label("provider_id"),
            Provider.name.label("provider_name"),
            ProviderPriceListConfig.name_price.label("provider_config_name"),
        )
        .select_from(PriceListAutoPartAssociation)
        .join(
            PriceList,
            PriceList.id == PriceListAutoPartAssociation.pricelist_id,
        )
        .join(
            AutoPart,
            AutoPart.id == PriceListAutoPartAssociation.autopart_id,
        )
        .join(Provider, Provider.id == PriceList.provider_id)
        .join(
            ProviderPriceListConfig,
            ProviderPriceListConfig.id == PriceList.provider_config_id,
        )
        .where(
            PriceList.provider_config_id == provider_config_id,
            PriceList.is_active.is_(True),
            AutoPart.oem_number.in_(normalized_oem_numbers),
        )
        .order_by(PriceList.date.asc(), PriceList.id.asc())
    )
    rows = list((await session.execute(stmt)).mappings().all())
    if not rows:
        return None

    snapshots_by_key: dict[tuple[date, int], dict[str, Any]] = {}
    normalized_exact = _normalize_oem(normalized_oem)
    for row in rows:
        snapshot_key = (row["pricelist_date"], row["pricelist_id"])
        snapshot = snapshots_by_key.setdefault(
            snapshot_key,
            {
                "pricelist_date": row["pricelist_date"],
                "pricelist_id": row["pricelist_id"],
                "provider_id": row["provider_id"],
                "provider_name": row["provider_name"],
                "provider_config_name": row.get("provider_config_name"),
                "total_quantity": 0,
                "exact_prices": [],
                "all_prices": [],
                "oem_groups": {},
            },
        )
        normalized_row_oem = _normalize_oem(row.get("oem_number")) or str(
            row.get("oem_number") or ""
        )
        oem_group = snapshot["oem_groups"].setdefault(
            normalized_row_oem,
            {
                "quantity": 0,
                "exact_prices": [],
                "all_prices": [],
            },
        )
        quantity = int(row.get("quantity") or 0)
        oem_group["quantity"] = max(
            int(oem_group.get("quantity") or 0),
            quantity,
        )
        price_value = _to_decimal(row.get("price"))
        if price_value is not None:
            oem_group["all_prices"].append(price_value)
            if normalized_row_oem == normalized_exact:
                oem_group["exact_prices"].append(price_value)

    for snapshot in snapshots_by_key.values():
        total_quantity = 0
        exact_prices = []
        all_prices = []
        for group in snapshot["oem_groups"].values():
            total_quantity += int(group.get("quantity") or 0)
            if group.get("exact_prices"):
                exact_prices.append(min(group["exact_prices"]))
            if group.get("all_prices"):
                all_prices.append(min(group["all_prices"]))
        snapshot["total_quantity"] = total_quantity
        snapshot["exact_prices"] = exact_prices
        snapshot["all_prices"] = all_prices
        snapshot.pop("oem_groups", None)

    snapshots = list(snapshots_by_key.values())
    latest_snapshot = snapshots[-1]

    receipt_events = []
    for row in history_rows:
        received_at = row.get("received_at")
        received_quantity = int(row.get("received_quantity") or 0)
        if received_at is None or received_quantity <= 0:
            continue
        receipt_events.append(
            {
                "received_at": received_at,
                "received_date": received_at.date(),
                "received_quantity": received_quantity,
            }
        )

    arrivals_last_30_days = 0
    arrivals_last_90_days = 0
    arrivals_last_365_days = 0
    sold_last_30_days = 0
    sold_last_90_days = 0
    sold_last_365_days = 0
    today = now_moscow().date()
    for previous_snapshot, current_snapshot in zip(snapshots, snapshots[1:]):
        previous_qty = int(previous_snapshot["total_quantity"])
        current_qty = int(current_snapshot["total_quantity"])
        interval_receipts = sum(
            event["received_quantity"]
            for event in receipt_events
            if previous_snapshot["pricelist_date"]
            < event["received_date"]
            <= current_snapshot["pricelist_date"]
        )
        expected_qty = previous_qty + interval_receipts
        inferred_additional_arrival = max(current_qty - expected_qty, 0)
        interval_arrivals = interval_receipts + inferred_additional_arrival
        decrease = max(expected_qty - current_qty, 0)

        snapshot_date = current_snapshot["pricelist_date"]
        if snapshot_date >= today - timedelta(days=30):
            arrivals_last_30_days += interval_arrivals
        if snapshot_date >= today - timedelta(days=30):
            sold_last_30_days += decrease
        if snapshot_date >= today - timedelta(days=90):
            arrivals_last_90_days += interval_arrivals
        if snapshot_date >= today - timedelta(days=90):
            sold_last_90_days += decrease
        if snapshot_date >= today - timedelta(days=365):
            arrivals_last_365_days += interval_arrivals
        if snapshot_date >= today - timedelta(days=365):
            sold_last_365_days += decrease

    latest_price_candidates = (
        latest_snapshot["exact_prices"] or latest_snapshot["all_prices"]
    )
    latest_price = (
        min(latest_price_candidates) if latest_price_candidates else None
    )
    average_daily_decrease_30_days = (
        Decimal(str(sold_last_30_days)) / Decimal("30")
        if sold_last_30_days > 0
        else None
    )
    estimated_days_left_30_days = None
    if (
        average_daily_decrease_30_days is not None
        and average_daily_decrease_30_days > 0
        and latest_snapshot["total_quantity"] > 0
    ):
        estimated_days_left_30_days = int(
            Decimal(str(latest_snapshot["total_quantity"]))
            / average_daily_decrease_30_days
        )

    return {
        "provider_config_id": provider_config_id,
        "provider_id": latest_snapshot["provider_id"],
        "provider_name": latest_snapshot["provider_name"],
        "provider_config_name": latest_snapshot.get("provider_config_name"),
        "latest_pricelist_date": latest_snapshot["pricelist_date"],
        "latest_price": latest_price,
        "current_quantity": int(latest_snapshot["total_quantity"]),
        "arrivals_last_30_days": arrivals_last_30_days,
        "arrivals_last_90_days": arrivals_last_90_days,
        "arrivals_last_365_days": arrivals_last_365_days,
        "sold_last_30_days": sold_last_30_days,
        "sold_last_90_days": sold_last_90_days,
        "sold_last_365_days": sold_last_365_days,
        "average_daily_decrease_30_days": (
            float(
                average_daily_decrease_30_days.quantize(
                    Decimal("0.01"), rounding=ROUND_HALF_UP
                )
            )
            if average_daily_decrease_30_days is not None
            else None
        ),
        "estimated_days_left_30_days": estimated_days_left_30_days,
    }


async def get_tracking_history_insights(
    session: AsyncSession,
    *,
    oem_number: Optional[str] = None,
    brand_name: Optional[str] = None,
    extra_oem_numbers: Optional[list[str]] = None,
    own_provider_config_id: Optional[int] = None,
) -> dict[str, Any]:
    normalized_oem = _normalize_oem(oem_number) or ""
    normalized_oem_numbers = await _resolve_tracking_oem_numbers(
        session,
        oem_number=oem_number,
        brand_name=brand_name,
        include_crosses=True,
    )
    extra_normalized_oems = [
        normalized
        for normalized in (
            _normalize_oem(value) for value in (extra_oem_numbers or [])
        )
        if normalized and normalized != normalized_oem
    ]
    if extra_normalized_oems:
        normalized_oem_numbers = list(
            dict.fromkeys((normalized_oem_numbers or []) + extra_normalized_oems)
        )
    cross_oem_numbers = [
        item
        for item in normalized_oem_numbers
        if item and item != normalized_oem and item not in extra_normalized_oems
    ]

    current_offer_rows = await _load_current_offer_candidates(
        session,
        normalized_oem_numbers=normalized_oem_numbers or [normalized_oem],
    )
    actionable_offer_rows = [
        row
        for row in current_offer_rows
        if _to_decimal(row.get("price")) is not None
        and int(row.get("quantity") or 0) > 0
    ]
    exact_offer_rows = [
        row
        for row in actionable_offer_rows
        if _normalize_oem(row.get("oem_number")) == normalized_oem
    ]
    exact_min_offer = (
        _build_offer_payload(min(exact_offer_rows, key=_offer_sort_key))
        if exact_offer_rows
        else None
    )
    min_offer_with_crosses = (
        _build_offer_payload(
            min(actionable_offer_rows, key=_offer_sort_key)
        )
        if actionable_offer_rows
        else None
    )

    history_rows = await list_tracking_history(
        session,
        oem_number=oem_number,
        brand_name=brand_name,
        extra_oem_numbers=extra_normalized_oems,
        limit=1000,
        sync_site=False,
        include_crosses=True,
    )
    exact_history_rows = [
        row
        for row in history_rows
        if _normalize_oem(row.get("oem_number")) == normalized_oem
    ]
    total_ordered_quantity_last_year = sum(
        int(row.get("ordered_quantity") or 0) for row in history_rows
    )
    total_received_quantity_last_year = sum(
        int(row.get("received_quantity") or 0) for row in history_rows
    )
    actual_lead_values = [
        int(row["actual_lead_days"])
        for row in history_rows
        if row.get("actual_lead_days") is not None
    ]
    fill_rate_percent = (
        _round_stat(
            (
                total_received_quantity_last_year
                / total_ordered_quantity_last_year
            )
            * 100,
            1,
        )
        if total_ordered_quantity_last_year > 0
        else None
    )
    exact_prices = [
        _to_decimal(row.get("price"))
        for row in exact_history_rows
        if _to_decimal(row.get("price")) is not None
    ]
    all_prices = [
        _to_decimal(row.get("price"))
        for row in history_rows
        if _to_decimal(row.get("price")) is not None
    ]

    own_price_configs = await _load_own_price_config_options(
        session,
        normalized_oem_numbers=normalized_oem_numbers or [normalized_oem],
    )
    resolved_own_provider_config_id = own_provider_config_id or next(
        (
            int(config["id"])
            for config in own_price_configs
            if bool(config.get("use_for_order_insights"))
        ),
        None,
    )
    own_price_analysis = None
    if resolved_own_provider_config_id is not None:
        own_price_analysis = await _build_own_price_analysis(
            session,
            normalized_oem=normalized_oem,
            normalized_oem_numbers=normalized_oem_numbers or [normalized_oem],
            provider_config_id=resolved_own_provider_config_id,
            history_rows=history_rows,
        )

    return {
        "oem_number": normalized_oem,
        "cross_oem_numbers": cross_oem_numbers,
        "site_cross_oem_numbers": extra_normalized_oems,
        "exact_min_offer": exact_min_offer,
        "min_offer_with_crosses": min_offer_with_crosses,
        "order_count_last_year": len(history_rows),
        "total_ordered_quantity_last_year": total_ordered_quantity_last_year,
        "total_received_quantity_last_year": total_received_quantity_last_year,
        "unique_suppliers_last_year": len(
            {
                row.get("provider_id") or row.get("provider_name")
                for row in history_rows
                if row.get("provider_id") is not None
                or row.get("provider_name")
            }
        ),
        "fill_rate_percent": fill_rate_percent,
        "historical_min_price_exact": min(exact_prices) if exact_prices else None,
        "historical_min_price_with_crosses": (
            min(all_prices) if all_prices else None
        ),
        "average_actual_lead_days": (
            _round_stat(sum(actual_lead_values) / len(actual_lead_values), 1)
            if actual_lead_values
            else None
        ),
        "last_ordered_at": max(
            (row.get("created_at") for row in history_rows),
            default=None,
        ),
        "last_received_at": max(
            (
                row.get("received_at")
                for row in history_rows
                if row.get("received_at") is not None
            ),
            default=None,
        ),
        "own_price_configs": own_price_configs,
        "own_price_analysis": own_price_analysis,
    }


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
    source_key = str(source_type or "").strip().lower()
    status_key = str(status or "").strip().upper() or None

    if source_key == "supplier":
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
        item, order = (await session.execute(stmt)).one_or_none() or (
            None,
            None,
        )
        if item is None or order is None:
            raise ValueError("Tracking item not found")
        if status_key:
            order.status = SUPPLIER_ORDER_STATUS[status_key]
        if received_quantity is not None:
            item.received_quantity, item.received_at = _set_received_metadata(
                received_quantity=received_quantity,
                received_at=item.received_at,
            )
        await session.commit()
        return {
            "source_type": "supplier",
            "item_id": item.id,
            "order_id": order.id,
            "status": order.status.name,
            "received_quantity": item.received_quantity,
            "received_at": item.received_at,
        }

    if source_key == "site":
        stmt = (
            select(OrderItem, Order)
            .join(Order, Order.id == OrderItem.order_id)
            .where(
                OrderItem.id == item_id,
                Order.source_type
                == ORDER_TRACKING_SOURCE.DRAGONZAP_SEARCH.value,
            )
        )
        item, order = (await session.execute(stmt)).one_or_none() or (
            None,
            None,
        )
        if item is None or order is None:
            raise ValueError("Tracking item not found")
        if status_key:
            order.status = TYPE_STATUS_ORDER[status_key]
            if status_key in {"ARRIVED", "SHIPPED"}:
                item.status = TYPE_ORDER_ITEM_STATUS.DELIVERED
            elif status_key in {"ERROR", "REFUSAL"}:
                item.status = TYPE_ORDER_ITEM_STATUS.ERROR
        if received_quantity is not None:
            item.received_quantity, item.received_at = _set_received_metadata(
                received_quantity=received_quantity,
                received_at=item.received_at,
            )
        await session.commit()
        return {
            "source_type": "site",
            "item_id": item.id,
            "order_id": order.id,
            "status": (order.status.name if order.status else "UNKNOWN"),
            "received_quantity": item.received_quantity,
            "received_at": item.received_at,
        }

    raise ValueError("Unsupported source type")


async def cleanup_old_tracking_history(
    session: AsyncSession,
    *,
    older_than_days: int = TRACKING_HISTORY_DAYS,
) -> dict[str, int]:
    cutoff = tracking_history_cutoff(older_than_days)

    supplier_ids = select(SupplierOrder.id).where(
        SupplierOrder.source_type == ORDER_TRACKING_SOURCE.SEARCH_OFFERS.value,
        SupplierOrder.created_at < cutoff,
    )
    order_ids = select(Order.id).where(
        Order.source_type == ORDER_TRACKING_SOURCE.DRAGONZAP_SEARCH.value,
        Order.created_at < cutoff,
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
        "supplier_items_deleted": supplier_items_deleted,
        "supplier_orders_deleted": supplier_orders_deleted,
        "order_items_deleted": order_items_deleted,
        "orders_deleted": orders_deleted,
    }
