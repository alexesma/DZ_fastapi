from __future__ import annotations

import logging
import os
from datetime import date, datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal
from math import ceil, sqrt
from typing import Any, Optional

from sqlalchemy import delete, func, literal, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased

from dz_fastapi.core.constants import URL_DZ_SEARCH
from dz_fastapi.core.time import now_moscow
from dz_fastapi.http.dz_site_client import DZSiteClient
from dz_fastapi.models.autopart import AutoPart
from dz_fastapi.models.brand import Brand
from dz_fastapi.models.cross import AutoPartCross, AutoPartInvalidCross
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
from dz_fastapi.services.crosses import load_bidirectional_cross_members
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


async def _load_tracking_source_autoparts(
    session: AsyncSession,
    *,
    normalized_oem: str,
    normalized_brand: Optional[str],
) -> list[AutoPart]:
    stmt = select(AutoPart).where(AutoPart.oem_number == normalized_oem)
    if normalized_brand:
        stmt = (
            stmt.join(Brand, Brand.id == AutoPart.brand_id)
            .where(Brand.name.ilike(normalized_brand))
        )
    result = await session.execute(stmt)
    autoparts = list(result.scalars().all())
    if autoparts or not normalized_brand:
        return autoparts

    fallback_stmt = select(AutoPart).where(AutoPart.oem_number == normalized_oem)
    fallback_result = await session.execute(fallback_stmt)
    return list(fallback_result.scalars().all())


async def _load_invalid_cross_state(
    session: AsyncSession,
    *,
    source_autopart_ids: list[int],
) -> tuple[set[str], set[int]]:
    if not source_autopart_ids:
        return set(), set()

    rows = (
        await session.execute(
            select(
                AutoPartInvalidCross.invalid_oem_number,
                AutoPartInvalidCross.invalid_autopart_id,
            ).where(
                AutoPartInvalidCross.source_autopart_id.in_(
                    source_autopart_ids
                )
            )
        )
    ).all()

    invalid_oems = {
        normalized
        for normalized in (
            _normalize_oem(row.invalid_oem_number) for row in rows
        )
        if normalized
    }
    invalid_autopart_ids = {
        int(row.invalid_autopart_id)
        for row in rows
        if row.invalid_autopart_id is not None
    }
    return invalid_oems, invalid_autopart_ids


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
    source_autoparts = await _load_tracking_source_autoparts(
        session,
        normalized_oem=normalized_oem,
        normalized_brand=normalized_brand,
    )
    source_autopart_ids = [autopart.id for autopart in source_autoparts]
    invalid_oems, invalid_autopart_ids = await _load_invalid_cross_state(
        session,
        source_autopart_ids=source_autopart_ids,
    )

    bidirectional_members = await load_bidirectional_cross_members(
        session,
        seed_autopart_ids=source_autopart_ids,
        excluded_autopart_ids=invalid_autopart_ids,
    )
    for member in bidirectional_members:
        normalized_component_oem = _normalize_oem(member.oem_number)
        if normalized_component_oem and normalized_component_oem not in invalid_oems:
            resolved_oems.add(normalized_component_oem)

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
            if normalized_cross_oem and normalized_cross_oem not in invalid_oems:
                resolved_oems.add(normalized_cross_oem)

        direct_cross_autopart_ids = [
            cross_autopart_id
            for _, cross_autopart_id in direct_cross_rows
            if cross_autopart_id is not None
            and cross_autopart_id not in invalid_autopart_ids
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
                if normalized_cross_oem and normalized_cross_oem not in invalid_oems:
                    resolved_oems.add(normalized_cross_oem)

        reverse_cross_stmt = (
            select(AutoPart.id, AutoPart.oem_number)
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
        reverse_cross_rows = (await session.execute(reverse_cross_stmt)).all()
        for reverse_autopart_id, reverse_oem_number in reverse_cross_rows:
            if reverse_autopart_id in invalid_autopart_ids:
                continue
            normalized_reverse_oem = _normalize_oem(reverse_oem_number)
            if normalized_reverse_oem and normalized_reverse_oem not in invalid_oems:
                resolved_oems.add(normalized_reverse_oem)

    return sorted(
        resolved_oems,
        key=lambda item: (item != normalized_oem, item),
    )


async def _resolve_tracking_cross_items(
    session: AsyncSession,
    *,
    oem_number: Optional[str] = None,
    brand_name: Optional[str] = None,
) -> list[dict[str, Any]]:
    normalized_oem = _normalize_oem(oem_number)
    if not normalized_oem:
        return []

    normalized_brand = _normalize_brand(brand_name)
    source_autoparts = await _load_tracking_source_autoparts(
        session,
        normalized_oem=normalized_oem,
        normalized_brand=normalized_brand,
    )
    source_autopart_ids = [autopart.id for autopart in source_autoparts]
    if not source_autopart_ids:
        return []

    invalid_oems, invalid_autopart_ids = await _load_invalid_cross_state(
        session,
        source_autopart_ids=source_autopart_ids,
    )

    items: dict[tuple[str, str], dict[str, Any]] = {}

    def _store_item(
        *,
        autopart_id: Optional[int],
        oem_number_value: Optional[str],
        brand_name_value: Optional[str],
        name_value: Optional[str],
    ) -> None:
        normalized_cross_oem = _normalize_oem(oem_number_value)
        if (
            not normalized_cross_oem
            or normalized_cross_oem == normalized_oem
            or normalized_cross_oem in invalid_oems
            or (
                autopart_id is not None
                and autopart_id in invalid_autopart_ids
            )
        ):
            return
        brand_value = str(brand_name_value or "").strip()
        key = (brand_value.upper(), normalized_cross_oem)
        items.setdefault(
            key,
            {
                "autopart_id": autopart_id,
                "oem_number": normalized_cross_oem,
                "brand_name": brand_value or None,
                "name": str(name_value or "").strip() or None,
            },
        )

    bidirectional_members = await load_bidirectional_cross_members(
        session,
        seed_autopart_ids=source_autopart_ids,
        excluded_autopart_ids=invalid_autopart_ids,
    )
    for member in bidirectional_members:
        _store_item(
            autopart_id=member.autopart_id,
            oem_number_value=member.oem_number,
            brand_name_value=member.brand_name,
            name_value=member.name,
        )

    direct_rows = (
        await session.execute(
            select(
                AutoPartCross.cross_autopart_id,
                AutoPartCross.cross_oem_number,
                Brand.name,
                AutoPart.name,
            )
            .select_from(AutoPartCross)
            .join(Brand, Brand.id == AutoPartCross.cross_brand_id)
            .outerjoin(
                AutoPart,
                AutoPart.id == AutoPartCross.cross_autopart_id,
            )
            .where(AutoPartCross.source_autopart_id.in_(source_autopart_ids))
        )
    ).all()
    for cross_autopart_id, cross_oem_number, cross_brand_name, cross_name in (
        direct_rows
    ):
        _store_item(
            autopart_id=cross_autopart_id,
            oem_number_value=cross_oem_number,
            brand_name_value=cross_brand_name,
            name_value=cross_name,
        )

    reverse_rows = (
        await session.execute(
            select(
                AutoPart.id,
                AutoPart.oem_number,
                Brand.name,
                AutoPart.name,
            )
            .select_from(AutoPart)
            .join(Brand, Brand.id == AutoPart.brand_id)
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
    ).all()
    for autopart_id, reverse_oem_number, reverse_brand_name, reverse_name in (
        reverse_rows
    ):
        _store_item(
            autopart_id=autopart_id,
            oem_number_value=reverse_oem_number,
            brand_name_value=reverse_brand_name,
            name_value=reverse_name,
        )

    return sorted(
        items.values(),
        key=lambda item: (
            str(item.get("brand_name") or ""),
            str(item.get("oem_number") or ""),
        ),
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
    normalized_oem = _normalize_oem(oem_number)
    normalized_brand = _normalize_brand(brand_name)
    source_autoparts = (
        await _load_tracking_source_autoparts(
            session,
            normalized_oem=normalized_oem,
            normalized_brand=normalized_brand,
        )
        if normalized_oem
        else []
    )
    source_autopart_ids = [autopart.id for autopart in source_autoparts]
    invalid_oems, _invalid_autopart_ids = await _load_invalid_cross_state(
        session,
        source_autopart_ids=source_autopart_ids,
    )
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
        if normalized and normalized not in invalid_oems
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
                # per-OEM quantities to avoid cross-cancellation in consumption calc
                "qty_by_oem": {},
                "exact_prices": [],
                "all_prices": [],
            },
        )
        quantity = int(row.get("quantity") or 0)
        snapshot["total_quantity"] += quantity
        normalized_row_oem = _normalize_oem(row.get("oem_number")) or str(
            row.get("oem_number") or ""
        )
        if normalized_row_oem:
            snapshot["qty_by_oem"][normalized_row_oem] = (
                snapshot["qty_by_oem"].get(normalized_row_oem, 0) + quantity
            )
        price_value = _to_decimal(row.get("price"))
        if price_value is not None:
            snapshot["all_prices"].append(price_value)
            if normalized_row_oem == normalized_exact:
                snapshot["exact_prices"].append(price_value)

    snapshots = list(snapshots_by_key.values())
    latest_snapshot = snapshots[-1]
    quantity_breakdown = [
        {
            "oem_number": oem_number,
            "quantity": int(quantity),
        }
        for oem_number, quantity in sorted(
            latest_snapshot["qty_by_oem"].items(),
            key=lambda item: (
                item[0] != normalized_exact,
                item[0],
            ),
        )
        if int(quantity or 0) > 0
    ]

    # receipt_events keyed by OEM so they can be matched per-OEM in the loop below
    receipt_events: list[dict[str, Any]] = []
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
                # normalised OEM lets us split receipts by position, not lump them
                "oem_number": _normalize_oem(row.get("oem_number")) or "",
            }
        )

    # Collect the full set of OEM numbers that ever appeared in any snapshot
    all_oems_in_snapshots: set[str] = set()
    for s in snapshots:
        all_oems_in_snapshots.update(s["qty_by_oem"].keys())

    arrivals_last_30_days = 0
    arrivals_last_90_days = 0
    arrivals_last_365_days = 0
    sold_last_30_days = 0
    sold_last_90_days = 0
    sold_last_365_days = 0
    today = now_moscow().date()
    for previous_snapshot, current_snapshot in zip(snapshots, snapshots[1:]):
        snapshot_date = current_snapshot["pricelist_date"]
        date_lo = previous_snapshot["pricelist_date"]
        date_hi = current_snapshot["pricelist_date"]

        # ── Per-OEM delta calculation ────────────────────────────────────────
        # Summing individual OEM deltas prevents cross-cancellation:
        # e.g. OEM_X drops 2 while cross OEM_Y rises 2 → combined total unchanged,
        # but per-OEM we correctly see 2 sold of X and 2 arrived for Y.
        interval_arrivals = 0
        decrease = 0

        for oem in all_oems_in_snapshots:
            prev_qty_oem = previous_snapshot["qty_by_oem"].get(oem, 0)
            curr_qty_oem = current_snapshot["qty_by_oem"].get(oem, 0)

            receipts_oem = sum(
                event["received_quantity"]
                for event in receipt_events
                if event["oem_number"] == oem
                and date_lo < event["received_date"] <= date_hi
            )
            expected_oem = prev_qty_oem + receipts_oem
            inferred_oem = max(curr_qty_oem - expected_oem, 0)
            interval_arrivals += receipts_oem + inferred_oem
            decrease += max(expected_oem - curr_qty_oem, 0)
        # ────────────────────────────────────────────────────────────────────

        if snapshot_date >= today - timedelta(days=30):
            arrivals_last_30_days += interval_arrivals
            sold_last_30_days += decrease
        if snapshot_date >= today - timedelta(days=90):
            arrivals_last_90_days += interval_arrivals
            sold_last_90_days += decrease
        if snapshot_date >= today - timedelta(days=365):
            arrivals_last_365_days += interval_arrivals
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
        "current_quantity_breakdown": quantity_breakdown,
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


_ACTIVE_ORDER_STATUSES = {
    # supplier orders
    "NEW", "SCHEDULED", "SENT",
    # site orders
    "NEW_OREDER", "ORDERED", "CONFIRMED", "PROCESSING",
}

_MONTH_NAMES_RU = [
    "Янв", "Фев", "Мар", "Апр", "Май", "Июн",
    "Июл", "Авг", "Сен", "Окт", "Ноя", "Дек",
]


def _compute_purchase_price_stats(
    history_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    """Return avg_purchase_price, last_purchase_price, price_trend, price_trend_pct."""
    priced = sorted(
        [
            row
            for row in history_rows
            if _to_decimal(row.get("price")) is not None
            and int(row.get("received_quantity") or 0) > 0
        ],
        key=lambda r: r.get("created_at") or datetime.min,
    )
    if not priced:
        return {
            "avg_purchase_price": None,
            "last_purchase_price": None,
            "price_trend": None,
            "price_trend_pct": None,
        }

    prices = [_to_decimal(r["price"]) for r in priced]  # type: ignore[arg-type]
    avg_val = float(sum(prices) / len(prices))
    last_val = float(prices[-1])

    price_trend: Optional[str] = None
    price_trend_pct: Optional[float] = None
    if len(priced) >= 2:
        mid = max(len(priced) // 2, 1)
        avg_old = sum(prices[:mid]) / mid
        avg_new = sum(prices[mid:]) / (len(prices) - mid)
        if avg_old > 0:
            pct = float((avg_new - avg_old) / avg_old * 100)
            price_trend_pct = round(pct, 1)
            if abs(pct) < 2.0:
                price_trend = "stable"
            elif pct > 0:
                price_trend = "up"
            else:
                price_trend = "down"

    return {
        "avg_purchase_price": round(avg_val, 2),
        "last_purchase_price": round(last_val, 2),
        "price_trend": price_trend,
        "price_trend_pct": price_trend_pct,
    }


def _compute_seasonality(
    history_rows: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Return (seasonality_list, peak_months_top3) grouped by calendar month."""
    from collections import defaultdict

    monthly: dict[str, dict[str, Any]] = defaultdict(
        lambda: {"count": 0, "qty": 0}
    )
    for row in history_rows:
        dt = row.get("created_at")
        if not dt:
            continue
        if hasattr(dt, "strftime"):
            month_key = dt.strftime("%Y-%m")
            month_num = int(dt.strftime("%m"))
        else:
            continue
        monthly[month_key]["count"] += 1
        monthly[month_key]["qty"] += int(row.get("ordered_quantity") or 0)
        monthly[month_key]["month_name"] = _MONTH_NAMES_RU[month_num - 1]

    seasonality = sorted(
        [
            {
                "month": k,
                "month_name": v["month_name"],
                "count": v["count"],
                "qty": v["qty"],
            }
            for k, v in monthly.items()
        ],
        key=lambda x: x["month"],
    )
    peak_months = sorted(seasonality, key=lambda x: x["qty"], reverse=True)[:3]
    return seasonality, peak_months


def _compute_supplier_stats(
    history_rows: list[dict[str, Any]],
    current_offer_rows: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], Optional[dict[str, Any]]]:
    """Per-provider stats from history + current offers. Returns (stats, best_supplier)."""
    from collections import defaultdict

    # group history by provider key
    provider_history: dict[Any, list[dict[str, Any]]] = defaultdict(list)
    for row in history_rows:
        key = row.get("provider_id") or row.get("provider_name") or "unknown"
        provider_history[key].append(row)

    # group actionable current offers by provider (skip own-price)
    provider_offers: dict[Any, list[dict[str, Any]]] = defaultdict(list)
    for row in current_offer_rows:
        if bool(row.get("is_own_price")):
            continue
        if (_to_decimal(row.get("price")) is None
                or int(row.get("quantity") or 0) <= 0):
            continue
        key = row.get("provider_id") or row.get("provider_name") or "unknown"
        provider_offers[key].append(dict(row))

    supplier_stats: list[dict[str, Any]] = []
    all_provider_keys = set(provider_history) | set(provider_offers)
    for provider_key in all_provider_keys:
        rows = provider_history.get(provider_key, [])
        total_ordered = sum(int(r.get("ordered_quantity") or 0) for r in rows)
        total_received = sum(int(r.get("received_quantity") or 0) for r in rows)
        fill_rate = (
            _round_stat(total_received / total_ordered * 100, 1)
            if total_ordered > 0
            else None
        )
        lead_vals = [
            int(r["actual_lead_days"])
            for r in rows
            if r.get("actual_lead_days") is not None
        ]
        avg_lead = (
            _round_stat(sum(lead_vals) / len(lead_vals), 1) if lead_vals else None
        )
        h_prices = [
            _to_decimal(r.get("price"))
            for r in rows
            if _to_decimal(r.get("price")) is not None
        ]
        avg_price = (
            round(float(sum(h_prices) / len(h_prices)), 2) if h_prices else None
        )
        sorted_rows = sorted(
            rows,
            key=lambda r: r.get("created_at") or datetime.min,
            reverse=True,
        )
        last_ordered_at = sorted_rows[0].get("created_at") if sorted_rows else None
        provider_name = (
            sorted_rows[0].get("provider_name")
            if sorted_rows
            else (
                provider_offers.get(provider_key, [{}])[0].get("provider_name")
                if provider_offers.get(provider_key)
                else str(provider_key)
            )
        )
        provider_id = (
            sorted_rows[0].get("provider_id")
            if sorted_rows
            else (
                provider_offers.get(provider_key, [{}])[0].get("provider_id")
                if provider_offers.get(provider_key)
                else None
            )
        )

        curr_offers = provider_offers.get(provider_key, [])
        best_curr = (
            min(curr_offers, key=_offer_sort_key) if curr_offers else None
        )
        effective_lead_days = (
            avg_lead
            if avg_lead is not None
            else (
                float(best_curr.get("max_delivery_day"))
                if best_curr and best_curr.get("max_delivery_day") is not None
                else (
                    float(best_curr.get("min_delivery_day"))
                    if best_curr and best_curr.get("min_delivery_day") is not None
                    else None
                )
            )
        )
        supplier_stats.append(
            {
                "provider_id": provider_id,
                "provider_name": provider_name,
                "order_count": len(rows),
                "fill_rate": fill_rate,
                "avg_lead_days": avg_lead,
                "effective_lead_days": effective_lead_days,
                "avg_price": avg_price,
                "last_ordered_at": last_ordered_at,
                "current_price": (
                    float(_to_decimal(best_curr.get("price")))  # type: ignore[arg-type]
                    if best_curr
                    else None
                ),
                "current_qty": (
                    int(best_curr.get("quantity") or 0) if best_curr else None
                ),
                "current_min_delivery": (
                    best_curr.get("min_delivery_day") if best_curr else None
                ),
                "current_max_delivery": (
                    best_curr.get("max_delivery_day") if best_curr else None
                ),
                "current_oem_number": (
                    best_curr.get("oem_number") if best_curr else None
                ),
                "current_brand_name": (
                    best_curr.get("brand_name") if best_curr else None
                ),
                "current_autopart_name": (
                    best_curr.get("autopart_name") if best_curr else None
                ),
                "current_autopart_id": (
                    best_curr.get("autopart_id") if best_curr else None
                ),
                "current_provider_config_id": (
                    best_curr.get("provider_config_id") if best_curr else None
                ),
                "current_provider_config_name": (
                    best_curr.get("provider_config_name") if best_curr else None
                ),
                "is_own_price": bool(
                    best_curr.get("is_own_price") if best_curr else False
                ),
            }
        )

    current_prices = [
        float(item["current_price"])
        for item in supplier_stats
        if item.get("current_price") is not None
    ]
    effective_leads = [
        float(item["effective_lead_days"])
        for item in supplier_stats
        if item.get("effective_lead_days") is not None
    ]
    min_price = min(current_prices) if current_prices else None
    max_price = max(current_prices) if current_prices else None
    min_lead = min(effective_leads) if effective_leads else None
    max_lead = max(effective_leads) if effective_leads else None

    def _normalized_inverse(
        value: Optional[float],
        minimum: Optional[float],
        maximum: Optional[float],
    ) -> float:
        if value is None:
            return 0.0
        if minimum is None or maximum is None or maximum <= minimum:
            return 100.0
        ratio = (float(value) - float(minimum)) / (float(maximum) - float(minimum))
        return max(0.0, min(100.0, 100.0 - ratio * 100.0))

    for item in supplier_stats:
        price_score = _normalized_inverse(
            item.get("current_price"), min_price, max_price
        )
        lead_score = _normalized_inverse(
            item.get("effective_lead_days"), min_lead, max_lead
        )
        fill_score = float(item.get("fill_rate") or 50.0)
        availability_score = 100.0 if int(item.get("current_qty") or 0) > 0 else 0.0
        weighted = (
            fill_score * 0.45
            + lead_score * 0.30
            + price_score * 0.15
            + availability_score * 0.10
        )
        item["score"] = round(weighted, 1)

    def _score(s: dict[str, Any]) -> tuple:
        has_current = s["current_price"] is not None
        fill = s["fill_rate"] or 0
        lead = s["effective_lead_days"] or 9999
        price = s["current_price"] or 9_999_999
        return (not has_current, -fill, lead, price)

    best_candidates = [
        item
        for item in supplier_stats
        if item.get("current_price") is not None and not item.get("is_own_price")
    ]
    best = min(best_candidates, key=_score) if best_candidates else None
    best_supplier = best if best else None
    return supplier_stats, best_supplier


def _select_best_supplier_by_price(
    supplier_stats: list[dict[str, Any]],
) -> Optional[dict[str, Any]]:
    candidates = [
        item
        for item in supplier_stats
        if item.get("current_price") is not None and not item.get("is_own_price")
    ]
    if not candidates:
        return None
    return min(
        candidates,
        key=lambda item: (
            float(item.get("current_price") or 9_999_999),
            float(item.get("effective_lead_days") or 9_999),
            -float(item.get("fill_rate") or 0),
        ),
    )


def _select_best_supplier_by_lead_time(
    supplier_stats: list[dict[str, Any]],
) -> Optional[dict[str, Any]]:
    candidates = [
        item
        for item in supplier_stats
        if item.get("current_price") is not None
        and item.get("effective_lead_days") is not None
        and not item.get("is_own_price")
    ]
    if not candidates:
        return None
    return min(
        candidates,
        key=lambda item: (
            float(item.get("effective_lead_days") or 9_999),
            -float(item.get("fill_rate") or 0),
            float(item.get("current_price") or 9_999_999),
        ),
    )


def _select_recommended_supplier_for_order(
    supplier_stats: list[dict[str, Any]],
) -> Optional[dict[str, Any]]:
    candidates = [
        item
        for item in supplier_stats
        if item.get("current_price") is not None and not item.get("is_own_price")
    ]
    if not candidates:
        return None

    in_stock_candidates = [
        item for item in candidates if int(item.get("current_qty") or 0) > 0
    ]
    base_candidates = in_stock_candidates or candidates

    reliable_candidates = [
        item
        for item in base_candidates
        if item.get("fill_rate") is None or float(item.get("fill_rate") or 0) >= 80.0
    ]
    preferred_candidates = reliable_candidates or base_candidates

    return min(
        preferred_candidates,
        key=lambda item: (
            float(item.get("current_price") or 9_999_999),
            -float(item.get("fill_rate") or 0),
            float(item.get("effective_lead_days") or 9_999),
            -int(item.get("current_qty") or 0),
        ),
    )


def _build_tracking_exceptions(
    *,
    own_price_analysis: Optional[dict[str, Any]],
    in_transit_qty: int,
    reorder_point: Optional[float],
    price_trend: Optional[str],
    price_trend_pct: Optional[float],
    best_supplier: Optional[dict[str, Any]],
    best_supplier_by_price: Optional[dict[str, Any]],
    best_supplier_by_lead_time: Optional[dict[str, Any]],
    missing_in_latest_pricelist: bool = False,
) -> list[dict[str, str]]:
    exceptions: list[dict[str, str]] = []

    if not best_supplier_by_price and not best_supplier_by_lead_time:
        exceptions.append(
            {
                "code": "no_supplier_offers",
                "severity": "critical",
                "title": "Нет актуальных предложений поставщиков",
                "description": (
                    "По позиции и кроссам сейчас нет доступных предложений "
                    "из прайсов поставщиков для создания закупки."
                ),
            }
        )

    if missing_in_latest_pricelist:
        exceptions.append(
            {
                "code": "missing_in_latest_pricelist",
                "severity": "critical",
                "title": "Позиция выпала из последнего нашего прайса",
                "description": (
                    "Позиция была в недавних выгрузках, но отсутствует "
                    "в последнем нашем прайсе. Её нужно отдельно проверить "
                    "и при необходимости срочно перезаказать."
                ),
            }
        )

    if own_price_analysis:
        current_qty = int(own_price_analysis.get("current_quantity") or 0)
        days_left = own_price_analysis.get("estimated_days_left_30_days")
        available_qty = current_qty + int(in_transit_qty or 0)
        if days_left is not None and days_left <= 7:
            exceptions.append(
                {
                    "code": "low_stock_cover_critical",
                    "severity": "critical",
                    "title": "Очень низкое покрытие остатком",
                    "description": (
                        "Текущего остатка и позиций в пути хватит "
                        f"примерно на {days_left} дн."
                    ),
                }
            )
        elif days_left is not None and days_left <= 14:
            exceptions.append(
                {
                    "code": "low_stock_cover_warning",
                    "severity": "warning",
                    "title": "Низкое покрытие остатком",
                    "description": (
                        "Текущего остатка и позиций в пути хватит "
                        f"примерно на {days_left} дн."
                    ),
                }
            )
        if reorder_point is not None and available_qty < reorder_point:
            exceptions.append(
                {
                    "code": "below_reorder_point",
                    "severity": "warning",
                    "title": "Позиция ниже точки дозаказа",
                    "description": (
                        f"Доступно {available_qty} шт с учётом позиций в пути, "
                        f"при точке дозаказа {round(float(reorder_point), 1)} шт."
                    ),
                }
            )

    if best_supplier and best_supplier.get("fill_rate") is not None and float(
        best_supplier.get("fill_rate") or 0
    ) < 80:
        exceptions.append(
            {
                "code": "supplier_fill_rate_low",
                "severity": "warning",
                "title": "Низкая надёжность лучшего поставщика",
                "description": (
                    f"{best_supplier.get('provider_name') or 'Поставщик'} "
                    "закрывает в среднем только "
                    f"{best_supplier.get('fill_rate')}% заказанного объёма."
                ),
            }
        )

    if price_trend == "up" and price_trend_pct is not None and price_trend_pct >= 10:
        exceptions.append(
            {
                "code": "purchase_price_growth",
                "severity": "info",
                "title": "Закупочная цена растёт",
                "description": (
                    "Средняя закупочная цена по позиции выросла "
                    f"на {price_trend_pct}% по сравнению с предыдущим периодом."
                ),
            }
        )

    return _prioritize_tracking_exceptions(exceptions, limit=5)


def _prioritize_tracking_exceptions(
    exceptions: list[dict[str, Any]],
    *,
    limit: Optional[int] = None,
) -> list[dict[str, Any]]:
    severity_rank = {"critical": 0, "warning": 1, "info": 2}
    prioritized = sorted(
        [
            item
            for item in exceptions
            if isinstance(item, dict) and item.get("code")
        ],
        key=lambda item: (
            severity_rank.get(str(item.get("severity") or "").lower(), 99),
            str(item.get("title") or ""),
            str(item.get("code") or ""),
        ),
    )
    if limit is None:
        return prioritized
    return prioritized[:limit]


def _build_draft_purchase_order(
    *,
    own_price_analysis: Optional[dict[str, Any]],
    in_transit_qty: int,
    reorder_point: Optional[float],
    optimal_order_qty: Optional[float],
    supplier: Optional[dict[str, Any]],
) -> Optional[dict[str, Any]]:
    if not supplier or supplier.get("provider_id") is None:
        return None
    if supplier.get("current_price") is None:
        return None

    current_qty = int((own_price_analysis or {}).get("current_quantity") or 0)
    available_qty = current_qty + int(in_transit_qty or 0)
    avg_daily = (own_price_analysis or {}).get("average_daily_decrease_30_days")
    lead_days = (
        float(supplier.get("effective_lead_days"))
        if supplier.get("effective_lead_days") is not None
        else None
    )
    target_qty: Optional[int] = None
    reason: Optional[str] = None

    if avg_daily and float(avg_daily) > 0:
        cover_days = max(int(round((lead_days or 7) + 7)), 14)
        target_qty = int(ceil(float(avg_daily) * cover_days))
        reason = (
            f"Цель покрытия: около {cover_days} дн. с учётом темпа расхода "
            f"{round(float(avg_daily), 2)} шт/день."
        )
    elif optimal_order_qty is not None:
        target_qty = int(ceil(float(optimal_order_qty)))
        reason = "Использована рассчитанная оптимальная партия."
    elif reorder_point is not None:
        target_qty = int(ceil(float(reorder_point)))
        reason = "Использована точка дозаказа как минимальная цель."

    if target_qty is None:
        return None

    recommended_qty = max(target_qty - available_qty, 0)
    if recommended_qty <= 0 and reorder_point is not None and available_qty < reorder_point:
        recommended_qty = max(int(ceil(float(reorder_point) - available_qty)), 1)
        reason = "Доступный остаток ниже точки дозаказа."
    if recommended_qty <= 0:
        return None

    return {
        "provider_id": int(supplier["provider_id"]),
        "provider_name": supplier.get("provider_name") or "—",
        "provider_config_id": supplier.get("current_provider_config_id"),
        "provider_config_name": supplier.get("current_provider_config_name"),
        "autopart_id": supplier.get("current_autopart_id"),
        "oem_number": supplier.get("current_oem_number") or "",
        "brand_name": supplier.get("current_brand_name"),
        "autopart_name": supplier.get("current_autopart_name"),
        "price": supplier.get("current_price"),
        "available_qty": available_qty,
        "in_transit_qty": int(in_transit_qty or 0),
        "target_qty": target_qty,
        "recommended_qty": recommended_qty,
        "lead_days_used": lead_days,
        "reason": reason,
    }


async def _compute_abc_xyz(
    session: AsyncSession,
    *,
    normalized_oem_numbers: list[str],
    history_rows: list[dict[str, Any]],
) -> Optional[dict[str, Any]]:
    group_oems = {
        normalized
        for normalized in (_normalize_oem(item) for item in normalized_oem_numbers)
        if normalized
    }
    if not group_oems:
        return None

    cutoff = tracking_history_cutoff()
    cutoff_date = cutoff.date()
    monthly_qty: dict[str, int] = {}
    current_month = date.today().replace(day=1)
    months: list[str] = []
    year = current_month.year
    month = current_month.month
    for offset in range(11, -1, -1):
        y = year
        m = month - offset
        while m <= 0:
            m += 12
            y -= 1
        months.append(f"{y:04d}-{m:02d}")
        monthly_qty[f"{y:04d}-{m:02d}"] = 0

    annual_ordered_qty = 0
    for row in history_rows:
        dt = row.get("created_at")
        normalized_row_oem = _normalize_oem(row.get("oem_number"))
        if (
            not dt
            or dt.date() < cutoff_date
            or normalized_row_oem not in group_oems
        ):
            continue
        month_key = dt.strftime("%Y-%m")
        if month_key in monthly_qty:
            qty = int(row.get("ordered_quantity") or 0)
            monthly_qty[month_key] += qty
            annual_ordered_qty += qty

    series = [monthly_qty[month_key] for month_key in months]
    active_months = sum(1 for value in series if value > 0)
    xyz_class: Optional[str] = None
    monthly_cv: Optional[float] = None
    if series and annual_ordered_qty > 0:
        mean_value = sum(series) / len(series)
        if mean_value > 0:
            variance = sum((value - mean_value) ** 2 for value in series) / len(series)
            std_dev = sqrt(variance)
            monthly_cv = round(std_dev / mean_value, 2)
            if monthly_cv <= 0.5:
                xyz_class = "X"
            elif monthly_cv <= 1.0:
                xyz_class = "Y"
            else:
                xyz_class = "Z"

    qty_by_oem = await _load_tracking_market_qty_by_oem(session, cutoff=cutoff)

    total_market_qty = sum(qty_by_oem.values())
    abc_class: Optional[str] = None
    cumulative_share_pct: Optional[float] = None
    group_qty = sum(qty_by_oem.get(oem, 0) for oem in group_oems)
    if total_market_qty > 0 and group_qty > 0:
        sorted_values = sorted(qty_by_oem.values(), reverse=True)
        cumulative_before = sum(value for value in sorted_values if value > group_qty)
        cumulative_share = (cumulative_before + group_qty) / total_market_qty
        cumulative_share_pct = round(cumulative_share * 100, 1)
        if cumulative_share <= 0.8:
            abc_class = "A"
        elif cumulative_share <= 0.95:
            abc_class = "B"
        else:
            abc_class = "C"

    return {
        "abc_class": abc_class,
        "xyz_class": xyz_class,
        "annual_ordered_qty": annual_ordered_qty,
        "monthly_cv": monthly_cv,
        "active_months": active_months,
        "cumulative_share_pct": cumulative_share_pct,
    }


async def _compute_single_oem_abc_xyz_batch(
    session: AsyncSession,
    *,
    normalized_oem_numbers: list[str],
    history_rows_by_oem: dict[str, list[dict[str, Any]]],
) -> dict[str, dict[str, Any]]:
    normalized_unique_oems = [
        normalized
        for normalized in {
            _normalize_oem(item) for item in normalized_oem_numbers if _normalize_oem(item)
        }
    ]
    if not normalized_unique_oems:
        return {}

    cutoff = tracking_history_cutoff()
    cutoff_date = cutoff.date()
    current_month = date.today().replace(day=1)
    months: list[str] = []
    year = current_month.year
    month = current_month.month
    for offset in range(11, -1, -1):
        y = year
        m = month - offset
        while m <= 0:
            m += 12
            y -= 1
        months.append(f"{y:04d}-{m:02d}")

    monthly_qty_by_oem: dict[str, dict[str, int]] = {
        oem: {month_key: 0 for month_key in months}
        for oem in normalized_unique_oems
    }
    annual_ordered_qty_by_oem = {oem: 0 for oem in normalized_unique_oems}

    for oem_number in normalized_unique_oems:
        monthly_qty = monthly_qty_by_oem[oem_number]
        for row in history_rows_by_oem.get(oem_number, []):
            dt = row.get("created_at")
            if not dt or dt.date() < cutoff_date:
                continue
            month_key = dt.strftime("%Y-%m")
            if month_key not in monthly_qty:
                continue
            qty = int(row.get("ordered_quantity") or 0)
            monthly_qty[month_key] += qty
            annual_ordered_qty_by_oem[oem_number] += qty

    qty_by_oem = await _load_tracking_market_qty_by_oem(session, cutoff=cutoff)

    total_market_qty = sum(qty_by_oem.values())
    sorted_values = sorted(qty_by_oem.values(), reverse=True)
    cumulative_before_cache: dict[int, int] = {}
    result: dict[str, dict[str, Any]] = {}

    for oem_number in normalized_unique_oems:
        series = [
            monthly_qty_by_oem[oem_number][month_key]
            for month_key in months
        ]
        annual_ordered_qty = annual_ordered_qty_by_oem[oem_number]
        active_months = sum(1 for value in series if value > 0)
        xyz_class: Optional[str] = None
        monthly_cv: Optional[float] = None
        if series and annual_ordered_qty > 0:
            mean_value = sum(series) / len(series)
            if mean_value > 0:
                variance = sum((value - mean_value) ** 2 for value in series) / len(
                    series
                )
                std_dev = sqrt(variance)
                monthly_cv = round(std_dev / mean_value, 2)
                if monthly_cv <= 0.5:
                    xyz_class = "X"
                elif monthly_cv <= 1.0:
                    xyz_class = "Y"
                else:
                    xyz_class = "Z"

        group_qty = qty_by_oem.get(oem_number, 0)
        abc_class: Optional[str] = None
        cumulative_share_pct: Optional[float] = None
        if total_market_qty > 0 and group_qty > 0:
            cumulative_before = cumulative_before_cache.get(group_qty)
            if cumulative_before is None:
                cumulative_before = sum(
                    value for value in sorted_values if value > group_qty
                )
                cumulative_before_cache[group_qty] = cumulative_before
            cumulative_share = (cumulative_before + group_qty) / total_market_qty
            cumulative_share_pct = round(cumulative_share * 100, 1)
            if cumulative_share <= 0.8:
                abc_class = "A"
            elif cumulative_share <= 0.95:
                abc_class = "B"
            else:
                abc_class = "C"

        result[oem_number] = {
            "abc_class": abc_class,
            "xyz_class": xyz_class,
            "annual_ordered_qty": annual_ordered_qty,
            "monthly_cv": monthly_cv,
            "active_months": active_months,
            "cumulative_share_pct": cumulative_share_pct,
        }

    return result


async def _load_tracking_market_qty_by_oem(
    session: AsyncSession,
    *,
    cutoff: datetime,
) -> dict[str, int]:
    cache_key = f"tracking_market_qty_by_oem:{cutoff.isoformat()}"
    cached = session.info.get(cache_key)
    if isinstance(cached, dict):
        return cached

    supplier_rows = (
        await session.execute(
            select(
                func.upper(SupplierOrderItem.oem_number).label("oem_number"),
                func.sum(SupplierOrderItem.quantity).label("qty"),
            )
            .select_from(SupplierOrderItem)
            .join(
                SupplierOrder,
                SupplierOrder.id == SupplierOrderItem.supplier_order_id,
            )
            .where(
                SupplierOrder.source_type == ORDER_TRACKING_SOURCE.SEARCH_OFFERS.value,
                SupplierOrder.created_at >= cutoff,
                SupplierOrderItem.oem_number.is_not(None),
            )
            .group_by(func.upper(SupplierOrderItem.oem_number))
        )
    ).all()
    site_rows = (
        await session.execute(
            select(
                func.upper(OrderItem.oem_number).label("oem_number"),
                func.sum(OrderItem.quantity).label("qty"),
            )
            .select_from(OrderItem)
            .join(Order, Order.id == OrderItem.order_id)
            .where(
                Order.source_type == ORDER_TRACKING_SOURCE.DRAGONZAP_SEARCH.value,
                Order.created_at >= cutoff,
                OrderItem.oem_number.is_not(None),
            )
            .group_by(func.upper(OrderItem.oem_number))
        )
    ).all()

    qty_by_oem: dict[str, int] = {}
    for oem_value, qty in list(supplier_rows) + list(site_rows):
        normalized = _normalize_oem(oem_value)
        if not normalized:
            continue
        qty_by_oem[normalized] = qty_by_oem.get(normalized, 0) + int(qty or 0)

    session.info[cache_key] = qty_by_oem
    return qty_by_oem


async def get_tracking_history_insights(
    session: AsyncSession,
    *,
    oem_number: Optional[str] = None,
    brand_name: Optional[str] = None,
    extra_oem_numbers: Optional[list[str]] = None,
    own_provider_config_id: Optional[int] = None,
) -> dict[str, Any]:
    normalized_oem = _normalize_oem(oem_number) or ""
    normalized_brand = _normalize_brand(brand_name)
    source_autoparts = (
        await _load_tracking_source_autoparts(
            session,
            normalized_oem=normalized_oem,
            normalized_brand=normalized_brand,
        )
        if normalized_oem
        else []
    )
    source_autopart_ids = [autopart.id for autopart in source_autoparts]
    invalid_oems, _invalid_autopart_ids = await _load_invalid_cross_state(
        session,
        source_autopart_ids=source_autopart_ids,
    )
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
        if (
            normalized
            and normalized != normalized_oem
            and normalized not in invalid_oems
        )
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
    cross_items = await _resolve_tracking_cross_items(
        session,
        oem_number=oem_number,
        brand_name=brand_name,
    )

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
    cross_offer_rows = [
        _build_offer_payload(row)
        for row in actionable_offer_rows
        if _normalize_oem(row.get("oem_number")) != normalized_oem
    ]

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

    # ── New analytics fields ─────────────────────────────────────────────────

    # 1. Average purchase price + trend (from actually-received history rows)
    purchase_price_stats = _compute_purchase_price_stats(history_rows)

    # 2. In-transit quantity (ordered but not yet received, active statuses)
    in_transit_qty = max(
        sum(
            int(row.get("ordered_quantity") or 0)
            - int(row.get("received_quantity") or 0)
            for row in history_rows
            if row.get("current_status") in _ACTIVE_ORDER_STATUSES
        ),
        0,
    )

    # 3. Seasonality breakdown
    seasonality, peak_months = _compute_seasonality(history_rows)

    # 4. Invalid-cross items for display (full detail, not just ids/oems)
    invalid_cross_items: list[dict[str, Any]] = []
    if source_autopart_ids:
        InvalidBrand = aliased(Brand, flat=True)
        InvalidAutopart = aliased(AutoPart, flat=True)
        inv_stmt = (
            select(
                AutoPartInvalidCross.id,
                AutoPartInvalidCross.source_autopart_id,
                AutoPartInvalidCross.invalid_oem_number,
                AutoPartInvalidCross.invalid_brand_name_raw,
                AutoPartInvalidCross.comment,
                AutoPartInvalidCross.invalid_autopart_id,
                InvalidBrand.name.label("invalid_brand_name"),
                InvalidAutopart.name.label("invalid_autopart_name"),
            )
            .outerjoin(
                InvalidBrand,
                InvalidBrand.id == AutoPartInvalidCross.invalid_brand_id,
            )
            .outerjoin(
                InvalidAutopart,
                InvalidAutopart.id == AutoPartInvalidCross.invalid_autopart_id,
            )
            .where(
                AutoPartInvalidCross.source_autopart_id.in_(source_autopart_ids)
            )
            .order_by(
                func.coalesce(
                    InvalidBrand.name,
                    AutoPartInvalidCross.invalid_brand_name_raw,
                ).asc(),
                AutoPartInvalidCross.invalid_oem_number.asc(),
            )
        )
        inv_rows = (await session.execute(inv_stmt)).all()
        invalid_cross_items = [
            {
                "id": r.id,
                "invalid_brand_name": (
                    r.invalid_brand_name or r.invalid_brand_name_raw
                ),
                "invalid_oem_number": r.invalid_oem_number,
                "invalid_autopart_name": r.invalid_autopart_name,
                "comment": r.comment,
            }
            for r in inv_rows
        ]

    # 5. Per-supplier stats + best supplier
    supplier_stats, best_supplier = _compute_supplier_stats(
        history_rows, list(current_offer_rows)
    )
    best_supplier_by_price = _select_best_supplier_by_price(supplier_stats)
    best_supplier_by_lead_time = _select_best_supplier_by_lead_time(
        supplier_stats
    )

    # 6. Reorder point + optimal order qty (uses own-price analysis)
    average_actual_lead_days = (
        _round_stat(sum(actual_lead_values) / len(actual_lead_values), 1)
        if actual_lead_values
        else None
    )
    reorder_point: Optional[float] = None
    optimal_order_qty: Optional[float] = None
    if own_price_analysis:
        avg_daily = own_price_analysis.get("average_daily_decrease_30_days")
        if avg_daily and average_actual_lead_days:
            reorder_point = round(float(avg_daily) * float(average_actual_lead_days), 1)
            optimal_order_qty = round(
                float(avg_daily) * float(average_actual_lead_days) * 1.5, 1
            )

    # 7. Markup / margin (our selling price vs average purchase price)
    markup_percent: Optional[float] = None
    margin_percent: Optional[float] = None
    avg_purchase_price = purchase_price_stats.get("avg_purchase_price")
    if own_price_analysis and avg_purchase_price:
        selling_price = own_price_analysis.get("latest_price")
        if selling_price is not None:
            sp = float(selling_price)
            pp = float(avg_purchase_price)
            if pp > 0:
                markup_percent = round((sp - pp) / pp * 100, 1)
            if sp > 0:
                margin_percent = round((sp - pp) / sp * 100, 1)

    exceptions = _build_tracking_exceptions(
        own_price_analysis=own_price_analysis,
        in_transit_qty=in_transit_qty,
        reorder_point=reorder_point,
        price_trend=purchase_price_stats["price_trend"],
        price_trend_pct=purchase_price_stats["price_trend_pct"],
        best_supplier=best_supplier,
        best_supplier_by_price=best_supplier_by_price,
        best_supplier_by_lead_time=best_supplier_by_lead_time,
    )
    recommended_supplier = _select_recommended_supplier_for_order(
        supplier_stats
    )
    if recommended_supplier is None:
        recommended_supplier = (
            best_supplier
            or best_supplier_by_price
            or best_supplier_by_lead_time
        )
    draft_purchase_order = _build_draft_purchase_order(
        own_price_analysis=own_price_analysis,
        in_transit_qty=in_transit_qty,
        reorder_point=reorder_point,
        optimal_order_qty=optimal_order_qty,
        supplier=recommended_supplier,
    )
    abc_xyz = await _compute_abc_xyz(
        session,
        normalized_oem_numbers=normalized_oem_numbers or [normalized_oem],
        history_rows=history_rows,
    )

    # ────────────────────────────────────────────────────────────────────────

    return {
        "oem_number": normalized_oem,
        "resolved_oem_numbers": normalized_oem_numbers or (
            [normalized_oem] if normalized_oem else []
        ),
        "cross_oem_numbers": cross_oem_numbers,
        "site_cross_oem_numbers": extra_normalized_oems,
        "cross_items": cross_items,
        "exact_min_offer": exact_min_offer,
        "min_offer_with_crosses": min_offer_with_crosses,
        "cross_offer_rows": cross_offer_rows,
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
        "average_actual_lead_days": average_actual_lead_days,
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
        # ── new ──
        "avg_purchase_price": purchase_price_stats["avg_purchase_price"],
        "last_purchase_price": purchase_price_stats["last_purchase_price"],
        "price_trend": purchase_price_stats["price_trend"],
        "price_trend_pct": purchase_price_stats["price_trend_pct"],
        "markup_percent": markup_percent,
        "margin_percent": margin_percent,
        "in_transit_qty": in_transit_qty,
        "reorder_point": reorder_point,
        "optimal_order_qty": optimal_order_qty,
        "seasonality": seasonality,
        "peak_months": peak_months,
        "supplier_stats": supplier_stats,
        "best_supplier": best_supplier,
        "best_supplier_by_price": best_supplier_by_price,
        "best_supplier_by_lead_time": best_supplier_by_lead_time,
        "recommended_supplier": recommended_supplier,
        "draft_purchase_order": draft_purchase_order,
        "exceptions": exceptions,
        "abc_xyz": abc_xyz,
        "invalid_cross_items": invalid_cross_items,
    }


async def _load_tracking_history_rows_for_oems(
    session: AsyncSession,
    *,
    normalized_oem_numbers: list[str],
) -> list[dict[str, Any]]:
    if not normalized_oem_numbers:
        return []

    cutoff = tracking_history_cutoff()

    supplier_rows = (
        await session.execute(
            select(
                literal("supplier").label("source_type"),
                SupplierOrder.provider_id.label("provider_id"),
                Provider.name.label("provider_name"),
                func.upper(SupplierOrderItem.oem_number).label("oem_number"),
                Brand.name.label("brand_name"),
                SupplierOrderItem.autopart_name.label("autopart_name"),
                SupplierOrderItem.quantity.label("ordered_quantity"),
                SupplierOrderItem.received_quantity.label("received_quantity"),
                SupplierOrderItem.price.label("price"),
                SupplierOrder.created_at.label("created_at"),
                SupplierOrderItem.received_at.label("received_at"),
                SupplierOrder.status.label("order_status"),
            )
            .select_from(SupplierOrderItem)
            .join(
                SupplierOrder,
                SupplierOrder.id == SupplierOrderItem.supplier_order_id,
            )
            .join(Provider, Provider.id == SupplierOrder.provider_id)
            .outerjoin(
                AutoPart,
                AutoPart.id == SupplierOrderItem.autopart_id,
            )
            .outerjoin(Brand, Brand.id == AutoPart.brand_id)
            .where(
                SupplierOrder.source_type
                == ORDER_TRACKING_SOURCE.SEARCH_OFFERS.value,
                SupplierOrder.created_at >= cutoff,
                SupplierOrderItem.oem_number.is_not(None),
                func.upper(SupplierOrderItem.oem_number).in_(
                    normalized_oem_numbers
                ),
            )
        )
    ).mappings().all()

    site_rows = (
        await session.execute(
            select(
                literal("site").label("source_type"),
                Order.provider_id.label("provider_id"),
                Provider.name.label("provider_name"),
                func.upper(OrderItem.oem_number).label("oem_number"),
                OrderItem.brand_name.label("brand_name"),
                OrderItem.autopart_name.label("autopart_name"),
                OrderItem.quantity.label("ordered_quantity"),
                OrderItem.received_quantity.label("received_quantity"),
                OrderItem.price.label("price"),
                Order.created_at.label("created_at"),
                OrderItem.received_at.label("received_at"),
                Order.status.label("order_status"),
                OrderItem.status.label("item_status"),
            )
            .select_from(OrderItem)
            .join(Order, Order.id == OrderItem.order_id)
            .join(Provider, Provider.id == Order.provider_id)
            .where(
                Order.source_type
                == ORDER_TRACKING_SOURCE.DRAGONZAP_SEARCH.value,
                Order.created_at >= cutoff,
                OrderItem.oem_number.is_not(None),
                func.upper(OrderItem.oem_number).in_(normalized_oem_numbers),
            )
        )
    ).mappings().all()

    rows: list[dict[str, Any]] = []
    for row in supplier_rows:
        rows.append(
            {
                "source_type": row["source_type"],
                "provider_id": row["provider_id"],
                "provider_name": row["provider_name"],
                "oem_number": row["oem_number"],
                "brand_name": row.get("brand_name"),
                "autopart_name": row.get("autopart_name"),
                "ordered_quantity": int(row.get("ordered_quantity") or 0),
                "received_quantity": int(row.get("received_quantity") or 0),
                "price": row.get("price"),
                "created_at": row["created_at"],
                "received_at": row.get("received_at"),
                "current_status": (
                    row["order_status"].name
                    if getattr(row.get("order_status"), "name", None)
                    else str(row.get("order_status") or "")
                ),
                "actual_lead_days": (
                    max(
                        (
                            row["received_at"].date() - row["created_at"].date()
                        ).days,
                        0,
                    )
                    if row.get("received_at") is not None
                    else None
                ),
            }
        )

    for row in site_rows:
        order_status = row.get("order_status")
        item_status = row.get("item_status")
        current_status = (
            order_status.name
            if getattr(order_status, "name", None)
            else (
                item_status.name
                if getattr(item_status, "name", None)
                else str(order_status or item_status or "")
            )
        )
        rows.append(
            {
                "source_type": row["source_type"],
                "provider_id": row["provider_id"],
                "provider_name": row["provider_name"],
                "oem_number": row["oem_number"],
                "brand_name": row.get("brand_name"),
                "autopart_name": row.get("autopart_name"),
                "ordered_quantity": int(row.get("ordered_quantity") or 0),
                "received_quantity": int(row.get("received_quantity") or 0),
                "price": row.get("price"),
                "created_at": row["created_at"],
                "received_at": row.get("received_at"),
                "current_status": current_status,
                "actual_lead_days": (
                    max(
                        (
                            row["received_at"].date() - row["created_at"].date()
                        ).days,
                        0,
                    )
                    if row.get("received_at") is not None
                    else None
                ),
            }
        )

    return rows


async def get_tracking_exceptions_queue(
    session: AsyncSession,
    *,
    own_provider_config_id: Optional[int] = None,
    severity: Optional[str] = None,
    search: Optional[str] = None,
    limit: int = 200,
) -> dict[str, Any]:
    config_stmt = (
        select(
            ProviderPriceListConfig.id.label("provider_config_id"),
            ProviderPriceListConfig.provider_id.label("provider_id"),
            ProviderPriceListConfig.name_price.label("provider_config_name"),
            Provider.name.label("provider_name"),
        )
        .select_from(ProviderPriceListConfig)
        .join(Provider, Provider.id == ProviderPriceListConfig.provider_id)
        .where(Provider.is_own_price.is_(True))
    )
    if own_provider_config_id is not None:
        config_stmt = config_stmt.where(
            ProviderPriceListConfig.id == own_provider_config_id
        )
    else:
        config_stmt = config_stmt.where(
            ProviderPriceListConfig.use_for_order_insights.is_(True)
        )
    config_stmt = config_stmt.order_by(ProviderPriceListConfig.id.asc()).limit(1)
    config_row = (await session.execute(config_stmt)).mappings().first()
    if not config_row:
        raise ValueError("Не найден конфиг нашего прайса для очереди исключений")

    provider_config_id = int(config_row["provider_config_id"])

    snapshot_stmt = (
        select(
            PriceList.id.label("pricelist_id"),
            PriceList.date.label("pricelist_date"),
            AutoPart.id.label("autopart_id"),
            AutoPart.oem_number.label("oem_number"),
            AutoPart.name.label("autopart_name"),
            Brand.name.label("brand_name"),
            PriceListAutoPartAssociation.quantity.label("quantity"),
            PriceListAutoPartAssociation.price.label("price"),
        )
        .select_from(PriceListAutoPartAssociation)
        .join(
            PriceList,
            PriceList.id == PriceListAutoPartAssociation.pricelist_id,
        )
        .join(AutoPart, AutoPart.id == PriceListAutoPartAssociation.autopart_id)
        .join(Brand, Brand.id == AutoPart.brand_id)
        .where(
            PriceList.provider_config_id == provider_config_id,
            PriceList.is_active.is_(True),
        )
        .order_by(
            PriceList.date.asc(),
            PriceList.id.asc(),
            AutoPart.oem_number.asc(),
        )
    )
    snapshot_rows = list((await session.execute(snapshot_stmt)).mappings().all())
    if not snapshot_rows:
        return {
            "provider_config_id": provider_config_id,
            "provider_id": int(config_row["provider_id"]),
            "provider_name": config_row["provider_name"],
            "provider_config_name": config_row.get("provider_config_name"),
            "generated_at": now_moscow(),
            "total_items": 0,
            "critical_count": 0,
            "warning_count": 0,
            "info_count": 0,
            "rows": [],
        }

    snapshots_by_key: dict[tuple[date, int], dict[str, Any]] = {}
    for row in snapshot_rows:
        snapshot_key = (row["pricelist_date"], row["pricelist_id"])
        snapshot = snapshots_by_key.setdefault(
            snapshot_key,
            {
                "pricelist_date": row["pricelist_date"],
                "pricelist_id": row["pricelist_id"],
                "qty_by_oem": {},
            },
        )
        oem_number = _normalize_oem(row.get("oem_number"))
        if not oem_number:
            continue
        qty = int(row.get("quantity") or 0)
        snapshot["qty_by_oem"][oem_number] = (
            snapshot["qty_by_oem"].get(oem_number, 0) + qty
        )

    snapshots = list(snapshots_by_key.values())
    latest_snapshot_key = max(snapshots_by_key.keys())
    latest_snapshot_rows = [
        row
        for row in snapshot_rows
        if (row["pricelist_date"], row["pricelist_id"]) == latest_snapshot_key
    ]

    latest_rows_by_oem: dict[str, dict[str, Any]] = {}
    latest_known_rows_by_oem: dict[str, dict[str, Any]] = {}
    for row in snapshot_rows:
        oem_number = _normalize_oem(row.get("oem_number"))
        if not oem_number:
            continue
        latest_known_rows_by_oem[oem_number] = {
            "oem_number": oem_number,
            "autopart_id": row.get("autopart_id"),
            "brand_name": row.get("brand_name"),
            "autopart_name": row.get("autopart_name"),
            "latest_price": _to_decimal(row.get("price")),
            "last_seen_pricelist_date": row.get("pricelist_date"),
            "last_seen_pricelist_id": row.get("pricelist_id"),
        }

    for row in latest_snapshot_rows:
        oem_number = _normalize_oem(row.get("oem_number"))
        if not oem_number:
            continue
        entry = latest_rows_by_oem.setdefault(
            oem_number,
            {
                "oem_number": oem_number,
                "autopart_id": row.get("autopart_id"),
                "brand_name": row.get("brand_name"),
                "autopart_name": row.get("autopart_name"),
                "current_quantity": 0,
                "latest_price": None,
            },
        )
        qty = int(row.get("quantity") or 0)
        entry["current_quantity"] += qty
        price_value = _to_decimal(row.get("price"))
        if entry["latest_price"] is None or (
            price_value is not None and price_value < entry["latest_price"]
        ):
            entry["latest_price"] = price_value
            entry["autopart_id"] = row.get("autopart_id")
            entry["brand_name"] = row.get("brand_name")
            entry["autopart_name"] = row.get("autopart_name")

    normalized_oem_numbers = sorted(latest_known_rows_by_oem.keys())
    history_rows = await _load_tracking_history_rows_for_oems(
        session,
        normalized_oem_numbers=normalized_oem_numbers,
    )
    current_offer_rows = await _load_current_offer_candidates(
        session,
        normalized_oem_numbers=normalized_oem_numbers,
    )

    history_by_oem: dict[str, list[dict[str, Any]]] = {}
    for row in history_rows:
        normalized = _normalize_oem(row.get("oem_number"))
        if not normalized:
            continue
        history_by_oem.setdefault(normalized, []).append(row)

    offers_by_oem: dict[str, list[dict[str, Any]]] = {}
    for row in current_offer_rows:
        normalized = _normalize_oem(row.get("oem_number"))
        if not normalized:
            continue
        offers_by_oem.setdefault(normalized, []).append(dict(row))

    sold_last_30_by_oem = {oem: 0 for oem in normalized_oem_numbers}
    today = now_moscow().date()
    for previous_snapshot, current_snapshot in zip(snapshots, snapshots[1:]):
        if current_snapshot["pricelist_date"] < today - timedelta(days=30):
            continue
        previous_qty_by_oem = previous_snapshot["qty_by_oem"]
        current_qty_by_oem = current_snapshot["qty_by_oem"]
        for oem in normalized_oem_numbers:
            prev_qty = int(previous_qty_by_oem.get(oem, 0))
            curr_qty = int(current_qty_by_oem.get(oem, 0))
            sold_last_30_by_oem[oem] += max(prev_qty - curr_qty, 0)

    severity_filter = str(severity or "").strip().lower() or None
    search_filter = str(search or "").strip().lower()
    severity_rank = {"critical": 0, "warning": 1, "info": 2}
    rows: list[dict[str, Any]] = []

    for oem_number, known_row in latest_known_rows_by_oem.items():
        latest = latest_rows_by_oem.get(oem_number) or {
            **known_row,
            "current_quantity": 0,
        }
        oem_history = history_by_oem.get(oem_number, [])
        supplier_stats, best_supplier = _compute_supplier_stats(
            oem_history,
            offers_by_oem.get(oem_number, []),
        )
        best_supplier_by_price = _select_best_supplier_by_price(supplier_stats)
        best_supplier_by_lead_time = _select_best_supplier_by_lead_time(
            supplier_stats
        )
        recommended_supplier = _select_recommended_supplier_for_order(
            supplier_stats
        )
        if recommended_supplier is None:
            recommended_supplier = (
                best_supplier
                or best_supplier_by_price
                or best_supplier_by_lead_time
            )

        sold_last_30_days = int(sold_last_30_by_oem.get(oem_number, 0))
        average_daily_decrease_30_days = (
            float(
                (
                    Decimal(str(sold_last_30_days)) / Decimal("30")
                ).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
            )
            if sold_last_30_days > 0
            else None
        )
        current_quantity = int(latest.get("current_quantity") or 0)
        missing_in_latest_pricelist = oem_number not in latest_rows_by_oem
        estimated_days_left_30_days = (
            int(current_quantity / average_daily_decrease_30_days)
            if average_daily_decrease_30_days and average_daily_decrease_30_days > 0
            else None
        )

        lead_values = [
            int(row["actual_lead_days"])
            for row in oem_history
            if row.get("actual_lead_days") is not None
        ]
        average_actual_lead_days = (
            _round_stat(sum(lead_values) / len(lead_values), 1)
            if lead_values
            else None
        )
        reorder_point = (
            round(
                float(average_daily_decrease_30_days) * float(average_actual_lead_days),
                1,
            )
            if average_daily_decrease_30_days and average_actual_lead_days
            else None
        )
        optimal_order_qty = (
            round(float(reorder_point) * 1.5, 1)
            if reorder_point is not None
            else None
        )
        in_transit_qty = max(
            sum(
                max(
                    int(row.get("ordered_quantity") or 0)
                    - int(row.get("received_quantity") or 0),
                    0,
                )
                for row in oem_history
                if row.get("current_status") in _ACTIVE_ORDER_STATUSES
            ),
            0,
        )
        purchase_stats = _compute_purchase_price_stats(oem_history)
        exceptions = _build_tracking_exceptions(
            own_price_analysis={
                "current_quantity": current_quantity,
                "estimated_days_left_30_days": estimated_days_left_30_days,
            },
            in_transit_qty=in_transit_qty,
            reorder_point=reorder_point,
            price_trend=purchase_stats.get("price_trend"),
            price_trend_pct=purchase_stats.get("price_trend_pct"),
            best_supplier=recommended_supplier,
            best_supplier_by_price=best_supplier_by_price,
            best_supplier_by_lead_time=best_supplier_by_lead_time,
            missing_in_latest_pricelist=missing_in_latest_pricelist,
        )
        if not exceptions:
            continue
        if (
            missing_in_latest_pricelist
            and sold_last_30_days <= 0
            and in_transit_qty <= 0
        ):
            continue

        row_severity = min(
            (severity_rank.get(item["severity"], 99) for item in exceptions),
            default=99,
        )
        severity_value = next(
            (
                name
                for name, rank in severity_rank.items()
                if rank == row_severity
            ),
            "info",
        )
        if severity_filter and severity_value != severity_filter:
            continue

        search_blob = " ".join(
            [
                str(latest.get("brand_name") or known_row.get("brand_name") or ""),
                str(oem_number or ""),
                str(
                    latest.get("autopart_name")
                    or known_row.get("autopart_name")
                    or ""
                ),
                str((recommended_supplier or {}).get("provider_name") or ""),
            ]
        ).lower()
        if search_filter and search_filter not in search_blob:
            continue

        draft = _build_draft_purchase_order(
            own_price_analysis={
                "current_quantity": current_quantity,
                "average_daily_decrease_30_days": average_daily_decrease_30_days,
            },
            in_transit_qty=in_transit_qty,
            reorder_point=reorder_point,
            optimal_order_qty=optimal_order_qty,
            supplier=recommended_supplier,
        )

        rows.append(
            {
                "oem_number": oem_number,
                "brand_name": latest.get("brand_name") or known_row.get("brand_name"),
                "autopart_name": latest.get("autopart_name")
                or known_row.get("autopart_name"),
                "autopart_id": latest.get("autopart_id")
                or known_row.get("autopart_id"),
                "current_quantity": current_quantity,
                "latest_price": float(
                    latest["latest_price"]
                    if latest.get("latest_price") is not None
                    else known_row.get("latest_price")
                )
                if (
                    latest.get("latest_price") is not None
                    or known_row.get("latest_price") is not None
                )
                else None,
                "in_transit_qty": in_transit_qty,
                "sold_last_30_days": sold_last_30_days,
                "average_daily_decrease_30_days": average_daily_decrease_30_days,
                "estimated_days_left_30_days": estimated_days_left_30_days,
                "reorder_point": reorder_point,
                "optimal_order_qty": optimal_order_qty,
                "recommended_order_qty": (
                    int(draft["recommended_qty"])
                    if draft and draft.get("recommended_qty") is not None
                    else None
                ),
                "severity": severity_value,
                "exception_codes": [item["code"] for item in exceptions],
                "exception_titles": [item["title"] for item in exceptions],
                "best_supplier_by_price": best_supplier_by_price,
                "best_supplier_by_lead_time": best_supplier_by_lead_time,
                "recommended_supplier": recommended_supplier,
            }
        )

    rows.sort(
        key=lambda item: (
            severity_rank.get(item["severity"], 99),
            item["estimated_days_left_30_days"]
            if item["estimated_days_left_30_days"] is not None
            else 9_999,
            item["recommended_order_qty"]
            if item["recommended_order_qty"] is not None
            else 9_999,
            item["oem_number"],
        )
    )
    limited_rows = rows[:limit]

    return {
        "provider_config_id": provider_config_id,
        "provider_id": int(config_row["provider_id"]),
        "provider_name": config_row["provider_name"],
        "provider_config_name": config_row.get("provider_config_name"),
        "generated_at": now_moscow(),
        "total_items": len(rows),
        "critical_count": sum(1 for row in rows if row["severity"] == "critical"),
        "warning_count": sum(1 for row in rows if row["severity"] == "warning"),
        "info_count": sum(1 for row in rows if row["severity"] == "info"),
        "rows": limited_rows,
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
