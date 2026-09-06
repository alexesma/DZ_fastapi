from __future__ import annotations

import os
import re
from collections import Counter
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any

import aiohttp
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from dz_fastapi.core.base import Customer, CustomerExternalReference, CustomerOrder, OrderItem
from dz_fastapi.core.time import now_moscow
from dz_fastapi.services.partssoft_reconciliation import (
    PARTS_SOFT_SOURCE,
    CustomerMatcher,
    normalize_digits,
)

MOSCOW_REGION_ID = 28
MAX_RECONCILIATION_DAYS = 7
PAGE_SIZE = 100
TRACKING_UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
    re.IGNORECASE,
)


def _text(value: Any) -> str:
    return str(value or "").strip()


def _normalized_text(value: Any) -> str:
    return _text(value).casefold()


def _money(value: Any) -> str:
    if value in (None, ""):
        return ""
    try:
        return str(Decimal(str(value)).quantize(Decimal("0.01")))
    except (InvalidOperation, ValueError):
        return _text(value)


def _parse_datetime(value: Any) -> datetime | None:
    raw = _text(value)
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None


def _remote_customer(order: dict[str, Any]) -> dict[str, Any]:
    customer = order.get("customer") or {}
    essential = customer.get("essential") or {}
    return {
        "external_id": customer.get("id") or order.get("customer_id"),
        "name": essential.get("company_name") or customer.get("compile_name") or "",
        "email": customer.get("email_org") or customer.get("email") or "",
        "inn": normalize_digits(essential.get("inn")),
        "kpp": normalize_digits(essential.get("kpp")),
    }


def _remote_order_number(order: dict[str, Any]) -> str:
    for key in ("load_order_client_number", "id_1c", "external_crm_id", "order_id"):
        value = _text(order.get(key))
        if value:
            return value
    return _text(order.get("id"))


def _remote_item_signature(item: dict[str, Any]) -> tuple[str, str, int, str]:
    return (
        _normalized_text(item.get("oem")),
        _normalized_text(item.get("make_name")),
        int(item.get("qnt") or 0),
        _money(item.get("cost")),
    )


def _local_item_signature(item: Any) -> tuple[str, str, int, str]:
    return (
        _normalized_text(item.oem),
        _normalized_text(item.brand),
        int(item.requested_qty or 0),
        _money(item.requested_price),
    )


def remote_order_fingerprint(order: dict[str, Any]) -> tuple[tuple[str, str, int, str], ...]:
    return tuple(sorted(_remote_item_signature(item) for item in order.get("order_items") or []))


def local_order_fingerprint(order: CustomerOrder) -> tuple[tuple[str, str, int, str], ...]:
    return tuple(sorted(_local_item_signature(item) for item in order.items or []))


async def _fetch_orders(days: int) -> tuple[datetime, datetime, list[dict[str, Any]]]:
    end = now_moscow()
    start = end - timedelta(days=days)
    base_url = os.getenv("V3_BASE_URL", "https://admin.dragonzap.ru/api/v3").rstrip("/")
    username = os.getenv("V3_USERNAME")
    password = os.getenv("V3_PASSWORD")
    if not username or not password:
        raise RuntimeError("Parts-Soft API credentials are not configured")
    if not base_url.startswith("https://"):
        raise RuntimeError("Parts-Soft API URL must use HTTPS")

    rows: list[dict[str, Any]] = []
    auth = aiohttp.BasicAuth(username, password)
    timeout = aiohttp.ClientTimeout(total=90)
    async with aiohttp.ClientSession(auth=auth, timeout=timeout) as client:
        page = 1
        while True:
            params = {
                "page": page,
                "per_page": PAGE_SIZE,
                "search[customer_region_id_eq]": MOSCOW_REGION_ID,
                "search[created_at_gteq]": start.isoformat(),
                "search[created_at_lteq]": end.isoformat(),
            }
            async with client.get(
                f"{base_url}/orders.json",
                params=params,
                allow_redirects=False,
            ) as response:
                response.raise_for_status()
                payload = await response.json()
            page_rows = payload.get("orders") if isinstance(payload, dict) else None
            if not isinstance(page_rows, list):
                raise RuntimeError("Unexpected Parts-Soft orders response")
            rows.extend(page_rows)
            if len(page_rows) < PAGE_SIZE:
                break
            page += 1
    return start, end, rows


async def reconcile_partssoft_orders(
    session: AsyncSession,
    *,
    days: int = MAX_RECONCILIATION_DAYS,
) -> dict[str, Any]:
    days = max(1, min(int(days), MAX_RECONCILIATION_DAYS))
    start, end, remote_orders = await _fetch_orders(days)

    references = (
        await session.scalars(
            select(CustomerExternalReference).where(
                CustomerExternalReference.source_system == PARTS_SOFT_SOURCE,
                CustomerExternalReference.is_active.is_(True),
            )
        )
    ).all()
    reference_by_customer = {row.customer_id: row.external_customer_id for row in references}
    local_customers = (await session.scalars(select(Customer))).all()
    matcher = CustomerMatcher(
        [
            {
                "id": row.id,
                "external_id": reference_by_customer.get(row.id),
                "name": row.name,
                "email": row.email_contact,
                "inn": row.inn,
                "kpp": row.kpp,
            }
            for row in local_customers
        ]
    )

    external_orders = (
        await session.scalars(
            select(CustomerOrder).where(
                CustomerOrder.external_source == PARTS_SOFT_SOURCE,
                CustomerOrder.external_order_id.is_not(None),
            )
        )
    ).all()
    external_order_map = {_text(row.external_order_id): row.id for row in external_orders}

    tracking_values = {
        _text(item.get("comment")).lower()
        for order in remote_orders
        for item in order.get("order_items") or []
        if TRACKING_UUID_RE.fullmatch(_text(item.get("comment")))
    }
    tracking_map: dict[str, int] = {}
    if tracking_values:
        tracking_rows = (
            await session.execute(
                select(OrderItem.tracking_uuid, OrderItem.order_id).where(
                    OrderItem.tracking_uuid.in_(tracking_values)
                )
            )
        ).all()
        tracking_map = {
            _text(row.tracking_uuid).lower(): int(row.order_id) for row in tracking_rows
        }

    local_order_start = start - timedelta(days=1)
    local_orders = (
        (
            await session.scalars(
                select(CustomerOrder)
                .options(selectinload(CustomerOrder.items))
                .where(CustomerOrder.received_at >= local_order_start)
            )
        )
        .unique()
        .all()
    )
    local_orders_by_customer: dict[int, list[CustomerOrder]] = {}
    for order in local_orders:
        local_orders_by_customer.setdefault(int(order.customer_id), []).append(order)

    results: list[dict[str, Any]] = []
    counts: Counter[str] = Counter()
    excluded_without_inn = 0
    for remote_order in remote_orders:
        remote_customer = _remote_customer(remote_order)
        if not remote_customer["inn"]:
            excluded_without_inn += 1
            continue

        external_order_id = _text(remote_order.get("id"))
        items = remote_order.get("order_items") or []
        item_tracking_ids = [
            _text(item.get("comment")).lower()
            for item in items
            if TRACKING_UUID_RE.fullmatch(_text(item.get("comment")))
        ]
        matched_site_order_ids = {
            tracking_map[value] for value in item_tracking_ids if value in tracking_map
        }
        matched_tracking_count = sum(value in tracking_map for value in item_tracking_ids)

        customer_match = matcher.match(remote_customer)
        local_customer_id = (
            customer_match.local_id
            if customer_match.classification in {"linked", "auto_match"}
            else None
        )
        matched_local_order_id: int | None = None
        classification: str
        match_basis: str

        if external_order_id in external_order_map:
            classification = "existing_external"
            match_basis = "external_order_id"
            matched_local_order_id = external_order_map[external_order_id]
        elif matched_site_order_ids and len(matched_site_order_ids) == 1:
            if item_tracking_ids and matched_tracking_count == len(item_tracking_ids):
                classification = "existing_site_order"
                match_basis = "tracking_uuid"
            else:
                classification = "partial_site_match"
                match_basis = "partial_tracking_uuid"
            matched_local_order_id = next(iter(matched_site_order_ids))
        elif len(matched_site_order_ids) > 1:
            classification = "order_conflict"
            match_basis = "tracking_uuid_multiple_orders"
        elif not local_customer_id:
            classification = (
                "customer_conflict"
                if customer_match.classification in {"review_match", "ambiguous"}
                else "customer_unmatched"
            )
            match_basis = customer_match.basis
        else:
            classification = "new_order"
            match_basis = customer_match.basis
            remote_number = _normalized_text(_remote_order_number(remote_order))
            remote_created_at = _parse_datetime(remote_order.get("created_at"))
            remote_fingerprint = remote_order_fingerprint(remote_order)
            for local_order in local_orders_by_customer.get(int(local_customer_id), []):
                if remote_number and remote_number == _normalized_text(local_order.order_number):
                    classification = "probable_duplicate"
                    match_basis = "customer_order_number"
                    matched_local_order_id = local_order.id
                    break
                local_date = local_order.order_date or (
                    local_order.received_at.date() if local_order.received_at else None
                )
                if (
                    remote_created_at
                    and local_date == remote_created_at.date()
                    and remote_fingerprint
                    and remote_fingerprint == local_order_fingerprint(local_order)
                ):
                    classification = "probable_duplicate"
                    match_basis = "customer_date_items"
                    matched_local_order_id = local_order.id
                    break

        counts[classification] += 1
        created_at = _parse_datetime(remote_order.get("created_at"))
        results.append(
            {
                "external_order_id": external_order_id,
                "order_number": _remote_order_number(remote_order),
                "created_at": created_at.isoformat() if created_at else None,
                "external_customer_id": remote_customer["external_id"],
                "customer_name": remote_customer["name"],
                "customer_inn": remote_customer["inn"],
                "customer_kpp": remote_customer["kpp"],
                "local_customer_id": local_customer_id,
                "local_order_id": matched_local_order_id,
                "classification": classification,
                "match_basis": match_basis,
                "items_count": len(items),
                "tracking_items_count": len(item_tracking_ids),
                "matched_tracking_items_count": matched_tracking_count,
                "importable": classification == "new_order",
                "review_required": classification
                in {
                    "partial_site_match",
                    "order_conflict",
                    "probable_duplicate",
                    "customer_conflict",
                    "customer_unmatched",
                },
            }
        )

    return {
        "read_only": True,
        "days": days,
        "date_from": start.isoformat(),
        "date_to": end.isoformat(),
        "region_id": MOSCOW_REGION_ID,
        "qualification": "region Moscow and customer INN is present",
        "remote_orders_total": len(remote_orders),
        "excluded_without_inn": excluded_without_inn,
        "qualified_orders": len(results),
        "items_total": sum(row["items_count"] for row in results),
        "counts": dict(sorted(counts.items())),
        "orders": results,
    }
