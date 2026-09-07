from __future__ import annotations

import os
import re
from collections import Counter
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from difflib import SequenceMatcher
from typing import Any
from urllib.parse import urlparse

import aiohttp
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from dz_fastapi.core.base import Customer, CustomerExternalReference, CustomerOrder, OrderItem
from dz_fastapi.core.time import now_moscow
from dz_fastapi.services.partssoft_reconciliation import (
    PARTS_SOFT_SOURCE,
    CustomerMatcher,
    flatten_remote_customer,
    normalize_digits,
    normalize_email,
    normalize_name,
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


def _decimal(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _integer(value: Any) -> int | None:
    decimal = _decimal(value)
    if decimal is None or decimal != decimal.to_integral_value():
        return None
    return int(decimal)


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
    flattened = flatten_remote_customer(customer)
    flattened["external_id"] = customer.get("id") or order.get("customer_id")
    return flattened


def _partssoft_registration_source(customer: dict[str, Any]) -> str | None:
    candidates = [
        customer.get(key)
        for key in (
            "source_site",
            "registration_source",
            "site",
            "domain",
            "host",
            "referrer",
            "referer",
        )
    ]
    candidates.append(os.getenv("PARTSSOFT_DEFAULT_SOURCE_SITE"))
    candidates.append(
        os.getenv("V3_BASE_URL") or "https://admin.dragonzap.ru/api/v3"
    )
    for candidate in candidates:
        value = _text(candidate).casefold()
        if not value:
            continue
        host = urlparse(value if "://" in value else f"//{value}").hostname or value
        if "dragonzap.ru" in host:
            return "dragonzap.ru"
        if host == "zap.ru" or host.endswith(".zap.ru"):
            return "zap.ru"
    return None


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
    base_url = (os.getenv("V3_BASE_URL") or "https://admin.dragonzap.ru/api/v3").rstrip("/")
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


async def _fetch_customer(external_customer_id: int) -> dict[str, Any]:
    base_url = (os.getenv("V3_BASE_URL") or "https://admin.dragonzap.ru/api/v3").rstrip("/")
    username = os.getenv("V3_USERNAME")
    password = os.getenv("V3_PASSWORD")
    if not username or not password:
        raise RuntimeError("Parts-Soft API credentials are not configured")
    if not base_url.startswith("https://"):
        raise RuntimeError("Parts-Soft API URL must use HTTPS")
    auth = aiohttp.BasicAuth(username, password)
    async with aiohttp.ClientSession(
        auth=auth,
        timeout=aiohttp.ClientTimeout(total=30),
    ) as client:
        async with client.get(
            f"{base_url}/customers/{int(external_customer_id)}.json",
            allow_redirects=False,
        ) as response:
            response.raise_for_status()
            payload = await response.json()
    customer = payload.get("customer") if isinstance(payload, dict) else None
    if not isinstance(customer, dict):
        raise RuntimeError("Unexpected Parts-Soft customer response")
    return customer


def _candidate_score(
    customer: Customer,
    *,
    name: str,
    inn: str,
    kpp: str,
    email: str,
) -> tuple[int, list[str]]:
    score = 0
    basis: list[str] = []
    local_inn = normalize_digits(customer.inn)
    local_kpp = normalize_digits(customer.kpp)
    local_email = normalize_email(customer.email_contact)
    remote_name = normalize_name(name)
    local_name = normalize_name(customer.name)
    if inn and local_inn == inn:
        score += 80
        basis.append("ИНН")
        if kpp and local_kpp == kpp:
            score += 20
            basis.append("КПП")
    if email and local_email == email:
        score += 70
        basis.append("email")
    if remote_name and local_name:
        ratio = SequenceMatcher(None, remote_name, local_name).ratio()
        if ratio == 1:
            score += 60
            basis.append("название")
        elif ratio >= 0.55:
            score += round(ratio * 40)
            basis.append("похожее название")
    return score, basis


async def search_local_customer_candidates(
    session: AsyncSession,
    *,
    name: str = "",
    inn: str = "",
    kpp: str = "",
    email: str = "",
    search: str = "",
    limit: int = 25,
) -> list[dict[str, Any]]:
    customers = (await session.scalars(select(Customer))).all()
    normalized_search = _normalized_text(search)
    search_digits = normalize_digits(search)
    scored: list[tuple[int, Customer, list[str]]] = []
    for customer in customers:
        score, basis = _candidate_score(
            customer,
            name=name,
            inn=normalize_digits(inn),
            kpp=normalize_digits(kpp),
            email=normalize_email(email),
        )
        if normalized_search:
            haystacks = (
                _normalized_text(customer.id),
                _normalized_text(customer.name),
                normalize_email(customer.email_contact),
                normalize_digits(customer.inn),
                normalize_digits(customer.kpp),
            )
            matches_search = any(
                normalized_search in value for value in haystacks if value
            ) or bool(search_digits and any(search_digits in value for value in haystacks))
            if not matches_search:
                continue
            score += 100
            basis = ["ручной поиск", *basis]
        if score > 0 or normalized_search:
            scored.append((score, customer, basis))
    scored.sort(key=lambda row: (-row[0], normalize_name(row[1].name), row[1].id))
    return [
        {
            "id": customer.id,
            "name": customer.name,
            "inn": customer.inn,
            "kpp": customer.kpp,
            "email": customer.email_contact,
            "score": score,
            "match_basis": basis,
        }
        for score, customer, basis in scored[: max(1, min(limit, 50))]
    ]


async def link_partssoft_customer(
    session: AsyncSession,
    *,
    external_customer_id: int,
    local_customer_id: int,
) -> dict[str, Any]:
    remote_raw = await _fetch_customer(external_customer_id)
    remote = _remote_customer({"customer": remote_raw})
    customer = await session.get(Customer, int(local_customer_id))
    if customer is None:
        raise LookupError("Local customer not found")

    existing = (
        await session.scalars(
            select(CustomerExternalReference).where(
                CustomerExternalReference.source_system == PARTS_SOFT_SOURCE,
                CustomerExternalReference.external_customer_id == int(external_customer_id),
            )
        )
    ).first()
    if existing is not None and existing.customer_id != customer.id:
        raise ValueError(
            f"Parts-Soft customer is already linked to local customer {existing.customer_id}"
        )

    filled_fields: list[str] = []
    conflicts: list[str] = []
    field_map = {
        "inn": "inn",
        "kpp": "kpp",
        "email": "email_contact",
        "company_type": "company_type",
        "legal_address": "legal_address",
        "postal_address": "postal_address",
        "phone": "phone",
        "additional_phone": "additional_phone",
        "vat_rate": "vat_rate",
        "bank_bik": "bank_bik",
        "bank_name": "bank_name",
        "bank_city": "bank_city",
        "bank_account": "bank_account",
        "correspondent_account": "correspondent_account",
        "credit_limit": "credit_limit",
        "payment_terms_days": "payment_terms_days",
    }
    normalizers = {
        "inn": normalize_digits,
        "kpp": normalize_digits,
        "email": normalize_email,
    }
    converters = {
        "credit_limit": _decimal,
        "vat_rate": _decimal,
        "payment_terms_days": _integer,
    }
    for remote_field, local_field in field_map.items():
        raw_remote_value = remote.get(remote_field)
        converted_remote_value = converters.get(remote_field, lambda value: value)(
            raw_remote_value
        )
        remote_value = _text(converted_remote_value)
        local_value = _text(getattr(customer, local_field, None))
        if remote_value and not local_value:
            if remote_field == "email":
                if not customer.is_valid_email(remote_value):
                    conflicts.append("email_contact_invalid")
                    continue
                email_owner = (
                    await session.scalars(
                        select(Customer).where(
                            Customer.email_contact.ilike(remote_value),
                            Customer.id != customer.id,
                        )
                    )
                ).first()
                if email_owner is not None:
                    conflicts.append("email_contact_used_by_another_customer")
                    continue
            setattr(customer, local_field, converted_remote_value)
            filled_fields.append(local_field)
        elif remote_value and local_value and (
            normalizers.get(remote_field, _normalized_text)(remote_value)
            != normalizers.get(remote_field, _normalized_text)(local_value)
        ):
            conflicts.append(local_field)

    if (
        remote.get("name")
        and customer.name
        and normalize_name(remote["name"]) != normalize_name(customer.name)
    ):
        conflicts.append("name")

    source_site = _partssoft_registration_source(remote_raw)
    if source_site and not _text(customer.registration_source):
        customer.registration_source = source_site
        filled_fields.append("registration_source")
    elif (
        source_site
        and _text(customer.registration_source).casefold() != source_site.casefold()
    ):
        conflicts.append("registration_source")

    if existing is None:
        existing = CustomerExternalReference(
            customer_id=customer.id,
            source_system=PARTS_SOFT_SOURCE,
            external_customer_id=int(external_customer_id),
        )
        session.add(existing)
    existing.external_customer_name = remote["name"] or None
    existing.external_classification = remote.get("partssoft_classification") or {}
    existing.external_payload = remote_raw
    existing.last_synced_at = now_moscow()
    existing.is_active = True
    try:
        await session.commit()
    except IntegrityError as exc:
        await session.rollback()
        raise ValueError("Customer link conflicts with an existing link") from exc
    await session.refresh(existing)
    return {
        "reference_id": existing.id,
        "external_customer_id": int(external_customer_id),
        "local_customer_id": customer.id,
        "local_customer_name": customer.name,
        "filled_fields": filled_fields,
        "conflicting_fields": conflicts,
    }


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
                "customer_email": remote_customer["email"],
                "local_customer_id": local_customer_id,
                "suggested_local_customer_id": customer_match.local_id,
                "customer_linked": customer_match.classification == "linked",
                "customer_match_classification": customer_match.classification,
                "customer_match_basis": customer_match.basis,
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
