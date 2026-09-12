from __future__ import annotations

import logging
import os
import re
from collections import Counter
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from difflib import SequenceMatcher
from typing import Any
from urllib.parse import urljoin, urlparse

import aiohttp
from sqlalchemy import delete, func, or_, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from dz_fastapi.api.validators import normalize_brand_name
from dz_fastapi.core.base import (
    Client,
    Customer,
    CustomerExternalReference,
    CustomerOrder,
    CustomerOrderItem,
    OrderItem,
    PartsSoftOrderSnapshot,
    Photo,
)
from dz_fastapi.core.time import now_moscow
from dz_fastapi.models.autopart import (
    TYPE_SUPPLIER_DECISION_STATUS,
    AutoPart,
    preprocess_oem_number,
)
from dz_fastapi.models.brand import Brand
from dz_fastapi.models.partner import CUSTOMER_ORDER_STATUS, TYPE_PRICES
from dz_fastapi.models.user import User, UserRole, UserStatus
from dz_fastapi.schemas.order import OrderPositionOut
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
AUTOMATIC_SYNC_DAYS = 1
PARTS_SOFT_RECOVERY_ORIGIN = "partssoft_recovery"
PARTSSOFT_API_AUTO_ORDER_ENABLED = os.getenv(
    "PARTSSOFT_API_AUTO_ORDER_ENABLED", "1"
).strip().lower() in {"1", "true", "yes", "on"}
PAGE_SIZE = 100
TRACKING_UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
    re.IGNORECASE,
)
PARTSSOFT_AUTO_TRACKING_RE = re.compile(r"^ps-\d+-\d+$", re.IGNORECASE)
logger = logging.getLogger("dz_fastapi")


def _partssoft_api_settings() -> tuple[str, str, str]:
    base_url = (os.getenv("V3_BASE_URL") or "https://admin.dragonzap.ru/api/v3").rstrip("/")
    username = os.getenv("V3_USERNAME")
    password = os.getenv("V3_PASSWORD")
    if not username or not password:
        raise RuntimeError("Parts-Soft API credentials are not configured")
    if not base_url.startswith("https://"):
        raise RuntimeError("Parts-Soft API URL must use HTTPS")
    return base_url, username, password


def _partssoft_public_url(path: Any) -> str | None:
    value = _text(path)
    if not value:
        return None
    base_url = (
        os.getenv("V3_BASE_URL") or "https://admin.dragonzap.ru/api/v3"
    ).rstrip("/")
    parsed = urlparse(base_url)
    origin = f"{parsed.scheme}://{parsed.netloc}/"
    return urljoin(origin, value)


def _text(value: Any) -> str:
    return str(value or "").strip()


def _normalized_text(value: Any) -> str:
    return _text(value).casefold()


def _is_order_tracking_token(value: Any) -> bool:
    normalized = _text(value)
    return bool(
        TRACKING_UUID_RE.fullmatch(normalized)
        or PARTSSOFT_AUTO_TRACKING_RE.fullmatch(normalized)
    )


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


async def _fetch_orders(
    days: int,
    *,
    region_id: int | None = MOSCOW_REGION_ID,
) -> tuple[datetime, datetime, list[dict[str, Any]]]:
    end = now_moscow()
    start = end - timedelta(days=days)
    base_url, username, password = _partssoft_api_settings()

    rows: list[dict[str, Any]] = []
    auth = aiohttp.BasicAuth(username, password)
    timeout = aiohttp.ClientTimeout(total=90)
    async with aiohttp.ClientSession(auth=auth, timeout=timeout) as client:
        page = 1
        while True:
            params = {
                "page": page,
                "per_page": PAGE_SIZE,
                "search[created_at_gteq]": start.isoformat(),
                "search[created_at_lteq]": end.isoformat(),
            }
            if region_id is not None:
                params["search[customer_region_id_eq]"] = region_id
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
    base_url, username, password = _partssoft_api_settings()
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


async def _store_order_snapshots(
    session: AsyncSession,
    remote_orders: list[dict[str, Any]],
) -> None:
    now = now_moscow()
    cutoff = now - timedelta(days=MAX_RECONCILIATION_DAYS)
    external_ids = [value for row in remote_orders if (value := _text(row.get("id")))]
    existing = {
        row.external_order_id: row
        for row in (
            await session.scalars(
                select(PartsSoftOrderSnapshot).where(
                    PartsSoftOrderSnapshot.external_order_id.in_(external_ids)
                )
            )
        ).all()
    } if external_ids else {}
    for remote_order in remote_orders:
        external_order_id = _text(remote_order.get("id"))
        if not external_order_id:
            continue
        remote_customer = _remote_customer(remote_order)
        snapshot = existing.get(external_order_id)
        if snapshot is None:
            snapshot = PartsSoftOrderSnapshot(external_order_id=external_order_id)
            session.add(snapshot)
        snapshot.order_created_at = _parse_datetime(remote_order.get("created_at"))
        snapshot.external_customer_id = _integer(remote_customer.get("external_id"))
        snapshot.payload = remote_order
        snapshot.last_seen_at = now
    await session.execute(
        delete(PartsSoftOrderSnapshot).where(
            or_(
                PartsSoftOrderSnapshot.order_created_at < cutoff,
                PartsSoftOrderSnapshot.order_created_at.is_(None),
            )
        )
    )
    await session.commit()


async def _load_order_snapshots(
    session: AsyncSession,
    *,
    start: datetime,
) -> list[dict[str, Any]]:
    rows = (
        await session.scalars(
            select(PartsSoftOrderSnapshot)
            .where(PartsSoftOrderSnapshot.order_created_at >= start)
            .order_by(PartsSoftOrderSnapshot.order_created_at.desc())
        )
    ).all()
    return [row.payload for row in rows if isinstance(row.payload, dict)]


async def _fetch_products(
    updated_since: datetime | None = None,
) -> list[dict[str, Any]]:
    """Read all product cards owned by this Parts-Soft installation."""
    base_url, username, password = _partssoft_api_settings()
    rows: list[dict[str, Any]] = []
    auth = aiohttp.BasicAuth(username, password)
    timeout = aiohttp.ClientTimeout(total=90)
    async with aiohttp.ClientSession(auth=auth, timeout=timeout) as client:
        page = 1
        while True:
            params: dict[str, Any] = {"page": page, "per_page": PAGE_SIZE}
            if updated_since is not None:
                params["search[updated_at_gteq]"] = updated_since.isoformat()
            async with client.get(
                f"{base_url}/products.json",
                params=params,
                allow_redirects=False,
            ) as response:
                response.raise_for_status()
                payload = await response.json()
            page_rows = payload.get("products") if isinstance(payload, dict) else None
            if not isinstance(page_rows, list):
                raise RuntimeError("Unexpected Parts-Soft products response")
            rows.extend(row for row in page_rows if isinstance(row, dict))
            if len(page_rows) < PAGE_SIZE:
                break
            page += 1
    return rows


def _positive_float(value: Any) -> float | None:
    parsed = _decimal(value)
    if parsed is None or parsed <= 0:
        return None
    return float(parsed)


def _product_photo_urls(product: dict[str, Any]) -> list[str]:
    candidates: list[Any] = [product.get("product_photo_url")]
    candidates.extend(product.get("external_image_urls") or [])
    candidates.extend(
        row.get("photo_url")
        for row in (product.get("images") or [])
        if isinstance(row, dict)
    )
    result: list[str] = []
    seen: set[str] = set()
    for candidate in candidates:
        url = _partssoft_public_url(candidate)
        if url and url not in seen:
            seen.add(url)
            result.append(url)
    return result


async def sync_partssoft_products(
    session: AsyncSession,
    *,
    full: bool = False,
) -> dict[str, Any]:
    """Merge Parts-Soft product cards into local nomenclature without duplicates."""
    updated_since = None
    if not full:
        updated_since = await session.scalar(
            select(func.max(AutoPart.partssoft_product_updated_at))
        )
    remote_products = await _fetch_products(updated_since=updated_since)
    counts: Counter[str] = Counter()
    synced_ids: list[int] = []
    conflicts: list[dict[str, Any]] = []

    brands = list((await session.scalars(select(Brand))).all())
    brands_by_name = {
        normalize_brand_name(brand.name): brand
        for brand in brands
        if normalize_brand_name(brand.name)
    }
    incoming_photo_urls = {
        url
        for product in remote_products
        for url in _product_photo_urls(product)
    }
    photos_by_url = {
        photo.url: photo
        for photo in (
            await session.scalars(
                select(Photo).where(Photo.url.in_(incoming_photo_urls))
            )
        ).all()
    } if incoming_photo_urls else {}

    for product in remote_products:
        external_id = _integer(product.get("id"))
        oem = preprocess_oem_number(_text(product.get("oem")))
        brand_name = normalize_brand_name(_text(product.get("make_name")))
        if external_id is None or not oem or not brand_name:
            counts["invalid"] += 1
            continue

        autopart = await session.scalar(
            select(AutoPart).where(AutoPart.partssoft_product_id == external_id)
        )
        brand = brands_by_name.get(brand_name)
        if brand is None:
            brand = Brand(name=brand_name)
            session.add(brand)
            await session.flush()
            brands_by_name[brand_name] = brand
            counts["brands_created"] += 1

        if autopart is None:
            autopart = await session.scalar(
                select(AutoPart).where(
                    AutoPart.brand_id == brand.id,
                    AutoPart.oem_number == oem,
                )
            )
        if (
            autopart is not None
            and autopart.partssoft_product_id not in (None, external_id)
        ):
            counts["external_id_conflicts"] += 1
            conflicts.append(
                {
                    "external_product_id": external_id,
                    "linked_external_product_id": autopart.partssoft_product_id,
                    "autopart_id": int(autopart.id),
                    "brand": brand_name,
                    "oem": oem,
                }
            )
            continue
        created = autopart is None
        name = (
            _text(product.get("detail_name"))
            or _text(product.get("name"))
            or oem
        )
        description = (
            _text(product.get("body"))
            or _text(product.get("seo_text"))
            or _text(product.get("meta_description"))
        )
        if created:
            autopart = AutoPart(
                brand_id=brand.id,
                oem_number=oem,
                name=name,
                description=description or None,
                width=_positive_float(product.get("width")),
                height=_positive_float(product.get("height")),
                length=_positive_float(product.get("length")),
                weight=_positive_float(product.get("weight")),
            )
            session.add(autopart)
            await session.flush()
            counts["created"] += 1
        else:
            if not _text(autopart.description) and description:
                autopart.description = description
                counts["descriptions_filled"] += 1
            for field in ("width", "height", "length", "weight"):
                if getattr(autopart, field, None) in (None, 0):
                    value = _positive_float(product.get(field))
                    if value is not None:
                        setattr(autopart, field, value)
            counts["updated"] += 1

        autopart.partssoft_product_id = external_id
        autopart.partssoft_product_updated_at = _parse_datetime(product.get("updated_at"))
        autopart.partssoft_synced_at = now_moscow()
        autopart.partssoft_payload = product
        session.add(autopart)
        await session.flush()

        for url in _product_photo_urls(product):
            photo = photos_by_url.get(url)
            if photo is None:
                photo = Photo(url=url, autopart_id=autopart.id)
                session.add(photo)
                photos_by_url[url] = photo
                counts["photos_added"] += 1
            elif photo.autopart_id == autopart.id:
                counts["photos_existing"] += 1
        synced_ids.append(int(autopart.id))

    await session.commit()
    return {
        "mode": "full" if updated_since is None else "incremental",
        "updated_since": updated_since.isoformat() if updated_since else None,
        "remote_products_total": len(remote_products),
        "counts": dict(sorted(counts.items())),
        "synced_autoparts": len(synced_ids),
        "conflicts": conflicts,
        "synced_at": now_moscow().isoformat(),
    }


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
    merge_existing_customer: bool = False,
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
    merged_customer_id: int | None = None
    if existing is not None and existing.customer_id != customer.id and existing.is_verified:
        if not merge_existing_customer:
            raise ValueError(
                f"Parts-Soft customer is already linked to local customer {existing.customer_id}. "
                "Choose duplicate merge to move its data."
            )
        merged_customer_id = int(existing.customer_id)
        try:
            await _merge_customer_into_target(
                session,
                source_customer_id=merged_customer_id,
                target_customer=customer,
            )
        except IntegrityError as exc:
            await session.rollback()
            raise ValueError(
                "Клиентов нельзя объединить автоматически: конфликтуют связанные настройки"
            ) from exc

    filled_fields: list[str] = []
    conflicts: list[str] = []
    field_map = {
        "legal_name": "legal_name",
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
    else:
        existing.customer_id = customer.id
    existing.external_customer_name = remote["name"] or None
    existing.external_classification = remote.get("partssoft_classification") or {}
    existing.external_payload = remote_raw
    existing.last_synced_at = now_moscow()
    existing.is_active = True
    existing.is_verified = True
    existing.match_basis = "manual"
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
        "merged_customer_id": merged_customer_id,
    }


async def _merge_customer_into_target(
    session: AsyncSession,
    *,
    source_customer_id: int,
    target_customer: Customer,
) -> None:
    if source_customer_id == target_customer.id:
        return
    source = await session.get(Customer, source_customer_id)
    if source is None:
        raise LookupError("Duplicate customer not found")

    merge_fields = (
        "legal_name", "inn", "kpp", "legal_address", "postal_address",
        "company_type", "phone", "additional_phone", "vat_rate", "bank_bik",
        "bank_name", "bank_city", "bank_account", "correspondent_account",
        "registration_source", "credit_limit", "payment_terms_days",
        "return_window_days", "description", "comment",
    )
    for field in merge_fields:
        if not _text(getattr(target_customer, field, None)) and _text(getattr(source, field, None)):
            setattr(target_customer, field, getattr(source, field))

    for table in Customer.metadata.tables.values():
        if table.name in {Customer.__table__.name, Client.__table__.name}:
            continue
        for column in table.columns:
            if any(fk.target_fullname == "customer.id" for fk in column.foreign_keys):
                await session.execute(
                    update(table)
                    .where(column == source_customer_id)
                    .values({column.name: target_customer.id})
                )
    await session.execute(
        delete(Customer.__table__).where(Customer.__table__.c.id == source_customer_id)
    )
    await session.execute(
        delete(Client.__table__).where(Client.__table__.c.id == source_customer_id)
    )
    session.expunge(source)


def _item_value(item: dict[str, Any], *keys: str) -> Any:
    containers = [
        item,
        item.get("offer"),
        item.get("price"),
        item.get("source"),
        item.get("supplier"),
    ]
    for container in containers:
        if not isinstance(container, dict):
            continue
        for key in keys:
            value = container.get(key)
            if value not in (None, ""):
                return value
    return None


async def _unique_customer_name(session: AsyncSession, preferred: str, external_id: int) -> str:
    base = _text(preferred) or f"Parts-Soft #{external_id}"
    candidate = base[:255]
    suffix = 1
    while (
        await session.scalar(
            select(Client.id).where(func.lower(Client.name) == candidate.casefold()).limit(1)
        )
        is not None
    ):
        suffix += 1
        tail = f" · Parts-Soft {external_id}-{suffix}"
        candidate = f"{base[: max(1, 255 - len(tail))]}{tail}"
    return candidate


def _fill_customer_from_partssoft(customer: Customer, remote: dict[str, Any]) -> None:
    for remote_field, local_field in {
        "legal_name": "legal_name",
        "inn": "inn",
        "kpp": "kpp",
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
    }.items():
        value = remote.get(remote_field)
        if value in (None, "") or _text(getattr(customer, local_field, None)):
            continue
        converter = {
            "vat_rate": _decimal,
            "credit_limit": _decimal,
            "payment_terms_days": _integer,
        }.get(remote_field, lambda item: item)
        converted = converter(value)
        if converted is not None:
            setattr(customer, local_field, converted)


async def _resolve_sync_customer(
    session: AsyncSession,
    remote_order: dict[str, Any],
) -> tuple[Customer | None, str]:
    remote_raw = remote_order.get("customer") or {}
    remote = _remote_customer(remote_order)
    external_id = _integer(remote.get("external_id"))
    if external_id is None:
        return None, "customer_external_id_missing"

    reference = await session.scalar(
        select(CustomerExternalReference).where(
            CustomerExternalReference.source_system == PARTS_SOFT_SOURCE,
            CustomerExternalReference.external_customer_id == external_id,
            CustomerExternalReference.is_active.is_(True),
            CustomerExternalReference.is_verified.is_(True),
        )
    )
    customer = await session.get(Customer, reference.customer_id) if reference else None
    customer_created = False
    if customer is None:
        candidates: list[Customer] = []
        inn = normalize_digits(remote.get("inn"))
        kpp = normalize_digits(remote.get("kpp"))
        email = normalize_email(remote.get("email"))
        if inn and kpp:
            candidates = list(
                (
                    await session.scalars(
                        select(Customer).where(Customer.inn == inn, Customer.kpp == kpp)
                    )
                ).all()
            )
        if not candidates and inn:
            same_inn = list(
                (
                    await session.scalars(
                        select(Customer).where(Customer.inn == inn)
                    )
                ).all()
            )
            if same_inn:
                return None, "customer_legal_details_conflict"
        if not candidates and not inn and email:
            candidates = list(
                (
                    await session.scalars(
                        select(Customer).where(func.lower(Customer.email_contact) == email)
                    )
                ).all()
            )
        if not candidates and not inn and normalize_digits(remote.get("phone")):
            phone = normalize_digits(remote.get("phone"))
            all_customers = (await session.scalars(select(Customer))).all()
            candidates = [
                row
                for row in all_customers
                if phone
                in {
                    normalize_digits(row.phone),
                    normalize_digits(row.additional_phone),
                }
            ]
        unique_ids = {int(row.id) for row in candidates}
        if len(unique_ids) > 1:
            return None, "customer_match_ambiguous"
        customer = candidates[0] if len(unique_ids) == 1 else None

    if customer is None:
        preferred_name = (
            _text(remote.get("display_name"))
            or normalize_email(remote.get("email"))
            or _text(remote.get("name"))
        )
        customer = Customer(
            name=await _unique_customer_name(session, preferred_name, external_id),
            legal_name=_text(remote.get("legal_name")) or None,
            email_contact=(
                remote.get("email")
                if remote.get("email") and Customer.is_valid_email(remote.get("email"))
                else None
            ),
            type_prices=(TYPE_PRICES.WHOLESALE if remote.get("inn") else TYPE_PRICES.RETAIL),
            registration_source=_partssoft_registration_source(remote_raw),
        )
        session.add(customer)
        await session.flush()
        customer_created = True

    _fill_customer_from_partssoft(customer, remote)
    if not _text(customer.registration_source):
        customer.registration_source = _partssoft_registration_source(remote_raw)
    if reference is None:
        reference_created = True
        reference = await session.scalar(
            select(CustomerExternalReference).where(
                CustomerExternalReference.source_system == PARTS_SOFT_SOURCE,
                CustomerExternalReference.external_customer_id == external_id,
            )
        )
        if reference is None:
            reference = CustomerExternalReference(
                customer_id=customer.id,
                source_system=PARTS_SOFT_SOURCE,
                external_customer_id=external_id,
            )
        elif not reference.is_verified:
            reference.customer_id = customer.id
    else:
        reference_created = False
    reference.external_customer_name = remote.get("legal_name") or remote.get("name") or None
    reference.external_classification = remote.get("partssoft_classification") or {}
    reference.external_payload = remote_raw
    reference.last_synced_at = now_moscow()
    reference.is_active = True
    if reference_created:
        # A newly created retail card belongs to this exact Parts-Soft ID.
        # Names are deliberately not used as identity, so two Alexanders stay
        # separate customers. Existing-card matches still require review.
        reference.is_verified = bool(
            not remote.get("inn")
            and (
                customer_created
                or reference.match_basis == "created_from_partssoft"
            )
        )
        reference.match_basis = (
            "created_from_partssoft" if customer_created else "automatic_match"
        )
    session.add_all([customer, reference])
    await session.flush()
    return customer, "created" if reference_created else "linked"


def _remote_item_to_customer_order_item(
    item: dict[str, Any],
    *,
    order_id: int,
    row_index: int,
) -> CustomerOrderItem | None:
    oem = _text(_item_value(item, "oem", "oem_number", "article"))
    brand = _text(_item_value(item, "make_name", "brand_name", "brand"))
    quantity = _integer(_item_value(item, "qnt", "quantity", "qty"))
    if not oem or not brand or quantity is None or quantity <= 0:
        return None
    hash_key = _text(_item_value(item, "hash_key", "api_hash"))
    system_hash = _text(_item_value(item, "system_hash"))
    provider_id = _text(_item_value(item, "supplier_id", "provider_id"))
    provider_name = _text(_item_value(item, "supplier_name", "provider_name"))
    requested_price = _decimal(_item_value(item, "cost", "price"))
    price_id = _text(_item_value(item, "price_id"))
    if (
        hash_key
        and (provider_id or provider_name)
        and requested_price is not None
        and requested_price > 0
    ):
        resolution = "api_ready"
    elif price_id:
        resolution = "partssoft_price"
    else:
        # Parts-Soft уже принял эту строку заказа, но не раскрыл источник
        # предложения в orders.json. Не пытаемся заказать её на сайте повторно.
        resolution = "partssoft_managed"
    source_payload = {
        "raw": item,
        "partssoft_price_id": price_id or None,
        "hash_key": hash_key or None,
        "system_hash": system_hash or None,
        "supplier_name": provider_name or None,
        "price_name": _item_value(item, "price_name"),
        "sup_logo": _item_value(item, "sup_logo"),
        "min_delivery_day": _item_value(item, "min_delivery_day"),
        "max_delivery_day": _item_value(item, "max_delivery_day"),
    }
    return CustomerOrderItem(
        order_id=order_id,
        external_order_item_id=_text(_item_value(item, "id", "order_item_id")) or None,
        external_offer_id=_text(_item_value(item, "offer_id", "price_id")) or None,
        external_provider_id=provider_id or None,
        external_warehouse_id=(
            _text(_item_value(item, "warehouse_id", "store_id")) or None
        ),
        source_resolution_status=resolution,
        source_payload=source_payload,
        row_index=row_index,
        oem=oem,
        brand=brand,
        name=_text(_item_value(item, "detail_name", "name")) or None,
        requested_qty=quantity,
        requested_price=requested_price,
    )


async def _auto_order_api_items(
    session: AsyncSession,
    *,
    order_ids: list[int],
) -> Counter[str]:
    counts: Counter[str] = Counter()
    if not PARTSSOFT_API_AUTO_ORDER_ENABLED or not order_ids:
        return counts
    if not _text(os.getenv("KEY_FOR_WEBSITE")):
        counts["api_order_skipped_missing_key"] += 1
        logger.error("Parts-Soft API auto-order skipped: KEY_FOR_WEBSITE is missing")
        return counts
    current_user = await session.scalar(
        select(User)
        .where(User.status == UserStatus.ACTIVE)
        .order_by((User.role == UserRole.ADMIN).desc(), User.id.asc())
        .limit(1)
    )
    if current_user is None:
        counts["api_order_skipped_no_user"] += 1
        logger.error("Parts-Soft API auto-order skipped: no active application user")
        return counts

    from dz_fastapi.services.site_order_sender import send_dragonzap_site_order

    orders = (
        (
            await session.scalars(
                select(CustomerOrder)
                .options(selectinload(CustomerOrder.items))
                .where(CustomerOrder.id.in_(order_ids))
            )
        )
        .unique()
        .all()
    )
    for order in orders:
        groups: dict[tuple[str, str], list[CustomerOrderItem]] = {}
        for item in order.items or []:
            if item.source_resolution_status != "api_ready":
                if item.source_resolution_status == "unresolved":
                    counts["source_unresolved_items"] += 1
                continue
            payload = item.source_payload or {}
            provider_id = _text(item.external_provider_id)
            provider_name = _text(payload.get("supplier_name"))
            groups.setdefault((provider_id, provider_name), []).append(item)
        for (provider_id, provider_name), items in groups.items():
            positions = [
                OrderPositionOut(
                    autopart_id=item.autopart_id,
                    oem_number=item.oem,
                    brand_name=item.brand,
                    autopart_name=item.name,
                    supplier_id=(int(provider_id) if provider_id.isdigit() else None),
                    supplier_name=provider_name or None,
                    price_name=(item.source_payload or {}).get("price_name"),
                    sup_logo=(item.source_payload or {}).get("sup_logo"),
                    quantity=int(item.requested_qty),
                    confirmed_price=float(item.requested_price or 0),
                    min_delivery_day=_integer(
                        (item.source_payload or {}).get("min_delivery_day")
                    ),
                    max_delivery_day=_integer(
                        (item.source_payload or {}).get("max_delivery_day")
                    ),
                    status=TYPE_SUPPLIER_DECISION_STATUS.SEND,
                    tracking_uuid=f"ps-{order.id}-{item.id}",
                    hash_key=(item.source_payload or {}).get("hash_key"),
                    system_hash=(item.source_payload or {}).get("system_hash"),
                )
                for item in items
            ]
            try:
                response = await send_dragonzap_site_order(
                    session=session,
                    current_user=current_user,
                    request=positions,
                    customer_id=int(order.customer_id),
                    order_comment=f"Автозаказ для Parts-Soft #{order.external_order_id}",
                )
                results = {
                    _text(row.get("request_tracking_uuid") or row.get("tracking_uuid")): row
                    for row in response.results
                }
                for item in items:
                    result = results.get(f"ps-{order.id}-{item.id}") or {}
                    if result.get("status") == "success":
                        item.source_resolution_status = "api_ordered"
                        counts["api_ordered_items"] += 1
                    else:
                        item.source_resolution_status = "api_order_error"
                        counts["api_order_errors"] += 1
                    session.add(item)
                await session.commit()
                logger.info(
                    "Parts-Soft API auto-order completed: customer_order_id=%s "
                    "external_order_id=%s supplier=%s items=%s successful=%s",
                    order.id,
                    order.external_order_id,
                    provider_id or provider_name,
                    len(items),
                    sum(
                        1
                        for item in items
                        if item.source_resolution_status == "api_ordered"
                    ),
                )
            except Exception:
                logger.exception(
                    "Parts-Soft API auto-order failed: customer_order_id=%s "
                    "external_order_id=%s supplier=%s",
                    order.id,
                    order.external_order_id,
                    provider_id or provider_name,
                )
                await session.rollback()
                for item in items:
                    item.source_resolution_status = "api_order_error"
                    session.add(item)
                await session.commit()
                counts["api_order_errors"] += len(items)
    return counts


async def sync_partssoft_orders(
    session: AsyncSession,
    *,
    days: int = AUTOMATIC_SYNC_DAYS,
) -> dict[str, Any]:
    """Import only orders absent locally; suspected duplicates stay in reconciliation."""
    days = max(1, min(int(days), AUTOMATIC_SYNC_DAYS))
    start, end, remote_orders = await _fetch_orders(days, region_id=None)
    snapshot_count = await session.scalar(select(func.count(PartsSoftOrderSnapshot.id)))
    if not snapshot_count:
        _, _, snapshot_orders = await _fetch_orders(MAX_RECONCILIATION_DAYS, region_id=None)
        await _store_order_snapshots(session, snapshot_orders)
    else:
        await _store_order_snapshots(session, remote_orders)
    counts: Counter[str] = Counter()
    imported_ids: list[int] = []

    existing_external_ids = set(
        (
            await session.scalars(
                select(CustomerOrder.external_order_id).where(
                    CustomerOrder.external_source == PARTS_SOFT_SOURCE,
                    CustomerOrder.external_order_id.is_not(None),
                )
            )
        ).all()
    )
    for remote_order in remote_orders:
        external_order_id = _text(remote_order.get("id"))
        if not external_order_id or external_order_id in existing_external_ids:
            counts["already_imported"] += 1
            continue

        tracking_ids = [
            _text(item.get("comment")).lower()
            for item in remote_order.get("order_items") or []
            if _is_order_tracking_token(item.get("comment"))
        ]
        if tracking_ids and await session.scalar(
            select(OrderItem.id).where(OrderItem.tracking_uuid.in_(tracking_ids)).limit(1)
        ):
            counts["existing_site_order"] += 1
            continue

        customer, customer_result = await _resolve_sync_customer(session, remote_order)
        if customer is None:
            await session.rollback()
            counts[customer_result] += 1
            continue

        remote_created_at = _parse_datetime(remote_order.get("created_at"))
        remote_date = remote_created_at.date() if remote_created_at else None
        fingerprint = remote_order_fingerprint(remote_order)
        probable_orders = (
            (
                await session.scalars(
                    select(CustomerOrder)
                    .options(selectinload(CustomerOrder.items))
                    .where(
                        CustomerOrder.customer_id == customer.id,
                        or_(
                            CustomerOrder.order_date == remote_date,
                            func.date(CustomerOrder.received_at) == remote_date,
                        ),
                    )
                )
            )
            .unique()
            .all()
            if remote_date is not None
            else []
        )
        if fingerprint and any(
            local_order_fingerprint(order) == fingerprint for order in probable_orders
        ):
            await session.commit()
            counts["probable_duplicate"] += 1
            continue

        order = CustomerOrder(
            customer_id=customer.id,
            external_source=PARTS_SOFT_SOURCE,
            external_order_id=external_order_id,
            import_origin=PARTS_SOFT_RECOVERY_ORIGIN,
            recovered_at=now_moscow(),
            external_payload=remote_order,
            status=CUSTOMER_ORDER_STATUS.NEW,
            received_at=remote_created_at or now_moscow(),
            source_email=_remote_customer(remote_order).get("email") or None,
            source_subject="Восстановлен из Parts-Soft",
            order_number=_remote_order_number(remote_order) or external_order_id,
            order_date=remote_date or now_moscow().date(),
        )
        session.add(order)
        await session.flush()
        items = [
            parsed
            for index, item in enumerate(remote_order.get("order_items") or [], start=1)
            if (
                parsed := _remote_item_to_customer_order_item(
                    item,
                    order_id=order.id,
                    row_index=index,
                )
            )
            is not None
        ]
        if not items:
            await session.rollback()
            counts["invalid_items"] += 1
            continue
        session.add_all(items)
        try:
            await session.commit()
        except IntegrityError:
            await session.rollback()
            counts["already_imported"] += 1
            continue
        imported_ids.append(int(order.id))
        existing_external_ids.add(external_order_id)
        counts["imported"] += 1

    counts.update(await _auto_order_api_items(session, order_ids=imported_ids))

    return {
        "days": days,
        "date_from": start.isoformat(),
        "date_to": end.isoformat(),
        "remote_orders_total": len(remote_orders),
        "counts": dict(sorted(counts.items())),
        "imported_order_ids": imported_ids,
    }


async def reconcile_partssoft_orders(
    session: AsyncSession,
    *,
    days: int = MAX_RECONCILIATION_DAYS,
    refresh_remote: bool = False,
) -> dict[str, Any]:
    days = max(1, min(int(days), MAX_RECONCILIATION_DAYS))
    end = now_moscow()
    start = end - timedelta(days=days)
    if refresh_remote:
        start, end, remote_orders = await _fetch_orders(days, region_id=None)
        await _store_order_snapshots(session, remote_orders)
    else:
        remote_orders = await _load_order_snapshots(session, start=start)
        if not remote_orders:
            start, end, remote_orders = await _fetch_orders(days, region_id=None)
            await _store_order_snapshots(session, remote_orders)

    references = (
        await session.scalars(
            select(CustomerExternalReference).where(
                CustomerExternalReference.source_system == PARTS_SOFT_SOURCE,
                CustomerExternalReference.is_active.is_(True),
                CustomerExternalReference.is_verified.is_(True),
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
                "phone": row.phone,
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
        if _is_order_tracking_token(item.get("comment"))
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
    for remote_order in remote_orders:
        remote_customer = _remote_customer(remote_order)

        external_order_id = _text(remote_order.get("id"))
        items = remote_order.get("order_items") or []
        item_tracking_ids = [
            _text(item.get("comment")).lower()
            for item in items
            if _is_order_tracking_token(item.get("comment"))
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
                "is_own_site_order": bool(
                    item_tracking_ids
                    and classification in {"existing_site_order", "partial_site_match"}
                ),
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
        "cached": not refresh_remote,
        "days": days,
        "date_from": start.isoformat(),
        "date_to": end.isoformat(),
        "region_id": MOSCOW_REGION_ID,
        "qualification": "all regions and all customer types",
        "remote_orders_total": len(remote_orders),
        "excluded_without_inn": 0,
        "qualified_orders": len(results),
        "items_total": sum(row["items_count"] for row in results),
        "counts": dict(sorted(counts.items())),
        "orders": results,
        "cache_updated_at": await session.scalar(
            select(func.max(PartsSoftOrderSnapshot.last_seen_at))
        ),
    }
