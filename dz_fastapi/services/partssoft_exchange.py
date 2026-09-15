"""Reliable outbound product exchange and inbound Parts-Soft documents."""

from __future__ import annotations

import json
import mimetypes
from collections import Counter
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import aiofiles
import aiohttp
from sqlalchemy import func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from dz_fastapi.api.validators import normalize_brand_name
from dz_fastapi.core.constants import get_upload_dir
from dz_fastapi.core.time import now_moscow
from dz_fastapi.models.autopart import AutoPart, preprocess_oem_number
from dz_fastapi.models.brand import Brand
from dz_fastapi.models.finance import InvoiceStatus, PaymentInvoice, PaymentInvoiceItem
from dz_fastapi.models.inventory import SyncStatus
from dz_fastapi.models.partner import (
    CustomerExternalReference,
    ProviderExternalReference,
    SupplierReceipt,
    SupplierReceiptItem,
)
from dz_fastapi.models.partssoft import PartsSoftDocumentSnapshot, PartsSoftProductOutbox
from dz_fastapi.services.partssoft_order_reconciliation import (
    PAGE_SIZE,
    _integer,
    _parse_datetime,
    _partssoft_api_settings,
    _text,
)
from dz_fastapi.services.partssoft_reconciliation import PARTS_SOFT_SOURCE


def _decimal(value: Any, default: Decimal = Decimal("0")) -> Decimal:
    try:
        return Decimal(str(value)) if value not in (None, "") else default
    except (InvalidOperation, TypeError, ValueError):
        return default


def _document_date(payload: dict[str, Any]) -> date | None:
    value = payload.get("document_created_at") or payload.get("created_at")
    parsed = _parse_datetime(value)
    return parsed.date() if parsed else None


async def _response_payload(response: aiohttp.ClientResponse) -> dict[str, Any]:
    """Read JSON even when Parts-Soft omits or mislabels Content-Type."""
    body = await response.read()
    if not body:
        return {}
    try:
        payload = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RuntimeError("Parts-Soft returned an invalid JSON response") from exc
    return payload if isinstance(payload, dict) else {"data": payload}


def _remote_product_id(payload: Any) -> int | None:
    if isinstance(payload, dict):
        direct = _integer(payload.get("id"))
        if direct is not None:
            return direct
        for key in ("product", "data"):
            nested = _remote_product_id(payload.get(key))
            if nested is not None:
                return nested
    if isinstance(payload, list):
        for row in payload:
            nested = _remote_product_id(row)
            if nested is not None:
                return nested
    return None


async def _local_product_form(autopart: AutoPart) -> tuple[aiohttp.FormData, list[str]]:
    form = aiohttp.FormData()
    values = {
        "product[detail_name]": autopart.name,
        "product[oem]": autopart.oem_number,
        "product[make_name]": autopart.brand.name,
        "product[body]": autopart.description or "",
        "product[weight]": autopart.weight,
        "product[width]": autopart.width,
        "product[height]": autopart.height,
        "product[length]": autopart.length,
        "product[meta_description]": autopart.description or "",
    }
    for key, value in values.items():
        if value is not None:
            form.add_field(key, str(value))

    payload = autopart.partssoft_payload or {}
    already_uploaded = set(payload.get("_outbound_photo_urls") or [])
    uploaded_urls: list[str] = []
    upload_root = Path(get_upload_dir()).resolve()
    for photo in autopart.photos or []:
        if not photo.url.startswith("/uploads/") or photo.url in already_uploaded:
            continue
        path = (upload_root / photo.url.removeprefix("/uploads/")).resolve()
        if upload_root not in path.parents or not path.is_file():
            continue
        async with aiofiles.open(path, "rb") as source:
            contents = await source.read()
        form.add_field(
            "product[images_attributes][][photo]",
            contents,
            filename=path.name,
            content_type=mimetypes.guess_type(path.name)[0] or "application/octet-stream",
        )
        uploaded_urls.append(photo.url)
    return form, uploaded_urls


async def _send_product_upsert(autopart: AutoPart) -> tuple[int, dict[str, Any], list[str]]:
    base_url, username, password = _partssoft_api_settings()
    form, uploaded_urls = await _local_product_form(autopart)
    external_id = _integer(autopart.partssoft_product_id)
    method = "PUT" if external_id is not None else "POST"
    url = (
        f"{base_url}/products/{external_id}.json"
        if external_id is not None
        else f"{base_url}/products.json"
    )
    auth = aiohttp.BasicAuth(username, password)
    async with aiohttp.ClientSession(
        auth=auth,
        timeout=aiohttp.ClientTimeout(total=120),
    ) as client:
        async with client.request(method, url, data=form, allow_redirects=False) as response:
            if response.status == 404 and external_id is not None:
                # The remote copy was deleted. Our database is authoritative, so recreate it.
                form, uploaded_urls = await _local_product_form(autopart)
                async with client.post(
                    f"{base_url}/products.json",
                    data=form,
                    allow_redirects=False,
                ) as recreated:
                    recreated.raise_for_status()
                    response_payload = await _response_payload(recreated)
            else:
                response.raise_for_status()
                response_payload = await _response_payload(response)
    resolved_id = _remote_product_id(response_payload) or external_id
    if resolved_id is None:
        raise RuntimeError("Parts-Soft did not return the product ID")
    return resolved_id, response_payload, uploaded_urls


async def _send_product_delete(external_id: int) -> None:
    base_url, username, password = _partssoft_api_settings()
    auth = aiohttp.BasicAuth(username, password)
    async with aiohttp.ClientSession(
        auth=auth,
        timeout=aiohttp.ClientTimeout(total=60),
    ) as client:
        async with client.delete(
            f"{base_url}/products/{external_id}.json",
            allow_redirects=False,
        ) as response:
            if response.status != 404:
                response.raise_for_status()


async def enqueue_all_local_products(session: AsyncSession) -> int:
    """Explicitly queue the complete local nomenclature; never runs implicitly."""
    autoparts = list((await session.scalars(select(AutoPart))).all())
    existing = {
        row.autopart_id: row
        for row in (await session.scalars(select(PartsSoftProductOutbox))).all()
    }
    now = now_moscow()
    for autopart in autoparts:
        row = existing.get(autopart.id)
        if row is None:
            row = PartsSoftProductOutbox(autopart_id=autopart.id)
            session.add(row)
        row.external_product_id = autopart.partssoft_product_id
        row.operation = "upsert"
        row.status = "pending"
        row.attempts = 0
        row.last_error = None
        row.available_at = now
        row.locked_at = None
        row.sent_at = None
    await session.commit()
    return len(autoparts)


async def process_product_outbox(session: AsyncSession, limit: int = 25) -> dict[str, Any]:
    now = now_moscow()
    rows = list(
        (
            await session.scalars(
                select(PartsSoftProductOutbox)
                .where(
                    or_(
                        PartsSoftProductOutbox.status.in_(("pending", "error")),
                        (
                            (PartsSoftProductOutbox.status == "processing")
                            & (PartsSoftProductOutbox.locked_at < now - timedelta(minutes=10))
                        ),
                    ),
                    PartsSoftProductOutbox.available_at <= now,
                )
                .order_by(PartsSoftProductOutbox.updated_at.asc())
                .limit(limit)
                .with_for_update(skip_locked=True)
            )
        ).all()
    )
    # Claim the whole batch before doing network I/O. Committing one row at a
    # time would release the locks on the remaining pending rows and let a
    # second scheduler instance send the same product concurrently.
    for row in rows:
        row.status = "processing"
        row.locked_at = now
    if rows:
        await session.commit()

    counts: Counter[str] = Counter()
    for row in rows:
        # A rollback expires ORM attributes. Keep the primary key separately so
        # the error path never tries to lazy-load ``row.id`` outside greenlet.
        row_id = row.id
        try:
            autopart = await session.scalar(
                select(AutoPart)
                .where(AutoPart.id == row.autopart_id)
                .options(selectinload(AutoPart.brand), selectinload(AutoPart.photos))
            )
            if row.operation == "delete" or autopart is None:
                external_id = _integer(row.external_product_id)
                if external_id is not None:
                    await _send_product_delete(external_id)
                counts["deleted"] += 1
            else:
                external_id, response_payload, uploaded_urls = await _send_product_upsert(autopart)
                autopart.partssoft_product_id = external_id
                merged_payload = dict(autopart.partssoft_payload or {})
                if isinstance(response_payload, dict):
                    remote = response_payload.get("product")
                    if isinstance(remote, dict):
                        merged_payload.update(remote)
                synced_photos = set(merged_payload.get("_outbound_photo_urls") or [])
                synced_photos.update(uploaded_urls)
                if synced_photos:
                    merged_payload["_outbound_photo_urls"] = sorted(synced_photos)
                autopart.partssoft_payload = merged_payload
                autopart.partssoft_synced_at = now_moscow()
                row.external_product_id = external_id
                counts["upserted"] += 1
                if uploaded_urls:
                    counts["photos_uploaded"] += len(uploaded_urls)
            row.status = "sent"
            row.sent_at = now_moscow()
            row.locked_at = None
            row.last_error = None
            await session.commit()
        except Exception as exc:
            await session.rollback()
            row = await session.get(PartsSoftProductOutbox, row_id)
            if row is None:
                counts["errors"] += 1
                continue
            row.attempts = int(row.attempts or 0) + 1
            row.last_error = str(exc)[:4000]
            row.status = "error"
            row.locked_at = None
            row.available_at = now_moscow() + timedelta(minutes=min(60, 2 ** min(row.attempts, 6)))
            await session.commit()
            counts["errors"] += 1
    return {"processed": len(rows), "counts": dict(sorted(counts.items()))}


async def product_outbox_status(session: AsyncSession) -> dict[str, int]:
    result = await session.execute(
        select(PartsSoftProductOutbox.status, func.count(PartsSoftProductOutbox.id)).group_by(
            PartsSoftProductOutbox.status
        )
    )
    counts = {status: int(count) for status, count in result.all()}
    return {
        "pending": counts.get("pending", 0),
        "processing": counts.get("processing", 0),
        "sent": counts.get("sent", 0),
        "error": counts.get("error", 0),
    }


async def _fetch_documents(
    endpoint: str,
    collection_key: str,
    *,
    since: datetime,
) -> list[dict[str, Any]]:
    base_url, username, password = _partssoft_api_settings()
    auth = aiohttp.BasicAuth(username, password)
    rows: list[dict[str, Any]] = []
    async with aiohttp.ClientSession(
        auth=auth,
        timeout=aiohttp.ClientTimeout(total=120),
    ) as client:
        page = 1
        page_signatures: set[tuple[Any, ...]] = set()
        while True:
            params = {
                "page": page,
                "per_page": PAGE_SIZE,
                "search[created_at_gteq]": since.isoformat(),
            }
            async with client.get(
                f"{base_url}/{endpoint}", params=params, allow_redirects=False
            ) as response:
                response.raise_for_status()
                payload = await response.json()
            page_rows = payload.get(collection_key) if isinstance(payload, dict) else None
            if not isinstance(page_rows, list):
                raise RuntimeError(f"Unexpected Parts-Soft {collection_key} response")
            signature = tuple(row.get("id") for row in page_rows if isinstance(row, dict))
            if signature and signature in page_signatures:
                raise RuntimeError(f"Parts-Soft repeated a {collection_key} page during pagination")
            page_signatures.add(signature)
            rows.extend(row for row in page_rows if isinstance(row, dict))
            if len(page_rows) < PAGE_SIZE:
                break
            page += 1
            if page > 1000:
                raise RuntimeError(f"Parts-Soft {collection_key} pagination exceeded 1000 pages")
    recent_rows: list[dict[str, Any]] = []
    for row in rows:
        created_at = _parse_datetime(row.get("document_created_at") or row.get("created_at"))
        if created_at is None:
            # Keep malformed rows as snapshots so they can be diagnosed in
            # the integration UI instead of silently disappearing.
            recent_rows.append(row)
            continue
        if created_at.tzinfo is None and since.tzinfo is not None:
            created_at = created_at.replace(tzinfo=since.tzinfo)
        if created_at >= since:
            recent_rows.append(row)
    return recent_rows


async def _find_autopart(
    session: AsyncSession,
    brand_name: Any,
    oem: Any,
) -> AutoPart | None:
    brand = normalize_brand_name(_text(brand_name))
    normalized_oem = preprocess_oem_number(_text(oem))
    if not brand or not normalized_oem:
        return None
    return await session.scalar(
        select(AutoPart)
        .join(Brand, Brand.id == AutoPart.brand_id)
        .where(
            func.lower(Brand.name) == brand.casefold(),
            AutoPart.oem_number == normalized_oem,
        )
        .limit(1)
    )


async def _import_customer_invoice(
    session: AsyncSession,
    snapshot: PartsSoftDocumentSnapshot,
    payload: dict[str, Any],
) -> bool:
    reference = await session.scalar(
        select(CustomerExternalReference).where(
            CustomerExternalReference.source_system == PARTS_SOFT_SOURCE,
            CustomerExternalReference.external_customer_id == snapshot.external_counterparty_id,
            CustomerExternalReference.is_active.is_(True),
            CustomerExternalReference.is_verified.is_(True),
        )
    )
    if reference is None:
        snapshot.import_status = "unmatched_counterparty"
        return False
    if snapshot.document_date is None:
        raise ValueError("Parts-Soft customer invoice has no valid document date")
    invoice = PaymentInvoice(
        customer_id=reference.customer_id,
        invoice_number=f"PS-{snapshot.external_document_id}",
        invoice_date=snapshot.document_date,
        total_amount=_decimal(payload.get("sum")),
        paid_amount=(_decimal(payload.get("sum")) if payload.get("payment_at") else Decimal("0")),
        status=(InvoiceStatus.PAID if payload.get("payment_at") else InvoiceStatus.SENT),
        notes=f"Parts-Soft накладная № {_text(payload.get('no')) or snapshot.external_document_id}",
        external_id=f"partssoft:invoice:{snapshot.external_document_id}",
        sync_status=SyncStatus.SYNCED,
    )
    session.add(invoice)
    await session.flush()
    for position, item in enumerate(payload.get("invoice_items") or [], start=1):
        if not isinstance(item, dict):
            continue
        autopart = await _find_autopart(session, item.get("make_name"), item.get("oem"))
        quantity = _decimal(item.get("qnt"), Decimal("1"))
        unit_price = _decimal(item.get("price"))
        session.add(
            PaymentInvoiceItem(
                invoice_id=invoice.id,
                position=position,
                autopart_id=autopart.id if autopart else None,
                name=_text(item.get("detail_name")) or _text(item.get("oem")) or "Товар",
                oem_number=_text(item.get("oem")) or None,
                quantity=quantity,
                unit_price=unit_price,
                vat_rate=_decimal(
                    item.get("effective_nds_percent"),
                    _decimal(payload.get("nds_percent")),
                ),
                total=(quantity * unit_price).quantize(Decimal("0.01")),
            )
        )
    snapshot.local_payment_invoice_id = invoice.id
    snapshot.import_status = "imported"
    return True


async def _import_supplier_invoice(
    session: AsyncSession,
    snapshot: PartsSoftDocumentSnapshot,
    payload: dict[str, Any],
) -> bool:
    reference = await session.scalar(
        select(ProviderExternalReference).where(
            ProviderExternalReference.source_system == PARTS_SOFT_SOURCE,
            ProviderExternalReference.external_supplier_id == snapshot.external_counterparty_id,
            ProviderExternalReference.is_active.is_(True),
        )
    )
    if reference is None:
        snapshot.import_status = "unmatched_counterparty"
        return False
    receipt = SupplierReceipt(
        provider_id=reference.provider_id,
        document_number=snapshot.document_number,
        document_date=snapshot.document_date,
        comment=(
            f"Импортировано из Parts-Soft, ID {snapshot.external_document_id}; "
            f"doc_guid={_text(payload.get('doc_guid')) or '—'}"
        ),
    )
    session.add(receipt)
    await session.flush()
    for item in payload.get("invoice_items") or []:
        if not isinstance(item, dict):
            continue
        autopart = await _find_autopart(session, item.get("make_name"), item.get("oem"))
        quantity = _integer(item.get("qnt")) or 0
        price = _decimal(item.get("price"))
        marking = item.get("marking") if isinstance(item.get("marking"), dict) else {}
        session.add(
            SupplierReceiptItem(
                receipt_id=receipt.id,
                autopart_id=autopart.id if autopart else None,
                oem_number=_text(item.get("oem")) or None,
                brand_name=_text(item.get("make_name")) or None,
                autopart_name=_text(item.get("detail_name")) or None,
                ordered_quantity=quantity,
                confirmed_quantity=quantity,
                received_quantity=quantity,
                price=price,
                total_price_with_vat=_decimal(
                    item.get("source_sum"),
                    (price * quantity).quantize(Decimal("0.01")),
                ),
                marking_codes=list(marking.get("income_codes") or []),
                gtd_code=_text(item.get("gtd")) or None,
                country_name=_text(item.get("country")) or None,
                document_pending=True,
            )
        )
    snapshot.local_supplier_receipt_id = receipt.id
    snapshot.import_status = "imported"
    return True


async def sync_partssoft_documents(
    session: AsyncSession,
    *,
    days: int = 30,
) -> dict[str, Any]:
    since = now_moscow() - timedelta(days=max(1, min(days, 365)))
    feeds = (
        ("invoice", "invoices.json", "invoices"),
        ("supplier_invoice", "supplier_invoices", "supplier_invoices"),
    )
    counts: Counter[str] = Counter()
    for document_type, endpoint, collection_key in feeds:
        remote_rows = await _fetch_documents(
            endpoint,
            collection_key,
            since=since,
        )
        counts[f"{document_type}_received"] += len(remote_rows)
        for payload in remote_rows:
            external_id = _integer(payload.get("id"))
            if external_id is None:
                counts["invalid"] += 1
                continue
            snapshot = await session.scalar(
                select(PartsSoftDocumentSnapshot).where(
                    PartsSoftDocumentSnapshot.document_type == document_type,
                    PartsSoftDocumentSnapshot.external_document_id == external_id,
                )
            )
            if snapshot is None:
                snapshot = PartsSoftDocumentSnapshot(
                    document_type=document_type,
                    external_document_id=external_id,
                )
                session.add(snapshot)
                await session.flush()
                counts["snapshots_created"] += 1
            else:
                counts["snapshots_updated"] += 1
            snapshot.external_counterparty_id = _integer(payload.get("customer_id"))
            snapshot.document_number = _text(payload.get("no")) or str(external_id)
            snapshot.document_date = _document_date(payload)
            snapshot.external_status = _text(payload.get("status")) or None
            snapshot.total_amount = _text(payload.get("sum")) or None
            snapshot.vat_rate = _text(payload.get("nds_percent")) or None
            snapshot.payload = payload
            snapshot.last_synced_at = now_moscow()
            snapshot.import_error = None
            already_imported = bool(
                snapshot.local_payment_invoice_id or snapshot.local_supplier_receipt_id
            )
            if already_imported:
                snapshot.import_status = "imported"
                counts["already_imported"] += 1
                continue
            try:
                async with session.begin_nested():
                    imported = (
                        await _import_customer_invoice(session, snapshot, payload)
                        if document_type == "invoice"
                        else await _import_supplier_invoice(session, snapshot, payload)
                    )
                    await session.flush()
                counts["imported" if imported else "unmatched"] += 1
            except Exception as exc:
                snapshot.import_status = "error"
                snapshot.import_error = str(exc)[:4000]
                counts["errors"] += 1
        await session.commit()
    return {
        "days": days,
        "counts": dict(sorted(counts.items())),
        "synced_at": now_moscow().isoformat(),
    }


async def document_sync_status(session: AsyncSession) -> dict[str, Any]:
    result = await session.execute(
        select(
            PartsSoftDocumentSnapshot.document_type,
            PartsSoftDocumentSnapshot.import_status,
            func.count(PartsSoftDocumentSnapshot.id),
        ).group_by(
            PartsSoftDocumentSnapshot.document_type,
            PartsSoftDocumentSnapshot.import_status,
        )
    )
    counts: dict[str, dict[str, int]] = {}
    for document_type, status, count in result.all():
        counts.setdefault(document_type, {})[status] = int(count)
    last_synced_at = await session.scalar(
        select(func.max(PartsSoftDocumentSnapshot.last_synced_at))
    )
    return {"documents": counts, "last_synced_at": last_synced_at}
