from __future__ import annotations

import hashlib
import logging
import os
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from io import BytesIO
from typing import Iterable, Optional

import aiofiles
import pandas as pd
from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload, selectinload

from dz_fastapi.core.constants import IMAP_SERVER
from dz_fastapi.core.email_folders import (DEFAULT_IMAP_FOLDER,
                                           resolve_imap_folders)
from dz_fastapi.core.time import now_moscow
from dz_fastapi.crud.email_account import crud_email_account
from dz_fastapi.models.email_account import EmailAccount
from dz_fastapi.models.notification import AppNotificationLevel
from dz_fastapi.models.partner import (Provider, SupplierOrder,
                                       SupplierOrderAttachment,
                                       SupplierOrderItem, SupplierOrderMessage)
from dz_fastapi.services.customer_orders import (
    EMAIL_FOLDER_ORDER, EMAIL_HOST_ORDER, EMAIL_NAME_ORDER,
    EMAIL_PASSWORD_ORDER, SimpleAttachment, _dedupe_order_messages,
    _extract_email, _fetch_gmail_messages, _fetch_order_messages,
    _fetch_resend_messages, _is_too_many_connections_error,
    _load_brand_alias_map, _message_sort_key, _normalize_key,
    _normalize_oem_key, _repair_cp1251_mojibake, _safe_float, _safe_int,
    _strip_html)
from dz_fastapi.services.notifications import create_admin_notifications
from dz_fastapi.services.order_status_mapping import (
    EXTERNAL_STATUS_SOURCE_SUPPLIER_EMAIL,
    apply_supplier_response_action_to_order, get_active_status_mappings,
    normalize_external_status_text, record_unmapped_external_status,
    select_best_mapping)

logger = logging.getLogger("dz_fastapi")

SUPPLIER_RESPONSE_LOOKBACK_DAYS = max(
    1,
    int(os.getenv("SUPPLIER_ORDER_RESPONSE_LOOKBACK_DAYS", "14")),
)
SUPPLIER_RESPONSE_DIR = os.getenv(
    "SUPPLIER_ORDER_RESPONSE_DIR",
    "uploads/orders/supplier_responses",
)

_RESPONSE_FILENAME_RE = re.compile(r"supplier[_ -]?order[_ -]?(\d+)", re.I)
_RESPONSE_SUBJECT_RE = re.compile(r"заказ\s+поставщику\s*[#№]?\s*(\d+)", re.I)
_DOCUMENT_KEYWORDS = (
    "наклад",
    "упд",
    "торг",
    "счет",
    "счёт",
    "invoice",
    "packing",
    "shipment",
)
_SUPPLIER_STATUS_PATTERNS = (
    (re.compile(r"нет\s+(позици|товар|налич)", re.I), "нет позиции"),
    (re.compile(r"отказ", re.I), "нет позиции"),
    (re.compile(r"частич", re.I), "частично"),
    (re.compile(r"собран", re.I), "собрано"),
    (re.compile(r"готов", re.I), "готово"),
    (re.compile(r"ожида", re.I), "ожидаем"),
)


@dataclass(slots=True)
class ParsedSupplierResponseRow:
    oem_number: str
    brand_name: Optional[str]
    confirmed_quantity: Optional[int]
    response_price: Optional[float]
    response_comment: Optional[str]
    response_status_raw: Optional[str]


@dataclass(slots=True)
class SupplierResponseProcessingStats:
    fetched_messages: int = 0
    processed_messages: int = 0
    matched_orders: int = 0
    stored_attachments: int = 0
    parsed_response_files: int = 0
    updated_items: int = 0
    updated_orders: int = 0
    unmapped_statuses: int = 0
    skipped_messages: int = 0

    def as_dict(self) -> dict[str, int]:
        return {
            "fetched_messages": self.fetched_messages,
            "processed_messages": self.processed_messages,
            "matched_orders": self.matched_orders,
            "stored_attachments": self.stored_attachments,
            "parsed_response_files": self.parsed_response_files,
            "updated_items": self.updated_items,
            "updated_orders": self.updated_orders,
            "unmapped_statuses": self.unmapped_statuses,
            "skipped_messages": self.skipped_messages,
        }


def supplier_response_cutoff(
    days: int = SUPPLIER_RESPONSE_LOOKBACK_DAYS,
) -> date:
    return (now_moscow() - timedelta(days=days)).date()


async def _notify_admins(
    session: AsyncSession,
    *,
    title: str,
    message: str,
    level: str = AppNotificationLevel.INFO,
    commit: bool = False,
) -> None:
    await create_admin_notifications(
        session=session,
        title=title,
        message=message,
        level=level,
        link="/customer-orders/receipts",
        commit=commit,
    )


async def _fetch_supplier_response_messages(
    session: AsyncSession,
    *,
    date_from: date,
    date_to: Optional[date] = None,
) -> list[tuple[object, Optional[EmailAccount]]]:
    accounts = await crud_email_account.get_active_by_purpose(
        session,
        "orders_out",
    )
    messages: list[tuple[object, Optional[EmailAccount]]] = []
    if accounts:
        for account in accounts:
            host = account.imap_host or EMAIL_HOST_ORDER
            transport = (account.transport or "smtp").strip().lower()
            folders = resolve_imap_folders(
                account.imap_folder,
                getattr(account, "imap_additional_folders", None),
                default=EMAIL_FOLDER_ORDER or DEFAULT_IMAP_FOLDER,
            )
            if transport == "resend_api":
                try:
                    account_messages = await _fetch_resend_messages(
                        account,
                        date_from,
                    )
                    messages.extend((msg, account) for msg in account_messages)
                except Exception as exc:
                    logger.error(
                        (
                            "Supplier response inbox fetch failed "
                            "for Resend %s: %s"
                        ),
                        account.email,
                        exc,
                        exc_info=True,
                    )
                continue
            if account.oauth_provider == "google":
                try:
                    account_messages = []
                    for label in folders:
                        account_messages.extend(
                            await _fetch_gmail_messages(
                                account,
                                date_from,
                                label=label,
                            )
                        )
                    messages.extend((msg, account) for msg in account_messages)
                except Exception as exc:
                    logger.error(
                        "Supplier response inbox fetch failed for %s: %s",
                        account.email,
                        exc,
                        exc_info=True,
                    )
                continue
            if not host:
                continue
            try:
                account_messages = []
                for folder in folders:
                    account_messages.extend(
                        await _fetch_order_messages(
                            host,
                            account.email,
                            account.password,
                            folder,
                            date_from,
                            False,
                            port=account.imap_port or IMAP_SERVER,
                            ssl=True,
                        )
                    )
                messages.extend((msg, account) for msg in account_messages)
            except Exception as exc:
                if _is_too_many_connections_error(exc):
                    logger.warning(
                        "Supplier response inbox fetch throttled for %s: %s",
                        account.email,
                        exc,
                    )
                else:
                    logger.error(
                        "Supplier response inbox fetch failed for %s: %s",
                        account.email,
                        exc,
                        exc_info=True,
                    )
    elif EMAIL_NAME_ORDER and EMAIL_PASSWORD_ORDER and EMAIL_HOST_ORDER:
        try:
            fallback_messages = await _fetch_order_messages(
                EMAIL_HOST_ORDER,
                EMAIL_NAME_ORDER,
                EMAIL_PASSWORD_ORDER,
                EMAIL_FOLDER_ORDER,
                date_from,
                False,
                port=IMAP_SERVER,
                ssl=True,
            )
            messages = [(msg, None) for msg in fallback_messages]
        except Exception as exc:
            logger.error(
                "Supplier response fallback inbox fetch failed: %s",
                exc,
                exc_info=True,
            )
    messages = _dedupe_order_messages(messages)
    messages.sort(key=_message_sort_key)
    if date_to is None:
        return messages
    filtered: list[tuple[object, Optional[EmailAccount]]] = []
    for msg, account in messages:
        received_at = _get_message_received_at(msg)
        if received_at and received_at.date() > date_to:
            continue
        filtered.append((msg, account))
    return filtered


def _get_message_received_at(msg: object) -> Optional[datetime]:
    return getattr(msg, "received_at", None) or getattr(msg, "date", None)


def _get_message_body_preview(msg: object) -> Optional[str]:
    text = str(getattr(msg, "text", "") or "").strip()
    if text:
        return text[:2000]
    html = str(getattr(msg, "html", "") or "").strip()
    if html:
        return _strip_html(html)[:2000]
    return None


def _iter_message_attachments(msg: object) -> list[SimpleAttachment]:
    attachments = getattr(msg, "attachments", None) or []
    result: list[SimpleAttachment] = []
    for attachment in attachments:
        filename = getattr(attachment, "filename", None)
        payload = getattr(attachment, "payload", None)
        if payload is None and isinstance(attachment, SimpleAttachment):
            payload = attachment.payload
        if payload is None:
            continue
        result.append(SimpleAttachment(filename=filename, payload=payload))
    return result


def _extract_supplier_order_id(*values: Optional[str]) -> Optional[int]:
    for value in values:
        text = str(value or "").strip()
        if not text:
            continue
        for pattern in (_RESPONSE_FILENAME_RE, _RESPONSE_SUBJECT_RE):
            match = pattern.search(text)
            if match:
                try:
                    return int(match.group(1))
                except (TypeError, ValueError):
                    continue
    return None


def _detect_supplier_status(
    subject: Optional[str],
    body_preview: Optional[str],
) -> Optional[str]:
    text_sources = [str(subject or ""), str(body_preview or "")]
    for pattern, label in _SUPPLIER_STATUS_PATTERNS:
        for source in text_sources:
            if source and pattern.search(source):
                return label
    subject_text = str(subject or "").strip()
    if subject_text:
        return subject_text[:255]
    body_text = str(body_preview or "").strip()
    if not body_text:
        return None
    first_line = next(
        (line.strip() for line in body_text.splitlines() if line.strip()),
        "",
    )
    return (first_line or body_text[:255])[:255]


def _attachment_extension(filename: Optional[str]) -> str:
    if not filename or "." not in filename:
        return ""
    return filename.rsplit(".", 1)[-1].strip().lower()


def _classify_attachment_kind(filename: Optional[str]) -> Optional[str]:
    lower_name = str(filename or "").strip().lower()
    if not lower_name:
        return None
    if _RESPONSE_FILENAME_RE.search(lower_name):
        return "RESPONSE_FILE"
    if any(keyword in lower_name for keyword in _DOCUMENT_KEYWORDS):
        return "SHIPPING_DOC"
    return None


def _normalize_response_header(value: object) -> str:
    normalized = normalize_external_status_text(value)
    return normalized.replace(" ", "")


def _parse_supplier_response_attachment(
    payload: bytes,
    filename: str,
) -> list[ParsedSupplierResponseRow]:
    ext = _attachment_extension(filename)
    if ext == "csv":
        df = pd.read_csv(BytesIO(payload))
    elif ext in {"xlsx", "xls"}:
        df = pd.read_excel(BytesIO(payload))
    else:
        return []
    if df.empty:
        return []

    headers = {
        _normalize_response_header(column): column for column in df.columns
    }
    oem_column = None
    for candidate in ("oem", "артикул", "номер", "oemномер"):
        if candidate in headers:
            oem_column = headers[candidate]
            break
    if oem_column is None:
        return []

    brand_column = None
    for candidate in ("brand", "бренд", "марка"):
        if candidate in headers:
            brand_column = headers[candidate]
            break

    qty_column = None
    for candidate in (
        "qty",
        "quantity",
        "кол",
        "колво",
        "количество",
        "подтверждено",
        "котгрузке",
        "отгрузка",
    ):
        if candidate in headers:
            qty_column = headers[candidate]
            break

    price_column = None
    for candidate in ("price", "цена", "ценаотгрузки"):
        if candidate in headers:
            price_column = headers[candidate]
            break

    comment_column = None
    for candidate in ("comment", "комментарий", "remark", "примечание"):
        if candidate in headers:
            comment_column = headers[candidate]
            break

    status_column = None
    for candidate in ("status", "статус", "state"):
        if candidate in headers:
            status_column = headers[candidate]
            break

    parsed_rows: list[ParsedSupplierResponseRow] = []
    for _, row in df.iterrows():
        oem_value = _normalize_oem_key(row.get(oem_column))
        if not oem_value:
            continue
        brand_value = None
        if brand_column is not None:
            raw_brand = row.get(brand_column)
            if raw_brand is not None and not pd.isna(raw_brand):
                brand_value = str(raw_brand).strip() or None
        qty_value = None
        if qty_column is not None:
            raw_qty = row.get(qty_column)
            if raw_qty is not None and not pd.isna(raw_qty):
                qty_value = _safe_int(raw_qty)
        price_value = None
        if price_column is not None:
            raw_price = row.get(price_column)
            if raw_price is not None and not pd.isna(raw_price):
                price_value = _safe_float(raw_price)
        comment_value = None
        if comment_column is not None:
            raw_comment = row.get(comment_column)
            if raw_comment is not None and not pd.isna(raw_comment):
                comment_value = _repair_cp1251_mojibake(raw_comment)
        status_value = None
        if status_column is not None:
            raw_status = row.get(status_column)
            if raw_status is not None and not pd.isna(raw_status):
                status_value = _repair_cp1251_mojibake(raw_status)
        parsed_rows.append(
            ParsedSupplierResponseRow(
                oem_number=oem_value,
                brand_name=brand_value,
                confirmed_quantity=qty_value,
                response_price=price_value,
                response_comment=comment_value,
                response_status_raw=status_value,
            )
        )
    return parsed_rows


async def _get_message_match_context(
    session: AsyncSession,
    *,
    sender_email: str,
    subject: Optional[str],
    body_preview: Optional[str],
    attachments: Iterable[SimpleAttachment],
) -> tuple[Optional[SupplierOrder], Optional[Provider]]:
    attachment_names = [attachment.filename for attachment in attachments]
    order_id = _extract_supplier_order_id(
        subject,
        body_preview,
        *attachment_names,
    )
    order = None
    if order_id is not None:
        order = (
            await session.execute(
                select(SupplierOrder)
                .options(
                    joinedload(SupplierOrder.provider),
                    selectinload(SupplierOrder.items),
                )
                .where(SupplierOrder.id == order_id)
            )
        ).scalar_one_or_none()
    if order is not None:
        return order, order.provider

    if not sender_email:
        return None, None

    provider_stmt = select(Provider).where(
        or_(
            Provider.email_contact == sender_email,
            Provider.email_incoming_price == sender_email,
        )
    )
    provider = (await session.execute(provider_stmt)).scalars().first()
    return None, provider


def _build_source_uid(
    msg: object,
    account: Optional[EmailAccount],
) -> Optional[str]:
    uid = getattr(msg, "uid", None)
    if uid in (None, ""):
        return None
    folder_name = str(getattr(msg, "folder_name", "") or "").strip()
    account_id = account.id if account else 0
    return f"{account_id}:{folder_name}:{uid}"[:128]


def _build_source_message_id(msg: object) -> Optional[str]:
    value = getattr(msg, "external_id", None)
    if value in (None, ""):
        return None
    return str(value)[:255]


async def _message_already_processed(
    session: AsyncSession,
    *,
    source_uid: Optional[str],
    source_message_id: Optional[str],
) -> bool:
    if source_message_id:
        stmt = select(SupplierOrderMessage.id).where(
            SupplierOrderMessage.source_message_id == source_message_id
        )
        if (await session.execute(stmt)).scalar_one_or_none() is not None:
            return True
    if source_uid:
        stmt = select(SupplierOrderMessage.id).where(
            SupplierOrderMessage.source_uid == source_uid
        )
        if (await session.execute(stmt)).scalar_one_or_none() is not None:
            return True
    return False


async def _store_supplier_message_attachment(
    *,
    message_id: int,
    attachment: SimpleAttachment,
) -> tuple[str, str]:
    filename = (
        str(attachment.filename or "attachment.bin").strip()
        or "attachment.bin"
    )
    digest = hashlib.sha256(attachment.payload).hexdigest()
    os.makedirs(SUPPLIER_RESPONSE_DIR, exist_ok=True)
    directory = os.path.join(SUPPLIER_RESPONSE_DIR, str(message_id))
    os.makedirs(directory, exist_ok=True)
    safe_name = re.sub(r"[^A-Za-z0-9._-]+", "_", filename)
    path = os.path.join(directory, f"{digest[:12]}_{safe_name}")
    async with aiofiles.open(path, "wb") as file_obj:
        await file_obj.write(attachment.payload)
    return path, digest


async def _apply_parsed_response_rows(
    session: AsyncSession,
    *,
    order: SupplierOrder,
    parsed_rows: list[ParsedSupplierResponseRow],
    default_raw_status: Optional[str],
    default_normalized_status: Optional[str],
) -> int:
    if not parsed_rows:
        return 0
    brand_aliases = await _load_brand_alias_map(session)
    exact_map: dict[tuple[str, str], list[SupplierOrderItem]] = {}
    oem_map: dict[str, list[SupplierOrderItem]] = {}
    for item in order.items or []:
        key = _normalize_key(item.oem_number, item.brand_name, brand_aliases)
        exact_map.setdefault(key, []).append(item)
        oem_key = _normalize_oem_key(item.oem_number)
        if oem_key:
            oem_map.setdefault(oem_key, []).append(item)

    updated = 0
    for row in parsed_rows:
        matched_item = None
        exact_key = _normalize_key(
            row.oem_number,
            row.brand_name,
            brand_aliases,
        )
        exact_candidates = exact_map.get(exact_key) or []
        if exact_candidates:
            matched_item = exact_candidates.pop(0)
        else:
            oem_candidates = (
                oem_map.get(_normalize_oem_key(row.oem_number)) or []
            )
            if len(oem_candidates) == 1:
                matched_item = oem_candidates[0]
        if matched_item is None:
            continue

        item_changed = False
        if (
            row.confirmed_quantity is not None
            and matched_item.confirmed_quantity != row.confirmed_quantity
        ):
            matched_item.confirmed_quantity = row.confirmed_quantity
            item_changed = True
        if (
            row.response_price is not None
            and matched_item.response_price != row.response_price
        ):
            matched_item.response_price = row.response_price
            item_changed = True
        next_comment = row.response_comment or matched_item.response_comment
        if next_comment != matched_item.response_comment:
            matched_item.response_comment = next_comment
            item_changed = True
        raw_status = row.response_status_raw or default_raw_status
        normalized_status = normalize_external_status_text(raw_status)
        if matched_item.response_status_raw != raw_status:
            matched_item.response_status_raw = raw_status
            item_changed = True
        if matched_item.response_status_normalized != (
            normalized_status or default_normalized_status or None
        ):
            matched_item.response_status_normalized = (
                normalized_status or default_normalized_status or None
            )
            item_changed = True
        matched_item.response_status_synced_at = now_moscow()
        if item_changed:
            updated += 1
    return updated


async def process_supplier_response_messages(
    session: AsyncSession,
    *,
    provider_id: Optional[int] = None,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
) -> dict[str, int]:
    if date_from is None:
        date_from = supplier_response_cutoff()
    stats = SupplierResponseProcessingStats()
    messages = await _fetch_supplier_response_messages(
        session,
        date_from=date_from,
        date_to=date_to,
    )
    stats.fetched_messages = len(messages)

    for msg, account in messages:
        source_uid = _build_source_uid(msg, account)
        source_message_id = _build_source_message_id(msg)
        if await _message_already_processed(
            session,
            source_uid=source_uid,
            source_message_id=source_message_id,
        ):
            stats.skipped_messages += 1
            continue

        attachments = _iter_message_attachments(msg)
        sender_email = _extract_email(getattr(msg, "from_", None))
        subject = str(getattr(msg, "subject", "") or "")
        body_preview = _get_message_body_preview(msg)
        raw_status = _detect_supplier_status(subject, body_preview)
        normalized_status = normalize_external_status_text(raw_status)
        order = None
        provider = None
        try:
            order, provider = await _get_message_match_context(
                session,
                sender_email=sender_email,
                subject=subject,
                body_preview=body_preview,
                attachments=attachments,
            )
            if (
                provider_id is not None
                and provider
                and provider.id != provider_id
            ):
                stats.skipped_messages += 1
                continue
            if provider is None:
                await _notify_admins(
                    session,
                    title="Не удалось привязать ответ поставщика",
                    message=(
                        f'Отправитель: {sender_email or "не определён"}\n'
                        f'Тема: {subject or "без темы"}'
                    ),
                    level=AppNotificationLevel.WARNING,
                    commit=True,
                )
                stats.skipped_messages += 1
                continue

            allow_shipping_docs = bool(
                getattr(
                    provider,
                    "supplier_response_allow_shipping_docs",
                    True,
                )
            )
            allow_response_files = bool(
                getattr(
                    provider,
                    "supplier_response_allow_response_files",
                    True,
                )
            )
            allow_text_status = bool(
                getattr(
                    provider,
                    "supplier_response_allow_text_status",
                    True,
                )
            )
            if not allow_text_status:
                raw_status = None
                normalized_status = ""

            mappings = await get_active_status_mappings(
                session,
                source_key=EXTERNAL_STATUS_SOURCE_SUPPLIER_EMAIL,
                provider_id=provider.id,
            )
            mapping = None
            if allow_text_status and normalized_status:
                mapping = select_best_mapping(
                    mappings,
                    normalized_status=normalized_status,
                    provider_id=provider.id,
                )

            message_row = SupplierOrderMessage(
                supplier_order_id=order.id if order else None,
                provider_id=provider.id,
                message_type="UNKNOWN",
                subject=subject[:500] or None,
                sender_email=sender_email or None,
                received_at=_get_message_received_at(msg) or now_moscow(),
                body_preview=body_preview,
                raw_status=raw_status,
                normalized_status=normalized_status or None,
                parse_confidence=(
                    1.0 if mapping else (0.8 if normalized_status else None)
                ),
                source_uid=source_uid,
                source_message_id=source_message_id,
                mapping_id=mapping.id if mapping else None,
            )
            session.add(message_row)
            await session.flush()

            parsed_response_file = False
            has_shipping_doc = False
            for attachment in attachments:
                file_path, digest = await _store_supplier_message_attachment(
                    message_id=message_row.id,
                    attachment=attachment,
                )
                attachment_kind = _classify_attachment_kind(
                    attachment.filename
                )
                if (
                    attachment_kind == "SHIPPING_DOC"
                    and not allow_shipping_docs
                ):
                    attachment_kind = None
                if attachment_kind == "SHIPPING_DOC":
                    has_shipping_doc = True
                parsed_rows: list[ParsedSupplierResponseRow] = []
                if (
                    allow_response_files
                    and order is not None
                    and (
                        attachment_kind == "RESPONSE_FILE"
                        or _attachment_extension(attachment.filename)
                        in {"xlsx", "xls", "csv"}
                    )
                ):
                    try:
                        parsed_rows = _parse_supplier_response_attachment(
                            attachment.payload,
                            attachment.filename or "",
                        )
                    except Exception as exc:
                        logger.warning(
                            (
                                "Failed to parse supplier response "
                                "attachment %s: %s"
                            ),
                            attachment.filename,
                            exc,
                        )
                if order is not None and parsed_rows:
                    stats.updated_items += await _apply_parsed_response_rows(
                        session,
                        order=order,
                        parsed_rows=parsed_rows,
                        default_raw_status=raw_status,
                        default_normalized_status=normalized_status or None,
                    )
                    parsed_response_file = True
                    attachment_kind = "RESPONSE_FILE"
                    stats.parsed_response_files += 1
                session.add(
                    SupplierOrderAttachment(
                        message_id=message_row.id,
                        filename=str(
                            attachment.filename or "attachment.bin"
                        )[:255],
                        mime_type=None,
                        file_path=file_path,
                        sha256=digest,
                        parsed_kind=attachment_kind,
                    )
                )
                stats.stored_attachments += 1

            if order is not None:
                stats.matched_orders += 1
                if raw_status:
                    order.response_status_raw = raw_status
                    order.response_status_normalized = (
                        normalized_status or None
                    )
                    order.response_status_synced_at = now_moscow()
                if mapping is not None:
                    apply_result = apply_supplier_response_action_to_order(
                        order=order,
                        mapping=mapping,
                        raw_status=raw_status,
                        normalized_status=normalized_status or None,
                        allow_quantity_updates=not parsed_response_file,
                    )
                    stats.updated_orders += apply_result["changed_orders"]
                    stats.updated_items += apply_result["updated_items"]

            if (
                mapping is None
                and normalized_status
                and raw_status
                and not parsed_response_file
                and allow_text_status
            ):
                await record_unmapped_external_status(
                    session,
                    source_key=EXTERNAL_STATUS_SOURCE_SUPPLIER_EMAIL,
                    provider_id=provider.id,
                    raw_status=raw_status,
                    normalized_status=normalized_status,
                    sample_payload={
                        "supplier_order_id": order.id if order else None,
                        "supplier_message_id": message_row.id,
                        "sender_email": sender_email,
                        "subject": subject,
                    },
                )
                stats.unmapped_statuses += 1

            if parsed_response_file:
                message_row.message_type = "RESPONSE_FILE"
            elif has_shipping_doc:
                message_row.message_type = "SHIPPING_DOC"
            elif raw_status:
                message_row.message_type = "STATUS"

            await session.commit()
            stats.processed_messages += 1
        except Exception as exc:
            await session.rollback()
            logger.error(
                "Failed to process supplier response message subject=%s: %s",
                subject,
                exc,
                exc_info=True,
            )
            await _notify_admins(
                session,
                title="Ошибка обработки ответа поставщика",
                message=(
                    f'Отправитель: {sender_email or "не определён"}\n'
                    f'Тема: {subject or "без темы"}\n'
                    f"Ошибка: {exc}"
                ),
                level=AppNotificationLevel.ERROR,
                commit=True,
            )
            stats.skipped_messages += 1

    return stats.as_dict()
