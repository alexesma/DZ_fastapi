from __future__ import annotations

import hashlib
import logging
import os
import re
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from email.header import decode_header
from io import BytesIO
from typing import Iterable, Optional

import aiofiles
import pandas as pd
from sqlalchemy import desc, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload, selectinload

from dz_fastapi.core.constants import IMAP_SERVER
from dz_fastapi.core.email_folders import (DEFAULT_IMAP_FOLDER,
                                           resolve_imap_folders)
from dz_fastapi.core.time import now_moscow
from dz_fastapi.crud.email_account import crud_email_account
from dz_fastapi.crud.settings import crud_customer_order_inbox_settings
from dz_fastapi.models.email_account import EmailAccount
from dz_fastapi.models.notification import AppNotificationLevel
from dz_fastapi.models.partner import (Provider, SupplierOrder,
                                       SupplierOrderAttachment,
                                       SupplierOrderItem, SupplierOrderMessage,
                                       SupplierReceipt, SupplierReceiptItem,
                                       SupplierResponseConfig)
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

DEFAULT_SUPPLIER_RESPONSE_LOOKBACK_DAYS = max(
    1,
    int(os.getenv("SUPPLIER_ORDER_RESPONSE_LOOKBACK_DAYS", "14")),
)
SUPPLIER_RESPONSE_DIR = os.getenv(
    "SUPPLIER_ORDER_RESPONSE_DIR",
    "uploads/orders/supplier_responses",
)

_RESPONSE_FILENAME_RE = re.compile(r"supplier[_ -]?order[_ -]?(\d+)", re.I)
_RESPONSE_SUBJECT_RE = re.compile(
    r"заказ\w*(?:\s+поставщику)?\s*[#№]?\s*(\d+)",
    re.I,
)
_DOCUMENT_KEYWORDS = (
    "наклад",
    "упд",
    "upd",
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
_ARTICLE_TOKEN_RE = re.compile(r"(?:[A-Za-z].*[0-9]|[0-9].*[A-Za-z])")
_TEXT_TOKEN_RE = re.compile(r"[A-Za-zА-Яа-яЁё0-9]+")
_DEFAULT_CONFIRM_KEYWORDS = [
    "в наличии",
    "есть",
    "отгружаем",
    "собрали",
    "да",
]
_DEFAULT_REJECT_KEYWORDS = [
    "нет",
    "0",
    "отсутствует",
    "не можем",
    "снято с производства",
]
_MAX_INT32 = 2_147_483_647


@dataclass(slots=True)
class ParsedSupplierResponseRow:
    oem_number: str
    brand_name: Optional[str]
    confirmed_quantity: Optional[int]
    response_price: Optional[float]
    response_comment: Optional[str]
    response_status_raw: Optional[str]
    text_decision: Optional[str] = None
    document_number: Optional[str] = None
    document_date: Optional[date] = None
    gtd_code: Optional[str] = None
    country_code: Optional[str] = None
    country_name: Optional[str] = None
    total_price_with_vat: Optional[float] = None


@dataclass(slots=True)
class ParsedSupplierTextResponse:
    rows: list[ParsedSupplierResponseRow] = field(default_factory=list)
    unresolved: list[str] = field(default_factory=list)


@dataclass(slots=True)
class AppliedSupplierResponseRow:
    supplier_order_item_id: int
    supplier_order_id: int
    received_quantity: int
    comment: Optional[str] = None
    response_price: Optional[float] = None
    document_number: Optional[str] = None
    document_date: Optional[date] = None
    gtd_code: Optional[str] = None
    country_code: Optional[str] = None
    country_name: Optional[str] = None
    total_price_with_vat: Optional[float] = None


@dataclass(slots=True)
class SupplierResponseProcessingStats:
    fetched_messages: int = 0
    processed_messages: int = 0
    matched_orders: int = 0
    stored_attachments: int = 0
    parsed_response_files: int = 0
    parsed_text_positions: int = 0
    recognized_positions: int = 0
    unresolved_positions: int = 0
    unresolved_examples: list[str] = field(default_factory=list)
    updated_items: int = 0
    updated_orders: int = 0
    unmapped_statuses: int = 0
    skipped_messages: int = 0
    created_receipts: int = 0
    updated_receipts: int = 0
    posted_receipts: int = 0
    draft_receipts: int = 0
    receipt_items_added: int = 0

    def add_unresolved(self, value: str) -> None:
        self.unresolved_positions += 1
        if len(self.unresolved_examples) < 25:
            self.unresolved_examples.append(value[:240])

    def as_dict(self) -> dict[str, object]:
        return {
            "fetched_messages": self.fetched_messages,
            "processed_messages": self.processed_messages,
            "matched_orders": self.matched_orders,
            "stored_attachments": self.stored_attachments,
            "parsed_response_files": self.parsed_response_files,
            "parsed_text_positions": self.parsed_text_positions,
            "recognized_positions": self.recognized_positions,
            "unresolved_positions": self.unresolved_positions,
            "unresolved_examples": self.unresolved_examples,
            "updated_items": self.updated_items,
            "updated_orders": self.updated_orders,
            "unmapped_statuses": self.unmapped_statuses,
            "skipped_messages": self.skipped_messages,
            "created_receipts": self.created_receipts,
            "updated_receipts": self.updated_receipts,
            "posted_receipts": self.posted_receipts,
            "draft_receipts": self.draft_receipts,
            "receipt_items_added": self.receipt_items_added,
        }


def supplier_response_cutoff(
    days: int = DEFAULT_SUPPLIER_RESPONSE_LOOKBACK_DAYS,
) -> date:
    return (now_moscow() - timedelta(days=days)).date()


async def _get_supplier_response_lookback_days(
    session: AsyncSession,
) -> int:
    try:
        inbox_settings = (
            await crud_customer_order_inbox_settings.get_or_create(
                session
            )
        )
    except Exception:
        return DEFAULT_SUPPLIER_RESPONSE_LOOKBACK_DAYS
    raw_value = getattr(
        inbox_settings,
        "supplier_response_lookback_days",
        DEFAULT_SUPPLIER_RESPONSE_LOOKBACK_DAYS,
    )
    try:
        value = int(raw_value or DEFAULT_SUPPLIER_RESPONSE_LOOKBACK_DAYS)
    except (TypeError, ValueError):
        value = DEFAULT_SUPPLIER_RESPONSE_LOOKBACK_DAYS
    return max(1, value)


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
    account_ids: Optional[set[int]] = None,
    include_default_orders_out: bool = True,
) -> list[tuple[object, Optional[EmailAccount]]]:
    accounts: list[EmailAccount] = []
    account_map: dict[int, EmailAccount] = {}
    logger.info(
        (
            "Supplier response inbox fetch init: date_from=%s date_to=%s "
            "include_default_orders_out=%s explicit_account_ids=%s"
        ),
        date_from,
        date_to,
        include_default_orders_out,
        sorted(account_ids or []),
    )
    if include_default_orders_out:
        default_accounts = await crud_email_account.get_active_by_purpose(
            session,
            "orders_out",
        )
        for account in default_accounts:
            account_map[account.id] = account
    if account_ids:
        explicit_accounts = (
            await session.execute(
                select(EmailAccount).where(
                    EmailAccount.id.in_(set(account_ids)),
                    EmailAccount.is_active.is_(True),
                )
            )
        ).scalars().all()
        for account in explicit_accounts:
            account_map[account.id] = account
    accounts = sorted(account_map.values(), key=lambda item: item.id)
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
            logger.info(
                (
                    "Supplier response inbox account start: account_id=%s "
                    "email=%s transport=%s folders=%s"
                ),
                account.id,
                account.email,
                transport,
                folders,
            )
            if transport == "resend_api":
                try:
                    account_messages = await _fetch_resend_messages(
                        account,
                        date_from,
                    )
                    logger.info(
                        (
                            "Supplier response inbox account done: "
                            "account_id=%s email=%s fetched=%s"
                        ),
                        account.id,
                        account.email,
                        len(account_messages),
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
                        logger.info(
                            (
                                "Supplier response Gmail fetch: "
                                "account_id=%s email=%s label=%s"
                            ),
                            account.id,
                            account.email,
                            label,
                        )
                        account_messages.extend(
                            await _fetch_gmail_messages(
                                account,
                                date_from,
                                label=label,
                            )
                        )
                    logger.info(
                        (
                            "Supplier response inbox account done: "
                            "account_id=%s email=%s fetched=%s"
                        ),
                        account.id,
                        account.email,
                        len(account_messages),
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
                    logger.info(
                        (
                            "Supplier response IMAP fetch: "
                            "account_id=%s email=%s folder=%s"
                        ),
                        account.id,
                        account.email,
                        folder,
                    )
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
                logger.info(
                    (
                        "Supplier response inbox account done: "
                        "account_id=%s email=%s fetched=%s"
                    ),
                    account.id,
                    account.email,
                    len(account_messages),
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
            logger.info(
                "Supplier response fallback inbox fetch start: email=%s",
                EMAIL_NAME_ORDER,
            )
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
            logger.info(
                "Supplier response fallback inbox done: fetched=%s",
                len(fallback_messages),
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


def _decode_mime_text(value: Optional[str]) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    try:
        parts = decode_header(text)
    except Exception:
        return text
    decoded: list[str] = []
    for part, encoding in parts:
        if isinstance(part, bytes):
            try:
                decoded.append(
                    part.decode(encoding or "utf-8", errors="ignore")
                )
            except Exception:
                decoded.append(part.decode("utf-8", errors="ignore"))
        else:
            decoded.append(str(part))
    result = "".join(decoded).strip()
    return result or text


def _iter_message_attachments(msg: object) -> list[SimpleAttachment]:
    attachments = getattr(msg, "attachments", None) or []
    result: list[SimpleAttachment] = []
    for attachment in attachments:
        filename = _decode_mime_text(getattr(attachment, "filename", None))
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
                    parsed = int(match.group(1))
                except (TypeError, ValueError):
                    continue
                if 1 <= parsed <= _MAX_INT32:
                    return parsed
                logger.info(
                    (
                        "Supplier response order id candidate ignored: "
                        "value=%s out_of_int32_range"
                    ),
                    parsed,
                )
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


def _compile_filename_pattern(
        pattern_value: Optional[str]
) -> Optional[re.Pattern]:
    pattern = str(pattern_value or "").strip()
    if not pattern:
        return None
    try:
        return re.compile(pattern, re.I)
    except re.error as exc:
        logger.warning(
            "Invalid supplier response filename pattern %r: %s",
            pattern,
            exc,
        )
        return None


def _classify_attachment_kind(
    filename: Optional[str],
    *,
    response_pattern: Optional[re.Pattern] = None,
    shipping_pattern: Optional[re.Pattern] = None,
) -> Optional[str]:
    raw_name = _decode_mime_text(filename)
    if not raw_name:
        return None
    lower_name = raw_name.lower()
    if response_pattern and response_pattern.search(raw_name):
        return "RESPONSE_FILE"
    if shipping_pattern and shipping_pattern.search(raw_name):
        return "SHIPPING_DOC"
    if _RESPONSE_FILENAME_RE.search(lower_name):
        return "RESPONSE_FILE"
    if any(keyword in lower_name for keyword in _DOCUMENT_KEYWORDS):
        return "SHIPPING_DOC"
    return None


def _normalize_response_header(value: object) -> str:
    normalized = normalize_external_status_text(value)
    return normalized.replace(" ", "")


def _parse_positive_int(value: object) -> Optional[int]:
    if value in (None, ""):
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    if parsed < 1:
        return None
    return parsed


def _resolve_column_by_number(
        df: pd.DataFrame, one_based: object
) -> Optional[object]:
    number = _parse_positive_int(one_based)
    if number is None:
        return None
    index = number - 1
    if index >= len(df.columns):
        return None
    return df.columns[index]


def _clean_text_value(value: object) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    text = _repair_cp1251_mojibake(value)
    text = str(text or "").strip()
    return text or None


def _parse_excel_like_date(value: object) -> Optional[date]:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, float) and pd.isna(value):
        return None
    normalized = _clean_text_value(value)
    if not normalized:
        return None
    parsed_ts = pd.to_datetime(
        normalized,
        dayfirst=True,
        errors="coerce",
    )
    if parsed_ts is None or pd.isna(parsed_ts):
        return None
    return parsed_ts.date()


def _resolve_price_without_vat(
    total_price_with_vat: Optional[float],
    quantity: Optional[int],
) -> Optional[float]:
    if total_price_with_vat is None:
        return None
    qty = _safe_int(quantity)
    if qty is None or qty <= 0:
        return None
    total = float(total_price_with_vat)
    if total <= 0:
        return None
    return round(total / qty, 2)


def _normalize_sender_emails(value: object) -> set[str]:
    if value is None:
        return set()
    if isinstance(value, str):
        raw_values = [value]
    elif isinstance(value, (list, tuple, set)):
        raw_values = list(value)
    else:
        raw_values = [value]
    result: set[str] = set()
    for raw in raw_values:
        for chunk in str(raw or "").split(","):
            cleaned = chunk.strip().lower()
            if cleaned:
                result.add(cleaned)
    return result


def _config_mismatch_reasons(
    config: SupplierResponseConfig,
    *,
    sender_email: str,
    account: Optional[EmailAccount],
) -> list[str]:
    reasons: list[str] = []
    account_id = account.id if account else None
    if (
        config.inbox_email_account_id is not None
        and config.inbox_email_account_id != account_id
    ):
        reasons.append(
            (
                "inbox_email mismatch "
                f"(expected account_id={config.inbox_email_account_id}, "
                f"got account_id={account_id})"
            )
        )
    allowed_senders = _normalize_sender_emails(config.sender_emails)
    normalized_sender = str(sender_email or "").strip().lower()
    if allowed_senders and normalized_sender not in allowed_senders:
        reasons.append(
            (
                "sender_email mismatch "
                f"(expected one of {sorted(allowed_senders)}, "
                f"got {normalized_sender or '<empty>'})"
            )
        )
    return reasons


def _config_matches_message(
    config: SupplierResponseConfig,
    *,
    sender_email: str,
    account: Optional[EmailAccount],
) -> bool:
    return not _config_mismatch_reasons(
        config,
        sender_email=sender_email,
        account=account,
    )


def _select_best_supplier_response_config(
    configs: Iterable[SupplierResponseConfig],
    *,
    sender_email: str,
    account: Optional[EmailAccount],
) -> Optional[SupplierResponseConfig]:
    matched: list[tuple[tuple[int, int, int], SupplierResponseConfig]] = []
    for config in configs:
        if not bool(getattr(config, "is_active", True)):
            continue
        if not _config_matches_message(
            config,
            sender_email=sender_email,
            account=account,
        ):
            continue
        score = (
            1 if config.inbox_email_account_id is not None else 0,
            1 if _normalize_sender_emails(config.sender_emails) else 0,
            -int(config.id or 0),
        )
        matched.append((score, config))
    if not matched:
        return None
    matched.sort(key=lambda item: item[0], reverse=True)
    return matched[0][1]


def _normalize_keywords(values: object, defaults: list[str]) -> set[str]:
    raw_values = (
        list(values)
        if isinstance(values, (list, tuple, set))
        else defaults
    )
    result: set[str] = set()
    for raw in raw_values or []:
        normalized = normalize_external_status_text(raw)
        if not normalized:
            continue
        result.add(normalized)
        result.add(normalized.split(" ")[0])
    return result


def _normalize_value_after_article_type(value: object) -> str:
    raw = getattr(value, "value", value)
    mode = str(raw or "both").strip().lower()
    if mode not in {"number", "text", "both"}:
        return "both"
    return mode


def _parse_text_value_after_article(
    value: str,
    *,
    value_mode: str,
    confirm_keywords: set[str],
    reject_keywords: set[str],
) -> tuple[Optional[str], Optional[int]]:
    normalized_text = normalize_external_status_text(value)
    if value_mode in {"number", "both"}:
        parsed_number = _safe_float(value)
        if parsed_number is not None:
            parsed_int = _safe_int(parsed_number)
            if parsed_int is None:
                parsed_int = int(parsed_number)
            if parsed_int <= 0:
                return "reject", 0
            return "confirm", parsed_int
        if value_mode == "number":
            return None, None
    if value_mode in {"text", "both"}:
        if normalized_text in reject_keywords:
            return "reject", 0
        if normalized_text in confirm_keywords:
            return "confirm", None
        for keyword in reject_keywords:
            if keyword and keyword in normalized_text:
                return "reject", 0
        for keyword in confirm_keywords:
            if keyword and keyword in normalized_text:
                return "confirm", None
    return None, None


def _parse_supplier_text_response(
    text: str,
    *,
    value_after_article_type: object,
    confirm_keywords: object,
    reject_keywords: object,
) -> ParsedSupplierTextResponse:
    value_mode = _normalize_value_after_article_type(value_after_article_type)
    confirm_set = _normalize_keywords(
        confirm_keywords,
        _DEFAULT_CONFIRM_KEYWORDS
    )
    reject_set = _normalize_keywords(
        reject_keywords,
        _DEFAULT_REJECT_KEYWORDS
    )
    tokens = _TEXT_TOKEN_RE.findall(text or "")
    result = ParsedSupplierTextResponse()
    if not tokens:
        return result

    for index, token in enumerate(tokens):
        if not _ARTICLE_TOKEN_RE.fullmatch(token or ""):
            continue
        if index + 1 >= len(tokens):
            result.unresolved.append(
                f"{token}: после артикула нет значения статуса"
            )
            continue
        status_token = tokens[index + 1]
        decision, qty = _parse_text_value_after_article(
            status_token,
            value_mode=value_mode,
            confirm_keywords=confirm_set,
            reject_keywords=reject_set,
        )
        if decision is None:
            result.unresolved.append(
                f"{token}: не удалось интерпретировать "
                f"значение '{status_token}'"
            )
            continue
        result.rows.append(
            ParsedSupplierResponseRow(
                oem_number=token,
                brand_name=None,
                confirmed_quantity=qty,
                response_price=None,
                response_comment=None,
                response_status_raw=status_token,
                text_decision=decision,
            )
        )
    return result


def _allowed_attachment_extensions(file_format: object) -> set[str]:
    raw = getattr(file_format, "value", file_format)
    normalized = str(raw or "").strip().lower()
    if normalized == "csv":
        return {"csv"}
    if normalized == "excel":
        return {"xlsx", "xls"}
    return {"xlsx", "xls", "csv"}


def _get_message_text_content(msg: object) -> str:
    text = str(getattr(msg, "text", "") or "").strip()
    if text:
        return text
    html = str(getattr(msg, "html", "") or "").strip()
    if html:
        return _strip_html(html)
    return ""


def _parse_supplier_response_attachment(
    payload: bytes,
    filename: str,
    *,
    file_payload_type: object = "response",
    start_row: object = 1,
    oem_col: object = None,
    brand_col: object = None,
    qty_col: object = None,
    price_col: object = None,
    comment_col: object = None,
    status_col: object = None,
    document_number_col: object = None,
    document_date_col: object = None,
    gtd_col: object = None,
    country_code_col: object = None,
    country_name_col: object = None,
    total_price_with_vat_col: object = None,
) -> list[ParsedSupplierResponseRow]:
    payload_type = str(
        getattr(file_payload_type, "value", file_payload_type) or "response"
    ).strip().lower()
    if payload_type not in {"response", "document"}:
        payload_type = "response"
    ext = _attachment_extension(filename)
    # Manual column mapping is enabled only when OEM column is provided.
    has_column_layout = _parse_positive_int(oem_col) is not None
    if ext == "csv":
        df = pd.read_csv(
            BytesIO(payload), header=None if has_column_layout else "infer"
        )
    elif ext in {"xlsx", "xls"}:
        df = pd.read_excel(
            BytesIO(payload),
            header=None if has_column_layout else 0,
        )
    else:
        return []
    start_row_num = _parse_positive_int(start_row) or 1
    if start_row_num > 1:
        df = df.iloc[start_row_num - 1:].reset_index(drop=True)
    if df.empty:
        return []

    if has_column_layout:
        oem_column = _resolve_column_by_number(df, oem_col)
        if oem_column is None:
            return []
        brand_column = _resolve_column_by_number(df, brand_col)
        qty_column = _resolve_column_by_number(df, qty_col)
        price_column = _resolve_column_by_number(df, price_col)
        comment_column = _resolve_column_by_number(df, comment_col)
        status_column = _resolve_column_by_number(df, status_col)
        document_number_column = _resolve_column_by_number(
            df,
            document_number_col,
        )
        document_date_column = _resolve_column_by_number(
            df,
            document_date_col,
        )
        gtd_column = _resolve_column_by_number(df, gtd_col)
        country_code_column = _resolve_column_by_number(df, country_code_col)
        country_name_column = _resolve_column_by_number(df, country_name_col)
        total_price_with_vat_column = _resolve_column_by_number(
            df,
            total_price_with_vat_col,
        )
    else:
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
        document_number_column = None
        for candidate in (
            "documentnumber",
            "номердокумента",
            "документномер",
            "номерупд",
            "номерттн",
        ):
            if candidate in headers:
                document_number_column = headers[candidate]
                break
        document_date_column = None
        for candidate in (
            "documentdate",
            "датадокумента",
            "датаупд",
            "датанакладной",
        ):
            if candidate in headers:
                document_date_column = headers[candidate]
                break
        gtd_column = None
        for candidate in ("gtd", "гтд"):
            if candidate in headers:
                gtd_column = headers[candidate]
                break
        country_code_column = None
        for candidate in (
            "countrycode",
            "кодстраны",
            "country_code",
        ):
            if candidate in headers:
                country_code_column = headers[candidate]
                break
        country_name_column = None
        for candidate in (
            "countryname",
            "страна",
            "названиестраны",
            "country_name",
        ):
            if candidate in headers:
                country_name_column = headers[candidate]
                break
        total_price_with_vat_column = None
        for candidate in (
            "sumwithvat",
            "summwithvat",
            "суммасндс",
            "сндс",
            "суммасндсруб",
        ):
            if candidate in headers:
                total_price_with_vat_column = headers[candidate]
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
        total_price_with_vat = None
        if total_price_with_vat_column is not None:
            raw_total_with_vat = row.get(total_price_with_vat_column)
            if raw_total_with_vat is not None and not pd.isna(
                raw_total_with_vat
            ):
                total_price_with_vat = _safe_float(raw_total_with_vat)
        if price_value is None:
            price_value = _resolve_price_without_vat(
                total_price_with_vat,
                qty_value,
            )
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
        document_number = None
        if document_number_column is not None:
            document_number = _clean_text_value(
                row.get(document_number_column)
            )
        document_date = None
        if document_date_column is not None:
            document_date = _parse_excel_like_date(
                row.get(document_date_column)
            )
        gtd_code = None
        if gtd_column is not None:
            gtd_code = _clean_text_value(row.get(gtd_column))
        country_code = None
        if country_code_column is not None:
            country_code = _clean_text_value(row.get(country_code_column))
        country_name = None
        if country_name_column is not None:
            country_name = _clean_text_value(row.get(country_name_column))
        parsed_rows.append(
            ParsedSupplierResponseRow(
                oem_number=oem_value,
                brand_name=brand_value,
                confirmed_quantity=qty_value,
                response_price=price_value,
                response_comment=comment_value,
                response_status_raw=status_value,
                document_number=document_number,
                document_date=document_date,
                gtd_code=gtd_code,
                country_code=country_code,
                country_name=country_name,
                total_price_with_vat=total_price_with_vat,
            )
        )
        if (
            payload_type == "document"
            and parsed_rows[-1].response_status_raw is None
        ):
            parsed_rows[-1].response_status_raw = "документ"
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
    if order_id is not None and not (1 <= order_id <= _MAX_INT32):
        logger.warning(
            (
                "Supplier response parsed order id ignored before lookup: "
                "value=%s out_of_int32_range"
            ),
            order_id,
        )
        order_id = None
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


def _build_import_error_details(reasons: Iterable[str]) -> Optional[str]:
    seen: set[str] = set()
    normalized: list[str] = []
    for raw in reasons:
        item = str(raw or "").strip()
        if not item:
            continue
        if item in seen:
            continue
        seen.add(item)
        normalized.append(item)
    if not normalized:
        return None
    return "; ".join(normalized)[:500]


def _parse_source_uid(
    source_uid: Optional[str],
) -> tuple[Optional[int], Optional[str], Optional[str]]:
    if not source_uid:
        return None, None, None
    raw = str(source_uid)
    first, sep, rest = raw.partition(":")
    account_id: Optional[int] = None
    try:
        parsed = int(first.strip())
        if parsed > 0:
            account_id = parsed
    except (TypeError, ValueError):
        account_id = None
    if not sep:
        return account_id, None, None
    folder_raw, sep2, uid_raw = rest.partition(":")
    folder = folder_raw.strip() or None
    if not sep2:
        return account_id, folder, None
    uid = uid_raw.strip() or None
    return account_id, folder, uid


def _extract_account_id_from_source_uid(
    source_uid: Optional[str],
) -> Optional[int]:
    account_id, _, _ = _parse_source_uid(source_uid)
    return account_id


def _build_import_error_hints(
    *,
    response_type: str,
    reasons: list[str],
    has_attachments: bool,
    subject: str,
    subject_raw: Optional[str],
) -> list[str]:
    hints: list[str] = []
    lowered = [item.lower() for item in reasons]
    if subject_raw and subject and subject_raw != subject:
        hints.append(
            (
                "Тема декодирована из MIME. Если шаблон ищется по теме, "
                "сверяйте с полем «Тема (как прочитана)»."
            )
        )
    if response_type == "file" and not has_attachments:
        hints.append(
            (
                "В конфигурации выбран режим «Файл», но во входящем письме "
                "нет вложений."
            )
        )
    if any("шаблон" in item and "имя файла" in item for item in lowered):
        hints.append(
            (
                "Проверьте regex «Шаблон имени файла»: он должен совпадать "
                "с фактическим именем вложения."
            )
        )
    if any("формат файла" in item for item in lowered):
        hints.append(
            (
                "Проверьте «Формат файла» (Excel/CSV): формат письма и "
                "настройки должны совпадать."
            )
        )
    if any("текст письма не удалось разобрать" in item for item in lowered):
        hints.append(
            (
                "Для текстового ответа проверьте словари статусов и "
                "«Что ожидаем после артикула»."
            )
        )
    if any("позиции не сопоставлены" in item for item in lowered):
        hints.append(
            (
                "Проверьте OEM/бренд в ответе: позиции не сопоставились "
                "с открытыми заказами поставщику."
            )
        )
    if has_attachments and response_type == "file":
        hints.append(
            (
                "Сверьте список вложений ниже: имя файла и расширение "
                "должны соответствовать настройке."
            )
        )
    return hints


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
) -> tuple[int, int, list[str], list[AppliedSupplierResponseRow]]:
    if not parsed_rows:
        return 0, 0, [], []
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
    matched_count = 0
    unresolved_oems: list[str] = []
    applied_rows: list[AppliedSupplierResponseRow] = []
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
            unresolved_oems.append(row.oem_number)
            continue
        matched_count += 1

        if _apply_row_to_item(
            row=row,
            matched_item=matched_item,
            default_raw_status=default_raw_status,
            default_normalized_status=default_normalized_status,
        ):
            updated += 1
        applied_rows.append(
            _build_applied_row_payload(
                row=row,
                matched_item=matched_item,
            )
        )
    return updated, matched_count, unresolved_oems, applied_rows


def _supplier_order_item_expected_quantity(item: SupplierOrderItem) -> int:
    if item.confirmed_quantity is not None:
        return int(item.confirmed_quantity or 0)
    return int(item.quantity or 0)


def _supplier_order_item_pending_quantity(item: SupplierOrderItem) -> int:
    expected_quantity = _supplier_order_item_expected_quantity(item)
    current_received = int(item.received_quantity or 0)
    return max(expected_quantity - current_received, 0)


def _select_single_item_candidate(
    candidates: list[SupplierOrderItem],
) -> Optional[SupplierOrderItem]:
    if not candidates:
        return None
    pending_candidates = [
        item
        for item in candidates
        if _supplier_order_item_pending_quantity(item) > 0
    ]
    if len(pending_candidates) == 1:
        return pending_candidates[0]
    if len(pending_candidates) > 1:
        return None
    if len(candidates) == 1:
        return candidates[0]
    return None


def _build_applied_row_payload(
    *,
    row: ParsedSupplierResponseRow,
    matched_item: SupplierOrderItem,
) -> AppliedSupplierResponseRow:
    resolved_qty = row.confirmed_quantity
    if resolved_qty is None:
        if row.text_decision == "reject":
            resolved_qty = 0
        elif row.text_decision == "confirm":
            resolved_qty = matched_item.confirmed_quantity
    parsed_qty = _safe_int(resolved_qty)
    if parsed_qty is None and resolved_qty not in (None, ""):
        try:
            parsed_qty = int(float(resolved_qty))
        except (TypeError, ValueError):
            parsed_qty = None
    if parsed_qty is None:
        parsed_qty = 0
    if parsed_qty < 0:
        parsed_qty = 0
    return AppliedSupplierResponseRow(
        supplier_order_item_id=int(matched_item.id),
        supplier_order_id=int(matched_item.supplier_order_id),
        received_quantity=parsed_qty,
        comment=row.response_comment or matched_item.response_comment,
        response_price=row.response_price,
        document_number=row.document_number,
        document_date=row.document_date,
        gtd_code=row.gtd_code,
        country_code=row.country_code,
        country_name=row.country_name,
        total_price_with_vat=row.total_price_with_vat,
    )


def _apply_row_to_item(
    *,
    row: ParsedSupplierResponseRow,
    matched_item: SupplierOrderItem,
    default_raw_status: Optional[str],
    default_normalized_status: Optional[str],
) -> bool:
    item_changed = False
    next_confirmed_quantity = row.confirmed_quantity
    if next_confirmed_quantity is None:
        if row.text_decision == "reject":
            next_confirmed_quantity = 0
        elif row.text_decision == "confirm":
            next_confirmed_quantity = matched_item.quantity
    if (
        next_confirmed_quantity is not None
        and matched_item.confirmed_quantity != next_confirmed_quantity
    ):
        matched_item.confirmed_quantity = next_confirmed_quantity
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
    return item_changed


async def _load_recent_provider_orders(
    session: AsyncSession,
    *,
    provider_id: int,
    date_from: date,
) -> list[SupplierOrder]:
    start_dt = datetime.combine(
        date_from,
        datetime.min.time(),
        tzinfo=now_moscow().tzinfo,
    )
    stmt = (
        select(SupplierOrder)
        .options(selectinload(SupplierOrder.items))
        .where(
            SupplierOrder.provider_id == provider_id,
            SupplierOrder.created_at >= start_dt,
        )
        .order_by(SupplierOrder.created_at.desc(), SupplierOrder.id.desc())
    )
    return list((await session.execute(stmt)).scalars().all())


async def _apply_parsed_rows_without_order_id(
    session: AsyncSession,
    *,
    provider_id: int,
    parsed_rows: list[ParsedSupplierResponseRow],
    default_raw_status: Optional[str],
    default_normalized_status: Optional[str],
    date_from: date,
) -> tuple[
    int,
    int,
    list[str],
    dict[int, list[AppliedSupplierResponseRow]],
]:
    if not parsed_rows:
        return 0, 0, [], {}
    provider_orders = await _load_recent_provider_orders(
        session,
        provider_id=provider_id,
        date_from=date_from,
    )
    if not provider_orders:
        return (
            0,
            0,
            [row.oem_number for row in parsed_rows],
            {},
        )
    brand_aliases = await _load_brand_alias_map(session)
    exact_map: dict[tuple[str, str], list[SupplierOrderItem]] = {}
    oem_map: dict[str, list[SupplierOrderItem]] = {}
    for order in provider_orders:
        for item in order.items or []:
            key = _normalize_key(
                item.oem_number,
                item.brand_name,
                brand_aliases,
            )
            exact_map.setdefault(key, []).append(item)
            oem_key = _normalize_oem_key(item.oem_number)
            if oem_key:
                oem_map.setdefault(oem_key, []).append(item)
    updated = 0
    matched_count = 0
    unresolved_oems: list[str] = []
    applied_rows_by_order: dict[int, list[AppliedSupplierResponseRow]] = {}
    for row in parsed_rows:
        exact_key = _normalize_key(
            row.oem_number,
            row.brand_name,
            brand_aliases,
        )
        matched_item = _select_single_item_candidate(
            exact_map.get(exact_key) or []
        )
        if matched_item is None:
            oem_key = _normalize_oem_key(row.oem_number)
            matched_item = _select_single_item_candidate(
                oem_map.get(oem_key) or []
            )
        if matched_item is None:
            unresolved_oems.append(row.oem_number)
            continue
        matched_count += 1
        if _apply_row_to_item(
            row=row,
            matched_item=matched_item,
            default_raw_status=default_raw_status,
            default_normalized_status=default_normalized_status,
        ):
            updated += 1
        applied_row = _build_applied_row_payload(
            row=row,
            matched_item=matched_item,
        )
        applied_rows_by_order.setdefault(
            int(matched_item.supplier_order_id),
            [],
        ).append(applied_row)
    return updated, matched_count, unresolved_oems, applied_rows_by_order


def _extract_shipping_document_number(
    shipping_filenames: list[str],
) -> Optional[str]:
    for filename in shipping_filenames:
        clean = str(filename or "").strip()
        if not clean:
            continue
        base_name = clean.rsplit(".", 1)[0].strip()
        if base_name:
            return base_name[:120]
        return clean[:120]
    return None


def _build_pending_receipt_items(
        order: SupplierOrder
) -> list[dict[str, object]]:
    items_payload: list[dict[str, object]] = []
    for order_item in order.items or []:
        expected_quantity = (
            int(order_item.confirmed_quantity)
            if order_item.confirmed_quantity is not None
            else int(order_item.quantity or 0)
        )
        current_received = int(order_item.received_quantity or 0)
        pending_quantity = max(expected_quantity - current_received, 0)
        if pending_quantity <= 0:
            continue
        items_payload.append(
            {
                "supplier_order_item_id": int(order_item.id),
                "received_quantity": pending_quantity,
                "comment": order_item.response_comment,
            }
        )
    return items_payload


def _build_receipt_items_from_applied_rows(
    order: SupplierOrder,
    applied_rows: list[AppliedSupplierResponseRow],
    *,
    cap_to_pending: bool,
) -> list[dict[str, object]]:
    if not applied_rows:
        return []
    order_items_by_id = {
        int(order_item.id): order_item for order_item in (order.items or [])
    }
    deduplicated: dict[int, AppliedSupplierResponseRow] = {}
    for row in applied_rows:
        if row.received_quantity <= 0:
            continue
        deduplicated[int(row.supplier_order_item_id)] = row

    items_payload: list[dict[str, object]] = []
    for supplier_order_item_id, row in deduplicated.items():
        order_item = order_items_by_id.get(supplier_order_item_id)
        if order_item is None:
            continue
        expected_quantity = (
            int(order_item.confirmed_quantity)
            if order_item.confirmed_quantity is not None
            else int(order_item.quantity or 0)
        )
        quantity = int(row.received_quantity or 0)
        if expected_quantity > 0:
            quantity = min(quantity, expected_quantity)
        if cap_to_pending:
            current_received = int(order_item.received_quantity or 0)
            pending_quantity = max(expected_quantity - current_received, 0)
            quantity = min(quantity, pending_quantity)
        if quantity <= 0:
            continue
        items_payload.append(
            {
                "supplier_order_item_id": supplier_order_item_id,
                "received_quantity": quantity,
                "comment": row.comment,
                "response_price": row.response_price,
                "gtd_code": row.gtd_code,
                "country_code": row.country_code,
                "country_name": row.country_name,
                "total_price_with_vat": row.total_price_with_vat,
            }
        )
    return items_payload


async def _find_open_supplier_receipt(
    session: AsyncSession,
    *,
    provider_id: int,
) -> Optional[SupplierReceipt]:
    stmt = (
        select(SupplierReceipt)
        .where(
            SupplierReceipt.provider_id == provider_id,
            SupplierReceipt.posted_at.is_(None),
        )
        .order_by(SupplierReceipt.created_at.desc(), SupplierReceipt.id.desc())
    )
    return (await session.execute(stmt)).scalars().first()


async def _append_supplier_receipt_items(
    session: AsyncSession,
    *,
    receipt: SupplierReceipt,
    order: SupplierOrder,
    items_payload: list[dict[str, object]],
    post_now: bool,
) -> int:
    if not items_payload:
        return 0
    order_items_by_id = {
        int(order_item.id): order_item for order_item in (order.items or [])
    }
    supplier_order_ids: set[int] = set()
    added = 0
    for payload in items_payload:
        supplier_order_item_id = int(payload.get("supplier_order_item_id"))
        order_item = order_items_by_id.get(supplier_order_item_id)
        if order_item is None:
            continue
        quantity = int(payload.get("received_quantity") or 0)
        if quantity <= 0:
            continue
        expected_quantity = (
            int(order_item.confirmed_quantity)
            if order_item.confirmed_quantity is not None
            else int(order_item.quantity or 0)
        )
        if post_now:
            current_received = int(order_item.received_quantity or 0)
            pending_quantity = max(expected_quantity - current_received, 0)
            quantity = min(quantity, pending_quantity)
            if quantity <= 0:
                continue
            order_item.received_quantity = current_received + quantity
            order_item.received_at = now_moscow()
        supplier_order_ids.add(int(order_item.supplier_order_id))
        session.add(
            SupplierReceiptItem(
                receipt_id=receipt.id,
                supplier_order_id=order_item.supplier_order_id,
                supplier_order_item_id=order_item.id,
                customer_order_item_id=order_item.customer_order_item_id,
                autopart_id=order_item.autopart_id,
                oem_number=order_item.oem_number,
                brand_name=order_item.brand_name,
                autopart_name=order_item.autopart_name,
                ordered_quantity=order_item.quantity,
                confirmed_quantity=order_item.confirmed_quantity,
                received_quantity=quantity,
                price=(
                    payload.get("response_price")
                    or order_item.response_price
                    or order_item.price
                ),
                comment=(
                    str(payload.get("comment") or "").strip()
                    or order_item.response_comment
                    or None
                ),
                gtd_code=str(payload.get("gtd_code") or "").strip() or None,
                country_code=(
                    str(payload.get("country_code") or "").strip() or None
                ),
                country_name=(
                    str(payload.get("country_name") or "").strip() or None
                ),
                total_price_with_vat=payload.get("total_price_with_vat"),
            )
        )
        added += 1
    if added and len(supplier_order_ids) == 1:
        receipt.supplier_order_id = next(iter(supplier_order_ids))
    if added and post_now:
        receipt.posted_at = now_moscow()
    return added


async def _create_or_update_supplier_receipt_from_message(
    session: AsyncSession,
    *,
    provider_id: int,
    order: SupplierOrder,
    message_row: SupplierOrderMessage,
    items_payload: list[dict[str, object]],
    post_now: bool,
    document_number: Optional[str] = None,
    document_date: Optional[date] = None,
    comment: Optional[str] = None,
) -> tuple[Optional[SupplierReceipt], int, bool]:
    if not items_payload:
        return None, 0, False
    created = False
    receipt: Optional[SupplierReceipt]
    if post_now:
        receipt = SupplierReceipt(
            provider_id=provider_id,
            supplier_order_id=None,
            source_message_id=message_row.id,
            document_number=document_number or None,
            document_date=document_date or now_moscow().date(),
            created_by_user_id=None,
            created_at=now_moscow(),
            posted_at=now_moscow(),
            comment=comment,
        )
        session.add(receipt)
        await session.flush()
        created = True
    else:
        receipt = await _find_open_supplier_receipt(
            session,
            provider_id=provider_id,
        )
        if receipt is None:
            receipt = SupplierReceipt(
                provider_id=provider_id,
                supplier_order_id=None,
                source_message_id=message_row.id,
                document_number=None,
                document_date=document_date or now_moscow().date(),
                created_by_user_id=None,
                created_at=now_moscow(),
                posted_at=None,
                comment=comment,
            )
            session.add(receipt)
            await session.flush()
            created = True

    added_items = await _append_supplier_receipt_items(
        session,
        receipt=receipt,
        order=order,
        items_payload=items_payload,
        post_now=post_now,
    )
    if added_items <= 0:
        if created:
            await session.delete(receipt)
            await session.flush()
        return None, 0, False
    return receipt, added_items, created


async def _load_supplier_response_configs(
    session: AsyncSession,
    *,
    provider_id: Optional[int] = None,
    supplier_response_config_id: Optional[int] = None,
) -> list[SupplierResponseConfig]:
    stmt = (
        select(SupplierResponseConfig)
        .options(joinedload(SupplierResponseConfig.provider))
        .where(SupplierResponseConfig.is_active.is_(True))
    )
    if provider_id is not None:
        stmt = stmt.where(SupplierResponseConfig.provider_id == provider_id)
    if supplier_response_config_id is not None:
        stmt = stmt.where(
            SupplierResponseConfig.id == supplier_response_config_id
        )
    stmt = stmt.order_by(SupplierResponseConfig.id.asc())
    return list((await session.execute(stmt)).scalars().all())


def _group_response_configs_by_provider(
    configs: Iterable[SupplierResponseConfig],
) -> dict[int, list[SupplierResponseConfig]]:
    grouped: dict[int, list[SupplierResponseConfig]] = {}
    for config in configs:
        grouped.setdefault(int(config.provider_id), []).append(config)
    return grouped


async def process_supplier_response_messages(
    session: AsyncSession,
    *,
    provider_id: Optional[int] = None,
    supplier_response_config_id: Optional[int] = None,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
) -> dict[str, object]:
    lookback_days = None
    if date_from is None:
        lookback_days = await _get_supplier_response_lookback_days(session)
        date_from = supplier_response_cutoff(days=lookback_days)
    logger.info(
        (
            "Supplier response processing started: provider_id=%s "
            "config_id=%s date_from=%s date_to=%s lookback_days=%s"
        ),
        provider_id,
        supplier_response_config_id,
        date_from,
        date_to,
        lookback_days,
    )
    stats = SupplierResponseProcessingStats()
    response_configs = await _load_supplier_response_configs(
        session,
        provider_id=provider_id,
        supplier_response_config_id=supplier_response_config_id,
    )
    configs_by_provider = _group_response_configs_by_provider(response_configs)
    selected_config = None
    if supplier_response_config_id is not None and response_configs:
        selected_config = response_configs[0]

    explicit_account_ids = {
        int(config.inbox_email_account_id)
        for config in response_configs
        if config.inbox_email_account_id is not None
    }
    include_default_orders_out = True
    if (
        supplier_response_config_id is not None
        and selected_config is not None
        and selected_config.inbox_email_account_id is not None
    ):
        include_default_orders_out = False

    fetch_kwargs: dict[str, object] = {
        "date_from": date_from,
        "date_to": date_to,
    }
    if explicit_account_ids:
        fetch_kwargs["account_ids"] = explicit_account_ids
    if not include_default_orders_out:
        fetch_kwargs["include_default_orders_out"] = False
    messages = await _fetch_supplier_response_messages(session, **fetch_kwargs)
    stats.fetched_messages = len(messages)
    logger.info(
        (
            "Supplier response messages fetched: count=%s "
            "provider_id=%s config_id=%s"
        ),
        stats.fetched_messages,
        provider_id,
        supplier_response_config_id,
    )

    total_messages = len(messages)
    for index, (msg, account) in enumerate(messages, start=1):
        source_uid = _build_source_uid(msg, account)
        source_message_id = _build_source_message_id(msg)
        if await _message_already_processed(
            session,
            source_uid=source_uid,
            source_message_id=source_message_id,
        ):
            logger.info(
                (
                    "Supplier response message skipped as duplicate: "
                    "idx=%s/%s source_uid=%s source_message_id=%s"
                ),
                index,
                total_messages,
                source_uid,
                source_message_id,
            )
            stats.skipped_messages += 1
            continue

        attachments = _iter_message_attachments(msg)
        sender_email = _extract_email(getattr(msg, "from_", None))
        subject = str(getattr(msg, "subject", "") or "")
        logger.info(
            (
                "Supplier response message start: idx=%s/%s sender=%s "
                "subject=%s attachments=%s account_id=%s"
            ),
            index,
            total_messages,
            sender_email,
            subject[:200],
            len(attachments),
            account.id if account else None,
        )
        body_preview = _get_message_body_preview(msg)
        message_text = _get_message_text_content(msg)
        raw_status = _detect_supplier_status(subject, body_preview)
        normalized_status = normalize_external_status_text(raw_status)
        recognized_before = stats.recognized_positions
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
            active_response_config: Optional[SupplierResponseConfig] = None
            if selected_config is not None:
                active_response_config = selected_config
                if provider is None:
                    provider = selected_config.provider
                if not _config_matches_message(
                    active_response_config,
                    sender_email=sender_email,
                    account=account,
                ):
                    reasons = _config_mismatch_reasons(
                        active_response_config,
                        sender_email=sender_email,
                        account=account,
                    )
                    logger.info(
                        (
                            "Supplier response message skipped: "
                            "selected config mismatch config_id=%s "
                            "provider_id=%s sender=%s account_id=%s "
                            "reasons=%s subject=%s"
                        ),
                        active_response_config.id,
                        active_response_config.provider_id,
                        sender_email or "<empty>",
                        account.id if account else None,
                        reasons,
                        subject[:200],
                    )
                    stats.skipped_messages += 1
                    continue
            elif provider is not None:
                provider_specific_configs = configs_by_provider.get(
                    provider.id, []
                )
                active_response_config = _select_best_supplier_response_config(
                    provider_specific_configs,
                    sender_email=sender_email,
                    account=account,
                )
                if (
                    provider_specific_configs
                    and active_response_config is None
                ):
                    details = [
                        (
                            f"cfg#{cfg.id}: "
                            f"{'; '.join(_config_mismatch_reasons(  # noqa: E501
                                cfg,
                                sender_email=sender_email,
                                account=account,
                            ))}"
                        )
                        for cfg in provider_specific_configs
                    ]
                    logger.info(
                        (
                            "Supplier response message skipped: "
                            "no provider config matched provider_id=%s "
                            "sender=%s account_id=%s details=%s "
                            "subject=%s"
                        ),
                        provider.id,
                        sender_email or "<empty>",
                        account.id if account else None,
                        details,
                        subject[:200],
                    )
                    stats.skipped_messages += 1
                    continue
            else:
                active_response_config = _select_best_supplier_response_config(
                    response_configs,
                    sender_email=sender_email,
                    account=account,
                )
                if active_response_config is not None:
                    provider = active_response_config.provider

            if (
                provider is not None
                and active_response_config is not None
                and int(active_response_config.provider_id) != int(provider.id)
            ):
                stats.skipped_messages += 1
                continue
            if (
                provider_id is not None
                and provider
                and provider.id != provider_id
            ):
                stats.skipped_messages += 1
                continue
            if provider is None:
                if (
                        provider_id is not None
                        or supplier_response_config_id is not None
                ):
                    logger.info(
                        (
                            "Supplier response message skipped: "
                            "provider not resolved for sender=%s "
                            "subject=%s account_id=%s"
                        ),
                        sender_email or "<empty>",
                        subject[:200],
                        account.id if account else None,
                    )
                    stats.skipped_messages += 1
                    continue
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

            if order is None:
                logger.info(
                    (
                        "Supplier response message has no explicit order id: "
                        "provider_id=%s sender=%s subject=%s"
                    ),
                    provider.id,
                    sender_email,
                    subject,
                )

            if active_response_config is not None:
                response_type_raw = getattr(
                    active_response_config,
                    "response_type",
                    "file",
                )
                response_type = str(
                    getattr(response_type_raw, "value", response_type_raw)
                ).strip().lower()
                allow_shipping_docs = bool(
                    getattr(
                        active_response_config,
                        "process_shipping_docs",
                        True,
                    )
                )
                allow_response_files = response_type == "file"
                allow_text_status = response_type == "text"
                response_filename_pattern = _compile_filename_pattern(
                    getattr(active_response_config, "filename_pattern", None)
                )
                shipping_doc_filename_pattern = _compile_filename_pattern(
                    getattr(
                        active_response_config,
                        "shipping_doc_filename_pattern",
                        None,
                    )
                )
                response_file_format = getattr(
                    active_response_config,
                    "file_format",
                    None,
                )
                file_payload_type = getattr(
                    active_response_config,
                    "file_payload_type",
                    "response",
                )
                response_start_row = getattr(
                    active_response_config,
                    "start_row",
                    1,
                )
                response_oem_col = getattr(
                    active_response_config,
                    "oem_col",
                    None,
                )
                response_brand_col = getattr(
                    active_response_config,
                    "brand_col",
                    None,
                )
                response_qty_col = getattr(
                    active_response_config,
                    "qty_col",
                    None,
                )
                response_price_col = getattr(
                    active_response_config,
                    "price_col",
                    None,
                )
                response_comment_col = getattr(
                    active_response_config,
                    "comment_col",
                    None,
                )
                response_status_col = getattr(
                    active_response_config,
                    "status_col",
                    None,
                )
                response_document_number_col = getattr(
                    active_response_config,
                    "document_number_col",
                    None,
                )
                response_document_date_col = getattr(
                    active_response_config,
                    "document_date_col",
                    None,
                )
                response_gtd_col = getattr(
                    active_response_config,
                    "gtd_col",
                    None,
                )
                response_country_code_col = getattr(
                    active_response_config,
                    "country_code_col",
                    None,
                )
                response_country_name_col = getattr(
                    active_response_config,
                    "country_name_col",
                    None,
                )
                response_total_price_with_vat_col = getattr(
                    active_response_config,
                    "total_price_with_vat_col",
                    None,
                )
                confirm_keywords = getattr(
                    active_response_config,
                    "confirm_keywords",
                    _DEFAULT_CONFIRM_KEYWORDS,
                )
                reject_keywords = getattr(
                    active_response_config,
                    "reject_keywords",
                    _DEFAULT_REJECT_KEYWORDS,
                )
                value_after_article_type = getattr(
                    active_response_config,
                    "value_after_article_type",
                    "both",
                )
            else:
                response_type = "file"
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
                response_filename_pattern = _compile_filename_pattern(
                    getattr(
                        provider,
                        "supplier_response_filename_pattern",
                        None
                    )
                )
                shipping_doc_filename_pattern = _compile_filename_pattern(
                    getattr(
                        provider,
                        "supplier_shipping_doc_filename_pattern",
                        None,
                    )
                )
                response_file_format = None
                file_payload_type = "response"
                response_start_row = getattr(
                    provider,
                    "supplier_response_start_row",
                    1,
                )
                response_oem_col = getattr(
                    provider,
                    "supplier_response_oem_col",
                    None,
                )
                response_brand_col = getattr(
                    provider,
                    "supplier_response_brand_col",
                    None,
                )
                response_qty_col = getattr(
                    provider,
                    "supplier_response_qty_col",
                    None,
                )
                response_price_col = getattr(
                    provider,
                    "supplier_response_price_col",
                    None,
                )
                response_comment_col = getattr(
                    provider,
                    "supplier_response_comment_col",
                    None,
                )
                response_status_col = getattr(
                    provider,
                    "supplier_response_status_col",
                    None,
                )
                response_document_number_col = None
                response_document_date_col = None
                response_gtd_col = None
                response_country_code_col = None
                response_country_name_col = None
                response_total_price_with_vat_col = None
                confirm_keywords = _DEFAULT_CONFIRM_KEYWORDS
                reject_keywords = _DEFAULT_REJECT_KEYWORDS
                value_after_article_type = "both"

            normalized_file_payload_type = str(
                getattr(file_payload_type, "value", file_payload_type)
                or "response"
            ).strip().lower()
            if normalized_file_payload_type not in {"response", "document"}:
                normalized_file_payload_type = "response"
            file_payload_is_document = (
                normalized_file_payload_type == "document"
            )
            expects_file_payload = (
                active_response_config is not None and response_type == "file"
            )
            expects_text_payload = (
                active_response_config is not None and response_type == "text"
            )
            logger.info(
                (
                    "Supplier response config resolved: config_id=%s "
                    "provider_id=%s response_type=%s sender_filter=%s "
                    "filename_pattern=%s shipping_pattern=%s "
                    "payload_type=%s account_filter=%s"
                ),
                active_response_config.id if active_response_config else None,
                provider.id,
                (
                    str(getattr(response_type_raw, "value", response_type_raw))
                    .strip()
                    .lower()
                    if active_response_config is not None
                    else "legacy"
                ),
                (
                    sorted(
                        _normalize_sender_emails(
                            active_response_config.sender_emails
                        )
                    )
                    if active_response_config is not None
                    else []
                ),
                (
                    response_filename_pattern.pattern
                    if response_filename_pattern is not None
                    else None
                ),
                (
                    shipping_doc_filename_pattern.pattern
                    if shipping_doc_filename_pattern is not None
                    else None
                ),
                normalized_file_payload_type,
                (
                    active_response_config.inbox_email_account_id
                    if active_response_config is not None
                    else None
                ),
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
                response_config_id=(
                    active_response_config.id
                    if active_response_config is not None
                    else None
                ),
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
            parsed_text_rows = False
            has_shipping_doc = False
            import_error_reasons: list[str] = []
            shipping_doc_filenames: list[str] = []
            matched_orders: dict[int, SupplierOrder] = {}
            if order is not None:
                matched_orders[int(order.id)] = order
            matched_order_ids_from_rows: set[int] = set()
            receipt_applied_rows_by_order: dict[
                int,
                list[AppliedSupplierResponseRow],
            ] = {}
            for attachment in attachments:
                file_path, digest = await _store_supplier_message_attachment(
                    message_id=message_row.id,
                    attachment=attachment,
                )
                attachment_kind = _classify_attachment_kind(
                    attachment.filename,
                    response_pattern=response_filename_pattern,
                    shipping_pattern=shipping_doc_filename_pattern,
                )
                if (
                    attachment_kind == "SHIPPING_DOC"
                    and not allow_shipping_docs
                ):
                    attachment_kind = None
                if attachment_kind == "SHIPPING_DOC":
                    has_shipping_doc = True
                    if attachment.filename:
                        shipping_doc_filenames.append(str(attachment.filename))
                parsed_rows: list[ParsedSupplierResponseRow] = []
                extension = _attachment_extension(attachment.filename)
                is_spreadsheet = extension in {"xlsx", "xls", "csv"}
                if response_filename_pattern is None:
                    response_candidate = (
                        attachment_kind == "RESPONSE_FILE" or is_spreadsheet
                    )
                else:
                    response_candidate = attachment_kind == "RESPONSE_FILE"
                    if not response_candidate and is_spreadsheet:
                        import_error_reasons.append(
                            (
                                "Имя файла не подходит под шаблон: "
                                f"{attachment.filename or '<empty>'}"
                            )
                        )
                        logger.info(
                            (
                                "Supplier response attachment skipped by "
                                "filename_pattern: config_id=%s filename=%s "
                                "pattern=%s"
                            ),
                            (
                                active_response_config.id
                                if active_response_config is not None
                                else None
                            ),
                            attachment.filename or "<empty>",
                            response_filename_pattern.pattern,
                        )
                if (
                    response_candidate
                    and extension not in _allowed_attachment_extensions(
                        response_file_format
                    )
                ):
                    import_error_reasons.append(
                        (
                            "Формат файла не подходит для конфигурации: "
                            f"{attachment.filename or '<empty>'}"
                        )
                    )
                    logger.info(
                        (
                            "Supplier response attachment skipped by "
                            "file_format: config_id=%s filename=%s "
                            "extension=%s allowed=%s"
                        ),
                        (
                            active_response_config.id
                            if active_response_config is not None
                            else None
                        ),
                        attachment.filename or "<empty>",
                        extension or "<none>",
                        sorted(
                            _allowed_attachment_extensions(
                                response_file_format
                            )
                        ),
                    )
                    response_candidate = False
                if (
                    allow_response_files
                    and response_candidate
                ):
                    try:
                        parsed_rows = _parse_supplier_response_attachment(
                            attachment.payload,
                            attachment.filename or "",
                            file_payload_type=normalized_file_payload_type,
                            start_row=response_start_row,
                            oem_col=response_oem_col,
                            brand_col=response_brand_col,
                            qty_col=response_qty_col,
                            price_col=response_price_col,
                            comment_col=response_comment_col,
                            status_col=response_status_col,
                            document_number_col=response_document_number_col,
                            document_date_col=response_document_date_col,
                            gtd_col=response_gtd_col,
                            country_code_col=response_country_code_col,
                            country_name_col=response_country_name_col,
                            total_price_with_vat_col=(
                                response_total_price_with_vat_col
                            ),
                        )
                    except Exception as exc:
                        import_error_reasons.append(
                            (
                                "Ошибка разбора вложения "
                                f"{attachment.filename or '<empty>'}: {exc}"
                            )
                        )
                        logger.warning(
                            (
                                "Failed to parse supplier response "
                                "attachment %s: %s"
                            ),
                            attachment.filename,
                            exc,
                        )
                if parsed_rows:
                    if file_payload_is_document:
                        has_shipping_doc = True
                        if attachment.filename:
                            shipping_doc_filenames.append(
                                str(attachment.filename)
                            )
                        attachment_kind = "SHIPPING_DOC"
                    else:
                        attachment_kind = "RESPONSE_FILE"
                    parsed_response_file = True
                    stats.parsed_response_files += 1
                    if order is not None:
                        (
                            updated_items,
                            matched_count,
                            unresolved_oems,
                            applied_rows,
                        ) = (
                            await _apply_parsed_response_rows(
                                session,
                                order=order,
                                parsed_rows=parsed_rows,
                                default_raw_status=raw_status,
                                default_normalized_status=normalized_status,
                            )
                        )
                        stats.updated_items += updated_items
                        receipt_applied_rows_by_order.setdefault(
                            int(order.id),
                            [],
                        ).extend(applied_rows)
                        stats.recognized_positions += matched_count
                        for unresolved_oem in unresolved_oems:
                            stats.add_unresolved(
                                (
                                    f"{unresolved_oem}: строка заказа "
                                    "не найдена"
                                )
                            )
                    else:
                        (
                            updated_items,
                            matched_count,
                            unresolved_oems,
                            applied_rows_map,
                        ) = await _apply_parsed_rows_without_order_id(
                            session,
                            provider_id=provider.id,
                            parsed_rows=parsed_rows,
                            default_raw_status=raw_status,
                            default_normalized_status=(
                                normalized_status or None
                            ),
                            date_from=date_from,
                        )
                        stats.updated_items += updated_items
                        stats.recognized_positions += matched_count
                        for order_key, rows in applied_rows_map.items():
                            matched_order_ids_from_rows.add(int(order_key))
                            receipt_applied_rows_by_order.setdefault(
                                int(order_key),
                                [],
                            ).extend(rows)
                        for unresolved_oem in unresolved_oems:
                            stats.add_unresolved(
                                (
                                    f"{unresolved_oem}: строка заказа "
                                    "не найдена/неоднозначна"
                                )
                            )
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

            if expects_file_payload and not attachments:
                import_error_reasons.append(
                    "В письме нет вложений для разбора ответа"
                )
            if (
                expects_file_payload
                and attachments
                and not parsed_response_file
                and not has_shipping_doc
            ):
                import_error_reasons.append(
                    (
                        "Не найдено подходящее вложение ответа "
                        "по текущим настройкам"
                    )
                )

            if (
                allow_text_status
                and provider is not None
                and message_text
            ):
                parsed_text = _parse_supplier_text_response(
                    message_text,
                    value_after_article_type=value_after_article_type,
                    confirm_keywords=confirm_keywords,
                    reject_keywords=reject_keywords,
                )
                for unresolved_entry in parsed_text.unresolved:
                    stats.add_unresolved(unresolved_entry)
                if parsed_text.rows:
                    (
                        updated_items,
                        matched_count,
                        unresolved_oems,
                        applied_rows,
                    ) = (0, 0, [], [])
                    if order is not None:
                        (
                            updated_items,
                            matched_count,
                            unresolved_oems,
                            applied_rows,
                        ) = await _apply_parsed_response_rows(
                            session,
                            order=order,
                            parsed_rows=parsed_text.rows,
                            default_raw_status=raw_status,
                            default_normalized_status=(
                                normalized_status or None
                            ),
                        )
                        receipt_applied_rows_by_order.setdefault(
                            int(order.id),
                            [],
                        ).extend(applied_rows)
                    else:
                        (
                            updated_items,
                            matched_count,
                            unresolved_oems,
                            applied_rows_map,
                        ) = await _apply_parsed_rows_without_order_id(
                            session,
                            provider_id=provider.id,
                            parsed_rows=parsed_text.rows,
                            default_raw_status=raw_status,
                            default_normalized_status=(
                                normalized_status or None
                            ),
                            date_from=date_from,
                        )
                        for order_key, rows in applied_rows_map.items():
                            matched_order_ids_from_rows.add(int(order_key))
                            receipt_applied_rows_by_order.setdefault(
                                int(order_key),
                                [],
                            ).extend(rows)
                    stats.updated_items += updated_items
                    stats.parsed_text_positions += len(parsed_text.rows)
                    stats.recognized_positions += matched_count
                    for unresolved_oem in unresolved_oems:
                        stats.add_unresolved(
                            (
                                f"{unresolved_oem}: строка заказа "
                                "не найдена/неоднозначна"
                            )
                        )
                    if matched_count:
                        parsed_text_rows = True
            if expects_text_payload and not parsed_text_rows:
                import_error_reasons.append(
                    "Текст письма не удалось разобрать по текущим правилам"
                )

            if matched_order_ids_from_rows:
                order_rows = (
                    await session.execute(
                        select(SupplierOrder)
                        .options(selectinload(SupplierOrder.items))
                        .where(
                            SupplierOrder.id.in_(matched_order_ids_from_rows)
                        )
                    )
                ).scalars().all()
                for matched_order in order_rows:
                    matched_orders[int(matched_order.id)] = matched_order
                if (
                    message_row.supplier_order_id is None
                    and len(matched_orders) == 1
                ):
                    message_row.supplier_order_id = next(
                        iter(matched_orders.keys())
                    )

            if matched_orders:
                stats.matched_orders += len(matched_orders)
                for matched_order in matched_orders.values():
                    if raw_status:
                        matched_order.response_status_raw = raw_status
                        matched_order.response_status_normalized = (
                            normalized_status or None
                        )
                        matched_order.response_status_synced_at = now_moscow()
                if mapping is not None and order is not None:
                    apply_result = apply_supplier_response_action_to_order(
                        order=order,
                        mapping=mapping,
                        raw_status=raw_status,
                        normalized_status=normalized_status or None,
                        allow_quantity_updates=not parsed_response_file
                        and not parsed_text_rows,
                    )
                    stats.updated_orders += apply_result["changed_orders"]
                    stats.updated_items += apply_result["updated_items"]

            if (
                mapping is None
                and normalized_status
                and raw_status
                and not parsed_response_file
                and not parsed_text_rows
                and allow_text_status
            ):
                await record_unmapped_external_status(
                    session,
                    source_key=EXTERNAL_STATUS_SOURCE_SUPPLIER_EMAIL,
                    provider_id=provider.id,
                    raw_status=raw_status,
                    normalized_status=normalized_status,
                    sample_payload={
                        "supplier_order_id": (
                            order.id
                            if order
                            else (
                                next(iter(matched_orders.keys()))
                                if len(matched_orders) == 1
                                else None
                            )
                        ),
                        "supplier_message_id": message_row.id,
                        "sender_email": sender_email,
                        "subject": subject,
                    },
                )
                stats.unmapped_statuses += 1

            if matched_orders:
                if has_shipping_doc:
                    for matched_order in matched_orders.values():
                        applied_rows = receipt_applied_rows_by_order.get(
                            int(matched_order.id),
                            [],
                        )
                        receipt_items_payload = (
                            _build_receipt_items_from_applied_rows(
                                matched_order,
                                applied_rows,
                                cap_to_pending=True,
                            )
                        )
                        if not receipt_items_payload and order is not None:
                            receipt_items_payload = (
                                _build_pending_receipt_items(
                                    matched_order
                                )
                            )
                        if not receipt_items_payload:
                            continue
                        row_document_number = next(
                            (
                                str(ap.document_number).strip()
                                for ap in applied_rows
                                if ap.document_number
                            ),
                            "",
                        )
                        row_document_date = next(
                            (
                                ap.document_date
                                for ap in applied_rows
                                if ap.document_date
                            ),
                            None,
                        )
                        receipt_document_number = (
                            row_document_number
                            or _extract_shipping_document_number(
                                shipping_doc_filenames
                            )
                        )
                        receipt_comment = (
                            "Авто-проведение по документу УПД/накладной "
                            "из почты"
                        )
                        receipt_writer = (
                            _create_or_update_supplier_receipt_from_message
                        )
                        _, added_items, _ = (
                            await receipt_writer(
                                session,
                                provider_id=provider.id,
                                order=matched_order,
                                message_row=message_row,
                                items_payload=receipt_items_payload,
                                post_now=True,
                                document_number=receipt_document_number,
                                document_date=row_document_date,
                                comment=receipt_comment,
                            )
                        )
                        if added_items > 0:
                            stats.created_receipts += 1
                            stats.posted_receipts += 1
                            stats.receipt_items_added += added_items
                            logger.info(
                                (
                                    "Auto-posted supplier receipt "
                                    "from message %s: "
                                    "provider_id=%s, items=%s"
                                ),
                                message_row.id,
                                provider.id,
                                added_items,
                            )
                else:
                    for matched_order in matched_orders.values():
                        applied_rows = receipt_applied_rows_by_order.get(
                            int(matched_order.id),
                            [],
                        )
                        receipt_items_payload = (
                            _build_receipt_items_from_applied_rows(
                                matched_order,
                                applied_rows,
                                cap_to_pending=False,
                            )
                        )
                        if receipt_items_payload:
                            receipt_creator = (
                                _create_or_update_supplier_receipt_from_message
                            )
                            _, added_items, created_receipt = (
                                await receipt_creator(
                                    session,
                                    provider_id=provider.id,
                                    order=matched_order,
                                    message_row=message_row,
                                    items_payload=receipt_items_payload,
                                    post_now=False,
                                    comment=(
                                        "Авто-черновик поступления из ответа "
                                        "поставщика"
                                    ),
                                )
                            )
                            if added_items > 0:
                                if created_receipt:
                                    stats.created_receipts += 1
                                    stats.draft_receipts += 1
                                else:
                                    stats.updated_receipts += 1
                                stats.receipt_items_added += added_items
                                logger.info(
                                    (
                                        "Updated draft supplier receipt from "
                                        "message %s: provider_id=%s, "
                                        "items=%s, created=%s"
                                    ),
                                    message_row.id,
                                    provider.id,
                                    added_items,
                                    created_receipt,
                                )

            recognized_for_message = (
                stats.recognized_positions - recognized_before
            )
            if (
                (expects_file_payload and parsed_response_file)
                or (expects_text_payload and parsed_text_rows)
            ) and recognized_for_message <= 0:
                import_error_reasons.append(
                    "Ответ распознан, но позиции не сопоставлены с заказами"
                )
            import_error_details = _build_import_error_details(
                import_error_reasons
            )
            is_import_error = False
            if expects_file_payload:
                if not has_shipping_doc and not parsed_response_file:
                    is_import_error = True
                elif parsed_response_file and recognized_for_message <= 0:
                    is_import_error = True
            if expects_text_payload:
                if not parsed_text_rows:
                    is_import_error = True
                elif recognized_for_message <= 0:
                    is_import_error = True
            if (
                not is_import_error
                and import_error_details
                and not has_shipping_doc
                and not parsed_response_file
                and not parsed_text_rows
            ):
                is_import_error = True

            if is_import_error:
                message_row.message_type = "IMPORT_ERROR"
                message_row.import_error_details = (
                    import_error_details
                    or (
                        "Не удалось обработать сообщение "
                        "по текущей конфигурации"
                    )
                )
            else:
                if has_shipping_doc:
                    message_row.message_type = "SHIPPING_DOC"
                elif parsed_response_file:
                    message_row.message_type = "RESPONSE_FILE"
                elif parsed_text_rows:
                    message_row.message_type = "TEXT_RESPONSE"
                elif raw_status:
                    message_row.message_type = "STATUS"
                message_row.import_error_details = None

            await session.commit()
            stats.processed_messages += 1
            logger.info(
                (
                    "Supplier response message done: idx=%s/%s "
                    "processed=%s recognized_positions=%s "
                    "unresolved_positions=%s receipts_created=%s "
                    "receipts_updated=%s receipts_posted=%s "
                    "message_type=%s import_error=%s"
                ),
                index,
                total_messages,
                stats.processed_messages,
                stats.recognized_positions,
                stats.unresolved_positions,
                stats.created_receipts,
                stats.updated_receipts,
                stats.posted_receipts,
                message_row.message_type,
                message_row.import_error_details,
            )
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

    result = stats.as_dict()
    logger.info(
        (
            "Supplier response processing finished: "
            "provider_id=%s config_id=%s fetched=%s processed=%s "
            "recognized=%s unresolved=%s created_receipts=%s "
            "updated_receipts=%s posted_receipts=%s"
        ),
        provider_id,
        supplier_response_config_id,
        result.get("fetched_messages", 0),
        result.get("processed_messages", 0),
        result.get("recognized_positions", 0),
        result.get("unresolved_positions", 0),
        result.get("created_receipts", 0),
        result.get("updated_receipts", 0),
        result.get("posted_receipts", 0),
    )
    return result


async def list_supplier_response_import_errors(
    session: AsyncSession,
    *,
    provider_id: int,
    supplier_response_config_id: int,
    limit: int = 50,
) -> list[dict[str, object]]:
    config = await session.get(
        SupplierResponseConfig,
        supplier_response_config_id,
    )
    if not config or int(config.provider_id) != int(provider_id):
        raise LookupError("Supplier response config not found")
    safe_limit = max(1, min(int(limit or 50), 200))
    response_type_raw = getattr(config, "response_type", "file")
    response_type = str(
        getattr(response_type_raw, "value", response_type_raw) or "file"
    ).strip().lower()
    response_mode_label = (
        "Файл"
        if response_type == "file"
        else "Текст письма"
    )
    expected_senders = sorted(_normalize_sender_emails(config.sender_emails))
    expectations: list[str] = []
    expectations.append(
        (
            "Конфигурация: "
            f"#{config.id} {str(getattr(config, 'name', '') or '').strip()}"
        )
    )
    expectations.append(f"Режим ответа: {response_mode_label}")
    if expected_senders:
        expectations.append(
            f"Ожидаемый sender_email: {', '.join(expected_senders)}"
        )
    inbox_account_id = getattr(config, "inbox_email_account_id", None)
    if inbox_account_id:
        expectations.append(
            f"Ожидаемый inbox_email_account_id: {inbox_account_id}"
        )
    if response_type == "file":
        pattern = str(getattr(config, "filename_pattern", "") or "").strip()
        if pattern:
            expectations.append(
                f"Шаблон имени файла (regex): {pattern}"
            )
        file_format = str(getattr(config, "file_format", "") or "").strip()
        if file_format:
            expectations.append(f"Формат файла: {file_format}")
        payload_type_raw = getattr(config, "file_payload_type", "response")
        payload_type = str(
            getattr(payload_type_raw, "value", payload_type_raw) or "response"
        ).strip().lower()
        payload_type_label = (
            "документ (УПД/накладная)"
            if payload_type == "document"
            else "ответ по позициям"
        )
        expectations.append(f"Тип файла: {payload_type_label}")
        start_row = getattr(config, "start_row", None)
        if start_row:
            expectations.append(f"Начальная строка: {start_row}")
        allowed_ext = sorted(
            _allowed_attachment_extensions(
                getattr(config, "file_format", None)
            )
        )
        expectations.append(
            f"Допустимые расширения: {', '.join(allowed_ext)}"
        )
    else:
        mode = _normalize_value_after_article_type(
            getattr(config, "value_after_article_type", "both")
        )
        expectations.append(
            (
                "Разбор текста после артикула: "
                f"{mode}"
            )
        )
    stmt = (
        select(SupplierOrderMessage)
        .options(selectinload(SupplierOrderMessage.attachments))
        .where(
            SupplierOrderMessage.provider_id == provider_id,
            (
                SupplierOrderMessage.response_config_id
                == supplier_response_config_id
            ),
            SupplierOrderMessage.message_type == "IMPORT_ERROR",
        )
        .order_by(
            desc(SupplierOrderMessage.received_at),
            desc(SupplierOrderMessage.id),
        )
        .limit(safe_limit)
    )
    rows = (await session.execute(stmt)).scalars().all()
    parsed_source_by_message: dict[
        int,
        tuple[Optional[int], Optional[str], Optional[str]],
    ] = {}
    account_ids: set[int] = set()
    for row in rows:
        account_id, folder_name, source_uid_value = _parse_source_uid(
            row.source_uid
        )
        parsed_source_by_message[int(row.id)] = (
            account_id,
            folder_name,
            source_uid_value,
        )
        if account_id is not None:
            account_ids.add(account_id)

    account_by_id: dict[int, EmailAccount] = {}
    if account_ids:
        account_rows = (
            await session.execute(
                select(EmailAccount).where(EmailAccount.id.in_(account_ids))
            )
        ).scalars().all()
        account_by_id = {int(item.id): item for item in account_rows}

    result: list[dict[str, object]] = []
    for row in rows:
        subject_raw = row.subject
        subject = _decode_mime_text(subject_raw)
        reasons = [
            item.strip()
            for item in str(row.import_error_details or "").split(";")
            if item.strip()
        ]
        account_id, source_folder, source_message_uid = (
            parsed_source_by_message.get(int(row.id), (None, None, None))
        )
        account = (
            account_by_id.get(account_id)
            if account_id is not None
            else None
        )
        attachment_filenames: list[str] = []
        attachment_details: list[str] = []
        for att in row.attachments or []:
            raw_name = str(att.filename or "").strip()
            decoded_name = _decode_mime_text(raw_name) or raw_name
            display_name = decoded_name or "<без имени>"
            kind = str(att.parsed_kind or "").strip()
            if decoded_name:
                attachment_filenames.append(decoded_name)
            elif raw_name:
                attachment_filenames.append(raw_name)
            if kind:
                details = f"{display_name} [{kind}]"
            else:
                details = display_name
            if raw_name and decoded_name and raw_name != decoded_name:
                details = f"{details} (raw: {raw_name})"
            attachment_details.append(details)
        manager_hints = _build_import_error_hints(
            response_type=response_type,
            reasons=reasons,
            has_attachments=bool(row.attachments),
            subject=subject or "",
            subject_raw=subject_raw,
        )
        result.append(
            {
                "id": int(row.id),
                "received_at": row.received_at,
                "sender_email": row.sender_email,
                "subject": subject or subject_raw,
                "subject_raw": subject_raw,
                "body_preview": row.body_preview,
                "message_type": row.message_type,
                "import_error_details": row.import_error_details,
                "import_error_reasons": reasons,
                "config_expectations": expectations,
                "source_uid": row.source_uid,
                "source_message_id": row.source_message_id,
                "account_id": account_id,
                "account_name": (
                    str(getattr(account, "name", "") or "").strip()
                    if account is not None
                    else None
                ),
                "account_email": (
                    str(getattr(account, "email", "") or "").strip()
                    if account is not None
                    else None
                ),
                "source_folder": source_folder,
                "source_message_uid": source_message_uid,
                "attachment_filenames": attachment_filenames,
                "attachment_details": attachment_details,
                "manager_hints": manager_hints,
            }
        )
    return result


async def retry_supplier_response_import_errors_for_config(
    session: AsyncSession,
    *,
    provider_id: int,
    supplier_response_config_id: int,
) -> dict[str, object]:
    config = await session.get(
        SupplierResponseConfig,
        supplier_response_config_id,
    )
    if not config or int(config.provider_id) != int(provider_id):
        raise LookupError("Supplier response config not found")

    stmt = (
        select(SupplierOrderMessage)
        .where(
            SupplierOrderMessage.provider_id == provider_id,
            (
                SupplierOrderMessage.response_config_id
                == supplier_response_config_id
            ),
            SupplierOrderMessage.message_type == "IMPORT_ERROR",
        )
        .order_by(
            SupplierOrderMessage.received_at.asc(),
            SupplierOrderMessage.id.asc(),
        )
    )
    rows = (await session.execute(stmt)).scalars().all()
    if not rows:
        return {
            "config_id": supplier_response_config_id,
            "total": 0,
            "queued": 0,
            "unretryable": 0,
            **SupplierResponseProcessingStats().as_dict(),
        }

    retryable = 0
    unretryable = 0
    date_from_values: list[date] = []
    for row in rows:
        if row.source_uid or row.source_message_id:
            row.source_uid = None
            row.source_message_id = None
            row.message_type = "RETRY_PENDING"
            retryable += 1
            received_date = (
                row.received_at.date()
                if row.received_at is not None
                else supplier_response_cutoff()
            )
            date_from_values.append(received_date)
        else:
            unretryable += 1
    await session.commit()

    if retryable <= 0:
        return {
            "config_id": supplier_response_config_id,
            "total": len(rows),
            "queued": 0,
            "unretryable": unretryable,
            **SupplierResponseProcessingStats().as_dict(),
        }

    retry_result = await process_supplier_response_messages(
        session=session,
        provider_id=provider_id,
        supplier_response_config_id=supplier_response_config_id,
        date_from=min(date_from_values) if date_from_values else None,
        date_to=None,
    )
    return {
        "config_id": supplier_response_config_id,
        "total": len(rows),
        "queued": retryable,
        "unretryable": unretryable,
        **retry_result,
    }
