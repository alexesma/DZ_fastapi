from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import re
import zipfile
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from email.header import decode_header
from io import BytesIO
from typing import Any, Iterable, Optional

import aiofiles
import httpx
import pandas as pd
from sqlalchemy import desc
from sqlalchemy import inspect as sa_inspect
from sqlalchemy import or_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload, selectinload

from dz_fastapi.core.base import AutoPart
from dz_fastapi.core.constants import IMAP_SERVER
from dz_fastapi.core.email_folders import (DEFAULT_IMAP_FOLDER,
                                           resolve_imap_folders)
from dz_fastapi.core.time import now_moscow
from dz_fastapi.crud.autopart import crud_autopart
from dz_fastapi.crud.brand import brand_crud
from dz_fastapi.crud.email_account import crud_email_account
from dz_fastapi.crud.settings import crud_customer_order_inbox_settings
from dz_fastapi.models.email_account import EmailAccount
from dz_fastapi.models.notification import AppNotificationLevel
from dz_fastapi.models.partner import (SUPPLIER_ORDER_STATUS,
                                       CustomerOrderItem, Provider,
                                       SupplierOrder, SupplierOrderAttachment,
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
    _strip_html, try_finalize_customer_order_response)
from dz_fastapi.services.inventory_stock import (
    apply_receipt_to_stock_by_id, resolve_warehouse_for_provider)
from dz_fastapi.services.notifications import create_admin_notifications
from dz_fastapi.services.order_status_mapping import (
    EXTERNAL_STATUS_SOURCE_SUPPLIER_EMAIL,
    apply_supplier_response_action_to_order, get_active_status_mappings,
    normalize_external_status_text, record_unmapped_external_status,
    select_best_mapping)
from dz_fastapi.services.supplier_workflow import (
    _match_site_order_item_for_receipt, _refresh_receipt_links)

try:
    from imap_tools.errors import MailboxFolderSelectError
except ImportError:  # pragma: no cover - compatibility fallback
    MailboxFolderSelectError = Exception

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
_RESPONSE_ALNUM_CODE_RE = re.compile(r"[AА](\d{7,12})", re.I)
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
_ARTICLE_TOKEN_RE = re.compile(
    r"(?:"
    r"(?=.*[A-Za-z])(?=.*[0-9])[A-Za-z0-9._/-]+"
    r"|"
    r"(?=(?:[^0-9]*[0-9]){6,})[0-9]+(?:[-/][0-9]+)+"
    r")"
)
_TEXT_TOKEN_RE = re.compile(r"[A-Za-zА-Яа-яЁё0-9][A-Za-zА-Яа-яЁё0-9._/-]*")
_INVALID_PARSED_OEM_KEYS = {"NAN", "NONE", "NULL", "NAT"}
# Detects the start of a quoted/forwarded reply block in email text.
# Matches a line of 5+ dashes (common separator) or a standalone
# "From:"/"Кому:" header line that indicates a forwarded message.
_QUOTED_REPLY_SEPARATOR_RE = re.compile(
    r"(?:^|\n)"                          # start of line
    r"[ \t]*"                             # optional leading spaces
    r"(?:"
    r"-{5,}"                              # -----  long dash divider
    r"|_{5,}"                             # _____  underscore divider
    r"|(?:From|Кому|Отправитель)\s*:"     # forwarded-message header keywords
    r")"
    r"[ \t]*(?:\r?\n|$)",                 # rest of line must be blank/end
    re.IGNORECASE,
)
_QUOTED_REPLY_INLINE_DASH_RE = re.compile(
    r"\s[-_]{8,}\s",
    re.IGNORECASE,
)
_CELL_REF_RE = re.compile(r'^\s*([A-Za-z]+)\s*([0-9]+)\s*$')
_RC_CELL_REF_RE = re.compile(r'^\s*R\s*([0-9]+)\s*C\s*([0-9]+)\s*$', re.I)
_DIGIT_PAIR_CELL_REF_RE = re.compile(r'^\s*([0-9]+)\s*[,;:xX]\s*([0-9]+)\s*$')
_DOCUMENT_NUMBER_RE = re.compile(
    r'(?:№|N)\s*([A-Za-zА-Яа-я0-9][A-Za-zА-Яа-я0-9._/-]*)'
)
_DOCUMENT_NUMBER_FALLBACK_RE = re.compile(
    r'(?:УПД|UPD|накладн\w*|invoice)\s*'
    r'([A-Za-zА-Яа-я0-9][A-Za-zА-Яа-я0-9._/-]*)',
    re.I,
)
_RUS_MONTHS = {
    "январ": 1,
    "феврал": 2,
    "март": 3,
    "апрел": 4,
    "ма": 5,
    "июн": 6,
    "июл": 7,
    "август": 8,
    "сентябр": 9,
    "октябр": 10,
    "ноябр": 11,
    "декабр": 12,
}
_DOCUMENT_TEXT_DATE_RE = re.compile(
    r'([0-3]?\d)\s+'
    r'(январ[ьяе]?|феврал[ьяе]?|март[ае]?|апрел[ьяе]?|ма[йяе]|'
    r'июн[ьяе]?|июл[ьяе]?|август[ае]?|сентябр[ьяе]?|'
    r'октябр[ьяе]?|ноябр[ьяе]?|декабр[ьяе]?)'
    r'\s+(\d{2,4})',
    re.I,
)
_DEFAULT_CONFIRM_KEYWORDS = [
    "в наличии",
    "есть",
    "в резерве",
    "зарезервировано",
    "отгружаем",
    "собрали",
    "готово",
    "отказов нет",
    "да",
]
_DEFAULT_REJECT_KEYWORDS = [
    "нет",
    "0",
    "отказ",
    "отсутствует",
    "не можем",
    "снято с производства",
]
_FUTURE_CONFIRM_WORDS = {
    "будет",
    "будут",
    "будем",
}
_MAX_INT32 = 2_147_483_647
_AUTO_CONFIRM_MISSING_COMMENT = (
    "Автоподтверждено: позиция отсутствует в ответе "
    "(режим исключений)"
)
_AUTO_CONFIRM_MISSING_STATUS = "автоподтверждено"
_AUTO_CONFIRM_TIMEOUT_COMMENT_TEMPLATE = (
    "Автоподтверждено: нет ответа поставщика более {minutes} мин"
)
_AUTO_CONFIRM_TIMEOUT_STATUS_TEMPLATE = (
    "автоподтверждено по таймауту ответа {minutes} мин"
)
_AUTO_REJECT_MISSING_COMMENT = (
    "Авто-отказ: позиция отсутствует в документе поставщика"
)
_AUTO_REJECT_MISSING_STATUS = "автоотказ по документу"
_AUTO_CONFIRM_TIMEOUT_RECEIPT_COMMENT = (
    "Авто-черновик поступления: автоподтверждение по таймауту ответа "
    "поставщика"
)
_AUTO_CONFIRM_TIMEOUT_BLOCKING_MESSAGE_TYPES = frozenset(
    {
        "RESPONSE_FILE",
        "TEXT_RESPONSE",
        "SHIPPING_DOC",
        "STATUS",
    }
)
_SUPPLIER_RESPONSE_AI_ALLOWED_TYPES = {
    "UNKNOWN",
    "IMPORT_ERROR",
    "RESPONSE_FILE",
    "TEXT_RESPONSE",
    "SHIPPING_DOC",
    "STATUS",
    "IGNORED",
    "RETRY_PENDING",
}
_DRAGONZAP_BRAND_KEY = "DRAGONZAP"


def _env_bool(name: str, default: bool = False) -> bool:
    raw = str(os.getenv(name, "")).strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "y", "on"}


def _env_int(name: str, default: int) -> int:
    raw = str(os.getenv(name, "")).strip()
    if not raw:
        return default
    try:
        return int(raw)
    except (TypeError, ValueError):
        return default


def _env_float(name: str, default: float) -> float:
    raw = str(os.getenv(name, "")).strip()
    if not raw:
        return default
    try:
        return float(raw)
    except (TypeError, ValueError):
        return default


_SUPPLIER_RESPONSE_AI_CLASSIFIER_ENABLED = _env_bool(
    "SUPPLIER_RESPONSE_AI_CLASSIFIER_ENABLED",
    False,
)
_SUPPLIER_RESPONSE_AI_CLASSIFIER_MODEL = str(
    os.getenv("SUPPLIER_RESPONSE_AI_CLASSIFIER_MODEL", "gpt-4o-mini")
).strip() or "gpt-4o-mini"
_SUPPLIER_RESPONSE_AI_CLASSIFIER_BASE_URL = str(
    os.getenv("SUPPLIER_RESPONSE_AI_CLASSIFIER_BASE_URL", "")
).strip() or "https://api.openai.com/v1"
_SUPPLIER_RESPONSE_AI_CLASSIFIER_API_KEY = str(
    os.getenv("OPENAI_API_KEY", "")
).strip()
_SUPPLIER_RESPONSE_AI_CLASSIFIER_TIMEOUT_SEC = max(
    3.0,
    _env_float("SUPPLIER_RESPONSE_AI_CLASSIFIER_TIMEOUT_SEC", 10.0),
)
_SUPPLIER_RESPONSE_AI_CLASSIFIER_MAX_PER_REQUEST = max(
    0,
    _env_int("SUPPLIER_RESPONSE_AI_CLASSIFIER_MAX_PER_REQUEST", 20),
)
_SUPPLIER_RESPONSE_FETCH_TIMEOUT_SEC = max(
    15.0,
    _env_float("SUPPLIER_RESPONSE_FETCH_TIMEOUT_SEC", 180.0),
)
_SUPPLIER_RESPONSE_FETCH_MAX_FROM_FILTERS = max(
    1,
    _env_int("SUPPLIER_RESPONSE_FETCH_MAX_FROM_FILTERS", 30),
)
_SUPPLIER_RESPONSE_IGNORE_INTERNAL_SENDERS = _env_bool(
    "SUPPLIER_RESPONSE_IGNORE_INTERNAL_SENDERS",
    True,
)


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
    source_name: Optional[str] = None


@dataclass(slots=True)
class ParsedSupplierTextResponse:
    rows: list[ParsedSupplierResponseRow] = field(default_factory=list)
    unresolved: list[str] = field(default_factory=list)
    parsed_positions: int = 0


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
    timeout_auto_confirmed_orders: int = 0

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
            "timeout_auto_confirmed_orders": (
                self.timeout_auto_confirmed_orders
            ),
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
    from_email_filters: Optional[Iterable[str]] = None,
    use_server_side_from_filters: bool = True,
) -> list[tuple[object, Optional[EmailAccount]]]:
    accounts: list[EmailAccount] = []
    account_map: dict[int, EmailAccount] = {}
    logger.info(
        (
            "Supplier response inbox fetch init: date_from=%s date_to=%s "
            "include_default_orders_out=%s explicit_account_ids=%s "
            "from_filters=%s use_server_side_from_filters=%s"
        ),
        date_from,
        date_to,
        include_default_orders_out,
        sorted(account_ids or []),
        list(from_email_filters or []),
        use_server_side_from_filters,
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
    normalized_from_filters: list[str] = []
    seen_filters: set[str] = set()
    for raw_filter in from_email_filters or []:
        normalized = str(raw_filter or "").strip().lower()
        if not normalized or normalized in seen_filters:
            continue
        seen_filters.add(normalized)
        normalized_from_filters.append(normalized)
    if (
        len(normalized_from_filters)
        > _SUPPLIER_RESPONSE_FETCH_MAX_FROM_FILTERS
    ):
        logger.warning(
            (
                "Supplier response sender filters truncated: "
                "requested=%s max=%s"
            ),
            len(normalized_from_filters),
            _SUPPLIER_RESPONSE_FETCH_MAX_FROM_FILTERS,
        )
        normalized_from_filters = []

    sender_filter_set = set(normalized_from_filters)
    internal_sender_set: set[str] = set()
    if _SUPPLIER_RESPONSE_IGNORE_INTERNAL_SENDERS and session is not None:
        try:
            internal_accounts = (
                await session.execute(
                    select(EmailAccount).where(
                        EmailAccount.is_active.is_(True)
                    )
                )
            ).scalars().all()
            for account in internal_accounts:
                sender_email = str(getattr(account, "email", "") or "")
                sender_email = sender_email.strip().lower()
                if sender_email:
                    internal_sender_set.add(sender_email)
        except Exception as internal_sender_exc:
            logger.warning(
                "Failed to load internal sender ignore list: %s",
                internal_sender_exc,
            )
    if internal_sender_set:
        logger.info(
            "Supplier response internal sender ignore list loaded: count=%s",
            len(internal_sender_set),
        )

    def _filter_messages_by_sender(
        raw_messages: Iterable[object],
    ) -> list[object]:
        if not sender_filter_set and not internal_sender_set:
            return list(raw_messages)
        filtered_messages: list[object] = []
        for raw_message in raw_messages:
            sender = _extract_email(getattr(raw_message, "from_", None))
            if sender in internal_sender_set:
                continue
            if sender in sender_filter_set:
                filtered_messages.append(raw_message)
            elif not sender_filter_set:
                filtered_messages.append(raw_message)
        return filtered_messages

    fetch_from_filters: list[Optional[str]] = (
        normalized_from_filters
        if use_server_side_from_filters and normalized_from_filters
        else [None]
    )
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
                    "email=%s transport=%s folders=%s from_filters=%s"
                ),
                account.id,
                account.email,
                transport,
                folders,
                normalized_from_filters or None,
            )
            if transport == "resend_api":
                try:
                    account_messages = await asyncio.wait_for(
                        _fetch_resend_messages(
                            account,
                            date_from,
                        ),
                        timeout=_SUPPLIER_RESPONSE_FETCH_TIMEOUT_SEC,
                    )
                    filtered_messages = _filter_messages_by_sender(
                        account_messages
                    )
                    logger.info(
                        (
                            "Supplier response inbox account done: "
                            "account_id=%s email=%s fetched=%s"
                        ),
                        account.id,
                        account.email,
                        len(filtered_messages),
                    )
                    messages.extend(
                        (msg, account) for msg in filtered_messages
                    )
                except asyncio.TimeoutError:
                    logger.warning(
                        (
                            "Supplier response Resend fetch timeout "
                            "for %s after %.0fs"
                        ),
                        account.email,
                        _SUPPLIER_RESPONSE_FETCH_TIMEOUT_SEC,
                    )
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
                        try:
                            label_messages = await asyncio.wait_for(
                                _fetch_gmail_messages(
                                    account,
                                    date_from,
                                    label=label,
                                ),
                                timeout=_SUPPLIER_RESPONSE_FETCH_TIMEOUT_SEC,
                            )
                            account_messages.extend(label_messages)
                        except asyncio.TimeoutError:
                            logger.warning(
                                (
                                    "Supplier response Gmail fetch timeout: "
                                    "account_id=%s email=%s label=%s "
                                    "timeout=%.0fs"
                                ),
                                account.id,
                                account.email,
                                label,
                                _SUPPLIER_RESPONSE_FETCH_TIMEOUT_SEC,
                            )
                    filtered_messages = _filter_messages_by_sender(
                        account_messages
                    )
                    logger.info(
                        (
                            "Supplier response inbox account done: "
                            "account_id=%s email=%s fetched=%s"
                        ),
                        account.id,
                        account.email,
                        len(filtered_messages),
                    )
                    messages.extend(
                        (msg, account) for msg in filtered_messages
                    )
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
                    for from_filter in fetch_from_filters:
                        logger.info(
                            (
                                "Supplier response IMAP fetch: "
                                "account_id=%s email=%s folder=%s "
                                "from_filter=%s"
                            ),
                            account.id,
                            account.email,
                            folder,
                            from_filter,
                        )
                        try:
                            account_messages.extend(
                                await asyncio.wait_for(
                                    _fetch_order_messages(
                                        host,
                                        account.email,
                                        account.password,
                                        folder,
                                        date_from,
                                        False,
                                        port=account.imap_port or IMAP_SERVER,
                                        ssl=True,
                                        from_email=from_filter,
                                    ),
                                    timeout=(
                                        _SUPPLIER_RESPONSE_FETCH_TIMEOUT_SEC
                                    ),
                                )
                            )
                        except MailboxFolderSelectError as folder_exc:
                            logger.warning(
                                (
                                    'Supplier response IMAP folder "%s" '
                                    "not found for %s, skipping folder. "
                                    "Error: %s"
                                ),
                                folder,
                                account.email,
                                folder_exc,
                            )
                            break
                        except asyncio.TimeoutError:
                            logger.warning(
                                (
                                    "Supplier response IMAP fetch timeout: "
                                    "account_id=%s email=%s folder=%s "
                                    "from_filter=%s timeout=%.0fs"
                                ),
                                account.id,
                                account.email,
                                folder,
                                from_filter,
                                _SUPPLIER_RESPONSE_FETCH_TIMEOUT_SEC,
                            )
                logger.info(
                    (
                        "Supplier response inbox account done: "
                        "account_id=%s email=%s fetched=%s"
                    ),
                    account.id,
                    account.email,
                    len(_filter_messages_by_sender(account_messages)),
                )
                filtered_messages = _filter_messages_by_sender(
                    account_messages
                )
                messages.extend(
                    (msg, account) for msg in filtered_messages
                )
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
            fallback_messages = []
            for from_filter in fetch_from_filters:
                try:
                    fallback_messages.extend(
                        await asyncio.wait_for(
                            _fetch_order_messages(
                                EMAIL_HOST_ORDER,
                                EMAIL_NAME_ORDER,
                                EMAIL_PASSWORD_ORDER,
                                EMAIL_FOLDER_ORDER,
                                date_from,
                                False,
                                port=IMAP_SERVER,
                                ssl=True,
                                from_email=from_filter,
                            ),
                            timeout=_SUPPLIER_RESPONSE_FETCH_TIMEOUT_SEC,
                        )
                    )
                except asyncio.TimeoutError:
                    logger.warning(
                        (
                            "Supplier response fallback inbox fetch timeout "
                            "for %s after %.0fs"
                        ),
                        EMAIL_NAME_ORDER,
                        _SUPPLIER_RESPONSE_FETCH_TIMEOUT_SEC,
                    )
            logger.info(
                "Supplier response fallback inbox done: fetched=%s",
                len(_filter_messages_by_sender(fallback_messages)),
            )
            filtered_fallback = _filter_messages_by_sender(
                fallback_messages
            )
            messages = [(msg, None) for msg in filtered_fallback]
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
        for pattern in (
            _RESPONSE_FILENAME_RE,
            _RESPONSE_SUBJECT_RE,
            _RESPONSE_ALNUM_CODE_RE,
        ):
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


def _compile_subject_pattern(
    pattern_value: Optional[str],
) -> Optional[re.Pattern]:
    pattern = str(pattern_value or "").strip()
    if not pattern:
        return None
    try:
        return re.compile(pattern, re.I)
    except re.error as exc:
        logger.warning(
            "Invalid supplier response subject pattern %r: %s",
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


def _column_label_to_index(label: str) -> Optional[int]:
    clean = str(label or "").strip().upper()
    if not clean:
        return None
    index = 0
    for char in clean:
        if not ("A" <= char <= "Z"):
            return None
        index = index * 26 + (ord(char) - ord("A") + 1)
    return index - 1 if index > 0 else None


def _resolve_cell_coordinates(reference: object) -> Optional[tuple[int, int]]:
    if reference in (None, ""):
        return None
    text = str(reference).strip()
    if not text:
        return None
    match = _CELL_REF_RE.fullmatch(text)
    if match is not None:
        col_index = _column_label_to_index(match.group(1))
        row_index = _parse_positive_int(match.group(2))
        if col_index is None or row_index is None:
            return None
        return row_index - 1, col_index
    match = _RC_CELL_REF_RE.fullmatch(text)
    if match is not None:
        row_index = _parse_positive_int(match.group(1))
        col_index = _parse_positive_int(match.group(2))
        if row_index is None or col_index is None:
            return None
        return row_index - 1, col_index - 1
    match = _DIGIT_PAIR_CELL_REF_RE.fullmatch(text)
    if match is not None:
        row_index = _parse_positive_int(match.group(1))
        col_index = _parse_positive_int(match.group(2))
        if row_index is None or col_index is None:
            return None
        return row_index - 1, col_index - 1
    return None


def _read_cell_value(df: pd.DataFrame, reference: object) -> object:
    coordinates = _resolve_cell_coordinates(reference)
    if coordinates is None:
        return None
    row_index, col_index = coordinates
    if row_index < 0 or col_index < 0:
        return None
    if row_index >= len(df.index) or col_index >= len(df.columns):
        return None
    return df.iat[row_index, col_index]


def _clean_text_value(value: object) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    text = _repair_cp1251_mojibake(value)
    text = str(text or "").strip()
    return text or None


def _parse_document_number_from_text(
    value: object,
    *,
    custom_regex: Optional[str] = None,
) -> Optional[str]:
    text = _clean_text_value(value)
    if not text:
        return None
    if custom_regex:
        try:
            match = re.search(custom_regex, text, flags=re.I)
        except re.error:
            match = None
        if match is not None:
            extracted = (
                match.group(1)
                if match.lastindex and match.lastindex >= 1
                else match.group(0)
            )
            cleaned = _clean_text_value(extracted)
            if cleaned:
                return cleaned[:120]
    if re.fullmatch(
        r"[A-Za-zА-Яа-я0-9][A-Za-zА-Яа-я0-9._/-]*",
        text,
    ):
        return text[:120]
    direct = _DOCUMENT_NUMBER_RE.search(text)
    if direct is not None:
        cleaned = _clean_text_value(direct.group(1))
        if cleaned:
            return cleaned[:120]
    fallback = _DOCUMENT_NUMBER_FALLBACK_RE.search(text)
    if fallback is not None:
        cleaned = _clean_text_value(fallback.group(1))
        if cleaned:
            return cleaned[:120]
    return None


def _parse_human_date_from_text(value: object) -> Optional[date]:
    text = _clean_text_value(value)
    if not text:
        return None
    direct = _parse_excel_like_date(text)
    if direct is not None:
        return direct
    match = _DOCUMENT_TEXT_DATE_RE.search(text)
    if match is None:
        return None
    day = _safe_int(match.group(1))
    raw_month = str(match.group(2) or "").strip().lower()
    year = _safe_int(match.group(3))
    if day is None or year is None:
        return None
    if year < 100:
        year += 2000
    month = None
    for prefix, number in _RUS_MONTHS.items():
        if raw_month.startswith(prefix):
            month = number
            break
    if month is None:
        return None
    try:
        return date(year, month, day)
    except ValueError:
        return None


def _extract_brand_from_name_text(
    *,
    text: Optional[str],
    oem_value: Optional[str],
    custom_regex: Optional[str] = None,
) -> Optional[str]:
    source = _clean_text_value(text)
    if not source:
        return None
    if custom_regex:
        try:
            match = re.search(custom_regex, source, flags=re.I)
        except re.error:
            match = None
        if match is not None:
            extracted = (
                match.group(1)
                if match.lastindex and match.lastindex >= 1
                else match.group(0)
            )
            normalized = _clean_text_value(extracted)
            if normalized:
                return normalized
    skip_oem = _normalize_oem_key(oem_value)
    for token in re.findall(r"[A-Za-zА-Яа-яЁё0-9._/-]+", source):
        cleaned = _clean_text_value(token)
        if not cleaned:
            continue
        if _normalize_oem_key(cleaned) == skip_oem:
            continue
        if not re.search(r"[A-Za-zА-Яа-яЁё]", cleaned):
            continue
        return cleaned
    return None


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


_VAT_RATE = 0.22  # Standard VAT rate used across the system


def _resolve_price_without_vat(
    total_price_with_vat: Optional[float],
    quantity: Optional[int],
) -> Optional[float]:
    """Return per-unit price WITH VAT given a total WITH VAT and quantity.

    All prices in the system are stored WITH VAT included, so no division
    by (1 + VAT_RATE) is applied — the stored price is simply total / qty.
    """
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


def _safe_email_account_id(account: Optional[EmailAccount]) -> Optional[int]:
    if account is None:
        return None
    raw_id = getattr(account, "__dict__", {}).get("id")
    if raw_id not in (None, ""):
        try:
            return int(raw_id)
        except (TypeError, ValueError):
            return None
    try:
        identity = sa_inspect(account).identity
    except Exception:
        identity = None
    if identity and identity[0] not in (None, ""):
        try:
            return int(identity[0])
        except (TypeError, ValueError):
            return None
    return None


def _config_mismatch_reasons(
    config: SupplierResponseConfig,
    *,
    sender_email: str,
    account: Optional[EmailAccount],
    subject: Optional[str] = None,
) -> list[str]:
    reasons: list[str] = []
    account_id = _safe_email_account_id(account)
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
    subject_pattern = _compile_subject_pattern(
        getattr(config, "subject_pattern", None)
    )
    if subject_pattern is not None:
        subject_text = str(subject or "").strip()
        if not subject_pattern.search(subject_text):
            reasons.append(
                (
                    "subject mismatch "
                    f"(pattern={subject_pattern.pattern}, "
                    f"got={subject_text[:120] or '<empty>'})"
                )
            )
    return reasons


def _config_matches_message(
    config: SupplierResponseConfig,
    *,
    sender_email: str,
    account: Optional[EmailAccount],
    subject: Optional[str] = None,
) -> bool:
    return not _config_mismatch_reasons(
        config,
        sender_email=sender_email,
        account=account,
        subject=subject,
    )


def _select_best_supplier_response_config(
    configs: Iterable[SupplierResponseConfig],
    *,
    sender_email: str,
    account: Optional[EmailAccount],
    subject: Optional[str] = None,
) -> Optional[SupplierResponseConfig]:
    matched: list[tuple[tuple[int, int, int], SupplierResponseConfig]] = []
    for config in configs:
        if not bool(getattr(config, "is_active", True)):
            continue
        if not _config_matches_message(
            config,
            sender_email=sender_email,
            account=account,
            subject=subject,
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


def _keyword_matches_text_token(
    token: str,
    keywords: set[str],
) -> bool:
    normalized = normalize_external_status_text(token)
    if not normalized:
        return False
    for keyword in keywords:
        marker = str(keyword or "").strip()
        if not marker:
            continue
        if len(marker) == 1:
            if normalized == marker:
                return True
            continue
        if normalized == marker or marker in normalized:
            return True
    return False


def _parse_text_value_after_article_window(
    tokens: list[str],
    *,
    start_index: int,
    value_mode: str,
    confirm_keywords: set[str],
    reject_keywords: set[str],
) -> tuple[Optional[str], Optional[int], Optional[str]]:
    max_end = min(len(tokens), start_index + 12)
    if start_index >= max_end:
        return None, None, None
    window: list[str] = []
    for token_index in range(start_index, max_end):
        token = tokens[token_index]
        if (
            token_index > start_index
            and _ARTICLE_TOKEN_RE.fullmatch(token or "")
        ):
            break
        window.append(token)
    if not window:
        return None, None, None

    if value_mode in {"text", "both"}:
        for token in window:
            if _keyword_matches_text_token(token, reject_keywords):
                return "reject", 0, token

    for idx, token in enumerate(window):
        normalized = normalize_external_status_text(token)
        if normalized in _FUTURE_CONFIRM_WORDS and idx + 1 < len(window):
            qty_candidate = window[idx + 1]
            qty_value = _safe_float(qty_candidate)
            if qty_value is None:
                continue
            parsed_qty = _safe_int(qty_value)
            if parsed_qty is None:
                parsed_qty = int(qty_value)
            if parsed_qty <= 0:
                return "reject", 0, f"{token} {qty_candidate}"
            return "confirm", parsed_qty, f"{token} {qty_candidate}"

    if value_mode in {"text", "both"}:
        for token in window:
            if _keyword_matches_text_token(token, confirm_keywords):
                return "confirm", None, token

    return None, None, None


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

    rows_by_oem: dict[str, int] = {}
    for index, token in enumerate(tokens):
        if not _ARTICLE_TOKEN_RE.fullmatch(token or ""):
            continue
        if index + 1 >= len(tokens):
            result.unresolved.append(
                f"{token}: после артикула нет значения статуса"
            )
            continue
        status_token = tokens[index + 1]
        immediate_decision, immediate_qty = _parse_text_value_after_article(
            status_token,
            value_mode=value_mode,
            confirm_keywords=confirm_set,
            reject_keywords=reject_set,
        )
        window_decision, window_qty, window_token = (
            _parse_text_value_after_article_window(
                tokens,
                start_index=index + 1,
                value_mode=value_mode,
                confirm_keywords=confirm_set,
                reject_keywords=reject_set,
            )
        )
        decision = immediate_decision
        qty = immediate_qty
        if window_decision is not None:
            normalized_window = normalize_external_status_text(
                window_token or ""
            )
            immediate_token_is_numeric = _safe_float(status_token) is not None
            if (
                window_decision == "reject"
                or normalized_window in _FUTURE_CONFIRM_WORDS
                or immediate_decision is None
                or (window_qty is not None and immediate_token_is_numeric)
            ):
                decision = window_decision
                qty = window_qty
                status_token = window_token or status_token
        if decision is None:
            result.unresolved.append(
                f"{token}: не удалось интерпретировать "
                f"значение '{status_token}'"
            )
            continue

        parsed_row = ParsedSupplierResponseRow(
            oem_number=token,
            brand_name=None,
            confirmed_quantity=qty,
            response_price=None,
            response_comment=None,
            response_status_raw=status_token,
            text_decision=decision,
        )
        result.parsed_positions += 1
        oem_key = _normalize_oem_key(token)
        existing_index = rows_by_oem.get(oem_key)
        if existing_index is None:
            rows_by_oem[oem_key] = len(result.rows)
            result.rows.append(parsed_row)
            continue

        existing_row = result.rows[existing_index]
        # Keep explicit reject if the same article appears in quoted text.
        if existing_row.text_decision == "reject":
            continue
        if parsed_row.text_decision == "reject":
            result.rows[existing_index] = parsed_row
            continue

        existing_is_numeric = (
            _safe_float(existing_row.response_status_raw) is not None
        )
        parsed_is_numeric = (
            _safe_float(parsed_row.response_status_raw) is not None
        )
        # Prefer textual status over numeric token for duplicate article.
        if existing_is_numeric and not parsed_is_numeric:
            result.rows[existing_index] = parsed_row
    return result


def _detect_global_text_decision(
    text: str,
    *,
    confirm_keywords: object,
    reject_keywords: object,
) -> tuple[Optional[str], Optional[str]]:
    normalized_text = normalize_external_status_text(text)
    if not normalized_text:
        return None, None
    reject_set = _normalize_keywords(
        reject_keywords,
        _DEFAULT_REJECT_KEYWORDS,
    )
    for keyword in sorted(reject_set, key=len, reverse=True):
        marker = str(keyword or "").strip()
        if not marker:
            continue
        if marker in normalized_text:
            return "reject", marker
    confirm_set = _normalize_keywords(
        confirm_keywords,
        _DEFAULT_CONFIRM_KEYWORDS,
    )
    for keyword in sorted(confirm_set, key=len, reverse=True):
        marker = str(keyword or "").strip()
        if not marker:
            continue
        if marker in normalized_text:
            return "confirm", marker
    return None, None


def _apply_global_text_decision_to_order(
    order: SupplierOrder,
    *,
    decision: str,
    status_label: Optional[str],
) -> tuple[int, list[AppliedSupplierResponseRow]]:
    normalized_decision = str(decision or "").strip().lower()
    if normalized_decision not in {"confirm", "reject"}:
        return 0, []
    status_raw = (
        str(status_label or "").strip()
        or ("подтверждено" if normalized_decision == "confirm" else "отказ")
    )
    status_normalized = normalize_external_status_text(status_raw) or None
    updated = 0
    applied_rows: list[AppliedSupplierResponseRow] = []
    for order_item in order.items or []:
        target_qty = (
            max(int(order_item.quantity or 0), 0)
            if normalized_decision == "confirm"
            else 0
        )
        if order_item.confirmed_quantity != target_qty:
            order_item.confirmed_quantity = target_qty
            updated += 1
        if order_item.response_status_raw != status_raw:
            order_item.response_status_raw = status_raw
            updated += 1
        if order_item.response_status_normalized != status_normalized:
            order_item.response_status_normalized = status_normalized
            updated += 1
        order_item.response_status_synced_at = now_moscow()
        applied_rows.append(
            AppliedSupplierResponseRow(
                supplier_order_item_id=int(order_item.id),
                supplier_order_id=int(order.id),
                received_quantity=target_qty,
                comment=order_item.response_comment,
                # Use the order item's own price (= supplier price-list price).
                # Text-type responses don't carry price information.
                response_price=(
                    float(order_item.price)
                    if order_item.price is not None
                    else None
                ),
                document_number=None,
                document_date=None,
                gtd_code=None,
                country_code=None,
                country_name=None,
                total_price_with_vat=None,
            )
        )
    return updated, applied_rows


def _build_global_decision_rows_from_text(
    text: str,
    *,
    global_decision: str,
    global_decision_token: Optional[str],
) -> list["ParsedSupplierResponseRow"]:
    """Extract all OEM article tokens from *text* and produce synthetic
    ParsedSupplierResponseRow objects for a global confirm/reject.

    Used when the email has no explicit order-ID but contains OEM numbers
    (e.g. in a quoted original order).  ``confirmed_quantity`` is intentionally
    left ``None`` so that ``_apply_row_to_item`` falls back to the order
    item's own ``quantity`` field via ``text_decision``.
    """
    tokens = _TEXT_TOKEN_RE.findall(text or "")
    seen: set[str] = set()
    rows: list[ParsedSupplierResponseRow] = []
    status_label = global_decision_token or global_decision
    for token in tokens:
        if not _ARTICLE_TOKEN_RE.fullmatch(token):
            continue
        norm = _normalize_oem_key(token)
        if not norm or norm in seen:
            continue
        seen.add(norm)
        rows.append(
            ParsedSupplierResponseRow(
                oem_number=token,
                brand_name=None,
                confirmed_quantity=None,
                response_price=None,
                response_comment=None,
                response_status_raw=status_label,
                text_decision=global_decision,
            )
        )
    return rows


def _allowed_attachment_extensions(file_format: object) -> set[str]:
    raw = getattr(file_format, "value", file_format)
    normalized = str(raw or "").strip().lower()
    if normalized == "csv":
        return {"csv"}
    if normalized == "excel":
        return {"xlsx", "xls"}
    return {"xlsx", "xls", "csv"}


_SUBJECT_PREFIX_RE = re.compile(
    r"^(?:re|fwd?|ответ|пересылка|fw)\s*:\s*",
    re.IGNORECASE,
)


def _strip_email_subject_prefix(subject: Optional[str]) -> str:
    """Strip Re:/Fwd:/Ответ: prefixes and return the meaningful part."""
    text = str(subject or "").strip()
    # Remove all leading Re:/Fwd: etc. prefixes
    while True:
        cleaned = _SUBJECT_PREFIX_RE.sub("", text).strip()
        if cleaned == text:
            break
        text = cleaned
    return text


def _strip_quoted_reply_content(text: str) -> str:
    """Remove the quoted/forwarded reply block from an email body.

    Suppliers often reply with a single word like "ЕСТЬ" and quote the
    original order below a ``-----`` separator.  Parsing should only
    see the supplier's own text, not the mirrored order content.
    """
    if not text:
        return text
    match = _QUOTED_REPLY_SEPARATOR_RE.search(text)
    if match:
        return text[: match.start()].strip()
    inline_match = _QUOTED_REPLY_INLINE_DASH_RE.search(text)
    if inline_match:
        return text[: inline_match.start()].strip()
    return text


def _get_message_text_content(msg: object) -> str:
    text = str(getattr(msg, "text", "") or "").strip()
    if text:
        return text
    html = str(getattr(msg, "html", "") or "").strip()
    if html:
        return _strip_html(html)
    return ""


def _ensure_xlsx_shared_strings(payload: bytes) -> bytes:
    """Исправляет два типа проблем с xl/sharedStrings.xml в XLSX-файлах,
    генерируемых 1С и аналогичными системами:

    1. Файл называется «xl/SharedStrings.xml» (с заглавной буквой S).
       openpyxl на Linux (case-sensitive FS) ищет строчный вариант —
       получает KeyError.  Решение: пересобираем ZIP, переименовывая
       файл в «xl/sharedStrings.xml».

    2. Файл отсутствует совсем, хотя ячейки ссылаются на него (t="s").
       Решение: сканируем листы, находим максимальный индекс и создаём
       sharedStrings с достаточным числом пустых записей-заглушек.
       Строковые значения ячеек будут пустыми, числовые — читаются корректно.
    """
    import re as _re
    _CANONICAL = 'xl/sharedStrings.xml'

    try:
        with zipfile.ZipFile(BytesIO(payload), 'r') as zin:
            names = zin.namelist()

            if _CANONICAL in names:
                return payload  # файл уже корректный

            # Ищем файл с другим регистром (например xl/SharedStrings.xml)
            wrong_case = next(
                (n for n in names if n.lower() == _CANONICAL.lower()),
                None,
            )

            buf = BytesIO()
            with zipfile.ZipFile(buf, 'w', zipfile.ZIP_DEFLATED) as zout:
                for name in names:
                    data = zin.read(name)
                    if name == wrong_case:
                        # Записываем под правильным именем вместо исходного
                        zout.writestr(_CANONICAL, data)
                        logger.debug(
                            'xlsx: renamed %s → %s (case fix)',
                            wrong_case, _CANONICAL,
                        )
                    else:
                        zout.writestr(name, data)

                if wrong_case is None:
                    # Файл вообще отсутствует — создаём заглушки
                    max_idx = -1
                    ws_re = _re.compile(rb't="s"[^>]*><v>(\d+)</v>', _re.S)
                    for name in names:
                        if (
                            name.startswith('xl/worksheets/')
                            and name.endswith('.xml')
                        ):
                            try:
                                ws_data = zin.read(name)
                                for m in ws_re.finditer(ws_data):
                                    idx = int(m.group(1))
                                    if idx > max_idx:
                                        max_idx = idx
                            except Exception:
                                pass

                    count = max(max_idx + 1, 1)
                    entries = b'<si><t/></si>' * count
                    shared_strings = (
                        b'<?xml version="1.0" encoding="UTF-8"'
                        b' standalone="yes"?>'
                        b'<sst xmlns="http://schemas.openxmlformats.org/'
                        b'spreadsheetml/2006/main"'
                        b' count="' + str(count).encode() + b'"'
                        b' uniqueCount="' + str(count).encode() + b'">'
                        + entries
                        + b'</sst>'
                    )
                    zout.writestr(_CANONICAL, shared_strings)
                    logger.warning(
                        'xlsx missing sharedStrings.xml entirely; '
                        'created %d placeholder entries — string cell '
                        'values will be empty',
                        count,
                    )

            return buf.getvalue()
    except zipfile.BadZipFile:
        return payload  # не zip — вернём как есть, ошибка поднимется позже


def _parse_supplier_response_attachment(
    payload: bytes,
    filename: str,
    *,
    file_payload_type: object = "response",
    start_row: object = 1,
    oem_col: object = None,
    brand_col: object = None,
    name_col: object = None,
    brand_from_name_regex: Optional[str] = None,
    qty_col: object = None,
    price_col: object = None,
    comment_col: object = None,
    status_col: object = None,
    document_number_col: object = None,
    document_date_col: object = None,
    document_number_cell: Optional[str] = None,
    document_date_cell: Optional[str] = None,
    document_meta_cell: Optional[str] = None,
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
        xlsx_payload = (
            _ensure_xlsx_shared_strings(payload) if ext == "xlsx" else payload
        )
        df = pd.read_excel(
            BytesIO(xlsx_payload),
            header=None if has_column_layout else 0,
        )
    else:
        return []
    full_df = df.copy()
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
        name_column = _resolve_column_by_number(df, name_col)
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
        name_column = None
        for candidate in (
            "name",
            "наименование",
            "товар",
            "description",
            "номенклатура",
        ):
            if candidate in headers:
                name_column = headers[candidate]
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

    static_document_number_raw = _read_cell_value(
        full_df,
        document_number_cell,
    )
    static_document_number = (
        _parse_document_number_from_text(static_document_number_raw)
        or _clean_text_value(static_document_number_raw)
    )
    static_document_date_raw = _read_cell_value(full_df, document_date_cell)
    static_document_date = (
        _parse_human_date_from_text(static_document_date_raw)
        or _parse_excel_like_date(static_document_date_raw)
    )
    meta_cell_raw = _read_cell_value(full_df, document_meta_cell)
    meta_document_number = _parse_document_number_from_text(meta_cell_raw)
    meta_document_date = _parse_human_date_from_text(meta_cell_raw)
    if static_document_number is None:
        static_document_number = meta_document_number
    if static_document_date is None:
        static_document_date = meta_document_date

    parsed_rows: list[ParsedSupplierResponseRow] = []
    for _, row in df.iterrows():
        raw_oem_value = row.get(oem_column)
        if raw_oem_value is None or pd.isna(raw_oem_value):
            continue
        oem_value = _normalize_oem_key(raw_oem_value)
        if not oem_value or oem_value in _INVALID_PARSED_OEM_KEYS:
            continue
        brand_value = None
        if brand_column is not None:
            raw_brand = row.get(brand_column)
            if raw_brand is not None and not pd.isna(raw_brand):
                brand_value = str(raw_brand).strip() or None
        name_value = None
        if name_column is not None:
            name_value = _clean_text_value(row.get(name_column))
        if brand_value is None:
            brand_value = _extract_brand_from_name_text(
                text=name_value,
                oem_value=oem_value,
                custom_regex=brand_from_name_regex,
            )
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
            raw_document_number = row.get(document_number_column)
            document_number = (
                _parse_document_number_from_text(raw_document_number)
                or _clean_text_value(raw_document_number)
            )
        if document_number is None:
            document_number = static_document_number
        document_date = None
        if document_date_column is not None:
            raw_document_date = row.get(document_date_column)
            document_date = (
                _parse_human_date_from_text(raw_document_date)
                or _parse_excel_like_date(raw_document_date)
            )
        if document_date is None:
            document_date = static_document_date
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
                source_name=name_value,
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
    account_id = _safe_email_account_id(account) or 0
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
    response_config: Optional[SupplierResponseConfig] = None,
) -> tuple[
    int,
    int,
    list[str],
    list[AppliedSupplierResponseRow],
    list[ParsedSupplierResponseRow],
]:
    if not parsed_rows:
        return 0, 0, [], [], []
    brand_aliases = await _load_brand_alias_map(session)
    exact_map: dict[tuple[str, str], list[SupplierOrderItem]] = {}
    oem_map: dict[str, list[SupplierOrderItem]] = {}
    for item in order.items or []:
        key = _normalize_key(item.oem_number, item.brand_name, brand_aliases)
        exact_map.setdefault(key, []).append(item)
        oem_key = _normalize_oem_key(item.oem_number)
        if oem_key:
            oem_map.setdefault(oem_key, []).append(item)
    priority_brand_keys = _config_priority_brand_keys(
        response_config,
        brand_aliases,
    )

    updated = 0
    matched_count = 0
    unresolved_oems: list[str] = []
    applied_rows: list[AppliedSupplierResponseRow] = []
    unmatched_rows: list[ParsedSupplierResponseRow] = []
    for row in parsed_rows:
        matched_item = _select_row_item_candidate(
            row=row,
            exact_map=exact_map,
            oem_map=oem_map,
            brand_aliases=brand_aliases,
            priority_brand_keys=priority_brand_keys,
        )
        if matched_item is None:
            if row.brand_name is None and priority_brand_keys:
                for key in priority_brand_keys:
                    if key:
                        row.brand_name = key
                        break
            if (
                _canonical_brand_key_for_value(row.brand_name, brand_aliases)
                == _DRAGONZAP_BRAND_KEY
            ):
                row.oem_number = _dragonzap_prefixed_oem_key(
                    _normalize_oem_key(row.oem_number)
                )
            unresolved_oems.append(row.oem_number)
            unmatched_rows.append(row)
            continue
        if not row.brand_name:
            row.brand_name = matched_item.brand_name
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
    return (
        updated,
        matched_count,
        unresolved_oems,
        applied_rows,
        unmatched_rows,
    )


def _auto_confirm_unmentioned_order_items(
    order: SupplierOrder,
    *,
    applied_rows: list[AppliedSupplierResponseRow],
) -> tuple[int, list[AppliedSupplierResponseRow]]:
    touched_item_ids = {
        int(row.supplier_order_item_id)
        for row in applied_rows
    }
    updated = 0
    generated_rows: list[AppliedSupplierResponseRow] = []
    for order_item in order.items or []:
        item_id = int(order_item.id)
        if item_id in touched_item_ids:
            continue
        if order_item.confirmed_quantity is not None:
            continue
        expected_quantity = int(order_item.quantity or 0)
        if expected_quantity < 0:
            expected_quantity = 0
        if order_item.confirmed_quantity != expected_quantity:
            order_item.confirmed_quantity = expected_quantity
            updated += 1
        if not str(order_item.response_comment or "").strip():
            order_item.response_comment = _AUTO_CONFIRM_MISSING_COMMENT
            updated += 1
        raw_status = str(order_item.response_status_raw or "").strip()
        if not raw_status:
            order_item.response_status_raw = _AUTO_CONFIRM_MISSING_STATUS
            raw_status = _AUTO_CONFIRM_MISSING_STATUS
            updated += 1
        normalized_status = normalize_external_status_text(raw_status)
        if (
            order_item.response_status_normalized
            != (normalized_status or None)
        ):
            order_item.response_status_normalized = normalized_status or None
            updated += 1
        order_item.response_status_synced_at = now_moscow()
        generated_rows.append(
            AppliedSupplierResponseRow(
                supplier_order_item_id=item_id,
                supplier_order_id=int(order.id),
                received_quantity=expected_quantity,
                comment=order_item.response_comment,
                # Use the order item's own price (= supplier price-list price).
                response_price=(
                    float(order_item.price)
                    if order_item.price is not None
                    else None
                ),
                document_number=None,
                document_date=None,
                gtd_code=None,
                country_code=None,
                country_name=None,
                total_price_with_vat=None,
            )
        )
    return updated, generated_rows


def _auto_reject_unmentioned_order_items(
    order: SupplierOrder,
    *,
    applied_rows: list[AppliedSupplierResponseRow],
) -> int:
    touched_item_ids = {
        int(row.supplier_order_item_id)
        for row in applied_rows
    }
    updated = 0
    for order_item in order.items or []:
        item_id = int(order_item.id)
        if item_id in touched_item_ids:
            continue
        if order_item.confirmed_quantity != 0:
            order_item.confirmed_quantity = 0
            updated += 1
        if not str(order_item.response_comment or "").strip():
            order_item.response_comment = _AUTO_REJECT_MISSING_COMMENT
            updated += 1
        if order_item.response_status_raw != _AUTO_REJECT_MISSING_STATUS:
            order_item.response_status_raw = _AUTO_REJECT_MISSING_STATUS
            updated += 1
        normalized_status = normalize_external_status_text(
            _AUTO_REJECT_MISSING_STATUS
        )
        if order_item.response_status_normalized != (
            normalized_status or None
        ):
            order_item.response_status_normalized = normalized_status or None
            updated += 1
        order_item.response_status_synced_at = now_moscow()
    return updated


async def _auto_confirm_orders_without_response_timeout(
    session: AsyncSession,
    *,
    response_configs: list[SupplierResponseConfig],
    stats: SupplierResponseProcessingStats,
) -> None:
    provider_timeouts: dict[int, int] = {}
    for config in response_configs:
        minutes_value = _safe_int(
            getattr(config, "auto_confirm_after_minutes", None)
        )
        if minutes_value is None or minutes_value <= 0:
            continue
        provider_key = int(config.provider_id)
        prev_value = provider_timeouts.get(provider_key)
        if prev_value is None or minutes_value < prev_value:
            provider_timeouts[provider_key] = minutes_value
    if not provider_timeouts:
        return

    now_dt = now_moscow()
    has_changes = False
    for provider_key, timeout_minutes in provider_timeouts.items():
        cutoff_dt = now_dt - timedelta(minutes=timeout_minutes)
        orders_stmt = (
            select(SupplierOrder)
            .options(selectinload(SupplierOrder.items))
            .where(
                SupplierOrder.provider_id == provider_key,
                SupplierOrder.status == SUPPLIER_ORDER_STATUS.SENT,
                SupplierOrder.sent_at.is_not(None),
                SupplierOrder.sent_at <= cutoff_dt,
            )
            .order_by(SupplierOrder.sent_at.asc(), SupplierOrder.id.asc())
        )
        provider_orders = (
            await session.execute(orders_stmt)
        ).scalars().all()
        if not provider_orders:
            continue

        for order in provider_orders:
            sent_at = order.sent_at
            if sent_at is None:
                continue
            has_message_stmt = (
                select(SupplierOrderMessage.id)
                .where(
                    SupplierOrderMessage.supplier_order_id == order.id,
                    SupplierOrderMessage.received_at >= sent_at,
                    SupplierOrderMessage.message_type.in_(
                        tuple(_AUTO_CONFIRM_TIMEOUT_BLOCKING_MESSAGE_TYPES)
                    ),
                )
                .limit(1)
            )
            has_message = (
                (await session.execute(has_message_stmt)).scalar_one_or_none()
                is not None
            )
            if has_message:
                continue

            status_label = _AUTO_CONFIRM_TIMEOUT_STATUS_TEMPLATE.format(
                minutes=timeout_minutes
            )
            comment_label = _AUTO_CONFIRM_TIMEOUT_COMMENT_TEMPLATE.format(
                minutes=timeout_minutes
            )
            changed_fields = 0
            for order_item in order.items or []:
                if order_item.confirmed_quantity is not None:
                    continue
                expected_quantity = max(int(order_item.quantity or 0), 0)
                order_item.confirmed_quantity = expected_quantity
                changed_fields += 1
                if not str(order_item.response_comment or "").strip():
                    order_item.response_comment = comment_label
                    changed_fields += 1
                raw_status = str(order_item.response_status_raw or "").strip()
                if not raw_status:
                    order_item.response_status_raw = status_label
                    raw_status = status_label
                    changed_fields += 1
                normalized_status = normalize_external_status_text(raw_status)
                if (
                    order_item.response_status_normalized
                    != (normalized_status or None)
                ):
                    order_item.response_status_normalized = (
                        normalized_status or None
                    )
                    changed_fields += 1
                order_item.response_status_synced_at = now_dt
            if changed_fields <= 0:
                continue
            has_changes = True
            stats.updated_items += changed_fields
            stats.updated_orders += 1
            stats.timeout_auto_confirmed_orders += 1
            order.response_status_raw = status_label
            order.response_status_normalized = normalize_external_status_text(
                status_label
            ) or None
            order.response_status_synced_at = now_dt

            receipt_items_payload = _build_pending_receipt_items(order)
            if receipt_items_payload:
                receipt = await _find_open_supplier_receipt(
                    session,
                    provider_id=provider_key,
                )
                created_receipt = False
                if receipt is None:
                    warehouse = await resolve_warehouse_for_provider(
                        session,
                        provider_id=provider_key,
                    )
                    receipt = SupplierReceipt(
                        provider_id=provider_key,
                        warehouse_id=warehouse.id,
                        supplier_order_id=int(order.id),
                        source_message_id=None,
                        document_number=None,
                        document_date=now_dt.date(),
                        created_by_user_id=None,
                        created_at=now_dt,
                        posted_at=None,
                        comment=_AUTO_CONFIRM_TIMEOUT_RECEIPT_COMMENT,
                    )
                    session.add(receipt)
                    await session.flush()
                    created_receipt = True

                added_items = await _append_supplier_receipt_items(
                    session,
                    receipt=receipt,
                    order=order,
                    items_payload=receipt_items_payload,
                    post_now=False,
                )
                if added_items > 0:
                    stats.receipt_items_added += added_items
                    if created_receipt:
                        stats.created_receipts += 1
                        stats.draft_receipts += 1
                    else:
                        stats.updated_receipts += 1
                    logger.info(
                        (
                            "Auto-created draft supplier receipt by timeout: "
                            "provider_id=%s order_id=%s items=%s "
                            "created=%s"
                        ),
                        provider_key,
                        order.id,
                        added_items,
                        created_receipt,
                    )
                elif created_receipt:
                    await session.delete(receipt)
                    await session.flush()
            logger.info(
                (
                    "Auto-confirmed supplier order by timeout: "
                    "provider_id=%s order_id=%s minutes=%s"
                ),
                provider_key,
                order.id,
                timeout_minutes,
            )

    if has_changes:
        await session.commit()


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
        return pending_candidates[0]
    if len(candidates) == 1:
        return candidates[0]
    return candidates[0]


def _canonical_brand_key_for_value(
    brand_name: Optional[str],
    brand_aliases: dict[str, str],
) -> str:
    return _normalize_key(None, brand_name, brand_aliases)[1]


def _config_priority_brand_keys(
    response_config: Optional[SupplierResponseConfig],
    brand_aliases: dict[str, str],
) -> list[str]:
    if response_config is None:
        return []
    candidates: list[str] = []
    fixed_brand_name = str(
        getattr(response_config, "fixed_brand_name", "") or ""
    ).strip()
    if fixed_brand_name:
        candidates.append(fixed_brand_name)
    for raw in (getattr(response_config, "brand_priority_list", None) or []):
        value = str(raw or "").strip()
        if value:
            candidates.append(value)
    result: list[str] = []
    seen: set[str] = set()
    for raw in candidates:
        canonical = _canonical_brand_key_for_value(raw, brand_aliases)
        if not canonical or canonical in seen:
            continue
        seen.add(canonical)
        result.append(canonical)
    return result


def _dragonzap_prefixed_oem_key(oem_key: str) -> str:
    clean = str(oem_key or "").strip().upper()
    if not clean:
        return ""
    if clean.startswith("DZ"):
        return clean
    return f"DZ{clean}"


def _collect_row_candidate_oem_keys(
    *,
    row: ParsedSupplierResponseRow,
    priority_brand_keys: list[str],
    brand_aliases: dict[str, str],
) -> list[str]:
    keys: list[str] = []
    primary = _normalize_oem_key(row.oem_number)
    if primary:
        keys.append(primary)
    row_brand_key = _canonical_brand_key_for_value(
        row.brand_name,
        brand_aliases,
    )
    should_try_dragonzap = (
        row_brand_key == _DRAGONZAP_BRAND_KEY
        or _DRAGONZAP_BRAND_KEY in priority_brand_keys
    )
    if should_try_dragonzap and primary:
        dragonzap_key = _dragonzap_prefixed_oem_key(primary)
        if dragonzap_key and dragonzap_key not in keys:
            keys.append(dragonzap_key)
        if primary.startswith("DZ") and len(primary) > 2:
            without_prefix = primary[2:]
            if without_prefix and without_prefix not in keys:
                keys.append(without_prefix)
    return keys


def _select_row_item_candidate(
    *,
    row: ParsedSupplierResponseRow,
    exact_map: dict[tuple[str, str], list[SupplierOrderItem]],
    oem_map: dict[str, list[SupplierOrderItem]],
    brand_aliases: dict[str, str],
    priority_brand_keys: list[str],
) -> Optional[SupplierOrderItem]:
    exact_key = _normalize_key(
        row.oem_number,
        row.brand_name,
        brand_aliases,
    )
    exact_candidates = exact_map.get(exact_key) or []
    if exact_candidates:
        return exact_candidates.pop(0)

    candidates: list[SupplierOrderItem] = []
    seen_ids: set[int] = set()
    for oem_key in _collect_row_candidate_oem_keys(
        row=row,
        priority_brand_keys=priority_brand_keys,
        brand_aliases=brand_aliases,
    ):
        for item in (oem_map.get(oem_key) or []):
            item_id = int(item.id)
            if item_id in seen_ids:
                continue
            seen_ids.add(item_id)
            candidates.append(item)
    if not candidates:
        return None

    row_brand_key = _canonical_brand_key_for_value(
        row.brand_name,
        brand_aliases,
    )
    if row_brand_key:
        same_brand = [
            item for item in candidates
            if _canonical_brand_key_for_value(item.brand_name, brand_aliases)
            == row_brand_key
        ]
        selected = _select_single_item_candidate(same_brand)
        if selected is not None:
            return selected

    for priority_key in priority_brand_keys:
        preferred = [
            item for item in candidates
            if _canonical_brand_key_for_value(item.brand_name, brand_aliases)
            == priority_key
        ]
        selected = _select_single_item_candidate(preferred)
        if selected is not None:
            return selected

    return _select_single_item_candidate(candidates)


def _build_applied_row_payload(
    *,
    row: ParsedSupplierResponseRow,
    matched_item: SupplierOrderItem,
) -> AppliedSupplierResponseRow:
    # For text decisions (ЕСТЬ/ОТКАЗ), quantity and price always come from
    # the supplier order item, NOT from the email.  This prevents misreading
    # quoted original-order tables (where customer-order numbers appear in the
    # "comment" column and get confused with quantities).
    if row.text_decision:
        if row.text_decision == "reject":
            parsed_qty = 0
        else:  # "confirm"
            parsed_qty = max(int(matched_item.quantity or 0), 0)
        # Price: use the order item's own price
        # (= price from supplier price list)
        receipt_price = (
            float(
                matched_item.price
            ) if matched_item.price is not None else None
        )
    else:
        resolved_qty = row.confirmed_quantity
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
        receipt_price = row.response_price

    return AppliedSupplierResponseRow(
        supplier_order_item_id=int(matched_item.id),
        supplier_order_id=int(matched_item.supplier_order_id),
        received_quantity=parsed_qty,
        comment=row.response_comment or matched_item.response_comment,
        response_price=receipt_price,
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

    # For text decisions (ЕСТЬ/ОТКАЗ), quantity always comes from the order
    # item — never from email parsing.  This avoids misreading quoted tables
    # where customer order numbers appear in adjacent columns and get confused
    # with quantities.
    if row.text_decision:
        if row.text_decision == "reject":
            next_confirmed_quantity = 0
        else:  # "confirm"
            next_confirmed_quantity = matched_item.quantity
    else:
        next_confirmed_quantity = row.confirmed_quantity

    if (
        next_confirmed_quantity is not None
        and matched_item.confirmed_quantity != next_confirmed_quantity
    ):
        matched_item.confirmed_quantity = next_confirmed_quantity
        item_changed = True

    # For text decisions don't update response_price on the order item —
    # the receipt will use the order item's own price (= supplier price list).
    if (
        row.response_price is not None
        and not row.text_decision
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
    response_config: Optional[SupplierResponseConfig] = None,
) -> tuple[
    int,
    int,
    list[str],
    dict[int, list[AppliedSupplierResponseRow]],
    list[ParsedSupplierResponseRow],
]:
    if not parsed_rows:
        return 0, 0, [], {}, []
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
            list(parsed_rows),
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
    priority_brand_keys = _config_priority_brand_keys(
        response_config,
        brand_aliases,
    )
    updated = 0
    matched_count = 0
    unresolved_oems: list[str] = []
    unmatched_rows: list[ParsedSupplierResponseRow] = []
    applied_rows_by_order: dict[int, list[AppliedSupplierResponseRow]] = {}
    for row in parsed_rows:
        matched_item = _select_row_item_candidate(
            row=row,
            exact_map=exact_map,
            oem_map=oem_map,
            brand_aliases=brand_aliases,
            priority_brand_keys=priority_brand_keys,
        )
        if matched_item is None:
            if row.brand_name is None and priority_brand_keys:
                for key in priority_brand_keys:
                    if key:
                        row.brand_name = key
                        break
            if (
                _canonical_brand_key_for_value(row.brand_name, brand_aliases)
                == _DRAGONZAP_BRAND_KEY
            ):
                row.oem_number = _dragonzap_prefixed_oem_key(
                    _normalize_oem_key(row.oem_number)
                )
            unresolved_oems.append(row.oem_number)
            unmatched_rows.append(row)
            continue
        if not row.brand_name:
            row.brand_name = matched_item.brand_name
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
    return (updated,
            matched_count,
            unresolved_oems,
            applied_rows_by_order,
            unmatched_rows)


def _extract_shipping_document_number(
    shipping_filenames: list[str],
) -> Optional[str]:
    for filename in shipping_filenames:
        clean = str(filename or "").strip()
        if not clean:
            continue
        base_name = clean.rsplit(".", 1)[0].strip()
        extracted_number = _parse_document_number_from_text(base_name)
        if extracted_number:
            return extracted_number[:120]
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


def _build_confirmed_receipt_items(
    order: SupplierOrder,
) -> list[dict[str, object]]:
    items_payload: list[dict[str, object]] = []
    for order_item in order.items or []:
        confirmed_qty = order_item.confirmed_quantity
        if confirmed_qty is None:
            continue
        quantity = max(int(confirmed_qty), 0)
        if quantity <= 0:
            continue
        current_received = int(order_item.received_quantity or 0)
        pending_quantity = max(quantity - current_received, 0)
        if pending_quantity <= 0:
            continue
        items_payload.append(
            {
                "supplier_order_item_id": int(order_item.id),
                "received_quantity": pending_quantity,
                "comment": order_item.response_comment,
                "response_price": order_item.response_price,
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


def _build_unlinked_receipt_items_from_rows(
    rows: list[ParsedSupplierResponseRow],
) -> list[dict[str, object]]:
    items_payload: list[dict[str, object]] = []
    for row in rows:
        quantity = _safe_int(row.confirmed_quantity)
        if quantity is None:
            quantity = 0
        quantity = max(int(quantity), 0)
        if quantity <= 0:
            continue
        items_payload.append(
            {
                "supplier_order_item_id": None,
                "received_quantity": quantity,
                "comment": row.response_comment,
                "response_price": row.response_price,
                "oem_number": row.oem_number,
                "brand_name": row.brand_name,
                "autopart_name": row.source_name,
                "gtd_code": row.gtd_code,
                "country_code": row.country_code,
                "country_name": row.country_name,
                "total_price_with_vat": row.total_price_with_vat,
            }
        )
    return items_payload


def _build_full_document_items_payload(
    *,
    applied_rows_by_order: dict[int, list[AppliedSupplierResponseRow]],
    all_orders_by_id: dict[int, SupplierOrder],
    unmatched_rows: list[ParsedSupplierResponseRow],
) -> list[dict[str, object]]:
    """Build a single flat items payload for a
    document receipt spanning all orders.

    All parsed rows from the document are included:
    - Matched rows → linked to supplier order items
    (supplier_order_item_id set).
    - Unmatched rows → not linked to any order (supplier_order_item_id=None).
    """
    all_order_items: dict[int, SupplierOrderItem] = {
        int(item.id): item
        for order in all_orders_by_id.values()
        for item in (order.items or [])
    }
    payload: list[dict[str, object]] = []
    seen_order_item_ids: set[int] = set()

    for applied_rows in applied_rows_by_order.values():
        for row in applied_rows:
            item_id = int(row.supplier_order_item_id)
            if item_id in seen_order_item_ids:
                continue
            seen_order_item_ids.add(item_id)
            if row.received_quantity <= 0:
                continue
            order_item = all_order_items.get(item_id)
            entry: dict[str, object] = {
                "supplier_order_item_id": row.supplier_order_item_id,
                "supplier_order_id": row.supplier_order_id,
                "received_quantity": row.received_quantity,
                "response_price": row.response_price,
                "total_price_with_vat": row.total_price_with_vat,
                "comment": row.comment,
                "gtd_code": row.gtd_code,
                "country_code": row.country_code,
                "country_name": row.country_name,
            }
            if order_item is not None:
                entry["oem_number"] = order_item.oem_number
                entry["brand_name"] = order_item.brand_name
                entry["autopart_name"] = order_item.autopart_name
                entry["ordered_quantity"] = order_item.quantity
                entry["confirmed_quantity"] = order_item.confirmed_quantity
                entry["autopart_id"] = order_item.autopart_id
                entry["customer_order_item_id"] = (
                    order_item.customer_order_item_id
                )
            payload.append(entry)

    for row in unmatched_rows:
        qty = _safe_int(row.confirmed_quantity)
        if qty is None:
            qty = 0
        qty = max(int(qty), 0)
        if qty <= 0:
            continue
        payload.append(
            {
                "supplier_order_item_id": None,
                "supplier_order_id": None,
                "received_quantity": qty,
                "response_price": row.response_price,
                "total_price_with_vat": row.total_price_with_vat,
                "comment": row.response_comment,
                "gtd_code": row.gtd_code,
                "country_code": row.country_code,
                "country_name": row.country_name,
                "oem_number": row.oem_number,
                "brand_name": row.brand_name,
                "autopart_name": row.source_name,
            }
        )

    return payload


def _build_document_items_payload_from_pending_orders(
    *,
    orders_by_id: dict[int, SupplierOrder],
) -> list[dict[str, object]]:
    payload: list[dict[str, object]] = []
    for order in orders_by_id.values():
        order_id = int(order.id)
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
            payload.append(
                {
                    "supplier_order_item_id": int(order_item.id),
                    "supplier_order_id": order_id,
                    "received_quantity": pending_quantity,
                    "response_price": order_item.response_price,
                    "total_price_with_vat": None,
                    "comment": order_item.response_comment,
                    "gtd_code": None,
                    "country_code": None,
                    "country_name": None,
                    "oem_number": order_item.oem_number,
                    "brand_name": order_item.brand_name,
                    "autopart_name": order_item.autopart_name,
                    "ordered_quantity": order_item.quantity,
                    "confirmed_quantity": order_item.confirmed_quantity,
                    "autopart_id": order_item.autopart_id,
                    "customer_order_item_id": (
                        order_item.customer_order_item_id
                    ),
                }
            )
    return payload


async def _append_document_receipt_items(
    session: AsyncSession,
    *,
    receipt: SupplierReceipt,
    items_payload: list[dict[str, object]],
    all_order_items_by_id: dict[int, SupplierOrderItem],
    response_config: Optional[SupplierResponseConfig] = None,
) -> int:
    """Add items to a single document receipt that may span multiple orders.

    Linked items (supplier_order_item_id set) update the order item's
    received_quantity.  Unlinked items are stored with null order references.
    """
    if not items_payload:
        return 0
    brand_aliases = await _load_brand_alias_map(session)
    priority_brand_keys = _config_priority_brand_keys(
        response_config,
        brand_aliases,
    )
    added = 0
    linked_supplier_order_item_ids: set[int] = set()
    linked_site_order_item_ids: set[int] = set()
    for payload in items_payload:
        supplier_order_item_id_raw = payload.get("supplier_order_item_id")
        quantity = int(payload.get("received_quantity") or 0)

        if supplier_order_item_id_raw is None:
            # Unlinked item — no order match
            if quantity <= 0:
                continue
            resolved = await _resolve_unlinked_payload_autopart(
                session,
                payload=payload,
                response_config=response_config,
                priority_brand_keys=priority_brand_keys,
                brand_aliases=brand_aliases,
            )
            matched_site_order_item = await _match_site_order_item_for_receipt(
                session,
                provider_id=receipt.provider_id,
                oem_number=(
                    resolved.get("oem_number")
                    or payload.get("oem_number")
                    or None
                ),
                brand_name=(
                    resolved.get("brand_name")
                    or payload.get("brand_name")
                    or None
                ),
                received_quantity=quantity,
                exclude_order_item_ids=linked_site_order_item_ids,
            )
            order_item_id = None
            if matched_site_order_item is not None:
                order_item_id = int(matched_site_order_item.id)
                linked_site_order_item_ids.add(order_item_id)
            session.add(
                SupplierReceiptItem(
                    receipt_id=receipt.id,
                    supplier_order_id=None,
                    supplier_order_item_id=None,
                    customer_order_item_id=None,
                    order_item_id=order_item_id,
                    autopart_id=(
                        resolved.get("autopart_id")
                        or (
                            matched_site_order_item.autopart_id
                            if matched_site_order_item is not None
                            else None
                        )
                    ),
                    oem_number=(
                        str(
                            resolved.get("oem_number")
                            or payload.get("oem_number")
                            or ""
                        ).strip()
                        or None
                    ),
                    brand_name=(
                        str(
                            resolved.get("brand_name")
                            or payload.get("brand_name")
                            or ""
                        ).strip()
                        or None
                    ),
                    autopart_name=(
                        str(
                            resolved.get("autopart_name")
                            or payload.get("autopart_name")
                            or ""
                        ).strip()
                        or None
                    ),
                    ordered_quantity=None,
                    confirmed_quantity=None,
                    received_quantity=quantity,
                    price=payload.get("response_price"),
                    comment=(
                        str(payload.get("comment") or "").strip() or None
                    ),
                    gtd_code=(
                        str(payload.get("gtd_code") or "").strip() or None
                    ),
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
        else:
            # Linked item — belongs to a supplier order
            if quantity <= 0:
                continue
            supplier_order_item_id = int(supplier_order_item_id_raw)
            order_item = all_order_items_by_id.get(supplier_order_item_id)
            # Cap to pending quantity to avoid over-receiving
            if order_item is not None:
                expected_qty = (
                    int(order_item.confirmed_quantity)
                    if order_item.confirmed_quantity is not None
                    else int(order_item.quantity or 0)
                )
                current_received = int(order_item.received_quantity or 0)
                pending = max(expected_qty - current_received, 0)
                quantity = min(quantity, pending)
                if quantity <= 0:
                    continue
                order_item.received_quantity = current_received + quantity
                order_item.received_at = now_moscow()
                linked_supplier_order_item_ids.add(int(order_item.id))
            session.add(
                SupplierReceiptItem(
                    receipt_id=receipt.id,
                    supplier_order_id=payload.get("supplier_order_id"),
                    supplier_order_item_id=supplier_order_item_id,
                    customer_order_item_id=payload.get(
                        "customer_order_item_id"
                    ),
                    autopart_id=payload.get("autopart_id"),
                    oem_number=(
                        str(payload.get("oem_number") or "").strip() or None
                    ),
                    brand_name=(
                        str(payload.get("brand_name") or "").strip() or None
                    ),
                    autopart_name=(
                        str(payload.get("autopart_name") or "").strip() or None
                    ),
                    ordered_quantity=_safe_int(
                        payload.get("ordered_quantity")
                    ),
                    confirmed_quantity=_safe_int(
                        payload.get("confirmed_quantity")
                    ),
                    received_quantity=quantity,
                    price=(
                        payload.get("response_price")
                        or (
                            float(order_item.price)
                            if order_item is not None
                            and order_item.price is not None
                            else None
                        )
                    ),
                    comment=(
                        str(payload.get("comment") or "").strip() or None
                    ),
                    gtd_code=(
                        str(payload.get("gtd_code") or "").strip() or None
                    ),
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

    if added:
        await _refresh_receipt_links(
            session,
            supplier_order_item_ids=linked_supplier_order_item_ids,
            order_item_ids=linked_site_order_item_ids,
        )
        receipt.posted_at = now_moscow()
    return added


async def _create_single_document_receipt(
    session: AsyncSession,
    *,
    provider_id: int,
    message_row: SupplierOrderMessage,
    items_payload: list[dict[str, object]],
    document_number: Optional[str] = None,
    document_date: Optional[date] = None,
    comment: Optional[str] = None,
    all_order_items_by_id: dict[int, SupplierOrderItem],
    response_config: Optional[SupplierResponseConfig] = None,
) -> tuple[Optional[SupplierReceipt], int]:
    """Create a single posted receipt for an entire document (УПД/накладная).

    One receipt covers all items regardless of how many supplier orders they
    belong to.  Items without an order match are stored with null order refs.
    """
    if not items_payload:
        return None, 0
    warehouse = await resolve_warehouse_for_provider(
        session,
        provider_id=provider_id,
    )
    receipt = SupplierReceipt(
        provider_id=provider_id,
        warehouse_id=warehouse.id,
        supplier_order_id=None,
        source_message_id=message_row.id,
        document_number=document_number or None,
        document_date=document_date or now_moscow().date(),
        created_by_user_id=None,
        created_at=now_moscow(),
        posted_at=None,  # will be set by _append_document_receipt_items
        comment=comment,
    )
    session.add(receipt)
    await session.flush()
    added = await _append_document_receipt_items(
        session,
        receipt=receipt,
        items_payload=items_payload,
        all_order_items_by_id=all_order_items_by_id,
        response_config=response_config,
    )
    if added <= 0:
        await session.delete(receipt)
        await session.flush()
        return None, 0
    await session.flush()
    if receipt.posted_at is not None:
        await apply_receipt_to_stock_by_id(session, receipt_id=receipt.id)
    return receipt, added


async def _find_open_supplier_receipt(
    session: AsyncSession,
    *,
    provider_id: int,
) -> Optional[SupplierReceipt]:
    stmt = (
        select(SupplierReceipt)
        .options(selectinload(SupplierReceipt.items))
        .where(
            SupplierReceipt.provider_id == provider_id,
            SupplierReceipt.posted_at.is_(None),
        )
        .order_by(SupplierReceipt.created_at.desc(), SupplierReceipt.id.desc())
    )
    return (await session.execute(stmt)).scalars().first()


async def _find_open_supplier_receipt_for_order(
    session: AsyncSession,
    *,
    provider_id: int,
    order_id: int,
) -> Optional[SupplierReceipt]:
    stmt = (
        select(SupplierReceipt)
        .options(selectinload(SupplierReceipt.items))
        .where(
            SupplierReceipt.provider_id == provider_id,
            SupplierReceipt.posted_at.is_(None),
            or_(
                SupplierReceipt.supplier_order_id == order_id,
                SupplierReceipt.supplier_order_id.is_(None),
            ),
        )
        .order_by(SupplierReceipt.created_at.desc(), SupplierReceipt.id.desc())
    )
    return (await session.execute(stmt)).scalars().first()


def _linked_receipt_quantities_by_order_item(
    items_payload: list[dict[str, object]],
) -> dict[int, int]:
    quantities: dict[int, int] = {}
    for payload in items_payload:
        supplier_order_item_raw = payload.get("supplier_order_item_id")
        if supplier_order_item_raw in (None, ""):
            continue
        try:
            supplier_order_item_id = int(supplier_order_item_raw)
        except (TypeError, ValueError):
            continue
        quantity = _safe_int(payload.get("received_quantity"))
        if quantity is None:
            continue
        quantity = max(int(quantity), 0)
        if quantity <= 0:
            continue
        quantities[supplier_order_item_id] = (
            quantities.get(supplier_order_item_id, 0) + quantity
        )
    return quantities


async def _consume_posted_quantities_from_open_draft(
    session: AsyncSession,
    *,
    draft_receipt: SupplierReceipt,
    order: SupplierOrder,
    linked_items_payload: list[dict[str, object]],
) -> tuple[int, bool]:
    linked_quantities = _linked_receipt_quantities_by_order_item(
        linked_items_payload
    )
    if not linked_quantities:
        return 0, False

    draft_items = (
        await session.execute(
            select(SupplierReceiptItem)
            .where(
                SupplierReceiptItem.receipt_id == draft_receipt.id,
                SupplierReceiptItem.supplier_order_item_id.in_(
                    tuple(linked_quantities.keys())
                ),
                or_(
                    SupplierReceiptItem.supplier_order_id == order.id,
                    SupplierReceiptItem.supplier_order_id.is_(None),
                ),
            )
            .order_by(SupplierReceiptItem.id.asc())
        )
    ).scalars().all()

    if not draft_items:
        return 0, False

    draft_items_by_order_item: dict[int, list[SupplierReceiptItem]] = {}
    for draft_item in draft_items:
        supplier_order_item_id = draft_item.supplier_order_item_id
        if supplier_order_item_id is None:
            continue
        draft_items_by_order_item.setdefault(
            int(supplier_order_item_id),
            [],
        ).append(draft_item)

    updated_or_deleted = 0
    for supplier_order_item_id, posted_qty in linked_quantities.items():
        remaining_to_consume = posted_qty
        for draft_item in draft_items_by_order_item.get(
            supplier_order_item_id,
            [],
        ):
            if remaining_to_consume <= 0:
                break
            current_qty = max(int(draft_item.received_quantity or 0), 0)
            if current_qty <= 0:
                continue
            consume_qty = min(current_qty, remaining_to_consume)
            remaining_to_consume -= consume_qty
            new_qty = current_qty - consume_qty
            if new_qty <= 0:
                if draft_item in (draft_receipt.items or []):
                    draft_receipt.items.remove(draft_item)
                else:
                    await session.delete(draft_item)
            else:
                draft_item.received_quantity = new_qty
            updated_or_deleted += 1

    if updated_or_deleted <= 0:
        return 0, False

    await session.flush()

    has_items = (
        await session.execute(
            select(SupplierReceiptItem.id)
            .where(SupplierReceiptItem.receipt_id == draft_receipt.id)
            .limit(1)
        )
    ).scalar_one_or_none()
    if has_items is not None:
        return updated_or_deleted, False

    await session.delete(draft_receipt)
    await session.flush()
    return updated_or_deleted, True


def _first_configured_brand_name(
    response_config: Optional[SupplierResponseConfig],
) -> Optional[str]:
    if response_config is None:
        return None
    fixed = str(getattr(response_config, "fixed_brand_name", "") or "").strip()
    if fixed:
        return fixed
    for raw in (getattr(response_config, "brand_priority_list", None) or []):
        value = str(raw or "").strip()
        if value:
            return value
    return None


async def _find_autoparts_by_oem(
    session: AsyncSession,
    *,
    oem_key: str,
) -> list[AutoPart]:
    if not oem_key:
        return []
    stmt = (
        select(AutoPart)
        .options(selectinload(AutoPart.brand))
        .where(AutoPart.oem_number == oem_key)
    )
    return list((await session.execute(stmt)).scalars().all())


def _pick_autopart_by_priority(
    autoparts: list[AutoPart],
    *,
    priority_brand_keys: list[str],
    brand_aliases: dict[str, str],
) -> Optional[AutoPart]:
    if not autoparts:
        return None
    for key in priority_brand_keys:
        for autopart in autoparts:
            if (
                _canonical_brand_key_for_value(
                    getattr(autopart.brand, "name", None),
                    brand_aliases,
                )
                == key
            ):
                return autopart
    return autoparts[0]


async def _get_or_create_dragonzap_autopart(
    session: AsyncSession,
    *,
    oem_key: str,
    fallback_name: Optional[str],
) -> Optional[AutoPart]:
    dragonzap_brand = await brand_crud.get_brand_by_name_or_none(
        "Dragonzap",
        session,
    )
    if dragonzap_brand is None:
        return None
    dz_oem = _dragonzap_prefixed_oem_key(oem_key)
    existing = await crud_autopart.get_autopart_by_oem_brand_or_none(
        oem_number=dz_oem,
        brand_id=dragonzap_brand.id,
        session=session,
    )
    if existing is not None:
        return existing

    source_same_brand = await crud_autopart.get_autopart_by_oem_brand_or_none(
        oem_number=oem_key,
        brand_id=dragonzap_brand.id,
        session=session,
    )
    source_name = None
    if source_same_brand is not None:
        source_name = str(source_same_brand.name or "").strip() or None
    if source_name is None:
        any_source = await _find_autoparts_by_oem(session, oem_key=oem_key)
        if any_source:
            source_name = str(any_source[0].name or "").strip() or None
    if source_name is None:
        source_name = str(fallback_name or "").strip() or None
    if source_name is None:
        source_name = f"Автозапчасть {oem_key}"

    created: Optional[AutoPart] = None
    try:
        async with session.begin_nested():
            created = AutoPart(
                brand_id=dragonzap_brand.id,
                oem_number=dz_oem,
                name=source_name,
            )
            session.add(created)
            await session.flush()
    except IntegrityError:
        created = None
    if created is not None:
        await session.refresh(created)
        return created
    return await crud_autopart.get_autopart_by_oem_brand_or_none(
        oem_number=dz_oem,
        brand_id=dragonzap_brand.id,
        session=session,
    )


async def _resolve_unlinked_payload_autopart(
    session: AsyncSession,
    *,
    payload: dict[str, object],
    response_config: Optional[SupplierResponseConfig],
    priority_brand_keys: list[str],
    brand_aliases: dict[str, str],
) -> dict[str, object]:
    oem_key = _normalize_oem_key(payload.get("oem_number"))
    if not oem_key:
        return {}

    brand_name = str(payload.get("brand_name") or "").strip() or None
    if brand_name is None:
        brand_name = _first_configured_brand_name(response_config)

    brand_key = _canonical_brand_key_for_value(brand_name, brand_aliases)
    fallback_name = str(payload.get("autopart_name") or "").strip() or None

    if (
        brand_key == _DRAGONZAP_BRAND_KEY
        or _DRAGONZAP_BRAND_KEY in priority_brand_keys
    ):
        dragonzap_part = await _get_or_create_dragonzap_autopart(
            session,
            oem_key=oem_key,
            fallback_name=fallback_name,
        )
        dz_oem = _dragonzap_prefixed_oem_key(oem_key)
        if dragonzap_part is not None:
            return {
                "autopart_id": int(dragonzap_part.id),
                "oem_number": dragonzap_part.oem_number,
                "brand_name": "Dragonzap",
                "autopart_name": dragonzap_part.name,
            }
        return {
            "oem_number": dz_oem,
            "brand_name": "Dragonzap",
            "autopart_name": fallback_name,
        }

    if brand_name:
        brand = await brand_crud.get_brand_by_name_or_none(brand_name, session)
        if brand is not None:
            matched = await crud_autopart.get_autopart_by_oem_brand_or_none(
                oem_number=oem_key,
                brand_id=brand.id,
                session=session,
            )
            if matched is not None:
                return {
                    "autopart_id": int(matched.id),
                    "oem_number": matched.oem_number,
                    "brand_name": brand.name,
                    "autopart_name": matched.name,
                }
            return {
                "oem_number": oem_key,
                "brand_name": brand.name,
                "autopart_name": fallback_name,
            }

    autoparts = await _find_autoparts_by_oem(session, oem_key=oem_key)
    if not autoparts:
        return {
            "oem_number": oem_key,
            "brand_name": brand_name,
            "autopart_name": fallback_name,
        }
    selected = _pick_autopart_by_priority(
        autoparts,
        priority_brand_keys=priority_brand_keys,
        brand_aliases=brand_aliases,
    )
    if selected is None:
        return {
            "oem_number": oem_key,
            "brand_name": brand_name,
            "autopart_name": fallback_name,
        }
    return {
        "autopart_id": int(selected.id),
        "oem_number": selected.oem_number,
        "brand_name": (
            str(getattr(selected.brand, "name", "") or "").strip()
            if "brand" in getattr(selected, "__dict__", {})
            else brand_name
        ),
        "autopart_name": selected.name,
    }


async def _append_supplier_receipt_items(
    session: AsyncSession,
    *,
    receipt: SupplierReceipt,
    order: SupplierOrder,
    items_payload: list[dict[str, object]],
    post_now: bool,
    response_config: Optional[SupplierResponseConfig] = None,
) -> int:
    if not items_payload:
        return 0
    brand_aliases = await _load_brand_alias_map(session)
    priority_brand_keys = _config_priority_brand_keys(
        response_config,
        brand_aliases,
    )
    order_items_by_id = {
        int(order_item.id): order_item for order_item in (order.items or [])
    }
    supplier_order_ids: set[int] = set()
    added = 0
    for payload in items_payload:
        supplier_order_item_raw = payload.get("supplier_order_item_id")
        if supplier_order_item_raw in (None, ""):
            quantity = int(payload.get("received_quantity") or 0)
            if quantity <= 0:
                continue
            resolved_identity = await _resolve_unlinked_payload_autopart(
                session,
                payload=payload,
                response_config=response_config,
                priority_brand_keys=priority_brand_keys,
                brand_aliases=brand_aliases,
            )
            supplier_order_ids.add(int(order.id))
            session.add(
                SupplierReceiptItem(
                    receipt_id=receipt.id,
                    supplier_order_id=order.id,
                    supplier_order_item_id=None,
                    customer_order_item_id=None,
                    autopart_id=resolved_identity.get("autopart_id"),
                    oem_number=(
                        str(
                            resolved_identity.get("oem_number")
                            or payload.get("oem_number")
                            or ""
                        ).strip()
                        or None
                    ),
                    brand_name=(
                        str(
                            resolved_identity.get("brand_name")
                            or payload.get("brand_name")
                            or ""
                        ).strip()
                        or None
                    ),
                    autopart_name=(
                        str(
                            resolved_identity.get("autopart_name")
                            or payload.get("autopart_name")
                            or ""
                        ).strip()
                        or None
                    ),
                    ordered_quantity=None,
                    confirmed_quantity=None,
                    received_quantity=quantity,
                    price=payload.get("response_price"),
                    comment=(
                        str(payload.get("comment") or "").strip() or None
                    ),
                    gtd_code=(
                        str(payload.get("gtd_code") or "").strip() or None
                    ),
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
            continue
        supplier_order_item_id = int(supplier_order_item_raw)
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
    response_config: Optional[SupplierResponseConfig] = None,
) -> tuple[Optional[SupplierReceipt], int, bool]:
    if not items_payload:
        return None, 0, False
    warehouse = await resolve_warehouse_for_provider(
        session,
        provider_id=provider_id,
    )
    created = False
    receipt: Optional[SupplierReceipt]
    if post_now:
        receipt = SupplierReceipt(
            provider_id=provider_id,
            warehouse_id=warehouse.id,
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
                warehouse_id=warehouse.id,
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
        elif receipt.warehouse_id is None:
            receipt.warehouse_id = warehouse.id

    added_items = await _append_supplier_receipt_items(
        session,
        receipt=receipt,
        order=order,
        items_payload=items_payload,
        post_now=post_now,
        response_config=response_config,
    )
    if added_items <= 0:
        if created:
            await session.delete(receipt)
            await session.flush()
        return None, 0, False
    await session.flush()
    if receipt.posted_at is not None:
        await apply_receipt_to_stock_by_id(session, receipt_id=receipt.id)
    return receipt, added_items, created


async def _load_supplier_response_configs(
    session: AsyncSession,
    *,
    provider_id: Optional[int] = None,
    supplier_response_config_id: Optional[int] = None,
    file_payload_mode: str = 'all',
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
    if file_payload_mode == 'responses':
        stmt = stmt.where(
            or_(
                SupplierResponseConfig.file_payload_type == 'response',
                SupplierResponseConfig.file_payload_type.is_(None),
            )
        )
    elif file_payload_mode == 'documents':
        stmt = stmt.where(
            SupplierResponseConfig.file_payload_type == 'document'
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


def _build_sender_filters_from_configs(
    configs: Iterable[SupplierResponseConfig],
) -> list[str]:
    sender_filters: list[str] = []
    seen_sender_filters: set[str] = set()
    for config in configs:
        for sender_filter in sorted(
            _normalize_sender_emails(config.sender_emails)
        ):
            if sender_filter in seen_sender_filters:
                continue
            seen_sender_filters.add(sender_filter)
            sender_filters.append(sender_filter)
    return sender_filters


async def process_supplier_response_messages(
    session: AsyncSession,
    *,
    provider_id: Optional[int] = None,
    supplier_response_config_id: Optional[int] = None,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
    file_payload_mode: str = 'all',
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
    pending_customer_order_ids: set[int] = set()

    async def _load_runtime_configs() -> tuple[
        list[SupplierResponseConfig],
        list[SupplierResponseConfig],
        dict[int, list[SupplierResponseConfig]],
        Optional[SupplierResponseConfig],
    ]:
        runtime_response_configs = await _load_supplier_response_configs(
            session,
            provider_id=provider_id,
            supplier_response_config_id=supplier_response_config_id,
            file_payload_mode=file_payload_mode,
        )
        runtime_opposite_mode_configs: list[SupplierResponseConfig] = []
        if file_payload_mode == "responses":
            runtime_opposite_mode_configs = (
                await _load_supplier_response_configs(
                    session,
                    provider_id=provider_id,
                    file_payload_mode="documents",
                )
            )
        elif file_payload_mode == "documents":
            runtime_opposite_mode_configs = (
                await _load_supplier_response_configs(
                    session,
                    provider_id=provider_id,
                    file_payload_mode="responses",
                )
            )
        runtime_configs_by_provider = _group_response_configs_by_provider(
            runtime_response_configs
        )
        runtime_selected_config = None
        if (
            supplier_response_config_id is not None
            and runtime_response_configs
        ):
            runtime_selected_config = runtime_response_configs[0]
        return (
            runtime_response_configs,
            runtime_opposite_mode_configs,
            runtime_configs_by_provider,
            runtime_selected_config,
        )

    (
        response_configs,
        opposite_mode_configs,
        configs_by_provider,
        selected_config,
    ) = await _load_runtime_configs()

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
    sender_filters = _build_sender_filters_from_configs(response_configs)
    from_email_filters: list[str] = []
    if sender_filters:
        should_apply_sender_filters = (
            supplier_response_config_id is not None
            or provider_id is not None
            or len(sender_filters) <= _SUPPLIER_RESPONSE_FETCH_MAX_FROM_FILTERS
        )
        if should_apply_sender_filters:
            from_email_filters = sender_filters
    use_server_side_from_filters = file_payload_mode == "responses"

    fetch_kwargs: dict[str, object] = {
        "date_from": date_from,
        "date_to": date_to,
        "use_server_side_from_filters": use_server_side_from_filters,
    }
    if explicit_account_ids:
        fetch_kwargs["account_ids"] = explicit_account_ids
    if not include_default_orders_out:
        fetch_kwargs["include_default_orders_out"] = False
    if from_email_filters:
        fetch_kwargs["from_email_filters"] = from_email_filters
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
    reload_runtime_configs = False
    for index, (msg, account) in enumerate(messages, start=1):
        if reload_runtime_configs:
            (
                response_configs,
                opposite_mode_configs,
                configs_by_provider,
                selected_config,
            ) = await _load_runtime_configs()
            reload_runtime_configs = False
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
        account_id = _safe_email_account_id(account)
        if opposite_mode_configs:
            opposite_match = _select_best_supplier_response_config(
                opposite_mode_configs,
                sender_email=sender_email,
                account=account,
                subject=subject,
            )
            if opposite_match is not None:
                logger.info(
                    (
                        "Supplier response message skipped by payload mode: "
                        "mode=%s matched_other_config=%s sender=%s "
                        "account_id=%s subject=%s"
                    ),
                    file_payload_mode,
                    opposite_match.id,
                    sender_email or "<empty>",
                    account_id,
                    subject[:200],
                )
                stats.skipped_messages += 1
                continue
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
            account_id,
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
                    subject=subject,
                ):
                    reasons = _config_mismatch_reasons(
                        active_response_config,
                        sender_email=sender_email,
                        account=account,
                        subject=subject,
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
                        account_id,
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
                    subject=subject,
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
                                subject=subject,
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
                        account_id,
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
                    subject=subject,
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
                        account_id,
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
                response_name_col = getattr(
                    active_response_config,
                    "name_col",
                    None,
                )
                response_brand_from_name_regex = getattr(
                    active_response_config,
                    "brand_from_name_regex",
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
                response_document_number_cell = getattr(
                    active_response_config,
                    "document_number_cell",
                    None,
                )
                response_document_date_cell = getattr(
                    active_response_config,
                    "document_date_cell",
                    None,
                )
                response_document_meta_cell = getattr(
                    active_response_config,
                    "document_meta_cell",
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
                auto_confirm_unmentioned_items = bool(
                    getattr(
                        active_response_config,
                        "auto_confirm_unmentioned_items",
                        False,
                    )
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
                response_name_col = None
                response_brand_from_name_regex = None
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
                response_document_number_cell = None
                response_document_date_cell = None
                response_document_meta_cell = None
                response_gtd_col = None
                response_country_code_col = None
                response_country_name_col = None
                response_total_price_with_vat_col = None
                confirm_keywords = _DEFAULT_CONFIRM_KEYWORDS
                reject_keywords = _DEFAULT_REJECT_KEYWORDS
                value_after_article_type = "both"
                auto_confirm_unmentioned_items = False

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
                    "payload_type=%s account_filter=%s "
                    "auto_confirm_unmentioned=%s"
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
                auto_confirm_unmentioned_items,
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
            receipt_extra_rows_by_order: dict[
                int,
                list[ParsedSupplierResponseRow],
            ] = {}
            # Document-receipt accumulators: collect ALL rows from the document
            # so one receipt can be created
            # covering all orders + unmatched items.
            document_all_applied_rows_by_order: dict[
                int,
                list[AppliedSupplierResponseRow],
            ] = {}
            document_all_unmatched_rows: list[ParsedSupplierResponseRow] = []
            auto_confirmed_order_ids: set[int] = set()
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
                            name_col=response_name_col,
                            brand_from_name_regex=(
                                response_brand_from_name_regex
                            ),
                            qty_col=response_qty_col,
                            price_col=response_price_col,
                            comment_col=response_comment_col,
                            status_col=response_status_col,
                            document_number_col=response_document_number_col,
                            document_date_col=response_document_date_col,
                            document_number_cell=(
                                response_document_number_cell
                            ),
                            document_date_cell=response_document_date_cell,
                            document_meta_cell=response_document_meta_cell,
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
                            exc_info=True,
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
                            unmatched_rows,
                        ) = (
                            await _apply_parsed_response_rows(
                                session,
                                order=order,
                                parsed_rows=parsed_rows,
                                default_raw_status=raw_status,
                                default_normalized_status=normalized_status,
                                response_config=active_response_config,
                            )
                        )
                        stats.updated_items += updated_items
                        receipt_applied_rows_by_order.setdefault(
                            int(order.id),
                            [],
                        ).extend(applied_rows)
                        if file_payload_is_document and unmatched_rows:
                            receipt_extra_rows_by_order.setdefault(
                                int(order.id),
                                [],
                            ).extend(unmatched_rows)
                        # Populate document-wide accumulators
                        if file_payload_is_document:
                            document_all_applied_rows_by_order.setdefault(
                                int(order.id), []
                            ).extend(applied_rows)
                            document_all_unmatched_rows.extend(unmatched_rows)
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
                            unmatched_rows_full,
                        ) = await _apply_parsed_rows_without_order_id(
                            session,
                            provider_id=provider.id,
                            parsed_rows=parsed_rows,
                            default_raw_status=raw_status,
                            default_normalized_status=(
                                normalized_status or None
                            ),
                            date_from=date_from,
                            response_config=active_response_config,
                        )
                        stats.updated_items += updated_items
                        stats.recognized_positions += matched_count
                        for order_key, rows in applied_rows_map.items():
                            matched_order_ids_from_rows.add(int(order_key))
                            receipt_applied_rows_by_order.setdefault(
                                int(order_key),
                                [],
                            ).extend(rows)
                        # Populate document-wide accumulators
                        if file_payload_is_document:
                            for order_key, rows in applied_rows_map.items():
                                document_all_applied_rows_by_order.setdefault(
                                    int(order_key), []
                                ).extend(rows)
                            document_all_unmatched_rows.extend(
                                unmatched_rows_full
                            )
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
                # Strip the quoted/forwarded reply block so we only parse
                # the supplier's own response text (e.g. "ЕСТЬ"), not the
                # mirrored original order that was forwarded to them.
                response_only_text = _strip_quoted_reply_content(message_text)
                # Prepend the subject line so decisions written there
                # (e.g. "Re: Заказ / отказ FZ01-19-241" or
                # "Re: Заказ / отказов нет") are also parsed.
                subject_stripped = _strip_email_subject_prefix(subject)
                response_text_with_subject = "\n".join(
                    s for s in [subject_stripped, response_only_text] if s
                )
                parsed_text = _parse_supplier_text_response(
                    response_text_with_subject,
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
                        _unused_unmatched_rows,
                    ) = (0, 0, [], [], [])
                    if order is not None:
                        (
                            updated_items,
                            matched_count,
                            unresolved_oems,
                            applied_rows,
                            _unused_unmatched_rows,
                        ) = await _apply_parsed_response_rows(
                            session,
                            order=order,
                            parsed_rows=parsed_text.rows,
                            default_raw_status=raw_status,
                            default_normalized_status=(
                                normalized_status or None
                            ),
                            response_config=active_response_config,
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
                            _unused_unmatched_rows,
                        ) = await _apply_parsed_rows_without_order_id(
                            session,
                            provider_id=provider.id,
                            parsed_rows=parsed_text.rows,
                            default_raw_status=raw_status,
                            default_normalized_status=(
                                normalized_status or None
                            ),
                            date_from=date_from,
                            response_config=active_response_config,
                        )
                        for order_key, rows in applied_rows_map.items():
                            matched_order_ids_from_rows.add(int(order_key))
                            receipt_applied_rows_by_order.setdefault(
                                int(order_key),
                                [],
                            ).extend(rows)
                    stats.updated_items += updated_items
                    stats.parsed_text_positions += (
                        parsed_text.parsed_positions
                    )
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
                        # Per-OEM parsing matched some items but may have
                        # skipped others (e.g. when the quoted original-order
                        # table is present in the email body and an OEM is
                        # mis-tokenised or absent).  If the supplier also
                        # wrote a global confirm keyword (e.g. "в резерве"),
                        # apply it as a fallback to every unmatched item so
                        # nothing is silently dropped from the receipt.
                        if order is not None:
                            (
                                _fallback_decision,
                                _fallback_token,
                            ) = _detect_global_text_decision(
                                response_text_with_subject,
                                confirm_keywords=confirm_keywords,
                                reject_keywords=reject_keywords,
                            )
                            if _fallback_decision == "confirm":
                                _touched_ids = {
                                    int(r.supplier_order_item_id)
                                    for r in applied_rows
                                }
                                (
                                    _fallback_updated,
                                    _fallback_rows,
                                ) = _apply_global_text_decision_to_order(
                                    order,
                                    decision="confirm",
                                    status_label=(
                                        _fallback_token
                                        or raw_status
                                        or None
                                    ),
                                )
                                _new_fallback_rows = [
                                    r for r in _fallback_rows
                                    if int(r.supplier_order_item_id)
                                    not in _touched_ids
                                    and (r.received_quantity or 0) > 0
                                ]
                                if _new_fallback_rows:
                                    receipt_applied_rows_by_order.setdefault(
                                        int(order.id),
                                        [],
                                    ).extend(_new_fallback_rows)
                                    stats.recognized_positions += len(
                                        _new_fallback_rows
                                    )
                                    if _fallback_updated > 0:
                                        stats.updated_items += (
                                            _fallback_updated
                                        )
                if not parsed_text_rows and not parsed_response_file:
                    (
                        global_decision,
                        global_decision_token,
                    ) = _detect_global_text_decision(
                        response_text_with_subject,
                        confirm_keywords=confirm_keywords,
                        reject_keywords=reject_keywords,
                    )
                    if global_decision is not None:
                        status_label = (
                            global_decision_token
                            or raw_status
                            or (
                                "подтверждено"
                                if global_decision == "confirm"
                                else "отказ"
                            )
                        )
                        if order is not None:
                            # Known order: apply decision to all its items
                            (
                                updated_items,
                                global_applied_rows,
                            ) = _apply_global_text_decision_to_order(
                                order,
                                decision=global_decision,
                                status_label=status_label,
                            )
                            if updated_items > 0:
                                stats.updated_items += updated_items
                            if global_applied_rows:
                                receipt_applied_rows_by_order.setdefault(
                                    int(order.id),
                                    [],
                                ).extend(global_applied_rows)
                                stats.recognized_positions += len(
                                    global_applied_rows
                                )
                            stats.parsed_text_positions += 1
                            parsed_text_rows = True
                        else:
                            # Order not identified by ID in subject/body.
                            # Use OEM tokens from the full email (including the
                            # quoted forwarded order) to match order items by
                            # article number, then apply the global decision.
                            global_rows = (
                                _build_global_decision_rows_from_text(
                                    message_text,
                                    global_decision=global_decision,
                                    global_decision_token=(
                                        global_decision_token
                                    ),
                                )
                            )
                            if global_rows:
                                (
                                    updated_items,
                                    matched_count,
                                    unresolved_oems,
                                    applied_rows_map,
                                    _unmatched_global,
                                ) = await _apply_parsed_rows_without_order_id(
                                    session,
                                    provider_id=provider.id,
                                    parsed_rows=global_rows,
                                    default_raw_status=status_label,
                                    default_normalized_status=(
                                        normalize_external_status_text(
                                            status_label
                                        )
                                        or None
                                    ),
                                    date_from=date_from,
                                    response_config=active_response_config,
                                )
                                stats.updated_items += updated_items
                                stats.recognized_positions += matched_count
                                for (order_key,
                                     rows) in applied_rows_map.items():
                                    matched_order_ids_from_rows.add(
                                        int(order_key)
                                    )
                                    receipt_applied_rows_by_order.setdefault(
                                        int(order_key),
                                        [],
                                    ).extend(rows)
                                if matched_count > 0:
                                    stats.parsed_text_positions += 1
                                    parsed_text_rows = True
            if (
                expects_text_payload
                and not parsed_text_rows
                and not raw_status
            ):
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
                if auto_confirm_unmentioned_items:
                    for matched_order in matched_orders.values():
                        order_key = int(matched_order.id)
                        applied_rows = receipt_applied_rows_by_order.get(
                            order_key,
                            [],
                        )
                        if not applied_rows:
                            continue
                        auto_updated, auto_rows = (
                            _auto_confirm_unmentioned_order_items(
                                matched_order,
                                applied_rows=applied_rows,
                            )
                        )
                        if auto_updated > 0:
                            stats.updated_items += auto_updated
                        if auto_rows:
                            receipt_applied_rows_by_order.setdefault(
                                order_key,
                                [],
                            ).extend(auto_rows)
                        auto_confirmed_order_ids.add(order_key)
                if (
                    has_shipping_doc
                    and parsed_response_file
                    and normalized_file_payload_type == "document"
                    and order is not None
                ):
                    target_order = matched_orders.get(int(order.id))
                    if target_order is not None:
                        order_key = int(target_order.id)
                        applied_rows = receipt_applied_rows_by_order.get(
                            order_key,
                            [],
                        )
                        reject_updates = _auto_reject_unmentioned_order_items(
                            target_order,
                            applied_rows=applied_rows,
                        )
                        if reject_updates > 0:
                            stats.updated_items += reject_updates
                stats.matched_orders += len(matched_orders)
                affected_customer_order_item_ids = {
                    int(order_item.customer_order_item_id)
                    for matched_order in matched_orders.values()
                    for order_item in (matched_order.items or [])
                    if order_item.customer_order_item_id is not None
                }
                if affected_customer_order_item_ids:
                    order_id_rows = (
                        await session.execute(
                            select(CustomerOrderItem.order_id).where(
                                CustomerOrderItem.id.in_(
                                    affected_customer_order_item_ids
                                )
                            )
                        )
                    ).scalars().all()
                    pending_customer_order_ids.update(
                        int(order_id)
                        for order_id in order_id_rows
                        if order_id is not None
                    )
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

            if has_shipping_doc:
                # Document receipt: ONE receipt for the ENTIRE document.
                # Contains ALL items — matched to orders AND unmatched.
                # Unmatched items get null supplier_order_item_id.
                all_orders_for_doc: dict[int, SupplierOrder] = dict(
                    matched_orders
                )
                all_order_items_for_doc: dict[int, SupplierOrderItem] = {
                    int(item.id): item
                    for order in all_orders_for_doc.values()
                    for item in (order.items or [])
                }
                doc_items_payload = _build_full_document_items_payload(
                    applied_rows_by_order=document_all_applied_rows_by_order,
                    all_orders_by_id=all_orders_for_doc,
                    unmatched_rows=document_all_unmatched_rows,
                )
                if not doc_items_payload and all_orders_for_doc:
                    doc_items_payload = (
                        _build_document_items_payload_from_pending_orders(
                            orders_by_id=all_orders_for_doc,
                        )
                    )
                # Extract document number / date from any matched applied row
                all_applied_flat = [
                    row
                    for rows in document_all_applied_rows_by_order.values()
                    for row in rows
                ]
                row_document_number = next(
                    (
                        str(ap.document_number).strip()
                        for ap in all_applied_flat
                        if ap.document_number
                    ),
                    "",
                )
                row_document_date = next(
                    (
                        ap.document_date
                        for ap in all_applied_flat
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
                if doc_items_payload:
                    posted_receipt, added_items = (
                        await _create_single_document_receipt(
                            session,
                            provider_id=provider.id,
                            message_row=message_row,
                            items_payload=doc_items_payload,
                            document_number=receipt_document_number,
                            document_date=row_document_date,
                            comment=(
                                "Авто-проведение по документу "
                                "УПД/накладной из почты"
                            ),
                            all_order_items_by_id=all_order_items_for_doc,
                            response_config=active_response_config,
                        )
                    )
                    if added_items > 0:
                        stats.created_receipts += 1
                        stats.posted_receipts += 1
                        stats.receipt_items_added += added_items
                        linked_items_payload = [
                            payload
                            for payload in doc_items_payload
                            if payload.get("supplier_order_item_id")
                            not in (None, "")
                        ]
                        if linked_items_payload and all_orders_for_doc:
                            updated_draft_receipt_ids: set[int] = set()
                            find_open_draft = (
                                _find_open_supplier_receipt_for_order
                            )
                            for matched_order in all_orders_for_doc.values():
                                draft_receipt = await find_open_draft(
                                    session,
                                    provider_id=provider.id,
                                    order_id=int(matched_order.id),
                                )
                                if draft_receipt is None:
                                    continue
                                (
                                    updated_rows,
                                    _draft_deleted,
                                ) = await (
                                    _consume_posted_quantities_from_open_draft(
                                        session,
                                        draft_receipt=draft_receipt,
                                        order=matched_order,
                                        linked_items_payload=(
                                            linked_items_payload
                                        ),
                                    )
                                )
                                if updated_rows <= 0:
                                    continue
                                updated_draft_receipt_ids.add(
                                    int(draft_receipt.id)
                                )
                            stats.updated_receipts += len(
                                updated_draft_receipt_ids
                            )
                        logger.info(
                            (
                                "Auto-posted single document receipt "
                                "from message %s: provider_id=%s "
                                "document=%s items=%s "
                                "(matched_orders=%s unmatched=%s)"
                            ),
                            message_row.id,
                            provider.id,
                            receipt_document_number or "—",
                            added_items,
                            len(all_orders_for_doc),
                            len(document_all_unmatched_rows),
                        )
            elif matched_orders:
                for matched_order in matched_orders.values():
                    order_key = int(matched_order.id)
                    applied_rows = receipt_applied_rows_by_order.get(
                        order_key,
                        [],
                    )
                    if order_key in auto_confirmed_order_ids:
                        receipt_items_payload = (
                            _build_pending_receipt_items(matched_order)
                        )
                    else:
                        receipt_items_payload = (
                            _build_receipt_items_from_applied_rows(
                                matched_order,
                                applied_rows,
                                cap_to_pending=False,
                            )
                        )
                        if (
                            not receipt_items_payload
                            and allow_text_status
                            and not parsed_response_file
                        ):
                            confirmed_items_payload = (
                                _build_confirmed_receipt_items(matched_order)
                            )
                            receipt_items_payload = confirmed_items_payload
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
                                response_config=active_response_config,
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
            ) and recognized_for_message <= 0 and not file_payload_is_document:
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
                elif (
                    parsed_response_file
                    and recognized_for_message <= 0
                    and not file_payload_is_document
                ):
                    is_import_error = True
            if expects_text_payload:
                if parsed_text_rows and recognized_for_message <= 0:
                    is_import_error = True
                elif not parsed_text_rows and not raw_status:
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
            reload_runtime_configs = True

    try:
        await _auto_confirm_orders_without_response_timeout(
            session,
            response_configs=response_configs,
            stats=stats,
        )
    except Exception as exc:
        await session.rollback()
        logger.error(
            "Failed to auto-confirm supplier orders by timeout: %s",
            exc,
            exc_info=True,
        )

    for customer_order_id in sorted(pending_customer_order_ids):
        try:
            await try_finalize_customer_order_response(
                session,
                order_id=customer_order_id,
            )
        except Exception as exc:
            await session.rollback()
            logger.error(
                "Failed to finalize customer order response order_id=%s: %s",
                customer_order_id,
                exc,
                exc_info=True,
            )

    result = stats.as_dict()
    logger.info(
        (
            "Supplier response processing finished: "
            "provider_id=%s config_id=%s fetched=%s processed=%s "
            "recognized=%s unresolved=%s created_receipts=%s "
            "updated_receipts=%s posted_receipts=%s "
            "timeout_auto_confirmed_orders=%s"
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
        result.get("timeout_auto_confirmed_orders", 0),
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
    if bool(getattr(config, "auto_confirm_unmentioned_items", False)):
        expectations.append(
            "Режим исключений: неуказанные позиции автоподтверждаются"
        )
    timeout_minutes = _safe_int(
        getattr(config, "auto_confirm_after_minutes", None)
    )
    if timeout_minutes and timeout_minutes > 0:
        expectations.append(
            (
                "Таймер авто-подтверждения без ответа: "
                f"{timeout_minutes} мин после отправки заказа"
            )
        )
    if expected_senders:
        expectations.append(
            f"Ожидаемый sender_email: {', '.join(expected_senders)}"
        )
    subject_pattern = str(
        getattr(config, "subject_pattern", "") or ""
    ).strip()
    if subject_pattern:
        expectations.append(
            f"Шаблон темы письма (regex): {subject_pattern}"
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


_MANUAL_SUPPLIER_MESSAGE_TYPES = {
    "UNKNOWN",
    "IMPORT_ERROR",
    "RESPONSE_FILE",
    "TEXT_RESPONSE",
    "SHIPPING_DOC",
    "STATUS",
    "IGNORED",
    "RETRY_PENDING",
}


def _normalize_suggested_message_type(value: object) -> Optional[str]:
    normalized = str(value or "").strip().upper()
    if normalized in _SUPPLIER_RESPONSE_AI_ALLOWED_TYPES:
        return normalized
    return None


def _normalize_suggestion_confidence(
    value: object,
    default: float = 0.5,
) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        parsed = default
    return min(max(parsed, 0.0), 1.0)


def _extract_text_from_ai_message_content(content: Any) -> str:
    if isinstance(content, str):
        return content.strip()
    if isinstance(content, list):
        chunks: list[str] = []
        for item in content:
            if isinstance(item, dict):
                text = str(item.get("text", "")).strip()
                if text:
                    chunks.append(text)
            else:
                text = str(item or "").strip()
                if text:
                    chunks.append(text)
        return "\n".join(chunks).strip()
    return str(content or "").strip()


def _extract_json_object(text: str) -> Optional[dict[str, Any]]:
    payload_text = str(text or "").strip()
    if not payload_text:
        return None
    try:
        parsed = json.loads(payload_text)
        if isinstance(parsed, dict):
            return parsed
    except (TypeError, ValueError):
        pass
    start = payload_text.find("{")
    end = payload_text.rfind("}")
    if start < 0 or end <= start:
        return None
    candidate = payload_text[start:end + 1]
    try:
        parsed = json.loads(candidate)
    except (TypeError, ValueError):
        return None
    if isinstance(parsed, dict):
        return parsed
    return None


def _guess_message_type_for_manager_rules(
    row: SupplierOrderMessage,
) -> dict[str, object]:
    attachment_kinds = {
        str(item.parsed_kind or "").strip().upper()
        for item in (row.attachments or [])
        if str(item.parsed_kind or "").strip()
    }
    if "SHIPPING_DOC" in attachment_kinds:
        return {
            "suggested_message_type": "SHIPPING_DOC",
            "suggested_confidence": 0.98,
            "suggested_explanation": (
                "Во вложениях уже определён тип SHIPPING_DOC."
            ),
            "suggested_source": "rules",
        }
    if "RESPONSE_FILE" in attachment_kinds:
        return {
            "suggested_message_type": "RESPONSE_FILE",
            "suggested_confidence": 0.95,
            "suggested_explanation": (
                "Во вложениях уже определён тип RESPONSE_FILE."
            ),
            "suggested_source": "rules",
        }
    preview = str(row.body_preview or "").strip()
    normalized_preview = normalize_external_status_text(preview)
    if preview and _ARTICLE_TOKEN_RE.search(preview):
        if any(
            token in normalized_preview
            for token in ("нет", "отказ", "будет", "есть", "в наличии")
        ):
            return {
                "suggested_message_type": "TEXT_RESPONSE",
                "suggested_confidence": 0.82,
                "suggested_explanation": (
                    "В тексте найден артикул и слова ответа поставщика."
                ),
                "suggested_source": "rules",
            }
        return {
            "suggested_message_type": "TEXT_RESPONSE",
            "suggested_confidence": 0.72,
            "suggested_explanation": (
                "В тексте найден артикул вида буквы+цифры."
            ),
            "suggested_source": "rules",
        }
    if str(row.raw_status or "").strip():
        return {
            "suggested_message_type": "STATUS",
            "suggested_confidence": 0.65,
            "suggested_explanation": (
                "Определён короткий статус из темы/текста письма."
            ),
            "suggested_source": "rules",
        }
    if str(row.import_error_details or "").strip():
        return {
            "suggested_message_type": "IMPORT_ERROR",
            "suggested_confidence": 0.60,
            "suggested_explanation": (
                "Письмо ранее завершилось ошибкой импорта."
            ),
            "suggested_source": "rules",
        }
    return {
        "suggested_message_type": "UNKNOWN",
        "suggested_confidence": 0.35,
        "suggested_explanation": (
            "Недостаточно признаков для уверенной классификации."
        ),
        "suggested_source": "rules",
    }


async def _guess_message_type_for_manager_ai(
    row: SupplierOrderMessage,
) -> Optional[dict[str, object]]:
    if not _SUPPLIER_RESPONSE_AI_CLASSIFIER_ENABLED:
        return None
    if not _SUPPLIER_RESPONSE_AI_CLASSIFIER_API_KEY:
        return None
    user_payload = {
        "sender_email": row.sender_email,
        "subject": _decode_mime_text(row.subject),
        "body_preview": str(row.body_preview or "")[:1500],
        "raw_status": row.raw_status,
        "current_message_type": row.message_type,
        "import_error_details": row.import_error_details,
        "attachments": [
            {
                "filename": _decode_mime_text(att.filename),
                "parsed_kind": att.parsed_kind,
            }
            for att in (row.attachments or [])
        ],
    }
    system_prompt = (
        "Ты классифицируешь входящее письмо поставщика. "
        "Верни JSON с полями: "
        "message_type (одно из UNKNOWN, IMPORT_ERROR, RESPONSE_FILE, "
        "TEXT_RESPONSE, SHIPPING_DOC, STATUS, IGNORED, RETRY_PENDING), "
        "confidence (0..1), explanation (кратко). "
        "Ответ только JSON без markdown."
    )
    payload = {
        "model": _SUPPLIER_RESPONSE_AI_CLASSIFIER_MODEL,
        "temperature": 0,
        "messages": [
            {"role": "system", "content": system_prompt},
            {
                "role": "user",
                "content": json.dumps(user_payload, ensure_ascii=False),
            },
        ],
    }
    url = (
        _SUPPLIER_RESPONSE_AI_CLASSIFIER_BASE_URL.rstrip("/")
        + "/chat/completions"
    )
    try:
        async with httpx.AsyncClient(
            timeout=_SUPPLIER_RESPONSE_AI_CLASSIFIER_TIMEOUT_SEC
        ) as client:
            response = await client.post(
                url,
                headers={
                    "Authorization": (
                        "Bearer "
                        + _SUPPLIER_RESPONSE_AI_CLASSIFIER_API_KEY
                    ),
                    "Content-Type": "application/json",
                },
                json=payload,
            )
        response.raise_for_status()
        data = response.json()
        choices = data.get("choices") or []
        if not choices:
            return None
        content = _extract_text_from_ai_message_content(
            choices[0].get("message", {}).get("content")
        )
        parsed = _extract_json_object(content)
        if not parsed:
            return None
        suggested_type = _normalize_suggested_message_type(
            parsed.get("message_type")
        )
        if not suggested_type:
            return None
        return {
            "suggested_message_type": suggested_type,
            "suggested_confidence": _normalize_suggestion_confidence(
                parsed.get("confidence"),
                default=0.55,
            ),
            "suggested_explanation": str(
                parsed.get("explanation")
                or "Классификация получена от AI-модуля."
            )[:500],
            "suggested_source": "ai",
        }
    except Exception as exc:
        logger.debug(
            "Supplier response AI classify failed for message_id=%s: %s",
            row.id,
            exc,
        )
        return None


async def list_supplier_response_messages_for_config(
    session: AsyncSession,
    *,
    provider_id: int,
    supplier_response_config_id: int,
    limit: int = 100,
    message_type: Optional[str] = None,
) -> list[dict[str, object]]:
    config = await session.get(
        SupplierResponseConfig,
        supplier_response_config_id,
    )
    if not config or int(config.provider_id) != int(provider_id):
        raise LookupError("Supplier response config not found")
    safe_limit = max(1, min(int(limit or 100), 300))
    normalized_filter = str(message_type or "").strip().upper()
    stmt = (
        select(SupplierOrderMessage)
        .options(selectinload(SupplierOrderMessage.attachments))
        .where(
            SupplierOrderMessage.provider_id == provider_id,
            (
                SupplierOrderMessage.response_config_id
                == supplier_response_config_id
            ),
        )
    )
    if normalized_filter:
        stmt = stmt.where(
            SupplierOrderMessage.message_type == normalized_filter
        )
    stmt = (
        stmt
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

    ai_budget = (
        _SUPPLIER_RESPONSE_AI_CLASSIFIER_MAX_PER_REQUEST
        if (
            _SUPPLIER_RESPONSE_AI_CLASSIFIER_ENABLED
            and _SUPPLIER_RESPONSE_AI_CLASSIFIER_API_KEY
        )
        else 0
    )
    result: list[dict[str, object]] = []
    for row in rows:
        suggestion = _guess_message_type_for_manager_rules(row)
        base_confidence = _normalize_suggestion_confidence(
            suggestion.get("suggested_confidence"),
            default=0.0,
        )
        if ai_budget > 0 and base_confidence < 0.95:
            ai_budget -= 1
            ai_suggestion = await _guess_message_type_for_manager_ai(row)
            if ai_suggestion:
                ai_confidence = _normalize_suggestion_confidence(
                    ai_suggestion.get("suggested_confidence"),
                    default=0.0,
                )
                if ai_confidence >= base_confidence:
                    suggestion = ai_suggestion

        account_id, source_folder, source_message_uid = (
            parsed_source_by_message.get(int(row.id), (None, None, None))
        )
        account = (
            account_by_id.get(account_id)
            if account_id is not None
            else None
        )
        subject_raw = row.subject
        subject = _decode_mime_text(subject_raw)
        attachment_details: list[str] = []
        for att in row.attachments or []:
            raw_name = str(att.filename or "").strip()
            decoded_name = _decode_mime_text(raw_name) or raw_name
            display_name = decoded_name or "<без имени>"
            kind = str(att.parsed_kind or "").strip()
            if kind:
                attachment_details.append(f"{display_name} [{kind}]")
            else:
                attachment_details.append(display_name)
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
                "attachment_details": attachment_details,
                "suggested_message_type": suggestion.get(
                    "suggested_message_type"
                ),
                "suggested_confidence": suggestion.get(
                    "suggested_confidence"
                ),
                "suggested_explanation": suggestion.get(
                    "suggested_explanation"
                ),
                "suggested_source": suggestion.get(
                    "suggested_source"
                ),
                "can_retry": bool(row.source_uid or row.source_message_id),
            }
        )
    return result


async def classify_supplier_response_message(
    session: AsyncSession,
    *,
    provider_id: int,
    supplier_response_config_id: int,
    message_id: int,
    message_type: str,
) -> dict[str, object]:
    config = await session.get(
        SupplierResponseConfig,
        supplier_response_config_id,
    )
    if not config or int(config.provider_id) != int(provider_id):
        raise LookupError("Supplier response config not found")
    message_row = await session.get(SupplierOrderMessage, message_id)
    if (
        message_row is None
        or int(message_row.provider_id) != int(provider_id)
        or int(message_row.response_config_id or 0)
        != int(supplier_response_config_id)
    ):
        raise LookupError("Supplier response message not found")
    normalized_type = str(message_type or "").strip().upper()
    if normalized_type not in _MANUAL_SUPPLIER_MESSAGE_TYPES:
        raise ValueError("Unsupported supplier response message_type")
    message_row.message_type = normalized_type
    if normalized_type != "IMPORT_ERROR":
        message_row.import_error_details = None
    session.add(message_row)
    await session.commit()
    return {
        "id": int(message_row.id),
        "message_type": message_row.message_type,
        "detail": "Классификация письма обновлена",
    }


async def retry_supplier_response_message_for_config(
    session: AsyncSession,
    *,
    provider_id: int,
    supplier_response_config_id: int,
    message_id: int,
) -> dict[str, object]:
    config = await session.get(
        SupplierResponseConfig,
        supplier_response_config_id,
    )
    if not config or int(config.provider_id) != int(provider_id):
        raise LookupError("Supplier response config not found")
    message_row = await session.get(SupplierOrderMessage, message_id)
    if (
        message_row is None
        or int(message_row.provider_id) != int(provider_id)
        or int(message_row.response_config_id or 0)
        != int(supplier_response_config_id)
    ):
        raise LookupError("Supplier response message not found")

    if not (message_row.source_uid or message_row.source_message_id):
        return {
            "config_id": supplier_response_config_id,
            "message_id": int(message_id),
            "queued": 0,
            "unretryable": 1,
            **SupplierResponseProcessingStats().as_dict(),
        }

    received_date = (
        message_row.received_at.date()
        if message_row.received_at is not None
        else supplier_response_cutoff()
    )
    message_row.source_uid = None
    message_row.source_message_id = None
    message_row.message_type = "RETRY_PENDING"
    session.add(message_row)
    await session.commit()

    retry_result = await process_supplier_response_messages(
        session=session,
        provider_id=provider_id,
        supplier_response_config_id=supplier_response_config_id,
        date_from=received_date,
        date_to=None,
    )
    return {
        "config_id": supplier_response_config_id,
        "message_id": int(message_id),
        "queued": 1,
        "unretryable": 0,
        **retry_result,
    }
