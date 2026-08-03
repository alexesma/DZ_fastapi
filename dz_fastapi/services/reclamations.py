"""Приём и первичная обработка рекламаций (претензий) от клиентов.

Этап 2: письма с ящика рекламаций читаем по IMAP (порт 993 открыт),
создаём Reclamation, определяем клиента по адресу отправителя,
сохраняем тело/вложения, извлекаем ссылки на порталы и базовые поля
(номер/дата документа, причина, артикулы) регулярками. AI-экстрактор
и движок проверки — следующие этапы.
"""
from __future__ import annotations

import hashlib
import html
import logging
import os
import re
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from html.parser import HTMLParser
from typing import Any, Optional

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from dz_fastapi.core.time import now_moscow
from dz_fastapi.models.autopart import AutoPart, preprocess_oem_number
from dz_fastapi.models.notification import AppNotification, AppNotificationLevel
from dz_fastapi.models.partner import (
    EMAIL_OUTBOX_STATUS,
    RECLAMATION_ATTACHMENT_KIND,
    RECLAMATION_ITEM_SOURCE,
    RECLAMATION_SOURCE,
    RECLAMATION_STATUS,
    RECLAMATION_TYPE,
    Customer,
    CustomerOrder,
    CustomerOrderItem,
    CustomerReclamationEmail,
    EmailOutbox,
    Provider,
    Reclamation,
    ReclamationAttachment,
    ReclamationItem,
    ReclamationMailboxState,
    ReclamationMailMessage,
)
from dz_fastapi.models.user import User, UserRole, UserStatus
from dz_fastapi.services.customer_return_ukd import create_customer_return_draft_from_reclamation
from dz_fastapi.services.notifications import create_notification, create_notifications_for_users
from dz_fastapi.services.reclamation_attachment_parser import parse_reclamation_attachment
from dz_fastapi.services.reclamation_audit import record_reclamation_event

logger = logging.getLogger("dz_fastapi")

RECLAMATION_ATTACHMENTS_DIR = os.path.join(
    "uploads", "reclamation_attachments"
)

_EMAIL_RE = re.compile(r"[\w.+-]+@[\w-]+\.[\w.-]+")
_URL_RE = re.compile(r"https?://[^\s<>\"')]+", re.IGNORECASE)
_FROZA_ITEM_FIELD_RE = re.compile(
    r"(?im)^\s*"
    r"(Товар|Артикул|Производитель|Количество|Причина возврата|Комментарий)"
    r"\s*:\s*(.*?)\s*$"
)
_FROZA_DOCUMENT_NUMBER_RE = re.compile(
    r"(?im)^\s*Номер\s+входящего\s+документа\s*:\s*(.*?)\s*$"
)
_FROZA_DOCUMENT_DATE_RE = re.compile(
    r"(?im)^\s*Дата\s+входящего\s+документа\s*:\s*(.*?)\s*$"
)
_GREENLIGHT_BOILERPLATE_MARKER = "основные причины формирования возвратов"
_GREENLIGHT_REASON_PATTERN = (
    r"(?:Отказ от товара по инициативе клиента|"
    r"НЕКОМПЛЕКТ(?:\s*\([^)]*\))?|"
    r"Штрихкод на товаре отсутствует(?:\s*\([^)]*\))?|"
    r"QR код на товаре(?:\s*\([^)]*\))?|"
    r"НЕТОВАРНЫЙ ВИД УПАКОВКИ(?:\s*\([^)]*\))?|"
    r"НЕКОНДИЦИЯ(?:\s*\([^)]*\))?|"
    r"Отсутствует/Повреждена контрольная марка ЧЗ|"
    r"Отказ от товара\s*-\s*Недовоз в поставке|"
    r"ЗАМЕНА НОМЕРА/БРЕНДА(?:\s*\([^)]*\))?)"
)
_GREENLIGHT_ROW_RE = re.compile(
    r"(?P<document_number>\d{1,30})\s+"
    r"(?P<document_date>\d{1,2}[./-]\d{1,2}[./-]\d{2,4})\s+"
    r"(?P<oem_number>[A-Za-zА-Яа-я0-9][A-Za-zА-Яа-я0-9./-]{4,})\s+"
    r"(?P<position_data>.+?)\s+"
    rf"(?P<reason>{_GREENLIGHT_REASON_PATTERN})"
    r"(?=\s+(?:\d{1,30}\s+\d{1,2}[./-]\d{1,2}[./-]\d{2,4})|$)",
    re.IGNORECASE,
)
_SHORTAGE_LINE_RE = re.compile(
    r"^\s*(?P<name>.+?)\s+"
    r"(?P<display_oem>[A-Za-zА-Яа-я0-9][A-Za-zА-Яа-я0-9./-]{4,})\s+"
    r"(?P<canonical_oem>[A-Za-zА-Яа-я0-9][A-Za-zА-Яа-я0-9./-]{4,})\s+"
    r"(?P<brand>[A-Za-zА-Яа-я][A-Za-zА-Яа-я0-9./-]{1,})\s*"
    r"[—–-]\s*(?P<quantity>\d+)\s*шт\.?\s*$",
    re.IGNORECASE | re.MULTILINE,
)
_INLINE_RETURN_WITH_BRAND_RE = re.compile(
    r"^\s*(?P<oem>[A-Za-zА-Яа-я0-9]"
    r"[A-Za-zА-Яа-я0-9./-]{4,})\s+"
    r"(?P<brand>[A-Za-zА-Яа-я][A-Za-zА-Яа-я0-9./-]{1,})\s+"
    r"(?P<name>[^\r\n]+?)\s*\r?\n"
    r"\s*в\s+количестве\s+(?P<quantity>\d+)"
    r"(?:\s*шт\.?)?\s*$",
    re.IGNORECASE | re.MULTILINE,
)
_INLINE_RETURN_QUANTITY_RE = re.compile(
    r"^\s*(?P<oem>[A-Za-zА-Яа-я0-9]"
    r"[A-Za-zА-Яа-я0-9./-]{4,})\s*"
    r"[-—–]\s*(?P<quantity>\d+)(?:\s*шт\.?)?\s*$",
    re.IGNORECASE | re.MULTILINE,
)
_INLINE_RETURN_REASON_RE = re.compile(
    r"^\s*Причина\s*[:—–-]\s*(?P<reason>[^\r\n]+)",
    re.IGNORECASE | re.MULTILINE,
)
_INLINE_RETURN_REASON_PHRASE_RE = re.compile(
    r"\bпо\s+причине\s+(?P<reason>[^\r\n]+)",
    re.IGNORECASE,
)
_INLINE_RETURN_SIMPLE_REASON_RE = re.compile(
    r"^\s*(?:>\s*)*(?P<reason>"
    r"Неверное вложение|Пересорт|Отказ клиента|Несоответствие товара"
    r")\s*$",
    re.IGNORECASE | re.MULTILINE,
)
_INLINE_PRODUCT_SENTENCE_RE = re.compile(
    r"\bпо\s+товару\s+"
    r"(?P<oem>[A-Za-zА-Яа-я0-9][A-Za-zА-Яа-я0-9./-]{4,})\s+"
    r"(?P<brand>[A-Za-zА-Яа-я][A-Za-zА-Яа-я0-9./-]{1,})\s+"
    r"(?P<details>[\s\S]{0,1200}?)"
    r"\bв\s+количестве\s+(?P<quantity>\d+)\s*шт\.?",
    re.IGNORECASE,
)
_INLINE_ARTICLE_QUANTITY_RE = re.compile(
    r"^\s*(?:>\s*)*Арт(?:икул)?\.?\s*[:№]?\s*"
    r"(?P<oem>[A-Za-zА-Яа-я0-9][A-Za-zА-Яа-я0-9./-]{4,})\s*"
    r"[—–-]\s*(?P<quantity>\d+)\s*шт\.?\s*$",
    re.IGNORECASE | re.MULTILINE,
)
# «№ УТ-1042», «номер УТ-1042 от 15.06.2026», «счёт 123 от 01.02.26»
_DOC_NUMBER_RE = re.compile(
    r"(?:№|номер|док(?:умент)?[а-я]*|сф|счет|счёт|накладн\w*|отгрузк\w*|"
    r"реализац\w*|утд?|уут?)\s*[:#№]?\s*([A-Za-zА-Яа-я0-9][\w\-/]{1,40})",
    re.IGNORECASE,
)
_DOC_DATE_RE = re.compile(
    r"от\s+(\d{1,2}[.\-/]\d{1,2}[.\-/]\d{2,4})",
    re.IGNORECASE,
)
# Причины: ищем по ключевым словам, определяем тип
_DEFECT_KEYWORDS = (
    "брак", "деформ", "трещин", "не работает", "неисправ", "течет",
    "течёт", "стук", "дефект", "скрип", "люфт", "гул",
    "несоответств",
)
_REFUSAL_KEYWORDS = (
    "отказ", "не подош", "не подходит", "не нужн", "передумал",
    "ошиб", "не тот", "перезаказ", "возврат",
)
_MIS_SORT_KEYWORDS = (
    "пересорт",
    "неверное вложение",
)

# Классификация вложений по имени файла
_ATTACHMENT_KIND_PATTERNS = (
    (RECLAMATION_ATTACHMENT_KIND.REMOVAL_ORDER,
     ("снят", "демонтаж", "removal")),
    (RECLAMATION_ATTACHMENT_KIND.INSTALLATION_ORDER,
     ("установ", "монтаж", "install")),
    (RECLAMATION_ATTACHMENT_KIND.DEFECT_REPORT,
     ("дефект", "акт", "defect")),
    (RECLAMATION_ATTACHMENT_KIND.PHOTO,
     (".jpg", ".jpeg", ".png", ".heic", "фото", "photo", "img")),
)


@dataclass
class ReclamationInboundAttachment:
    filename: Optional[str]
    payload: bytes
    content_type: Optional[str] = None


@dataclass
class ReclamationInboundEmail:
    from_: str
    subject: str = ""
    body_text: str = ""
    body_html: str = ""
    message_id: Optional[str] = None
    in_reply_to: Optional[str] = None
    references: Optional[str] = None
    received_at: Optional[datetime] = None
    uid: Optional[str] = None
    email_account_id: Optional[int] = None
    folder: Optional[str] = None
    attachments: list[ReclamationInboundAttachment] = field(
        default_factory=list
    )


def extract_sender_email(raw: str) -> str:
    m = _EMAIL_RE.search(str(raw or ""))
    return m.group(0).lower() if m else ""


def extract_links(text: str) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    decoded_text = html.unescape(str(text or ""))
    for m in _URL_RE.finditer(decoded_text):
        url = m.group(0).rstrip(".,);")
        if url not in seen:
            seen.add(url)
            result.append(url)
    return result


_MESSAGE_ID_RE = re.compile(r"<[^<>\s]+>")


def _message_ids(*values: Optional[str]) -> set[str]:
    result: set[str] = set()
    for value in values:
        raw = str(value or "").strip()
        if not raw:
            continue
        matches = _MESSAGE_ID_RE.findall(raw)
        if matches:
            result.update(matches)
        else:
            result.update(part for part in raw.split() if part)
    return result


def _normalized_thread_subject(value: Optional[str]) -> str:
    subject = html.unescape(str(value or "")).strip()
    subject = re.sub(
        r"^(?:(?:re|fw|fwd|ответ)\s*:\s*)+",
        "",
        subject,
        flags=re.IGNORECASE,
    )
    return re.sub(r"\s+", " ", subject).casefold()


def _looks_like_thread_followup(email: ReclamationInboundEmail) -> bool:
    subject = html.unescape(str(email.subject or "")).strip()
    if re.match(
        r"^(?:re|fw|fwd|ответ)\s*:",
        subject,
        flags=re.IGNORECASE,
    ):
        return True
    body = _plain_email_text(email.body_text or email.body_html)
    head = re.sub(r"\s+", " ", body[:2000]).casefold()
    return any(
        marker in head
        for marker in (
            "ответ ожидаем",
            "ожидаем ответ",
            "напоминаем",
            "повторно направляем",
            "повторный запрос",
            "есть ли решение",
            "просим дать ответ",
        )
    )


def _header_value(headers: dict, *names: str) -> Optional[str]:
    for name in names:
        value = headers.get(name)
        if isinstance(value, (tuple, list)):
            value = " ".join(str(item) for item in value if item)
        if value:
            return str(value)
    return None


def extract_froza_email_item(text: str) -> Optional[dict[str, Any]]:
    """Извлекает единственную позицию из стандартного письма Froza."""
    normalized = html.unescape(str(text or "")).replace("\xa0", " ")
    normalized = re.sub(
        r"(?i)<br\s*/?>|</(?:p|div|li|tr|td|h[1-6])\s*>",
        "\n",
        normalized,
    )
    normalized = re.sub(r"<[^>]+>", "", normalized)
    fields = {
        match.group(1).strip().casefold(): match.group(2).strip()
        for match in _FROZA_ITEM_FIELD_RE.finditer(normalized)
        if match.group(2).strip()
    }
    oem_number = preprocess_oem_number(fields.get("артикул") or "")
    quantity_match = re.search(r"\d+", fields.get("количество") or "")
    if not oem_number or quantity_match is None:
        return None
    quantity = int(quantity_match.group(0))
    if quantity <= 0:
        return None
    return {
        "oem_number": oem_number,
        "brand_name": fields.get("производитель") or None,
        "autopart_name": fields.get("товар") or None,
        "quantity": quantity,
        "reason": fields.get("причина возврата") or None,
        "comment": fields.get("комментарий") or None,
    }


def _plain_email_text(text: str) -> str:
    normalized = html.unescape(str(text or "")).replace("\xa0", " ")
    normalized = re.sub(
        r"(?is)<(?:style|script)\b[^>]*>.*?</(?:style|script)\s*>",
        " ",
        normalized,
    )
    normalized = re.sub(
        r"(?i)<br\s*/?>|</(?:p|div|li|tr|td|th|h[1-6])\s*>",
        "\n",
        normalized,
    )
    return re.sub(r"<[^>]+>", "", normalized)


class _HtmlTableRowsParser(HTMLParser):
    """Collects visible cells without treating CSS as email content."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.rows: list[list[str]] = []
        self._table_depth = 0
        self._row: Optional[list[str]] = None
        self._cell_parts: Optional[list[str]] = None

    def handle_starttag(
        self,
        tag: str,
        attrs: list[tuple[str, Optional[str]]],
    ) -> None:
        tag = tag.casefold()
        if tag == "table":
            self._table_depth += 1
        elif tag == "tr" and self._table_depth:
            self._row = []
        elif (
            tag in {"td", "th"}
            and self._table_depth
            and self._row is not None
        ):
            self._cell_parts = []
        elif tag == "br" and self._cell_parts is not None:
            self._cell_parts.append("\n")

    def handle_data(self, data: str) -> None:
        if self._cell_parts is not None:
            self._cell_parts.append(data)

    def handle_endtag(self, tag: str) -> None:
        tag = tag.casefold()
        if tag in {"td", "th"} and self._cell_parts is not None:
            value = re.sub(
                r"\s+",
                " ",
                "".join(self._cell_parts).replace("\xa0", " "),
            ).strip()
            if self._row is not None:
                self._row.append(value)
            self._cell_parts = None
        elif tag == "tr" and self._row is not None:
            if any(self._row):
                self.rows.append(self._row)
            self._row = None
            self._cell_parts = None
        elif tag == "table" and self._table_depth:
            self._table_depth -= 1


def _return_table_column(value: str) -> Optional[str]:
    normalized = re.sub(
        r"[^a-zа-я0-9]+",
        "",
        str(value or "").casefold().replace("ё", "е"),
    )
    if normalized in {"артикул", "oem", "номердетали"}:
        return "oem_number"
    if "производител" in normalized or normalized == "бренд":
        return "brand_name"
    if (
        "наименован" in normalized
        or "номенклатур" in normalized
        or normalized == "товар"
    ):
        return "autopart_name"
    if normalized in {"колво", "количество", "количествошт"}:
        return "quantity"
    if "номернакладн" in normalized or "номердокумент" in normalized:
        return "document_number"
    if "дата" in normalized and (
        "накладн" in normalized or "документ" in normalized
    ):
        return "document_date"
    return None


def extract_html_return_table_items(text: str) -> list[dict[str, Any]]:
    """Extracts rows from return tables with explicit article columns."""
    source = str(text or "")
    if "<table" not in source.casefold():
        return []

    parser = _HtmlTableRowsParser()
    try:
        parser.feed(source)
        parser.close()
    except Exception:  # noqa: BLE001
        logger.debug(
            "Failed to parse reclamation HTML table",
            exc_info=True,
        )
        return []

    header_map: dict[int, str] = {}
    items_by_oem: dict[str, dict[str, Any]] = {}
    for row in parser.rows:
        detected = {
            index: column
            for index, value in enumerate(row)
            if (column := _return_table_column(value))
        }
        detected_columns = set(detected.values())
        if (
            "oem_number" in detected_columns
            and "quantity" in detected_columns
            and detected_columns
            & {"brand_name", "autopart_name"}
        ):
            header_map = detected
            continue
        if not header_map:
            continue

        values = {
            column: row[index].strip()
            for index, column in header_map.items()
            if index < len(row) and row[index].strip()
        }
        oem_number = preprocess_oem_number(
            values.get("oem_number") or ""
        )
        quantity_match = re.search(
            r"\d+(?:[.,]\d+)?",
            values.get("quantity") or "",
        )
        if not oem_number or quantity_match is None:
            continue
        quantity = int(
            float(quantity_match.group(0).replace(",", "."))
        )
        if quantity <= 0:
            continue

        document_date = _parse_email_date(
            values.get("document_date") or ""
        )
        item = {
            "oem_number": oem_number,
            "brand_name": values.get("brand_name") or None,
            "autopart_name": values.get("autopart_name") or None,
            "quantity": quantity,
            "document_number": (
                values.get("document_number") or None
            ),
            "document_date": (
                document_date.isoformat() if document_date else None
            ),
        }
        existing = items_by_oem.get(oem_number)
        if existing is None:
            items_by_oem[oem_number] = item
        else:
            existing["quantity"] = (
                int(existing.get("quantity") or 0) + quantity
            )
    return list(items_by_oem.values())


def _parse_email_date(value: str) -> Optional[date]:
    normalized = str(value or "").replace("/", ".").replace("-", ".")
    for date_format in ("%d.%m.%Y", "%d.%m.%y"):
        try:
            return datetime.strptime(normalized, date_format).date()
        except ValueError:
            continue
    return None


def extract_greenlight_return_items(text: str) -> list[dict[str, Any]]:
    """Разбирает строки возврата до справочного блока письма Гринлайт."""
    plain_text = _plain_email_text(text)
    if (
        "номер документа поступления" not in plain_text.casefold()
        or "причина возврата" not in plain_text.casefold()
    ):
        return []
    operational_text = re.split(
        _GREENLIGHT_BOILERPLATE_MARKER,
        plain_text,
        maxsplit=1,
        flags=re.IGNORECASE,
    )[0]
    compact_text = re.sub(r"\s+", " ", operational_text).strip()

    result: list[dict[str, Any]] = []
    for match in _GREENLIGHT_ROW_RE.finditer(compact_text):
        position_data = match.group("position_data").strip()
        amount_match = re.match(
            r"^(?P<label>.+?)\s+"
            r"(?P<quantity>\d+)\s+"
            r"(?P<amount>\d[\d\s]*(?:[.,]\d{1,2})?)$",
            position_data,
        )
        if amount_match is None:
            continue
        label_parts = amount_match.group("label").split()
        if len(label_parts) < 2:
            continue
        quantity = int(amount_match.group("quantity"))
        oem_number = preprocess_oem_number(match.group("oem_number"))
        if not oem_number or quantity <= 0:
            continue
        document_date = _parse_email_date(match.group("document_date"))
        amount_raw = re.sub(r"\s+", "", amount_match.group("amount"))
        try:
            amount = float(amount_raw.replace(",", "."))
        except ValueError:
            amount = None
        result.append(
            {
                "document_number": match.group("document_number"),
                "document_date": (
                    document_date.isoformat() if document_date else None
                ),
                "oem_number": oem_number,
                "autopart_name": " ".join(label_parts[:-1]),
                "brand_name": label_parts[-1],
                "quantity": quantity,
                "total_amount": amount,
                "reason": match.group("reason").strip(),
            }
        )
    return result


def extract_shortage_items(text: str) -> list[dict[str, Any]]:
    """Извлекает позиции из блока «Недовоз» в письме клиента."""
    plain_text = _plain_email_text(text)
    match = re.search(r"(?im)^\s*Недовоз\s*:\s*$", plain_text)
    if match is None:
        return []
    shortage_block = plain_text[match.end():]
    shortage_block = re.split(
        r"(?im)^\s*(?:ООО|ИП|С уважением\b)",
        shortage_block,
        maxsplit=1,
    )[0]

    items: list[dict[str, Any]] = []
    for row in _SHORTAGE_LINE_RE.finditer(shortage_block):
        display_oem = preprocess_oem_number(row.group("display_oem"))
        canonical_oem = preprocess_oem_number(row.group("canonical_oem"))
        oem_number = canonical_oem or display_oem
        quantity = int(row.group("quantity"))
        if not oem_number or quantity <= 0:
            continue
        items.append(
            {
                "oem_number": oem_number,
                "brand_name": row.group("brand").strip(),
                "autopart_name": row.group("name").strip(" .,:;"),
                "quantity": quantity,
                "reason": "Недовоз",
            }
        )
    return items


def extract_inline_return_items(text: str) -> list[dict[str, Any]]:
    """Извлекает явно записанные позиции из свободного текста письма."""
    plain_text = _plain_email_text(text)
    reason_match = (
        _INLINE_RETURN_REASON_RE.search(plain_text)
        or _INLINE_RETURN_REASON_PHRASE_RE.search(plain_text)
        or _INLINE_RETURN_SIMPLE_REASON_RE.search(plain_text)
    )
    reason = (
        reason_match.group("reason").strip(" .,:;")
        if reason_match
        else None
    )
    items_by_oem: dict[str, dict[str, Any]] = {}

    for row in _INLINE_PRODUCT_SENTENCE_RE.finditer(plain_text):
        oem_number = preprocess_oem_number(row.group("oem"))
        quantity = int(row.group("quantity"))
        if not oem_number or quantity <= 0:
            continue
        details = row.group("details").strip(" .,:;")
        parsed_reason = reason
        if "несоответств" in details.casefold():
            parsed_reason = "Несоответствие товара"
        items_by_oem[oem_number] = {
            "oem_number": oem_number,
            "brand_name": row.group("brand").strip(),
            "quantity": quantity,
            "reason": parsed_reason,
        }

    for row in _INLINE_ARTICLE_QUANTITY_RE.finditer(plain_text):
        oem_number = preprocess_oem_number(row.group("oem"))
        quantity = int(row.group("quantity"))
        if not oem_number or quantity <= 0:
            continue
        items_by_oem[oem_number] = {
            "oem_number": oem_number,
            "quantity": quantity,
            "reason": reason,
        }

    for row in _INLINE_RETURN_WITH_BRAND_RE.finditer(plain_text):
        oem_number = preprocess_oem_number(row.group("oem"))
        quantity = int(row.group("quantity"))
        if not oem_number or quantity <= 0:
            continue
        items_by_oem[oem_number] = {
            "oem_number": oem_number,
            "brand_name": row.group("brand").strip(),
            "autopart_name": row.group("name").strip(" .,:;"),
            "quantity": quantity,
            "reason": reason,
        }

    for row in _INLINE_RETURN_QUANTITY_RE.finditer(plain_text):
        oem_number = preprocess_oem_number(row.group("oem"))
        quantity = int(row.group("quantity"))
        if not oem_number or quantity <= 0:
            continue
        current = items_by_oem.get(oem_number, {})
        items_by_oem[oem_number] = {
            **current,
            "oem_number": oem_number,
            "quantity": quantity,
            "reason": current.get("reason") or reason,
        }

    return list(items_by_oem.values())


def _plain_oem_candidate_text(text: str) -> str:
    """Убирает служебные фрагменты, которые часто похожи на OEM."""
    plain_text = _plain_email_text(text)
    plain_text = re.sub(
        r"(?i)\[?\s*cid:[^\s\]<>\"']+\]?",
        " ",
        plain_text,
    )
    plain_text = re.sub(
        r"(?i)\bimage\d+\.(?:jpe?g|png|gif|bmp|webp|tiff?)\b",
        " ",
        plain_text,
    )
    plain_text = re.sub(r"https?://\S+|www\.\S+", " ", plain_text)
    plain_text = re.sub(
        r"\b[\w.+-]+@[\w.-]+\.[A-Za-zА-Яа-я]{2,}\b",
        " ",
        plain_text,
    )
    plain_text = re.sub(
        r"\b\d{1,2}[./-]\d{1,2}[./-]\d{2,4}\b",
        " ",
        plain_text,
    )
    plain_text = re.sub(r"\b\d{1,2}:\d{2}(?::\d{2})?\b", " ", plain_text)
    plain_text = re.sub(
        r"(?i)\b(?:сф|сч[её]т|накладн\w*|документ\w*|акт\w*)"
        r"\s*(?:№|#)?\s*[A-Za-zА-Яа-я0-9/-]+",
        " ",
        plain_text,
    )
    return plain_text


def classify_reclamation_service_email(
    *,
    sender: str,
    subject: str,
    body: str,
) -> str | None:
    """Определяет служебные письма, которые не являются рекламациями."""
    sender_text = str(sender or "").casefold()
    subject_text = _plain_email_text(subject or "").casefold()
    body_text = _plain_email_text(body or "").casefold()
    combined = f"{subject_text}\n{body_text}"

    if (
        re.search(r"\bупд\s*№?\s*[a-zа-яё0-9/-]+", combined, re.IGNORECASE)
        and "успешно загружено в базу" in combined
    ):
        return "document_delivery_confirmation"

    if (
        "акт сверки" in combined
        or "сверка взаиморасчетов" in combined
        or (
            sender_text.startswith("sverka@")
            and "сверк" in subject_text
        )
    ):
        return "reconciliation_statement"

    return None


def apply_froza_email_item(reclamation: Reclamation) -> bool:
    """Исправляет техническое количество 1 по сохранённому письму Froza."""
    parsed = extract_froza_email_item(reclamation.email_body or "")
    if parsed is None:
        return False
    parsed_oem = preprocess_oem_number(parsed["oem_number"])
    matching_items = [
        item
        for item in (reclamation.items or [])
        if preprocess_oem_number(item.oem_number or "") == parsed_oem
    ]
    if len(matching_items) != 1:
        return False

    item = matching_items[0]
    changed = False
    if int(item.quantity or 0) != int(parsed["quantity"]):
        item.quantity = int(parsed["quantity"])
        changed = True
    for field_name in ("brand_name", "autopart_name", "reason"):
        parsed_value = parsed.get(field_name)
        if parsed_value and not getattr(item, field_name, None):
            setattr(item, field_name, parsed_value)
            changed = True
    return changed


def _preferred_source_link(links: list[str]) -> Optional[str]:
    """Prefer actionable portal links over generic site/account links."""
    for link in links:
        lowered = link.lower()
        if "froza.ru/supplier/one-question/" in lowered:
            return link
    for link in links:
        lowered = link.lower()
        if "srm.armtek.ru/returns-management/" in lowered:
            return link
    return links[0] if links else None


def _parse_doc_date(text: str) -> Optional[date]:
    m = _DOC_DATE_RE.search(str(text or ""))
    if not m:
        return None
    raw = m.group(1).replace("/", ".").replace("-", ".")
    for fmt in ("%d.%m.%Y", "%d.%m.%y"):
        try:
            return datetime.strptime(raw, fmt).date()
        except ValueError:
            continue
    return None


RECLAMATION_TYPE_DEFECT = "defect"
RECLAMATION_TYPE_REFUSAL = "customer_refusal"
RECLAMATION_TYPE_SHORTAGE = "shortage"
RECLAMATION_TYPE_MIS_SORT = "mis_sort"


def classify_reclamation_type(text: str) -> Optional[str]:
    lowered = str(text or "").lower()
    operational_text = lowered.split(
        _GREENLIGHT_BOILERPLATE_MARKER,
        1,
    )[0]
    if "недовоз" in operational_text or "недопостав" in operational_text:
        return RECLAMATION_TYPE_SHORTAGE
    if any(kw in operational_text for kw in _MIS_SORT_KEYWORDS):
        return RECLAMATION_TYPE_MIS_SORT
    if any(kw in operational_text for kw in _DEFECT_KEYWORDS):
        return RECLAMATION_TYPE_DEFECT
    if any(kw in operational_text for kw in _REFUSAL_KEYWORDS):
        return RECLAMATION_TYPE_REFUSAL
    return None


def _structured_attachment_reason(
    extracted_data: Optional[dict[str, Any]],
) -> str:
    """Return the explicit reason parsed from an attachment, if available."""
    for parsed in (extracted_data or {}).get("attachments") or []:
        reason = str(parsed.get("reason") or "").strip()
        if reason:
            return reason
        for item in parsed.get("items") or []:
            reason = str(item.get("reason") or "").strip()
            if reason:
                return reason
    return ""


def extract_fields(subject: str, body: str) -> dict[str, Any]:
    """Регулярное извлечение полей из письма (первый слой распознавания)."""
    text = f"{subject}\n{body}"
    froza_item = extract_froza_email_item(body)
    if froza_item is not None:
        plain_body = _plain_email_text(body)
        number_match = _FROZA_DOCUMENT_NUMBER_RE.search(plain_body)
        date_match = _FROZA_DOCUMENT_DATE_RE.search(plain_body)
        document_date = _parse_email_date(
            date_match.group(1).strip() if date_match else ""
        )
        return {
            "document_number": (
                number_match.group(1).strip(" .,:;")
                if number_match
                else None
            ),
            "document_date": (
                document_date.isoformat() if document_date else None
            ),
            "reclamation_type": classify_reclamation_type(
                froza_item.get("reason") or ""
            ),
            "links": extract_links(body),
            "froza_email_item": froza_item,
        }
    html_table_items = extract_html_return_table_items(body)
    if html_table_items:
        first_item = html_table_items[0]
        return {
            "document_number": first_item.get("document_number"),
            "document_date": first_item.get("document_date"),
            "reclamation_type": classify_reclamation_type(text),
            "links": extract_links(body),
            "html_table_items": html_table_items,
        }
    shortage_items = extract_shortage_items(body)
    if shortage_items:
        doc_match = _DOC_NUMBER_RE.search(text)
        doc_number = (
            doc_match.group(1).strip(" .,:;") if doc_match else None
        )
        doc_date = _parse_doc_date(text)
        return {
            "document_number": doc_number,
            "document_date": doc_date.isoformat() if doc_date else None,
            "reclamation_type": RECLAMATION_TYPE_SHORTAGE,
            "links": extract_links(body),
            "shortage_items": shortage_items,
        }
    greenlight_items = extract_greenlight_return_items(body)
    if greenlight_items:
        first_item = greenlight_items[0]
        return {
            "document_number": first_item.get("document_number"),
            "document_date": first_item.get("document_date"),
            "reclamation_type": classify_reclamation_type(
                first_item.get("reason") or ""
            ),
            "links": extract_links(body),
            "greenlight_items": greenlight_items,
        }
    inline_items = extract_inline_return_items(body)
    if inline_items:
        doc_match = _DOC_NUMBER_RE.search(text)
        doc_number = (
            doc_match.group(1).strip(" .,:;") if doc_match else None
        )
        doc_date = _parse_doc_date(text)
        first_item = inline_items[0]
        return {
            "document_number": doc_number,
            "document_date": doc_date.isoformat() if doc_date else None,
            "reclamation_type": classify_reclamation_type(
                first_item.get("reason") or text
            ),
            "links": extract_links(body),
            "inline_items": inline_items,
        }
    doc_number = None
    m = _DOC_NUMBER_RE.search(text)
    if m:
        candidate = m.group(1).strip(" .,:;")
        # Отсекаем слишком короткие/мусорные
        if len(candidate) >= 2 and not candidate.isalpha():
            doc_number = candidate
    doc_date = _parse_doc_date(text)
    rec_type = classify_reclamation_type(text)
    return {
        "document_number": doc_number,
        "document_date": doc_date.isoformat() if doc_date else None,
        "reclamation_type": rec_type,
        "links": extract_links(body),
    }


async def _match_oems_in_text(
    session: AsyncSession,
    text: str,
    limit: int = 10,
    customer_id: Optional[int] = None,
    customer_ids: Optional[list[int]] = None,
) -> list[dict[str, Any]]:
    """Ищет артикулы в номенклатуре и прошлых заказах клиента."""
    text = _plain_oem_candidate_text(text)
    tokens = {
        preprocess_oem_number(tok)
        for tok in re.findall(r"[A-Za-z0-9][A-Za-z0-9\-./]{3,}", text or "")
    }
    tokens = {t for t in tokens if t and len(t) >= 4}
    if not tokens:
        return []
    rows = (
        await session.execute(
            select(
                AutoPart.id,
                AutoPart.oem_number,
                AutoPart.name,
            ).where(AutoPart.oem_number.in_(list(tokens)))
            .limit(limit)
        )
    ).all()
    result = [
        {
            "autopart_id": int(r[0]),
            "oem_number": r[1],
            "autopart_name": r[2],
            "brand_name": None,
        }
        for r in rows
    ]
    matched_oems = {str(item["oem_number"]) for item in result}
    remaining = tokens - matched_oems

    if remaining:
        normalized_order_oem = func.upper(
            func.regexp_replace(
                CustomerOrderItem.oem,
                "[^A-Za-z0-9]",
                "",
                "g",
            )
        )
        order_stmt = (
            select(
                CustomerOrderItem.autopart_id,
                normalized_order_oem.label("oem_number"),
                CustomerOrderItem.name,
                CustomerOrderItem.brand,
            )
            .join(
                CustomerOrder,
                CustomerOrder.id == CustomerOrderItem.order_id,
            )
            .where(normalized_order_oem.in_(remaining))
            .order_by(
                CustomerOrder.received_at.desc(),
                CustomerOrderItem.id.desc(),
            )
        )
        scoped_customer_ids = {
            int(value)
            for value in (customer_ids or [])
            if value is not None
        }
        if customer_id is not None:
            scoped_customer_ids.add(int(customer_id))
        if scoped_customer_ids:
            order_stmt = order_stmt.where(
                CustomerOrder.customer_id.in_(scoped_customer_ids)
            )
        order_rows = (await session.execute(order_stmt)).all()
        for row in order_rows:
            normalized = str(row.oem_number or "")
            if not normalized or normalized in matched_oems:
                continue
            result.append(
                {
                    "autopart_id": (
                        int(row.autopart_id) if row.autopart_id else None
                    ),
                    "oem_number": normalized,
                    "autopart_name": row.name,
                    "brand_name": row.brand,
                }
            )
            matched_oems.add(normalized)
            if len(result) >= limit:
                break

    # Смешанный буквенно-цифровой номер достаточно характерен для артикула.
    # Сохраняем его для ручной проверки, даже если номенклатура ещё не создана.
    for token in sorted(tokens - matched_oems):
        if not (re.search(r"[A-Z]", token) and re.search(r"\d", token)):
            continue
        if len(token) < 6 or len(token) > 24:
            continue
        if len(re.findall(r"\d", token)) < 3:
            continue
        result.append(
            {
                "autopart_id": None,
                "oem_number": token,
                "autopart_name": None,
                "brand_name": None,
            }
        )
        if len(result) >= limit:
            break

    return result


async def _refresh_saved_attachment_extractions(
    session: AsyncSession,
    reclamation: Reclamation,
) -> list[dict[str, Any]]:
    """Повторно применяет актуальные парсеры к сохранённым Excel-файлам."""
    extracted_data = dict(reclamation.extracted_data or {})
    extractions = [
        dict(item)
        for item in (extracted_data.get("attachments") or [])
        if isinstance(item, dict)
    ]
    rows = (
        await session.execute(
            select(ReclamationAttachment).where(
                ReclamationAttachment.reclamation_id == reclamation.id
            )
        )
    ).scalars().all()
    changed = False
    for attachment in rows:
        filename = str(attachment.file_name or "")
        if os.path.splitext(filename)[1].casefold() not in {".xls", ".xlsx"}:
            continue
        file_path = str(attachment.local_file_path or "").strip()
        if not file_path or not os.path.isfile(file_path):
            continue
        try:
            with open(file_path, "rb") as file_handle:
                payload = file_handle.read()
            parsed = parse_reclamation_attachment(filename, payload)
        except OSError as exc:
            logger.warning(
                "Не удалось повторно прочитать вложение рекламации #%s %s: %s",
                reclamation.id,
                filename,
                exc,
            )
            continue
        if not parsed:
            continue
        parsed["source_sha256"] = hashlib.sha256(payload).hexdigest()
        matching_index = next(
            (
                index
                for index, current in enumerate(extractions)
                if current.get("filename") == filename
            ),
            None,
        )
        if matching_index is None:
            extractions.append(parsed)
        elif extractions[matching_index] != parsed:
            extractions[matching_index] = parsed
        else:
            continue
        changed = True
    if changed:
        extracted_data["attachments"] = extractions
        reclamation.extracted_data = extracted_data
        session.add(reclamation)
        await session.flush()
    return extractions


async def recognize_reclamation_items(
    session: AsyncSession,
    reclamation: Reclamation,
) -> int:
    """Дополняет позиции карточки по сохранённому письму и истории заказов."""
    attachment_extractions = await _refresh_saved_attachment_extractions(
        session,
        reclamation,
    )
    await session.refresh(reclamation, attribute_names=["items"])
    attachment_reason = _structured_attachment_reason(
        reclamation.extracted_data
    )
    attachment_items = [
        item
        for extraction in attachment_extractions
        for item in (extraction.get("items") or [])
        if preprocess_oem_number(item.get("oem_number") or "")
    ]
    froza_item = extract_froza_email_item(reclamation.email_body or "")
    structured_item_updated = apply_froza_email_item(reclamation)
    html_table_items = extract_html_return_table_items(
        reclamation.email_body or ""
    )
    greenlight_items = extract_greenlight_return_items(
        reclamation.email_body or ""
    )
    shortage_items = extract_shortage_items(reclamation.email_body or "")
    inline_items = extract_inline_return_items(reclamation.email_body or "")
    # Стандартное письмо Froza содержит ровно одну позицию. Общий поиск чисел
    # здесь недопустим: номер входящего документа может совпасть с OEM в базе.
    structured_items = (
        ([froza_item] if froza_item is not None else [])
        or attachment_items
        or html_table_items
        or shortage_items
        or greenlight_items
        or inline_items
    )
    matched = await _match_oems_in_text(
        session,
        f"{reclamation.email_subject or ''}\n{reclamation.email_body or ''}",
        customer_id=reclamation.customer_id,
    )
    resolved_by_oem = {
        preprocess_oem_number(item["oem_number"]): item
        for item in matched
    }
    created = 0
    if structured_items:
        expected_oems = {
            preprocess_oem_number(item["oem_number"])
            for item in structured_items
        }
        for existing_item in list(reclamation.items or []):
            existing_oem = preprocess_oem_number(
                existing_item.oem_number or ""
            )
            item_source = str(
                getattr(
                    existing_item.item_source,
                    "value",
                    existing_item.item_source,
                )
            )
            is_unlinked_auto_item = (
                item_source == RECLAMATION_ITEM_SOURCE.UNKNOWN.value
                and existing_item.shipment_item_id is None
                and existing_item.stock_lot_id is None
                and existing_item.source_provider_id is None
            )
            if existing_oem not in expected_oems and is_unlinked_auto_item:
                reclamation.items.remove(existing_item)
                structured_item_updated = True

        existing_by_oem = {
            preprocess_oem_number(item.oem_number or ""): item
            for item in (reclamation.items or [])
            if item.oem_number
        }
        for parsed_item in structured_items:
            normalized = preprocess_oem_number(parsed_item["oem_number"])
            existing_item = existing_by_oem.get(normalized)
            resolved_item = resolved_by_oem.get(normalized, {})
            if existing_item is None:
                existing_item = ReclamationItem(
                    oem_number=normalized,
                    brand_name=(
                        parsed_item.get("brand_name")
                        or resolved_item.get("brand_name")
                    ),
                    autopart_name=(
                        resolved_item.get("autopart_name")
                        or parsed_item.get("autopart_name")
                    ),
                    autopart_id=resolved_item.get("autopart_id"),
                    quantity=parsed_item.get("quantity") or 1,
                    reason=parsed_item.get("reason"),
                    item_source=RECLAMATION_ITEM_SOURCE.UNKNOWN,
                )
                reclamation.items.append(existing_item)
                existing_by_oem[normalized] = existing_item
                created += 1
                continue

            updates = {
                "quantity": parsed_item.get("quantity") or 1,
                "reason": parsed_item.get("reason"),
            }
            if not existing_item.brand_name:
                updates["brand_name"] = (
                    parsed_item.get("brand_name")
                    or resolved_item.get("brand_name")
                )
            if not existing_item.autopart_name:
                updates["autopart_name"] = (
                    resolved_item.get("autopart_name")
                    or parsed_item.get("autopart_name")
                )
            if existing_item.autopart_id is None:
                updates["autopart_id"] = resolved_item.get("autopart_id")
            for field_name, value in updates.items():
                if value is not None and getattr(
                    existing_item, field_name
                ) != value:
                    setattr(existing_item, field_name, value)
                    structured_item_updated = True

        fields = extract_fields(
            reclamation.email_subject or "",
            reclamation.email_body or "",
        )
        first_item = structured_items[0]
        reclamation.stated_document_number = fields.get("document_number")
        document_date = fields.get("document_date")
        reclamation.stated_document_date = (
            date.fromisoformat(document_date) if document_date else None
        )
        preferred_reason = attachment_reason or str(
            first_item.get("reason") or ""
        ).strip()
        reclamation.stated_reason = preferred_reason or None
        reclamation.reclamation_type = (
            classify_reclamation_type(preferred_reason)
            or fields.get("reclamation_type")
        )
        extracted_data = dict(reclamation.extracted_data or {})
        extracted_key = (
            "froza_email_item"
            if froza_item is not None
            else "attachment_items"
            if attachment_items
            else "html_table_items"
            if html_table_items
            else "shortage_items"
            if shortage_items
            else "greenlight_items"
            if greenlight_items
            else "inline_items"
        )
        extracted_data[extracted_key] = (
            froza_item if froza_item is not None else structured_items
        )
        reclamation.extracted_data = extracted_data
        structured_item_updated = True
    else:
        document_oem = preprocess_oem_number(
            reclamation.stated_document_number or ""
        )
        if document_oem:
            for existing_item in list(reclamation.items or []):
                item_source = str(
                    getattr(
                        existing_item.item_source,
                        "value",
                        existing_item.item_source,
                    )
                )
                if (
                    preprocess_oem_number(existing_item.oem_number or "")
                    == document_oem
                    and item_source
                    == RECLAMATION_ITEM_SOURCE.UNKNOWN.value
                    and existing_item.shipment_item_id is None
                    and existing_item.stock_lot_id is None
                    and existing_item.source_provider_id is None
                ):
                    reclamation.items.remove(existing_item)
                    structured_item_updated = True
        existing_oems = {
            preprocess_oem_number(item.oem_number or "")
            for item in (reclamation.items or [])
            if item.oem_number
        }
        for item in matched:
            normalized = preprocess_oem_number(item["oem_number"] or "")
            if (
                not normalized
                or normalized == document_oem
                or normalized in existing_oems
            ):
                continue
            reclamation.items.append(
                ReclamationItem(
                    oem_number=normalized,
                    brand_name=item.get("brand_name"),
                    autopart_name=item.get("autopart_name"),
                    autopart_id=item.get("autopart_id"),
                    quantity=1,
                    item_source=RECLAMATION_ITEM_SOURCE.UNKNOWN,
                )
            )
            existing_oems.add(normalized)
            created += 1
    if attachment_reason:
        attachment_type = classify_reclamation_type(attachment_reason)
        if reclamation.stated_reason != attachment_reason:
            reclamation.stated_reason = attachment_reason
            structured_item_updated = True
        if attachment_type and reclamation.reclamation_type != attachment_type:
            reclamation.reclamation_type = attachment_type
            structured_item_updated = True

    return_document_extraction = next(
        (
            extraction
            for extraction in attachment_extractions
            if extraction.get("parser") in {
                "customer_return_upd_xlsx",
                "torg2_xls",
            }
            and extraction.get("items")
        ),
        None,
    )
    if return_document_extraction is not None:
        await create_customer_return_draft_from_reclamation(
            session,
            reclamation=reclamation,
            extraction=return_document_extraction,
        )

    if created or structured_item_updated:
        session.add(reclamation)
        await session.flush()
    return created


async def resolve_customer_by_email(
    session: AsyncSession,
    sender_email: str,
) -> Optional[int]:
    """Возвращает клиента, только если адрес соответствует одному юрлицу."""
    customer_ids = await resolve_customer_ids_by_email(
        session,
        sender_email,
    )
    return customer_ids[0] if len(customer_ids) == 1 else None


async def resolve_customer_ids_by_email(
    session: AsyncSession,
    sender_email: str,
) -> list[int]:
    """Возвращает все юрлица, использующие адрес для рекламаций."""
    email = str(sender_email or "").strip().lower()
    if not email:
        return []
    rows = (
        await session.execute(
            select(CustomerReclamationEmail.customer_id).where(
                CustomerReclamationEmail.email == email
            )
        )
    ).scalars().all()
    customer_ids = {int(row) for row in rows}
    contact_rows = (
        await session.execute(
            select(Customer.id).where(
                func.lower(Customer.email_contact) == email
            )
        )
    ).scalars().all()
    customer_ids.update(int(row) for row in contact_rows)
    return sorted(customer_ids)


def _structured_items_from_fields(
    fields: dict[str, Any],
) -> list[dict[str, Any]]:
    froza_item = fields.get("froza_email_item")
    if isinstance(froza_item, dict):
        return [froza_item]
    return list(
        fields.get("html_table_items")
        or fields.get("shortage_items")
        or fields.get("greenlight_items")
        or fields.get("inline_items")
        or []
    )


async def _resolve_customer_from_orders(
    session: AsyncSession,
    *,
    customer_ids: list[int],
    oem_numbers: set[str],
    document_number: Optional[str],
) -> Optional[int]:
    """Выбирает юрлицо только при однозначном совпадении заказа."""
    if len(customer_ids) == 1:
        return customer_ids[0]
    if not customer_ids:
        return None

    document = str(document_number or "").strip()
    if document:
        document_customer_ids = set(
            (
                await session.execute(
                    select(CustomerOrder.customer_id)
                    .where(
                        CustomerOrder.customer_id.in_(customer_ids),
                        func.upper(CustomerOrder.order_number)
                        == document.upper(),
                    )
                    .distinct()
                )
            ).scalars().all()
        )
        if len(document_customer_ids) == 1:
            return int(next(iter(document_customer_ids)))

    normalized_oems = {
        preprocess_oem_number(value) for value in oem_numbers if value
    }
    normalized_oems.discard("")
    if not normalized_oems:
        return None
    normalized_order_oem = func.upper(
        func.regexp_replace(
            CustomerOrderItem.oem,
            "[^A-Za-z0-9]",
            "",
            "g",
        )
    )
    item_customer_ids = set(
        (
            await session.execute(
                select(CustomerOrder.customer_id)
                .join(
                    CustomerOrderItem,
                    CustomerOrderItem.order_id == CustomerOrder.id,
                )
                .where(
                    CustomerOrder.customer_id.in_(customer_ids),
                    normalized_order_oem.in_(normalized_oems),
                )
                .distinct()
            )
        ).scalars().all()
    )
    if len(item_customer_ids) == 1:
        return int(next(iter(item_customer_ids)))
    return None


def classify_attachment_kind(filename: Optional[str]) -> str:
    name = str(filename or "").lower()
    for kind, patterns in _ATTACHMENT_KIND_PATTERNS:
        if any(p in name for p in patterns):
            return kind.value
    return RECLAMATION_ATTACHMENT_KIND.OTHER.value


def _save_attachment_to_disk(
    reclamation_id: int,
    attachment: ReclamationInboundAttachment,
) -> str:
    safe_name = re.sub(
        r"[^A-Za-z0-9._-]", "_", attachment.filename or "attachment.bin"
    )
    rel_dir = os.path.join(
        RECLAMATION_ATTACHMENTS_DIR, str(reclamation_id)
    )
    os.makedirs(rel_dir, exist_ok=True)
    rel_path = os.path.join(rel_dir, safe_name)
    with open(rel_path, "wb") as fh:
        fh.write(attachment.payload or b"")
    return rel_path


async def _find_thread_reclamation(
    session: AsyncSession,
    *,
    email: ReclamationInboundEmail,
    sender_email: str,
    fields: dict[str, Any],
) -> Optional[Reclamation]:
    reference_ids = _message_ids(email.in_reply_to, email.references)
    if reference_ids:
        variants = set(reference_ids)
        variants.update(value.strip("<>") for value in reference_ids)
        referenced = (
            await session.execute(
                select(Reclamation)
                .where(Reclamation.email_message_id.in_(variants))
                .options(selectinload(Reclamation.items))
                .order_by(Reclamation.id.desc())
                .limit(1)
            )
        ).scalar_one_or_none()
        if referenced is not None:
            return referenced

    normalized_subject = _normalized_thread_subject(email.subject)
    structured_oems = {
        preprocess_oem_number(item.get("oem_number") or "")
        for item in _structured_items_from_fields(fields)
    }
    structured_oems.discard("")
    if not sender_email or not structured_oems:
        return None

    candidates = (
        (
            await session.execute(
                select(Reclamation)
                .where(Reclamation.sender_email == sender_email)
                .options(selectinload(Reclamation.items))
                .order_by(Reclamation.email_received_at.desc().nullslast())
                .limit(50)
            )
        )
        .scalars()
        .all()
    )
    received_date = (
        email.received_at.date() if email.received_at is not None else None
    )
    document_number = str(fields.get("document_number") or "").strip()
    document_matches: list[Reclamation] = []
    followup_matches: list[Reclamation] = []
    is_followup = _looks_like_thread_followup(email)
    for candidate in candidates:
        candidate_date = (
            candidate.email_received_at.date()
            if candidate.email_received_at is not None
            else None
        )
        if (
            received_date is not None
            and candidate_date is not None
            and abs((received_date - candidate_date).days) > 60
        ):
            continue
        candidate_oems = {
            preprocess_oem_number(item.oem_number or "")
            for item in (candidate.items or [])
        }
        if not structured_oems & candidate_oems:
            continue
        if (
            normalized_subject
            and _normalized_thread_subject(candidate.email_subject)
            == normalized_subject
        ):
            return candidate
        candidate_document = str(
            candidate.stated_document_number or ""
        ).strip()
        if (
            document_number
            and candidate_document
            and candidate_document.casefold() == document_number.casefold()
        ):
            document_matches.append(candidate)
        if is_followup:
            followup_matches.append(candidate)
    if len(document_matches) == 1:
        return document_matches[0]
    if len(followup_matches) == 1:
        return followup_matches[0]
    return None


async def _append_thread_message(
    session: AsyncSession,
    *,
    reclamation: Reclamation,
    email: ReclamationInboundEmail,
) -> bool:
    extracted_data = dict(reclamation.extracted_data or {})
    thread_messages = list(extracted_data.get("thread_messages") or [])
    message_id = str(email.message_id or "").strip() or None
    if message_id and any(
        str(item.get("message_id") or "").strip() == message_id
        for item in thread_messages
    ):
        return False

    readable_body = email.body_text or _plain_email_text(email.body_html)
    thread_messages.append(
        {
            "direction": "incoming",
            "message_id": message_id,
            "in_reply_to": email.in_reply_to,
            "references": email.references,
            "from_email": extract_sender_email(email.from_),
            "subject": email.subject or None,
            "received_at": (
                email.received_at.isoformat() if email.received_at else None
            ),
            "body": readable_body[:50000] or None,
        }
    )
    extracted_data["thread_messages"] = thread_messages[-50:]
    reclamation.extracted_data = extracted_data
    session.add(reclamation)

    for attachment in email.attachments or []:
        try:
            path = _save_attachment_to_disk(reclamation.id, attachment)
        except Exception:  # noqa: BLE001
            logger.warning(
                "Не удалось сохранить вложение ответа по рекламации #%s",
                reclamation.id,
            )
            path = None
        session.add(
            ReclamationAttachment(
                reclamation_id=reclamation.id,
                kind=classify_attachment_kind(attachment.filename),
                file_name=attachment.filename,
                content_type=attachment.content_type,
                local_file_path=path,
                size_bytes=len(attachment.payload or b""),
            )
        )

    await record_reclamation_event(
        session,
        reclamation_id=int(reclamation.id),
        event_type="thread_message_received",
        details={
            "from_email": extract_sender_email(email.from_),
            "subject": email.subject,
            "message_id": message_id,
            "attachments_count": len(email.attachments or []),
        },
    )
    notify_user_ids: set[int] = set()
    if reclamation.shortage_assigned_to_user_id:
        notify_user_ids.add(int(reclamation.shortage_assigned_to_user_id))
    role_user_ids = (
        await session.execute(
            select(User.id).where(
                User.status == UserStatus.ACTIVE,
                User.role.in_(
                    [UserRole.ADMIN, UserRole.RECLAMATION]
                ),
            )
        )
    ).scalars().all()
    notify_user_ids.update(int(user_id) for user_id in role_user_ids)
    await create_notifications_for_users(
        session,
        user_ids=notify_user_ids,
        title=f"Новый ответ по рекламации #{reclamation.id}",
        message=(
            f"{extract_sender_email(email.from_) or 'Клиент'}: "
            f"{email.subject or 'без темы'}"
        ),
        level=AppNotificationLevel.INFO,
        link=f"/reclamations?openId={reclamation.id}",
        payload={
            "notification_type": "reclamation_thread_reply",
            "reclamation_id": int(reclamation.id),
        },
        commit=False,
    )
    await session.commit()

    if email.attachments and reclamation.status == RECLAMATION_STATUS.WAITING_DOCS:
        try:
            from dz_fastapi.services.reclamation_check import run_reclamation_check

            await run_reclamation_check(
                session,
                reclamation_id=int(reclamation.id),
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "Не удалось автоматически перепроверить рекламацию #%s "
                "после получения документов: %s",
                reclamation.id,
                exc,
            )
    return True


async def ingest_reclamation_email(
    session: AsyncSession,
    email: ReclamationInboundEmail,
) -> Optional[Reclamation]:
    """Создаёт рекламацию из письма (идемпотентно по Message-ID)."""
    body_for_extraction = "\n".join(
        part for part in (email.body_text, email.body_html) if part
    )
    fields = extract_fields(email.subject, body_for_extraction)
    source_link = _preferred_source_link(fields.get("links") or [])
    sender = extract_sender_email(email.from_)

    if email.message_id:
        existing = (
            await session.execute(
                select(Reclamation).where(
                    Reclamation.email_message_id == email.message_id
                )
            )
        ).scalar_one_or_none()
        if existing is not None:
            normalized_existing_link = html.unescape(
                str(existing.source_link or "")
            ).strip()
            link_changed = (
                bool(normalized_existing_link)
                and normalized_existing_link != existing.source_link
            )
            if link_changed:
                existing.source_link = normalized_existing_link
            if not existing.source_link and source_link:
                existing.source_link = source_link
                link_changed = True
            mailbox_changed = False
            if email.uid:
                extracted_data = dict(existing.extracted_data or {})
                mailbox_data = dict(extracted_data.get("mailbox") or {})
                next_mailbox_data = {
                    **mailbox_data,
                    "email_account_id": email.email_account_id,
                    "folder": email.folder or "INBOX",
                    "uid": str(email.uid),
                }
                if next_mailbox_data != mailbox_data:
                    extracted_data["mailbox"] = next_mailbox_data
                    existing.extracted_data = extracted_data
                    mailbox_changed = True
            if link_changed:
                extracted_data = dict(existing.extracted_data or {})
                extracted_data["links"] = fields.get("links") or []
                existing.extracted_data = extracted_data
            if link_changed or mailbox_changed:
                session.add(existing)
                await session.commit()
                logger.info(
                    "Обновлены данные исходного письма рекламации #%s",
                    existing.id,
                )
            mailbox_status = (
                (existing.extracted_data or {})
                .get("mailbox", {})
                .get("answered_flag_status")
            )
            if email.uid and mailbox_status != "marked":
                from dz_fastapi.models.partner import EMAIL_OUTBOX_STATUS, EmailOutbox
                from dz_fastapi.services.email_outbox import mark_reclamation_source_answered

                sent_reply = (
                    await session.execute(
                        select(EmailOutbox)
                        .where(
                            EmailOutbox.source_type == "reclamation",
                            EmailOutbox.source_id == existing.id,
                            EmailOutbox.status
                            == EMAIL_OUTBOX_STATUS.SENT,
                        )
                        .order_by(EmailOutbox.id.desc())
                        .limit(1)
                    )
                ).scalar_one_or_none()
                if sent_reply is not None:
                    await mark_reclamation_source_answered(
                        session,
                        reclamation_id=int(existing.id),
                        outbox_id=int(sent_reply.id),
                        from_email=sent_reply.from_email,
                        sent_at=sent_reply.sent_at,
                    )
            logger.debug(
                "Рекламация по письму %s уже создана (#%s)",
                email.message_id,
                existing.id,
            )
            return None

    thread_reclamation = await _find_thread_reclamation(
        session,
        email=email,
        sender_email=sender,
        fields=fields,
    )
    if thread_reclamation is not None:
        appended = await _append_thread_message(
            session,
            reclamation=thread_reclamation,
            email=email,
        )
        logger.info(
            "Письмо %s %s с рекламацией #%s как продолжение переписки",
            email.message_id or email.uid,
            "связано" if appended else "уже было связано",
            thread_reclamation.id,
        )
        return None

    customer_ids = await resolve_customer_ids_by_email(session, sender)
    attachment_extractions = []
    for attachment in email.attachments or []:
        parsed = parse_reclamation_attachment(
            attachment.filename,
            attachment.payload,
        )
        if not parsed:
            continue
        parsed["source_sha256"] = hashlib.sha256(
            attachment.payload or b""
        ).hexdigest()
        attachment_extractions.append(parsed)
    structured_items = _structured_items_from_fields(fields)
    customer_match_oems = {
        preprocess_oem_number(item.get("oem_number") or "")
        for item in structured_items
    }
    customer_match_oems.update(
        preprocess_oem_number(item.get("oem_number") or "")
        for parsed in attachment_extractions
        for item in (parsed.get("items") or [])
    )
    customer_match_oems.discard("")
    customer_id = await _resolve_customer_from_orders(
        session,
        customer_ids=customer_ids,
        oem_numbers=customer_match_oems,
        document_number=fields.get("document_number"),
    )
    attachment_document = next(
        (
            parsed
            for parsed in attachment_extractions
            if parsed.get("document_number")
        ),
        {},
    )
    attachment_reason = next(
        (
            str(parsed.get("reason") or "").strip()
            for parsed in attachment_extractions
            if parsed.get("reason")
        ),
        "",
    )
    document_number = (
        attachment_document.get("document_number")
        or fields.get("document_number")
    )
    document_date_value = (
        attachment_document.get("document_date")
        or fields.get("document_date")
    )
    doc_date = None
    if document_date_value:
        try:
            doc_date = date.fromisoformat(str(document_date_value))
        except ValueError:
            doc_date = None
    # The structured reason from a known attachment format is more reliable
    # than generic form headings such as "Брак" found in the sheet text.
    reclamation_type = (
        classify_reclamation_type(attachment_reason)
        if attachment_reason
        else None
    ) or fields.get("reclamation_type")

    extracted_data = dict(fields)
    if len(customer_ids) > 1:
        customer_rows = (
            await session.execute(
                select(Customer.id, Customer.name)
                .where(Customer.id.in_(customer_ids))
                .order_by(Customer.name, Customer.id)
            )
        ).all()
        extracted_data["customer_candidates"] = [
            {"id": int(row.id), "name": row.name}
            for row in customer_rows
        ]
        extracted_data["customer_resolution"] = (
            "matched_by_order" if customer_id else "ambiguous"
        )
    if attachment_extractions:
        extracted_data["attachments"] = attachment_extractions
    if email.uid:
        extracted_data["mailbox"] = {
            "email_account_id": email.email_account_id,
            "folder": email.folder or "INBOX",
            "uid": str(email.uid),
        }

    reclamation = Reclamation(
        source=RECLAMATION_SOURCE.EMAIL,
        status=(
            RECLAMATION_STATUS.RECOGNIZED
            if customer_id
            else RECLAMATION_STATUS.NEW
        ),
        reclamation_type=reclamation_type,
        customer_id=customer_id,
        sender_email=sender or None,
        source_link=source_link,
        email_message_id=email.message_id,
        email_subject=(email.subject or "")[:998] or None,
        email_received_at=email.received_at or now_moscow(),
        email_body=email.body_text or email.body_html or None,
        stated_document_number=document_number,
        stated_document_date=doc_date,
        stated_reason=(
            attachment_reason
            or next(
                (
                    str(item.get("reason") or "").strip()
                    for item in (
                        fields.get("html_table_items")
                        or fields.get("shortage_items")
                        or fields.get("greenlight_items")
                        or fields.get("inline_items")
                        or []
                    )
                    if item.get("reason")
                ),
                "",
            )
            or None
        ),
        extracted_data=extracted_data,
    )
    session.add(reclamation)
    await session.flush()

    # Позиции по найденным артикулам в тексте
    matched = await _match_oems_in_text(
        session,
        f"{email.subject}\n{body_for_extraction}",
        customer_id=customer_id,
        customer_ids=customer_ids,
    )
    if customer_id is None and len(customer_ids) > 1:
        customer_id = await _resolve_customer_from_orders(
            session,
            customer_ids=customer_ids,
            oem_numbers={
                preprocess_oem_number(item.get("oem_number") or "")
                for item in matched
            },
            document_number=document_number,
        )
        if customer_id is not None:
            reclamation.customer_id = customer_id
            reclamation.status = RECLAMATION_STATUS.RECOGNIZED
            extracted_data["customer_resolution"] = "matched_by_order"
            reclamation.extracted_data = extracted_data
    candidates = {
        preprocess_oem_number(item["oem_number"]): {
            **item,
            "quantity": 1,
            "reason": None,
        }
        for item in matched
        if preprocess_oem_number(item.get("oem_number") or "")
    }
    document_oem = preprocess_oem_number(document_number or "")
    if document_oem:
        candidates.pop(document_oem, None)
    if structured_items:
        resolved_by_oem = {
            preprocess_oem_number(item["oem_number"]): item
            for item in matched
        }
        candidates = {}
        for parsed_item in structured_items:
            normalized = preprocess_oem_number(
                parsed_item.get("oem_number") or ""
            )
            candidate = dict(resolved_by_oem.get(normalized, {}))
            candidate.update(
                {
                    key: value
                    for key, value in parsed_item.items()
                    if key
                    in {
                        "oem_number",
                        "brand_name",
                        "autopart_name",
                        "quantity",
                        "reason",
                    }
                    and value is not None
                }
            )
            candidates[normalized] = candidate
    froza_email_item = extract_froza_email_item(body_for_extraction)
    if froza_email_item is not None:
        normalized = preprocess_oem_number(froza_email_item["oem_number"])
        # Froza присылает одну структурированную позицию. Не смешиваем её с
        # общим поиском чисел, иначе номер документа может стать второй деталью.
        resolved_by_oem = {
            preprocess_oem_number(item["oem_number"]): item
            for item in matched
        }
        candidate = dict(resolved_by_oem.get(normalized, {}))
        candidate.update(
            {
                key: value
                for key, value in froza_email_item.items()
                if key not in {"comment", "autopart_name"}
                and value is not None
            }
        )
        candidate["autopart_name"] = (
            candidate.get("autopart_name")
            or froza_email_item.get("autopart_name")
        )
        candidates = {normalized: candidate}
        extracted_data = {
            **extracted_data,
            "froza_email_item": froza_email_item,
        }
        reclamation.extracted_data = extracted_data
        reclamation.stated_reason = froza_email_item.get("reason")
        reclamation.reclamation_type = (
            classify_reclamation_type(
                froza_email_item.get("reason") or ""
            )
            or reclamation.reclamation_type
        )

    attachment_items = [
        item
        for parsed in attachment_extractions
        for item in (parsed.get("items") or [])
        if preprocess_oem_number(item.get("oem_number") or "")
    ]
    if attachment_items:
        attachment_oems = "\n".join(
            str(item.get("oem_number") or "") for item in attachment_items
        )
        resolved_attachment_items = await _match_oems_in_text(
            session,
            attachment_oems,
            customer_id=customer_id,
            customer_ids=customer_ids,
        )
        resolved_by_oem = {
            preprocess_oem_number(item["oem_number"]): item
            for item in resolved_attachment_items
        }
        for parsed_item in attachment_items:
            normalized = preprocess_oem_number(
                parsed_item.get("oem_number") or ""
            )
            candidate = dict(resolved_by_oem.get(normalized, {}))
            candidate.update(
                {
                    key: value
                    for key, value in candidates.get(normalized, {}).items()
                    if value is not None
                }
            )
            candidate["oem_number"] = normalized
            for field_name in (
                "brand_name",
                "autopart_name",
                "reason",
            ):
                if parsed_item.get(field_name):
                    candidate[field_name] = parsed_item[field_name]
            candidate["quantity"] = max(
                1, int(parsed_item.get("quantity") or 1)
            )
            candidates[normalized] = candidate

    for item in candidates.values():
        session.add(
            ReclamationItem(
                reclamation_id=reclamation.id,
                oem_number=item["oem_number"],
                brand_name=item.get("brand_name"),
                autopart_name=item.get("autopart_name"),
                autopart_id=item.get("autopart_id"),
                quantity=item.get("quantity") or 1,
                reason=item.get("reason"),
                item_source=RECLAMATION_ITEM_SOURCE.UNKNOWN,
            )
        )

    await session.flush()
    return_document_extraction = next(
        (
            parsed
            for parsed in attachment_extractions
            if parsed.get("parser") in {
                "customer_return_upd_xlsx",
                "torg2_xls",
            }
            and parsed.get("items")
        ),
        None,
    )
    if return_document_extraction is not None:
        draft = await create_customer_return_draft_from_reclamation(
            session,
            reclamation=reclamation,
            extraction=return_document_extraction,
        )
        if draft is not None:
            await record_reclamation_event(
                session,
                reclamation_id=int(reclamation.id),
                event_type="ukd_draft_created",
                details={
                    "return_from_customer_id": int(draft.id),
                    "external_document_number": draft.external_document_number,
                    "source_document_number": draft.source_document_number,
                    "matched_shipment_document_id": draft.shipment_document_id,
                    "matched_source_upd_id": (
                        draft.source_diadoc_outgoing_document_id
                    ),
                },
            )

    # Вложения
    for att in email.attachments or []:
        try:
            path = _save_attachment_to_disk(reclamation.id, att)
        except Exception:  # noqa: BLE001
            logger.warning(
                "Не удалось сохранить вложение рекламации #%s",
                reclamation.id,
            )
            path = None
        session.add(
            ReclamationAttachment(
                reclamation_id=reclamation.id,
                kind=classify_attachment_kind(att.filename),
                file_name=att.filename,
                content_type=att.content_type,
                local_file_path=path,
                size_bytes=len(att.payload or b""),
            )
        )

    await record_reclamation_event(
        session,
        reclamation_id=int(reclamation.id),
        event_type="created_from_email",
        details={
            "sender_email": sender,
            "subject": email.subject,
            "items_count": len(candidates),
            "attachments_count": len(email.attachments or []),
        },
    )
    await session.commit()
    await session.refresh(reclamation)
    logger.info(
        "Создана рекламация #%s (клиент=%s, позиций=%s, вложений=%s)",
        reclamation.id,
        customer_id,
        len(candidates),
        len(email.attachments or []),
    )
    return reclamation


def _fetch_reclamation_imap_sync(
    host: str,
    email: str,
    password: str,
    folder: str,
    port: int,
    since_date: date,
    last_uid: int = 0,
    limit: int = 200,
) -> list[ReclamationInboundEmail]:
    """Читает ящик рекламаций (тело + Message-ID + вложения)."""
    from imap_tools import AND

    from dz_fastapi.services.email import _create_mailbox

    result: list[ReclamationInboundEmail] = []
    mb = _create_mailbox(host, port, True).login(email, password)
    with mb as mailbox:
        mailbox.folder.set(folder)
        criteria = (
            f"UID {int(last_uid) + 1}:*"
            if int(last_uid or 0) > 0
            else AND(date_gte=since_date, all=True)
        )
        for msg in mailbox.fetch(
            criteria,
            charset="utf-8",
            mark_seen=False,
        ):
            if len(result) >= max(1, int(limit)):
                break
            sender = str(getattr(msg, "from_", "") or "").lower()
            if extract_sender_email(sender) == str(email or "").lower():
                continue  # пропускаем свои же исходящие
            attachments = [
                ReclamationInboundAttachment(
                    filename=att.filename,
                    payload=att.payload,
                    content_type=getattr(att, "content_type", None),
                )
                for att in (msg.attachments or [])
            ]
            headers = getattr(msg, "headers", {}) or {}
            message_id = _header_value(
                headers,
                "message-id",
                "Message-ID",
            )
            in_reply_to = _header_value(
                headers,
                "in-reply-to",
                "In-Reply-To",
            )
            references = _header_value(
                headers,
                "references",
                "References",
            )
            result.append(
                ReclamationInboundEmail(
                    from_=str(getattr(msg, "from_", "") or ""),
                    subject=str(getattr(msg, "subject", "") or ""),
                    body_text=str(getattr(msg, "text", None) or ""),
                    body_html=str(getattr(msg, "html", None) or ""),
                    message_id=message_id or (
                        str(msg.uid) if msg.uid else None
                    ),
                    in_reply_to=in_reply_to,
                    references=references,
                    received_at=getattr(msg, "date", None),
                    uid=str(msg.uid) if msg.uid else None,
                    attachments=attachments,
                )
            )
    return result


async def fetch_reclamation_emails(
    account,
    days: int = 7,
    *,
    last_uid: int = 0,
    limit: int = 200,
) -> list[ReclamationInboundEmail]:
    """IMAP-чтение ящика рекламаций (в отдельном потоке)."""
    import asyncio

    host = getattr(account, "imap_host", None)
    if not host:
        raise RuntimeError(
            "У почтового ящика рекламаций не указан IMAP-сервер"
        )
    since = (now_moscow() - timedelta(days=days)).date()
    account_id = int(account.id)
    account_email = str(account.email)
    folder = (getattr(account, "imap_folder", None) or "INBOX").strip()
    try:
        emails = await asyncio.wait_for(
            asyncio.to_thread(
                _fetch_reclamation_imap_sync,
                host,
                account.email,
                account.password,
                folder,
                int(getattr(account, "imap_port", 993) or 993),
                since,
                int(last_uid or 0),
                int(limit),
            ),
            timeout=max(
                30,
                int(
                    os.getenv(
                        "RECLAMATION_IMAP_TIMEOUT_SEC",
                        "180",
                    )
                ),
            ),
        )
        for item in emails:
            item.email_account_id = account_id
            item.folder = folder
            if item.message_id == item.uid and item.uid:
                item.message_id = (
                    f"imap:{account_id}:{folder}:{item.uid}"
                )
        return emails
    except Exception as exc:  # noqa: BLE001
        logger.error(
            "Ошибка чтения ящика рекламаций id=%s: %s",
            account_id,
            exc,
        )
        raise RuntimeError(
            "Не удалось прочитать почтовый ящик рекламаций "
            f"{account_email}: {exc}"
        ) from exc


async def create_manual_reclamation(
    session: AsyncSession,
    *,
    customer_id: Optional[int],
    sender_email: Optional[str] = None,
    subject: Optional[str] = None,
    body: Optional[str] = None,
    source_link: Optional[str] = None,
) -> Reclamation:
    """Ручное заведение рекламации (когда пришла не письмом)."""
    fields = extract_fields(subject or "", body or "")
    reclamation = Reclamation(
        source=(
            RECLAMATION_SOURCE.LINK
            if source_link
            else RECLAMATION_SOURCE.MANUAL
        ),
        status=(
            RECLAMATION_STATUS.RECOGNIZED
            if customer_id
            else RECLAMATION_STATUS.NEW
        ),
        reclamation_type=fields.get("reclamation_type"),
        customer_id=customer_id,
        sender_email=(sender_email or None),
        source_link=source_link,
        email_subject=(subject or None),
        email_body=(body or None),
        email_received_at=now_moscow(),
        stated_document_number=fields.get("document_number"),
        extracted_data=fields,
    )
    session.add(reclamation)
    await session.flush()
    matched = await _match_oems_in_text(
        session,
        f"{subject or ''}\n{body or ''}",
        customer_id=customer_id,
    )
    for item in matched:
        session.add(
            ReclamationItem(
                reclamation_id=reclamation.id,
                oem_number=item["oem_number"],
                brand_name=item.get("brand_name"),
                autopart_name=item["autopart_name"],
                autopart_id=item["autopart_id"],
                quantity=1,
                item_source=RECLAMATION_ITEM_SOURCE.UNKNOWN,
            )
        )
    await session.commit()
    await session.refresh(reclamation)
    return reclamation


async def get_reclamations_summary(session: AsyncSession) -> dict[str, Any]:
    status_rows = (
        await session.execute(
            select(Reclamation.status, func.count()).group_by(
                Reclamation.status
            )
        )
    ).all()
    by_status = {
        str(getattr(st, "value", st)): int(cnt) for st, cnt in status_rows
    }
    without_customer = (
        await session.execute(
            select(func.count())
            .select_from(Reclamation)
            .where(Reclamation.customer_id.is_(None))
        )
    ).scalar() or 0
    return {
        "total": sum(by_status.values()),
        "by_status": by_status,
        "without_customer": int(without_customer),
    }


async def list_reclamations(
    session: AsyncSession,
    *,
    status: Optional[str] = None,
    customer_id: Optional[int] = None,
    without_customer: bool = False,
    order: str = "newest",
    limit: int = 100,
) -> list[dict[str, Any]]:
    requested_statuses = {
        part.strip() for part in str(status or "").split(",") if part.strip()
    }
    if not requested_statuses or RECLAMATION_STATUS.WAITING_SUPPLIER in requested_statuses:
        await _restore_waiting_supplier_statuses(session)
    # Для ручных заявок дата письма может отсутствовать, поэтому используем
    # дату создания как резервную и id для стабильного порядка.
    sort_date = func.coalesce(
        Reclamation.email_received_at,
        Reclamation.created_at,
    )
    order_by = (
        (sort_date.asc(), Reclamation.id.asc())
        if str(order or "newest").lower() == "oldest"
        else (sort_date.desc(), Reclamation.id.desc())
    )
    stmt = (
        select(Reclamation, Customer.name)
        .outerjoin(Customer, Customer.id == Reclamation.customer_id)
        .order_by(*order_by)
        .limit(max(1, min(int(limit or 100), 500)))
    )
    if status:
        # Допускаем несколько статусов через запятую (для очереди-этапов)
        statuses = [
            part.strip() for part in str(status).split(",") if part.strip()
        ]
        if len(statuses) == 1:
            stmt = stmt.where(Reclamation.status == statuses[0])
        elif statuses:
            stmt = stmt.where(Reclamation.status.in_(statuses))
    if customer_id is not None:
        stmt = stmt.where(Reclamation.customer_id == int(customer_id))
    if without_customer:
        stmt = stmt.where(Reclamation.customer_id.is_(None))
    rows = (await session.execute(stmt)).all()
    result: list[dict[str, Any]] = []
    for rec, customer_name in rows:
        if (rec.extracted_data or {}).get("service_mail_type"):
            continue
        result.append(
            {
                "id": int(rec.id),
                "source": str(getattr(rec.source, "value", rec.source)),
                "status": str(getattr(rec.status, "value", rec.status)),
                "reclamation_type": (
                    str(
                        getattr(
                            rec.reclamation_type,
                            "value",
                            rec.reclamation_type,
                        )
                    )
                    if rec.reclamation_type
                    else None
                ),
                "customer_id": rec.customer_id,
                "customer_name": customer_name,
                "sender_email": rec.sender_email,
                "email_subject": rec.email_subject,
                "email_received_at": rec.email_received_at,
                "stated_document_number": rec.stated_document_number,
                "stated_document_date": rec.stated_document_date,
                "recommendation": rec.recommendation,
                "resolution": rec.resolution,
                "items_count": len(rec.items or []),
                "attachments_count": len(rec.attachments or []),
                "created_at": rec.created_at,
            }
        )
    return result


async def _restore_waiting_supplier_statuses(session: AsyncSession) -> int:
    """Восстанавливает этап по уже созданной очереди писем поставщикам."""
    reclamation_ids = (
        await session.execute(
            select(EmailOutbox.source_id)
            .where(
                EmailOutbox.source_type == "reclamation_supplier",
                EmailOutbox.status.in_(
                    [EMAIL_OUTBOX_STATUS.PENDING, EMAIL_OUTBOX_STATUS.SENT]
                ),
                EmailOutbox.source_id.is_not(None),
            )
            .distinct()
        )
    ).scalars().all()
    if not reclamation_ids:
        return 0
    rows = (
        await session.execute(
            select(Reclamation).where(
                Reclamation.id.in_(reclamation_ids),
                Reclamation.status.in_(
                    [
                        RECLAMATION_STATUS.NEW,
                        RECLAMATION_STATUS.RECOGNIZED,
                        RECLAMATION_STATUS.CHECKED,
                        RECLAMATION_STATUS.WAITING_DOCS,
                    ]
                ),
            )
        )
    ).scalars().all()
    for rec in rows:
        rec.status = RECLAMATION_STATUS.WAITING_SUPPLIER
        session.add(rec)
    if rows:
        await session.commit()
    return len(rows)


async def assign_reclamation_customer(
    session: AsyncSession,
    *,
    reclamation_id: int,
    customer_id: int,
    remember_email: bool = False,
) -> Reclamation:
    rec = await session.get(Reclamation, reclamation_id)
    if rec is None:
        raise ValueError("Рекламация не найдена")
    customer = await session.get(Customer, customer_id)
    if customer is None:
        raise ValueError("Клиент не найден")
    rec.customer_id = customer_id
    if rec.status == RECLAMATION_STATUS.NEW:
        rec.status = RECLAMATION_STATUS.RECOGNIZED
    # Запомнить адрес отправителя за клиентом
    if remember_email and rec.sender_email:
        email = rec.sender_email.strip().lower()
        exists = (
            await session.execute(
                select(CustomerReclamationEmail.id).where(
                    CustomerReclamationEmail.customer_id == customer_id,
                    CustomerReclamationEmail.email == email,
                )
            )
        ).scalar_one_or_none()
        if exists is None:
            session.add(
                CustomerReclamationEmail(
                    customer_id=customer_id,
                    email=email,
                    comment="Добавлено из рекламации",
                )
            )
    session.add(rec)
    await session.commit()
    await session.refresh(rec)
    return rec


_VALID_STATUSES = {s.value for s in RECLAMATION_STATUS}
_VALID_TYPES = {t.value for t in RECLAMATION_TYPE}
_VALID_ITEM_SOURCES = {s.value for s in RECLAMATION_ITEM_SOURCE}
# Решения, которые автоматически проставляют финальный статус
_RESOLUTION_STATUS = {
    "approved": RECLAMATION_STATUS.APPROVED,
    "rejected": RECLAMATION_STATUS.REJECTED,
}


async def update_reclamation(
    session: AsyncSession,
    *,
    reclamation_id: int,
    status: Optional[str] = None,
    reclamation_type: Optional[str] = None,
    resolution: Optional[str] = None,
    resolution_comment: Optional[str] = None,
    resolved_by_user_id: Optional[int] = None,
) -> Reclamation:
    """Меняет статус/тип/решение рекламации (действия из карточки)."""
    rec = await session.get(Reclamation, reclamation_id)
    if rec is None:
        raise ValueError("Рекламация не найдена")

    if reclamation_type is not None:
        if reclamation_type and reclamation_type not in _VALID_TYPES:
            raise ValueError(f"Недопустимый тип: {reclamation_type}")
        rec.reclamation_type = reclamation_type or None

    if resolution is not None:
        if (
            resolution == "rejected"
            and not str(resolution_comment or "").strip()
        ):
            raise ValueError("Для отказа обязательно укажите причину")
        rec.resolution = resolution or None
        rec.resolution_comment = (
            str(resolution_comment).strip()
            if resolution_comment is not None
            else None
        )
        if resolution:
            rec.resolved_at = now_moscow()
            rec.resolved_by_user_id = resolved_by_user_id
            # Решение задаёт финальный статус, если статус явно не передан
            if status is None and resolution in _RESOLUTION_STATUS:
                rec.status = _RESOLUTION_STATUS[resolution]
        else:
            rec.resolved_at = None
            rec.resolved_by_user_id = None
    elif resolution_comment is not None:
        rec.resolution_comment = resolution_comment

    if status is not None:
        if status not in _VALID_STATUSES:
            raise ValueError(f"Недопустимый статус: {status}")
        rec.status = status

    session.add(rec)
    await session.commit()
    await session.refresh(rec)
    return rec


async def update_reclamation_item(
    session: AsyncSession,
    *,
    reclamation_id: int,
    item_id: int,
    item_source: Optional[str] = None,
    reason: Optional[str] = None,
    quantity: Optional[int] = None,
    source_provider_id: Optional[int] = None,
) -> Reclamation:
    """Правит позицию рекламации: источник (наш склад/транзит), причину,
    количество. Возвращает саму рекламацию с перезагруженными позициями."""
    item = await session.get(ReclamationItem, item_id)
    if item is None or int(item.reclamation_id) != int(reclamation_id):
        raise ValueError("Позиция рекламации не найдена")
    if item_source is not None:
        if item_source not in _VALID_ITEM_SOURCES:
            raise ValueError(f"Недопустимый источник: {item_source}")
        item.item_source = item_source
    if reason is not None:
        item.reason = reason or None
    if quantity is not None:
        item.quantity = max(1, int(quantity))
    if source_provider_id is not None:
        provider = await session.get(Provider, source_provider_id)
        if provider is None:
            raise ValueError("Поставщик не найден")
        item.source_provider_id = int(source_provider_id)
        item.item_source = RECLAMATION_ITEM_SOURCE.SUPPLIER_TRANSIT
    session.add(item)
    await session.commit()
    rec = await session.get(Reclamation, reclamation_id)
    await session.refresh(rec)
    return rec


async def assign_shortage_reviewer(
    session: AsyncSession,
    *,
    reclamation_id: int,
    user_id: int,
) -> Reclamation:
    """Назначает сотрудника для проверки факта недовоза."""
    rec = (
        await session.execute(
            select(Reclamation)
            .where(Reclamation.id == reclamation_id)
            .options(
                selectinload(Reclamation.customer),
                selectinload(Reclamation.items),
            )
        )
    ).scalar_one_or_none()
    if rec is None:
        raise ValueError("Рекламация не найдена")
    rec_type = str(
        getattr(rec.reclamation_type, "value", rec.reclamation_type)
    )
    if rec_type != RECLAMATION_TYPE_SHORTAGE:
        raise ValueError("Назначение доступно только для недовоза")

    user = await session.get(User, user_id)
    if user is None or user.status != UserStatus.ACTIVE:
        raise ValueError("Выбранный сотрудник не найден или не активен")

    rec.shortage_assigned_to_user_id = int(user.id)
    rec.shortage_assigned_at = now_moscow()
    rec.shortage_status = "pending_confirmation"
    rec.shortage_confirmed_by_user_id = None
    rec.shortage_confirmed_at = None
    rec.shortage_comment = None
    rec.shortage_snoozed_until = None
    session.add(rec)
    await session.flush()

    await _mark_shortage_notifications_read(
        session,
        reclamation_id=reclamation_id,
    )
    payload = _shortage_notification_payload(rec)
    await create_notification(
        session,
        user_id=int(user.id),
        title=f"Проверьте недовоз по рекламации #{rec.id}",
        message=(
            f"Клиент: {getattr(rec.customer, 'name', None) or 'не указан'}. "
            f"Документ: {rec.stated_document_number or 'не указан'}. "
            f"Позиций: {len(rec.items or [])}."
        ),
        level=AppNotificationLevel.WARNING,
        link=f"/reclamations?openId={rec.id}",
        payload=payload,
        commit=False,
    )
    await session.commit()
    await session.refresh(rec)
    return rec


async def confirm_shortage(
    session: AsyncSession,
    *,
    reclamation_id: int,
    confirmed: bool,
    comment: Optional[str],
    user_id: int,
) -> Reclamation:
    """Фиксирует проверку недовоза и сотрудника, принявшего решение."""
    rec = await session.get(Reclamation, reclamation_id)
    if rec is None:
        raise ValueError("Рекламация не найдена")
    rec_type = str(
        getattr(rec.reclamation_type, "value", rec.reclamation_type)
    )
    if rec_type != RECLAMATION_TYPE_SHORTAGE:
        raise ValueError("Подтверждение доступно только для недовоза")

    clean_comment = str(comment or "").strip()
    checked_at = now_moscow()
    rec.shortage_status = "confirmed" if confirmed else "not_confirmed"
    rec.shortage_confirmed_by_user_id = int(user_id)
    rec.shortage_confirmed_at = checked_at
    rec.shortage_comment = clean_comment or None
    rec.shortage_snoozed_until = None
    rec.resolution = "approved" if confirmed else "rejected"
    rec.resolution_comment = (
        clean_comment
        or (
            "Недовоз подтверждён ответственным сотрудником"
            if confirmed
            else "Недовоз не подтверждён ответственным сотрудником"
        )
    )
    rec.resolved_by_user_id = int(user_id)
    rec.resolved_at = checked_at
    rec.status = (
        RECLAMATION_STATUS.APPROVED
        if confirmed
        else RECLAMATION_STATUS.REJECTED
    )
    session.add(rec)
    await _mark_shortage_notifications_read(
        session,
        reclamation_id=reclamation_id,
    )
    await session.commit()
    await session.refresh(rec)
    return rec


def _shortage_notification_payload(
    reclamation: Reclamation,
) -> dict[str, Any]:
    check_items = {
        int(item.get("item_id")): item
        for item in (
            (reclamation.check_result or {}).get("items") or []
        )
        if item.get("item_id") is not None
    }
    items = []
    for item in reclamation.items or []:
        check = check_items.get(int(item.id), {})
        candidates = check.get("supplier_candidates") or []
        supplier_names = [
            candidate.get("provider_name")
            or f"ID {candidate.get('provider_id')}"
            for candidate in candidates
            if candidate.get("provider_id")
        ]
        items.append(
            {
                "item_id": int(item.id),
                "brand_name": item.brand_name,
                "oem_number": item.oem_number,
                "autopart_name": item.autopart_name,
                "quantity": int(item.quantity or 1),
                "order_date": check.get("customer_order_date"),
                "order_number": check.get("customer_order_number"),
                "supplier_id": check.get("supplier_id"),
                "supplier_name": check.get("supplier_name"),
                "supplier_names": supplier_names,
            }
        )
    return {
        "notification_type": "reclamation_shortage",
        "reclamation_id": int(reclamation.id),
        "customer_id": reclamation.customer_id,
        "customer_name": (
            getattr(reclamation.customer, "name", None)
            if reclamation.customer
            else None
        ),
        "document_number": reclamation.stated_document_number,
        "document_date": (
            reclamation.stated_document_date.isoformat()
            if reclamation.stated_document_date
            else None
        ),
        "reason": reclamation.stated_reason or "Недовоз",
        "positions_count": len(items),
        "items": items,
    }


async def _mark_shortage_notifications_read(
    session: AsyncSession,
    *,
    reclamation_id: int,
    user_id: Optional[int] = None,
) -> None:
    stmt = select(AppNotification).where(
        AppNotification.read_at.is_(None),
        AppNotification.link == f"/reclamations?openId={reclamation_id}",
    )
    if user_id is not None:
        stmt = stmt.where(AppNotification.user_id == user_id)
    rows = (await session.execute(stmt)).scalars().all()
    read_at = now_moscow()
    for notification in rows:
        payload = notification.payload or {}
        if (
            payload.get("notification_type") == "reclamation_shortage"
            and int(payload.get("reclamation_id") or 0)
            == int(reclamation_id)
        ):
            notification.read_at = read_at
            session.add(notification)


async def postpone_shortage_review(
    session: AsyncSession,
    *,
    reclamation_id: int,
    minutes: int,
    user_id: int,
) -> Reclamation:
    if minutes not in {15, 30, 60}:
        raise ValueError("Можно отложить только на 15, 30 или 60 минут")
    rec = (
        await session.execute(
            select(Reclamation)
            .where(Reclamation.id == reclamation_id)
            .options(
                selectinload(Reclamation.customer),
                selectinload(Reclamation.items),
            )
        )
    ).scalar_one_or_none()
    if rec is None:
        raise ValueError("Рекламация не найдена")
    if rec.shortage_status in {"confirmed", "not_confirmed"}:
        raise ValueError("Проверка недовоза уже завершена")

    remind_at = now_moscow() + timedelta(minutes=minutes)
    rec.shortage_status = "pending_confirmation"
    rec.shortage_snoozed_until = remind_at
    session.add(rec)
    await _mark_shortage_notifications_read(
        session,
        reclamation_id=reclamation_id,
        user_id=user_id,
    )
    await create_notification(
        session,
        user_id=user_id,
        title=f"Напоминание: недовоз по рекламации #{rec.id}",
        message=(
            f"Клиент: {getattr(rec.customer, 'name', None) or 'не указан'}. "
            f"Проверьте комплектацию отгрузки."
        ),
        level=AppNotificationLevel.WARNING,
        link=f"/reclamations?openId={rec.id}",
        payload=_shortage_notification_payload(rec),
        available_at=remind_at,
        commit=False,
    )
    await session.commit()
    await session.refresh(rec)
    return rec


async def add_shortage_evidence(
    session: AsyncSession,
    *,
    reclamation_id: int,
    filename: str,
    payload: bytes,
    content_type: Optional[str],
) -> ReclamationAttachment:
    rec = await session.get(Reclamation, reclamation_id)
    if rec is None:
        raise ValueError("Рекламация не найдена")
    if not payload:
        raise ValueError("Файл доказательства пуст")
    inbound = ReclamationInboundAttachment(
        filename=filename,
        payload=payload,
        content_type=content_type,
    )
    file_path = _save_attachment_to_disk(reclamation_id, inbound)
    attachment = ReclamationAttachment(
        reclamation_id=reclamation_id,
        kind=RECLAMATION_ATTACHMENT_KIND.SHORTAGE_EVIDENCE,
        file_name=filename,
        content_type=content_type,
        local_file_path=file_path,
        size_bytes=len(payload),
    )
    session.add(attachment)
    await session.commit()
    await session.refresh(attachment)
    return attachment


async def get_reclamation_account(session: AsyncSession):
    """Возвращает активный почтовый ящик с назначением reclamation(s)."""
    from dz_fastapi.models.email_account import EmailAccount

    accounts = (
        await session.execute(
            select(EmailAccount).where(EmailAccount.is_active.is_(True))
        )
    ).scalars().all()
    for acc in accounts:
        purposes = [str(p).lower() for p in (acc.purposes or [])]
        if "reclamation" in purposes or "reclamations" in purposes:
            return acc
    return None


async def _archive_existing_service_reclamations(
    session: AsyncSession,
) -> int:
    """Убирает ранее созданные служебные письма из рабочих очередей."""
    rows = (
        await session.execute(
            select(Reclamation)
            .where(
                Reclamation.source == RECLAMATION_SOURCE.EMAIL,
                Reclamation.status != RECLAMATION_STATUS.CLOSED,
            )
            .order_by(Reclamation.id.desc())
            .limit(500)
        )
    ).scalars().all()
    archived = 0
    for rec in rows:
        service_mail_type = classify_reclamation_service_email(
            sender=rec.sender_email or "",
            subject=rec.email_subject or "",
            body=rec.email_body or "",
        )
        if not service_mail_type:
            continue
        rec.status = RECLAMATION_STATUS.CLOSED
        rec.extracted_data = {
            **(rec.extracted_data or {}),
            "service_mail_type": service_mail_type,
            "archived_automatically": True,
        }
        session.add(rec)
        archived += 1
    if archived:
        await session.commit()
    return archived


async def sync_reclamation_mailbox(session: AsyncSession) -> dict[str, Any]:
    """Инкрементально читает ящик и изолирует результат каждого письма."""
    account = await get_reclamation_account(session)
    if account is None:
        return {
            "fetched": 0,
            "created": 0,
            "skipped": 0,
            "errors": 0,
            "note": (
                "Не найден активный почтовый ящик с назначением "
                "«reclamation». Добавьте его в разделе Почтовые ящики."
            ),
        }

    account_id = int(account.id)
    account_email = str(account.email)
    archived_service = await _archive_existing_service_reclamations(session)
    folder = (getattr(account, "imap_folder", None) or "INBOX").strip()
    mailbox_state = (
        await session.execute(
            select(ReclamationMailboxState).where(
                ReclamationMailboxState.email_account_id == account_id,
                ReclamationMailboxState.folder == folder,
            )
        )
    ).scalar_one_or_none()
    if mailbox_state is None:
        mailbox_state = ReclamationMailboxState(
            email_account_id=account_id,
            folder=folder,
            last_uid=0,
        )
        session.add(mailbox_state)
        await session.commit()
        await session.refresh(mailbox_state)
        await session.refresh(account)

    mailbox_state_id = int(mailbox_state.id)
    previous_uid = int(mailbox_state.last_uid or 0)
    emails = await fetch_reclamation_emails(
        account,
        days=max(
            1,
            int(os.getenv("RECLAMATION_IMAP_INITIAL_DAYS", "7")),
        ),
        last_uid=previous_uid,
        limit=max(
            1,
            int(os.getenv("RECLAMATION_IMAP_BATCH_LIMIT", "200")),
        ),
    )
    from dz_fastapi.services.reclamation_armtek import (
        ArmtekPortalError,
        is_armtek_portal_notice,
        sync_armtek_open_returns,
    )

    created = 0
    skipped = 0
    errors = 0
    armtek_results: list[dict[str, Any]] = []
    armtek_errors: list[str] = []
    max_uid = previous_uid
    for email in emails:
        uid = str(email.uid or email.message_id or "").strip()
        if not uid:
            logger.warning(
                "Письмо рекламации без UID/Message-ID пропущено: %s",
                email.subject,
            )
            skipped += 1
            continue
        try:
            max_uid = max(max_uid, int(uid))
        except ValueError:
            pass

        mail_row = (
            await session.execute(
                select(ReclamationMailMessage).where(
                    ReclamationMailMessage.email_account_id == account_id,
                    ReclamationMailMessage.folder == folder,
                    ReclamationMailMessage.uid == uid,
                )
            )
        ).scalar_one_or_none()
        if mail_row and mail_row.processing_status in {
            "processed",
            "skipped",
        }:
            skipped += 1
            continue
        if mail_row is None:
            mail_row = ReclamationMailMessage(
                email_account_id=account_id,
                folder=folder,
                uid=uid,
            )
        mail_row.message_id = email.message_id
        mail_row.sender_email = extract_sender_email(email.from_)
        mail_row.subject = email.subject
        mail_row.received_at = email.received_at
        mail_row.body_text = email.body_text
        mail_row.body_html = email.body_html
        mail_row.processing_status = "processing"
        mail_row.processing_error = None
        mail_row.attempts = int(mail_row.attempts or 0) + 1
        session.add(mail_row)
        await session.commit()
        await session.refresh(mail_row)
        mail_row_id = int(mail_row.id)

        sender = extract_sender_email(email.from_)
        message_body = "\n".join(
            part for part in (email.body_text, email.body_html) if part
        )
        try:
            reclamation_id: int | None = None
            service_mail_type = classify_reclamation_service_email(
                sender=sender,
                subject=email.subject or "",
                body=message_body,
            )
            if service_mail_type:
                mail_row = await session.get(
                    ReclamationMailMessage,
                    mail_row_id,
                )
                if mail_row is not None:
                    mail_row.processing_status = "skipped"
                    mail_row.processing_error = None
                    mail_row.parser_version = f"service:{service_mail_type}"[:32]
                    mail_row.processed_at = now_moscow()
                    session.add(mail_row)

                if service_mail_type == "reconciliation_statement":
                    user_ids = (
                        await session.execute(
                            select(User.id).where(
                                User.status == UserStatus.ACTIVE,
                                User.role.in_(
                                    [UserRole.ADMIN, UserRole.RECLAMATION]
                                ),
                            )
                        )
                    ).scalars().all()
                    await create_notifications_for_users(
                        session,
                        user_ids=user_ids,
                        title="Получен акт сверки",
                        message=(
                            f"{sender or 'Неизвестный отправитель'} · "
                            f"{email.subject or 'без темы'}"
                        ),
                        level=AppNotificationLevel.INFO,
                        link="/inbox",
                        payload={
                            "notification_type": "reconciliation_statement",
                            "mail_message_id": mail_row_id,
                            "uid": uid,
                        },
                        commit=False,
                    )
                await session.commit()
                skipped += 1
                continue

            if is_armtek_portal_notice(sender=sender, body=message_body):
                customer_id = await resolve_customer_by_email(session, sender)
                result = await sync_armtek_open_returns(
                    session,
                    customer_id=customer_id,
                    sender_email=sender,
                    email_received_at=email.received_at,
                )
                armtek_results.append(result)
                created += int(result.get("created") or 0)
            else:
                rec = await ingest_reclamation_email(session, email)
                if rec is not None:
                    reclamation_id = int(rec.id)
                    created += 1
                else:
                    skipped += 1

            mail_row = await session.get(
                ReclamationMailMessage,
                mail_row_id,
            )
            if mail_row is not None:
                mail_row.reclamation_id = reclamation_id
                mail_row.processing_status = (
                    "processed" if reclamation_id else "skipped"
                )
                mail_row.processed_at = now_moscow()
                session.add(mail_row)
                await session.commit()
        except Exception as exc:  # noqa: BLE001
            await session.rollback()
            errors += 1
            error_text = str(exc)[:4000]
            if isinstance(exc, ArmtekPortalError):
                armtek_errors.append(error_text)
            logger.exception(
                "Ошибка обработки письма рекламации account_id=%s "
                "folder=%s uid=%s",
                account_id,
                folder,
                uid,
            )
            mail_row = await session.get(
                ReclamationMailMessage,
                mail_row_id,
            )
            if mail_row is not None:
                mail_row.processing_status = "error"
                mail_row.processing_error = error_text
                mail_row.processed_at = now_moscow()
                session.add(mail_row)
            user_ids = (
                await session.execute(
                    select(User.id).where(
                        User.status == UserStatus.ACTIVE,
                        User.role.in_(
                            [UserRole.ADMIN, UserRole.RECLAMATION]
                        ),
                    )
                )
            ).scalars().all()
            await create_notifications_for_users(
                session,
                user_ids=user_ids,
                title="Не обработано письмо рекламации",
                message=(
                    f"{sender or 'Неизвестный отправитель'} · "
                    f"{email.subject or 'без темы'}: {error_text[:500]}"
                ),
                level=AppNotificationLevel.ERROR,
                link="/reclamations",
                payload={
                    "notification_type": "reclamation_mail_error",
                    "mail_message_id": mail_row_id,
                    "uid": uid,
                },
                commit=False,
            )
            await session.commit()

    mailbox_state = await session.get(
        ReclamationMailboxState,
        mailbox_state_id,
    )
    if mailbox_state is not None:
        mailbox_state.last_uid = max_uid
        mailbox_state.last_checked_at = now_moscow()
        mailbox_state.last_error = (
            f"Ошибок обработки писем: {errors}" if errors else None
        )
        session.add(mailbox_state)
        await session.commit()
    return {
        "fetched": len(emails),
        "created": created,
        "skipped": skipped,
        "errors": errors,
        "account_email": account_email,
        "armtek": armtek_results,
        "armtek_errors": armtek_errors,
        "archived_service": archived_service,
    }


async def cleanup_closed_reclamation_files(
    session: AsyncSession,
    *,
    max_days: int = 180,
) -> dict[str, int]:
    """Remove old files from closed cases while retaining their audit rows."""
    retention_days = max(1, int(max_days))
    cutoff = now_moscow() - timedelta(days=retention_days)
    attachments = (
        await session.execute(
            select(ReclamationAttachment)
            .join(
                Reclamation,
                Reclamation.id == ReclamationAttachment.reclamation_id,
            )
            .where(
                Reclamation.status == RECLAMATION_STATUS.CLOSED,
                Reclamation.updated_at < cutoff,
                ReclamationAttachment.local_file_path.isnot(None),
            )
        )
    ).scalars().all()

    storage_root = os.path.abspath(RECLAMATION_ATTACHMENTS_DIR)
    removed = 0
    missing = 0
    skipped = 0
    for attachment in attachments:
        file_path = os.path.abspath(attachment.local_file_path or "")
        try:
            inside_storage = (
                os.path.commonpath([storage_root, file_path]) == storage_root
            )
        except ValueError:
            inside_storage = False
        if not inside_storage:
            skipped += 1
            logger.warning(
                "Skip reclamation attachment outside storage root: id=%s path=%s",
                attachment.id,
                attachment.local_file_path,
            )
            continue

        try:
            os.remove(file_path)
            removed += 1
        except FileNotFoundError:
            missing += 1
        except OSError:
            skipped += 1
            logger.warning(
                "Failed to remove old reclamation attachment: id=%s path=%s",
                attachment.id,
                file_path,
                exc_info=True,
            )
            continue

        attachment.local_file_path = None
        session.add(attachment)
        try:
            os.rmdir(os.path.dirname(file_path))
        except OSError:
            pass

    await session.commit()
    return {
        "candidates": len(attachments),
        "removed": removed,
        "missing": missing,
        "skipped": skipped,
    }
