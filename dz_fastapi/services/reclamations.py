"""Приём и первичная обработка рекламаций (претензий) от клиентов.

Этап 2: письма с ящика рекламаций читаем по IMAP (порт 993 открыт),
создаём Reclamation, определяем клиента по адресу отправителя,
сохраняем тело/вложения, извлекаем ссылки на порталы и базовые поля
(номер/дата документа, причина, артикулы) регулярками. AI-экстрактор
и движок проверки — следующие этапы.
"""
from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Any, Optional

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.core.time import now_moscow
from dz_fastapi.models.autopart import AutoPart, preprocess_oem_number
from dz_fastapi.models.partner import (
    RECLAMATION_ATTACHMENT_KIND,
    RECLAMATION_ITEM_SOURCE,
    RECLAMATION_SOURCE,
    RECLAMATION_STATUS,
    RECLAMATION_TYPE,
    Customer,
    CustomerOrder,
    CustomerOrderItem,
    CustomerReclamationEmail,
    Reclamation,
    ReclamationAttachment,
    ReclamationItem,
)
from dz_fastapi.services.reclamation_attachment_parser import parse_reclamation_attachment

logger = logging.getLogger("dz_fastapi")

RECLAMATION_ATTACHMENTS_DIR = os.path.join(
    "uploads", "reclamation_attachments"
)

_EMAIL_RE = re.compile(r"[\w.+-]+@[\w-]+\.[\w.-]+")
_URL_RE = re.compile(r"https?://[^\s<>\"')]+", re.IGNORECASE)
# «№ УТ-1042», «номер УТ-1042 от 15.06.2026», «счёт 123 от 01.02.26»
_DOC_NUMBER_RE = re.compile(
    r"(?:№|номер|док(?:умент)?[а-я]*|счет|счёт|накладн\w*|отгрузк\w*|"
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
)
_REFUSAL_KEYWORDS = (
    "отказ", "не подош", "не подходит", "не нужн", "передумал",
    "ошиб", "пересорт", "не тот", "перезаказ", "возврат",
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
    message_id: Optional[str] = None
    received_at: Optional[datetime] = None
    uid: Optional[str] = None
    attachments: list[ReclamationInboundAttachment] = field(
        default_factory=list
    )


def extract_sender_email(raw: str) -> str:
    m = _EMAIL_RE.search(str(raw or ""))
    return m.group(0).lower() if m else ""


def extract_links(text: str) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for m in _URL_RE.finditer(str(text or "")):
        url = m.group(0).rstrip(".,);")
        if url not in seen:
            seen.add(url)
            result.append(url)
    return result


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


def classify_reclamation_type(text: str) -> Optional[str]:
    lowered = str(text or "").lower()
    if any(kw in lowered for kw in _DEFECT_KEYWORDS):
        return RECLAMATION_TYPE_DEFECT
    if any(kw in lowered for kw in _REFUSAL_KEYWORDS):
        return RECLAMATION_TYPE_REFUSAL
    return None


def extract_fields(subject: str, body: str) -> dict[str, Any]:
    """Регулярное извлечение полей из письма (первый слой распознавания)."""
    text = f"{subject}\n{body}"
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
) -> list[dict[str, Any]]:
    """Ищет артикулы в номенклатуре и прошлых заказах клиента."""
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
        if customer_id is not None:
            order_stmt = order_stmt.where(
                CustomerOrder.customer_id == customer_id
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


async def recognize_reclamation_items(
    session: AsyncSession,
    reclamation: Reclamation,
) -> int:
    """Дополняет позиции карточки по сохранённому письму и истории заказов."""
    existing_oems = {
        preprocess_oem_number(item.oem_number or "")
        for item in (reclamation.items or [])
        if item.oem_number
    }
    matched = await _match_oems_in_text(
        session,
        f"{reclamation.email_subject or ''}\n{reclamation.email_body or ''}",
        customer_id=reclamation.customer_id,
    )
    created = 0
    for item in matched:
        normalized = preprocess_oem_number(item["oem_number"] or "")
        if not normalized or normalized in existing_oems:
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
    if created:
        session.add(reclamation)
        await session.flush()
    return created


async def resolve_customer_by_email(
    session: AsyncSession,
    sender_email: str,
) -> Optional[int]:
    """Определяет клиента по адресу: сначала список почт рекламаций,
    затем контактный email клиента."""
    email = str(sender_email or "").strip().lower()
    if not email:
        return None
    row = (
        await session.execute(
            select(CustomerReclamationEmail.customer_id).where(
                CustomerReclamationEmail.email == email
            )
        )
    ).scalar_one_or_none()
    if row is not None:
        return int(row)
    customer_id = (
        await session.execute(
            select(Customer.id).where(
                func.lower(Customer.email_contact) == email
            )
        )
    ).scalar_one_or_none()
    return int(customer_id) if customer_id is not None else None


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


async def ingest_reclamation_email(
    session: AsyncSession,
    email: ReclamationInboundEmail,
) -> Optional[Reclamation]:
    """Создаёт рекламацию из письма (идемпотентно по Message-ID)."""
    if email.message_id:
        existing = (
            await session.execute(
                select(Reclamation.id).where(
                    Reclamation.email_message_id == email.message_id
                )
            )
        ).scalar_one_or_none()
        if existing is not None:
            logger.debug(
                "Рекламация по письму %s уже создана (#%s)",
                email.message_id,
                existing,
            )
            return None

    sender = extract_sender_email(email.from_)
    customer_id = await resolve_customer_by_email(session, sender)
    fields = extract_fields(email.subject, email.body_text)
    attachment_extractions = [
        parsed
        for attachment in (email.attachments or [])
        if (
            parsed := parse_reclamation_attachment(
                attachment.filename,
                attachment.payload,
            )
        )
    ]
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
    reclamation_type = fields.get("reclamation_type")
    if not reclamation_type and attachment_reason:
        reclamation_type = classify_reclamation_type(attachment_reason)

    extracted_data = dict(fields)
    if attachment_extractions:
        extracted_data["attachments"] = attachment_extractions

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
        source_link=(fields["links"][0] if fields.get("links") else None),
        email_message_id=email.message_id,
        email_subject=(email.subject or "")[:998] or None,
        email_received_at=email.received_at or now_moscow(),
        email_body=email.body_text or None,
        stated_document_number=document_number,
        stated_document_date=doc_date,
        stated_reason=attachment_reason or None,
        extracted_data=extracted_data,
    )
    session.add(reclamation)
    await session.flush()

    # Позиции по найденным артикулам в тексте
    matched = await _match_oems_in_text(
        session,
        f"{email.subject}\n{email.body_text}",
        customer_id=customer_id,
    )
    candidates = {
        preprocess_oem_number(item["oem_number"]): {
            **item,
            "quantity": 1,
            "reason": None,
        }
        for item in matched
        if preprocess_oem_number(item.get("oem_number") or "")
    }
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
) -> list[ReclamationInboundEmail]:
    """Читает ящик рекламаций (тело + Message-ID + вложения)."""
    from imap_tools import AND

    from dz_fastapi.services.email import _create_mailbox

    result: list[ReclamationInboundEmail] = []
    mb = _create_mailbox(host, port, True).login(email, password)
    with mb as mailbox:
        mailbox.folder.set(folder)
        for msg in mailbox.fetch(
            AND(date_gte=since_date, all=True),
            charset="utf-8",
            mark_seen=False,
        ):
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
            message_id = None
            mid = headers.get("message-id") or headers.get("Message-ID")
            if isinstance(mid, (tuple, list)) and mid:
                message_id = str(mid[0])
            elif mid:
                message_id = str(mid)
            result.append(
                ReclamationInboundEmail(
                    from_=str(getattr(msg, "from_", "") or ""),
                    subject=str(getattr(msg, "subject", "") or ""),
                    body_text=str(
                        getattr(msg, "text", None)
                        or getattr(msg, "html", "")
                        or ""
                    ),
                    message_id=message_id or (
                        str(msg.uid) if msg.uid else None
                    ),
                    received_at=getattr(msg, "date", None),
                    uid=str(msg.uid) if msg.uid else None,
                    attachments=attachments,
                )
            )
    return result


async def fetch_reclamation_emails(
    account,
    days: int = 7,
) -> list[ReclamationInboundEmail]:
    """IMAP-чтение ящика рекламаций (в отдельном потоке)."""
    import asyncio

    host = getattr(account, "imap_host", None)
    if not host:
        raise RuntimeError(
            "У почтового ящика рекламаций не указан IMAP-сервер"
        )
    since = (now_moscow() - timedelta(days=days)).date()
    folder = (getattr(account, "imap_folder", None) or "INBOX").strip()
    try:
        return await asyncio.wait_for(
            asyncio.to_thread(
                _fetch_reclamation_imap_sync,
                host,
                account.email,
                account.password,
                folder,
                int(getattr(account, "imap_port", 993) or 993),
                since,
            ),
            timeout=120,
        )
    except Exception as exc:  # noqa: BLE001
        logger.error(
            "Ошибка чтения ящика рекламаций id=%s: %s",
            getattr(account, "id", "?"),
            exc,
        )
        raise RuntimeError(
            "Не удалось прочитать почтовый ящик рекламаций "
            f"{getattr(account, 'email', '')}: {exc}"
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
                    CustomerReclamationEmail.email == email
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
        rec.resolution = resolution or None
        rec.resolution_comment = resolution_comment
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
    session.add(item)
    await session.commit()
    rec = await session.get(Reclamation, reclamation_id)
    await session.refresh(rec)
    return rec


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


async def sync_reclamation_mailbox(session: AsyncSession) -> dict[str, Any]:
    """Читает ящик рекламаций (purpose=reclamation) и создаёт рекламации."""
    account = await get_reclamation_account(session)
    if account is None:
        return {
            "fetched": 0,
            "created": 0,
            "skipped": 0,
            "note": (
                "Не найден активный почтовый ящик с назначением "
                "«reclamation». Добавьте его в разделе Почтовые ящики."
            ),
        }

    emails = await fetch_reclamation_emails(account)
    from dz_fastapi.services.reclamation_armtek import (
        ArmtekPortalError,
        is_armtek_portal_notice,
        sync_armtek_open_returns,
    )

    created = 0
    skipped = 0
    armtek_results: list[dict[str, Any]] = []
    armtek_errors: list[str] = []
    for email in emails:
        sender = extract_sender_email(email.from_)
        if is_armtek_portal_notice(sender=sender, body=email.body_text):
            customer_id = await resolve_customer_by_email(session, sender)
            try:
                result = await sync_armtek_open_returns(
                    session,
                    customer_id=customer_id,
                    sender_email=sender,
                    email_received_at=email.received_at,
                )
            except ArmtekPortalError as exc:
                logger.error(
                    "Не удалось синхронизировать возвраты Armtek: %s",
                    exc,
                )
                armtek_errors.append(str(exc))
                skipped += 1
            else:
                armtek_results.append(result)
                created += int(result.get("created") or 0)
            continue
        rec = await ingest_reclamation_email(session, email)
        if rec is not None:
            created += 1
        else:
            skipped += 1
    return {
        "fetched": len(emails),
        "created": created,
        "skipped": skipped,
        "account_email": account.email,
        "armtek": armtek_results,
        "armtek_errors": armtek_errors,
    }
