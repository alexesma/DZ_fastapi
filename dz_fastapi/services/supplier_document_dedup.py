"""Защита от дублей документов поставщиков между почтой и Диадоком.

Ключ документа: поставщик + тип + нормализованный номер + дата. Номер и дату
берём из XML (формализованный УПД ФНС); из PDF/Excel не извлекаем — там
дедуплицировать не пытаемся (кроме совпадения sha256 файла).

Правила:
* Диадок главнее почты: письмо с документом, который уже есть в Диадоке,
  не обрабатывается и помечается дублем (DUPLICATE_DOC).
* Письмо обработано раньше — Диадок-документ привязывается к существующему
  поступлению («тот же документ»), второе поступление не создаётся.
* Ключ совпал, но итоговая сумма отличается больше чем на 0.01 ₽ —
  не склеиваем и не обрабатываем: «расхождение по сумме», решение за
  администратором (resolve_amount_mismatch).
* Поступлениями становятся только УПД, накладные и корректировки; счета на
  оплату и т.п. из Диадока пропускаются.
"""

from __future__ import annotations

import datetime as dt
import logging
import os
import re
from dataclasses import dataclass
from decimal import Decimal
from types import SimpleNamespace
from typing import Any, Optional

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from dz_fastapi.core.time import now_moscow
from dz_fastapi.models.diadoc import DiadocIncomingDocument
from dz_fastapi.models.partner import SupplierOrderAttachment, SupplierOrderMessage, SupplierReceipt
from dz_fastapi.services.supplier_receipt_corrections import is_correction_document

logger = logging.getLogger("dz_fastapi")

AMOUNT_TOLERANCE = Decimal("0.01")
MESSAGE_TYPE_DUPLICATE = "DUPLICATE_DOC"
MESSAGE_TYPE_MISMATCH = "DOCUMENT_MISMATCH"
# Блокирующее окно у администраторов: закрывается только решением.
MISMATCH_NOTIFICATION_TYPE = "supplier_document_mismatch_action"

_NUMBER_CLEAN_RE = re.compile(r"[^0-9A-Za-zА-Яа-яЁё]+")

_DIADOC_KIND_BY_TYPE = {
    "universaltransferdocument": "upd",
    "universaltransferdocumentrevision": "upd",
    "xmltorg12": "torg12",
    "torg12": "torg12",
    "universalcorrectiondocument": "ukd",
    "universalcorrectiondocumentrevision": "ukd",
    "invoicecorrection": "kschf",
}
# Типы, которые точно не являются поступлением.
_NON_RECEIPT_TYPES = {
    "invoice",
    "invoicerevision",
    "xmlacceptancecertificate",
    "acceptancecertificate",
    "contract",
    "pricelist",
    "proformainvoice",
    "reconciliationact",
}


@dataclass(slots=True)
class DocumentIdentity:
    kind: Optional[str]
    number: Optional[str]
    date: Optional[dt.date]
    total: Optional[Decimal]

    @property
    def usable(self) -> bool:
        return bool(self.kind and self.number and self.date)


_TRAILING_DIGITS_RE = re.compile(r"(\d+)$")


def normalize_document_number(value: object) -> Optional[str]:
    """Сравнимая форма номера.

    1С печатает «0К00-005846», а в Диадок тот же документ уходит как «5846»,
    поэтому сравниваем порядковую часть: хвост из цифр без ведущих нулей.
    Префикс не теряется бесследно только благодаря остальным частям ключа
    (поставщик, тип, дата). Номера без цифр на конце сравниваются целиком.
    """
    text = _NUMBER_CLEAN_RE.sub("", str(value or "")).upper()
    if not text:
        return None
    match = _TRAILING_DIGITS_RE.search(text)
    if match:
        return match.group(1).lstrip("0") or "0"
    return text


def _to_decimal(value: object) -> Optional[Decimal]:
    if value in (None, ""):
        return None
    try:
        return Decimal(str(value)).quantize(Decimal("0.01"))
    except Exception:
        return None


def totals_differ(a: object, b: object) -> bool:
    """True только если обе суммы известны и различаются больше 0.01 ₽."""
    left, right = _to_decimal(a), _to_decimal(b)
    if left is None or right is None:
        return False
    return abs(left - right) > AMOUNT_TOLERANCE


_TRANSPORT_FILE_PREFIXES = ("on_trn", "on_epl", "on_zayavka", "on_zayav")
_ACT_FILE_PREFIXES = ("on_akt", "dp_rezruisp", "dp_akt")
_TORG12_FILE_PREFIXES = ("on_naklpost",)
_DISCREPANCY_FILE_PREFIXES = ("dp_prirasx", "dp_rasx")
_UKD_FILE_PREFIXES = ("on_korschfdop", "on_nkorschfdop")
_UPD_FILE_PREFIXES = ("on_nschfdop",)
_ACT_TYPES = {
    "xmlacceptancecertificate",
    "acceptancecertificate",
    "servicedetails",
    "reconciliationact",
}


def classify_diadoc_document(
    type_named_id: object, filename: object
) -> str:
    """Категория документа по TypeNamedId и префиксу имени файла Диадока.

    upd | ukd | kschf | torg12 | act | discrepancy_act | transport |
    invoice | payment_invoice | other. Настройки по каждому поставщику не нужны.
    """
    type_key = str(type_named_id or "").strip().lower()
    name = str(filename or "").strip().lower()
    # Имя файла Диадока надёжнее TypeNamedId: XmlTorg12/XmlAcceptanceCertificate
    # часто содержат обычный УПД (ON_NSCHFDOPPR_…).
    if name.startswith(_DISCREPANCY_FILE_PREFIXES):
        return "discrepancy_act"
    if name.startswith(_TRANSPORT_FILE_PREFIXES):
        return "transport"
    if name.startswith(_UKD_FILE_PREFIXES):
        return "ukd"
    if name.startswith(_UPD_FILE_PREFIXES):
        return "upd"
    if name.startswith(_ACT_FILE_PREFIXES):
        return "act"
    if name.startswith(_TORG12_FILE_PREFIXES):
        return "torg12"
    if type_key in _DIADOC_KIND_BY_TYPE:
        return _DIADOC_KIND_BY_TYPE[type_key]
    if "waybill" in type_key or type_key in {"orderrequest"}:
        return "transport"
    if type_key in _ACT_TYPES:
        return "act"
    if type_key in {"invoice", "invoicerevision"} or name.startswith(
        "on_sfakt"
    ):
        return "invoice"
    if type_key == "nonformalized" and name.endswith(".pdf"):
        return "payment_invoice"
    if type_key in _NON_RECEIPT_TYPES:
        return "other_non_receipt"
    return "other"


def kind_from_diadoc_type(type_named_id: object, filename: object) -> Optional[str]:
    """Вид документа для ключа дедупа (None — дедуплицировать нечем)."""
    category = classify_diadoc_document(type_named_id, filename)
    return category if category in {"upd", "ukd", "kschf", "torg12"} else None


def is_receipt_document(type_named_id: object, filename: object) -> bool:
    """Может ли документ из Диадока стать поступлением/корректировкой."""
    if is_correction_document(type_named_id, filename):
        return True
    return classify_diadoc_document(type_named_id, filename) not in {
        "transport",
        "act",
        "discrepancy_act",
        "invoice",
        "payment_invoice",
        "other_non_receipt",
    }


def identity_from_xml(payload: bytes, filename: str = "") -> Optional[DocumentIdentity]:
    """Идентичность документа из XML УПД/УКД; None, если файл не опознан."""
    from dz_fastapi.services.upd_xml import (
        TORG12_FILENAME_PREFIXES,
        looks_like_upd_xml,
        parse_correction_header,
        parse_torg12_xml,
        parse_upd_xml,
    )

    if not payload or not str(filename or "").lower().endswith(".xml"):
        return None
    name = str(filename or "").lower()
    if name.startswith(TORG12_FILENAME_PREFIXES):
        try:
            torg = parse_torg12_xml(payload)
        except ValueError:
            return None
        return DocumentIdentity(
            kind="torg12",
            number=normalize_document_number(torg.document_number),
            date=torg.document_date,
            total=_to_decimal(torg.total_with_vat),
        )
    try:
        if looks_like_upd_xml(payload, filename):
            parsed = parse_upd_xml(payload)
            kind = (
                "ukd"
                if parsed.knd in {"1115127", "1115132"}
                or name.startswith(_UKD_FILE_PREFIXES)
                else "upd"
            )
            return DocumentIdentity(
                kind=kind,
                number=normalize_document_number(parsed.document_number),
                date=parsed.document_date,
                total=_to_decimal(parsed.total_with_vat),
            )
    except Exception:
        pass
    # УКД/КСЧФ: шапка СвКСчФ вместо СвСчФакт
    header = parse_correction_header(payload)
    if header is None:
        return None
    number, header_date = header
    return DocumentIdentity(
        kind="ukd",
        number=normalize_document_number(number),
        date=header_date,
        total=None,
    )


def build_dedup_key(
    provider_id: Optional[int], identity: Optional[DocumentIdentity]
) -> Optional[str]:
    if not provider_id or identity is None or not identity.usable:
        return None
    return (
        f"{int(provider_id)}|{identity.kind}|{identity.number}|"
        f"{identity.date.isoformat()}"
    )


def apply_diadoc_metadata_identity(document: DiadocIncomingDocument) -> None:
    """Ключ из метаданных списка Диадока (до скачивания файла)."""
    meta = document.raw_metadata or {}
    kind = kind_from_diadoc_type(meta.get("TypeNamedId"), document.file_name)
    document.document_kind = kind
    identity = DocumentIdentity(
        kind=kind,
        number=normalize_document_number(document.document_number),
        date=document.document_date,
        total=_to_decimal(document.total_amount),
    )
    document.dedup_key = build_dedup_key(document.provider_id, identity)


def effective_category(document: DiadocIncomingDocument) -> str:
    """Категория с учётом содержимого: любой тип Диадока с УПД внутри — УПД."""
    meta = document.raw_metadata or {}
    category = classify_diadoc_document(
        meta.get("TypeNamedId"), document.file_name
    )
    path = document.local_file_path
    name = str(document.file_name or "").lower()
    if (
        category in {"act", "torg12", "other"}
        and name.endswith(".xml")
        and path
        and os.path.exists(os.path.abspath(path))
    ):
        from dz_fastapi.services.upd_xml import looks_like_upd_xml

        with open(os.path.abspath(path), "rb") as fh:
            if looks_like_upd_xml(fh.read(), name):
                return "upd"
    return category


def _is_processable_other(document: DiadocIncomingDocument) -> bool:
    """Неклассифицированный файл, который всё же можно разбирать как документ.

    Таблицы (xlsx/xls/csv) идут по настройкам поставщика; XML — только если
    по содержимому это УПД. Остальное (pdf, архивы…) — «прочее».
    """
    name = str(document.file_name or "").lower()
    ext = name.rsplit(".", 1)[-1] if "." in name else ""
    if ext in {"xlsx", "xls", "csv"}:
        return True
    if ext == "xml":
        path = document.local_file_path
        if path and os.path.exists(os.path.abspath(path)):
            from dz_fastapi.services.upd_xml import looks_like_upd_xml

            with open(os.path.abspath(path), "rb") as fh:
                return looks_like_upd_xml(fh.read(), name)
        return True  # файла ещё нет: решим после скачивания
    return False


def apply_category_status(document: DiadocIncomingDocument) -> Optional[str]:
    """Статус для документов, которые никогда не станут поступлением.

    Ставится при синхронизации, даже когда автообработка выключена, чтобы
    в списке входящих сразу было видно «ЭТрН» / «Акт». Возвращает категорию.
    """
    category = effective_category(document)
    if category not in {"transport", "act", "discrepancy_act"}:
        return category
    document.document_kind = category
    if str(document.status or "synced").lower() == "synced":
        document.status = {
            "transport": "transport",
            "act": "service_act",
            "discrepancy_act": "discrepancy_act",
        }[category]
        document.import_error_details = {
            "transport": (
                "Перевозочный документ (ЭТрН): поступление не создаётся, "
                "ведётся во вкладке «Перевозки (ЭТрН)»"
            ),
            "act": "Акт (работы/услуги): в товарные поступления не передаётся",
            "discrepancy_act": (
                "Акт о расхождениях при приёмке: нужно решение (рекламация)"
            ),
        }[category]
    return category


def refresh_diadoc_identity_from_file(document: DiadocIncomingDocument) -> None:
    """Уточняет ключ и сумму по скачанному XML (если это УПД)."""
    apply_diadoc_metadata_identity(document)
    path = document.local_file_path
    if not path or not os.path.exists(os.path.abspath(path)):
        return
    if not str(document.file_name or path).lower().endswith(".xml"):
        return
    with open(os.path.abspath(path), "rb") as fh:
        identity = identity_from_xml(fh.read(), document.file_name or path)
    if identity is None:
        return
    document.total_amount = identity.total
    if identity.usable:
        document.document_kind = identity.kind
        document.dedup_key = build_dedup_key(document.provider_id, identity)


# ---------------------------------------------------------------- почта


async def apply_email_dedup(
    session: AsyncSession,
    *,
    message_row: SupplierOrderMessage,
    provider_id: int,
    attachments: list[tuple[str, bytes, str]],
) -> Optional[str]:
    """Проверка письма до разбора. attachments: (имя, содержимое, sha256).

    Возвращает None (обрабатывать как обычно), "duplicate" или "mismatch".
    Для необработанных писем сохраняет ключ в message_row.
    """
    identity: Optional[DocumentIdentity] = None
    for filename, payload, _digest in attachments:
        identity = identity_from_xml(payload, filename)
        if identity is not None:
            break
    key = build_dedup_key(provider_id, identity)
    if identity is not None:
        message_row.document_kind = identity.kind
        message_row.document_total = identity.total
    if key:
        message_row.document_key = key

    digests = [d for _f, _p, d in attachments if d]
    candidates: list[DiadocIncomingDocument] = []
    if key:
        candidates = list(
            (
                await session.execute(
                    select(DiadocIncomingDocument)
                    .where(
                        DiadocIncomingDocument.dedup_key == key,
                        DiadocIncomingDocument.status != "skipped",
                    )
                    .order_by(DiadocIncomingDocument.id.asc())
                )
            )
            .scalars()
            .all()
        )
    if not candidates and digests:
        candidates = list(
            (
                await session.execute(
                    select(DiadocIncomingDocument)
                    .where(
                        DiadocIncomingDocument.provider_id == provider_id,
                        DiadocIncomingDocument.content_sha256.in_(digests),
                    )
                    .order_by(DiadocIncomingDocument.id.asc())
                )
            )
            .scalars()
            .all()
        )
    if not candidates:
        return None
    original = candidates[0]
    same_file = bool(digests and original.content_sha256 in digests)
    if (
        not same_file
        and identity is not None
        and totals_differ(original.total_amount, identity.total)
    ):
        message_row.message_type = MESSAGE_TYPE_MISMATCH
        message_row.duplicate_of_diadoc_id = int(original.id)
        message_row.import_error_details = (
            f"Расхождение по сумме с документом из Диадок "
            f"№{original.document_number}: письмо {identity.total} ₽, "
            f"Диадок {original.total_amount} ₽"
        )[:500]
        if original.amount_mismatch_status is None:
            # Диадок главнее: версия из письма игнорируется, админу — инфо.
            original.amount_mismatch_status = "applied"
            original.mismatch_message_id = int(message_row.id)
            original.mismatch_other_total = identity.total
            original.mismatch_resolved_at = now_moscow()
            _log(
                original,
                action="auto_keep_diadoc",
                result="email_ignored",
                email_total=str(identity.total),
                diadoc_total=str(original.total_amount),
            )
            await _notify_mismatch(
                session, original, identity.total, "письмо", auto=True
            )
        session.add(original)
        return "mismatch"
    message_row.message_type = MESSAGE_TYPE_DUPLICATE
    message_row.duplicate_of_diadoc_id = int(original.id)
    message_row.import_error_details = (
        f"Дубль документа из Диадок №{original.document_number or '—'}"
        f" от {original.document_date or '—'} (id {original.id}); "
        "письмо не обрабатывалось"
    )[:500]
    return "duplicate"


async def _notify_mismatch(
    session: AsyncSession,
    document: DiadocIncomingDocument,
    other_total: Optional[Decimal],
    other_source: str,
    auto: bool = False,
    note: Optional[str] = None,
) -> None:
    from dz_fastapi.models.notification import AppNotificationLevel
    from dz_fastapi.services.notifications import create_admin_notifications

    provider_name = getattr(getattr(document, "provider", None), "name", None)
    await create_admin_notifications(
        session=session,
        title="Расхождение по сумме документа поставщика",
        message=(
            f"Документ №{document.document_number or '—'} от "
            f"{document.document_date or '—'}: Диадок "
            f"{document.total_amount} ₽, {other_source} {other_total} ₽. "
            + (
                (note or "Данные Диадока главнее — применены автоматически.")
                + " Версия из письма не использована."
                if auto
                else "Нужно решение администратора."
            )
        ),
        level=AppNotificationLevel.ERROR,
        link=f"/documents/diadoc?tab=inbound&incomingId={document.id}",
        payload={
            "notification_type": MISMATCH_NOTIFICATION_TYPE,
            "diadoc_document_id": int(document.id),
            "document_number": document.document_number,
            "document_date": (
                document.document_date.isoformat()
                if document.document_date
                else None
            ),
            "provider_name": provider_name,
            "diadoc_total": (
                str(document.total_amount)
                if document.total_amount is not None
                else None
            ),
            "other_total": str(other_total) if other_total is not None else None,
            "other_source": other_source,
            "auto": auto,
            "note": note,
        },
        commit=False,
    )


async def close_mismatch_notifications(
    session: AsyncSession, *, document_id: int
) -> int:
    """Закрывает все открытые окна расхождения по документу."""
    from dz_fastapi.models.notification import AppNotification

    rows = (
        (
            await session.execute(
                select(AppNotification).where(
                    AppNotification.read_at.is_(None)
                )
            )
        )
        .scalars()
        .all()
    )
    closed = 0
    now = now_moscow()
    for row in rows:
        payload = row.payload if isinstance(row.payload, dict) else {}
        if (
            payload.get("notification_type") == MISMATCH_NOTIFICATION_TYPE
            and payload.get("diadoc_document_id") == int(document_id)
        ):
            row.read_at = now
            closed += 1
    return closed


async def acknowledge_mismatch(
    session: AsyncSession, *, document_id: int
) -> DiadocIncomingDocument:
    """Администратор просмотрел расхождение: окно больше не возвращается."""
    document = await session.get(DiadocIncomingDocument, document_id)
    if document is None:
        raise LookupError("Document not found")
    if document.amount_mismatch_status not in (
        "applied",
        "correction_required",
        "dismissed",
    ):
        raise ValueError("Расхождение ещё требует решения")
    document.mismatch_acknowledged_at = now_moscow()
    await close_mismatch_notifications(session, document_id=int(document.id))
    await session.commit()
    return document


# --------------------------------------------------------------- Диадок


async def _email_message_for_key(
    session: AsyncSession, document: DiadocIncomingDocument
) -> Optional[SupplierOrderMessage]:
    """Письмо с тем же документом (по ключу или по sha256 файла)."""
    base = (
        select(SupplierOrderMessage)
        .options(selectinload(SupplierOrderMessage.receipts))
        .where(
            SupplierOrderMessage.provider_id == document.provider_id,
            SupplierOrderMessage.duplicate_of_diadoc_id.is_(None),
        )
        .order_by(SupplierOrderMessage.id.asc())
    )
    if document.supplier_order_message_id is not None:
        base = base.where(
            SupplierOrderMessage.id != document.supplier_order_message_id
        )
    if document.dedup_key:
        rows = list(
            (
                await session.execute(
                    base.where(
                        SupplierOrderMessage.document_key == document.dedup_key
                    )
                )
            )
            .scalars()
            .all()
        )
        if rows:
            # предпочитаем письмо, по которому уже есть поступление
            rows.sort(key=lambda m: (not m.receipts, m.id))
            return rows[0]
    if document.content_sha256:
        rows = list(
            (
                await session.execute(
                    base.join(
                        SupplierOrderAttachment,
                        SupplierOrderAttachment.message_id
                        == SupplierOrderMessage.id,
                    ).where(
                        SupplierOrderAttachment.sha256
                        == document.content_sha256,
                        SupplierOrderMessage.source_uid.is_not(None),
                    )
                )
            )
            .scalars()
            .all()
        )
        if rows:
            return rows[0]
    number = str(document.document_number or "").strip().lower()
    if number and document.document_date is not None:
        # Письма, обработанные до введения ключа дедупликации, и документы в
        # таблицах без XML: сверяем по номеру и дате уже созданного поступления.
        existing = (
            (
                await session.execute(
                    select(SupplierOrderMessage)
                    .join(
                        SupplierReceipt,
                        SupplierReceipt.source_message_id
                        == SupplierOrderMessage.id,
                    )
                    .options(selectinload(SupplierOrderMessage.receipts))
                    .where(
                        SupplierOrderMessage.provider_id
                        == document.provider_id,
                        SupplierOrderMessage.source_uid.is_not(None),
                        func.lower(func.trim(SupplierReceipt.document_number))
                        == number,
                        SupplierReceipt.document_date == document.document_date,
                        SupplierOrderMessage.id
                        != (document.supplier_order_message_id or 0),
                    )
                    .order_by(SupplierOrderMessage.id.asc())
                )
            )
            .scalars()
            .first()
        )
        if existing is not None:
            return existing
    return None


async def evaluate_diadoc_document(
    session: AsyncSession, *, document: DiadocIncomingDocument
) -> dict[str, Any]:
    """Решает, можно ли обрабатывать Диадок-документ как новое поступление.

    action: proceed | skipped | linked | duplicate | mismatch |
    customer | unknown.
    """
    if document.duplicate_kind == "email" and document.matched_message_id:
        return {"action": "linked", "message_id": document.matched_message_id}
    if document.duplicate_kind == "diadoc":
        return {"action": "duplicate"}
    if document.amount_mismatch_status in ("pending", "correction_required"):
        return {"action": "mismatch"}
    if document.amount_mismatch_status in ("applied", "dismissed"):
        return {"action": "proceed"}

    if document.supplier_order_message_id is not None:
        own = (
            await session.execute(
                select(SupplierReceipt.id)
                .where(
                    SupplierReceipt.source_message_id
                    == document.supplier_order_message_id
                )
                .limit(1)
            )
        ).first()
        if own is not None:
            return {"action": "proceed"}

    meta = document.raw_metadata or {}
    category = effective_category(document)
    if category == "transport":
        document.document_kind = "transport"
        document.status = "transport"
        document.import_error_details = (
            "Перевозочный документ (ЭТрН): поступление не создаётся, "
            "ведётся во вкладке «Перевозки (ЭТрН)»"
        )
        session.add(document)
        await session.commit()
        return {"action": "transport"}
    if category in {"act", "discrepancy_act"}:
        from dz_fastapi.services.diadoc_details import collect_details

        document.document_kind = category
        if category == "discrepancy_act":
            from dz_fastapi.services.diadoc_discrepancy import process_discrepancy_act

            document.parsed_details = await process_discrepancy_act(
                session, document=document
            )
        else:
            document.parsed_details = collect_details(document, category)
        if category == "act":
            document.status = "service_act"
            document.import_error_details = (
                "Акт (работы/услуги): в товарные поступления не передаётся, "
                "обработка вручную"
            )
        else:
            document.status = "discrepancy_act"
            document.import_error_details = (
                document.parsed_details.get("hint")
                or "Акт о расхождениях при приёмке: ничего не проводится "
                "автоматически, нужно решение (рекламация)"
            )
        session.add(document)
        await session.commit()
        return {"action": document.status}
    if category == "other" and not _is_processable_other(document):
        document.document_kind = "other"
        document.status = "other_doc"
        document.import_error_details = (
            "Тип документа не распознан: сохранён в списке, обработка вручную"
        )
        session.add(document)
        await session.commit()
        return {"action": "other_doc"}

    # Направление: документ по нашей продаже клиенту поступлением не станет.
    from dz_fastapi.services.diadoc_direction import (
        DIRECTION_CUSTOMER,
        DIRECTION_UNKNOWN,
        detect_document_direction,
        has_customer_binding,
    )

    direction = await detect_document_direction(session, document)
    if direction == DIRECTION_CUSTOMER:
        document.status = "customer_doc"
        document.import_error_details = (
            "Документ по нашей продаже клиенту: поступление не создаётся"
        )
        session.add(document)
        await session.commit()
        return {"action": "customer"}
    if direction == DIRECTION_UNKNOWN:
        document.status = "direction_unknown"
        document.import_error_details = (
            "Направление документа (поставщик/клиент) определить не удалось: "
            "контрагент привязан и к поставщику, и к клиенту. "
            "Нужно решение вручную"
        )
        session.add(document)
        await session.commit()
        return {"action": "unknown"}

    # Контрагент одновременно клиент и документ про возврат товара: это
    # возможный возврат от клиента, а не обычная закупка.
    from dz_fastapi.services.diadoc_details import detect_return_marker

    return_text = detect_return_marker(document)
    if return_text and (
        document.customer_id
        or await has_customer_binding(session, document)
    ):
        document.status = "possible_return"
        document.parsed_details = {
            **(document.parsed_details or {}),
            "possible_return": True,
            "return_text": return_text[:300],
        }
        document.import_error_details = (
            "Возможный возврат товара от клиента (в основании: "
            f"«{return_text[:120]}»): в закупку автоматически не берём, "
            "нужно решение (возврат от клиента)"
        )
        session.add(document)
        await session.commit()
        return {"action": "possible_return"}

    if category in {
        "invoice",
        "payment_invoice",
        "other_non_receipt",
    } and not is_correction_document(
        meta.get("TypeNamedId"), document.file_name
    ):
        document.status = "skipped"
        document.import_error_details = (
            "Не поступление (счёт на оплату или другой документ): "
            "в поступления не передаётся"
        )
        session.add(document)
        await session.commit()
        return {"action": "skipped"}
    is_correction = is_correction_document(
        meta.get("TypeNamedId"), document.file_name
    )

    refresh_diadoc_identity_from_file(document)
    if not document.provider_id or not (
        document.dedup_key or document.content_sha256
    ):
        return {"action": "proceed"}

    # 1) тот же документ уже пришёл другим Диадок-сообщением
    if document.dedup_key:
        other = (
            (
                await session.execute(
                    select(DiadocIncomingDocument)
                    .where(
                        DiadocIncomingDocument.dedup_key == document.dedup_key,
                        DiadocIncomingDocument.id < document.id,
                        DiadocIncomingDocument.status != "skipped",
                    )
                    .order_by(DiadocIncomingDocument.id.asc())
                )
            )
            .scalars()
            .first()
        )
        if other is not None:
            if totals_differ(other.total_amount, document.total_amount):
                return await _flag_mismatch(
                    session,
                    document,
                    other_message_id=other.supplier_order_message_id,
                    other_total=other.total_amount,
                    source="другой документ Диадок",
                )
            document.duplicate_kind = "diadoc"
            document.duplicate_of_document_id = int(
                other.duplicate_of_document_id or other.id
            )
            document.status = "duplicate"
            document.import_error_details = (
                f"Дубль документа Диадок id {other.id}"
            )
            session.add(document)
            await session.commit()
            return {"action": "duplicate"}

    # 2) письмо
    email_msg = await _email_message_for_key(session, document)
    if email_msg is None:
        return {"action": "proceed"}
    if is_correction:
        # корректировка уже получена письмом: вторую не заводим
        _link_to_message(document, email_msg)
        session.add(document)
        await session.commit()
        return {"action": "linked", "message_id": int(email_msg.id)}
    if not email_msg.receipts:
        # письмо не дало поступления — главным остаётся Диадок
        email_msg.duplicate_of_diadoc_id = int(document.id)
        if email_msg.message_type in (None, "UNKNOWN", "IMPORT_ERROR"):
            email_msg.message_type = MESSAGE_TYPE_DUPLICATE
        email_msg.import_error_details = (
            f"Тот же документ пришёл через Диадок (id {document.id})"
        )
        session.add(email_msg)
        await session.commit()
        return {"action": "proceed"}
    attachments = list(
        (
            await session.execute(
                select(SupplierOrderAttachment).where(
                    SupplierOrderAttachment.message_id == email_msg.id
                )
            )
        )
        .scalars()
        .all()
    )
    same_file = bool(
        document.content_sha256
        and any(a.sha256 == document.content_sha256 for a in attachments)
    )
    if not same_file and totals_differ(
        document.total_amount, email_msg.document_total
    ):
        return await _flag_mismatch(
            session,
            document,
            other_message_id=int(email_msg.id),
            other_total=email_msg.document_total,
            source="письмо",
        )
    _link_to_message(document, email_msg)
    session.add(document)
    await session.commit()
    return {"action": "linked", "message_id": int(email_msg.id)}


def _link_to_message(
    document: DiadocIncomingDocument, message: SupplierOrderMessage
) -> None:
    document.duplicate_kind = "email"
    document.matched_message_id = int(message.id)
    document.supplier_order_message_id = int(message.id)
    document.status = "processed"
    document.registered_at = document.registered_at or now_moscow()
    document.import_error_details = (
        "Тот же документ уже получен по почте; второе поступление не "
        "создаётся, подпись и статусы — по Диадоку"
    )


async def _flag_mismatch(
    session: AsyncSession,
    document: DiadocIncomingDocument,
    *,
    other_message_id: Optional[int],
    other_total: Optional[Decimal],
    source: str,
) -> dict[str, Any]:
    document.amount_mismatch_status = "pending"
    document.mismatch_message_id = other_message_id
    document.mismatch_other_total = other_total
    document.status = "amount_mismatch"
    document.import_error_details = (
        f"Расхождение по сумме: Диадок {document.total_amount} ₽, "
        f"{source} {other_total} ₽ — нужно решение администратора"
    )
    session.add(document)
    await session.commit()
    if source == "письмо" and other_message_id:
        # Диадок главнее почты: заменяем черновик или заводим КСФ
        # для проведённого поступления; админа только уведомляем.
        outcome = await resolve_amount_mismatch(
            session, document_id=int(document.id), action="apply_diadoc",
            automatic=True,
        )
        notes = {
            "reposted": (
                "Проведённое поступление распроведено и проведено заново "
                "по данным Диадока."
            ),
            "draft_replaced": (
                "Непроведённое поступление заменено данными Диадока."
            ),
            "correction_required": (
                "Поступление не удалось перепровести (товар уже расходован "
                "или есть КСФ) — создана КСФ для ручной корректировки."
            ),
        }
        await _notify_mismatch(
            session, document, other_total, source, auto=True,
            note=notes.get(str(outcome.get("result") or outcome.get("status"))),
        )
        await session.commit()
        return {"action": "applied", "outcome": outcome.get("status")}
    await _notify_mismatch(session, document, other_total, source)
    await session.commit()
    return {"action": "mismatch"}


def _log(document: DiadocIncomingDocument, **entry: Any) -> None:
    entry["at"] = now_moscow().isoformat()
    document.mismatch_log = [*(document.mismatch_log or []), entry]


async def _resolve_amount_mismatch(
    session: AsyncSession,
    *,
    document_id: int,
    action: str,
    user_id: Optional[int] = None,
    automatic: bool = False,
) -> dict[str, Any]:
    """action: apply_diadoc | keep_existing."""
    from dz_fastapi.services.diadoc_documents import (
        get_diadoc_incoming_document,
        process_diadoc_incoming_document,
        register_diadoc_document_as_supplier_message,
    )
    from dz_fastapi.services.supplier_receipt_corrections import register_incoming_correction

    document = await get_diadoc_incoming_document(
        session, document_id=document_id
    )
    if document is None:
        raise LookupError("Document not found")
    if document.amount_mismatch_status != "pending":
        raise ValueError("У документа нет нерешённого расхождения по сумме")
    if action not in ("apply_diadoc", "keep_existing"):
        raise ValueError("Unknown action")

    email_msg = None
    if document.mismatch_message_id:
        email_msg = (
            await session.execute(
                select(SupplierOrderMessage)
                .options(
                    selectinload(SupplierOrderMessage.receipts).selectinload(
                        SupplierReceipt.items
                    )
                )
                .where(SupplierOrderMessage.id == document.mismatch_message_id)
            )
        ).scalar_one_or_none()
    receipts = list(email_msg.receipts) if email_msg is not None else []

    if action == "keep_existing":
        document.amount_mismatch_status = "dismissed"
        document.mismatch_resolved_at = now_moscow()
        _log(document, action=action, user_id=user_id, automatic=automatic)
        if receipts:
            _link_to_message(document, email_msg)
        else:
            document.status = "synced"
            document.import_error_details = None
        session.add(document)
        await session.commit()
        return {
            "status": "dismissed",
            "receipt_ids": [int(r.id) for r in receipts],
        }

    posted = [r for r in receipts if r.posted_at is not None]
    reposted = False
    if posted:
        # Диадок главнее: распроводим (остатки откатываются штатным
        # unpost), заменяем строки и проводим заново по данным Диадока.
        from dz_fastapi.services.supplier_workflow import unpost_supplier_receipt

        email_msg_id = int(email_msg.id)
        posted_ids = [int(r.id) for r in posted]
        try:
            for receipt_id in posted_ids:
                await unpost_supplier_receipt(
                    session,
                    receipt_id=receipt_id,
                    user=SimpleNamespace(id=user_id),
                )
            reposted = True
        except (ValueError, LookupError) as exc:
            # товар уже расходован / есть проведённая КСФ и т.п.
            await session.rollback()
            reason = str(exc)
            document = await get_diadoc_incoming_document(
                session, document_id=document_id
            )
            email_msg = await session.get(SupplierOrderMessage, email_msg_id)
            diadoc_msg = await register_diadoc_document_as_supplier_message(
                session, document=document, provider_id=document.provider_id
            )
            diadoc_msg.message_type = "CORRECTION_DOC"
            correction = await register_incoming_correction(
                session,
                provider_id=int(document.provider_id),
                message_id=int(diadoc_msg.id),
                source_kind="diadoc",
                document_number=document.document_number,
                document_date=document.document_date,
            )
            document.amount_mismatch_status = "correction_required"
            document.status = "registered"
            document.import_error_details = (
                f"Не удалось перепровести ({reason}); остатки не менялись, "
                f"требуется корректировка (КСФ #{correction.id})"
            )[:2000]
            _log(
                document,
                action=action,
                automatic=automatic,
                user_id=user_id,
                result="correction_required",
                reason=reason,
                correction_id=int(correction.id),
                receipt_ids=posted_ids,
                old_total=str(email_msg.document_total),
                new_total=str(document.total_amount),
            )
            session.add(document)
            await session.commit()
            return {
                "status": "correction_required",
                "correction_id": int(correction.id),
                "receipt_ids": posted_ids,
                "reason": reason,
            }
        # после unpost поступления снова черновики — заменяются ниже
        email_msg = (
            await session.execute(
                select(SupplierOrderMessage)
                .options(
                    selectinload(SupplierOrderMessage.receipts).selectinload(
                        SupplierReceipt.items
                    )
                )
                .where(SupplierOrderMessage.id == email_msg_id)
            )
        ).scalar_one()
        receipts = list(email_msg.receipts)

    from dz_fastapi.services.supplier_workflow import delete_supplier_receipt

    removed: list[dict[str, Any]] = []
    for receipt in receipts:
        removed.append(
            {
                "receipt_id": int(receipt.id),
                "items": [
                    {
                        "oem": i.oem_number,
                        "qty": i.received_quantity,
                        "total": str(i.total_price_with_vat),
                    }
                    for i in receipt.items
                ],
            }
        )
        # черновик: на остатки не влияет; связи заказов пересчитываются
        await delete_supplier_receipt(session, receipt_id=int(receipt.id))
    if email_msg is not None:
        email_msg.message_type = MESSAGE_TYPE_DUPLICATE
        email_msg.duplicate_of_diadoc_id = int(document.id)
        email_msg.import_error_details = (
            "Строки заменены данными Диадок после расхождения по сумме"
        )
    document.amount_mismatch_status = "applied"
    document.mismatch_resolved_at = now_moscow()
    document.status = "synced"
    document.import_error_details = None
    _log(
        document,
        action=action,
        automatic=automatic,
        user_id=user_id,
        result="reposted" if reposted else "draft_replaced",
        old_total=str(email_msg.document_total) if email_msg else None,
        removed_receipts=removed,
    )
    session.add(document)
    await session.commit()
    result = await process_diadoc_incoming_document(
        session,
        document=document,
        provider_id=int(document.provider_id),
        download_content_if_missing=False,
        register_if_needed=True,
    )
    return {
        "status": "applied",
        "result": "reposted" if reposted else "draft_replaced",
        **result,
    }


async def resolve_amount_mismatch(
    session: AsyncSession,
    *,
    document_id: int,
    action: str,
    user_id: Optional[int] = None,
    automatic: bool = False,
) -> dict[str, Any]:
    result = await _resolve_amount_mismatch(
        session,
        document_id=document_id,
        action=action,
        user_id=user_id,
        automatic=automatic,
    )
    if not automatic:
        # решение принято администратором — окно закрываем
        document = await session.get(DiadocIncomingDocument, document_id)
        if document is not None:
            document.mismatch_acknowledged_at = now_moscow()
            await close_mismatch_notifications(
                session, document_id=document_id
            )
            await session.commit()
    return result


async def find_email_receipt_for_document(
    session: AsyncSession,
    document: DiadocIncomingDocument,
    *,
    exclude_message_id: Optional[int] = None,
) -> Optional[SupplierReceipt]:
    """Поступление, созданное из письма по тому же документу (номер+дата)."""
    number = str(document.document_number or "").strip().lower()
    if not number or document.document_date is None or not document.provider_id:
        return None
    stmt = (
        select(SupplierReceipt)
        .join(
            SupplierOrderMessage,
            SupplierOrderMessage.id == SupplierReceipt.source_message_id,
        )
        .where(
            SupplierReceipt.provider_id == document.provider_id,
            func.lower(func.trim(SupplierReceipt.document_number)) == number,
            SupplierReceipt.document_date == document.document_date,
            SupplierOrderMessage.source_uid.is_not(None),
        )
        .order_by(SupplierReceipt.id.asc())
        .limit(1)
    )
    if exclude_message_id:
        stmt = stmt.where(SupplierReceipt.source_message_id != exclude_message_id)
    return (await session.execute(stmt)).scalars().first()


async def merge_diadoc_duplicate_receipt(
    session: AsyncSession,
    *,
    document_id: int,
    user_id: Optional[int] = None,
    apply: bool = False,
) -> dict[str, Any]:
    """Убирает поступление из Диадока, дублирующее поступление из письма.

    Остаётся более раннее поступление из письма; дубль из Диадока
    распроводится (остатки откатываются штатным unpost) и удаляется, документ
    Диадока привязывается к письму. По умолчанию — только предпросмотр.
    """
    from dz_fastapi.services.supplier_workflow import (
        delete_supplier_receipt,
        unpost_supplier_receipt,
    )

    document = (
        await session.execute(
            select(DiadocIncomingDocument).where(
                DiadocIncomingDocument.id == document_id
            )
        )
    ).scalar_one_or_none()
    if document is None:
        raise LookupError("Документ не найден")
    result: dict[str, Any] = {
        "document_id": int(document.id),
        "document_number": document.document_number,
        "status": "none",
    }
    if document.supplier_order_message_id is None:
        return result
    own_message = (
        await session.execute(
            select(SupplierOrderMessage)
            .options(selectinload(SupplierOrderMessage.receipts))
            .where(SupplierOrderMessage.id == document.supplier_order_message_id)
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    if own_message is None or not own_message.receipts:
        return result
    if own_message.source_uid is not None:
        return result  # это письмо, а не Диадок
    email_receipt = await find_email_receipt_for_document(
        session, document, exclude_message_id=int(own_message.id)
    )
    if email_receipt is None:
        return result
    dup_ids = [int(r.id) for r in own_message.receipts]
    result.update(
        status="duplicate",
        diadoc_receipt_ids=dup_ids,
        kept_receipt_id=int(email_receipt.id),
        diadoc_posted=any(r.posted_at is not None for r in own_message.receipts),
    )
    if not apply:
        return result

    email_message_id = int(email_receipt.source_message_id)
    kept_id = int(email_receipt.id)
    own_message_id = int(own_message.id)
    posted_ids = [
        int(r.id) for r in own_message.receipts if r.posted_at is not None
    ]
    try:
        for receipt_id in posted_ids:
            await unpost_supplier_receipt(
                session,
                receipt_id=receipt_id,
                user=SimpleNamespace(id=user_id),
            )
        for receipt_id in dup_ids:
            await delete_supplier_receipt(session, receipt_id=receipt_id)
    except (ValueError, LookupError) as exc:
        await session.rollback()
        result.update(status="failed", reason=str(exc))
        return result

    session.expire_all()  # удалённые поступления не должны оставаться в коллекциях
    document = (
        await session.execute(
            select(DiadocIncomingDocument).where(
                DiadocIncomingDocument.id == document_id
            )
        )
    ).scalar_one()
    email_message = await session.get(SupplierOrderMessage, email_message_id)
    old_message = await session.get(SupplierOrderMessage, own_message_id)
    _link_to_message(document, email_message)
    document.status = "processed"
    document.import_error_details = None
    session.add(document)
    if old_message is not None:
        old_message.message_type = MESSAGE_TYPE_DUPLICATE
        old_message.import_error_details = (
            f"Дубль поступления из письма #{kept_id}"
        )[:500]
        session.add(old_message)
    await session.commit()
    result["status"] = "merged"
    return result


async def find_diadoc_documents_with_duplicate_receipts(
    session: AsyncSession, *, limit: int = 500
) -> list[int]:
    rows = (
        await session.execute(
            select(DiadocIncomingDocument.id)
            .where(
                DiadocIncomingDocument.supplier_order_message_id.is_not(None),
                DiadocIncomingDocument.duplicate_kind.is_(None),
                DiadocIncomingDocument.status.in_(
                    ("processed", "registered", "error")
                ),
            )
            .order_by(DiadocIncomingDocument.id.desc())
            .limit(limit)
        )
    ).scalars().all()
    return [int(r) for r in rows]
