"""Автоматическое заведение входящих документов поставщиков (счета, акты)
из Диадока в реестр SupplierDocument для отчёта «Кредиторы»."""

from __future__ import annotations

import re
from datetime import date, datetime
from decimal import Decimal
from typing import Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.models.diadoc import DiadocIncomingDocument
from dz_fastapi.models.finance import SupplierDocument

_INVOICE_NAME_RE = re.compile(r"^\s*сч[её]т", re.IGNORECASE)
_RECONCILIATION_RE = re.compile(r"сверк", re.IGNORECASE)
_NUMBER_DATE_RE = re.compile(
    r"№\s*(?P<number>\S+)\s+от\s+(?P<date>\d{2}\.\d{2}\.\d{4})",
    re.IGNORECASE,
)


def classify_supplier_document(
    document: DiadocIncomingDocument,
) -> Optional[str]:
    """Тип документа для реестра или None, если он в реестр не идёт."""
    name = str(document.file_name or "")
    if _RECONCILIATION_RE.search(name):
        return None  # акт сверки долга не создаёт
    if document.document_kind == "act":
        return "act"
    if _INVOICE_NAME_RE.match(name):
        return "invoice"
    return None


def _number_and_date(
    document: DiadocIncomingDocument,
) -> tuple[Optional[str], Optional[date]]:
    number = str(document.document_number or "").strip() or None
    doc_date = document.document_date
    if isinstance(doc_date, datetime):
        doc_date = doc_date.date()
    match = _NUMBER_DATE_RE.search(str(document.file_name or ""))
    if match:
        number = number or match.group("number")
        if doc_date is None:
            try:
                doc_date = datetime.strptime(
                    match.group("date"), "%d.%m.%Y"
                ).date()
            except ValueError:
                pass
    return number, doc_date


async def register_supplier_document_from_diadoc(
    session: AsyncSession,
    document: DiadocIncomingDocument,
) -> Optional[SupplierDocument]:
    """Создаёт запись реестра по счёту/акту поставщика (идемпотентно)."""
    if not document.id or document.provider_id is None:
        return None
    if document.direction == "customer":
        return None
    doc_type = classify_supplier_document(document)
    if doc_type is None:
        return None
    existing = (
        await session.execute(
            select(SupplierDocument).where(
                SupplierDocument.diadoc_document_id == document.id
            )
        )
    ).scalar_one_or_none()
    if existing is not None:
        return existing
    number, doc_date = _number_and_date(document)
    amount = (
        Decimal(str(document.total_amount))
        if document.total_amount is not None
        else None
    )
    row = SupplierDocument(
        provider_id=int(document.provider_id),
        doc_type=doc_type,
        number=number,
        doc_date=doc_date,
        amount=amount,
        status="open",
        diadoc_document_id=int(document.id),
        file_name=document.file_name,
    )
    session.add(row)
    await session.flush()
    return row
