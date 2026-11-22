"""Реализации по исходящим УПД Диадока, оформленным вне системы.

Реализации оформляют в 1С, а УПД уходит клиенту через Диадок. Чтобы такие
продажи были видны в системе (список реализаций, анализ), по УПД создаётся
``ShipmentDocument`` со статусом ``IMPORTED``. Он не списывает остатки, не
уходит в 1С и не попадает в конвейер ЭДО: те отбирают только DRAFT/POSTED.

Защита от дублей: один УПД — одна реализация (``source_diadoc_document_id``),
а если в системе уже есть наша реализация того же клиента с той же суммой
в пределах нескольких дней, УПД считается её отправкой и не импортируется.
"""

from __future__ import annotations

import logging
import os
import re
from datetime import date, datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal
from typing import Any, Optional

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from dz_fastapi.core.time import MOSCOW_TZ, now_moscow
from dz_fastapi.models.autopart import AutoPart, preprocess_oem_number
from dz_fastapi.models.diadoc import DiadocOutgoingDocument
from dz_fastapi.models.inventory import (
    ShipmentDocument,
    ShipmentDocumentItem,
    ShipmentDocumentStatus,
    SyncStatus,
)
from dz_fastapi.models.partner import Customer
from dz_fastapi.services.diadoc_direction import get_our_inn
from dz_fastapi.services.upd_xml import UpdXmlDocument, extract_document_parties, parse_upd_xml

logger = logging.getLogger("dz_fastapi")

UPD_TYPE = "UniversalTransferDocument"
DUPLICATE_WINDOW_DAYS = 3
KNOWN_VAT_RATES = (Decimal("0"), Decimal("10"), Decimal("20"), Decimal("22"))
DEFAULT_VAT = Decimal("22")


def _q2(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def _vat_rate(net_price: Optional[float], total: Optional[float], qty: Optional[int]) -> Decimal:
    """Ставка НДС по строке: брутто / нетто, привязанная к известным ставкам."""
    if not net_price or not total or not qty:
        return DEFAULT_VAT
    ratio = Decimal(str(total)) / (Decimal(str(net_price)) * Decimal(qty))
    percent = (ratio - 1) * 100
    return min(KNOWN_VAT_RATES, key=lambda rate: abs(rate - percent))


async def _find_customer(
    session: AsyncSession, outgoing: DiadocOutgoingDocument, buyer_inn: Optional[str]
) -> Optional[Customer]:
    if outgoing.customer_id:
        return await session.get(Customer, outgoing.customer_id)
    if buyer_inn:
        return (
            (await session.execute(select(Customer).where(Customer.inn == buyer_inn)))
            .scalars()
            .first()
        )
    return None


_WORD_RE = re.compile(r"[0-9a-zа-яё]{3,}", re.IGNORECASE)
MATCH_THRESHOLD = 0.5


def _name_tokens(value: Optional[str]) -> set[str]:
    return {w.lower().replace("ё", "е") for w in _WORD_RE.findall(value or "")}


def name_score(upd_name: Optional[str], part_name: Optional[str], brand: Optional[str]) -> float:
    """Похожесть названия строки УПД на название номенклатуры (0..1).

    Артикулы разных брендов совпадают (21968 у FEBI и у другого бренда), поэтому
    одного артикула мало: сверяем и слова названия, и упоминание бренда.
    """
    a, b = _name_tokens(upd_name), _name_tokens(part_name)
    if not a or not b:
        return 0.0
    common = len(a & b)
    score = max(common / len(a | b), common / min(len(a), len(b)) * 0.8)
    if brand and brand.lower() in (upd_name or "").lower():
        score = min(1.0, score + 0.3)
    return round(score, 3)


async def _find_autopart(
    session: AsyncSession, article: Optional[str], name: Optional[str]
) -> tuple[Optional[int], Optional[str]]:
    """Номенклатура по артикулу УПД; при нескольких брендах — лучшая по названию.

    Если даже лучший кандидат не похож по названию, привязку не делаем:
    ошибочная связь хуже, чем строка «на сопоставление».
    """
    if not article:
        return None, None
    normalized = preprocess_oem_number(str(article))
    if not normalized:
        return None, None
    parts = (
        (
            await session.execute(
                select(AutoPart)
                .options(selectinload(AutoPart.brand))
                .where(AutoPart.oem_number == normalized)
                .order_by(AutoPart.id)
            )
        )
        .scalars()
        .all()
    )
    best: Optional[AutoPart] = None
    best_score = 0.0
    for part in parts:
        score = name_score(name, part.name, part.brand.name if part.brand else None)
        if score > best_score:
            best, best_score = part, score
    if best is None or best_score < MATCH_THRESHOLD:
        return None, None
    return int(best.id), "exact"


def _document_datetime(upd: UpdXmlDocument, outgoing: DiadocOutgoingDocument) -> datetime:
    day: Optional[date] = upd.shipment_date or upd.document_date or outgoing.document_date
    if day is None:
        return now_moscow()
    return datetime(day.year, day.month, day.day, 12, 0, tzinfo=MOSCOW_TZ)


async def _duplicate_of_system_shipment(
    session: AsyncSession,
    *,
    customer_id: Optional[int],
    total: Decimal,
    moment: datetime,
) -> Optional[int]:
    """Наша собственная реализация того же клиента на ту же сумму рядом по дате."""
    if customer_id is None or total <= 0:
        return None
    rows = (
        (
            await session.execute(
                select(ShipmentDocument).where(
                    ShipmentDocument.customer_id == customer_id,
                    ShipmentDocument.status.in_(
                        [ShipmentDocumentStatus.DRAFT, ShipmentDocumentStatus.POSTED]
                    ),
                    ShipmentDocument.doc_date >= moment - timedelta(days=DUPLICATE_WINDOW_DAYS),
                    ShipmentDocument.doc_date <= moment + timedelta(days=DUPLICATE_WINDOW_DAYS),
                )
            )
        )
        .scalars()
        .all()
    )
    for shipment in rows:
        shipment_total = sum(
            (Decimal(str(item.price or 0)) * item.quantity for item in shipment.items),
            Decimal("0"),
        )
        if abs(_q2(shipment_total) - total) <= Decimal("1.00"):
            return int(shipment.id)
    return None


def _mark(outgoing: DiadocOutgoingDocument, **info: Any) -> None:
    outgoing.metadata_json = {**(outgoing.metadata_json or {}), "shipment_import": info}


async def import_shipment_from_outgoing_upd(
    session: AsyncSession,
    outgoing: DiadocOutgoingDocument,
    *,
    our_inn: Optional[str] = None,
) -> dict[str, Any]:
    """Создаёт импортную реализацию по одному исходящему УПД."""
    existing = await session.scalar(
        select(ShipmentDocument.id).where(ShipmentDocument.source_diadoc_document_id == outgoing.id)
    )
    if existing is not None:
        return {"created": False, "reason": "already_imported", "shipment_id": existing}
    if outgoing.type_named_id != UPD_TYPE:
        return {"created": False, "reason": "not_upd"}
    if outgoing.status in ("revoked", "rejected"):
        _mark(outgoing, skipped="revoked")
        session.add(outgoing)
        return {"created": False, "reason": "revoked"}
    path = os.path.abspath(outgoing.local_file_path) if outgoing.local_file_path else None
    if not path or not os.path.exists(path):
        return {"created": False, "reason": "no_file"}

    with open(path, "rb") as fh:
        payload = fh.read()
    try:
        upd = parse_upd_xml(payload)
    except ValueError as exc:
        _mark(outgoing, skipped="unparsable", error=str(exc)[:200])
        session.add(outgoing)
        return {"created": False, "reason": "unparsable"}
    parties = extract_document_parties(payload)
    if our_inn and parties and parties.seller_inn and parties.seller_inn != our_inn:
        _mark(outgoing, skipped="seller_is_not_us")
        session.add(outgoing)
        return {"created": False, "reason": "seller_is_not_us"}
    if not upd.items:
        _mark(outgoing, skipped="no_items")
        session.add(outgoing)
        return {"created": False, "reason": "no_items"}

    customer = await _find_customer(session, outgoing, parties.buyer_inn if parties else None)
    moment = _document_datetime(upd, outgoing)
    total = _q2(Decimal(str(upd.total_with_vat or 0)))
    duplicate_id = await _duplicate_of_system_shipment(
        session,
        customer_id=customer.id if customer else None,
        total=total,
        moment=moment,
    )
    if duplicate_id is not None:
        _mark(outgoing, skipped="system_shipment_exists", shipment_id=duplicate_id)
        session.add(outgoing)
        return {"created": False, "reason": "system_shipment_exists", "shipment_id": duplicate_id}

    number = upd.document_number or outgoing.document_number
    document = ShipmentDocument(
        doc_number=str(number) if number else None,
        doc_date=moment,
        status=ShipmentDocumentStatus.IMPORTED,
        customer_id=customer.id if customer else None,
        reason=f"Импорт из Диадока: УПД № {number or '—'} от {moment:%d.%m.%Y}",
        sync_status=SyncStatus.SYNCED,
        source_diadoc_document_id=outgoing.id,
    )
    session.add(document)
    await session.flush()

    matched = 0
    review: list[dict[str, Any]] = []
    for line in upd.items:
        if not line.quantity or line.quantity <= 0:
            continue
        autopart_id, match = await _find_autopart(session, line.oem_number, line.name)
        gross_unit = (
            _q2(Decimal(str(line.total_with_vat)) / line.quantity)
            if line.total_with_vat
            else _q2(Decimal(str(line.price or 0)))
        )
        vat_rate = _vat_rate(line.price, line.total_with_vat, line.quantity)
        vat_amount = None
        if line.price and line.total_with_vat:
            vat_amount = _q2(
                Decimal(str(line.total_with_vat)) - Decimal(str(line.price)) * line.quantity
            )
        elif line.total_with_vat:
            # В строке нет цены без НДС: считаем НДС из суммы по ставке.
            total_line = Decimal(str(line.total_with_vat))
            vat_amount = _q2(total_line - total_line / (1 + vat_rate / 100))
        details = {
            "import_article": line.oem_number,
            "import_gtd_number": line.gtd_code,
            "import_country_code": line.country_code,
            "import_country_name": line.country_name,
            "import_marking_codes": list(line.marking_codes or []) or None,
            "import_vat_amount": vat_amount,
        }
        if autopart_id is None:
            review.append(
                {
                    "line": line.line_number,
                    "article": line.oem_number,
                    "name": line.name,
                    "quantity": line.quantity,
                    "price": str(gross_unit),
                    "vat_rate": str(vat_rate),
                    "vat_amount": str(vat_amount) if vat_amount is not None else None,
                    "gtd": line.gtd_code,
                    "country_code": line.country_code,
                    "country_name": line.country_name,
                    "marking_codes": list(line.marking_codes or []),
                }
            )
            continue
        session.add(
            ShipmentDocumentItem(
                document_id=document.id,
                autopart_id=autopart_id,
                customer_oem=line.oem_number,
                customer_name=(line.name or "")[:512] or None,
                quantity=line.quantity,
                price=gross_unit,
                vat_rate=vat_rate,
                import_match=match,
                **details,
            )
        )
        matched += 1
    document.import_review = review or None
    notes = [f"Сумма УПД с НДС: {total}. Строк в УПД: {len(upd.items)}, сопоставлено: {matched}."]
    if review:
        notes.append(
            f"На сопоставление: {len(review)} (артикул не найден или название не совпало)."
        )
    if customer is None:
        notes.append("Клиент не определён по ящику и ИНН покупателя.")
    document.notes = "\n".join(notes)
    _mark(outgoing, shipment_id=document.id, matched=matched, unmatched=len(review))
    session.add_all([document, outgoing])
    await session.flush()
    return {
        "created": True,
        "reason": "created",
        "shipment_id": document.id,
        "matched": matched,
        "unmatched": len(review),
    }


async def import_pending_upd_shipments(
    session: AsyncSession, *, environment: str, limit: int = 200
) -> dict[str, Any]:
    """Реализации по ещё не обработанным внешним исходящим УПД."""
    our_inn = await get_our_inn(session, environment)
    rows = (
        (
            await session.execute(
                select(DiadocOutgoingDocument)
                .outerjoin(
                    ShipmentDocument,
                    ShipmentDocument.source_diadoc_document_id == DiadocOutgoingDocument.id,
                )
                .where(
                    DiadocOutgoingDocument.environment == environment,
                    DiadocOutgoingDocument.source_type == "diadoc_external",
                    DiadocOutgoingDocument.type_named_id == UPD_TYPE,
                    ShipmentDocument.id.is_(None),
                    DiadocOutgoingDocument.metadata_json[("shipment_import", "skipped")]
                    .as_string()
                    .is_(None),
                )
                .order_by(DiadocOutgoingDocument.id)
                .limit(max(1, int(limit)))
            )
        )
        .scalars()
        .all()
    )
    result: dict[str, Any] = {"checked": 0, "created": 0, "skipped": {}, "errors": []}
    for outgoing in rows:
        result["checked"] += 1
        try:
            async with session.begin_nested():
                item = await import_shipment_from_outgoing_upd(session, outgoing, our_inn=our_inn)
        except Exception as exc:  # noqa: BLE001
            logger.exception("Shipment import failed for diadoc doc %s", outgoing.id)
            result["errors"].append(f"{outgoing.id}: {exc}"[:300])
            continue
        if item["created"]:
            result["created"] += 1
        else:
            reason = item["reason"]
            result["skipped"][reason] = result["skipped"].get(reason, 0) + 1
    await session.commit()
    return result


async def resolve_review_line(
    session: AsyncSession,
    shipment: ShipmentDocument,
    *,
    index: int,
    autopart_id: int,
) -> ShipmentDocumentItem:
    """Сопоставляет строку «на сопоставление» с номенклатурой вручную."""
    if shipment.status != ShipmentDocumentStatus.IMPORTED:
        raise ValueError("Сопоставление доступно только для импортных реализаций")
    lines = list(shipment.import_review or [])
    if not 0 <= index < len(lines):
        raise ValueError("Строка не найдена")
    if await session.get(AutoPart, autopart_id) is None:
        raise ValueError("Номенклатура не найдена")
    line = lines.pop(index)
    item = ShipmentDocumentItem(
        document_id=shipment.id,
        autopart_id=autopart_id,
        customer_oem=line.get("article"),
        customer_name=(line.get("name") or "")[:512] or None,
        quantity=int(line["quantity"]),
        price=Decimal(str(line.get("price") or 0)),
        vat_rate=Decimal(str(line.get("vat_rate") or DEFAULT_VAT)),
        import_article=line.get("article"),
        import_match="manual",
        import_gtd_number=line.get("gtd"),
        import_country_code=line.get("country_code"),
        import_country_name=line.get("country_name"),
        import_marking_codes=line.get("marking_codes") or None,
        import_vat_amount=(
            Decimal(str(line["vat_amount"])) if line.get("vat_amount") is not None else None
        ),
    )
    session.add(item)
    shipment.import_review = lines or None
    session.add(shipment)
    await session.flush()
    return item


async def rebuild_imported_shipments(session: AsyncSession, *, environment: str) -> int:
    """Удаляет импортные реализации и очищает отметки, чтобы собрать заново.

    Нужно после улучшения разбора. Затрагивает только статус IMPORTED: наши
    собственные реализации не трогаются. Возвращает число удалённых.
    """
    ids = (
        (
            await session.execute(
                select(ShipmentDocument.id).where(
                    ShipmentDocument.status == ShipmentDocumentStatus.IMPORTED
                )
            )
        )
        .scalars()
        .all()
    )
    if ids:
        await session.execute(
            delete(ShipmentDocumentItem).where(ShipmentDocumentItem.document_id.in_(ids))
        )
        await session.execute(delete(ShipmentDocument).where(ShipmentDocument.id.in_(ids)))
    outgoing = (
        (
            await session.execute(
                select(DiadocOutgoingDocument).where(
                    DiadocOutgoingDocument.environment == environment,
                    DiadocOutgoingDocument.source_type == "diadoc_external",
                )
            )
        )
        .scalars()
        .all()
    )
    for item in outgoing:
        meta = dict(item.metadata_json or {})
        if meta.pop("shipment_import", None) is not None:
            item.metadata_json = meta
            session.add(item)
    await session.commit()
    return len(ids)
