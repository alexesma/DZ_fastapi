"""Safe customer-return draft flow used before a legal UKD is produced."""

from __future__ import annotations

import re
from datetime import date, datetime
from decimal import ROUND_HALF_UP, Decimal
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from dz_fastapi.models.diadoc import DiadocOutgoingDocument
from dz_fastapi.models.inventory import (
    ReturnDocumentStatus,
    ReturnFromCustomer,
    ReturnItem,
    ShipmentDocument,
    ShipmentDocumentItem,
)
from dz_fastapi.models.partner import Customer, Reclamation, ReclamationItem
from dz_fastapi.services.inventory_stock import (
    approve_return_from_customer,
    reject_return_from_customer,
)

DEFAULT_WHOLESALE_VAT_RATE = Decimal("22.00")


def _normalized(value: Any) -> str:
    return re.sub(r"[^A-ZА-Я0-9]", "", str(value or "").upper())


def _as_date(value: Any) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value)) if value else None
    except ValueError:
        return None


def _as_decimal(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        return Decimal(str(value).replace(",", "."))
    except Exception:  # noqa: BLE001
        return None


def _matches_source_basis(
    *,
    document_number: Any,
    document_date: Any,
    basis_number: str | None,
    basis_date: date | None,
) -> bool:
    if basis_number and _normalized(document_number) != _normalized(basis_number):
        return False
    if basis_date and _as_date(document_date) != basis_date:
        return False
    return True


def _gross_unit_price(item: dict[str, Any]) -> Decimal | None:
    quantity = Decimal(str(max(1, int(item.get("quantity") or 1))))
    total = _as_decimal(item.get("total_with_vat"))
    if total is not None:
        return (total / quantity).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    net = _as_decimal(item.get("unit_price_without_vat"))
    vat_rate = _as_decimal(item.get("vat_rate")) or DEFAULT_WHOLESALE_VAT_RATE
    if net is None:
        return None
    return (net * (Decimal("1") + vat_rate / Decimal("100"))).quantize(
        Decimal("0.01"),
        rounding=ROUND_HALF_UP,
    )


async def _resolve_customer_id(
    session: AsyncSession,
    *,
    current_customer_id: int | None,
    extraction: dict[str, Any],
) -> int | None:
    seller_inn = _normalized(extraction.get("seller_inn"))
    seller_kpp = _normalized(extraction.get("seller_kpp"))
    if not seller_inn:
        return current_customer_id
    stmt = select(Customer).where(Customer.inn == seller_inn)
    if seller_kpp:
        stmt = stmt.where(Customer.kpp == seller_kpp)
    matches = (await session.execute(stmt)).scalars().all()
    if len(matches) == 1:
        return int(matches[0].id)
    return current_customer_id


async def _load_return(
    session: AsyncSession,
    return_id: int,
) -> ReturnFromCustomer | None:
    return (
        await session.execute(
            select(ReturnFromCustomer)
            .where(ReturnFromCustomer.id == return_id)
            .options(
                selectinload(ReturnFromCustomer.customer),
                selectinload(ReturnFromCustomer.shipment_document).selectinload(
                    ShipmentDocument.items
                ),
                selectinload(ReturnFromCustomer.source_diadoc_outgoing_document),
                selectinload(ReturnFromCustomer.diadoc_outgoing_document),
                selectinload(ReturnFromCustomer.items)
                .selectinload(ReturnItem.shipment_item)
                .selectinload(ShipmentDocumentItem.autopart),
                selectinload(ReturnFromCustomer.items).selectinload(
                    ReturnItem.autopart
                ),
            )
        )
    ).scalar_one_or_none()


async def _find_source_shipment(
    session: AsyncSession,
    *,
    customer_id: int | None,
    document_number: str | None,
    document_date: date | None,
) -> ShipmentDocument | None:
    if not customer_id or not document_number:
        return None
    stmt = (
        select(ShipmentDocument)
        .where(ShipmentDocument.customer_id == customer_id)
        .options(
            selectinload(ShipmentDocument.items).selectinload(
                ShipmentDocumentItem.autopart
            )
        )
        .order_by(ShipmentDocument.id.desc())
    )
    rows = (await session.execute(stmt)).scalars().unique().all()
    number = _normalized(document_number)
    matches = [row for row in rows if _normalized(row.doc_number) == number]
    if document_date:
        matches = [
            row
            for row in matches
            if _as_date(getattr(row, "doc_date", None)) == document_date
        ]
    return matches[0] if len(matches) == 1 else None


async def _find_source_upd(
    session: AsyncSession,
    *,
    shipment: ShipmentDocument | None,
    customer_id: int | None,
    document_number: str | None,
    document_date: date | None,
) -> DiadocOutgoingDocument | None:
    stmt = select(DiadocOutgoingDocument).where(
        DiadocOutgoingDocument.type_named_id == "UniversalTransferDocument",
    )
    if shipment is not None:
        exact_rows = (
            await session.execute(
                stmt.where(
                    DiadocOutgoingDocument.source_type == "shipment_document",
                    DiadocOutgoingDocument.source_id == shipment.id,
                ).order_by(DiadocOutgoingDocument.id.desc())
            )
        ).scalars().all()
        exact_rows = [
            row
            for row in exact_rows
            if _matches_source_basis(
                document_number=row.document_number,
                document_date=row.document_date,
                basis_number=document_number,
                basis_date=document_date,
            )
        ]
        exact_valid = [
            row
            for row in exact_rows
            if not row.is_draft
            and row.status not in {"draft", "error", "rejected", "revoked"}
        ]
        exact_candidates = exact_valid or exact_rows
        if exact_candidates:
            return max(exact_candidates, key=lambda row: int(row.id))
    if not customer_id or not document_number:
        return None
    stmt = stmt.where(DiadocOutgoingDocument.customer_id == customer_id)
    rows = (await session.execute(stmt)).scalars().all()
    normalized_number = _normalized(document_number)
    matches = [
        row
        for row in rows
        if _normalized(row.document_number) == normalized_number
        and (
            document_date is None
            or _as_date(row.document_date) == document_date
        )
    ]
    valid = [
        row
        for row in matches
        if not row.is_draft
        and row.status not in {"draft", "error", "rejected", "revoked"}
    ]
    candidates = valid or matches
    return max(candidates, key=lambda row: int(row.id)) if candidates else None


def _match_shipment_item(
    shipment: ShipmentDocument | None,
    oem_number: str | None,
) -> ShipmentDocumentItem | None:
    if shipment is None or not oem_number:
        return None
    target = _normalized(oem_number)
    matches = [
        item
        for item in (shipment.items or [])
        if _normalized(getattr(getattr(item, "autopart", None), "oem_number", None))
        == target
    ]
    return matches[0] if len(matches) == 1 else None


async def create_customer_return_draft_from_reclamation(
    session: AsyncSession,
    *,
    reclamation: Reclamation,
    extraction: dict[str, Any],
) -> ReturnFromCustomer | None:
    if extraction.get("parser") != "customer_return_upd_xlsx":
        return None
    source_hash = str(extraction.get("source_sha256") or "").strip() or None
    if source_hash:
        existing = (
            await session.execute(
                select(ReturnFromCustomer).where(
                    ReturnFromCustomer.source_file_sha256 == source_hash
                )
            )
        ).scalar_one_or_none()
        if existing is not None:
            reclamation.return_from_customer_id = existing.id
            return await _load_return(session, int(existing.id))

    source_number = str(extraction.get("original_document_number") or "").strip() or None
    source_date = _as_date(extraction.get("original_document_date"))
    customer_id = await _resolve_customer_id(
        session,
        current_customer_id=reclamation.customer_id,
        extraction=extraction,
    )
    if customer_id is not None and customer_id != reclamation.customer_id:
        reclamation.customer_id = customer_id
    shipment = await _find_source_shipment(
        session,
        customer_id=customer_id,
        document_number=source_number,
        document_date=source_date,
    )
    source_upd = await _find_source_upd(
        session,
        shipment=shipment,
        customer_id=customer_id,
        document_number=source_number,
        document_date=source_date,
    )

    draft = ReturnFromCustomer(
        doc_number=None,
        customer_id=customer_id,
        shipment_document_id=shipment.id if shipment else None,
        source_diadoc_outgoing_document_id=source_upd.id if source_upd else None,
        warehouse_id=shipment.warehouse_id if shipment else None,
        source_kind="customer_return_upd_xlsx",
        external_document_number=(
            str(extraction.get("document_number") or "").strip() or None
        ),
        external_document_date=_as_date(extraction.get("document_date")),
        source_document_number=source_number,
        source_document_date=source_date,
        source_file_name=str(extraction.get("filename") or "").strip() or None,
        source_file_sha256=source_hash,
        reason=str(extraction.get("reason") or "Возврат товара").strip(),
        notes="Черновик создан автоматически из документа возврата клиента.",
        status=ReturnDocumentStatus.CREATED,
    )
    session.add(draft)
    await session.flush()

    reclamation_rows = (
        await session.execute(
            select(ReclamationItem).where(
                ReclamationItem.reclamation_id == reclamation.id
            )
        )
    ).scalars().all()
    reclamation_items = {
        _normalized(item.oem_number): item
        for item in reclamation_rows
        if item.oem_number
    }
    for parsed in extraction.get("items") or []:
        oem = _normalized(parsed.get("oem_number"))
        shipment_item = _match_shipment_item(shipment, oem)
        reclamation_item: ReclamationItem | None = reclamation_items.get(oem)
        vat_rate = _as_decimal(parsed.get("vat_rate")) or DEFAULT_WHOLESALE_VAT_RATE
        price = (
            Decimal(str(shipment_item.price))
            if shipment_item is not None and shipment_item.price is not None
            else _gross_unit_price(parsed)
        )
        session.add(
            ReturnItem(
                return_from_customer_id=draft.id,
                shipment_item_id=shipment_item.id if shipment_item else None,
                autopart_id=(
                    shipment_item.autopart_id
                    if shipment_item is not None
                    else getattr(reclamation_item, "autopart_id", None)
                ),
                quantity=max(1, int(parsed.get("quantity") or 1)),
                price=price,
                vat_rate=(
                    Decimal(str(shipment_item.vat_rate))
                    if shipment_item is not None
                    else vat_rate
                ),
                price_includes_vat=True,
                gtd_number=parsed.get("gtd_number"),
                country_code=parsed.get("country_code"),
                country_name=parsed.get("country_name"),
                oem_number=oem,
                brand_name=parsed.get("brand_name"),
                autopart_name=parsed.get("autopart_name"),
                notes="Импортировано из документа клиента",
            )
        )
    await session.flush()
    reclamation.return_from_customer_id = draft.id
    session.add(reclamation)
    return await _load_return(session, int(draft.id))


async def rematch_customer_return_draft(
    session: AsyncSession,
    *,
    return_id: int,
) -> ReturnFromCustomer:
    draft = await _load_return(session, return_id)
    if draft is None:
        raise ValueError("Черновик возврата не найден")
    if draft.status != ReturnDocumentStatus.CREATED:
        raise ValueError("Повторно сопоставлять можно только новый черновик")
    shipment = await _find_source_shipment(
        session,
        customer_id=draft.customer_id,
        document_number=draft.source_document_number,
        document_date=draft.source_document_date,
    )
    draft.shipment_document_id = shipment.id if shipment else None
    source_upd = await _find_source_upd(
        session,
        shipment=shipment,
        customer_id=draft.customer_id,
        document_number=draft.source_document_number,
        document_date=draft.source_document_date,
    )
    draft.source_diadoc_outgoing_document_id = source_upd.id if source_upd else None
    for item in draft.items or []:
        matched = _match_shipment_item(shipment, item.oem_number)
        item.shipment_item_id = matched.id if matched else None
        if matched is not None:
            item.autopart_id = matched.autopart_id
            item.price = matched.price
            item.vat_rate = matched.vat_rate
            session.add(item)
    session.add(draft)
    await session.flush()
    return await _load_return(session, return_id)  # type: ignore[return-value]


async def _prior_corrected_quantity(
    session: AsyncSession,
    *,
    shipment_item_id: int,
    exclude_return_id: int,
) -> int:
    value = await session.scalar(
        select(func.coalesce(func.sum(ReturnItem.quantity), 0))
        .join(
            ReturnFromCustomer,
            ReturnFromCustomer.id == ReturnItem.return_from_customer_id,
        )
        .where(
            ReturnItem.shipment_item_id == shipment_item_id,
            ReturnFromCustomer.id != exclude_return_id,
            ReturnFromCustomer.status == ReturnDocumentStatus.CONFIRMED,
            ReturnFromCustomer.diadoc_outgoing_document_id.is_not(None),
        )
    )
    return int(value or 0)


async def build_customer_return_draft_status(
    session: AsyncSession,
    *,
    return_id: int,
) -> dict[str, Any]:
    draft = await _load_return(session, return_id)
    if draft is None:
        raise ValueError("Черновик возврата не найден")
    blockers: list[str] = []
    warnings: list[str] = []
    line_rows: list[dict[str, Any]] = []
    if draft.customer_id is None:
        blockers.append("Не определено юридическое лицо клиента.")
    if not draft.source_document_number:
        blockers.append("В документе возврата не распознан номер исходной УПД.")
    if not draft.source_document_date:
        blockers.append("В документе возврата не распознана дата исходной УПД.")
    if draft.shipment_document_id is None:
        blockers.append("Не найдена исходная реализация в складских документах.")
    else:
        shipment = draft.shipment_document
        if shipment is None:
            blockers.append("Привязанная исходная реализация недоступна.")
        elif not _matches_source_basis(
            document_number=shipment.doc_number,
            document_date=shipment.doc_date,
            basis_number=draft.source_document_number,
            basis_date=draft.source_document_date,
        ):
            blockers.append(
                "Реквизиты реализации не совпадают с основанием передачи "
                "во входящем УПД."
            )
    if draft.source_diadoc_outgoing_document_id is None:
        blockers.append("Не найдена наша исходящая УПД в Диадоке.")
    else:
        source_upd = draft.source_diadoc_outgoing_document
        if source_upd is None:
            blockers.append("Привязанная исходящая УПД недоступна.")
        elif source_upd.type_named_id != "UniversalTransferDocument":
            blockers.append("Привязанный документ Диадока не является УПД.")
        elif source_upd.source_type != "shipment_document":
            blockers.append("Исходящая УПД не связана с нашей реализацией.")
        elif source_upd.source_id != draft.shipment_document_id:
            blockers.append("Исходящая УПД относится к другой реализации.")
        elif not _matches_source_basis(
            document_number=source_upd.document_number,
            document_date=source_upd.document_date,
            basis_number=draft.source_document_number,
            basis_date=draft.source_document_date,
        ):
            blockers.append(
                "Реквизиты исходящей УПД не совпадают с основанием передачи "
                "во входящем УПД."
            )
        elif source_upd.is_draft or source_upd.status == "draft":
            blockers.append("Исходящая УПД ещё является черновиком Диадока.")
        elif source_upd.status in {"error", "rejected", "revoked"}:
            blockers.append(
                "Исходящая УПД имеет недопустимый статус: "
                f"{source_upd.status}."
            )
        elif not source_upd.message_id or not source_upd.entity_id:
            blockers.append("У исходящей УПД нет идентификаторов Диадока.")
    if draft.status != ReturnDocumentStatus.CONFIRMED:
        blockers.append("Возврат ещё не подтверждён складом.")
    if not draft.items:
        blockers.append("В черновике нет товарных строк.")

    for item in draft.items or []:
        source = item.shipment_item
        row_blockers: list[str] = []
        prior_qty = 0
        source_qty = int(getattr(source, "quantity", 0) or 0)
        if source is None:
            row_blockers.append("Строка не сопоставлена со строкой исходной УПД.")
        else:
            prior_qty = await _prior_corrected_quantity(
                session,
                shipment_item_id=int(source.id),
                exclude_return_id=int(draft.id),
            )
            available = source_qty - prior_qty
            if int(item.quantity or 0) > available:
                row_blockers.append(
                    f"Запрошено {item.quantity} шт., доступно к корректировке "
                    f"{max(0, available)} шт."
                )
            if source.price is None:
                row_blockers.append("В исходной строке отсутствует цена.")
            if Decimal(str(source.vat_rate or 0)) != Decimal("22.00"):
                row_blockers.append("Ставка НДС исходной строки отличается от 22%.")
        if Decimal(str(item.vat_rate or 0)) != Decimal("22.00"):
            row_blockers.append("В строке возврата ставка НДС отличается от 22%.")
        blockers.extend(
            f"{item.brand_name or ''} {item.oem_number or ''}: {text}".strip()
            for text in row_blockers
        )
        line_rows.append(
            {
                "return_item_id": int(item.id),
                "shipment_item_id": item.shipment_item_id,
                "brand_name": item.brand_name,
                "oem_number": item.oem_number,
                "name": item.autopart_name,
                "return_quantity": int(item.quantity or 0),
                "source_quantity": source_qty if source else None,
                "previously_corrected_quantity": prior_qty,
                "quantity_before": source_qty - prior_qty if source else None,
                "quantity_after": (
                    source_qty - prior_qty - int(item.quantity or 0)
                    if source
                    else None
                ),
                "gross_unit_price": str(item.price) if item.price is not None else None,
                "vat_rate": str(item.vat_rate or DEFAULT_WHOLESALE_VAT_RATE),
                "blockers": row_blockers,
            }
        )
    if draft.diadoc_outgoing_document_id:
        blockers.append("УКД по этому возврату уже создан в Диадоке.")
    shipment = draft.shipment_document
    source_upd = draft.source_diadoc_outgoing_document
    source_basis_verified = bool(
        draft.source_document_number
        and draft.source_document_date
        and shipment is not None
        and source_upd is not None
        and _matches_source_basis(
            document_number=shipment.doc_number,
            document_date=shipment.doc_date,
            basis_number=draft.source_document_number,
            basis_date=draft.source_document_date,
        )
        and _matches_source_basis(
            document_number=source_upd.document_number,
            document_date=source_upd.document_date,
            basis_number=draft.source_document_number,
            basis_date=draft.source_document_date,
        )
        and source_upd.source_type == "shipment_document"
        and source_upd.source_id == draft.shipment_document_id
    )
    return {
        "return_id": int(draft.id),
        "status": str(draft.status.value),
        "external_document_number": draft.external_document_number,
        "external_document_date": draft.external_document_date,
        "source_document_number": draft.source_document_number,
        "source_document_date": draft.source_document_date,
        "shipment_document_id": draft.shipment_document_id,
        "source_diadoc_outgoing_document_id": draft.source_diadoc_outgoing_document_id,
        "source_basis_verified": source_basis_verified,
        "ukd_outgoing_document_id": draft.diadoc_outgoing_document_id,
        "ready_to_issue": not blockers and not draft.diadoc_outgoing_document_id,
        "blockers": list(dict.fromkeys(blockers)),
        "warnings": warnings,
        "items": line_rows,
    }


async def link_customer_return_source(
    session: AsyncSession,
    *,
    return_id: int,
    shipment_document_id: int,
    source_diadoc_outgoing_document_id: int,
) -> ReturnFromCustomer:
    """Bind a return draft to the exact realization and its outgoing UPD."""
    draft = await _load_return(session, return_id)
    if draft is None:
        raise ValueError("Черновик возврата не найден")
    if draft.status not in {
        ReturnDocumentStatus.CREATED,
        ReturnDocumentStatus.APPROVED,
    }:
        raise ValueError(
            "Источник можно менять только до фактической приёмки возврата."
        )
    shipment = (
        await session.execute(
            select(ShipmentDocument)
            .where(ShipmentDocument.id == shipment_document_id)
            .options(
                selectinload(ShipmentDocument.items).selectinload(
                    ShipmentDocumentItem.autopart
                )
            )
        )
    ).scalar_one_or_none()
    if shipment is None:
        raise ValueError("Исходная реализация не найдена")
    if draft.customer_id and shipment.customer_id != draft.customer_id:
        raise ValueError("Реализация относится к другому клиенту")
    if not draft.source_document_number or not draft.source_document_date:
        raise ValueError(
            "Во входящем УПД не распознано основание передачи: номер и дата."
        )
    if not _matches_source_basis(
        document_number=shipment.doc_number,
        document_date=shipment.doc_date,
        basis_number=draft.source_document_number,
        basis_date=draft.source_document_date,
    ):
        raise ValueError(
            "Выбранная реализация не совпадает с основанием передачи "
            "во входящем УПД"
        )
    source_upd = await session.get(
        DiadocOutgoingDocument,
        source_diadoc_outgoing_document_id,
    )
    if source_upd is None:
        raise ValueError("Исходящая УПД не найдена")
    if source_upd.type_named_id != "UniversalTransferDocument":
        raise ValueError("Выбранный документ Диадока не является УПД")
    if not _matches_source_basis(
        document_number=source_upd.document_number,
        document_date=source_upd.document_date,
        basis_number=draft.source_document_number,
        basis_date=draft.source_document_date,
    ):
        raise ValueError(
            "Выбранная УПД не совпадает с основанием передачи "
            "во входящем УПД"
        )
    if source_upd.customer_id and source_upd.customer_id != shipment.customer_id:
        raise ValueError("Выбранная УПД относится к другому клиенту")
    if source_upd.source_id and (
        source_upd.source_type != "shipment_document"
        or source_upd.source_id != shipment.id
    ):
        raise ValueError("Выбранная УПД относится к другой реализации")
    if (
        source_upd.document_number
        and shipment.doc_number
        and _normalized(source_upd.document_number)
        != _normalized(shipment.doc_number)
    ):
        raise ValueError("Номер выбранной УПД не совпадает с реализацией")
    if (
        source_upd.document_date
        and _as_date(shipment.doc_date)
        and _as_date(source_upd.document_date) != _as_date(shipment.doc_date)
    ):
        raise ValueError("Дата выбранной УПД не совпадает с реализацией")

    source_upd.customer_id = source_upd.customer_id or shipment.customer_id
    source_upd.source_type = "shipment_document"
    source_upd.source_id = shipment.id
    session.add(source_upd)

    draft.customer_id = draft.customer_id or shipment.customer_id
    draft.shipment_document_id = shipment.id
    draft.source_diadoc_outgoing_document_id = source_upd.id
    draft.warehouse_id = draft.warehouse_id or shipment.warehouse_id
    for item in draft.items or []:
        matched = _match_shipment_item(shipment, item.oem_number)
        item.shipment_item_id = matched.id if matched else None
        if matched is not None:
            item.autopart_id = matched.autopart_id
            item.price = matched.price
            item.vat_rate = matched.vat_rate
        session.add(item)
    session.add(draft)
    await session.flush()
    return await _load_return(session, return_id)  # type: ignore[return-value]


async def decide_customer_return_draft(
    session: AsyncSession,
    *,
    return_id: int,
    decision: str,
) -> ReturnFromCustomer:
    if decision == "approved":
        return await approve_return_from_customer(session, doc_id=return_id)
    if decision == "rejected":
        return await reject_return_from_customer(session, doc_id=return_id)
    raise ValueError("Допустимы решения approved или rejected")
