"""Immutable posted corrections with per-line before/after snapshots."""

from decimal import Decimal

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from dz_fastapi.core.time import now_moscow
from dz_fastapi.models.inventory import ShipmentDocument, ShipmentDocumentStatus
from dz_fastapi.models.shipment_correction import (
    ShipmentCorrection,
    ShipmentCorrectionItem,
    ShipmentCorrectionStatus,
)
from dz_fastapi.schemas.shipment_correction import ShipmentCorrectionSave


def _load_query():
    return select(ShipmentCorrection).options(
        selectinload(ShipmentCorrection.items),
        selectinload(ShipmentCorrection.shipment),
    )


async def get_correction(
    session: AsyncSession, correction_id: int, *, lock: bool = False,
) -> ShipmentCorrection | None:
    query = _load_query().where(ShipmentCorrection.id == correction_id)
    if lock:
        query = query.with_for_update(of=ShipmentCorrection).execution_options(
            populate_existing=True
        )
    return (
        await session.execute(query)
    ).scalar_one_or_none()


async def list_corrections(
    session: AsyncSession, *, shipment_id: int | None = None,
) -> list[ShipmentCorrection]:
    query = _load_query().order_by(ShipmentCorrection.id.desc()).limit(200)
    if shipment_id is not None:
        query = query.where(ShipmentCorrection.shipment_id == shipment_id)
    return list((await session.execute(query)).scalars().all())


def serialize_correction(doc: ShipmentCorrection) -> dict:
    shipment = doc.shipment
    customer = getattr(shipment, "customer", None)
    return {
        "id": doc.id,
        "shipment_id": doc.shipment_id,
        "shipment_number": getattr(shipment, "doc_number", None),
        "shipment_date": shipment.doc_date,
        "customer_id": shipment.customer_id,
        "customer_name": getattr(customer, "name", None),
        "doc_number": doc.doc_number,
        "doc_date": doc.doc_date,
        "basis_number": doc.basis_number,
        "basis_date": doc.basis_date,
        "basis_description": doc.basis_description,
        "reason": doc.reason,
        "document_function": doc.document_function,
        "status": doc.status.value,
        "sync_status": doc.sync_status.value,
        "external_id": doc.external_id,
        "diadoc_outgoing_document_id": doc.diadoc_outgoing_document_id,
        "created_at": doc.created_at,
        "posted_at": doc.posted_at,
        "items": [
            {
                "id": row.id,
                "shipment_item_id": row.shipment_item_id,
                "original_number": row.original_number,
                "product_name": row.product_name,
                "vendor_code": row.vendor_code,
                "quantity_before": row.quantity_before,
                "quantity_after": row.quantity_after,
                "price_before": row.price_before,
                "price_after": row.price_after,
                "vat_rate": row.vat_rate,
            }
            for row in doc.items
        ],
    }


async def _current_line_values(
    session: AsyncSession, shipment: ShipmentDocument,
) -> dict[int, tuple[int, Decimal]]:
    current = {
        row.id: (int(row.quantity), Decimal(str(row.price or 0)))
        for row in shipment.items
    }
    corrections = list((await session.execute(
        _load_query()
        .where(
            ShipmentCorrection.shipment_id == shipment.id,
            ShipmentCorrection.status == ShipmentCorrectionStatus.POSTED,
        )
        .order_by(ShipmentCorrection.id.asc())
    )).scalars().all())
    for correction in corrections:
        for row in correction.items:
            current[row.shipment_item_id] = (
                int(row.quantity_after), Decimal(str(row.price_after))
            )
    return current


async def _validate_source_document_function(
    session: AsyncSession, doc: ShipmentCorrection,
) -> None:
    from dz_fastapi.services.diadoc_outgoing import (
        _map_transfer_function_to_correction_function,
        _source_upd_for_correction,
    )

    source = await _source_upd_for_correction(session, doc)
    if source is None:
        return
    expected = _map_transfer_function_to_correction_function(
        source.document_function
    )
    if doc.document_function != expected:
        raise ValueError(
            f"Функция УКД должна быть {expected} для исходящей УПД"
        )


async def _set_correction_fields(
    session: AsyncSession,
    doc: ShipmentCorrection,
    payload: ShipmentCorrectionSave,
) -> None:
    shipment = await session.get(ShipmentDocument, payload.shipment_id)
    if shipment is None:
        raise ValueError("Исходная реализация не найдена")
    if shipment.status != ShipmentDocumentStatus.POSTED:
        raise ValueError("Корректировать можно только проведённую реализацию")
    if shipment.customer_id is None:
        raise ValueError("В исходной реализации не указан клиент")
    if any(row.price is None for row in shipment.items):
        raise ValueError("В исходной реализации есть строки без цены")
    if any(
        Decimal(str(22 if row.vat_rate is None else row.vat_rate)) != Decimal("22")
        for row in shipment.items
    ):
        raise ValueError("Корректировка поддерживает только НДС 22%")
    if doc.id and doc.shipment_id != shipment.id:
        raise ValueError("Нельзя изменить исходную реализацию корректировки")
    duplicate_number = await session.scalar(
        select(ShipmentCorrection.id)
        .where(
            ShipmentCorrection.doc_number == payload.doc_number.strip(),
            ShipmentCorrection.id != (doc.id or -1),
        )
        .limit(1)
    )
    if duplicate_number is not None:
        raise ValueError("Номер корректировки уже используется")

    source_rows = {row.id: (index, row) for index, row in enumerate(
        shipment.items, start=1
    )}
    current = await _current_line_values(session, shipment)
    seen = set()
    changed = []
    for entry in payload.items:
        if entry.shipment_item_id in seen:
            raise ValueError("Строка реализации указана дважды")
        seen.add(entry.shipment_item_id)
        if entry.shipment_item_id not in source_rows:
            raise ValueError("Строка не принадлежит исходной реализации")
        number, row = source_rows[entry.shipment_item_id]
        before_qty, before_price = current[row.id]
        after_qty = int(entry.quantity_after)
        after_price = Decimal(str(entry.price_after))
        if (before_qty, before_price) == (after_qty, after_price):
            continue
        product_name = " ".join(filter(None, (
            row.customer_brand,
            row.customer_oem,
            row.customer_name,
        ))) or f"Товар #{row.autopart_id}"
        changed.append(ShipmentCorrectionItem(
            shipment_item_id=row.id,
            original_number=number,
            product_name=product_name,
            vendor_code=row.customer_oem or getattr(row.autopart, "oem_number", None),
            quantity_before=before_qty,
            quantity_after=after_qty,
            price_before=before_price,
            price_after=after_price,
            vat_rate=Decimal("22"),
        ))
    if not changed:
        raise ValueError("Укажите изменённое количество или цену")

    doc.shipment_id = shipment.id
    doc.doc_number = payload.doc_number.strip()
    doc.doc_date = payload.doc_date
    doc.basis_number = payload.basis_number.strip()
    doc.basis_date = payload.basis_date
    doc.basis_description = payload.basis_description.strip()
    doc.reason = payload.reason.strip()
    doc.document_function = payload.document_function
    await _validate_source_document_function(session, doc)
    doc.items = changed


async def save_correction(
    session: AsyncSession,
    payload: ShipmentCorrectionSave,
    *,
    correction_id: int | None = None,
    user_id: int | None = None,
) -> ShipmentCorrection:
    doc = await get_correction(session, correction_id, lock=True) if correction_id else None
    if correction_id and doc is None:
        raise LookupError("Корректировка не найдена")
    if doc is not None and doc.status != ShipmentCorrectionStatus.DRAFT:
        raise ValueError("Проведённую корректировку нельзя изменять")
    if doc is None:
        doc = ShipmentCorrection(created_by_user_id=user_id)
    await _set_correction_fields(session, doc, payload)
    session.add(doc)
    await session.flush()
    await session.commit()
    return await get_correction(session, doc.id)


async def post_correction(
    session: AsyncSession, correction_id: int,
) -> ShipmentCorrection:
    doc = await get_correction(session, correction_id, lock=True)
    if doc is None:
        raise LookupError("Корректировка не найдена")
    if doc.status != ShipmentCorrectionStatus.DRAFT:
        raise ValueError("Корректировка уже проведена")
    await session.execute(
        select(ShipmentDocument.id)
        .where(ShipmentDocument.id == doc.shipment_id)
        .with_for_update()
    )
    await session.refresh(doc.shipment, attribute_names=["status"])
    if doc.shipment.status != ShipmentDocumentStatus.POSTED:
        raise ValueError("Исходная реализация больше не проведена")
    await _validate_source_document_function(session, doc)
    current = await _current_line_values(session, doc.shipment)
    for item in doc.items:
        if current[item.shipment_item_id] != (
            int(item.quantity_before), Decimal(str(item.price_before))
        ):
            raise ValueError(
                "После создания черновика реализация уже корректировалась. "
                "Создайте новый черновик с актуальными количествами."
            )
    doc.status = ShipmentCorrectionStatus.POSTED
    doc.posted_at = now_moscow()
    await session.flush()
    from dz_fastapi.services.one_c_outbox import enqueue_shipment_correction_event

    await enqueue_shipment_correction_event(session, doc.id)
    await session.commit()
    return await get_correction(session, doc.id)
