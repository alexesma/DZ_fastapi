"""Supplier quantity corrections (КСФ/УКД) for posted receipts.

An incoming document is a draft until a manager matches its rows to the
original receipt. The original receipt and its stock movement stay immutable.
"""

from __future__ import annotations

import re
from datetime import date

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from dz_fastapi.core.time import now_moscow
from dz_fastapi.models.inventory import CrossDockingLabel, MovementType, StockLot, StockOrderPackage
from dz_fastapi.models.partner import (
    STOCK_ORDER_STATUS,
    StockOrderItem,
    SupplierOrderMessage,
    SupplierReceipt,
    SupplierReceiptCorrection,
    SupplierReceiptCorrectionItem,
)
from dz_fastapi.services.inventory_stock import _apply_stock_delta
from dz_fastapi.services.supplier_workflow import _refresh_receipt_links

_CORRECTION_RE = re.compile(
    r"(?:^|[^А-Яа-яЁёA-Za-z0-9])(?:ксф|укд)(?=$|[^А-Яа-яЁёA-Za-z0-9])"
    r"|корректир|universalcorrectiondocument",
    re.I,
)


def is_correction_document(*values: object) -> bool:
    """Use only document metadata/filename/subject, never arbitrary body text."""
    return any(_CORRECTION_RE.search(str(value or "")) for value in values)


def _query():
    return select(SupplierReceiptCorrection).options(
        selectinload(SupplierReceiptCorrection.items).selectinload(
            SupplierReceiptCorrectionItem.receipt_item
        ),
        selectinload(SupplierReceiptCorrection.source_message).selectinload(
            SupplierOrderMessage.attachments
        ),
    )


async def get_correction(session: AsyncSession, correction_id: int):
    correction = (
        await session.execute(_query().where(SupplierReceiptCorrection.id == correction_id))
    ).scalar_one_or_none()
    if correction is None:
        raise LookupError("КСФ не найдена")
    return correction


async def list_corrections(
    session: AsyncSession, *, provider_id: int | None = None, status: str | None = None
):
    query = _query().order_by(SupplierReceiptCorrection.id.desc()).limit(200)
    if provider_id is not None:
        query = query.where(SupplierReceiptCorrection.provider_id == provider_id)
    if status is not None:
        query = query.where(SupplierReceiptCorrection.status == status)
    return (await session.execute(query)).scalars().all()


async def register_incoming_correction(
    session: AsyncSession,
    *,
    provider_id: int,
    message_id: int,
    source_kind: str,
    document_number: str | None = None,
    document_date: date | None = None,
):
    existing = (
        await session.execute(
            select(SupplierReceiptCorrection).where(
                SupplierReceiptCorrection.source_message_id == message_id
            )
        )
    ).scalar_one_or_none()
    if existing is not None:
        return existing
    correction = SupplierReceiptCorrection(
        provider_id=provider_id,
        source_message_id=message_id,
        source_kind=source_kind,
        document_number=document_number,
        document_date=document_date,
        status="draft",
    )
    session.add(correction)
    await session.flush()
    return correction


async def save_correction_draft(
    session: AsyncSession,
    *,
    correction_id: int | None,
    receipt_id: int,
    document_number: str | None,
    document_date: date | None,
    comment: str | None,
    items: list[dict],
    user_id: int | None,
):
    receipt = (
        await session.execute(
            select(SupplierReceipt)
            .options(selectinload(SupplierReceipt.items))
            .where(SupplierReceipt.id == receipt_id)
        )
    ).scalar_one_or_none()
    if receipt is None:
        raise LookupError("Исходный приход не найден")
    if receipt.posted_at is None:
        raise ValueError("КСФ можно привязать только к проведённому приходу")
    if correction_id is None:
        correction = SupplierReceiptCorrection(
            provider_id=receipt.provider_id,
            source_kind="manual",
            status="draft",
            created_by_user_id=user_id,
            items=[],
        )
        session.add(correction)
        await session.flush()
    else:
        correction = await get_correction(session, correction_id)
        if correction.posted_at is not None:
            raise ValueError("Проведённую КСФ редактировать нельзя")
        if correction.provider_id != receipt.provider_id:
            raise ValueError("Поставщик КСФ не совпадает с исходным приходом")
    receipt_items = {int(row.id): row for row in receipt.items}
    if not items:
        raise ValueError("Укажите хотя бы одну строку КСФ")
    seen: set[int] = set()
    for payload in items:
        item_id = int(payload["receipt_item_id"])
        quantity_after = int(payload["quantity_after"])
        if item_id in seen or item_id not in receipt_items:
            raise ValueError("Строки КСФ должны быть уникальны и принадлежать приходу")
        if quantity_after < 0:
            raise ValueError("Исправленное количество не может быть отрицательным")
        seen.add(item_id)
    correction.receipt_id = receipt.id
    correction.document_number = document_number
    correction.document_date = document_date
    correction.comment = comment
    correction.items.clear()
    await session.flush()
    for payload in items:
        correction.items.append(
            SupplierReceiptCorrectionItem(
                receipt_item_id=int(payload["receipt_item_id"]),
                quantity_after=int(payload["quantity_after"]),
            )
        )
    await session.commit()
    return await get_correction(session, correction.id)


async def post_correction(session: AsyncSession, *, correction_id: int):
    await session.execute(
        select(SupplierReceiptCorrection.id)
        .where(SupplierReceiptCorrection.id == correction_id)
        .with_for_update()
    )
    correction = await get_correction(session, correction_id)
    if correction.posted_at is not None:
        return correction
    if correction.receipt_id is None or not correction.items:
        raise ValueError("Сначала сопоставьте КСФ с приходом и его строками")
    receipt = (
        await session.execute(
            select(SupplierReceipt)
            .options(selectinload(SupplierReceipt.items))
            .where(SupplierReceipt.id == correction.receipt_id)
            .with_for_update()
        )
    ).scalar_one_or_none()
    if receipt is None or receipt.posted_at is None:
        raise ValueError("Исходный приход должен быть проведён")
    if receipt.provider_id != correction.provider_id:
        raise ValueError("Поставщик КСФ не совпадает с приходом")
    receipt_items = {int(row.id): row for row in receipt.items}
    changed = False
    for row in correction.items:
        original = receipt_items.get(int(row.receipt_item_id))
        if original is None:
            raise ValueError("Строка КСФ не принадлежит исходному приходу")
        previous_delta = (
            await session.execute(
                select(func.coalesce(func.sum(SupplierReceiptCorrectionItem.quantity_delta), 0))
                .join(SupplierReceiptCorrection)
                .where(
                    SupplierReceiptCorrectionItem.receipt_item_id == original.id,
                    SupplierReceiptCorrection.posted_at.is_not(None),
                )
            )
        ).scalar_one()
        net_before = int(original.received_quantity or 0) - int(previous_delta or 0)
        after = int(row.quantity_after)
        if after < 0 or after > net_before:
            raise ValueError(
                f"Строка {original.id}: исправленное количество должно быть от 0 до {net_before}"
            )
        delta = net_before - after
        row.quantity_before = net_before
        row.quantity_delta = delta
        if not delta:
            continue
        changed = True
        stock_order_item = (
            await session.execute(
                select(StockOrderItem)
                .options(selectinload(StockOrderItem.stock_order))
                .where(StockOrderItem.supplier_receipt_item_id == original.id)
            )
        ).scalar_one_or_none()
        label = (
            await session.execute(
                select(CrossDockingLabel).where(
                    CrossDockingLabel.supplier_receipt_item_id == original.id
                )
            )
        ).scalar_one_or_none()
        if stock_order_item is not None:
            if (
                int(stock_order_item.picked_quantity or 0) > 0
                or stock_order_item.stock_order.status != STOCK_ORDER_STATUS.NEW
            ):
                raise ValueError(
                    f"Строка {original.id} уже участвует в сборке/отгрузке; "
                    "сначала разберите её вручную"
                )
            packages = (
                await session.execute(
                    select(StockOrderPackage.id)
                    .where(StockOrderPackage.stock_order_id == stock_order_item.stock_order_id)
                    .limit(1)
                )
            ).scalar_one_or_none()
            if packages is not None:
                raise ValueError(
                    f"Строка {original.id} входит в упаковку; сначала разберите её вручную"
                )
        if label is not None and int(label.print_count or 0) > 0:
            raise ValueError(
                f"По строке {original.id} напечатана этикетка; сначала обработайте её вручную"
            )
        lots = (
            (
                await session.execute(
                    select(StockLot)
                    .where(StockLot.source_receipt_item_id == original.id)
                    .order_by(StockLot.id)
                    .with_for_update(of=StockLot)
                )
            )
            .scalars()
            .all()
        )
        available = sum(int(lot.remaining_quantity or 0) for lot in lots)
        if lots and available < delta:
            raise ValueError(
                f"Строка {original.id}: на складе осталось {available}, "
                f"КСФ уменьшает на {delta}; товар уже расходован"
            )
        if not lots and original.autopart_id is not None:
            raise ValueError(
                f"Строка {original.id}: складская партия не найдена; "
                "проверьте её перед проведением КСФ"
            )
        if lots and original.marking_codes:
            raise ValueError(
                f"Строка {original.id} содержит коды маркировки; "
                "обработайте КСФ вручную с проверкой кодов"
            )
        left = delta
        for lot in lots:
            taken = min(int(lot.remaining_quantity or 0), left)
            if not taken:
                continue
            lot.remaining_quantity -= taken
            await _apply_stock_delta(
                session,
                autopart_id=lot.autopart_id,
                storage_location_id=lot.storage_location_id,
                quantity_delta=-taken,
                movement_type=MovementType.SUPPLIER_CORRECTION,
                reference_id=correction.id,
                reference_type="supplier_receipt_correction",
                stock_lot_id=lot.id,
                notes=f"КСФ {correction.document_number or correction.id}, приход {receipt.id}",
            )
            left -= taken
            if not left:
                break
        if stock_order_item is not None:
            if after:
                stock_order_item.quantity = after
            else:
                await session.delete(stock_order_item)
        if label is not None:
            if after:
                label.quantity = after
            else:
                await session.delete(label)
    if not changed:
        raise ValueError("В КСФ нет уменьшения количества")
    correction.posted_at = now_moscow()
    correction.status = "posted"
    await session.flush()
    await _refresh_receipt_links(
        session,
        supplier_order_item_ids={
            int(item.supplier_order_item_id)
            for item in receipt.items
            if item.supplier_order_item_id
        },
        order_item_ids={int(item.order_item_id) for item in receipt.items if item.order_item_id},
    )
    await session.commit()
    return await get_correction(session, correction.id)


def serialize_correction(correction):
    return {
        "id": correction.id,
        "provider_id": correction.provider_id,
        "receipt_id": correction.receipt_id,
        "source_message_id": correction.source_message_id,
        "source_kind": correction.source_kind,
        "document_number": correction.document_number,
        "document_date": correction.document_date,
        "status": correction.status,
        "comment": correction.comment,
        "created_at": correction.created_at,
        "posted_at": correction.posted_at,
        "source_subject": correction.source_message.subject if correction.source_message else None,
        "source_preview": (
            correction.source_message.body_preview if correction.source_message else None
        ),
        "source_received_at": (
            correction.source_message.received_at if correction.source_message else None
        ),
        "attachments": [
            {"id": attachment.id, "filename": attachment.filename}
            for attachment in (
                correction.source_message.attachments if correction.source_message else []
            )
        ],
        "items": [
            {
                "id": row.id,
                "receipt_item_id": row.receipt_item_id,
                "quantity_before": row.quantity_before,
                "quantity_after": row.quantity_after,
                "quantity_delta": row.quantity_delta,
                "oem_number": row.receipt_item.oem_number if row.receipt_item else None,
                "brand_name": row.receipt_item.brand_name if row.receipt_item else None,
            }
            for row in correction.items
        ],
    }
