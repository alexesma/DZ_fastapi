"""Приёмка и клиентские этикетки для cross-docking."""

from __future__ import annotations

from datetime import date

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload, selectinload

from dz_fastapi.core.time import now_moscow
from dz_fastapi.models.inventory import (
    CrossDockingItemStatus,
    CrossDockingLabel,
    CrossDockingLabelPrintEvent,
    CrossDockingLabelStatus,
)
from dz_fastapi.models.partner import (
    CustomerOrder,
    CustomerOrderItem,
    StockOrderItem,
    SupplierReceipt,
    SupplierReceiptItem,
)


def _order_date(order: CustomerOrder | None) -> date | None:
    if order is None:
        return None
    value = order.received_at or order.created_at
    return value.date() if value is not None else None


async def ensure_cross_docking_labels(
    session: AsyncSession,
    *,
    receipt_id: int,
    user_id: int | None = None,
) -> dict[str, int]:
    """Create one stable customer label for every linked receipt line."""
    receipt = (
        await session.execute(
            select(SupplierReceipt)
            .where(SupplierReceipt.id == receipt_id)
            .options(
                selectinload(SupplierReceipt.items)
                .joinedload(SupplierReceiptItem.customer_order_item)
                .joinedload(CustomerOrderItem.order)
                .joinedload(CustomerOrder.customer),
            )
        )
    ).scalar_one_or_none()
    if receipt is None:
        raise LookupError("Документ поступления не найден")
    if receipt.posted_at is None:
        raise ValueError("Этикетки создаются только после проведения поступления")

    linked_items = [
        item
        for item in receipt.items or []
        if item.customer_order_item_id is not None
        and int(item.received_quantity or 0) > 0
    ]
    if not linked_items:
        return {"created": 0, "updated": 0}

    item_ids = [int(item.id) for item in linked_items]
    existing_labels = (
        (
            await session.execute(
                select(CrossDockingLabel).where(
                    CrossDockingLabel.supplier_receipt_item_id.in_(item_ids)
                )
            )
        )
        .scalars()
        .all()
    )
    labels_by_item_id = {
        int(label.supplier_receipt_item_id): label
        for label in existing_labels
    }
    stock_rows = (
        (
            await session.execute(
                select(StockOrderItem).where(
                    StockOrderItem.supplier_receipt_item_id.in_(item_ids)
                )
            )
        )
        .scalars()
        .all()
    )
    stock_by_receipt_item_id = {
        int(row.supplier_receipt_item_id): row
        for row in stock_rows
        if row.supplier_receipt_item_id is not None
    }

    created = 0
    updated = 0
    accepted_at = now_moscow()
    has_document = bool(str(receipt.document_number or "").strip())
    for receipt_item in linked_items:
        customer_item = receipt_item.customer_order_item
        customer_order = customer_item.order if customer_item else None
        customer = customer_order.customer if customer_order else None
        stock_item = stock_by_receipt_item_id.get(int(receipt_item.id))

        if receipt_item.cross_docking_status is None:
            receipt_item.cross_docking_status = CrossDockingItemStatus.RECEIVED
            receipt_item.document_pending = not has_document
            receipt_item.accepted_at = receipt.posted_at or accepted_at
            receipt_item.accepted_by_user_id = user_id

        label = labels_by_item_id.get(int(receipt_item.id))
        if label is None:
            label = CrossDockingLabel(
                supplier_receipt_item_id=receipt_item.id,
                stock_order_item_id=stock_item.id if stock_item else None,
                customer_order_item_id=customer_item.id if customer_item else None,
                quantity=int(receipt_item.received_quantity),
                requested_brand=str(
                    (customer_item.brand if customer_item else None)
                    or receipt_item.brand_name
                    or "БЕЗ БРЕНДА"
                ),
                requested_oem=str(
                    (customer_item.oem if customer_item else None)
                    or receipt_item.oem_number
                    or f"ITEM-{receipt_item.id}"
                ),
                requested_name=(
                    (customer_item.name if customer_item else None)
                    or receipt_item.autopart_name
                ),
                customer_name=(customer.name if customer else None),
                order_number=(customer_order.order_number if customer_order else None),
                order_date=_order_date(customer_order),
                barcode=f"XD-{receipt.id:08d}-{receipt_item.id:08d}",
                status=CrossDockingLabelStatus.PENDING,
            )
            session.add(label)
            receipt_item.cross_docking_status = (
                CrossDockingItemStatus.LABEL_PENDING
            )
            created += 1
            continue

        if int(label.print_count or 0) == 0:
            label.quantity = int(receipt_item.received_quantity)
            label.stock_order_item_id = stock_item.id if stock_item else None
            receipt_item.cross_docking_status = (
                CrossDockingItemStatus.LABEL_PENDING
            )
            updated += 1
        else:
            receipt_item.cross_docking_status = (
                CrossDockingItemStatus.READY_FOR_CUSTOMER
            )
            receipt_item.ready_at = label.last_printed_at

    await session.flush()
    return {"created": created, "updated": updated}


async def list_cross_docking_labels(
    session: AsyncSession,
    *,
    receipt_id: int,
) -> list[CrossDockingLabel]:
    return list(
        (
            await session.execute(
                select(CrossDockingLabel)
                .join(
                    SupplierReceiptItem,
                    SupplierReceiptItem.id
                    == CrossDockingLabel.supplier_receipt_item_id,
                )
                .where(SupplierReceiptItem.receipt_id == receipt_id)
                .options(
                    joinedload(CrossDockingLabel.last_printed_by_user),
                    selectinload(CrossDockingLabel.print_events).joinedload(
                        CrossDockingLabelPrintEvent.printed_by_user
                    ),
                )
                .order_by(CrossDockingLabel.id.asc())
            )
        )
        .scalars()
        .unique()
        .all()
    )


def serialize_cross_docking_label(label: CrossDockingLabel) -> dict:
    return {
        "id": label.id,
        "supplier_receipt_item_id": label.supplier_receipt_item_id,
        "stock_order_item_id": label.stock_order_item_id,
        "customer_order_item_id": label.customer_order_item_id,
        "quantity": label.quantity,
        "requested_brand": label.requested_brand,
        "requested_oem": label.requested_oem,
        "requested_name": label.requested_name,
        "customer_name": label.customer_name,
        "order_number": label.order_number,
        "order_date": label.order_date,
        "barcode": label.barcode,
        "status": label.status,
        "print_count": label.print_count,
        "last_printed_at": label.last_printed_at,
        "last_printed_by_name": (
            label.last_printed_by_user.name
            or label.last_printed_by_user.email
            if label.last_printed_by_user
            else None
        ),
        "last_print_reason": label.last_print_reason,
        "print_history": [
            {
                "id": event.id,
                "print_number": event.print_number,
                "printed_at": event.printed_at,
                "printed_by_name": (
                    event.printed_by_user.name
                    or event.printed_by_user.email
                    if event.printed_by_user
                    else None
                ),
                "reason": event.reason,
            }
            for event in label.print_events or []
        ],
    }


async def mark_cross_docking_labels_printed(
    session: AsyncSession,
    *,
    receipt_id: int,
    user_id: int | None,
    label_ids: list[int] | None = None,
    reason: str | None = None,
) -> list[CrossDockingLabel]:
    labels = await list_cross_docking_labels(session, receipt_id=receipt_id)
    selected_ids = {int(value) for value in (label_ids or []) if value}
    if selected_ids:
        labels = [label for label in labels if int(label.id) in selected_ids]
    if not labels:
        raise ValueError("Для печати не выбраны cross-docking этикетки")

    normalized_reason = str(reason or "").strip() or None
    if any(int(label.print_count or 0) > 0 for label in labels):
        if not normalized_reason:
            raise ValueError("Для повторной печати укажите причину")

    printed_at = now_moscow()
    receipt_item_ids = [int(label.supplier_receipt_item_id) for label in labels]
    receipt_items = (
        (
            await session.execute(
                select(SupplierReceiptItem).where(
                    SupplierReceiptItem.id.in_(receipt_item_ids)
                )
            )
        )
        .scalars()
        .all()
    )
    receipt_items_by_id = {int(item.id): item for item in receipt_items}

    for label in labels:
        label.print_count = int(label.print_count or 0) + 1
        label.status = CrossDockingLabelStatus.PRINTED
        label.last_printed_at = printed_at
        label.last_printed_by_user_id = user_id
        label.last_print_reason = normalized_reason
        label.print_events.append(
            CrossDockingLabelPrintEvent(
                print_number=label.print_count,
                printed_by_user_id=user_id,
                printed_at=printed_at,
                reason=normalized_reason,
            )
        )
        receipt_item = receipt_items_by_id.get(
            int(label.supplier_receipt_item_id)
        )
        if receipt_item is not None:
            receipt_item.cross_docking_status = (
                CrossDockingItemStatus.READY_FOR_CUSTOMER
            )
            receipt_item.ready_at = printed_at

    await session.flush()
    return await list_cross_docking_labels(session, receipt_id=receipt_id)


async def reset_cross_docking_acceptance(
    session: AsyncSession,
    *,
    receipt_id: int,
) -> None:
    labels = await list_cross_docking_labels(session, receipt_id=receipt_id)
    if any(int(label.print_count or 0) > 0 for label in labels):
        raise ValueError(
            "Нельзя распровести поступление: клиентская этикетка "
            "cross-docking уже напечатана"
        )
    for label in labels:
        await session.delete(label)

    receipt_items = (
        (
            await session.execute(
                select(SupplierReceiptItem).where(
                    SupplierReceiptItem.receipt_id == receipt_id,
                    SupplierReceiptItem.customer_order_item_id.is_not(None),
                )
            )
        )
        .scalars()
        .all()
    )
    for item in receipt_items:
        item.cross_docking_status = None
        item.document_pending = False
        item.accepted_at = None
        item.accepted_by_user_id = None
        item.ready_at = None
    await session.flush()


async def update_cross_docking_document_status(
    session: AsyncSession,
    *,
    receipt_id: int,
    document_pending: bool,
    document_number: str | None = None,
    document_date: date | None = None,
) -> SupplierReceipt:
    receipt = await session.get(SupplierReceipt, receipt_id)
    if receipt is None:
        raise LookupError("Документ поступления не найден")
    if receipt.posted_at is None:
        raise ValueError("Сначала проведите поступление")
    if document_number is not None:
        receipt.document_number = str(document_number).strip() or None
    if document_date is not None:
        receipt.document_date = document_date
    if not document_pending and not str(receipt.document_number or "").strip():
        raise ValueError(
            "Чтобы снять DOC_PENDING, укажите номер входящего документа"
        )
    rows = (
        (
            await session.execute(
                select(SupplierReceiptItem).where(
                    SupplierReceiptItem.receipt_id == receipt_id,
                    SupplierReceiptItem.customer_order_item_id.is_not(None),
                    SupplierReceiptItem.received_quantity > 0,
                )
            )
        )
        .scalars()
        .all()
    )
    for row in rows:
        row.document_pending = bool(document_pending)
    await session.flush()
    return receipt


async def sync_posted_cross_docking_labels(
    session: AsyncSession,
    *,
    user_id: int | None = None,
) -> dict:
    """Backfill labels and acceptance state for previously posted receipts."""
    receipt_ids = list(
        (
            await session.execute(
                select(SupplierReceipt.id)
                .join(SupplierReceiptItem)
                .outerjoin(
                    CrossDockingLabel,
                    CrossDockingLabel.supplier_receipt_item_id
                    == SupplierReceiptItem.id,
                )
                .where(
                    SupplierReceipt.posted_at.is_not(None),
                    SupplierReceiptItem.customer_order_item_id.is_not(None),
                    SupplierReceiptItem.received_quantity > 0,
                    CrossDockingLabel.id.is_(None),
                )
                .distinct()
                .order_by(SupplierReceipt.id.asc())
            )
        )
        .scalars()
        .all()
    )
    created = 0
    updated = 0
    errors: list[dict[str, str | int]] = []
    for receipt_id in receipt_ids:
        try:
            async with session.begin_nested():
                result = await ensure_cross_docking_labels(
                    session,
                    receipt_id=int(receipt_id),
                    user_id=user_id,
                )
        except (LookupError, ValueError) as exc:
            errors.append(
                {"receipt_id": int(receipt_id), "error": str(exc)}
            )
            continue
        created += int(result.get("created", 0))
        updated += int(result.get("updated", 0))
    await session.flush()
    return {
        "label_receipts_processed": len(receipt_ids),
        "labels_created": created,
        "labels_updated": updated,
        "label_receipts_skipped": len(errors),
        "label_errors": errors,
    }
