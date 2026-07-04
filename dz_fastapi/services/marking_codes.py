"""Учёт кодов маркировки (КИЗ/СИЗ) поверх складских FIFO-партий."""

from __future__ import annotations

import logging
import re
from typing import Any, Iterable, Optional

from sqlalchemy import func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.core.time import now_moscow
from dz_fastapi.models.autopart import AutoPart
from dz_fastapi.models.inventory import (
    MarkingCodeStatus,
    MarkingMovementType,
    ProductMarkingCode,
    ProductMarkingCodeMovement,
    ShipmentDocumentItemLotAllocation,
    StockLot,
)
from dz_fastapi.models.partner import SupplierReceiptItem

logger = logging.getLogger(__name__)

_MARKING_CODE_WHITESPACE_RE = re.compile(r"[\r\n\t ]+")


def normalize_marking_codes(values: Iterable[object] | object | None) -> list[str]:
    """Нормализует список КИЗ без потери значимых GS1-символов."""
    if values is None:
        return []
    if isinstance(values, str):
        raw_values: Iterable[object] = [values]
    else:
        try:
            raw_values = list(values)  # type: ignore[arg-type]
        except TypeError:
            raw_values = [values]

    result: list[str] = []
    seen: set[str] = set()
    for value in raw_values:
        text = str(value or "").strip()
        if not text:
            continue
        code = _MARKING_CODE_WHITESPACE_RE.sub("", text)
        if code and code not in seen:
            seen.add(code)
            result.append(code)
    return result


async def register_receipt_marking_codes(
    session: AsyncSession,
    *,
    receipt_item: SupplierReceiptItem,
    stock_lot: StockLot,
    codes: Iterable[object] | object | None,
) -> list[str]:
    normalized_codes = normalize_marking_codes(codes)
    if not normalized_codes:
        return []

    receipt_item.marking_codes = normalized_codes
    stock_lot.marking_codes = normalized_codes

    quantity = int(getattr(receipt_item, "received_quantity", 0) or 0)
    if quantity > 0 and len(normalized_codes) != quantity:
        logger.warning(
            "Receipt item %s has %s marking codes for quantity %s",
            receipt_item.id,
            len(normalized_codes),
            quantity,
        )

    now = now_moscow()
    existing_rows = await session.execute(
        select(ProductMarkingCode).where(
            ProductMarkingCode.code.in_(normalized_codes)
        )
    )
    existing_by_code = {
        str(row.code): row for row in existing_rows.scalars().all()
    }

    for code in normalized_codes:
        row = existing_by_code.get(code)
        if row is None:
            row = ProductMarkingCode(
                code=code,
                created_at=now,
            )
            session.add(row)
            await session.flush()
        row.status = MarkingCodeStatus.IN_STOCK
        row.autopart_id = stock_lot.autopart_id
        row.warehouse_id = (
            receipt_item.warehouse_id or getattr(stock_lot, "warehouse_id", None)
        )
        row.storage_location_id = stock_lot.storage_location_id
        row.stock_lot_id = stock_lot.id
        row.supplier_receipt_id = receipt_item.receipt_id
        row.supplier_receipt_item_id = receipt_item.id
        row.shipment_document_id = None
        row.shipment_document_item_id = None
        row.shipment_allocation_id = None
        row.received_at = stock_lot.received_at or now
        row.shipped_at = None
        row.last_error = None
        row.raw_payload = {
            "source": "incoming_upd",
            "receipt_id": receipt_item.receipt_id,
            "receipt_item_id": receipt_item.id,
            "stock_lot_id": stock_lot.id,
        }
        session.add(
            ProductMarkingCodeMovement(
                marking_code_id=row.id,
                movement_type=MarkingMovementType.STOCKED,
                autopart_id=stock_lot.autopart_id,
                stock_lot_id=stock_lot.id,
                supplier_receipt_id=receipt_item.receipt_id,
                supplier_receipt_item_id=receipt_item.id,
                metadata_json={"source": "incoming_upd"},
            )
        )

    await session.flush()
    return normalized_codes


async def return_marking_codes_from_customer(
    session: AsyncSession,
    *,
    shipment_item_id: Optional[int],
    new_stock_lot: StockLot,
    quantity: int,
    return_document_id: int,
) -> list[str]:
    """Возврат от клиента: коды исходной отгрузки → обратно в наличие.

    Коды ищем по строке отгрузки, из которой товар уезжал; привязываем к
    новому лоту возврата. Если строка-источник неизвестна — кодов не
    двигаем (расхождение будет видно на странице маркировки).
    """
    quantity = max(int(quantity or 0), 0)
    if quantity <= 0 or shipment_item_id is None:
        return []
    rows = (
        await session.execute(
            select(ProductMarkingCode)
            .where(
                ProductMarkingCode.shipment_document_item_id
                == int(shipment_item_id),
                ProductMarkingCode.status == MarkingCodeStatus.SHIPPED,
            )
            .order_by(ProductMarkingCode.id.asc())
            .limit(quantity)
        )
    ).scalars().all()
    if not rows:
        return []
    codes: list[str] = []
    for row in rows:
        codes.append(str(row.code))
        row.status = MarkingCodeStatus.IN_STOCK
        row.stock_lot_id = new_stock_lot.id
        row.storage_location_id = new_stock_lot.storage_location_id
        row.shipment_document_id = None
        row.shipment_document_item_id = None
        row.shipment_allocation_id = None
        row.shipped_at = None
        session.add(
            ProductMarkingCodeMovement(
                marking_code_id=row.id,
                movement_type=MarkingMovementType.RETURNED_FROM_CUSTOMER,
                autopart_id=row.autopart_id,
                stock_lot_id=new_stock_lot.id,
                metadata_json={
                    "source": "return_from_customer",
                    "return_document_id": int(return_document_id),
                },
            )
        )
    new_codes = normalize_marking_codes(
        list(new_stock_lot.marking_codes or []) + codes
    )
    new_stock_lot.marking_codes = new_codes
    await session.flush()
    logger.info(
        "Returned %s marking codes from customer (return #%s)",
        len(codes),
        return_document_id,
    )
    return codes


async def return_marking_codes_to_supplier(
    session: AsyncSession,
    *,
    stock_lot_ids: Iterable[int],
    quantity: int,
    return_document_id: int,
) -> list[str]:
    """Возврат поставщику: коды списанных партий уходят от нас."""
    lot_ids = [int(lot_id) for lot_id in stock_lot_ids if lot_id]
    quantity = max(int(quantity or 0), 0)
    if quantity <= 0 or not lot_ids:
        return []
    rows = (
        await session.execute(
            select(ProductMarkingCode)
            .where(
                ProductMarkingCode.stock_lot_id.in_(lot_ids),
                ProductMarkingCode.status == MarkingCodeStatus.IN_STOCK,
            )
            .order_by(ProductMarkingCode.id.asc())
            .limit(quantity)
        )
    ).scalars().all()
    if not rows:
        return []
    now = now_moscow()
    codes: list[str] = []
    for row in rows:
        codes.append(str(row.code))
        row.status = MarkingCodeStatus.RETURNED_TO_SUPPLIER
        row.shipped_at = now
        session.add(
            ProductMarkingCodeMovement(
                marking_code_id=row.id,
                movement_type=MarkingMovementType.RETURNED_TO_SUPPLIER,
                autopart_id=row.autopart_id,
                stock_lot_id=row.stock_lot_id,
                supplier_receipt_id=row.supplier_receipt_id,
                metadata_json={
                    "source": "return_to_supplier",
                    "return_document_id": int(return_document_id),
                },
            )
        )
    await session.flush()
    logger.info(
        "Returned %s marking codes to supplier (return #%s)",
        len(codes),
        return_document_id,
    )
    return codes


async def list_receipt_marking_discrepancies(
    session: AsyncSession,
    *,
    limit: int = 100,
) -> list[dict[str, Any]]:
    """Строки приёмки, где число КИЗ не совпадает с количеством штук."""
    from dz_fastapi.models.partner import Provider, SupplierReceipt

    codes_length = func.json_array_length(
        SupplierReceiptItem.marking_codes
    )
    stmt = (
        select(
            SupplierReceiptItem,
            SupplierReceipt.document_number,
            SupplierReceipt.document_date,
            Provider.name.label("provider_name"),
            codes_length.label("codes_count"),
        )
        .join(
            SupplierReceipt,
            SupplierReceipt.id == SupplierReceiptItem.receipt_id,
        )
        .join(Provider, Provider.id == SupplierReceipt.provider_id)
        .where(
            SupplierReceiptItem.marking_codes.is_not(None),
            codes_length > 0,
            codes_length != func.coalesce(
                SupplierReceiptItem.received_quantity, 0
            ),
        )
        .order_by(SupplierReceiptItem.id.desc())
        .limit(max(1, min(int(limit or 100), 500)))
    )
    rows = (await session.execute(stmt)).all()
    result: list[dict[str, Any]] = []
    for item, doc_number, doc_date, provider_name, codes_count in rows:
        result.append(
            {
                "receipt_item_id": int(item.id),
                "receipt_id": int(item.receipt_id),
                "document_number": doc_number,
                "document_date": doc_date,
                "provider_name": provider_name,
                "oem_number": item.oem_number,
                "brand_name": item.brand_name,
                "autopart_name": item.autopart_name,
                "received_quantity": int(item.received_quantity or 0),
                "codes_count": int(codes_count or 0),
            }
        )
    return result


async def get_marking_codes_summary(
    session: AsyncSession,
) -> dict[str, Any]:
    """Сводка реестра КИЗ: счётчики по статусам + охват позиций."""
    status_rows = (
        await session.execute(
            select(
                ProductMarkingCode.status,
                func.count(),
            ).group_by(ProductMarkingCode.status)
        )
    ).all()
    by_status = {
        str(getattr(status, "value", status)): int(count)
        for status, count in status_rows
    }
    autoparts_with_codes = (
        await session.execute(
            select(
                func.count(func.distinct(ProductMarkingCode.autopart_id))
            ).where(ProductMarkingCode.autopart_id.is_not(None))
        )
    ).scalar() or 0
    return {
        "total": sum(by_status.values()),
        "by_status": by_status,
        "autoparts_with_codes": int(autoparts_with_codes),
    }


async def list_marking_codes(
    session: AsyncSession,
    *,
    status: Optional[str] = None,
    autopart_id: Optional[int] = None,
    query: Optional[str] = None,
    limit: int = 200,
) -> list[dict[str, Any]]:
    """Реестр кодов с позицией и привязками к документам."""
    from dz_fastapi.models.brand import Brand

    stmt = (
        select(ProductMarkingCode, AutoPart, Brand.name)
        .outerjoin(
            AutoPart, AutoPart.id == ProductMarkingCode.autopart_id
        )
        .outerjoin(Brand, Brand.id == AutoPart.brand_id)
        .order_by(ProductMarkingCode.id.desc())
        .limit(max(1, min(int(limit or 200), 1000)))
    )
    normalized_status = str(status or "").strip().lower()
    if normalized_status:
        stmt = stmt.where(
            ProductMarkingCode.status == normalized_status
        )
    if autopart_id is not None:
        stmt = stmt.where(
            ProductMarkingCode.autopart_id == int(autopart_id)
        )
    needle = str(query or "").strip()
    if needle:
        pattern = f"%{needle}%"
        stmt = stmt.where(
            or_(
                ProductMarkingCode.code.ilike(pattern),
                AutoPart.oem_number.ilike(pattern),
            )
        )
    rows = (await session.execute(stmt)).all()
    result: list[dict[str, Any]] = []
    for code_row, autopart, brand_name in rows:
        result.append(
            {
                "id": int(code_row.id),
                "code": str(code_row.code),
                "status": str(
                    getattr(code_row.status, "value", code_row.status)
                ),
                "autopart_id": code_row.autopart_id,
                "oem_number": getattr(autopart, "oem_number", None),
                "brand_name": brand_name,
                "autopart_name": getattr(autopart, "name", None),
                "stock_lot_id": code_row.stock_lot_id,
                "supplier_receipt_id": code_row.supplier_receipt_id,
                "shipment_document_id": code_row.shipment_document_id,
                "received_at": code_row.received_at,
                "shipped_at": code_row.shipped_at,
                "withdrawn_at": code_row.withdrawn_at,
                "last_error": code_row.last_error,
            }
        )
    return result


async def get_marking_code_movements(
    session: AsyncSession,
    *,
    marking_code_id: int,
) -> list[dict[str, Any]]:
    """История движений одного кода (для разбора инцидентов)."""
    rows = (
        await session.execute(
            select(ProductMarkingCodeMovement)
            .where(
                ProductMarkingCodeMovement.marking_code_id
                == int(marking_code_id)
            )
            .order_by(ProductMarkingCodeMovement.id.asc())
        )
    ).scalars().all()
    return [
        {
            "id": int(row.id),
            "movement_type": str(
                getattr(row.movement_type, "value", row.movement_type)
            ),
            "stock_lot_id": row.stock_lot_id,
            "supplier_receipt_id": row.supplier_receipt_id,
            "shipment_document_id": row.shipment_document_id,
            "created_at": row.created_at,
            "metadata": row.metadata_json or {},
        }
        for row in rows
    ]


async def release_marking_codes_for_shipment_allocation(
    session: AsyncSession,
    *,
    allocation: ShipmentDocumentItemLotAllocation,
) -> list[str]:
    """Возвращает КИЗ отменяемой отгрузки обратно в наличие.

    Вызывается при распроведении накладной ДО удаления allocation —
    иначе коды навсегда остаются в статусе SHIPPED и их нельзя выдать
    в следующую отгрузку.
    """
    rows = await session.execute(
        select(ProductMarkingCode).where(
            ProductMarkingCode.shipment_allocation_id == allocation.id,
            ProductMarkingCode.status == MarkingCodeStatus.SHIPPED,
        )
    )
    marking_rows = list(rows.scalars().all())
    if not marking_rows:
        return []
    codes: list[str] = []
    for row in marking_rows:
        codes.append(str(row.code))
        row.status = MarkingCodeStatus.IN_STOCK
        row.shipment_allocation_id = None
        row.shipment_document_item_id = None
        row.shipment_document_id = None
        row.shipped_at = None
        session.add(
            ProductMarkingCodeMovement(
                marking_code_id=row.id,
                movement_type=MarkingMovementType.UNPOSTED,
                autopart_id=row.autopart_id,
                stock_lot_id=row.stock_lot_id,
                supplier_receipt_id=row.supplier_receipt_id,
                supplier_receipt_item_id=row.supplier_receipt_item_id,
                shipment_allocation_id=allocation.id,
                metadata_json={"source": "shipment_unpost"},
            )
        )
    allocation.marking_codes = []
    await session.flush()
    logger.info(
        "Released %s marking codes from shipment allocation %s",
        len(codes),
        allocation.id,
    )
    return codes


async def allocate_marking_codes_for_shipment_allocation(
    session: AsyncSession,
    *,
    allocation: ShipmentDocumentItemLotAllocation,
    shipment_document_id: int | None = None,
    shipment_document_item_id: int | None = None,
) -> list[str]:
    quantity = max(int(allocation.quantity or 0), 0)
    if quantity <= 0 or allocation.stock_lot_id is None:
        return []

    rows = await session.execute(
        select(ProductMarkingCode)
        .where(
            ProductMarkingCode.stock_lot_id == allocation.stock_lot_id,
            ProductMarkingCode.status == MarkingCodeStatus.IN_STOCK,
        )
        .order_by(ProductMarkingCode.id.asc())
        .limit(quantity)
    )
    marking_rows = list(rows.scalars().all())
    codes = [str(row.code) for row in marking_rows]
    if not codes:
        return []
    if len(codes) < quantity:
        logger.warning(
            "Shipment allocation %s requested %s marking codes, found %s",
            allocation.id,
            quantity,
            len(codes),
        )

    now = now_moscow()
    allocation.marking_codes = codes
    for row in marking_rows:
        row.status = MarkingCodeStatus.SHIPPED
        row.shipment_allocation_id = allocation.id
        row.shipment_document_item_id = shipment_document_item_id
        row.shipment_document_id = shipment_document_id
        row.shipped_at = now
        session.add(
            ProductMarkingCodeMovement(
                marking_code_id=row.id,
                movement_type=MarkingMovementType.SHIPPED,
                autopart_id=row.autopart_id,
                stock_lot_id=row.stock_lot_id,
                supplier_receipt_id=row.supplier_receipt_id,
                supplier_receipt_item_id=row.supplier_receipt_item_id,
                shipment_document_id=shipment_document_id,
                shipment_document_item_id=shipment_document_item_id,
                shipment_allocation_id=allocation.id,
                metadata_json={"source": "shipment_fifo"},
            )
        )
    await session.flush()
    return codes
