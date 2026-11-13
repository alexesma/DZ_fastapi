"""Сканирование кодов маркировки при сборке отгрузки.

При проведении отгрузки коды раньше назначались сами — первые свободные из
партии. Кладовщик же берёт любую коробку, и в УПД покупателю уходили коды
не тех штук, что лежат в посылке. Здесь код сканируется при сборке и
закрепляется за строкой отгрузки: до проведения скан запоминается и
применяется при проведении, после проведения — заменяет автоматически
назначенный код, который никто не сканировал.
"""

from __future__ import annotations

from collections import Counter
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.core.time import now_moscow
from dz_fastapi.models.inventory import (
    MarkingCodeStatus,
    MarkingMovementType,
    ProductMarkingCode,
    ProductMarkingCodeMovement,
    ShipmentDocument,
    ShipmentDocumentItem,
    ShipmentDocumentItemLotAllocation,
    ShipmentDocumentStatus,
    ShipmentMarkingScan,
    StockLot,
)
from dz_fastapi.models.partner import SupplierReceiptItem
from dz_fastapi.models.settings import GisMtSettings
from dz_fastapi.services.marking_aggregate import (
    AggregateError,
    expand_aggregate,
    gis_mt_client_or_none,
    parse_transport_code,
)
from dz_fastapi.services.marking_code_parse import parse_marking_code

SOURCE_SCAN = "shipment_scan"
SOURCE_SWAP = "shipment_scan_swap"

_STATUS_TEXT = {
    MarkingCodeStatus.RECEIVED: "принят, но ещё не на остатке",
    MarkingCodeStatus.RESERVED: "зарезервирован",
    MarkingCodeStatus.SHIPPED: "уже отгружен по другому документу",
    MarkingCodeStatus.RETURNED_TO_SUPPLIER: "возвращён поставщику",
    MarkingCodeStatus.WITHDRAWN: "выведен из оборота",
    MarkingCodeStatus.ERROR: "помечен ошибочным",
}


class ShipmentMarkingScanError(ValueError):
    """Код нельзя закрепить за отгрузкой; текст — для кладовщика."""


async def shipment_codes_auto_fifo(session: AsyncSession) -> bool:
    """Назначать ли коды при проведении автоматически (по умолчанию да)."""
    value = (
        await session.execute(
            select(GisMtSettings.shipment_codes_auto_fifo).limit(1)
        )
    ).scalar_one_or_none()
    return True if value is None else bool(value)


def _capacity(allocation: ShipmentDocumentItemLotAllocation) -> int:
    return max(int(allocation.quantity or 0), 0) - len(
        allocation.marking_codes or []
    )


def _ship(
    session: AsyncSession,
    row: ProductMarkingCode,
    *,
    allocation: ShipmentDocumentItemLotAllocation,
    document_id: int,
    item_id: int,
    source: str,
) -> None:
    allocation.marking_codes = [*(allocation.marking_codes or []), str(row.code)]
    row.status = MarkingCodeStatus.SHIPPED
    row.shipment_allocation_id = allocation.id
    row.shipment_document_item_id = item_id
    row.shipment_document_id = document_id
    row.shipped_at = now_moscow()
    session.add(
        ProductMarkingCodeMovement(
            marking_code_id=row.id,
            movement_type=MarkingMovementType.SHIPPED,
            autopart_id=row.autopart_id,
            stock_lot_id=row.stock_lot_id,
            supplier_receipt_id=row.supplier_receipt_id,
            supplier_receipt_item_id=row.supplier_receipt_item_id,
            shipment_document_id=document_id,
            shipment_document_item_id=item_id,
            shipment_allocation_id=allocation.id,
            metadata_json={"source": source},
        )
    )


def _pick_allocation(
    allocations: list[ShipmentDocumentItemLotAllocation],
    lot_id: Optional[int],
) -> Optional[ShipmentDocumentItemLotAllocation]:
    """Списание со свободным местом под код; сначала — из партии кода."""
    free = [a for a in allocations if _capacity(a) > 0]
    return next(
        (a for a in free if a.stock_lot_id == lot_id), free[0] if free else None
    )


async def _align_lot(
    session: AsyncSession,
    *,
    document_id: int,
    item: ShipmentDocumentItem,
    allocations: list[ShipmentDocumentItemLotAllocation],
    donor: ShipmentDocumentItemLotAllocation,
    row: ProductMarkingCode,
) -> ShipmentDocumentItemLotAllocation:
    """Переносит одну штуку списания на партию отсканированного кода.

    Списание идёт по FIFO, а кладовщик мог взять штуку из другой партии.
    Чтобы остатки партий и себестоимость соответствовали тому, что реально
    ушло, одна штука возвращается в партию ``donor`` и списывается из партии
    кода. Если перенос невозможен (в партии кода нет остатка, она закреплена
    за заказом клиента), списание остаётся прежним — код всё равно уйдёт.
    """
    from dz_fastapi.models.inventory import MovementType
    from dz_fastapi.services.inventory_stock import (
        _apply_stock_delta,
        _quantize_money,
        _quantize_unit_cost,
        _to_decimal,
    )

    lot_id = row.stock_lot_id
    if lot_id is None or donor.stock_lot_id is None or donor.stock_lot_id == lot_id:
        return donor
    new_lot = await session.get(StockLot, lot_id)
    old_lot = await session.get(StockLot, donor.stock_lot_id)
    if (
        new_lot is None
        or old_lot is None
        or new_lot.autopart_id != item.autopart_id
        or int(new_lot.remaining_quantity or 0) < 1
    ):
        return donor
    if new_lot.source_receipt_item_id is not None:
        for_order = (
            await session.execute(
                select(SupplierReceiptItem.customer_order_item_id).where(
                    SupplierReceiptItem.id == new_lot.source_receipt_item_id
                )
            )
        ).scalar_one_or_none()
        if for_order is not None:
            return donor

    note = f"Отгрузка #{document_id}: партия по коду маркировки"
    await _apply_stock_delta(
        session,
        autopart_id=item.autopart_id,
        storage_location_id=old_lot.storage_location_id,
        quantity_delta=1,
        movement_type=MovementType.RECEIPT,
        reference_id=document_id,
        reference_type="shipment_lot_swap",
        notes=note,
        stock_lot_id=old_lot.id,
    )
    movement = await _apply_stock_delta(
        session,
        autopart_id=item.autopart_id,
        storage_location_id=new_lot.storage_location_id,
        quantity_delta=-1,
        movement_type=MovementType.SHIPMENT,
        reference_id=document_id,
        reference_type="shipment_document",
        notes=note,
        stock_lot_id=new_lot.id,
    )
    old_lot.remaining_quantity = int(old_lot.remaining_quantity or 0) + 1
    new_lot.remaining_quantity = int(new_lot.remaining_quantity or 0) - 1

    def set_cost(allocation: ShipmentDocumentItemLotAllocation) -> None:
        unit = _to_decimal(allocation.unit_cost_price)
        allocation.total_cost_price = (
            _quantize_money(unit * int(allocation.quantity)) if unit is not None else None
        )

    donor.quantity = int(donor.quantity or 0) - 1
    set_cost(donor)
    target = next((a for a in allocations if a.stock_lot_id == lot_id), None)
    if target is None:
        receipt = None
        if new_lot.source_receipt_id is not None:
            from dz_fastapi.models.partner import SupplierReceipt

            receipt = await session.get(SupplierReceipt, new_lot.source_receipt_id)
        target = ShipmentDocumentItemLotAllocation(
            shipment_document_item_id=item.id,
            stock_lot_id=lot_id,
            stock_movement_id=movement.id if movement is not None else None,
            provider_id=receipt.provider_id if receipt is not None else None,
            quantity=1,
            unit_cost_price=_quantize_unit_cost(_to_decimal(new_lot.cost_price)),
            marking_codes=[],
        )
        session.add(target)
        allocations.append(target)
    else:
        target.quantity = int(target.quantity or 0) + 1
    set_cost(target)
    if donor.quantity <= 0 and not donor.marking_codes:
        allocations.remove(donor)
        await session.delete(donor)
        if item.lot_id == old_lot.id:
            item.lot_id = lot_id
    await session.flush()

    costs = [a.total_cost_price for a in allocations]
    if costs and all(c is not None for c in costs):
        total = sum((_to_decimal(c) for c in costs), start=_to_decimal(0))
        item.cost_total = _quantize_money(total)
        quantity = int(item.quantity or 0)
        item.cost_price = (
            _quantize_unit_cost(total / quantity) if quantity > 0 else None
        )
    else:
        item.cost_total = None
        item.cost_price = None
    return target


async def assign_scanned_codes(
    session: AsyncSession,
    *,
    document_id: int,
    item: ShipmentDocumentItem,
    allocations: list[ShipmentDocumentItemLotAllocation],
) -> int:
    """При проведении закрепляет за строкой коды, отсканированные при сборке."""
    rows = (
        (
            await session.execute(
                select(ProductMarkingCode)
                .join(
                    ShipmentMarkingScan,
                    ShipmentMarkingScan.marking_code_id == ProductMarkingCode.id,
                )
                .where(
                    ShipmentMarkingScan.shipment_document_id == document_id,
                    ShipmentMarkingScan.shipment_document_item_id == item.id,
                    ProductMarkingCode.status == MarkingCodeStatus.IN_STOCK,
                )
                .order_by(ShipmentMarkingScan.id)
                .with_for_update(of=ProductMarkingCode)
            )
        )
        .scalars()
        .all()
    )
    assigned = 0
    for row in rows:
        allocation = _pick_allocation(allocations, row.stock_lot_id)
        if allocation is None:
            break
        allocation = await _align_lot(
            session,
            document_id=document_id,
            item=item,
            allocations=allocations,
            donor=allocation,
            row=row,
        )
        _ship(
            session,
            row,
            allocation=allocation,
            document_id=document_id,
            item_id=item.id,
            source=SOURCE_SCAN,
        )
        assigned += 1
    if assigned:
        await session.flush()
    return assigned


async def _replace_in_posted(
    session: AsyncSession,
    *,
    document: ShipmentDocument,
    item: ShipmentDocumentItem,
    row: ProductMarkingCode,
    scanned_codes: set[str],
) -> None:
    """Проведённая отгрузка: отсканированный код вместо несканированного."""
    allocations = list(item.allocations or [])
    if not allocations:
        raise ShipmentMarkingScanError(
            "У строки нет списания по партиям — код закрепить не к чему"
        )
    allocation = _pick_allocation(allocations, row.stock_lot_id)
    if allocation is None:
        candidates = [
            (a, code)
            for a in allocations
            for code in (a.marking_codes or [])
            if code not in scanned_codes
        ]
        if not candidates:
            raise ShipmentMarkingScanError(
                "По этой строке уже отсканированы все коды"
            )
        allocation, old_code = next(
            (c for c in candidates if c[0].stock_lot_id == row.stock_lot_id),
            candidates[0],
        )
        old = (
            await session.execute(
                select(ProductMarkingCode)
                .where(ProductMarkingCode.code == old_code)
                .with_for_update(of=ProductMarkingCode)
            )
        ).scalar_one_or_none()
        allocation.marking_codes = [
            code for code in (allocation.marking_codes or []) if code != old_code
        ]
        if old is not None:
            old.status = MarkingCodeStatus.IN_STOCK
            old.shipment_allocation_id = None
            old.shipment_document_item_id = None
            old.shipment_document_id = None
            old.shipped_at = None
            session.add(
                ProductMarkingCodeMovement(
                    marking_code_id=old.id,
                    movement_type=MarkingMovementType.UNPOSTED,
                    autopart_id=old.autopart_id,
                    stock_lot_id=old.stock_lot_id,
                    supplier_receipt_id=old.supplier_receipt_id,
                    supplier_receipt_item_id=old.supplier_receipt_item_id,
                    shipment_allocation_id=allocation.id,
                    metadata_json={
                        "source": SOURCE_SWAP,
                        "replaced_by": str(row.code),
                    },
                )
            )
    allocation = await _align_lot(
        session,
        document_id=document.id,
        item=item,
        allocations=allocations,
        donor=allocation,
        row=row,
    )
    _ship(
        session,
        row,
        allocation=allocation,
        document_id=document.id,
        item_id=item.id,
        source=SOURCE_SCAN,
    )


async def record_shipment_marking_scan(
    session: AsyncSession,
    document: ShipmentDocument,
    raw: object,
    *,
    user_id: Optional[int] = None,
) -> dict[str, Any]:
    """Записывает скан кода при сборке. Коммит — на вызывающем."""
    parsed = parse_marking_code(raw)
    if parsed is None:
        raise ShipmentMarkingScanError("Это не код маркировки")
    if document.status == ShipmentDocumentStatus.CANCELLED:
        raise ShipmentMarkingScanError("Отгрузка отменена")
    row = (
        await session.execute(
            select(ProductMarkingCode)
            .where(ProductMarkingCode.code == parsed.ki)
            .with_for_update(of=ProductMarkingCode)
        )
    ).scalar_one_or_none()
    if row is None:
        raise ShipmentMarkingScanError(
            "Код не числится в нашем учёте — передать его покупателю нельзя"
        )
    scans = list(
        (
            await session.execute(
                select(ShipmentMarkingScan).where(
                    ShipmentMarkingScan.shipment_document_id == document.id
                )
            )
        )
        .scalars()
        .all()
    )
    existing = next((s for s in scans if s.marking_code_id == row.id), None)
    if existing is not None:
        return {
            "id": existing.id,
            "code": parsed.ki,
            "item_id": existing.shipment_document_item_id,
            "duplicate": True,
            "replaced_auto_code": False,
        }
    items = [
        item
        for item in document.items or []
        if item.autopart_id is not None and item.autopart_id == row.autopart_id
    ]
    if not items:
        raise ShipmentMarkingScanError(
            "В отгрузке нет товара, к которому относится этот код"
        )
    scanned_by_item = Counter(s.shipment_document_item_id for s in scans)
    already_here = (
        row.status == MarkingCodeStatus.SHIPPED
        and row.shipment_document_id == document.id
    )
    if already_here:
        item = next(
            (i for i in items if i.id == row.shipment_document_item_id), items[0]
        )
    elif row.status == MarkingCodeStatus.IN_STOCK:
        other = (
            await session.execute(
                select(ShipmentDocument.id, ShipmentDocument.doc_number)
                .join(
                    ShipmentMarkingScan,
                    ShipmentMarkingScan.shipment_document_id == ShipmentDocument.id,
                )
                .where(
                    ShipmentMarkingScan.marking_code_id == row.id,
                    ShipmentDocument.id != document.id,
                    ShipmentDocument.status == ShipmentDocumentStatus.DRAFT,
                )
                .limit(1)
            )
        ).first()
        if other is not None:
            raise ShipmentMarkingScanError(
                "Код уже отсканирован в отгрузке "
                f"{other.doc_number or '#' + str(other.id)}"
            )
        item = next(
            (
                i
                for i in items
                if scanned_by_item[i.id] < int(i.quantity or 0)
            ),
            None,
        )
        if item is None:
            total = sum(int(i.quantity or 0) for i in items)
            raise ShipmentMarkingScanError(
                f"По этому товару уже отсканированы все {total} шт."
            )
    else:
        raise ShipmentMarkingScanError(
            "Код нельзя отгрузить: "
            + _STATUS_TEXT.get(row.status, str(row.status))
        )
    scan = ShipmentMarkingScan(
        shipment_document_id=document.id,
        shipment_document_item_id=item.id,
        marking_code_id=row.id,
        code=parsed.ki,
        scanned_by_user_id=user_id,
    )
    session.add(scan)
    replaced = False
    if document.status == ShipmentDocumentStatus.POSTED and not already_here:
        await _replace_in_posted(
            session,
            document=document,
            item=item,
            row=row,
            scanned_codes={s.code for s in scans} | {parsed.ki},
        )
        replaced = True
    await session.flush()
    return {
        "id": scan.id,
        "code": parsed.ki,
        "item_id": item.id,
        "duplicate": False,
        "replaced_auto_code": replaced,
    }


async def record_shipment_aggregate_scan(
    session: AsyncSession,
    document: ShipmentDocument,
    raw: object,
    *,
    user_id: Optional[int] = None,
) -> Optional[dict[str, Any]]:
    """Скан упаковки при сборке: закрепляет за отгрузкой каждую её штуку.

    Возвращает ``None``, если это не упаковка. Штуки, которые закрепить
    нельзя (нет в учёте, уже отгружены), перечисляются в ``errors``.
    """
    transport = parse_transport_code(raw)
    code = transport
    if code is None:
        parsed = parse_marking_code(raw)
        if parsed is None:
            return None
        known = (
            await session.execute(
                select(ProductMarkingCode.id).where(
                    ProductMarkingCode.code == parsed.ki
                )
            )
        ).scalar_one_or_none()
        # Групповая упаковка выглядит как обычный код, но в учёте её нет.
        if known is not None or await gis_mt_client_or_none(session) is None:
            return None
        code = parsed.ki
    try:
        units = await expand_aggregate(session, code)
    except AggregateError as exc:
        if transport is None:
            return None
        raise ShipmentMarkingScanError(str(exc)) from exc
    if not units:
        if transport is None:
            return None
        raise ShipmentMarkingScanError("ГИС МТ не знает вложений этой упаковки")

    accepted = duplicates = 0
    errors: list[dict[str, str]] = []
    for unit in units:
        try:
            async with session.begin_nested():
                result = await record_shipment_marking_scan(
                    session, document, unit, user_id=user_id
                )
        except ShipmentMarkingScanError as exc:
            errors.append({"code": unit, "message": str(exc)})
            continue
        if result["duplicate"]:
            duplicates += 1
        else:
            accepted += 1
    return {
        "aggregate": True,
        "code": code,
        "units": len(units),
        "accepted": accepted,
        "duplicates": duplicates,
        "errors": errors[:20],
        "errors_total": len(errors),
    }


async def shipment_marking_state(
    session: AsyncSession, document: ShipmentDocument
) -> dict[str, Any]:
    """Сколько кодов нужно, назначено и подтверждено сканом — по строкам."""
    items = list(document.items or [])
    autopart_ids = {int(i.autopart_id) for i in items if i.autopart_id}
    marked: set[int] = set()
    if autopart_ids:
        marked = set(
            (
                await session.execute(
                    select(ProductMarkingCode.autopart_id)
                    .where(ProductMarkingCode.autopart_id.in_(autopart_ids))
                    .distinct()
                )
            )
            .scalars()
            .all()
        )
    from dz_fastapi.services.marking_codes import autoparts_marked_by_category

    marked |= await autoparts_marked_by_category(session, autopart_ids)
    scans = list(
        (
            await session.execute(
                select(ShipmentMarkingScan)
                .where(ShipmentMarkingScan.shipment_document_id == document.id)
                .order_by(ShipmentMarkingScan.id)
            )
        )
        .scalars()
        .all()
    )
    lines = []
    for item in items:
        item_scans = [s for s in scans if s.shipment_document_item_id == item.id]
        scanned_codes = {s.code for s in item_scans}
        assigned = [
            code
            for allocation in (item.allocations or [])
            for code in (allocation.marking_codes or [])
        ]
        is_marked = bool(assigned or item_scans) or item.autopart_id in marked
        if not is_marked:
            continue
        autopart = item.__dict__.get("autopart")
        brand = getattr(autopart, "__dict__", {}).get("brand")
        lines.append(
            {
                "item_id": item.id,
                "autopart_id": item.autopart_id,
                "oem": getattr(autopart, "oem_number", None),
                "brand": getattr(brand, "name", None),
                "name": getattr(autopart, "name", None),
                "quantity": int(item.quantity or 0),
                "assigned": len(assigned),
                "scanned": len(item_scans),
                "unscanned_codes": [
                    code for code in assigned if code not in scanned_codes
                ],
                "scans": [{"id": s.id, "code": s.code} for s in item_scans],
            }
        )
    return {
        "shipment_id": document.id,
        "status": str(getattr(document.status, "value", document.status)),
        "auto_fifo": await shipment_codes_auto_fifo(session),
        "expected": sum(line["quantity"] for line in lines),
        "assigned": sum(line["assigned"] for line in lines),
        "scanned": sum(line["scanned"] for line in lines),
        "lines": lines,
    }
