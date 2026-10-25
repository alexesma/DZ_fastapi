"""Customer-order assembly, own-stock labels and partial realization drafts."""

from collections import defaultdict
from datetime import date, datetime, time
from decimal import Decimal
from uuid import uuid4

from sqlalchemy import String, cast, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload, selectinload

from dz_fastapi.models.autopart import StorageLocation
from dz_fastapi.models.inventory import (
    CrossDockingLabel,
    CustomerOrderPickLabel,
    ReserveStatus,
    ShipmentDocument,
    ShipmentDocumentItem,
    ShipmentDocumentStatus,
    ShipmentPackage,
    ShipmentPackageItem,
    StockByLocation,
    StockLot,
    StockReserve,
    Warehouse,
)
from dz_fastapi.models.partner import (
    CUSTOMER_ORDER_ITEM_STATUS,
    Customer,
    CustomerOrder,
    CustomerOrderItem,
    StockOrderItem,
    SupplierReceipt,
    SupplierReceiptItem,
)
from dz_fastapi.services.inventory_stock import ensure_default_warehouse


def _money(value):
    return float(value or 0)


async def _assembly_data(session: AsyncSession, order_ids: list[int] | None = None, *,
                         customer_id: int | None = None,
                         date_from: date | None = None,
                         date_to: date | None = None,
                         search: str | None = None,
                         limit: int = 200):
    stmt = select(CustomerOrder).options(
        joinedload(CustomerOrder.customer), selectinload(CustomerOrder.items),
    ).where(CustomerOrder.deleted_at.is_(None))
    if order_ids is not None:
        stmt = stmt.where(CustomerOrder.id.in_(order_ids))
    if customer_id is not None:
        stmt = stmt.where(CustomerOrder.customer_id == customer_id)
    if date_from is not None:
        stmt = stmt.where(CustomerOrder.received_at >= datetime.combine(date_from, time.min))
    if date_to is not None:
        stmt = stmt.where(CustomerOrder.received_at <= datetime.combine(date_to, time.max))
    if search:
        pattern = f"%{search.strip()[:100]}%"
        stmt = stmt.where(or_(
            cast(CustomerOrder.id, String).ilike(pattern),
            CustomerOrder.order_number.ilike(pattern),
            CustomerOrder.items.any(or_(
                CustomerOrderItem.oem.ilike(pattern),
                CustomerOrderItem.brand.ilike(pattern),
                CustomerOrderItem.name.ilike(pattern),
            )),
        ))
    orders = (await session.execute(
        stmt.order_by(CustomerOrder.received_at.desc(), CustomerOrder.id.desc()).limit(limit)
    )).scalars().unique().all()
    item_ids = [item.id for order in orders for item in order.items]
    if not item_ids:
        return orders, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}

    own_rows = (await session.execute(
        select(StockOrderItem).join(
            CustomerOrderItem,
            CustomerOrderItem.id == StockOrderItem.customer_order_item_id,
        ).where(
            StockOrderItem.customer_order_item_id.in_(item_ids),
            StockOrderItem.supplier_receipt_item_id.is_(None),
            CustomerOrderItem.status == CUSTOMER_ORDER_ITEM_STATUS.OWN_STOCK,
        )
    )).scalars().all()
    own_by_item = defaultdict(list)
    for row in own_rows:
        own_by_item[row.customer_order_item_id].append(row)
    own_ids = [row.id for row in own_rows]
    labels = (await session.execute(
        select(CustomerOrderPickLabel)
        .options(joinedload(CustomerOrderPickLabel.storage_location))
        .where(CustomerOrderPickLabel.stock_order_item_id.in_(own_ids))
    )).scalars().all() if own_ids else []
    own_labels = defaultdict(list)
    for label in labels:
        if label.cancelled_at is None:
            own_labels[label.stock_order_item_id].append(label)
    reserve_ids = {label.reserve_id for label in labels if label.reserve_id is not None}
    active_reserves = (await session.execute(
        select(StockReserve).where(
            StockReserve.id.in_(reserve_ids),
            StockReserve.status == ReserveStatus.ACTIVE,
        )
    )).scalars().all() if reserve_ids else []
    reserves_by_id = {reserve.id: reserve for reserve in active_reserves}

    own_part_ids = {row.autopart_id for row in own_rows if row.autopart_id is not None}
    location_choices = defaultdict(list)
    if own_part_ids:
        balances = (await session.execute(
            select(StockByLocation, StorageLocation)
            .join(StorageLocation, StorageLocation.id == StockByLocation.storage_location_id)
            .where(StockByLocation.autopart_id.in_(own_part_ids),
                   StockByLocation.quantity > 0)
            .order_by(StorageLocation.name, StorageLocation.id)
        )).all()
        reserves = (await session.execute(
            select(StockReserve.autopart_id, StockReserve.storage_location_id,
                   func.sum(StockReserve.quantity))
            .where(StockReserve.autopart_id.in_(own_part_ids),
                   StockReserve.status == ReserveStatus.ACTIVE)
            .group_by(StockReserve.autopart_id, StockReserve.storage_location_id)
        )).all()
        reserved = {(part_id, location_id): int(qty)
                    for part_id, location_id, qty in reserves}
        cross_lots = (await session.execute(
            select(StockLot.autopart_id, StockLot.storage_location_id,
                   func.sum(StockLot.remaining_quantity))
            .join(SupplierReceiptItem,
                  SupplierReceiptItem.id == StockLot.source_receipt_item_id)
            .where(StockLot.autopart_id.in_(own_part_ids),
                   StockLot.remaining_quantity > 0,
                   SupplierReceiptItem.customer_order_item_id.is_not(None))
            .group_by(StockLot.autopart_id, StockLot.storage_location_id)
        )).all()
        cross_stock = {(part_id, location_id): int(qty)
                       for part_id, location_id, qty in cross_lots}
        for balance, location in balances:
            part_id = balance.autopart_id
            capacity = max(0, int(balance.quantity)
                           - reserved.get((part_id, location.id), 0)
                           - reserved.get((part_id, None), 0)
                           - cross_stock.get((part_id, location.id), 0))
            if capacity:
                location_choices[part_id].append((location.id, location.name, capacity))

    receipts = (await session.execute(
        select(SupplierReceiptItem).join(SupplierReceipt)
        .where(SupplierReceiptItem.customer_order_item_id.in_(item_ids),
               SupplierReceipt.posted_at.is_not(None))
    )).scalars().all()
    receipt_by_item = defaultdict(list)
    for row in receipts:
        receipt_by_item[row.customer_order_item_id].append(row)
    receipt_ids = [row.id for row in receipts]
    cross_stock_rows = (await session.execute(
        select(StockOrderItem).where(
            StockOrderItem.supplier_receipt_item_id.in_(receipt_ids))
    )).scalars().all() if receipt_ids else []
    cross_stock_by_receipt = {
        row.supplier_receipt_item_id: row for row in cross_stock_rows
    }
    cross_labels = (await session.execute(
        select(CrossDockingLabel).where(
            CrossDockingLabel.supplier_receipt_item_id.in_(receipt_ids))
    )).scalars().all() if receipt_ids else []
    cross_by_receipt = {label.supplier_receipt_item_id: label for label in cross_labels}
    lots = (await session.execute(
        select(StockLot).where(StockLot.source_receipt_item_id.in_(receipt_ids))
    )).scalars().all() if receipt_ids else []
    lots_by_receipt = defaultdict(list)
    for lot in lots:
        lots_by_receipt[lot.source_receipt_item_id].append(lot)

    shipment_rows = (await session.execute(
        select(ShipmentDocumentItem, ShipmentDocument.status)
        .join(ShipmentDocument)
        .where(ShipmentDocumentItem.customer_order_item_id.in_(item_ids),
               ShipmentDocument.status != ShipmentDocumentStatus.CANCELLED)
    )).all()
    committed = defaultdict(int)
    source_used = defaultdict(int)
    draft_lot_used = defaultdict(int)
    for row, status in shipment_rows:
        committed[row.customer_order_item_id] += int(row.quantity or 0)
        if row.assembly_pick_label_id:
            source_used[("own", row.assembly_pick_label_id)] += int(row.quantity or 0)
        if row.cross_docking_label_id:
            source_used[("cross", row.cross_docking_label_id)] += int(row.quantity or 0)
        if status == ShipmentDocumentStatus.DRAFT and row.preferred_lot_id:
            draft_lot_used[row.preferred_lot_id] += int(row.quantity or 0)
    return (orders, own_by_item, own_labels, receipt_by_item,
            cross_by_receipt, lots_by_receipt, committed, source_used,
            location_choices, cross_stock_by_receipt, reserves_by_id,
            draft_lot_used)


def _sources(item, own_rows, own_labels, receipts, cross_by_receipt,
             lots_by_receipt, source_used, reserves_by_id, draft_lot_used):
    result = []
    for stock_row in own_rows.get(item.id, []):
        for label in own_labels.get(stock_row.id, []):
            used = source_used[("own", label.id)]
            reserve = reserves_by_id.get(label.reserve_id)
            result.append({
                "type": "own", "id": label.id, "stock_item_id": stock_row.id,
                "autopart_id": stock_row.autopart_id,
                "quantity": label.quantity,
                "free": max(
                    0,
                    min(label.quantity - used, reserve.quantity if reserve else 0),
                ),
                "location_id": label.storage_location_id,
                "location_name": label.storage_location.name,
                "reserve_id": label.reserve_id,
                "lot_id": None,
            })
    for receipt in receipts.get(item.id, []):
        label = cross_by_receipt.get(receipt.id)
        if label is None or int(label.print_count or 0) <= 0:
            continue
        remaining_label = max(
            0, min(int(label.quantity), int(receipt.received_quantity or 0))
            - source_used[("cross", label.id)],
        )
        for lot in sorted(lots_by_receipt.get(receipt.id, []), key=lambda row: row.id):
            free = min(
                remaining_label,
                max(0, int(lot.remaining_quantity or 0) - draft_lot_used[lot.id]),
            )
            if free <= 0:
                continue
            result.append({
                "type": "cross", "id": label.id, "stock_item_id": None,
                "autopart_id": lot.autopart_id,
                "quantity": free, "free": free,
                "location_id": lot.storage_location_id, "location_name": None,
                "reserve_id": None, "lot_id": lot.id,
            })
            remaining_label -= free
            if remaining_label <= 0:
                break
    return result


async def list_assembly(session: AsyncSession, *, customer_id: int | None = None,
                        order_ids: list[int] | None = None,
                        date_from: date | None = None,
                        date_to: date | None = None,
                        search: str | None = None,
                        limit: int = 200):
    (orders, own_rows, own_labels, receipts, cross_by_receipt,
     lots_by_receipt, committed, source_used, location_choices,
     cross_stock_by_receipt, reserves_by_id, draft_lot_used) = await _assembly_data(
         session, order_ids, customer_id=customer_id,
         date_from=date_from, date_to=date_to, search=search, limit=limit)
    result = []
    for order in orders:
        items = []
        totals = defaultdict(float)
        counts = defaultdict(int)
        for item in order.items:
            ordered = max(int(item.ship_qty or 0), 0)
            own_ordered = sum(max(int(row.quantity or 0), 0) for row in own_rows.get(item.id, []))
            received = sum(
                max(int(row.received_quantity or 0), 0)
                for row in receipts.get(item.id, [])
            )
            cross_labeled = sum(
                min(int(cross_by_receipt[row.id].quantity or 0),
                    int(row.received_quantity or 0))
                for row in receipts.get(item.id, [])
                if row.id in cross_by_receipt and int(cross_by_receipt[row.id].print_count or 0) > 0
            )
            own_labeled = sum(
                int(label.quantity or 0) for row in own_rows.get(item.id, [])
                for label in own_labels.get(row.id, [])
            )
            stock_items = []
            for row in own_rows.get(item.id, []):
                printed = sum(label.quantity for label in own_labels.get(row.id, []))
                missing = max(0, int(row.quantity or 0) - printed)
                choices = location_choices.get(row.autopart_id, [])
                fit = next((choice for choice in choices if choice[2] >= missing), None)
                printed_locations = [
                    {"id": label.storage_location_id,
                     "name": label.storage_location.name}
                    for label in own_labels.get(row.id, [])
                ]
                stock_items.append({
                    "id": row.id, "quantity": int(row.quantity or 0),
                    "labeled": printed, "remaining": missing,
                    "location": fit[1] if fit else (
                        ", ".join(name for _, name, _ in choices) if choices else None
                    ),
                    "location_split": bool(choices and fit is None),
                    "location_choices": [
                        {"id": location_id, "name": name, "available": capacity}
                        for location_id, name, capacity in choices
                    ],
                    "printed_locations": printed_locations,
                })
            cross_stock_items = [
                {"id": cross_stock_by_receipt[receipt.id].id,
                 "quantity": cross_stock_by_receipt[receipt.id].quantity,
                 "picked_quantity": cross_stock_by_receipt[receipt.id].picked_quantity}
                for receipt in receipts.get(item.id, [])
                if receipt.id in cross_stock_by_receipt
            ]
            sources = _sources(item, own_rows, own_labels, receipts,
                               cross_by_receipt, lots_by_receipt, source_used,
                               reserves_by_id, draft_lot_used)
            available = max(0, min(ordered - committed[item.id],
                                   sum(source["free"] for source in sources)))
            price = _money(item.requested_price)
            totals["order"] += price * int(item.requested_qty or 0)
            totals["own"] += price * own_ordered
            totals["cross"] += price * max(0, ordered - own_ordered)
            counts["ordered"] += ordered
            counts["lines"] += 1
            counts["received"] += received
            counts["cross_labeled"] += cross_labeled
            counts["own_labeled"] += own_labeled
            counts["ready"] += available
            source_payload = item.source_payload or {}
            pending_switch = source_payload.get("receipt_stock_source_suggestion")
            source_switch = (
                pending_switch if pending_switch and pending_switch.get("status") == "blocked"
                else source_payload.get("receipt_stock_source_switch")
            )
            items.append({
                "id": item.id, "order_id": order.id,
                "customer_id": order.customer_id,
                "brand": item.brand, "oem": item.oem, "name": item.name,
                "actual_brand": item.actual_brand,
                "actual_oem": item.actual_oem,
                "source_switch": source_switch,
                "price": price, "ordered": ordered, "own_ordered": own_ordered,
                "received": received, "cross_labeled": cross_labeled,
                "own_labeled": own_labeled, "committed": committed[item.id],
                "ready": available,
                "stock_item_ids": [row.id for row in own_rows.get(item.id, [])],
                "stock_items": stock_items,
                "cross_stock_items": cross_stock_items,
                "sources": sources,
            })
        result.append({
            "id": order.id, "number": order.order_number or str(order.id),
            "received_at": order.received_at,
            "customer_id": order.customer_id,
            "customer_name": (
                getattr(order.customer, "document_name", None)
                or getattr(order.customer, "name", None)
                or "—"
            ),
            "customer_label_name": (
                (getattr(order.customer, "label_name", None) or "").strip()
                or getattr(order.customer, "document_name", None)
                or getattr(order.customer, "name", None)
                or "—"
            ),
            "amounts": dict(totals), "counts": dict(counts), "items": items,
        })
    return result


async def _own_location_capacity(session: AsyncSession, autopart_id: int,
                                 location_id: int) -> int:
    physical = (await session.execute(
        select(func.coalesce(func.sum(StockByLocation.quantity), 0)).where(
            StockByLocation.autopart_id == autopart_id,
            StockByLocation.storage_location_id == location_id,
        )
    )).scalar_one()
    reserved = (await session.execute(
        select(func.coalesce(func.sum(StockReserve.quantity), 0)).where(
            StockReserve.autopart_id == autopart_id,
            StockReserve.status == ReserveStatus.ACTIVE,
            (StockReserve.storage_location_id == location_id)
            | StockReserve.storage_location_id.is_(None),
        )
    )).scalar_one()
    cross_lots = (await session.execute(
        select(func.coalesce(func.sum(StockLot.remaining_quantity), 0))
        .join(SupplierReceiptItem,
              SupplierReceiptItem.id == StockLot.source_receipt_item_id)
        .where(StockLot.autopart_id == autopart_id,
               StockLot.storage_location_id == location_id,
               StockLot.remaining_quantity > 0,
               SupplierReceiptItem.customer_order_item_id.is_not(None))
    )).scalar_one()
    return max(0, int(physical) - int(reserved) - int(cross_lots))


async def print_own_labels(session: AsyncSession, stock_item_ids: list[int],
                           user_id: int,
                           storage_location_id: int | None = None):
    if not stock_item_ids or len(stock_item_ids) > 200:
        raise ValueError("Выберите от 1 до 200 складских позиций")
    rows = (await session.execute(
        select(StockOrderItem).where(StockOrderItem.id.in_(set(stock_item_ids)))
        .with_for_update()
    )).scalars().all()
    if len(rows) != len(set(stock_item_ids)):
        raise ValueError("Часть складских позиций не найдена")
    results = []
    for row in rows:
        if row.supplier_receipt_item_id is not None or row.customer_order_item_id is None:
            raise ValueError("Этикетки этого раздела доступны только для нашего склада")
        customer_item = await session.get(CustomerOrderItem, row.customer_order_item_id)
        if customer_item is None or customer_item.status != CUSTOMER_ORDER_ITEM_STATUS.OWN_STOCK:
            raise ValueError("Позиция не относится к нашему складу")
        order = await session.get(CustomerOrder, customer_item.order_id)
        if order.deleted_at is not None or row.autopart_id is None:
            raise ValueError("Позиция заказа неактивна")
        await session.execute(
            select(CustomerOrder.id).where(CustomerOrder.id == order.id)
            .with_for_update()
        )
        existing = (await session.execute(
            select(func.coalesce(func.sum(CustomerOrderPickLabel.quantity), 0)).where(
                CustomerOrderPickLabel.stock_order_item_id == row.id,
                CustomerOrderPickLabel.cancelled_at.is_(None),
            )
        )).scalar_one()
        labeled_total = (await session.execute(
            select(func.coalesce(func.sum(CustomerOrderPickLabel.quantity), 0))
            .join(StockOrderItem,
                  StockOrderItem.id == CustomerOrderPickLabel.stock_order_item_id)
            .where(StockOrderItem.customer_order_item_id == customer_item.id,
                   CustomerOrderPickLabel.cancelled_at.is_(None))
        )).scalar_one()
        unlabelled_committed = (await session.execute(
            select(func.coalesce(func.sum(ShipmentDocumentItem.quantity), 0))
            .join(ShipmentDocument)
            .where(ShipmentDocumentItem.customer_order_item_id == customer_item.id,
                   ShipmentDocumentItem.assembly_pick_label_id.is_(None),
                   ShipmentDocument.status != ShipmentDocumentStatus.CANCELLED)
        )).scalar_one()
        needed = max(
            0,
            min(
                int(row.quantity or 0) - int(existing),
                int(customer_item.ship_qty or 0)
                - int(labeled_total) - int(unlabelled_committed),
            ),
        )
        if not needed:
            continue
        location_rows = (await session.execute(
            select(StockByLocation, StorageLocation)
            .join(StorageLocation, StorageLocation.id == StockByLocation.storage_location_id)
            .where(StockByLocation.autopart_id == row.autopart_id,
                   StockByLocation.quantity > 0)
            .order_by(StorageLocation.name, StorageLocation.id)
            .with_for_update(of=StockByLocation)
        )).all()
        capacities = []
        for _, location in location_rows:
            if storage_location_id is not None and location.id != storage_location_id:
                continue
            capacity = await _own_location_capacity(session, row.autopart_id, location.id)
            if capacity > 0:
                capacities.append((location, capacity))
        first_fit = next((pair for pair in capacities if pair[1] >= needed), None)
        chosen = [first_fit] if first_fit else capacities
        printed_qty = 0
        customer = await session.get(Customer, order.customer_id)
        for location, capacity in chosen:
            qty = min(needed, capacity)
            if qty <= 0:
                continue
            reserve = StockReserve(
                autopart_id=row.autopart_id, storage_location_id=location.id,
                quantity=qty, status=ReserveStatus.ACTIVE,
                customer_order_item_id=customer_item.id,
                stock_order_item_id=row.id,
                notes="Этикетка отбора заказа клиента",
            )
            session.add(reserve)
            await session.flush()
            label = CustomerOrderPickLabel(
                stock_order_item_id=row.id, storage_location_id=location.id,
                reserve_id=reserve.id, quantity=qty, barcode=uuid4().hex,
                printed_by_user_id=user_id,
            )
            session.add(label)
            await session.flush()
            label.barcode = f"DZ-OWN-{label.id:010d}"
            results.append({
                "id": label.id, "barcode": label.barcode,
                "quantity": qty, "location": location.name,
                "brand": customer_item.brand, "oem": customer_item.oem,
                "name": customer_item.name,
                "order_number": order.order_number or str(order.id),
                "customer_name": (
                    (customer.label_name or "").strip() or customer.document_name
                    if customer else ""
                ),
            })
            needed -= qty
            printed_qty += qty
            if needed == 0:
                break
        row.picked_quantity = min(
            int(row.quantity or 0), int(row.picked_quantity or 0) + printed_qty
        )
    await session.flush()
    return results


async def create_realization_draft(session: AsyncSession, order_ids: list[int]):
    if not order_ids or len(order_ids) > 100:
        raise ValueError("Выберите от 1 до 100 заказов клиента")
    selected_ids = sorted(set(order_ids))
    locked = (await session.execute(
        select(CustomerOrder).where(CustomerOrder.id.in_(selected_ids)).with_for_update()
    )).scalars().all()
    if len(locked) != len(selected_ids) or any(row.deleted_at is not None for row in locked):
        raise ValueError("Один из заказов не найден")
    customers = {row.customer_id for row in locked}
    if len(customers) != 1:
        raise ValueError("В один документ можно объединить только заказы одного клиента")
    data = await list_assembly(session, order_ids=selected_ids, limit=len(selected_ids))
    warehouses = (await session.execute(
        select(Warehouse).where(Warehouse.is_active.is_(True)).order_by(Warehouse.id)
    )).scalars().all()
    warehouse = warehouses[0] if len(warehouses) == 1 else await ensure_default_warehouse(session)
    doc = ShipmentDocument(
        customer_id=next(iter(customers)),
        customer_order_id=selected_ids[0] if len(selected_ids) == 1 else None,
        warehouse_id=warehouse.id, assembly_managed=True,
        reason="Комплектация заказов клиентов",
        notes="Заказы клиентов: " + ", ".join(map(str, selected_ids)),
    )
    session.add(doc)
    await session.flush()
    doc.doc_number = f"SHP-CO-{doc.id:06d}"
    total = 0
    draft_items = []
    for order in data:
        for item in order["items"]:
            remaining = int(item["ready"])
            for source in item["sources"]:
                qty = min(remaining, int(source["free"]))
                if qty <= 0:
                    continue
                draft_item = ShipmentDocumentItem(
                    document_id=doc.id,
                    autopart_id=source["autopart_id"],
                    customer_order_item_id=item["id"],
                    customer_oem=item["oem"], customer_brand=item["brand"],
                    customer_name=item["name"], quantity=qty,
                    price=Decimal(str(item["price"])), vat_rate=Decimal("22.00"),
                    storage_location_id=source["location_id"],
                    reserve_id=source["reserve_id"],
                    preferred_lot_id=source["lot_id"],
                    assembly_pick_label_id=source["id"] if source["type"] == "own" else None,
                    cross_docking_label_id=source["id"] if source["type"] == "cross" else None,
                )
                session.add(draft_item)
                draft_items.append(draft_item)
                remaining -= qty
                total += qty
                if remaining == 0:
                    break
    if total == 0:
        raise ValueError("В выбранных заказах пока нет готового количества")
    await session.flush()
    for draft_item in draft_items:
        await validate_assembly_quantity(session, draft_item, int(draft_item.quantity))
    return doc


async def validate_assembly_quantity(session: AsyncSession,
                                     item: ShipmentDocumentItem,
                                     quantity: int) -> None:
    """Bound draft edits by printed sources and the unsent order remainder."""
    if quantity <= 0 or item.customer_order_item_id is None:
        raise ValueError("Количество должно быть больше нуля")
    customer_item = await session.get(CustomerOrderItem, item.customer_order_item_id)
    if customer_item is None:
        raise ValueError("Строка заказа клиента не найдена")
    await session.execute(
        select(CustomerOrder.id).where(CustomerOrder.id == customer_item.order_id)
        .with_for_update()
    )
    other_rows = (await session.execute(
        select(ShipmentDocumentItem).join(ShipmentDocument).where(
            ShipmentDocumentItem.customer_order_item_id == item.customer_order_item_id,
            ShipmentDocumentItem.id != item.id,
            ShipmentDocument.status != ShipmentDocumentStatus.CANCELLED,
        )
    )).scalars().all()
    other_order_qty = sum(int(row.quantity or 0) for row in other_rows)
    if quantity + other_order_qty > int(customer_item.ship_qty or 0):
        raise ValueError("Количество превышает остаток заказа клиента")
    if item.assembly_pick_label_id is not None:
        label = await session.get(CustomerOrderPickLabel, item.assembly_pick_label_id)
        if label is None or label.cancelled_at is not None:
            raise ValueError("Этикетка отбора отменена")
        used = sum(int(row.quantity or 0) for row in other_rows
                   if row.assembly_pick_label_id == label.id)
        if quantity + used > int(label.quantity or 0):
            raise ValueError("Количество превышает напечатанные этикетки")
        reserve = await session.get(StockReserve, label.reserve_id)
        if reserve is None or reserve.status != ReserveStatus.ACTIVE:
            raise ValueError("Резерв по этикетке отбора отсутствует")
        if quantity > int(reserve.quantity or 0):
            raise ValueError("Количество превышает остаток резерва")
    elif item.cross_docking_label_id is not None:
        label = await session.get(CrossDockingLabel, item.cross_docking_label_id)
        if label is None or int(label.print_count or 0) <= 0:
            raise ValueError("Этикетка поступления ещё не напечатана")
        receipt_item = await session.get(SupplierReceiptItem, label.supplier_receipt_item_id)
        receipt = (await session.get(SupplierReceipt, receipt_item.receipt_id)
                   if receipt_item else None)
        if receipt is None or receipt.posted_at is None:
            raise ValueError("Поступление поставщика не проведено")
        used = sum(int(row.quantity or 0) for row in other_rows
                   if row.cross_docking_label_id == label.id)
        if quantity + used > min(int(label.quantity or 0),
                                 int(receipt_item.received_quantity or 0)):
            raise ValueError("Количество превышает этикетку поступления")
        if item.preferred_lot_id is None:
            raise ValueError("Партия поступления не найдена")
        lot = await session.get(StockLot, item.preferred_lot_id)
        if lot is None:
            raise ValueError("Партия поступления не найдена")
        # Posted rows have already reduced lot.remaining_quantity; only drafts
        # need to be subtracted from the physical remainder.
        draft_rows = (await session.execute(
            select(ShipmentDocumentItem.quantity)
            .join(ShipmentDocument)
            .where(ShipmentDocumentItem.preferred_lot_id == lot.id,
                   ShipmentDocumentItem.id != item.id,
                   ShipmentDocument.status == ShipmentDocumentStatus.DRAFT)
        )).scalars().all()
        other_draft = sum(int(value or 0) for value in draft_rows)
        if quantity + other_draft > int(lot.remaining_quantity or 0):
            raise ValueError("Количество превышает остаток партии поступления")
    else:
        raise ValueError("У строки черновика нет источника комплектации")
    boxed = (await session.execute(
        select(func.coalesce(func.sum(ShipmentPackageItem.quantity), 0))
        .where(ShipmentPackageItem.shipment_item_id == item.id)
    )).scalar_one()
    if quantity < int(boxed):
        raise ValueError("Сначала уменьшите количество в коробках")


async def validate_assembly_packing(session: AsyncSession,
                                    doc: ShipmentDocument) -> None:
    if not doc.assembly_managed:
        return
    items = (await session.execute(
        select(ShipmentDocumentItem).where(ShipmentDocumentItem.document_id == doc.id)
    )).scalars().all()
    if not items:
        raise ValueError("В черновике нет позиций")
    customer_items = (await session.execute(
        select(CustomerOrderItem.id, CustomerOrderItem.order_id)
        .where(CustomerOrderItem.id.in_({item.customer_order_item_id for item in items}))
    )).all()
    order_ids = sorted({order_id for _, order_id in customer_items})
    await session.execute(
        select(CustomerOrder.id).where(CustomerOrder.id.in_(order_ids))
        .order_by(CustomerOrder.id).with_for_update()
    )
    packages = (await session.execute(
        select(ShipmentPackage).where(ShipmentPackage.document_id == doc.id)
    )).scalars().all()
    if not packages or any(package.sealed_at is None for package in packages):
        raise ValueError("Распределите товар по коробкам и закройте каждую коробку")
    item_ids = [item.id for item in items]
    allocations = (await session.execute(
        select(ShipmentPackageItem.shipment_item_id,
               func.sum(ShipmentPackageItem.quantity))
        .where(ShipmentPackageItem.shipment_item_id.in_(item_ids))
        .group_by(ShipmentPackageItem.shipment_item_id)
    )).all()
    boxed = {item_id: int(qty) for item_id, qty in allocations}
    for item in items:
        await validate_assembly_quantity(session, item, int(item.quantity))
        if boxed.get(item.id, 0) != int(item.quantity):
            raise ValueError("Количество в коробках не совпадает с черновиком")
