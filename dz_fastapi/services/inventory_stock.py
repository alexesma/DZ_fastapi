"""
Inventory stock service.

Use-case functions (public API of this module):
  receive_stock(...)              — post / unpost a supplier receipt
  writeoff_stock_fifo(...)        — FIFO write-off with reason
  reconcile_stock_absolute(...)   — set absolute quantity
  (inventory correction)
  transfer_stock_with_lot_trace(...)  — move between locations preserving GTD
  post_stock_document(...)        — post a manual receipt / write-off document
  unpost_stock_document(...)      — reverse a posted document
  backfill_opening_balance_lots() — one-time: create opening_balance lots for
                                    all stock rows that have no lot yet
  dispatch_stock_order(...)       — FIFO shipment for a stock order
  get_lots_for_autopart(...)      — query lots for a given autopart

Internal helpers (prefixed with _):
  _apply_stock_delta(...)         — low-level: update StockByLocation + create
                                    StockMovement
  _create_stock_lot(...)          — low-level: insert new StockLot
  _consume_fifo(...)              — internal FIFO engine (no top-level callers
                                    should use this directly)
  _reverse_receipt_lots(...)      — delete / zero lots on receipt unpost

Invariants enforced by this module:
  1. sum(StockLot.remaining_quantity for lot where lot.autopart_id=X,
         lot.storage_location_id=L) == StockByLocation.quantity(X, L)
     — maintained by always going through _apply_stock_delta + lot updates
       in the same transaction.
  2. Every write-off goes through FIFO — no direct quantity decrements.
  3. Every receipt creates a StockLot (source_type=RECEIPT or MANUAL).
"""
from __future__ import annotations

import logging
from typing import Optional

from sqlalchemy import asc, func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from dz_fastapi.core.time import now_moscow
from dz_fastapi.models.autopart import (LocationType, StorageLocation,
                                        autopart_storage_association)
from dz_fastapi.models.brand import Brand
from dz_fastapi.models.inventory import (LotSourceType, MovementType,
                                         StockByLocation, StockDocument,
                                         StockDocumentStatus,
                                         StockDocumentType, StockLot,
                                         StockMovement, Warehouse)
from dz_fastapi.models.partner import (Provider, SupplierReceipt,
                                       SupplierReceiptItem)

logger = logging.getLogger(__name__)

DEFAULT_WAREHOUSE_NAME = 'Основной склад'
DEFAULT_WAREHOUSE_COMMENT = (
    'Склад по умолчанию для входящих документов и первичного размещения.'
)
RECEIVING_LOCATION_CODE = 'RECEIVING'


# ═══════════════════════════════════════════════════════════════════════════════
# Warehouse / location helpers
# ═══════════════════════════════════════════════════════════════════════════════

def _normalize_system_location_name(warehouse_id: int) -> str:
    return f'WH{int(warehouse_id)} RECEIVING'


async def get_warehouse_by_id(
    session: AsyncSession,
    warehouse_id: int,
) -> Optional[Warehouse]:
    return await session.get(Warehouse, int(warehouse_id))


async def ensure_default_warehouse(session: AsyncSession) -> Warehouse:
    stmt = select(Warehouse).where(Warehouse.name == DEFAULT_WAREHOUSE_NAME)
    warehouse = (await session.execute(stmt)).scalar_one_or_none()
    if warehouse is None:
        warehouse = Warehouse(
            name=DEFAULT_WAREHOUSE_NAME,
            comment=DEFAULT_WAREHOUSE_COMMENT,
            is_active=True,
        )
        session.add(warehouse)
        await session.flush()
    await ensure_receiving_location(session, warehouse)
    return warehouse


async def resolve_warehouse_for_provider(
    session: AsyncSession,
    *,
    provider_id: int | None = None,
    explicit_warehouse_id: int | None = None,
) -> Warehouse:
    if explicit_warehouse_id is not None:
        warehouse = await get_warehouse_by_id(
            session,
            int(explicit_warehouse_id)
        )
        if warehouse is None:
            raise LookupError('Склад не найден')
        return warehouse

    if provider_id is not None:
        provider = await session.get(Provider, int(provider_id))
        if provider is not None and provider.default_warehouse_id is not None:
            warehouse = await get_warehouse_by_id(
                session, int(provider.default_warehouse_id)
            )
            if warehouse is not None:
                return warehouse

    return await ensure_default_warehouse(session)


async def ensure_receiving_location(
    session: AsyncSession,
    warehouse: Warehouse,
) -> StorageLocation:
    stmt = select(StorageLocation).where(
        StorageLocation.warehouse_id == warehouse.id,
        StorageLocation.system_code == RECEIVING_LOCATION_CODE,
    )
    location = (await session.execute(stmt)).scalar_one_or_none()
    if location is not None:
        return location

    location = StorageLocation(
        name=_normalize_system_location_name(int(warehouse.id)),
        warehouse_id=warehouse.id,
        location_type=LocationType.OTHER,
        capacity=None,
        system_code=RECEIVING_LOCATION_CODE,
    )
    session.add(location)
    await session.flush()
    return location


async def resolve_receipt_item_autopart_id(
    session: AsyncSession,
    item: SupplierReceiptItem,
) -> Optional[int]:
    if item.autopart_id is not None:
        return int(item.autopart_id)

    oem_number = str(item.oem_number or '').strip()
    if not oem_number:
        return None

    from dz_fastapi.models.autopart import AutoPart  # avoid circular import

    parts = (
        await session.execute(
            select(AutoPart)
            .where(AutoPart.oem_number == oem_number)
            .options(selectinload(AutoPart.brand))
        )
    ).scalars().all()
    if not parts:
        return None
    if len(parts) == 1:
        return int(parts[0].id)

    brand_name = str(item.brand_name or '').strip()
    if not brand_name:
        return None

    normalized_brand = brand_name.casefold()
    for part in parts:
        brand = getattr(part, 'brand', None)
        if brand and str(
                brand.name or ''
        ).strip().casefold() == normalized_brand:
            return int(part.id)

    brand_stmt = select(Brand.id).where(Brand.name.ilike(brand_name))
    brand_id = (await session.execute(brand_stmt)).scalar_one_or_none()
    if brand_id is None:
        return None
    for part in parts:
        if int(part.brand_id or 0) == int(brand_id):
            return int(part.id)
    return None


# ═══════════════════════════════════════════════════════════════════════════════
# Internal low-level primitives
# ═══════════════════════════════════════════════════════════════════════════════

async def _ensure_autopart_location_link(
    session: AsyncSession,
    *,
    autopart_id: int,
    storage_location_id: int,
) -> None:
    exists_stmt = select(autopart_storage_association.c.autopart_id).where(
        autopart_storage_association.c.autopart_id == autopart_id,
        autopart_storage_association.c.storage_location_id
        == storage_location_id,
    )
    exists_row = (await session.execute(exists_stmt)).first()
    if exists_row is not None:
        return
    await session.execute(
        autopart_storage_association.insert().values(
            autopart_id=autopart_id,
            storage_location_id=storage_location_id,
        )
    )


async def _apply_stock_delta(
    session: AsyncSession,
    *,
    autopart_id: int,
    storage_location_id: int,
    quantity_delta: int,
    movement_type: MovementType,
    reference_id: int | None = None,
    reference_type: str | None = None,
    notes: str | None = None,
    stock_lot_id: int | None = None,
    operation_uid: str | None = None,
) -> Optional[StockMovement]:
    """Update StockByLocation and record a StockMovement.

    Returns the created StockMovement, or None if quantity_delta == 0.
    Raises ValueError if the resulting stock would go negative.
    """
    quantity_delta = int(quantity_delta or 0)
    if quantity_delta == 0:
        return None

    stmt = select(StockByLocation).where(
        StockByLocation.autopart_id == autopart_id,
        StockByLocation.storage_location_id == storage_location_id,
    )
    stock_row = (await session.execute(stmt)).scalar_one_or_none()
    qty_before = int(stock_row.quantity or 0) if stock_row is not None else 0
    qty_after = qty_before + quantity_delta
    if qty_after < 0:
        raise ValueError(
            f'Недостаточно остатка для движения: '
            f'autopart_id={autopart_id} location_id={storage_location_id} '
            f'before={qty_before} delta={quantity_delta}'
        )

    if stock_row is None:
        stock_row = StockByLocation(
            autopart_id=autopart_id,
            storage_location_id=storage_location_id,
            quantity=qty_after,
        )
        session.add(stock_row)
        await session.flush()  # prevent UniqueViolation on bulk inserts
    elif qty_after == 0:
        await session.delete(stock_row)
    else:
        stock_row.quantity = qty_after
        stock_row.updated_at = now_moscow()

    if qty_after > 0:
        await _ensure_autopart_location_link(
            session,
            autopart_id=autopart_id,
            storage_location_id=storage_location_id,
        )

    movement = StockMovement(
        autopart_id=autopart_id,
        storage_location_id=storage_location_id,
        movement_type=movement_type,
        quantity=quantity_delta,
        qty_before=qty_before,
        qty_after=qty_after,
        reference_id=reference_id,
        reference_type=reference_type,
        notes=notes,
        stock_lot_id=stock_lot_id,
        operation_uid=operation_uid,
    )
    session.add(movement)
    await session.flush()  # populate movement.id before returning
    return movement


# Keep the old name as an alias so existing call-sites outside this module
# don't break while we migrate them to the explicit use-case functions.
apply_stock_delta = _apply_stock_delta


async def _create_stock_lot(
    session: AsyncSession,
    *,
    autopart_id: int,
    storage_location_id: int,
    quantity: int,
    source_type: LotSourceType = LotSourceType.RECEIPT,
    gtd_number: Optional[str] = None,
    country_code: Optional[str] = None,
    country_name: Optional[str] = None,
    source_receipt_id: Optional[int] = None,
    source_receipt_item_id: Optional[int] = None,
    source_document_item_id: Optional[int] = None,
    received_at=None,
    external_id: Optional[str] = None,
) -> StockLot:
    """Insert a new StockLot and return it (flushed, so .id is available)."""
    lot = StockLot(
        autopart_id=autopart_id,
        storage_location_id=storage_location_id,
        source_type=source_type,
        gtd_number=str(gtd_number).strip() if gtd_number else None,
        country_code=str(country_code).strip() if country_code else None,
        country_name=str(country_name).strip() if country_name else None,
        initial_quantity=quantity,
        remaining_quantity=quantity,
        source_receipt_id=source_receipt_id,
        source_receipt_item_id=source_receipt_item_id,
        source_document_item_id=source_document_item_id,
        received_at=received_at or now_moscow(),
        external_id=external_id,
    )
    session.add(lot)
    await session.flush()
    return lot


async def _consume_fifo(
    session: AsyncSession,
    *,
    autopart_id: int,
    storage_location_id: Optional[int],
    quantity: int,
    movement_type: MovementType,
    reference_id: Optional[int] = None,
    reference_type: Optional[str] = None,
    notes: Optional[str] = None,
) -> list[StockMovement]:
    """Internal FIFO engine.

    Deducts `quantity` units starting from the oldest lots.
    storage_location_id=None → global FIFO across all locations for the part.
    Returns list of created StockMovement objects (one per lot touched).
    """
    quantity = int(quantity)
    if quantity <= 0:
        return []

    lots_stmt = select(StockLot).where(
        StockLot.autopart_id == autopart_id,
        StockLot.remaining_quantity > 0,
    )
    if storage_location_id is not None:
        lots_stmt = lots_stmt.where(
            StockLot.storage_location_id == storage_location_id
        )
    lots_stmt = lots_stmt.order_by(asc(StockLot.received_at), asc(StockLot.id))
    lots = (await session.execute(lots_stmt)).scalars().all()

    remaining_to_consume = quantity
    movements: list[StockMovement] = []

    for lot in lots:
        if remaining_to_consume <= 0:
            break
        take = min(lot.remaining_quantity, remaining_to_consume)
        lot.remaining_quantity -= take
        remaining_to_consume -= take

        effective_location = (
            storage_location_id
            if storage_location_id is not None
            else lot.storage_location_id
        )

        mv = await _apply_stock_delta(
            session,
            autopart_id=autopart_id,
            storage_location_id=effective_location,
            quantity_delta=-take,
            movement_type=movement_type,
            reference_id=reference_id,
            reference_type=reference_type,
            notes=notes,
            stock_lot_id=lot.id,
        )
        if mv is not None:
            movements.append(mv)

    # Handle unlotted stock (pre-dates lot tracking)
    if remaining_to_consume > 0:
        fallback_location = storage_location_id
        if fallback_location is None:
            sbl_stmt = (
                select(StockByLocation)
                .where(
                    StockByLocation.autopart_id == autopart_id,
                    StockByLocation.quantity > 0,
                )
                .limit(1)
            )
            sbl = (await session.execute(sbl_stmt)).scalar_one_or_none()
            if sbl:
                fallback_location = sbl.storage_location_id

        mv = await _apply_stock_delta(
            session,
            autopart_id=autopart_id,
            storage_location_id=fallback_location,
            quantity_delta=-remaining_to_consume,
            movement_type=movement_type,
            reference_id=reference_id,
            reference_type=reference_type,
            notes=notes,
            stock_lot_id=None,
        )
        if mv is not None:
            movements.append(mv)

    return movements


# Keep old name as alias for callers outside this module
consume_stock_fifo = _consume_fifo


async def _reverse_receipt_lots(
    session: AsyncSession,
    *,
    receipt_id: int,
) -> None:
    """Delete/zero lots created when the receipt was posted.

    - Untouched lots (remaining == initial) → physically deleted.
    - Partially consumed lots → zeroed (preserves audit trail).
    """
    stmt = select(StockLot).where(StockLot.source_receipt_id == receipt_id)
    lots = (await session.execute(stmt)).scalars().all()
    for lot in lots:
        if lot.remaining_quantity == lot.initial_quantity:
            await session.delete(lot)
        else:
            lot.remaining_quantity = 0


# ═══════════════════════════════════════════════════════════════════════════════
# Use-case: receive_stock  (supplier receipt post / unpost)
# ═══════════════════════════════════════════════════════════════════════════════

async def receive_stock(
    session: AsyncSession,
    *,
    receipt: SupplierReceipt,
    reverse: bool = False,
) -> None:
    """Post (or unpost) a SupplierReceipt to the stock ledger.

    On post:   creates StockLot + StockMovement(RECEIPT) per item.
    On unpost: deletes/zeros lots, creates negative StockMovement(RECEIPT).
    """
    doc_warehouse = await resolve_warehouse_for_provider(
        session,
        provider_id=receipt.provider_id,
        explicit_warehouse_id=receipt.warehouse_id,
    )
    receipt.warehouse_id = doc_warehouse.id
    doc_receiving_location = await ensure_receiving_location(
        session,
        doc_warehouse
    )

    multiplier = -1 if reverse else 1
    note_prefix = 'Распроведение поступления' if reverse else 'Поступление'
    note_suffix = (
        f' ({receipt.document_number})'
        if str(receipt.document_number or '').strip()
        else ''
    )

    _item_location_cache: dict[int, object] = {}
    received_at = now_moscow()

    for item in receipt.items or []:
        autopart_id = await resolve_receipt_item_autopart_id(session, item)
        if autopart_id is None:
            continue
        quantity = max(int(item.received_quantity or 0), 0)
        if quantity <= 0:
            continue

        item_warehouse_id = getattr(item, 'warehouse_id', None)
        if item_warehouse_id and item_warehouse_id != doc_warehouse.id:
            if item_warehouse_id not in _item_location_cache:
                item_wh = await get_warehouse_by_id(
                    session, item_warehouse_id
                )
                if item_wh is not None:
                    _item_location_cache[item_warehouse_id] = (
                        await ensure_receiving_location(session, item_wh)
                    )
                else:
                    _item_location_cache[
                        item_warehouse_id
                    ] = doc_receiving_location
            receiving_location = _item_location_cache[item_warehouse_id]
        else:
            receiving_location = doc_receiving_location

        lot_id: Optional[int] = None
        if not reverse:
            lot = await _create_stock_lot(
                session,
                autopart_id=autopart_id,
                storage_location_id=receiving_location.id,
                quantity=quantity,
                source_type=LotSourceType.RECEIPT,
                gtd_number=getattr(item, 'gtd_code', None),
                country_code=getattr(item, 'country_code', None),
                country_name=getattr(item, 'country_name', None),
                source_receipt_id=receipt.id,
                source_receipt_item_id=item.id,
                received_at=received_at,
            )
            lot_id = lot.id
        else:
            # Reverse: use the lot's actual
            # remaining qty (some may be consumed)
            # so we never try to make SBL go below zero.
            lot_stmt = select(StockLot).where(
                StockLot.source_receipt_item_id == item.id
            )
            lot = (await session.execute(lot_stmt)).scalar_one_or_none()
            if lot is not None:
                quantity = lot.remaining_quantity
                # only reverse what's still there
                if lot.remaining_quantity == lot.initial_quantity:
                    await session.delete(lot)
                else:
                    lot.remaining_quantity = 0
                await session.flush()
            else:
                quantity = 0  # lot was already fully consumed / deleted

        if quantity <= 0:
            continue

        await _apply_stock_delta(
            session,
            autopart_id=autopart_id,
            storage_location_id=receiving_location.id,
            quantity_delta=quantity * multiplier,
            movement_type=MovementType.RECEIPT,
            reference_id=receipt.id,
            reference_type='supplier_receipt',
            notes=f'{note_prefix} #{receipt.id}{note_suffix}',
            stock_lot_id=lot_id,
        )


# Keep old name as alias
apply_receipt_to_stock = receive_stock


async def apply_receipt_to_stock_by_id(
    session: AsyncSession,
    *,
    receipt_id: int,
    reverse: bool = False,
) -> None:
    stmt = (
        select(SupplierReceipt)
        .options(selectinload(SupplierReceipt.items))
        .where(SupplierReceipt.id == receipt_id)
    )
    receipt = (await session.execute(stmt)).scalar_one_or_none()
    if receipt is None:
        raise LookupError('Документ поступления не найден')
    await receive_stock(session, receipt=receipt, reverse=reverse)


# ═══════════════════════════════════════════════════════════════════════════════
# Use-case: writeoff_stock_fifo
# ═══════════════════════════════════════════════════════════════════════════════

async def writeoff_stock_fifo(
    session: AsyncSession,
    *,
    autopart_id: int,
    storage_location_id: Optional[int],
    quantity: int,
    reason: Optional[str] = None,
    reference_id: Optional[int] = None,
    reference_type: Optional[str] = None,
    operation_uid: Optional[str] = None,
) -> list[StockMovement]:
    """Write off `quantity` units by FIFO with an optional reason.

    Returns the list of created StockMovement records.
    """
    notes = reason or 'Ручное списание'
    movements = await _consume_fifo(
        session,
        autopart_id=autopart_id,
        storage_location_id=storage_location_id,
        quantity=quantity,
        movement_type=MovementType.WRITEOFF,
        reference_id=reference_id,
        reference_type=reference_type,
        notes=notes,
    )
    # Attach operation_uid to the first movement (idempotency token)
    if operation_uid and movements:
        movements[0].operation_uid = operation_uid
    return movements


# ═══════════════════════════════════════════════════════════════════════════════
# Use-case: reconcile_stock_absolute  (inventory correction)
# ═══════════════════════════════════════════════════════════════════════════════

async def reconcile_stock_absolute(
    session: AsyncSession,
    *,
    autopart_id: int,
    storage_location_id: int,
    target_quantity: int,
    inventory_session_id: Optional[int] = None,
    notes: Optional[str] = None,
) -> Optional[StockMovement]:
    """Set stock to `target_quantity` for a given autopart + location.

    Used when completing an InventorySession to apply counted quantities.
    - If target > current: creates an INVENTORY movement (positive delta)
      and a new StockLot(source_type=INVENTORY_CORRECTION).
    - If target < current: FIFO write-down (negative INVENTORY movement).
    - If target == current: no-op, returns None.

    Returns the created StockMovement (or None if no change).
    """
    sbl_stmt = select(StockByLocation).where(
        StockByLocation.autopart_id == autopart_id,
        StockByLocation.storage_location_id == storage_location_id,
    )
    sbl = (await session.execute(sbl_stmt)).scalar_one_or_none()
    current = int(sbl.quantity) if sbl else 0
    delta = target_quantity - current

    if delta == 0:
        return None

    ref_note = notes or (
        f'Коррекция инвентаризации #{inventory_session_id}'
        if inventory_session_id
        else 'Коррекция инвентаризации'
    )

    if delta > 0:
        # Излишек — создаём лот
        lot = await _create_stock_lot(
            session,
            autopart_id=autopart_id,
            storage_location_id=storage_location_id,
            quantity=delta,
            source_type=LotSourceType.INVENTORY_CORRECTION,
        )
        mv = await _apply_stock_delta(
            session,
            autopart_id=autopart_id,
            storage_location_id=storage_location_id,
            quantity_delta=delta,
            movement_type=MovementType.INVENTORY,
            reference_id=inventory_session_id,
            reference_type='inventory',
            notes=ref_note,
            stock_lot_id=lot.id,
        )
    else:
        # Недостача — FIFO списание
        mvs = await _consume_fifo(
            session,
            autopart_id=autopart_id,
            storage_location_id=storage_location_id,
            quantity=abs(delta),
            movement_type=MovementType.INVENTORY,
            reference_id=inventory_session_id,
            reference_type='inventory',
            notes=ref_note,
        )
        mv = mvs[0] if mvs else None

    return mv


# ═══════════════════════════════════════════════════════════════════════════════
# Use-case: transfer_stock_with_lot_trace
# ═══════════════════════════════════════════════════════════════════════════════

async def transfer_stock_with_lot_trace(
    session: AsyncSession,
    *,
    autopart_id: int,
    from_location_id: int,
    to_location_id: int,
    quantity: int,
    notes: Optional[str] = None,
) -> dict:
    """Move `quantity` units between locations, preserving GTD / received_at.

    Lots from the source location are moved (FIFO) to the destination,
    creating new destination lots with the same gtd_number and received_at
    so that the FIFO order is preserved globally.

    Returns dict with keys:
      autopart_id, from_location_id, to_location_id, quantity,
      lots_transferred (list of {lot_id, gtd_number, quantity}),
      movement_out_id, movement_in_id
    """
    quantity = int(quantity)
    if quantity <= 0:
        raise ValueError('Количество должно быть > 0')

    note = notes or (
        f'Перемещение: loc#{from_location_id} → loc#{to_location_id}'
    )

    # 1. Load source lots (FIFO)
    lots_stmt = (
        select(StockLot)
        .where(
            StockLot.autopart_id == autopart_id,
            StockLot.storage_location_id == from_location_id,
            StockLot.remaining_quantity > 0,
        )
        .order_by(asc(StockLot.received_at), asc(StockLot.id))
    )
    source_lots = (await session.execute(lots_stmt)).scalars().all()

    remaining = quantity
    transferred_lots: list[dict] = []

    for lot in source_lots:
        if remaining <= 0:
            break
        take = min(lot.remaining_quantity, remaining)
        remaining -= take
        lot.remaining_quantity -= take

        # Find or create the matching lot at destination
        dest_lot_stmt = select(StockLot).where(
            StockLot.autopart_id == autopart_id,
            StockLot.storage_location_id == to_location_id,
            StockLot.source_type == lot.source_type,
            StockLot.gtd_number == lot.gtd_number,
            StockLot.source_receipt_id == lot.source_receipt_id,
            StockLot.source_document_item_id == lot.source_document_item_id,
        )
        dest_lot = (await session.execute(dest_lot_stmt)).scalar_one_or_none()

        if dest_lot is not None:
            dest_lot.remaining_quantity += take
            dest_lot.initial_quantity += take
        else:
            dest_lot = await _create_stock_lot(
                session,
                autopart_id=autopart_id,
                storage_location_id=to_location_id,
                quantity=take,
                source_type=LotSourceType.TRANSFER,
                gtd_number=lot.gtd_number,
                country_code=lot.country_code,
                country_name=lot.country_name,
                source_receipt_id=lot.source_receipt_id,
                source_receipt_item_id=lot.source_receipt_item_id,
                received_at=lot.received_at,   # preserve original date!
            )

        transferred_lots.append({
            'lot_id': lot.id,
            'gtd_number': lot.gtd_number,
            'quantity': take,
        })

    out_movement: Optional[StockMovement] = None
    in_movement: Optional[StockMovement] = None

    # 2. Unlotted stock (pre-dates lot tracking)
    if remaining > 0:
        out_movement = await _apply_stock_delta(
            session,
            autopart_id=autopart_id,
            storage_location_id=from_location_id,
            quantity_delta=-remaining,
            movement_type=MovementType.TRANSFER_OUT,
            reference_type='transfer',
            notes=note,
        )
        in_movement = await _apply_stock_delta(
            session,
            autopart_id=autopart_id,
            storage_location_id=to_location_id,
            quantity_delta=remaining,
            movement_type=MovementType.TRANSFER_IN,
            reference_type='transfer',
            notes=note,
        )

    # 3. StockByLocation update for lot-tracked portion
    lot_qty = quantity - remaining
    if lot_qty > 0:
        lot_out = await _apply_stock_delta(
            session,
            autopart_id=autopart_id,
            storage_location_id=from_location_id,
            quantity_delta=-lot_qty,
            movement_type=MovementType.TRANSFER_OUT,
            reference_type='transfer',
            notes=note,
        )
        lot_in = await _apply_stock_delta(
            session,
            autopart_id=autopart_id,
            storage_location_id=to_location_id,
            quantity_delta=lot_qty,
            movement_type=MovementType.TRANSFER_IN,
            reference_type='transfer',
            notes=note,
        )
        if lot_out:
            out_movement = lot_out
        if lot_in:
            in_movement = lot_in

    await session.flush()

    return {
        'autopart_id': autopart_id,
        'from_location_id': from_location_id,
        'to_location_id': to_location_id,
        'quantity': quantity,
        'lots_transferred': transferred_lots,
        'movement_out_id': out_movement.id if out_movement else None,
        'movement_in_id': in_movement.id if in_movement else None,
    }


# Keep old name as alias
transfer_with_lots = transfer_stock_with_lot_trace


# ═══════════════════════════════════════════════════════════════════════════════
# Use-case: post_stock_document / unpost_stock_document
# ═══════════════════════════════════════════════════════════════════════════════

async def post_stock_document(
    session: AsyncSession,
    *,
    document_id: int,
) -> dict:
    """Post a DRAFT StockDocument — update stock and create lots/movements.

    - MANUAL_RECEIPT:
    creates StockLot(MANUAL) + StockMovement(MANUAL) per line.
    - MANUAL_WRITEOFF: FIFO write-off with StockMovement(WRITEOFF) per lot.

    Returns summary dict.
    """
    stmt = (
        select(StockDocument)
        .options(selectinload(StockDocument.items))
        .where(StockDocument.id == document_id)
    )
    doc = (await session.execute(stmt)).scalar_one_or_none()
    if doc is None:
        raise LookupError('Документ не найден')
    if doc.status != StockDocumentStatus.DRAFT:
        raise ValueError(
            f'Документ не в статусе DRAFT (текущий: {doc.status})'
        )

    processed = 0
    movements_created = 0

    for item in doc.items or []:
        qty = int(item.quantity or 0)
        if qty <= 0:
            continue

        # Resolve storage_location: item → document warehouse RECEIVING
        if item.storage_location_id is None and doc.warehouse_id is not None:
            wh = await get_warehouse_by_id(session, doc.warehouse_id)
            if wh:
                loc = await ensure_receiving_location(session, wh)
                item.storage_location_id = loc.id

        if item.storage_location_id is None:
            logger.warning(
                'StockDocument item id=%s: no storage_location — skipping',
                item.id,
            )
            continue

        if doc.doc_type == StockDocumentType.MANUAL_RECEIPT:
            lot = await _create_stock_lot(
                session,
                autopart_id=item.autopart_id,
                storage_location_id=item.storage_location_id,
                quantity=qty,
                source_type=LotSourceType.MANUAL,
                gtd_number=item.gtd_number,
                country_code=item.country_code,
                country_name=item.country_name,
                source_document_item_id=item.id,
            )
            item.lot_id = lot.id

            mv = await _apply_stock_delta(
                session,
                autopart_id=item.autopart_id,
                storage_location_id=item.storage_location_id,
                quantity_delta=qty,
                movement_type=MovementType.MANUAL,
                reference_id=doc.id,
                reference_type='stock_document',
                notes=doc.reason or f'Ручное оприходование #{doc.id}',
                stock_lot_id=lot.id,
            )
            if mv:
                movements_created += 1

        elif doc.doc_type == StockDocumentType.MANUAL_WRITEOFF:
            mvs = await _consume_fifo(
                session,
                autopart_id=item.autopart_id,
                storage_location_id=item.storage_location_id,
                quantity=qty,
                movement_type=MovementType.WRITEOFF,
                reference_id=doc.id,
                reference_type='stock_document',
                notes=doc.reason or f'Ручное списание #{doc.id}',
            )
            movements_created += len(mvs)

        processed += 1

    doc.status = StockDocumentStatus.POSTED
    doc.posted_at = now_moscow()
    await session.flush()

    return {
        'document_id': document_id,
        'doc_type': doc.doc_type,
        'items_processed': processed,
        'movements_created': movements_created,
    }


async def unpost_stock_document(
    session: AsyncSession,
    *,
    document_id: int,
) -> dict:
    """Reverse a POSTED StockDocument.

    - MANUAL_RECEIPT: deletes/zeros lots, creates negative MANUAL movements.
    - MANUAL_WRITEOFF: NOT reversible automatically (FIFO lots may be partially
      consumed again). Raises ValueError — user must handle manually.
    """
    stmt = (
        select(StockDocument)
        .options(selectinload(StockDocument.items))
        .where(StockDocument.id == document_id)
    )
    doc = (await session.execute(stmt)).scalar_one_or_none()
    if doc is None:
        raise LookupError('Документ не найден')
    if doc.status != StockDocumentStatus.POSTED:
        raise ValueError(
            f'Документ не проведён (текущий статус: {doc.status})'
        )

    if doc.doc_type == StockDocumentType.MANUAL_WRITEOFF:
        raise ValueError(
            'Распроведение списания не поддерживается автоматически. '
            'Создайте документ оприходования для корректировки.'
        )

    processed = 0
    for item in doc.items or []:
        qty = int(item.quantity or 0)
        if qty <= 0 or item.storage_location_id is None:
            continue

        # Reverse the lot
        if item.lot_id is not None:
            lot = await session.get(StockLot, item.lot_id)
            if lot is not None:
                if lot.remaining_quantity == lot.initial_quantity:
                    await session.delete(lot)
                else:
                    lot.remaining_quantity = 0
            item.lot_id = None

        await _apply_stock_delta(
            session,
            autopart_id=item.autopart_id,
            storage_location_id=item.storage_location_id,
            quantity_delta=-qty,
            movement_type=MovementType.MANUAL,
            reference_id=doc.id,
            reference_type='stock_document',
            notes=f'Распроведение ручного оприходования #{doc.id}',
        )
        processed += 1

    doc.status = StockDocumentStatus.CANCELLED
    await session.flush()

    return {
        'document_id': document_id,
        'items_reversed': processed,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# Use-case: dispatch_stock_order
# ═══════════════════════════════════════════════════════════════════════════════

async def dispatch_stock_order(
    session: AsyncSession,
    *,
    stock_order_id: int,
) -> dict:
    """Dispatch a stock order: FIFO shipment per line,
    set status DISPATCHED."""
    from dz_fastapi.models.partner import STOCK_ORDER_STATUS, StockOrder

    stmt = (
        select(StockOrder)
        .options(selectinload(StockOrder.items))
        .where(StockOrder.id == stock_order_id)
    )
    order = (await session.execute(stmt)).scalar_one_or_none()
    if order is None:
        raise LookupError('Складской заказ не найден')
    if order.status == STOCK_ORDER_STATUS.DISPATCHED:
        raise ValueError('Заказ уже отгружен')

    total_movements = 0
    processed_items = 0

    for item in order.items or []:
        qty = int(item.picked_quantity or 0)
        if qty <= 0:
            qty = int(item.quantity or 0)
        if qty <= 0 or item.autopart_id is None:
            continue

        movements = await _consume_fifo(
            session,
            autopart_id=item.autopart_id,
            storage_location_id=None,   # global FIFO
            quantity=qty,
            movement_type=MovementType.SHIPMENT,
            reference_id=stock_order_id,
            reference_type='stock_order',
            notes=f'Отгрузка заказа #{stock_order_id}',
        )
        total_movements += len(movements)
        processed_items += 1

    order.status = STOCK_ORDER_STATUS.DISPATCHED
    await session.flush()

    return {
        'stock_order_id': stock_order_id,
        'processed_items': processed_items,
        'movements_created': total_movements,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# Use-case: backfill_opening_balance_lots
# ═══════════════════════════════════════════════════════════════════════════════

async def backfill_opening_balance_lots(
    session: AsyncSession,
) -> dict:
    """One-time backfill: create opening_balance StockLots for every
    StockByLocation row that has no active lot yet.

    This ensures the lot-tracking invariant holds for stock that existed
    before the lot feature was introduced.

    Returns: {
    'lots_created': N, 'locations_processed': N, 'autoparts_skipped': N
    }
    """
    # All stock rows
    sbl_stmt = select(StockByLocation).where(StockByLocation.quantity > 0)
    all_sbl = (await session.execute(sbl_stmt)).scalars().all()

    lots_created = 0
    locations_processed = 0
    autoparts_skipped = 0

    for sbl in all_sbl:
        # Check if there are already any active lots for this (part, location)
        existing_stmt = select(func.sum(StockLot.remaining_quantity)).where(
            StockLot.autopart_id == sbl.autopart_id,
            StockLot.storage_location_id == sbl.storage_location_id,
            StockLot.remaining_quantity > 0,
        )
        existing_qty = (
            await session.execute(existing_stmt)
        ).scalar_one_or_none() or 0

        locations_processed += 1

        if int(existing_qty) >= int(sbl.quantity):
            # Already covered — skip
            autoparts_skipped += 1
            continue

        gap = int(sbl.quantity) - int(existing_qty)
        if gap <= 0:
            autoparts_skipped += 1
            continue

        await _create_stock_lot(
            session,
            autopart_id=sbl.autopart_id,
            storage_location_id=sbl.storage_location_id,
            quantity=gap,
            source_type=LotSourceType.OPENING_BALANCE,
            # No GTD for opening balance — unknown provenance
        )
        lots_created += 1

        logger.info(
            'backfill: created opening_balance lot '
            'autopart_id=%s location_id=%s qty=%s',
            sbl.autopart_id,
            sbl.storage_location_id,
            gap,
        )

    await session.flush()
    return {
        'lots_created': lots_created,
        'locations_processed': locations_processed,
        'autoparts_skipped': autoparts_skipped,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# Query helpers
# ═══════════════════════════════════════════════════════════════════════════════

async def get_lots_for_autopart(
    session: AsyncSession,
    *,
    autopart_id: int,
    storage_location_id: Optional[int] = None,
    only_active: bool = False,
) -> list[StockLot]:
    """Return all lots for an autopart, optionally filtered by location."""
    stmt = select(StockLot).where(StockLot.autopart_id == autopart_id)
    if storage_location_id is not None:
        stmt = stmt.where(StockLot.storage_location_id == storage_location_id)
    if only_active:
        stmt = stmt.where(StockLot.remaining_quantity > 0)
    stmt = stmt.order_by(asc(StockLot.received_at), asc(StockLot.id))
    return (await session.execute(stmt)).scalars().all()
