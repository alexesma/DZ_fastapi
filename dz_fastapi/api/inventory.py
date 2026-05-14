"""
Inventory API
─────────────
Endpoints for managing StockByLocation, InventorySession, InventoryItem,
StockMovement, StockReserve, ShipmentDocument, and transfers.

1С integration endpoints:
  GET  /inventory/movements/export/          — выгрузка непросинхронизированных
  POST /inventory/movements/bulk-sync/       — пакетное подтверждение из 1С
  PATCH /inventory/movements/{id}/sync/      — обновление статуса одного движения
"""

import logging
from datetime import datetime
from typing import List, Optional

from fastapi import APIRouter, Body, Depends, HTTPException, Query, status
from sqlalchemy import delete, func, insert, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.orm import selectinload

from dz_fastapi.core.db import get_session
from dz_fastapi.core.time import now_moscow
from dz_fastapi.models.autopart import AutoPart, StorageLocation, autopart_storage_association
from dz_fastapi.models.inventory import (
    InventoryItem,
    InventorySession,
    InventoryStatus,
    LotSourceType,
    MovementType,
    ReserveStatus,
    ReturnDocumentStatus,
    ReturnFromCustomer,
    ReturnItem,
    ReturnToSupplier,
    ShipmentDocument,
    ShipmentDocumentItem,
    ShipmentDocumentStatus,
    StockByLocation,
    StockDocument,
    StockDocumentItem,
    StockDocumentStatus,
    StockDocumentType,
    StockLot,
    StockMovement,
    StockReserve,
    SyncStatus,
)
from dz_fastapi.models.partner import SupplierReceipt, SupplierReceiptItem
from dz_fastapi.schemas.inventory import (
    BackfillResult,
    DocumentBulkSyncRequest,
    DocumentBulkSyncResult,
    DocumentsExportOut,
    DocumentSyncUpdate,
    InventoryItemCountUpdate,
    InventoryItemOut,
    InventorySessionCreate,
    InventorySessionListItem,
    InventorySessionOut,
    InventorySessionUpdate,
    MovementBulkSyncRequest,
    MovementBulkSyncResult,
    MovementsExportOut,
    MovementSyncUpdate,
    ReturnFromCustomerCreate,
    ReturnFromCustomerListItem,
    ReturnFromCustomerOut,
    ReturnFromCustomerUpdate,
    ReturnItemCreate,
    ReturnItemOut,
    ReturnItemUpdate,
    ReturnToSupplierCreate,
    ReturnToSupplierListItem,
    ReturnToSupplierOut,
    ReturnToSupplierUpdate,
    ShipmentBulkSyncRequest,
    ShipmentBulkSyncResult,
    ShipmentDocumentCreate,
    ShipmentDocumentItemCreate,
    ShipmentDocumentItemOut,
    ShipmentDocumentItemUpdate,
    ShipmentDocumentListItem,
    ShipmentDocumentOut,
    ShipmentDocumentUpdate,
    ShipmentPostResult,
    ShipmentsExportOut,
    ShipmentSyncUpdate,
    StockByLocationOut,
    StockByLocationUpsert,
    StockDocumentCreate,
    StockDocumentItemCreate,
    StockDocumentItemOut,
    StockDocumentItemUpdate,
    StockDocumentListItem,
    StockDocumentOut,
    StockDocumentUpdate,
    StockLotOut,
    StockMovementCreate,
    StockMovementOut,
    StockReserveCancelRequest,
    StockReserveCancelResult,
    StockReserveCreate,
    StockReserveOut,
    TransferRequest,
    TransferResult,
)
from dz_fastapi.services.inventory_stock import _apply_stock_delta as apply_stock_delta
from dz_fastapi.services.inventory_stock import _consume_fifo as consume_stock_fifo
from dz_fastapi.services.inventory_stock import (
    approve_return_from_customer,
    approve_return_to_supplier,
    backfill_opening_balance_lots,
    cancel_reserve,
    confirm_return_from_customer,
    confirm_return_to_supplier,
    create_reserve,
    get_available_quantity,
    get_lots_for_autopart,
    get_reserved_quantity,
    post_shipment_document,
    post_stock_document,
    reconcile_stock_absolute,
    reject_return_from_customer,
    reject_return_to_supplier,
    ship_return_from_customer,
    ship_return_to_supplier,
    transfer_stock_with_lot_trace,
    unpost_shipment_document,
    unpost_stock_document,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/inventory", tags=["inventory"])


# ─── helpers ───────────────────────────────────────────────────────────────


def _sbl_to_out(
    sbl: StockByLocation,
    reserved: int = 0,
) -> StockByLocationOut:
    ap = sbl.autopart
    loc = sbl.storage_location
    available = max(0, sbl.quantity - reserved)
    return StockByLocationOut(
        id=sbl.id,
        autopart_id=sbl.autopart_id,
        storage_location_id=sbl.storage_location_id,
        quantity=sbl.quantity,
        reserved=reserved,
        available=available,
        updated_at=sbl.updated_at,
        autopart_oem=ap.oem_number if ap else None,
        autopart_name=ap.name if ap else None,
        autopart_brand=ap.brand.name if (ap and ap.brand) else None,
        storage_location_name=loc.name if loc else None,
    )


def _item_to_out(item: InventoryItem) -> InventoryItemOut:
    ap = item.autopart
    loc = item.storage_location
    return InventoryItemOut(
        id=item.id,
        session_id=item.session_id,
        autopart_id=item.autopart_id,
        storage_location_id=item.storage_location_id,
        expected_qty=item.expected_qty,
        actual_qty=item.actual_qty,
        discrepancy=item.discrepancy,
        counted_at=item.counted_at,
        autopart_oem=ap.oem_number if ap else None,
        autopart_name=ap.name if ap else None,
        autopart_brand=ap.brand.name if (ap and ap.brand) else None,
        storage_location_name=loc.name if loc else None,
    )


async def _ensure_sbl(
    session: AsyncSession,
    autopart_id: int,
    storage_location_id: int,
    quantity: int,
) -> StockByLocation:
    """Upsert a StockByLocation record and
    keep autopart_storage_association in sync."""
    result = await session.execute(
        select(StockByLocation).where(
            StockByLocation.autopart_id == autopart_id,
            StockByLocation.storage_location_id == storage_location_id,
        )
    )
    sbl = result.scalar_one_or_none()

    if sbl is None:
        sbl = StockByLocation(
            autopart_id=autopart_id,
            storage_location_id=storage_location_id,
            quantity=quantity,
        )
        session.add(sbl)
        # also create M2M link so catalog shows the location
        assoc_exists = (
            await session.execute(
                select(autopart_storage_association).where(
                    autopart_storage_association.c.autopart_id == autopart_id,
                    autopart_storage_association.c.storage_location_id
                    == storage_location_id,
                )
            )
        ).first()
        if not assoc_exists:
            await session.execute(
                insert(autopart_storage_association).values(
                    autopart_id=autopart_id,
                    storage_location_id=storage_location_id,
                )
            )
    else:
        sbl.quantity = quantity
        sbl.updated_at = now_moscow()

    return sbl


def _movement_to_out(m: StockMovement) -> StockMovementOut:
    lot = getattr(m, "stock_lot", None)
    ap = m.autopart
    return StockMovementOut(
        id=m.id,
        autopart_id=m.autopart_id,
        storage_location_id=m.storage_location_id,
        movement_type=m.movement_type,
        quantity=m.quantity,
        qty_before=m.qty_before,
        qty_after=m.qty_after,
        reference_id=m.reference_id,
        reference_type=m.reference_type,
        notes=m.notes,
        created_at=m.created_at,
        stock_lot_id=m.stock_lot_id,
        # Денормализованные из лота (lot lazy='joined' — уже загружен)
        gtd_number=lot.gtd_number if lot else None,
        lot_source_type=lot.source_type if lot else None,
        # Синхронизация с 1С
        external_id=m.external_id,
        operation_uid=m.operation_uid,
        sync_status=m.sync_status,
        synced_at=m.synced_at,
        # Денормализованные из автозапчасти
        autopart_oem=ap.oem_number if ap else None,
        autopart_name=ap.name if ap else None,
        autopart_brand=ap.brand.name if (ap and ap.brand) else None,
        storage_location_name=(
            m.storage_location.name if m.storage_location else None
        ),
    )


def _movements_query(
    autopart_id: Optional[int] = None,
    storage_location_id: Optional[int] = None,
    movement_type: Optional[MovementType] = None,
    sync_status: Optional[SyncStatus] = None,
    date_from: Optional[datetime] = None,
    date_to: Optional[datetime] = None,
    reference_id: Optional[int] = None,
    reference_type: Optional[str] = None,
):
    """Строит базовый SELECT с eager-loading и всеми опциональными фильтрами."""
    stmt = (
        select(StockMovement)
        .options(
            selectinload(StockMovement.autopart).selectinload(AutoPart.brand),
            selectinload(StockMovement.storage_location),
        )
        .order_by(StockMovement.created_at.desc())
    )
    if autopart_id is not None:
        stmt = stmt.where(StockMovement.autopart_id == autopart_id)
    if storage_location_id is not None:
        stmt = stmt.where(
            StockMovement.storage_location_id == storage_location_id
        )
    if movement_type is not None:
        stmt = stmt.where(StockMovement.movement_type == movement_type)
    if sync_status is not None:
        stmt = stmt.where(StockMovement.sync_status == sync_status)
    if date_from is not None:
        stmt = stmt.where(StockMovement.created_at >= date_from)
    if date_to is not None:
        stmt = stmt.where(StockMovement.created_at <= date_to)
    if reference_id is not None:
        stmt = stmt.where(StockMovement.reference_id == reference_id)
    if reference_type is not None:
        stmt = stmt.where(StockMovement.reference_type == reference_type)
    return stmt


# ─── StockByLocation endpoints ─────────────────────────────────────────────


@router.get(
    "/stock/",
    response_model=List[StockByLocationOut],
    summary="Остатки по ячейкам",
)
async def list_stock_by_location(
    storage_location_id: Optional[int] = Query(None),
    autopart_id: Optional[int] = Query(None),
    session: AsyncSession = Depends(get_session),
):
    """Return StockByLocation records with optional filters."""
    stmt = select(StockByLocation).options(
        selectinload(StockByLocation.autopart).selectinload(AutoPart.brand),
        selectinload(StockByLocation.storage_location),
    )
    if storage_location_id:
        stmt = stmt.where(
            StockByLocation.storage_location_id == storage_location_id
        )
    if autopart_id:
        stmt = stmt.where(StockByLocation.autopart_id == autopart_id)
    stmt = stmt.order_by(
        StockByLocation.storage_location_id, StockByLocation.autopart_id
    )
    rows = (await session.execute(stmt)).scalars().all()

    # Batch-fetch reserved quantities for all (autopart, location) pairs
    if rows:
        key_pairs = [(r.autopart_id, r.storage_location_id) for r in rows]
        reserve_stmt = (
            select(
                StockReserve.autopart_id,
                StockReserve.storage_location_id,
                func.sum(StockReserve.quantity).label("total_reserved"),
            )
            .where(
                StockReserve.status == ReserveStatus.ACTIVE,
                StockReserve.autopart_id.in_([p[0] for p in key_pairs]),
            )
            .group_by(
                StockReserve.autopart_id,
                StockReserve.storage_location_id,
            )
        )
        reserve_rows = (await session.execute(reserve_stmt)).all()
        reserved_map = {
            (rr.autopart_id, rr.storage_location_id): int(rr.total_reserved)
            for rr in reserve_rows
        }
    else:
        reserved_map = {}

    return [
        _sbl_to_out(
            r,
            reserved=reserved_map.get(
                (r.autopart_id, r.storage_location_id), 0
            ),
        )
        for r in rows
    ]


@router.put(
    "/stock/",
    response_model=StockByLocationOut,
    status_code=status.HTTP_200_OK,
    summary="Установить остаток запчасти в ячейке (upsert)",
)
async def upsert_stock_by_location(
    data: StockByLocationUpsert,
    session: AsyncSession = Depends(get_session),
):
    """
    Create or update the quantity for a specific (autopart, location) pair.
    Also ensures the autopart_storage_association link exists.
    If quantity is set to 0 the record stays (preserves history);
    delete it explicitly if you want to remove it entirely.
    """
    # Validate references
    if not (await session.get(AutoPart, data.autopart_id)):
        raise HTTPException(status_code=404, detail="Запчасть не найдена")
    if not (await session.get(StorageLocation, data.storage_location_id)):
        raise HTTPException(
            status_code=404, detail="Место хранения не найдено"
        )

    await reconcile_stock_absolute(
        session,
        autopart_id=data.autopart_id,
        storage_location_id=data.storage_location_id,
        target_quantity=data.quantity,
        notes="Ручная установка остатка через PUT /inventory/stock",
    )
    if data.quantity == 0:
        await _ensure_sbl(
            session,
            data.autopart_id,
            data.storage_location_id,
            0,
        )
    await session.commit()

    # Reload with relationships
    result = await session.execute(
        select(StockByLocation)
        .where(
            StockByLocation.autopart_id == data.autopart_id,
            StockByLocation.storage_location_id == data.storage_location_id,
        )
        .options(
            selectinload(StockByLocation.autopart).selectinload(
                AutoPart.brand
            ),
            selectinload(StockByLocation.storage_location),
        )
    )
    return _sbl_to_out(result.scalar_one())


@router.delete(
    "/stock/{sbl_id}/",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Удалить запись остатка (убрать запчасть из ячейки)",
)
async def delete_stock_by_location(
    sbl_id: int,
    remove_location_link: bool = Query(
        True,
        description="Также убрать запись из autopart_storage_association",
    ),
    session: AsyncSession = Depends(get_session),
):
    sbl = await session.get(StockByLocation, sbl_id)
    if not sbl:
        raise HTTPException(status_code=404, detail="Запись не найдена")

    ap_id = sbl.autopart_id
    loc_id = sbl.storage_location_id
    await session.delete(sbl)

    if remove_location_link:
        await session.execute(
            delete(autopart_storage_association).where(
                autopart_storage_association.c.autopart_id == ap_id,
                autopart_storage_association.c.storage_location_id == loc_id,
            )
        )

    await session.commit()


# ─── InventorySession CRUD ──────────────────────────────────────────────────


@router.post(
    "/sessions/",
    status_code=status.HTTP_201_CREATED,
    response_model=InventorySessionOut,
    summary="Начать новый сеанс инвентаризации",
)
async def start_inventory_session(
    data: InventorySessionCreate,
    session: AsyncSession = Depends(get_session),
):
    """
    Creates a new InventorySession. Populates InventoryItems
    from StockByLocation
    (expected_qty = current per-location quantity). Falls back to 0 for items
    that have a location link but no StockByLocation record yet.
    """
    inv_session = InventorySession(
        name=data.name,
        scope_type=data.scope_type,
        scope_value=data.scope_value,
        notes=data.notes,
    )
    session.add(inv_session)
    await session.flush()

    # ── Filter locations by scope ───────────────────────────────────────────
    loc_stmt = select(StorageLocation)
    if data.scope_type.value == "shelf" and data.scope_value:
        prefix = data.scope_value.upper()
        loc_stmt = loc_stmt.where(StorageLocation.name.like(f"{prefix}%"))
    elif data.scope_type.value == "location" and data.scope_value:
        loc_stmt = loc_stmt.where(
            StorageLocation.name == data.scope_value.upper()
        )
    loc_ids = [r.id for r in (await session.execute(loc_stmt)).scalars().all()]

    # ── Get StockByLocation rows for these locations (non-zero only) ────────
    if loc_ids:
        sbl_rows = (
            (
                await session.execute(
                    select(StockByLocation).where(
                        StockByLocation.storage_location_id.in_(loc_ids),
                        StockByLocation.quantity != 0,
                    )
                )
            )
            .scalars()
            .all()
        )
    else:
        sbl_rows = []

    # ── Build InventoryItems ────────────────────────────────────────────────
    seen: set[tuple[int, int]] = set()
    for sbl in sbl_rows:
        key = (sbl.autopart_id, sbl.storage_location_id)
        if key in seen:
            continue
        seen.add(key)
        session.add(
            InventoryItem(
                session_id=inv_session.id,
                autopart_id=sbl.autopart_id,
                storage_location_id=sbl.storage_location_id,
                expected_qty=sbl.quantity,
            )
        )

    await session.commit()

    # ── Reload with relationships ───────────────────────────────────────────
    result = await session.execute(
        select(InventorySession)
        .where(InventorySession.id == inv_session.id)
        .options(
            selectinload(InventorySession.items)
            .selectinload(InventoryItem.autopart)
            .selectinload(AutoPart.brand),
            selectinload(InventorySession.items).selectinload(
                InventoryItem.storage_location
            ),
        )
    )
    inv_session = result.scalar_one()
    out_items = [_item_to_out(it) for it in inv_session.items]
    return InventorySessionOut(
        id=inv_session.id,
        name=inv_session.name,
        started_at=inv_session.started_at,
        finished_at=inv_session.finished_at,
        status=inv_session.status,
        scope_type=inv_session.scope_type,
        scope_value=inv_session.scope_value,
        notes=inv_session.notes,
        items=out_items,
    )


@router.get(
    "/sessions/",
    response_model=List[InventorySessionListItem],
    summary="Список сеансов инвентаризации",
)
async def list_inventory_sessions(
    status_filter: Optional[InventoryStatus] = Query(None, alias="status"),
    offset: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=200),
    session: AsyncSession = Depends(get_session),
):
    stmt = select(InventorySession)
    if status_filter:
        stmt = stmt.where(InventorySession.status == status_filter)
    stmt = (
        stmt.order_by(InventorySession.started_at.desc())
        .offset(offset)
        .limit(limit)
    )
    sessions = (await session.execute(stmt)).scalars().all()

    counts: dict[int, tuple[int, int]] = {}
    if sessions:
        ids = [s.id for s in sessions]
        count_rows = (
            await session.execute(
                select(
                    InventoryItem.session_id,
                    func.count(InventoryItem.id).label("total"),
                    func.count(InventoryItem.actual_qty).label("counted"),
                )
                .where(InventoryItem.session_id.in_(ids))
                .group_by(InventoryItem.session_id)
            )
        ).all()
        counts = {r[0]: (r[1], r[2]) for r in count_rows}

    return [
        InventorySessionListItem(
            id=s.id,
            name=s.name,
            started_at=s.started_at,
            finished_at=s.finished_at,
            status=s.status,
            scope_type=s.scope_type,
            scope_value=s.scope_value,
            item_count=counts.get(s.id, (0, 0))[0],
            counted_count=counts.get(s.id, (0, 0))[1],
        )
        for s in sessions
    ]


@router.get(
    "/sessions/{session_id}/",
    response_model=InventorySessionOut,
    summary="Сеанс инвентаризации (с позициями)",
)
async def get_inventory_session(
    session_id: int,
    session: AsyncSession = Depends(get_session),
):
    result = await session.execute(
        select(InventorySession)
        .where(InventorySession.id == session_id)
        .options(
            selectinload(InventorySession.items)
            .selectinload(InventoryItem.autopart)
            .selectinload(AutoPart.brand),
            selectinload(InventorySession.items).selectinload(
                InventoryItem.storage_location
            ),
        )
    )
    inv_session = result.scalar_one_or_none()
    if not inv_session:
        raise HTTPException(status_code=404, detail="Сеанс не найден")

    return InventorySessionOut(
        id=inv_session.id,
        name=inv_session.name,
        started_at=inv_session.started_at,
        finished_at=inv_session.finished_at,
        status=inv_session.status,
        scope_type=inv_session.scope_type,
        scope_value=inv_session.scope_value,
        notes=inv_session.notes,
        items=[_item_to_out(it) for it in inv_session.items],
    )


@router.patch(
    "/sessions/{session_id}/",
    response_model=InventorySessionOut,
    summary="Обновить название/примечания сеанса",
)
async def update_inventory_session(
    session_id: int,
    data: InventorySessionUpdate,
    session: AsyncSession = Depends(get_session),
):
    result = await session.execute(
        select(InventorySession).where(InventorySession.id == session_id)
    )
    inv_session = result.scalar_one_or_none()
    if not inv_session:
        raise HTTPException(status_code=404, detail="Сеанс не найден")
    if inv_session.status != InventoryStatus.ACTIVE:
        raise HTTPException(
            status_code=400, detail="Можно редактировать только активный сеанс"
        )
    if data.name is not None:
        inv_session.name = data.name
    if data.notes is not None:
        inv_session.notes = data.notes
    await session.commit()
    return await get_inventory_session(session_id, session)


@router.patch(
    "/sessions/{session_id}/items/{item_id}/",
    response_model=InventoryItemOut,
    summary="Внести фактическое количество для позиции",
)
async def count_inventory_item(
    session_id: int,
    item_id: int,
    data: InventoryItemCountUpdate,
    session: AsyncSession = Depends(get_session),
):
    result = await session.execute(
        select(InventoryItem)
        .where(
            InventoryItem.id == item_id, InventoryItem.session_id == session_id
        )
        .options(
            selectinload(InventoryItem.autopart).selectinload(AutoPart.brand),
            selectinload(InventoryItem.storage_location),
        )
    )
    item = result.scalar_one_or_none()
    if not item:
        raise HTTPException(status_code=404, detail="Позиция не найдена")

    sess_status = (
        await session.execute(
            select(InventorySession.status).where(
                InventorySession.id == session_id
            )
        )
    ).scalar_one_or_none()
    if sess_status != InventoryStatus.ACTIVE:
        raise HTTPException(
            status_code=400, detail="Сеанс завершён или отменён"
        )

    item.actual_qty = data.actual_qty
    item.discrepancy = data.actual_qty - item.expected_qty
    item.counted_at = now_moscow()
    await session.commit()
    return _item_to_out(item)


@router.post(
    "/sessions/{session_id}/complete/",
    response_model=InventorySessionOut,
    summary="Завершить инвентаризацию и обновить остатки по ячейкам",
)
async def complete_inventory_session(
    session_id: int,
    apply_adjustments: bool = Body(
        True,
        embed=True,
        description="Обновить StockByLocation "
        "и создать StockMovement для расхождений",
    ),
    session: AsyncSession = Depends(get_session),
):
    """
    Marks session COMPLETED.
    If apply_adjustments=True:
      - Sets StockByLocation.quantity = actual_qty for each item.
      - Creates StockMovement(type=inventory) for every non-zero discrepancy.
    Uncounted items are treated as confirmed
    (actual = expected, discrepancy = 0).
    """
    result = await session.execute(
        select(InventorySession)
        .where(InventorySession.id == session_id)
        .options(
            selectinload(InventorySession.items)
            .selectinload(InventoryItem.autopart)
            .selectinload(AutoPart.brand),
            selectinload(InventorySession.items).selectinload(
                InventoryItem.storage_location
            ),
        )
    )
    inv_session = result.scalar_one_or_none()
    if not inv_session:
        raise HTTPException(status_code=404, detail="Сеанс не найден")
    if inv_session.status != InventoryStatus.ACTIVE:
        raise HTTPException(
            status_code=400, detail="Сеанс уже завершён или отменён"
        )

    for item in inv_session.items:
        if item.actual_qty is None:
            item.actual_qty = item.expected_qty
            item.discrepancy = 0
            item.counted_at = now_moscow()

        if apply_adjustments:
            await reconcile_stock_absolute(
                session,
                autopart_id=item.autopart_id,
                storage_location_id=item.storage_location_id,
                target_quantity=item.actual_qty,
                inventory_session_id=inv_session.id,
                notes=f"Инвентаризация «{inv_session.name}»",
            )

    inv_session.status = InventoryStatus.COMPLETED
    inv_session.finished_at = now_moscow()
    await session.commit()

    return InventorySessionOut(
        id=inv_session.id,
        name=inv_session.name,
        started_at=inv_session.started_at,
        finished_at=inv_session.finished_at,
        status=inv_session.status,
        scope_type=inv_session.scope_type,
        scope_value=inv_session.scope_value,
        notes=inv_session.notes,
        items=[_item_to_out(it) for it in inv_session.items],
    )


@router.post(
    "/sessions/{session_id}/cancel/",
    response_model=InventorySessionListItem,
    summary="Отменить сеанс инвентаризации",
)
async def cancel_inventory_session(
    session_id: int,
    session: AsyncSession = Depends(get_session),
):
    result = await session.execute(
        select(InventorySession).where(InventorySession.id == session_id)
    )
    inv_session = result.scalar_one_or_none()
    if not inv_session:
        raise HTTPException(status_code=404, detail="Сеанс не найден")
    if inv_session.status != InventoryStatus.ACTIVE:
        raise HTTPException(
            status_code=400, detail="Сеанс уже завершён или отменён"
        )
    inv_session.status = InventoryStatus.CANCELLED
    inv_session.finished_at = now_moscow()
    await session.commit()
    return InventorySessionListItem(
        id=inv_session.id,
        name=inv_session.name,
        started_at=inv_session.started_at,
        finished_at=inv_session.finished_at,
        status=inv_session.status,
        scope_type=inv_session.scope_type,
        scope_value=inv_session.scope_value,
        item_count=0,
        counted_count=0,
    )


# ─── StockMovement ──────────────────────────────────────────────────────────


@router.get(
    "/movements/",
    response_model=List[StockMovementOut],
    summary="История движений товара",
)
async def list_stock_movements(
    autopart_id: Optional[int] = Query(None),
    storage_location_id: Optional[int] = Query(None),
    movement_type: Optional[MovementType] = Query(None),
    sync_status: Optional[SyncStatus] = Query(
        None,
        description="Фильтр по статусу синхронизации с 1С",
    ),
    date_from: Optional[datetime] = Query(
        None, description="Движения с этой даты (включительно)"
    ),
    date_to: Optional[datetime] = Query(
        None, description="Движения по эту дату (включительно)"
    ),
    reference_id: Optional[int] = Query(None),
    reference_type: Optional[str] = Query(None),
    offset: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=500),
    session: AsyncSession = Depends(get_session),
):
    stmt = (
        _movements_query(
            autopart_id=autopart_id,
            storage_location_id=storage_location_id,
            movement_type=movement_type,
            sync_status=sync_status,
            date_from=date_from,
            date_to=date_to,
            reference_id=reference_id,
            reference_type=reference_type,
        )
        .offset(offset)
        .limit(limit)
    )

    movements = (await session.execute(stmt)).scalars().all()
    return [_movement_to_out(m) for m in movements]


@router.post(
    "/movements/",
    response_model=StockMovementOut,
    status_code=status.HTTP_201_CREATED,
    summary="Создать движение товара вручную",
)
async def create_stock_movement(
    data: StockMovementCreate,
    session: AsyncSession = Depends(get_session),
):
    if not (await session.get(AutoPart, data.autopart_id)):
        raise HTTPException(status_code=404, detail="Запчасть не найдена")
    if data.storage_location_id is None:
        raise HTTPException(
            status_code=400,
            detail="Для ручного движения требуется storage_location_id",
        )
    if not (await session.get(StorageLocation, data.storage_location_id)):
        raise HTTPException(
            status_code=404,
            detail="Место хранения не найдено",
        )

    if data.quantity == 0:
        raise HTTPException(
            status_code=400,
            detail="Количество движения не может быть 0",
        )

    try:
        created_movements: list[StockMovement]
        if data.quantity > 0:
            lot = StockLot(
                autopart_id=data.autopart_id,
                storage_location_id=data.storage_location_id,
                source_type=LotSourceType.MANUAL,
                initial_quantity=data.quantity,
                remaining_quantity=data.quantity,
                received_at=now_moscow(),
            )
            session.add(lot)
            await session.flush()
            mv = await apply_stock_delta(
                session,
                autopart_id=data.autopart_id,
                storage_location_id=data.storage_location_id,
                quantity_delta=data.quantity,
                movement_type=data.movement_type,
                reference_type="manual_movement",
                notes=data.notes,
                stock_lot_id=lot.id,
                operation_uid=data.operation_uid,
            )
            created_movements = [mv] if mv is not None else []
        else:
            created_movements = await consume_stock_fifo(
                session,
                autopart_id=data.autopart_id,
                storage_location_id=data.storage_location_id,
                quantity=abs(data.quantity),
                movement_type=data.movement_type,
                reference_type="manual_movement",
                notes=data.notes,
            )
            if data.operation_uid and created_movements:
                created_movements[0].operation_uid = data.operation_uid
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    if not created_movements:
        raise HTTPException(
            status_code=400,
            detail="Не удалось создать движение с указанными параметрами",
        )

    movement_ids = [m.id for m in created_movements]
    result = await session.execute(
        select(StockMovement)
        .where(StockMovement.id.in_(movement_ids))
        .options(
            selectinload(StockMovement.autopart).selectinload(AutoPart.brand),
            selectinload(StockMovement.storage_location),
        )
        .order_by(StockMovement.created_at.desc(), StockMovement.id.desc())
    )
    movement = result.scalars().first()
    await session.commit()
    return _movement_to_out(movement)


# ─── 1С integration: движения ───────────────────────────────────────────────
# ВАЖНО: статические пути (/export/, /bulk-sync/) объявлены РАНЬШЕ
# параметрических (/{movement_id}/), иначе FastAPI поймает "export" как int.


@router.get(
    "/movements/export/",
    response_model=MovementsExportOut,
    summary="Выгрузка движений для 1С (только непросинхронизированные)",
    description=(
        "Возвращает движения со статусом `pending` (или другим, если указан "
        "`sync_status`). Используется 1С для периодического получения новых "
        "складских движений. После обработки вызывайте `POST /movements/bulk-sync/`."
    ),
)
async def export_movements_for_1c(
    movement_type: Optional[MovementType] = Query(None),
    sync_status: Optional[SyncStatus] = Query(
        SyncStatus.PENDING,
        description="Статус синхронизации (по умолчанию pending)",
    ),
    date_from: Optional[datetime] = Query(None),
    date_to: Optional[datetime] = Query(None),
    offset: int = Query(0, ge=0),
    limit: int = Query(200, ge=1, le=1000),
    session: AsyncSession = Depends(get_session),
):
    # Считаем total без LIMIT/OFFSET
    count_stmt = (
        _movements_query(
            movement_type=movement_type,
            sync_status=sync_status,
            date_from=date_from,
            date_to=date_to,
        )
        .with_only_columns(func.count())
        .order_by(None)
    )
    total = (await session.execute(count_stmt)).scalar_one()

    rows = (
        (
            await session.execute(
                _movements_query(
                    movement_type=movement_type,
                    sync_status=sync_status,
                    date_from=date_from,
                    date_to=date_to,
                )
                .offset(offset)
                .limit(limit)
            )
        )
        .scalars()
        .all()
    )

    return MovementsExportOut(
        total=total,
        items=[_movement_to_out(m) for m in rows],
    )


@router.patch(
    "/movements/{movement_id}/sync/",
    response_model=StockMovementOut,
    summary="Обновить статус синхронизации движения (вызов из 1С)",
    description=(
        "Обновляет `sync_status`, `external_id` и `synced_at` для одного "
        "движения. При `sync_status=synced` автоматически проставляется "
        "`synced_at = now()`."
    ),
)
async def sync_stock_movement(
    movement_id: int,
    data: MovementSyncUpdate,
    session: AsyncSession = Depends(get_session),
):
    result = await session.execute(
        select(StockMovement)
        .where(StockMovement.id == movement_id)
        .options(
            selectinload(StockMovement.autopart).selectinload(AutoPart.brand),
            selectinload(StockMovement.storage_location),
        )
    )
    movement = result.scalars().first()
    if movement is None:
        raise HTTPException(status_code=404, detail="Движение не найдено")

    movement.sync_status = data.sync_status
    if data.external_id is not None:
        movement.external_id = data.external_id
    if data.sync_status == SyncStatus.SYNCED:
        movement.synced_at = now_moscow()
    else:
        movement.synced_at = None

    await session.commit()
    await session.refresh(movement)
    return _movement_to_out(movement)


@router.post(
    "/movements/bulk-sync/",
    response_model=MovementBulkSyncResult,
    summary="Пакетное подтверждение синхронизации из 1С",
    description=(
        "Обновляет статусы сразу для нескольких движений. "
        "Возвращает количество обновлённых записей и список ID не найденных."
    ),
)
async def bulk_sync_movements(
    data: MovementBulkSyncRequest,
    session: AsyncSession = Depends(get_session),
):
    ids = [item.id for item in data.items]
    existing = (
        (
            await session.execute(
                select(StockMovement.id).where(StockMovement.id.in_(ids))
            )
        )
        .scalars()
        .all()
    )
    existing_set = set(existing)
    not_found = [i for i in ids if i not in existing_set]

    now = now_moscow()
    updated = 0
    for item in data.items:
        if item.id not in existing_set:
            continue
        values: dict = {"sync_status": item.sync_status}
        if item.external_id is not None:
            values["external_id"] = item.external_id
        if item.sync_status == SyncStatus.SYNCED:
            values["synced_at"] = now
        else:
            values["synced_at"] = None
        await session.execute(
            update(StockMovement)
            .where(StockMovement.id == item.id)
            .values(**values)
        )
        updated += 1

    await session.commit()
    return MovementBulkSyncResult(updated=updated, not_found=not_found)


@router.get(
    "/movements/{movement_id}/",
    response_model=StockMovementOut,
    summary="Получить одно движение по ID",
)
async def get_stock_movement(
    movement_id: int,
    session: AsyncSession = Depends(get_session),
):
    result = await session.execute(
        select(StockMovement)
        .where(StockMovement.id == movement_id)
        .options(
            selectinload(StockMovement.autopart).selectinload(AutoPart.brand),
            selectinload(StockMovement.storage_location),
        )
    )
    movement = result.scalars().first()
    if movement is None:
        raise HTTPException(status_code=404, detail="Движение не найдено")
    return _movement_to_out(movement)


# ─── Transfer ───────────────────────────────────────────────────────────────


@router.post(
    "/transfer/",
    response_model=TransferResult,
    status_code=status.HTTP_201_CREATED,
    summary="Переместить N единиц товара между ячейками",
)
async def transfer_autopart(
    data: TransferRequest,
    session: AsyncSession = Depends(get_session),
):
    """
    Moves `quantity` units of an autopart
    from one storage location to another:
    1. Checks source StockByLocation has enough stock.
    2. Decreases source quantity (removes record + M2M if reaches 0).
    3. Increases destination quantity (creates record + M2M if new).
    4. Creates TRANSFER_OUT and TRANSFER_IN StockMovement records.
    """
    if data.from_location_id == data.to_location_id:
        raise HTTPException(
            status_code=400, detail="Место-источник и назначение совпадают"
        )

    # ── Validate locations ──────────────────────────────────────────────────
    locs = (
        (
            await session.execute(
                select(StorageLocation).where(
                    StorageLocation.id.in_(
                        [data.from_location_id, data.to_location_id]
                    )
                )
            )
        )
        .scalars()
        .all()
    )
    loc_map = {loc.id: loc for loc in locs}

    if data.from_location_id not in loc_map:
        raise HTTPException(
            status_code=404, detail="Место-источник не найдено"
        )
    if data.to_location_id not in loc_map:
        raise HTTPException(
            status_code=404, detail="Место-назначение не найдено"
        )

    # ── Check source StockByLocation ─────────────────────────────────────────
    src_sbl_result = await session.execute(
        select(StockByLocation).where(
            StockByLocation.autopart_id == data.autopart_id,
            StockByLocation.storage_location_id == data.from_location_id,
        )
    )
    src_sbl = src_sbl_result.scalar_one_or_none()
    if not src_sbl or src_sbl.quantity < data.quantity:
        available = src_sbl.quantity if src_sbl else 0
        raise HTTPException(
            status_code=400,
            detail=(
                f"Недостаточно товара в "
                f"«{loc_map[data.from_location_id].name}»: "
                f"доступно {available}, запрошено {data.quantity}"
            ),
        )

    # ── Capacity check for destination ───────────────────────────────────────
    dest_loc = loc_map[data.to_location_id]
    dest_sbl_result = await session.execute(
        select(StockByLocation).where(
            StockByLocation.autopart_id == data.autopart_id,
            StockByLocation.storage_location_id == data.to_location_id,
        )
    )
    dest_sbl = dest_sbl_result.scalar_one_or_none()

    if dest_loc.capacity is not None and dest_sbl is None:
        # new SKU coming in — check SKU count
        current_sku_count = (
            await session.execute(
                select(func.count())
                .select_from(StockByLocation)
                .where(
                    StockByLocation.storage_location_id == data.to_location_id
                )
            )
        ).scalar_one()
        if current_sku_count >= dest_loc.capacity:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Место «{dest_loc.name}» заполнено "
                    f"({current_sku_count}/{dest_loc.capacity} SKU)"
                ),
            )

    from_name = loc_map[data.from_location_id].name
    to_name = loc_map[data.to_location_id].name
    note = data.notes or f"Перемещение {from_name} → {to_name}"

    # Переносим лоты (ГТД) между ячейками (flush внутри функции)
    transfer_result = await transfer_stock_with_lot_trace(
        session,
        autopart_id=data.autopart_id,
        from_location_id=data.from_location_id,
        to_location_id=data.to_location_id,
        quantity=data.quantity,
        notes=note,
    )

    await session.commit()

    return TransferResult(
        autopart_id=data.autopart_id,
        from_location_id=data.from_location_id,
        to_location_id=data.to_location_id,
        movement_out_id=transfer_result["movement_out_id"],
        movement_in_id=transfer_result["movement_in_id"],
    )


# ─── Stock Lots (GTD / партионный учёт) ────────────────────────────────────


def _lot_to_out(lot: StockLot) -> StockLotOut:
    loc = getattr(lot, "storage_location", None)
    return StockLotOut(
        id=lot.id,
        autopart_id=lot.autopart_id,
        storage_location_id=lot.storage_location_id,
        storage_location_name=loc.name if loc else None,
        source_type=lot.source_type,
        gtd_number=lot.gtd_number,
        country_code=lot.country_code,
        country_name=lot.country_name,
        initial_quantity=lot.initial_quantity,
        remaining_quantity=lot.remaining_quantity,
        source_receipt_id=lot.source_receipt_id,
        source_receipt_item_id=lot.source_receipt_item_id,
        source_document_item_id=lot.source_document_item_id,
        external_id=lot.external_id,
        sync_status=lot.sync_status,
        received_at=lot.received_at,
        created_at=lot.created_at,
    )


@router.get(
    "/autoparts/{autopart_id}/lots",
    response_model=List[StockLotOut],
    summary="Партии (лоты) по артикулу",
)
async def get_autopart_lots(
    autopart_id: int,
    storage_location_id: Optional[int] = Query(default=None),
    only_active: bool = Query(
        default=False, description="Только с остатком > 0"
    ),
    session: AsyncSession = Depends(get_session),
):
    lots = await get_lots_for_autopart(
        session,
        autopart_id=autopart_id,
        storage_location_id=storage_location_id,
        only_active=only_active,
    )
    return [_lot_to_out(lot) for lot in lots]


@router.get(
    "/lots",
    response_model=List[StockLotOut],
    summary="Список партий с фильтрацией",
)
async def list_stock_lots(
    autopart_id: Optional[int] = Query(default=None),
    storage_location_id: Optional[int] = Query(default=None),
    gtd_number: Optional[str] = Query(default=None),
    source_receipt_id: Optional[int] = Query(default=None),
    only_active: bool = Query(default=False),
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
    session: AsyncSession = Depends(get_session),
):
    from sqlalchemy import asc as _asc

    stmt = select(StockLot)
    if autopart_id is not None:
        stmt = stmt.where(StockLot.autopart_id == autopart_id)
    if storage_location_id is not None:
        stmt = stmt.where(StockLot.storage_location_id == storage_location_id)
    if gtd_number is not None:
        stmt = stmt.where(StockLot.gtd_number.ilike(f"%{gtd_number}%"))
    if source_receipt_id is not None:
        stmt = stmt.where(StockLot.source_receipt_id == source_receipt_id)
    if only_active:
        stmt = stmt.where(StockLot.remaining_quantity > 0)
    stmt = stmt.order_by(_asc(StockLot.received_at), _asc(StockLot.id))
    stmt = stmt.offset(offset).limit(limit)
    lots = (await session.execute(stmt)).scalars().all()
    return [_lot_to_out(lot) for lot in lots]


@router.get(
    "/lots/{lot_id}",
    response_model=StockLotOut,
    summary="Партия по ID",
)
async def get_stock_lot(
    lot_id: int,
    session: AsyncSession = Depends(get_session),
):
    lot = await session.get(StockLot, lot_id)
    if lot is None:
        raise HTTPException(status_code=404, detail="Партия не найдена")
    return _lot_to_out(lot)


# ─── StockDocument (ручные документы оприходования / списания) ─────────────


def _doc_item_to_out(item: StockDocumentItem) -> StockDocumentItemOut:
    ap = getattr(item, "autopart", None)
    loc = getattr(item, "storage_location", None)
    return StockDocumentItemOut(
        id=item.id,
        document_id=item.document_id,
        autopart_id=item.autopart_id,
        storage_location_id=item.storage_location_id,
        quantity=item.quantity,
        gtd_number=item.gtd_number,
        country_code=item.country_code,
        country_name=item.country_name,
        lot_id=item.lot_id,
        notes=item.notes,
        autopart_oem=ap.oem_number if ap else None,
        autopart_name=ap.name if ap else None,
        autopart_brand=ap.brand.name if (ap and ap.brand) else None,
        storage_location_name=loc.name if loc else None,
    )


def _doc_to_out(doc: StockDocument) -> StockDocumentOut:
    wh = getattr(doc, "warehouse", None)
    return StockDocumentOut(
        id=doc.id,
        doc_type=doc.doc_type,
        status=doc.status,
        document_number=doc.document_number,
        document_date=doc.document_date,
        warehouse_id=doc.warehouse_id,
        warehouse_name=wh.name if wh else None,
        reason=doc.reason,
        notes=doc.notes,
        external_id=doc.external_id,
        sync_status=doc.sync_status,
        created_at=doc.created_at,
        posted_at=doc.posted_at,
        items=[_doc_item_to_out(i) for i in (doc.items or [])],
    )


def _doc_to_list_item(doc: StockDocument) -> StockDocumentListItem:
    wh = getattr(doc, "warehouse", None)
    return StockDocumentListItem(
        id=doc.id,
        doc_type=doc.doc_type,
        status=doc.status,
        document_number=doc.document_number,
        document_date=doc.document_date,
        warehouse_id=doc.warehouse_id,
        warehouse_name=wh.name if wh else None,
        reason=doc.reason,
        item_count=len(doc.items or []),
        external_id=doc.external_id,
        sync_status=doc.sync_status,
        created_at=doc.created_at,
        posted_at=doc.posted_at,
    )


@router.get(
    "/documents/",
    response_model=List[StockDocumentListItem],
    summary="Список документов ручного оприходования / списания",
)
async def list_stock_documents(
    doc_type: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    warehouse_id: Optional[int] = Query(None),
    offset: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=200),
    db: AsyncSession = Depends(get_session),
):
    stmt = (
        select(StockDocument)
        .order_by(StockDocument.document_date.desc(), StockDocument.id.desc())
        .offset(offset)
        .limit(limit)
    )
    if doc_type:
        stmt = stmt.where(StockDocument.doc_type == doc_type)
    if status:
        stmt = stmt.where(StockDocument.status == status)
    if warehouse_id:
        stmt = stmt.where(StockDocument.warehouse_id == warehouse_id)
    docs = (await db.execute(stmt)).scalars().all()

    return [_doc_to_list_item(doc) for doc in docs]


@router.post(
    "/documents/",
    response_model=StockDocumentOut,
    status_code=status.HTTP_201_CREATED,
    summary="Создать документ оприходования или списания (черновик)",
)
async def create_stock_document(
    data: StockDocumentCreate,
    db: AsyncSession = Depends(get_session),
):
    doc = StockDocument(
        doc_type=data.doc_type,
        status=StockDocumentStatus.DRAFT,
        document_number=data.document_number,
        document_date=data.document_date or now_moscow(),
        warehouse_id=data.warehouse_id,
        reason=data.reason,
        notes=data.notes,
        external_id=data.external_id,
    )
    db.add(doc)
    await db.flush()

    for item_data in data.items:
        item = StockDocumentItem(
            document_id=doc.id,
            autopart_id=item_data.autopart_id,
            storage_location_id=item_data.storage_location_id,
            quantity=item_data.quantity,
            gtd_number=item_data.gtd_number,
            country_code=item_data.country_code,
            country_name=item_data.country_name,
            notes=item_data.notes,
        )
        db.add(item)

    await db.flush()

    # Reload with relationships
    stmt = (
        select(StockDocument)
        .options(selectinload(StockDocument.items))
        .where(StockDocument.id == doc.id)
    )
    doc = (await db.execute(stmt)).scalar_one()
    await db.commit()
    return _doc_to_out(doc)


@router.get(
    "/documents/{doc_id}",
    response_model=StockDocumentOut,
    summary="Документ оприходования / списания по ID",
)
async def get_stock_document(
    doc_id: int,
    db: AsyncSession = Depends(get_session),
):
    stmt = (
        select(StockDocument)
        .options(
            selectinload(StockDocument.items)
            .selectinload(StockDocumentItem.autopart)
            .selectinload(AutoPart.brand),
            selectinload(StockDocument.items).selectinload(
                StockDocumentItem.storage_location
            ),
        )
        .where(StockDocument.id == doc_id)
    )
    doc = (await db.execute(stmt)).scalar_one_or_none()
    if doc is None:
        raise HTTPException(status_code=404, detail="Документ не найден")
    return _doc_to_out(doc)


@router.patch(
    "/documents/{doc_id}",
    response_model=StockDocumentOut,
    summary="Обновить реквизиты черновика",
)
async def update_stock_document(
    doc_id: int,
    data: StockDocumentUpdate,
    db: AsyncSession = Depends(get_session),
):
    stmt = (
        select(StockDocument)
        .options(selectinload(StockDocument.items))
        .where(StockDocument.id == doc_id)
    )
    doc = (await db.execute(stmt)).scalar_one_or_none()
    if doc is None:
        raise HTTPException(status_code=404, detail="Документ не найден")
    if doc.status != StockDocumentStatus.DRAFT:
        raise HTTPException(
            status_code=400,
            detail="Редактировать можно только черновик",
        )
    for field, value in data.model_dump(exclude_unset=True).items():
        setattr(doc, field, value)
    await db.commit()
    return _doc_to_out(doc)


@router.post(
    "/documents/{doc_id}/items",
    response_model=StockDocumentItemOut,
    status_code=status.HTTP_201_CREATED,
    summary="Добавить строку в черновик",
)
async def add_document_item(
    doc_id: int,
    data: StockDocumentItemCreate,
    db: AsyncSession = Depends(get_session),
):
    doc = await db.get(StockDocument, doc_id)
    if doc is None:
        raise HTTPException(status_code=404, detail="Документ не найден")
    if doc.status != StockDocumentStatus.DRAFT:
        raise HTTPException(
            status_code=400, detail="Документ не в статусе черновика"
        )

    item = StockDocumentItem(
        document_id=doc_id,
        autopart_id=data.autopart_id,
        storage_location_id=data.storage_location_id,
        quantity=data.quantity,
        gtd_number=data.gtd_number,
        country_code=data.country_code,
        country_name=data.country_name,
        notes=data.notes,
    )
    db.add(item)
    await db.flush()

    # Reload with relationships
    stmt = (
        select(StockDocumentItem)
        .options(
            selectinload(StockDocumentItem.autopart).selectinload(
                AutoPart.brand
            ),
            selectinload(StockDocumentItem.storage_location),
        )
        .where(StockDocumentItem.id == item.id)
    )
    item = (await db.execute(stmt)).scalar_one()
    await db.commit()
    return _doc_item_to_out(item)


@router.patch(
    "/documents/{doc_id}/items/{item_id}",
    response_model=StockDocumentItemOut,
    summary="Обновить строку черновика",
)
async def update_document_item(
    doc_id: int,
    item_id: int,
    data: StockDocumentItemUpdate,
    db: AsyncSession = Depends(get_session),
):
    doc = await db.get(StockDocument, doc_id)
    if doc is None:
        raise HTTPException(status_code=404, detail="Документ не найден")
    if doc.status != StockDocumentStatus.DRAFT:
        raise HTTPException(
            status_code=400, detail="Документ не в статусе черновика"
        )

    stmt = (
        select(StockDocumentItem)
        .options(
            selectinload(StockDocumentItem.autopart).selectinload(
                AutoPart.brand
            ),
            selectinload(StockDocumentItem.storage_location),
        )
        .where(
            StockDocumentItem.id == item_id,
            StockDocumentItem.document_id == doc_id,
        )
    )
    item = (await db.execute(stmt)).scalar_one_or_none()
    if item is None:
        raise HTTPException(status_code=404, detail="Строка не найдена")

    for field, value in data.model_dump(exclude_unset=True).items():
        setattr(item, field, value)
    await db.commit()
    return _doc_item_to_out(item)


@router.delete(
    "/documents/{doc_id}/items/{item_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Удалить строку черновика",
)
async def delete_document_item(
    doc_id: int,
    item_id: int,
    db: AsyncSession = Depends(get_session),
):
    doc = await db.get(StockDocument, doc_id)
    if doc is None:
        raise HTTPException(status_code=404, detail="Документ не найден")
    if doc.status != StockDocumentStatus.DRAFT:
        raise HTTPException(
            status_code=400, detail="Документ не в статусе черновика"
        )

    item = await db.get(StockDocumentItem, item_id)
    if item is None or item.document_id != doc_id:
        raise HTTPException(status_code=404, detail="Строка не найдена")
    await db.delete(item)
    await db.commit()


@router.post(
    "/documents/{doc_id}/post",
    summary="Провести документ — обновить остатки",
)
async def post_document(
    doc_id: int,
    db: AsyncSession = Depends(get_session),
):
    try:
        result = await post_stock_document(db, document_id=doc_id)
    except LookupError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    await db.commit()
    return result


@router.post(
    "/documents/{doc_id}/unpost",
    summary="Распровести документ — отменить изменения остатков",
)
async def unpost_document(
    doc_id: int,
    db: AsyncSession = Depends(get_session),
):
    try:
        result = await unpost_stock_document(db, document_id=doc_id)
    except LookupError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    await db.commit()
    return result


@router.delete(
    "/documents/{doc_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Удалить черновик",
)
async def delete_stock_document(
    doc_id: int,
    db: AsyncSession = Depends(get_session),
):
    doc = await db.get(StockDocument, doc_id)
    if doc is None:
        raise HTTPException(status_code=404, detail="Документ не найден")
    if doc.status == StockDocumentStatus.POSTED:
        raise HTTPException(
            status_code=400,
            detail="Нельзя удалить проведённый документ. "
            "Сначала распроведите.",
        )
    await db.delete(doc)
    await db.commit()


# ─── Admin / backfill ────────────────────────────────────────────────────────


@router.post(
    "/admin/backfill-lots",
    response_model=BackfillResult,
    summary="Backfill: создать opening_balance лоты для товара без партий",
    description=(
        "Одноразовая операция — создаёт лоты с source_type=opening_balance "
        "для всех позиций StockByLocation, у которых нет активных лотов. "
        "Безопасно запускать повторно: "
        "позиции с уже покрытым остатком пропускаются."
    ),
)
async def run_backfill_lots(
    db: AsyncSession = Depends(get_session),
):
    result = await backfill_opening_balance_lots(db)
    await db.commit()
    return BackfillResult(**result)


# ═══════════════════════════════════════════════════════════════════════════════
# Резервы (StockReserve)
# ═══════════════════════════════════════════════════════════════════════════════


def _reserve_to_out(r: StockReserve) -> StockReserveOut:
    ap = r.autopart
    loc = r.storage_location
    return StockReserveOut(
        id=r.id,
        autopart_id=r.autopart_id,
        storage_location_id=r.storage_location_id,
        quantity=r.quantity,
        status=r.status,
        customer_order_item_id=r.customer_order_item_id,
        stock_order_item_id=r.stock_order_item_id,
        expires_at=r.expires_at,
        released_at=r.released_at,
        notes=r.notes,
        external_id=r.external_id,
        sync_status=r.sync_status,
        created_at=r.created_at,
        autopart_oem=ap.oem_number if ap else None,
        autopart_name=ap.name if ap else None,
        autopart_brand=ap.brand.name if (ap and ap.brand) else None,
        storage_location_name=loc.name if loc else None,
    )


@router.post(
    "/reserves/",
    response_model=StockReserveOut,
    status_code=status.HTTP_201_CREATED,
    summary="Создать резерв товара под заказ",
)
async def create_stock_reserve(
    data: StockReserveCreate,
    session: AsyncSession = Depends(get_session),
):
    """Резервирует `quantity` единиц запчасти.
    Проверяет, что свободного остатка достаточно (физический − уже зарезервированный).
    """
    if not (await session.get(AutoPart, data.autopart_id)):
        raise HTTPException(status_code=404, detail="Запчасть не найдена")
    if data.storage_location_id and not (
        await session.get(StorageLocation, data.storage_location_id)
    ):
        raise HTTPException(status_code=404, detail="Ячейка не найдена")

    try:
        reserve = await create_reserve(
            session,
            autopart_id=data.autopart_id,
            quantity=data.quantity,
            storage_location_id=data.storage_location_id,
            customer_order_item_id=data.customer_order_item_id,
            stock_order_item_id=data.stock_order_item_id,
            expires_at=data.expires_at,
            notes=data.notes,
            external_id=data.external_id,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    # Reload with joined autopart.brand
    result = await session.execute(
        select(StockReserve)
        .where(StockReserve.id == reserve.id)
        .options(
            selectinload(StockReserve.autopart).selectinload(AutoPart.brand),
            selectinload(StockReserve.storage_location),
        )
    )
    reserve = result.scalar_one()
    await session.commit()
    return _reserve_to_out(reserve)


@router.get(
    "/reserves/",
    response_model=List[StockReserveOut],
    summary="Список резервов",
)
async def list_reserves(
    autopart_id: Optional[int] = Query(None),
    storage_location_id: Optional[int] = Query(None),
    status_filter: Optional[ReserveStatus] = Query(
        None,
        alias="status",
        description="Фильтр по статусу (по умолчанию все)",
    ),
    customer_order_item_id: Optional[int] = Query(None),
    stock_order_item_id: Optional[int] = Query(None),
    offset: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=500),
    session: AsyncSession = Depends(get_session),
):
    stmt = (
        select(StockReserve)
        .options(
            selectinload(StockReserve.autopart).selectinload(AutoPart.brand),
            selectinload(StockReserve.storage_location),
        )
        .order_by(StockReserve.created_at.desc())
        .offset(offset)
        .limit(limit)
    )
    if autopart_id is not None:
        stmt = stmt.where(StockReserve.autopart_id == autopart_id)
    if storage_location_id is not None:
        stmt = stmt.where(
            StockReserve.storage_location_id == storage_location_id
        )
    if status_filter is not None:
        stmt = stmt.where(StockReserve.status == status_filter)
    if customer_order_item_id is not None:
        stmt = stmt.where(
            StockReserve.customer_order_item_id == customer_order_item_id
        )
    if stock_order_item_id is not None:
        stmt = stmt.where(
            StockReserve.stock_order_item_id == stock_order_item_id
        )
    rows = (await session.execute(stmt)).scalars().all()
    return [_reserve_to_out(r) for r in rows]


@router.get(
    "/reserves/{reserve_id}/",
    response_model=StockReserveOut,
    summary="Получить резерв по ID",
)
async def get_reserve(
    reserve_id: int,
    session: AsyncSession = Depends(get_session),
):
    result = await session.execute(
        select(StockReserve)
        .where(StockReserve.id == reserve_id)
        .options(
            selectinload(StockReserve.autopart).selectinload(AutoPart.brand),
            selectinload(StockReserve.storage_location),
        )
    )
    reserve = result.scalar_one_or_none()
    if reserve is None:
        raise HTTPException(status_code=404, detail="Резерв не найден")
    return _reserve_to_out(reserve)


@router.delete(
    "/reserves/{reserve_id}/",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Отменить резерв",
)
async def cancel_stock_reserve(
    reserve_id: int,
    session: AsyncSession = Depends(get_session),
):
    """Переводит резерв в статус CANCELLED. Нельзя отменить уже снятый резерв."""
    reserve = await session.get(StockReserve, reserve_id)
    if reserve is None:
        raise HTTPException(status_code=404, detail="Резерв не найден")
    if reserve.status == ReserveStatus.RELEASED:
        raise HTTPException(
            status_code=400,
            detail="Резерв уже снят при отгрузке — отменить нельзя",
        )
    if reserve.status == ReserveStatus.CANCELLED:
        raise HTTPException(
            status_code=400,
            detail="Резерв уже отменён",
        )
    await cancel_reserve(session, reserve)
    await session.commit()


@router.post(
    "/reserves/bulk-cancel/",
    response_model=StockReserveCancelResult,
    summary="Массовая отмена резервов (например, при отмене заказа)",
)
async def bulk_cancel_reserves(
    data: StockReserveCancelRequest,
    session: AsyncSession = Depends(get_session),
):
    rows = (
        (
            await session.execute(
                select(StockReserve).where(
                    StockReserve.id.in_(data.reserve_ids)
                )
            )
        )
        .scalars()
        .all()
    )

    found_ids = {r.id for r in rows}
    not_found = [i for i in data.reserve_ids if i not in found_ids]
    already_inactive: list[int] = []
    cancelled_count = 0

    for reserve in rows:
        if reserve.status != ReserveStatus.ACTIVE:
            already_inactive.append(reserve.id)
            continue
        await cancel_reserve(session, reserve)
        cancelled_count += 1

    await session.commit()
    return StockReserveCancelResult(
        cancelled=cancelled_count,
        not_found=not_found,
        already_inactive=already_inactive,
    )


@router.get(
    "/available/",
    summary="Свободный остаток по запчасти",
    response_model=dict,
)
async def get_available_stock(
    autopart_id: int = Query(...),
    storage_location_id: Optional[int] = Query(None),
    session: AsyncSession = Depends(get_session),
):
    """Возвращает физический, зарезервированный и свободный остатки."""
    physical = await get_available_quantity(
        session,
        autopart_id=autopart_id,
        storage_location_id=storage_location_id,
    ) + await get_reserved_quantity(
        session,
        autopart_id=autopart_id,
        storage_location_id=storage_location_id,
    )
    reserved = await get_reserved_quantity(
        session,
        autopart_id=autopart_id,
        storage_location_id=storage_location_id,
    )
    return {
        "autopart_id": autopart_id,
        "storage_location_id": storage_location_id,
        "physical": physical,
        "reserved": reserved,
        "available": max(0, physical - reserved),
    }


# ═══════════════════════════════════════════════════════════════════════════════
# Накладные на отгрузку (ShipmentDocument)
# ═══════════════════════════════════════════════════════════════════════════════


def _shipment_item_to_out(
    item: ShipmentDocumentItem,
) -> ShipmentDocumentItemOut:
    ap = item.autopart
    loc = item.storage_location
    lot = item.lot if hasattr(item, "lot") else None
    return ShipmentDocumentItemOut(
        id=item.id,
        document_id=item.document_id,
        autopart_id=item.autopart_id,
        storage_location_id=item.storage_location_id,
        quantity=item.quantity,
        price=item.price,
        reserve_id=item.reserve_id,
        lot_id=item.lot_id,
        notes=item.notes,
        autopart_oem=ap.oem_number if ap else None,
        autopart_name=ap.name if ap else None,
        autopart_brand=ap.brand.name if (ap and ap.brand) else None,
        storage_location_name=loc.name if loc else None,
        gtd_number=lot.gtd_number if lot else None,
    )


def _shipment_to_out(doc: ShipmentDocument) -> ShipmentDocumentOut:
    customer = doc.customer
    warehouse = doc.warehouse
    items = [_shipment_item_to_out(i) for i in (doc.items or [])]
    return ShipmentDocumentOut(
        id=doc.id,
        doc_number=doc.doc_number,
        doc_date=doc.doc_date,
        status=doc.status,
        customer_id=doc.customer_id,
        customer_name=customer.name if customer else None,
        customer_order_id=doc.customer_order_id,
        warehouse_id=doc.warehouse_id,
        warehouse_name=warehouse.name if warehouse else None,
        reason=doc.reason,
        notes=doc.notes,
        external_id=doc.external_id,
        sync_status=doc.sync_status,
        created_at=doc.created_at,
        posted_at=doc.posted_at,
        items=items,
        total_quantity=sum(i.quantity for i in items),
    )


def _shipment_to_list_item(doc: ShipmentDocument) -> ShipmentDocumentListItem:
    customer = doc.customer
    warehouse = doc.warehouse
    items = doc.items or []
    return ShipmentDocumentListItem(
        id=doc.id,
        doc_number=doc.doc_number,
        doc_date=doc.doc_date,
        status=doc.status,
        customer_id=doc.customer_id,
        customer_name=customer.name if customer else None,
        customer_order_id=doc.customer_order_id,
        warehouse_id=doc.warehouse_id,
        warehouse_name=warehouse.name if warehouse else None,
        reason=doc.reason,
        item_count=len(items),
        total_quantity=sum(i.quantity for i in items),
        sync_status=doc.sync_status,
        created_at=doc.created_at,
        posted_at=doc.posted_at,
    )


async def _load_shipment(
    session: AsyncSession, doc_id: int
) -> ShipmentDocument:
    """Load ShipmentDocument with all relations needed for output."""
    result = await session.execute(
        select(ShipmentDocument)
        .where(ShipmentDocument.id == doc_id)
        .options(
            selectinload(ShipmentDocument.items)
            .selectinload(ShipmentDocumentItem.autopart)
            .selectinload(AutoPart.brand),
            selectinload(ShipmentDocument.items).selectinload(
                ShipmentDocumentItem.storage_location
            ),
            selectinload(ShipmentDocument.items).selectinload(
                ShipmentDocumentItem.lot
            ),
            selectinload(ShipmentDocument.customer),
            selectinload(ShipmentDocument.warehouse),
        )
    )
    return result.scalar_one_or_none()


@router.post(
    "/shipments/",
    response_model=ShipmentDocumentOut,
    status_code=status.HTTP_201_CREATED,
    summary="Создать накладную на отгрузку",
)
async def create_shipment_document(
    data: ShipmentDocumentCreate,
    session: AsyncSession = Depends(get_session),
):
    doc = ShipmentDocument(
        doc_number=data.doc_number,
        doc_date=data.doc_date or now_moscow(),
        customer_id=data.customer_id,
        customer_order_id=data.customer_order_id,
        warehouse_id=data.warehouse_id,
        reason=data.reason,
        notes=data.notes,
        external_id=data.external_id,
    )
    session.add(doc)
    await session.flush()

    for item_data in data.items:
        item = ShipmentDocumentItem(
            document_id=doc.id,
            autopart_id=item_data.autopart_id,
            storage_location_id=item_data.storage_location_id,
            quantity=item_data.quantity,
            price=item_data.price,
            reserve_id=item_data.reserve_id,
            notes=item_data.notes,
        )
        session.add(item)

    await session.flush()
    doc = await _load_shipment(session, doc.id)
    await session.commit()
    return _shipment_to_out(doc)


@router.get(
    "/shipments/",
    response_model=List[ShipmentDocumentListItem],
    summary="Список накладных на отгрузку",
)
async def list_shipment_documents(
    status_filter: Optional[ShipmentDocumentStatus] = Query(
        None, alias="status"
    ),
    customer_id: Optional[int] = Query(None),
    customer_order_id: Optional[int] = Query(None),
    sync_status: Optional[SyncStatus] = Query(None),
    date_from: Optional[datetime] = Query(None),
    date_to: Optional[datetime] = Query(None),
    offset: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=500),
    session: AsyncSession = Depends(get_session),
):
    stmt = (
        select(ShipmentDocument)
        .options(
            selectinload(ShipmentDocument.items),
            selectinload(ShipmentDocument.customer),
            selectinload(ShipmentDocument.warehouse),
        )
        .order_by(ShipmentDocument.doc_date.desc(), ShipmentDocument.id.desc())
        .offset(offset)
        .limit(limit)
    )
    if status_filter is not None:
        stmt = stmt.where(ShipmentDocument.status == status_filter)
    if customer_id is not None:
        stmt = stmt.where(ShipmentDocument.customer_id == customer_id)
    if customer_order_id is not None:
        stmt = stmt.where(
            ShipmentDocument.customer_order_id == customer_order_id
        )
    if sync_status is not None:
        stmt = stmt.where(ShipmentDocument.sync_status == sync_status)
    if date_from is not None:
        stmt = stmt.where(ShipmentDocument.doc_date >= date_from)
    if date_to is not None:
        stmt = stmt.where(ShipmentDocument.doc_date <= date_to)

    rows = (await session.execute(stmt)).scalars().all()
    return [_shipment_to_list_item(r) for r in rows]


@router.get(
    "/shipments/{doc_id}/",
    response_model=ShipmentDocumentOut,
    summary="Получить накладную по ID",
)
async def get_shipment_document(
    doc_id: int,
    session: AsyncSession = Depends(get_session),
):
    doc = await _load_shipment(session, doc_id)
    if doc is None:
        raise HTTPException(status_code=404, detail="Накладная не найдена")
    return _shipment_to_out(doc)


@router.patch(
    "/shipments/{doc_id}/",
    response_model=ShipmentDocumentOut,
    summary="Обновить реквизиты накладной (только DRAFT)",
)
async def update_shipment_document(
    doc_id: int,
    data: ShipmentDocumentUpdate,
    session: AsyncSession = Depends(get_session),
):
    doc = await session.get(ShipmentDocument, doc_id)
    if doc is None:
        raise HTTPException(status_code=404, detail="Накладная не найдена")
    if doc.status != ShipmentDocumentStatus.DRAFT:
        raise HTTPException(
            status_code=400,
            detail="Можно редактировать только черновик",
        )
    for field, value in data.model_dump(exclude_unset=True).items():
        setattr(doc, field, value)
    await session.flush()
    doc = await _load_shipment(session, doc_id)
    await session.commit()
    return _shipment_to_out(doc)


@router.post(
    "/shipments/{doc_id}/items/",
    response_model=ShipmentDocumentItemOut,
    status_code=status.HTTP_201_CREATED,
    summary="Добавить строку в накладную (только DRAFT)",
)
async def add_shipment_item(
    doc_id: int,
    data: ShipmentDocumentItemCreate,
    session: AsyncSession = Depends(get_session),
):
    doc = await session.get(ShipmentDocument, doc_id)
    if doc is None:
        raise HTTPException(status_code=404, detail="Накладная не найдена")
    if doc.status != ShipmentDocumentStatus.DRAFT:
        raise HTTPException(
            status_code=400,
            detail="Можно добавлять строки только в черновик",
        )
    item = ShipmentDocumentItem(
        document_id=doc_id,
        autopart_id=data.autopart_id,
        storage_location_id=data.storage_location_id,
        quantity=data.quantity,
        price=data.price,
        reserve_id=data.reserve_id,
        notes=data.notes,
    )
    session.add(item)
    await session.flush()

    result = await session.execute(
        select(ShipmentDocumentItem)
        .where(ShipmentDocumentItem.id == item.id)
        .options(
            selectinload(ShipmentDocumentItem.autopart).selectinload(
                AutoPart.brand
            ),
            selectinload(ShipmentDocumentItem.storage_location),
            selectinload(ShipmentDocumentItem.lot),
        )
    )
    item = result.scalar_one()
    await session.commit()
    return _shipment_item_to_out(item)


@router.patch(
    "/shipments/{doc_id}/items/{item_id}/",
    response_model=ShipmentDocumentItemOut,
    summary="Обновить строку накладной (только DRAFT)",
)
async def update_shipment_item(
    doc_id: int,
    item_id: int,
    data: ShipmentDocumentItemUpdate,
    session: AsyncSession = Depends(get_session),
):
    doc = await session.get(ShipmentDocument, doc_id)
    if doc is None:
        raise HTTPException(status_code=404, detail="Накладная не найдена")
    if doc.status != ShipmentDocumentStatus.DRAFT:
        raise HTTPException(
            status_code=400,
            detail="Можно редактировать только черновик",
        )
    item = await session.get(ShipmentDocumentItem, item_id)
    if item is None or item.document_id != doc_id:
        raise HTTPException(status_code=404, detail="Строка не найдена")

    for field, value in data.model_dump(exclude_unset=True).items():
        setattr(item, field, value)
    await session.flush()

    result = await session.execute(
        select(ShipmentDocumentItem)
        .where(ShipmentDocumentItem.id == item_id)
        .options(
            selectinload(ShipmentDocumentItem.autopart).selectinload(
                AutoPart.brand
            ),
            selectinload(ShipmentDocumentItem.storage_location),
            selectinload(ShipmentDocumentItem.lot),
        )
    )
    item = result.scalar_one()
    await session.commit()
    return _shipment_item_to_out(item)


@router.delete(
    "/shipments/{doc_id}/items/{item_id}/",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Удалить строку накладной (только DRAFT)",
)
async def delete_shipment_item(
    doc_id: int,
    item_id: int,
    session: AsyncSession = Depends(get_session),
):
    doc = await session.get(ShipmentDocument, doc_id)
    if doc is None:
        raise HTTPException(status_code=404, detail="Накладная не найдена")
    if doc.status != ShipmentDocumentStatus.DRAFT:
        raise HTTPException(
            status_code=400,
            detail="Можно удалять строки только в черновике",
        )
    item = await session.get(ShipmentDocumentItem, item_id)
    if item is None or item.document_id != doc_id:
        raise HTTPException(status_code=404, detail="Строка не найдена")
    await session.delete(item)
    await session.commit()


@router.post(
    "/shipments/{doc_id}/post/",
    response_model=ShipmentPostResult,
    summary="Провести накладную на отгрузку",
    description=(
        "Снимает резервы, расходует FIFO-лоты, уменьшает остатки, "
        "создаёт StockMovement(SHIPMENT) для каждой строки."
    ),
)
async def post_shipment(
    doc_id: int,
    session: AsyncSession = Depends(get_session),
):
    try:
        result = await post_shipment_document(session, doc_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    await session.commit()
    return ShipmentPostResult(
        document_id=doc_id,
        movements_created=result["movements_created"],
        reserves_released=result["reserves_released"],
        lots_consumed=result["lot_ids"],
    )


@router.post(
    "/shipments/{doc_id}/unpost/",
    response_model=dict,
    summary="Отменить проводку накладной",
    description=(
        "Создаёт обратные движения, восстанавливает остатки и лоты. "
        "Резервы не восстанавливаются — при необходимости создайте новые."
    ),
)
async def unpost_shipment(
    doc_id: int,
    session: AsyncSession = Depends(get_session),
):
    try:
        result = await unpost_shipment_document(session, doc_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    await session.commit()
    return result


@router.delete(
    "/shipments/{doc_id}/",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Удалить накладную (только DRAFT или CANCELLED)",
)
async def delete_shipment_document(
    doc_id: int,
    session: AsyncSession = Depends(get_session),
):
    doc = await session.get(ShipmentDocument, doc_id)
    if doc is None:
        raise HTTPException(status_code=404, detail="Накладная не найдена")
    if doc.status == ShipmentDocumentStatus.POSTED:
        raise HTTPException(
            status_code=400,
            detail="Проведённую накладную нельзя удалить — сначала отмените проводку",
        )
    await session.delete(doc)
    await session.commit()


# ═══════════════════════════════════════════════════════════════════════════════
# Возвраты
# ═══════════════════════════════════════════════════════════════════════════════


def _return_item_to_out(item: ReturnItem) -> ReturnItemOut:
    autopart = getattr(item, "autopart", None)
    location = getattr(item, "storage_location", None)
    return ReturnItemOut(
        id=item.id,
        return_from_customer_id=item.return_from_customer_id,
        return_to_supplier_id=item.return_to_supplier_id,
        shipment_item_id=item.shipment_item_id,
        supplier_receipt_item_id=item.supplier_receipt_item_id,
        customer_order_item_id=item.customer_order_item_id,
        supplier_order_item_id=item.supplier_order_item_id,
        order_item_id=item.order_item_id,
        autopart_id=item.autopart_id,
        storage_location_id=item.storage_location_id,
        lot_id=item.lot_id,
        quantity=item.quantity,
        price=item.price,
        gtd_number=item.gtd_number,
        country_code=item.country_code,
        country_name=item.country_name,
        oem_number=item.oem_number,
        brand_name=item.brand_name,
        autopart_name=item.autopart_name,
        notes=item.notes,
        storage_location_name=location.name if location else None,
        autopart_oem=autopart.oem_number if autopart else item.oem_number,
        autopart_brand=(
            autopart.brand.name
            if autopart is not None and autopart.brand is not None
            else item.brand_name
        ),
    )


def _customer_return_to_out(doc: ReturnFromCustomer) -> ReturnFromCustomerOut:
    customer = getattr(doc, "customer", None)
    warehouse = getattr(doc, "warehouse", None)
    diadoc_outgoing = getattr(doc, "diadoc_outgoing_document", None)
    items = [_return_item_to_out(item) for item in (doc.items or [])]
    return ReturnFromCustomerOut(
        id=doc.id,
        doc_number=doc.doc_number,
        doc_date=doc.doc_date,
        status=doc.status,
        customer_id=doc.customer_id,
        customer_name=customer.name if customer else None,
        shipment_document_id=doc.shipment_document_id,
        warehouse_id=doc.warehouse_id,
        warehouse_name=warehouse.name if warehouse else None,
        diadoc_outgoing_document_id=doc.diadoc_outgoing_document_id,
        diadoc_outgoing_status=(
            diadoc_outgoing.status if diadoc_outgoing else None
        ),
        reason=doc.reason,
        notes=doc.notes,
        external_id=doc.external_id,
        sync_status=doc.sync_status,
        created_at=doc.created_at,
        approved_at=doc.approved_at,
        shipped_at=doc.shipped_at,
        confirmed_at=doc.confirmed_at,
        rejected_at=doc.rejected_at,
        items=items,
        total_quantity=sum(
            int(item.quantity or 0) for item in doc.items or []
        ),
    )


def _customer_return_to_list_item(
    doc: ReturnFromCustomer,
) -> ReturnFromCustomerListItem:
    customer = getattr(doc, "customer", None)
    warehouse = getattr(doc, "warehouse", None)
    diadoc_outgoing = getattr(doc, "diadoc_outgoing_document", None)
    return ReturnFromCustomerListItem(
        id=doc.id,
        doc_number=doc.doc_number,
        doc_date=doc.doc_date,
        status=doc.status,
        customer_id=doc.customer_id,
        customer_name=customer.name if customer else None,
        shipment_document_id=doc.shipment_document_id,
        warehouse_id=doc.warehouse_id,
        warehouse_name=warehouse.name if warehouse else None,
        reason=doc.reason,
        item_count=len(doc.items or []),
        total_quantity=sum(
            int(item.quantity or 0) for item in doc.items or []
        ),
        diadoc_outgoing_document_id=doc.diadoc_outgoing_document_id,
        diadoc_outgoing_status=(
            diadoc_outgoing.status if diadoc_outgoing else None
        ),
        sync_status=doc.sync_status,
        created_at=doc.created_at,
        approved_at=doc.approved_at,
        shipped_at=doc.shipped_at,
        confirmed_at=doc.confirmed_at,
        rejected_at=doc.rejected_at,
    )


def _supplier_return_to_out(doc: ReturnToSupplier) -> ReturnToSupplierOut:
    provider = getattr(doc, "provider", None)
    warehouse = getattr(doc, "warehouse", None)
    diadoc_outgoing = getattr(doc, "diadoc_outgoing_document", None)
    items = [_return_item_to_out(item) for item in (doc.items or [])]
    return ReturnToSupplierOut(
        id=doc.id,
        doc_number=doc.doc_number,
        doc_date=doc.doc_date,
        status=doc.status,
        provider_id=doc.provider_id,
        provider_name=provider.name if provider else None,
        supplier_receipt_id=doc.supplier_receipt_id,
        warehouse_id=doc.warehouse_id,
        warehouse_name=warehouse.name if warehouse else None,
        diadoc_outgoing_document_id=doc.diadoc_outgoing_document_id,
        diadoc_outgoing_status=(
            diadoc_outgoing.status if diadoc_outgoing else None
        ),
        reason=doc.reason,
        notes=doc.notes,
        external_id=doc.external_id,
        sync_status=doc.sync_status,
        created_at=doc.created_at,
        approved_at=doc.approved_at,
        shipped_at=doc.shipped_at,
        confirmed_at=doc.confirmed_at,
        rejected_at=doc.rejected_at,
        items=items,
        total_quantity=sum(
            int(item.quantity or 0) for item in doc.items or []
        ),
    )


def _supplier_return_to_list_item(
    doc: ReturnToSupplier,
) -> ReturnToSupplierListItem:
    provider = getattr(doc, "provider", None)
    warehouse = getattr(doc, "warehouse", None)
    diadoc_outgoing = getattr(doc, "diadoc_outgoing_document", None)
    return ReturnToSupplierListItem(
        id=doc.id,
        doc_number=doc.doc_number,
        doc_date=doc.doc_date,
        status=doc.status,
        provider_id=doc.provider_id,
        provider_name=provider.name if provider else None,
        supplier_receipt_id=doc.supplier_receipt_id,
        warehouse_id=doc.warehouse_id,
        warehouse_name=warehouse.name if warehouse else None,
        reason=doc.reason,
        item_count=len(doc.items or []),
        total_quantity=sum(
            int(item.quantity or 0) for item in doc.items or []
        ),
        diadoc_outgoing_document_id=doc.diadoc_outgoing_document_id,
        diadoc_outgoing_status=(
            diadoc_outgoing.status if diadoc_outgoing else None
        ),
        sync_status=doc.sync_status,
        created_at=doc.created_at,
        approved_at=doc.approved_at,
        shipped_at=doc.shipped_at,
        confirmed_at=doc.confirmed_at,
        rejected_at=doc.rejected_at,
    )


async def _load_customer_return_doc(
    session: AsyncSession,
    doc_id: int,
) -> ReturnFromCustomer | None:
    result = await session.execute(
        select(ReturnFromCustomer)
        .where(ReturnFromCustomer.id == doc_id)
        .options(
            selectinload(ReturnFromCustomer.customer),
            selectinload(ReturnFromCustomer.warehouse),
            selectinload(ReturnFromCustomer.shipment_document),
            selectinload(ReturnFromCustomer.items)
            .selectinload(ReturnItem.autopart)
            .selectinload(AutoPart.brand),
            selectinload(ReturnFromCustomer.items).selectinload(
                ReturnItem.storage_location
            ),
            selectinload(ReturnFromCustomer.items).selectinload(
                ReturnItem.lot
            ),
        )
    )
    return result.scalar_one_or_none()


async def _load_supplier_return_doc(
    session: AsyncSession,
    doc_id: int,
) -> ReturnToSupplier | None:
    result = await session.execute(
        select(ReturnToSupplier)
        .where(ReturnToSupplier.id == doc_id)
        .options(
            selectinload(ReturnToSupplier.provider),
            selectinload(ReturnToSupplier.warehouse),
            selectinload(ReturnToSupplier.supplier_receipt),
            selectinload(ReturnToSupplier.items)
            .selectinload(ReturnItem.autopart)
            .selectinload(AutoPart.brand),
            selectinload(ReturnToSupplier.items).selectinload(
                ReturnItem.storage_location
            ),
            selectinload(ReturnToSupplier.items).selectinload(ReturnItem.lot),
        )
    )
    return result.scalar_one_or_none()


async def _populate_return_item_from_payload(
    session: AsyncSession,
    item: ReturnItem,
    payload: ReturnItemCreate | ReturnItemUpdate,
) -> None:
    explicit = payload.model_dump(exclude_unset=True)

    shipment_item_id = explicit.get("shipment_item_id")
    if shipment_item_id is not None:
        shipment_item = (
            await session.execute(
                select(ShipmentDocumentItem)
                .where(ShipmentDocumentItem.id == shipment_item_id)
                .options(
                    selectinload(ShipmentDocumentItem.autopart).selectinload(
                        AutoPart.brand
                    ),
                    selectinload(ShipmentDocumentItem.lot),
                )
            )
        ).scalar_one_or_none()
        if shipment_item is None:
            raise HTTPException(
                status_code=404,
                detail="Строка отгрузки для возврата не найдена",
            )
        item.shipment_item_id = shipment_item.id
        item.autopart_id = shipment_item.autopart_id
        if "storage_location_id" in explicit:
            item.storage_location_id = explicit.get("storage_location_id")
        item.price = explicit.get("price", shipment_item.price)
        item.oem_number = (
            getattr(
                getattr(shipment_item, "autopart", None), "oem_number", None
            )
            or item.oem_number
        )
        item.brand_name = (
            getattr(
                getattr(
                    getattr(shipment_item, "autopart", None), "brand", None
                ),
                "name",
                None,
            )
            or item.brand_name
        )
        item.autopart_name = (
            getattr(getattr(shipment_item, "autopart", None), "name", None)
            or item.autopart_name
        )
        source_lot = getattr(shipment_item, "lot", None)
        if source_lot is not None:
            item.gtd_number = item.gtd_number or source_lot.gtd_number
            item.country_code = item.country_code or source_lot.country_code
            item.country_name = item.country_name or source_lot.country_name

    supplier_receipt_item_id = explicit.get("supplier_receipt_item_id")
    if supplier_receipt_item_id is not None:
        source_item = (
            await session.execute(
                select(SupplierReceiptItem)
                .where(SupplierReceiptItem.id == supplier_receipt_item_id)
                .options(selectinload(SupplierReceiptItem.autopart))
            )
        ).scalar_one_or_none()
        if source_item is None:
            raise HTTPException(
                status_code=404,
                detail="Строка поступления для возврата не найдена",
            )
        item.supplier_receipt_item_id = source_item.id
        item.autopart_id = source_item.autopart_id
        item.price = explicit.get("price", source_item.price)
        item.gtd_number = source_item.gtd_code
        item.country_code = source_item.country_code
        item.country_name = source_item.country_name
        item.oem_number = source_item.oem_number
        item.brand_name = source_item.brand_name
        item.autopart_name = source_item.autopart_name
        item.customer_order_item_id = source_item.customer_order_item_id
        item.supplier_order_item_id = source_item.supplier_order_item_id
        item.order_item_id = source_item.order_item_id

        if explicit.get("lot_id") is None:
            lot_stmt = (
                select(StockLot.id)
                .where(
                    StockLot.source_receipt_item_id == source_item.id,
                    StockLot.remaining_quantity > 0,
                )
                .order_by(StockLot.received_at.asc(), StockLot.id.asc())
                .limit(1)
            )
            item.lot_id = (
                await session.execute(lot_stmt)
            ).scalar_one_or_none()

    for field, value in explicit.items():
        setattr(item, field, value)

    if item.autopart_id is None:
        raise HTTPException(
            status_code=400,
            detail="Не удалось определить запчасть для строки возврата",
        )


@router.get(
    "/customer-returns/",
    response_model=List[ReturnFromCustomerListItem],
    summary="Список возвратов от клиентов",
)
async def list_customer_returns(
    status_filter: Optional[ReturnDocumentStatus] = Query(
        None,
        alias="status",
    ),
    customer_id: Optional[int] = Query(None),
    shipment_document_id: Optional[int] = Query(None),
    warehouse_id: Optional[int] = Query(None),
    offset: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=500),
    session: AsyncSession = Depends(get_session),
):
    stmt = (
        select(ReturnFromCustomer)
        .options(
            selectinload(ReturnFromCustomer.items),
            selectinload(ReturnFromCustomer.customer),
            selectinload(ReturnFromCustomer.warehouse),
        )
        .order_by(
            ReturnFromCustomer.doc_date.desc(), ReturnFromCustomer.id.desc()
        )
        .offset(offset)
        .limit(limit)
    )
    if status_filter is not None:
        stmt = stmt.where(ReturnFromCustomer.status == status_filter)
    if customer_id is not None:
        stmt = stmt.where(ReturnFromCustomer.customer_id == customer_id)
    if shipment_document_id is not None:
        stmt = stmt.where(
            ReturnFromCustomer.shipment_document_id == shipment_document_id
        )
    if warehouse_id is not None:
        stmt = stmt.where(ReturnFromCustomer.warehouse_id == warehouse_id)
    rows = (await session.execute(stmt)).scalars().all()
    return [_customer_return_to_list_item(row) for row in rows]


@router.post(
    "/customer-returns/",
    response_model=ReturnFromCustomerOut,
    status_code=status.HTTP_201_CREATED,
    summary="Создать возврат от клиента",
)
async def create_customer_return(
    data: ReturnFromCustomerCreate,
    session: AsyncSession = Depends(get_session),
):
    doc = ReturnFromCustomer(
        doc_number=data.doc_number,
        doc_date=data.doc_date or now_moscow(),
        customer_id=data.customer_id,
        shipment_document_id=data.shipment_document_id,
        warehouse_id=data.warehouse_id,
        reason=data.reason,
        notes=data.notes,
        external_id=data.external_id,
        status=ReturnDocumentStatus.CREATED,
    )
    if doc.shipment_document_id:
        shipment = await session.get(
            ShipmentDocument, doc.shipment_document_id
        )
        if shipment is not None:
            if doc.customer_id is None:
                doc.customer_id = shipment.customer_id
            if doc.warehouse_id is None:
                doc.warehouse_id = shipment.warehouse_id
    session.add(doc)
    await session.flush()

    for item_payload in data.items:
        item = ReturnItem(
            return_from_customer_id=doc.id,
            quantity=item_payload.quantity,
            notes=item_payload.notes,
        )
        session.add(item)
        await session.flush()
        await _populate_return_item_from_payload(session, item, item_payload)

    await session.flush()
    doc = await _load_customer_return_doc(session, doc.id)
    await session.commit()
    return _customer_return_to_out(doc)


@router.get(
    "/customer-returns/{doc_id}/",
    response_model=ReturnFromCustomerOut,
    summary="Возврат от клиента по ID",
)
async def get_customer_return(
    doc_id: int,
    session: AsyncSession = Depends(get_session),
):
    doc = await _load_customer_return_doc(session, doc_id)
    if doc is None:
        raise HTTPException(status_code=404, detail="Возврат не найден")
    return _customer_return_to_out(doc)


@router.patch(
    "/customer-returns/{doc_id}/",
    response_model=ReturnFromCustomerOut,
    summary="Обновить возврат от клиента",
)
async def update_customer_return(
    doc_id: int,
    data: ReturnFromCustomerUpdate,
    session: AsyncSession = Depends(get_session),
):
    doc = await session.get(ReturnFromCustomer, doc_id)
    if doc is None:
        raise HTTPException(status_code=404, detail="Возврат не найден")
    if doc.status != ReturnDocumentStatus.CREATED:
        raise HTTPException(
            status_code=400,
            detail="Редактировать можно только возврат в статусе CREATED",
        )
    for field, value in data.model_dump(exclude_unset=True).items():
        setattr(doc, field, value)
    await session.flush()
    doc = await _load_customer_return_doc(session, doc_id)
    await session.commit()
    return _customer_return_to_out(doc)


@router.post(
    "/customer-returns/{doc_id}/items/",
    response_model=ReturnItemOut,
    status_code=status.HTTP_201_CREATED,
    summary="Добавить строку в возврат от клиента",
)
async def add_customer_return_item(
    doc_id: int,
    data: ReturnItemCreate,
    session: AsyncSession = Depends(get_session),
):
    doc = await session.get(ReturnFromCustomer, doc_id)
    if doc is None:
        raise HTTPException(status_code=404, detail="Возврат не найден")
    if doc.status != ReturnDocumentStatus.CREATED:
        raise HTTPException(
            status_code=400,
            detail="Добавлять строки можно только в CREATED",
        )
    item = ReturnItem(
        return_from_customer_id=doc_id,
        quantity=data.quantity,
        notes=data.notes,
    )
    session.add(item)
    await session.flush()
    await _populate_return_item_from_payload(session, item, data)
    await session.flush()
    result = await session.execute(
        select(ReturnItem)
        .where(ReturnItem.id == item.id)
        .options(
            selectinload(ReturnItem.autopart).selectinload(AutoPart.brand),
            selectinload(ReturnItem.storage_location),
            selectinload(ReturnItem.lot),
        )
    )
    item = result.scalar_one()
    await session.commit()
    return _return_item_to_out(item)


@router.patch(
    "/customer-returns/{doc_id}/items/{item_id}/",
    response_model=ReturnItemOut,
    summary="Обновить строку возврата от клиента",
)
async def update_customer_return_item(
    doc_id: int,
    item_id: int,
    data: ReturnItemUpdate,
    session: AsyncSession = Depends(get_session),
):
    doc = await session.get(ReturnFromCustomer, doc_id)
    if doc is None:
        raise HTTPException(status_code=404, detail="Возврат не найден")
    if doc.status != ReturnDocumentStatus.CREATED:
        raise HTTPException(
            status_code=400,
            detail="Редактировать строки можно только в CREATED",
        )
    item = await session.get(ReturnItem, item_id)
    if item is None or item.return_from_customer_id != doc_id:
        raise HTTPException(status_code=404, detail="Строка не найдена")
    await _populate_return_item_from_payload(session, item, data)
    await session.flush()
    result = await session.execute(
        select(ReturnItem)
        .where(ReturnItem.id == item.id)
        .options(
            selectinload(ReturnItem.autopart).selectinload(AutoPart.brand),
            selectinload(ReturnItem.storage_location),
            selectinload(ReturnItem.lot),
        )
    )
    item = result.scalar_one()
    await session.commit()
    return _return_item_to_out(item)


@router.delete(
    "/customer-returns/{doc_id}/items/{item_id}/",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Удалить строку возврата от клиента",
)
async def delete_customer_return_item(
    doc_id: int,
    item_id: int,
    session: AsyncSession = Depends(get_session),
):
    doc = await session.get(ReturnFromCustomer, doc_id)
    if doc is None:
        raise HTTPException(status_code=404, detail="Возврат не найден")
    if doc.status != ReturnDocumentStatus.CREATED:
        raise HTTPException(
            status_code=400,
            detail="Удалять строки можно только в CREATED",
        )
    item = await session.get(ReturnItem, item_id)
    if item is None or item.return_from_customer_id != doc_id:
        raise HTTPException(status_code=404, detail="Строка не найдена")
    await session.delete(item)
    await session.commit()


@router.post(
    "/customer-returns/{doc_id}/approve/",
    response_model=ReturnFromCustomerOut,
    summary="Согласовать возврат от клиента",
)
async def approve_customer_return(
    doc_id: int,
    session: AsyncSession = Depends(get_session),
):
    try:
        await approve_return_from_customer(session, doc_id=doc_id)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    doc = await _load_customer_return_doc(session, doc_id)
    await session.commit()
    return _customer_return_to_out(doc)


@router.post(
    "/customer-returns/{doc_id}/ship/",
    response_model=ReturnFromCustomerOut,
    summary="Перевести возврат от клиента в SHIPPED",
)
async def ship_customer_return(
    doc_id: int,
    session: AsyncSession = Depends(get_session),
):
    try:
        await ship_return_from_customer(session, doc_id=doc_id)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    doc = await _load_customer_return_doc(session, doc_id)
    await session.commit()
    return _customer_return_to_out(doc)


@router.post(
    "/customer-returns/{doc_id}/confirm/",
    response_model=ReturnFromCustomerOut,
    summary="Подтвердить приёмку возврата от клиента",
)
async def confirm_customer_return(
    doc_id: int,
    session: AsyncSession = Depends(get_session),
):
    try:
        await confirm_return_from_customer(session, doc_id=doc_id)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    doc = await _load_customer_return_doc(session, doc_id)
    await session.commit()
    return _customer_return_to_out(doc)


@router.post(
    "/customer-returns/{doc_id}/reject/",
    response_model=ReturnFromCustomerOut,
    summary="Отклонить возврат от клиента",
)
async def reject_customer_return(
    doc_id: int,
    session: AsyncSession = Depends(get_session),
):
    try:
        await reject_return_from_customer(session, doc_id=doc_id)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    doc = await _load_customer_return_doc(session, doc_id)
    await session.commit()
    return _customer_return_to_out(doc)


@router.delete(
    "/customer-returns/{doc_id}/",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Удалить возврат от клиента",
)
async def delete_customer_return(
    doc_id: int,
    session: AsyncSession = Depends(get_session),
):
    doc = await session.get(ReturnFromCustomer, doc_id)
    if doc is None:
        raise HTTPException(status_code=404, detail="Возврат не найден")
    if doc.status not in {
        ReturnDocumentStatus.CREATED,
        ReturnDocumentStatus.REJECTED,
    }:
        raise HTTPException(
            status_code=400,
            detail="Удалять можно только CREATED или REJECTED возврат",
        )
    await session.delete(doc)
    await session.commit()


@router.get(
    "/supplier-returns/",
    response_model=List[ReturnToSupplierListItem],
    summary="Список возвратов поставщикам",
)
async def list_supplier_returns(
    status_filter: Optional[ReturnDocumentStatus] = Query(
        None,
        alias="status",
    ),
    provider_id: Optional[int] = Query(None),
    supplier_receipt_id: Optional[int] = Query(None),
    warehouse_id: Optional[int] = Query(None),
    offset: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=500),
    session: AsyncSession = Depends(get_session),
):
    stmt = (
        select(ReturnToSupplier)
        .options(
            selectinload(ReturnToSupplier.items),
            selectinload(ReturnToSupplier.provider),
            selectinload(ReturnToSupplier.warehouse),
        )
        .order_by(ReturnToSupplier.doc_date.desc(), ReturnToSupplier.id.desc())
        .offset(offset)
        .limit(limit)
    )
    if status_filter is not None:
        stmt = stmt.where(ReturnToSupplier.status == status_filter)
    if provider_id is not None:
        stmt = stmt.where(ReturnToSupplier.provider_id == provider_id)
    if supplier_receipt_id is not None:
        stmt = stmt.where(
            ReturnToSupplier.supplier_receipt_id == supplier_receipt_id
        )
    if warehouse_id is not None:
        stmt = stmt.where(ReturnToSupplier.warehouse_id == warehouse_id)
    rows = (await session.execute(stmt)).scalars().all()
    return [_supplier_return_to_list_item(row) for row in rows]


@router.post(
    "/supplier-returns/",
    response_model=ReturnToSupplierOut,
    status_code=status.HTTP_201_CREATED,
    summary="Создать возврат поставщику",
)
async def create_supplier_return(
    data: ReturnToSupplierCreate,
    session: AsyncSession = Depends(get_session),
):
    doc = ReturnToSupplier(
        doc_number=data.doc_number,
        doc_date=data.doc_date or now_moscow(),
        provider_id=data.provider_id,
        supplier_receipt_id=data.supplier_receipt_id,
        warehouse_id=data.warehouse_id,
        reason=data.reason,
        notes=data.notes,
        external_id=data.external_id,
        status=ReturnDocumentStatus.CREATED,
    )
    if doc.supplier_receipt_id:
        receipt = await session.get(SupplierReceipt, doc.supplier_receipt_id)
        if receipt is not None:
            if doc.provider_id is None:
                doc.provider_id = getattr(receipt, "provider_id", None)
            if doc.warehouse_id is None:
                doc.warehouse_id = getattr(receipt, "warehouse_id", None)
    session.add(doc)
    await session.flush()

    for item_payload in data.items:
        item = ReturnItem(
            return_to_supplier_id=doc.id,
            quantity=item_payload.quantity,
            notes=item_payload.notes,
        )
        session.add(item)
        await session.flush()
        await _populate_return_item_from_payload(session, item, item_payload)

    await session.flush()
    doc = await _load_supplier_return_doc(session, doc.id)
    await session.commit()
    return _supplier_return_to_out(doc)


@router.get(
    "/supplier-returns/{doc_id}/",
    response_model=ReturnToSupplierOut,
    summary="Возврат поставщику по ID",
)
async def get_supplier_return(
    doc_id: int,
    session: AsyncSession = Depends(get_session),
):
    doc = await _load_supplier_return_doc(session, doc_id)
    if doc is None:
        raise HTTPException(status_code=404, detail="Возврат не найден")
    return _supplier_return_to_out(doc)


@router.patch(
    "/supplier-returns/{doc_id}/",
    response_model=ReturnToSupplierOut,
    summary="Обновить возврат поставщику",
)
async def update_supplier_return(
    doc_id: int,
    data: ReturnToSupplierUpdate,
    session: AsyncSession = Depends(get_session),
):
    doc = await session.get(ReturnToSupplier, doc_id)
    if doc is None:
        raise HTTPException(status_code=404, detail="Возврат не найден")
    if doc.status != ReturnDocumentStatus.CREATED:
        raise HTTPException(
            status_code=400,
            detail="Редактировать можно только возврат в статусе CREATED",
        )
    for field, value in data.model_dump(exclude_unset=True).items():
        setattr(doc, field, value)
    await session.flush()
    doc = await _load_supplier_return_doc(session, doc_id)
    await session.commit()
    return _supplier_return_to_out(doc)


@router.post(
    "/supplier-returns/{doc_id}/items/",
    response_model=ReturnItemOut,
    status_code=status.HTTP_201_CREATED,
    summary="Добавить строку в возврат поставщику",
)
async def add_supplier_return_item(
    doc_id: int,
    data: ReturnItemCreate,
    session: AsyncSession = Depends(get_session),
):
    doc = await session.get(ReturnToSupplier, doc_id)
    if doc is None:
        raise HTTPException(status_code=404, detail="Возврат не найден")
    if doc.status != ReturnDocumentStatus.CREATED:
        raise HTTPException(
            status_code=400,
            detail="Добавлять строки можно только в CREATED",
        )
    item = ReturnItem(
        return_to_supplier_id=doc_id,
        quantity=data.quantity,
        notes=data.notes,
    )
    session.add(item)
    await session.flush()
    await _populate_return_item_from_payload(session, item, data)
    await session.flush()
    result = await session.execute(
        select(ReturnItem)
        .where(ReturnItem.id == item.id)
        .options(
            selectinload(ReturnItem.autopart).selectinload(AutoPart.brand),
            selectinload(ReturnItem.storage_location),
            selectinload(ReturnItem.lot),
        )
    )
    item = result.scalar_one()
    await session.commit()
    return _return_item_to_out(item)


@router.patch(
    "/supplier-returns/{doc_id}/items/{item_id}/",
    response_model=ReturnItemOut,
    summary="Обновить строку возврата поставщику",
)
async def update_supplier_return_item(
    doc_id: int,
    item_id: int,
    data: ReturnItemUpdate,
    session: AsyncSession = Depends(get_session),
):
    doc = await session.get(ReturnToSupplier, doc_id)
    if doc is None:
        raise HTTPException(status_code=404, detail="Возврат не найден")
    if doc.status != ReturnDocumentStatus.CREATED:
        raise HTTPException(
            status_code=400,
            detail="Редактировать строки можно только в CREATED",
        )
    item = await session.get(ReturnItem, item_id)
    if item is None or item.return_to_supplier_id != doc_id:
        raise HTTPException(status_code=404, detail="Строка не найдена")
    await _populate_return_item_from_payload(session, item, data)
    await session.flush()
    result = await session.execute(
        select(ReturnItem)
        .where(ReturnItem.id == item.id)
        .options(
            selectinload(ReturnItem.autopart).selectinload(AutoPart.brand),
            selectinload(ReturnItem.storage_location),
            selectinload(ReturnItem.lot),
        )
    )
    item = result.scalar_one()
    await session.commit()
    return _return_item_to_out(item)


@router.delete(
    "/supplier-returns/{doc_id}/items/{item_id}/",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Удалить строку возврата поставщику",
)
async def delete_supplier_return_item(
    doc_id: int,
    item_id: int,
    session: AsyncSession = Depends(get_session),
):
    doc = await session.get(ReturnToSupplier, doc_id)
    if doc is None:
        raise HTTPException(status_code=404, detail="Возврат не найден")
    if doc.status != ReturnDocumentStatus.CREATED:
        raise HTTPException(
            status_code=400,
            detail="Удалять строки можно только в CREATED",
        )
    item = await session.get(ReturnItem, item_id)
    if item is None or item.return_to_supplier_id != doc_id:
        raise HTTPException(status_code=404, detail="Строка не найдена")
    await session.delete(item)
    await session.commit()


@router.post(
    "/supplier-returns/{doc_id}/approve/",
    response_model=ReturnToSupplierOut,
    summary="Согласовать возврат поставщику",
)
async def approve_supplier_return(
    doc_id: int,
    session: AsyncSession = Depends(get_session),
):
    try:
        await approve_return_to_supplier(session, doc_id=doc_id)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    doc = await _load_supplier_return_doc(session, doc_id)
    await session.commit()
    return _supplier_return_to_out(doc)


@router.post(
    "/supplier-returns/{doc_id}/ship/",
    response_model=ReturnToSupplierOut,
    summary="Отгрузить возврат поставщику",
)
async def ship_supplier_return(
    doc_id: int,
    session: AsyncSession = Depends(get_session),
):
    try:
        await ship_return_to_supplier(session, doc_id=doc_id)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    doc = await _load_supplier_return_doc(session, doc_id)
    await session.commit()
    return _supplier_return_to_out(doc)


@router.post(
    "/supplier-returns/{doc_id}/confirm/",
    response_model=ReturnToSupplierOut,
    summary="Подтвердить возврат поставщику",
)
async def confirm_supplier_return(
    doc_id: int,
    session: AsyncSession = Depends(get_session),
):
    try:
        await confirm_return_to_supplier(session, doc_id=doc_id)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    doc = await _load_supplier_return_doc(session, doc_id)
    await session.commit()
    return _supplier_return_to_out(doc)


@router.post(
    "/supplier-returns/{doc_id}/reject/",
    response_model=ReturnToSupplierOut,
    summary="Отклонить возврат поставщику",
)
async def reject_supplier_return(
    doc_id: int,
    session: AsyncSession = Depends(get_session),
):
    try:
        await reject_return_to_supplier(session, doc_id=doc_id)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    doc = await _load_supplier_return_doc(session, doc_id)
    await session.commit()
    return _supplier_return_to_out(doc)


@router.delete(
    "/supplier-returns/{doc_id}/",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Удалить возврат поставщику",
)
async def delete_supplier_return(
    doc_id: int,
    session: AsyncSession = Depends(get_session),
):
    doc = await session.get(ReturnToSupplier, doc_id)
    if doc is None:
        raise HTTPException(status_code=404, detail="Возврат не найден")
    if doc.status not in {
        ReturnDocumentStatus.CREATED,
        ReturnDocumentStatus.REJECTED,
    }:
        raise HTTPException(
            status_code=400,
            detail="Удалять можно только CREATED или REJECTED возврат",
        )
    await session.delete(doc)
    await session.commit()


# ─── 1С Sync — ShipmentDocument ──────────────────────────────────────────────
# ВАЖНО: /shipments/export/ и /shipments/bulk-sync/ объявлены ДО /{doc_id}/
# чтобы FastAPI не пытался разобрать "export" как int.


@router.get(
    "/shipments/export/",
    response_model=ShipmentsExportOut,
    summary="Экспорт накладных для 1С (статус pending)",
)
async def export_shipments_for_1c(
    limit: int = Query(default=200, ge=1, le=1000),
    sync_status: SyncStatus = Query(default=SyncStatus.PENDING),
    session: AsyncSession = Depends(get_session),
):
    """Возвращает накладные с заданным sync_status для передачи в 1С.
    После обработки вызывайте `POST /shipments/bulk-sync/`.
    """
    stmt = (
        select(ShipmentDocument)
        .where(ShipmentDocument.sync_status == sync_status)
        .options(
            selectinload(ShipmentDocument.items),
            selectinload(ShipmentDocument.customer),
            selectinload(ShipmentDocument.warehouse),
        )
        .order_by(ShipmentDocument.id)
        .limit(limit)
    )
    rows = (await session.execute(stmt)).scalars().all()
    items = [_shipment_to_list_item(r) for r in rows]
    return ShipmentsExportOut(total=len(items), items=items)


@router.patch(
    "/shipments/{doc_id}/sync/",
    response_model=ShipmentDocumentListItem,
    summary="Обновить sync_status накладной (из 1С)",
)
async def sync_shipment_document(
    doc_id: int,
    body: ShipmentSyncUpdate,
    session: AsyncSession = Depends(get_session),
):
    doc = await session.get(ShipmentDocument, doc_id)
    if doc is None:
        raise HTTPException(status_code=404, detail="Накладная не найдена")
    doc.sync_status = body.sync_status
    if body.external_id is not None:
        doc.external_id = body.external_id
    await session.commit()
    await session.refresh(doc)
    stmt = (
        select(ShipmentDocument)
        .where(ShipmentDocument.id == doc_id)
        .options(
            selectinload(ShipmentDocument.items),
            selectinload(ShipmentDocument.customer),
            selectinload(ShipmentDocument.warehouse),
        )
    )
    doc = (await session.execute(stmt)).scalar_one()
    return _shipment_to_list_item(doc)


@router.post(
    "/shipments/bulk-sync/",
    response_model=ShipmentBulkSyncResult,
    summary="Массовое обновление sync_status накладных (подтверждение из 1С)",
)
async def bulk_sync_shipments(
    body: ShipmentBulkSyncRequest,
    session: AsyncSession = Depends(get_session),
):
    ids = [i.shipment_id for i in body.items]
    existing = (
        (
            await session.execute(
                select(ShipmentDocument.id).where(ShipmentDocument.id.in_(ids))
            )
        )
        .scalars()
        .all()
    )
    existing_set = set(existing)

    updated = 0
    errors = []
    for item in body.items:
        if item.shipment_id not in existing_set:
            errors.append(item.shipment_id)
            continue
        stmt = (
            update(ShipmentDocument)
            .where(ShipmentDocument.id == item.shipment_id)
            .values(
                sync_status=item.sync_status,
                **(
                    {"external_id": item.external_id}
                    if item.external_id
                    else {}
                ),
            )
        )
        await session.execute(stmt)
        updated += 1
    await session.commit()
    return ShipmentBulkSyncResult(updated=updated, errors=errors)


# ─── 1С Sync — StockDocument ──────────────────────────────────────────────────
# ВАЖНО: /documents/export/ и /documents/bulk-sync/ уже стоят ПОСЛЕ
# параметрических /{doc_id}/ для StockDocument — поэтому используем
# вспомогательные пути без конфликта с int.
# Маршруты /documents/export-1c/ и /documents/bulk-sync-1c/ выбраны
# чтобы не пересекаться с существующими /{doc_id}/ роутами (там уже есть
# GET /documents/{id} и другие). Если переупорядочить — можно убрать суффикс.


@router.get(
    "/documents/export-1c/",
    response_model=DocumentsExportOut,
    summary="Экспорт документов оприходования/списания для 1С",
)
async def export_documents_for_1c(
    limit: int = Query(default=200, ge=1, le=1000),
    sync_status: SyncStatus = Query(default=SyncStatus.PENDING),
    doc_type: Optional[StockDocumentType] = Query(default=None),
    session: AsyncSession = Depends(get_session),
):
    """Возвращает StockDocument-ы с заданным sync_status."""
    stmt = (
        select(StockDocument)
        .where(StockDocument.sync_status == sync_status)
        .options(
            selectinload(StockDocument.items),
            selectinload(StockDocument.warehouse),
        )
        .order_by(StockDocument.id)
        .limit(limit)
    )
    if doc_type is not None:
        stmt = stmt.where(StockDocument.doc_type == doc_type)
    rows = (await session.execute(stmt)).scalars().all()
    items = [_doc_to_list_item(r) for r in rows]
    return DocumentsExportOut(total=len(items), items=items)


@router.patch(
    "/documents/{doc_id}/sync/",
    response_model=StockDocumentListItem,
    summary="Обновить sync_status документа (из 1С)",
)
async def sync_stock_document(
    doc_id: int,
    body: DocumentSyncUpdate,
    session: AsyncSession = Depends(get_session),
):
    doc = await session.get(StockDocument, doc_id)
    if doc is None:
        raise HTTPException(status_code=404, detail="Документ не найден")
    doc.sync_status = body.sync_status
    if body.external_id is not None:
        doc.external_id = body.external_id
    await session.commit()
    await session.refresh(doc)
    stmt = (
        select(StockDocument)
        .where(StockDocument.id == doc_id)
        .options(
            selectinload(StockDocument.items),
            selectinload(StockDocument.warehouse),
        )
    )
    doc = (await session.execute(stmt)).scalar_one()
    return _doc_to_list_item(doc)


@router.post(
    "/documents/bulk-sync-1c/",
    response_model=DocumentBulkSyncResult,
    summary="Массовое обновление sync_status документов (подтверждение из 1С)",
)
async def bulk_sync_documents(
    body: DocumentBulkSyncRequest,
    session: AsyncSession = Depends(get_session),
):
    ids = [i.document_id for i in body.items]
    existing = (
        (
            await session.execute(
                select(StockDocument.id).where(StockDocument.id.in_(ids))
            )
        )
        .scalars()
        .all()
    )
    existing_set = set(existing)

    updated = 0
    errors = []
    for item in body.items:
        if item.document_id not in existing_set:
            errors.append(item.document_id)
            continue
        stmt = (
            update(StockDocument)
            .where(StockDocument.id == item.document_id)
            .values(
                sync_status=item.sync_status,
                **(
                    {"external_id": item.external_id}
                    if item.external_id
                    else {}
                ),
            )
        )
        await session.execute(stmt)
        updated += 1
    await session.commit()
    return DocumentBulkSyncResult(updated=updated, errors=errors)
