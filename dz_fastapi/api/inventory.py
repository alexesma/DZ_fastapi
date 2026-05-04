"""
Inventory API
─────────────
Endpoints for managing StockByLocation, InventorySession, InventoryItem,
StockMovement, and transfers between storage locations.
"""
import logging
from typing import List, Optional

from fastapi import APIRouter, Body, Depends, HTTPException, Query, status
from sqlalchemy import delete, func, insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.orm import selectinload

from dz_fastapi.core.db import get_session
from dz_fastapi.core.time import now_moscow
from dz_fastapi.models.autopart import (AutoPart, StorageLocation,
                                        autopart_storage_association)
from dz_fastapi.models.inventory import (InventoryItem, InventorySession,
                                         InventoryStatus, LotSourceType,
                                         MovementType, StockByLocation,
                                         StockDocument, StockDocumentItem,
                                         StockDocumentStatus, StockLot,
                                         StockMovement)
from dz_fastapi.schemas.inventory import (BackfillResult,
                                          InventoryItemCountUpdate,
                                          InventoryItemOut,
                                          InventorySessionCreate,
                                          InventorySessionListItem,
                                          InventorySessionOut,
                                          InventorySessionUpdate,
                                          StockByLocationOut,
                                          StockByLocationUpsert,
                                          StockDocumentCreate,
                                          StockDocumentItemCreate,
                                          StockDocumentItemOut,
                                          StockDocumentItemUpdate,
                                          StockDocumentListItem,
                                          StockDocumentOut,
                                          StockDocumentUpdate, StockLotOut,
                                          StockMovementCreate,
                                          StockMovementOut, TransferRequest,
                                          TransferResult)
from dz_fastapi.services.inventory_stock import \
    _apply_stock_delta as apply_stock_delta
from dz_fastapi.services.inventory_stock import \
    _consume_fifo as consume_stock_fifo
from dz_fastapi.services.inventory_stock import (backfill_opening_balance_lots,
                                                 get_lots_for_autopart,
                                                 post_stock_document,
                                                 reconcile_stock_absolute,
                                                 transfer_stock_with_lot_trace,
                                                 unpost_stock_document)

logger = logging.getLogger(__name__)
router = APIRouter(prefix='/inventory', tags=['inventory'])


# ─── helpers ───────────────────────────────────────────────────────────────

def _sbl_to_out(sbl: StockByLocation) -> StockByLocationOut:
    ap = sbl.autopart
    loc = sbl.storage_location
    return StockByLocationOut(
        id=sbl.id,
        autopart_id=sbl.autopart_id,
        storage_location_id=sbl.storage_location_id,
        quantity=sbl.quantity,
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
        assoc_exists = (await session.execute(
            select(autopart_storage_association).where(
                autopart_storage_association.c.autopart_id == autopart_id,
                autopart_storage_association.c.storage_location_id
                == storage_location_id,
            )
        )).first()
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
    lot = getattr(m, 'stock_lot', None)
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
        autopart_oem=m.autopart.oem_number if m.autopart else None,
        autopart_name=m.autopart.name if m.autopart else None,
        storage_location_name=(
            m.storage_location.name if m.storage_location else None
        ),
    )


# ─── StockByLocation endpoints ─────────────────────────────────────────────


@router.get(
    '/stock/',
    response_model=List[StockByLocationOut],
    summary='Остатки по ячейкам',
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
        StockByLocation.storage_location_id,
        StockByLocation.autopart_id
    )
    rows = (await session.execute(stmt)).scalars().all()
    return [_sbl_to_out(r) for r in rows]


@router.put(
    '/stock/',
    response_model=StockByLocationOut,
    status_code=status.HTTP_200_OK,
    summary='Установить остаток запчасти в ячейке (upsert)',
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
        raise HTTPException(status_code=404, detail='Запчасть не найдена')
    if not (await session.get(StorageLocation, data.storage_location_id)):
        raise HTTPException(
            status_code=404,
            detail='Место хранения не найдено'
        )

    await reconcile_stock_absolute(
        session,
        autopart_id=data.autopart_id,
        storage_location_id=data.storage_location_id,
        target_quantity=data.quantity,
        notes='Ручная установка остатка через PUT /inventory/stock',
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
    '/stock/{sbl_id}/',
    status_code=status.HTTP_204_NO_CONTENT,
    summary='Удалить запись остатка (убрать запчасть из ячейки)',
)
async def delete_stock_by_location(
    sbl_id: int,
    remove_location_link: bool = Query(
        True,
        description='Также убрать запись из autopart_storage_association',
    ),
    session: AsyncSession = Depends(get_session),
):
    sbl = await session.get(StockByLocation, sbl_id)
    if not sbl:
        raise HTTPException(status_code=404, detail='Запись не найдена')

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
    '/sessions/',
    status_code=status.HTTP_201_CREATED,
    response_model=InventorySessionOut,
    summary='Начать новый сеанс инвентаризации',
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
    if data.scope_type.value == 'shelf' and data.scope_value:
        prefix = data.scope_value.upper()
        loc_stmt = loc_stmt.where(StorageLocation.name.like(f'{prefix}%'))
    elif data.scope_type.value == 'location' and data.scope_value:
        loc_stmt = loc_stmt.where(
            StorageLocation.name == data.scope_value.upper()
        )
    loc_ids = [r.id for r in (
        await session.execute(loc_stmt)
    ).scalars().all()]

    # ── Get StockByLocation rows for these locations (non-zero only) ────────
    if loc_ids:
        sbl_rows = (await session.execute(
            select(StockByLocation).where(
                StockByLocation.storage_location_id.in_(loc_ids),
                StockByLocation.quantity != 0,
            )
        )).scalars().all()
    else:
        sbl_rows = []

    # ── Build InventoryItems ────────────────────────────────────────────────
    seen: set[tuple[int, int]] = set()
    for sbl in sbl_rows:
        key = (sbl.autopart_id, sbl.storage_location_id)
        if key in seen:
            continue
        seen.add(key)
        session.add(InventoryItem(
            session_id=inv_session.id,
            autopart_id=sbl.autopart_id,
            storage_location_id=sbl.storage_location_id,
            expected_qty=sbl.quantity,
        ))

    await session.commit()

    # ── Reload with relationships ───────────────────────────────────────────
    result = await session.execute(
        select(InventorySession)
        .where(InventorySession.id == inv_session.id)
        .options(
            selectinload(InventorySession.items)
            .selectinload(InventoryItem.autopart)
            .selectinload(AutoPart.brand),
            selectinload(InventorySession.items)
            .selectinload(InventoryItem.storage_location),
        )
    )
    inv_session = result.scalar_one()
    out_items = [_item_to_out(it) for it in inv_session.items]
    return InventorySessionOut(
        id=inv_session.id, name=inv_session.name,
        started_at=inv_session.started_at,
        finished_at=inv_session.finished_at,
        status=inv_session.status, scope_type=inv_session.scope_type,
        scope_value=inv_session.scope_value, notes=inv_session.notes,
        items=out_items,
    )


@router.get(
    '/sessions/',
    response_model=List[InventorySessionListItem],
    summary='Список сеансов инвентаризации',
)
async def list_inventory_sessions(
    status_filter: Optional[InventoryStatus] = Query(None, alias='status'),
    offset: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=200),
    session: AsyncSession = Depends(get_session),
):
    stmt = select(InventorySession)
    if status_filter:
        stmt = stmt.where(InventorySession.status == status_filter)
    stmt = stmt.order_by(
        InventorySession.started_at.desc()
    ).offset(offset).limit(limit)
    sessions = (await session.execute(stmt)).scalars().all()

    counts: dict[int, tuple[int, int]] = {}
    if sessions:
        ids = [s.id for s in sessions]
        count_rows = (await session.execute(
            select(
                InventoryItem.session_id,
                func.count(InventoryItem.id).label('total'),
                func.count(InventoryItem.actual_qty).label('counted'),
            )
            .where(InventoryItem.session_id.in_(ids))
            .group_by(InventoryItem.session_id)
        )).all()
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
    '/sessions/{session_id}/',
    response_model=InventorySessionOut,
    summary='Сеанс инвентаризации (с позициями)',
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
            selectinload(InventorySession.items)
            .selectinload(InventoryItem.storage_location),
        )
    )
    inv_session = result.scalar_one_or_none()
    if not inv_session:
        raise HTTPException(status_code=404, detail='Сеанс не найден')

    return InventorySessionOut(
        id=inv_session.id, name=inv_session.name,
        started_at=inv_session.started_at,
        finished_at=inv_session.finished_at,
        status=inv_session.status, scope_type=inv_session.scope_type,
        scope_value=inv_session.scope_value, notes=inv_session.notes,
        items=[_item_to_out(it) for it in inv_session.items],
    )


@router.patch(
    '/sessions/{session_id}/',
    response_model=InventorySessionOut,
    summary='Обновить название/примечания сеанса',
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
        raise HTTPException(status_code=404, detail='Сеанс не найден')
    if inv_session.status != InventoryStatus.ACTIVE:
        raise HTTPException(
            status_code=400,
            detail='Можно редактировать только активный сеанс'
        )
    if data.name is not None:
        inv_session.name = data.name
    if data.notes is not None:
        inv_session.notes = data.notes
    await session.commit()
    return await get_inventory_session(session_id, session)


@router.patch(
    '/sessions/{session_id}/items/{item_id}/',
    response_model=InventoryItemOut,
    summary='Внести фактическое количество для позиции',
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
            InventoryItem.id == item_id,
            InventoryItem.session_id == session_id
        )
        .options(
            selectinload(
                InventoryItem.autopart
            ).selectinload(AutoPart.brand),
            selectinload(InventoryItem.storage_location),
        )
    )
    item = result.scalar_one_or_none()
    if not item:
        raise HTTPException(status_code=404, detail='Позиция не найдена')

    sess_status = (await session.execute(
        select(InventorySession.status).where(
            InventorySession.id == session_id
        )
    )).scalar_one_or_none()
    if sess_status != InventoryStatus.ACTIVE:
        raise HTTPException(
            status_code=400,
            detail='Сеанс завершён или отменён'
        )

    item.actual_qty = data.actual_qty
    item.discrepancy = data.actual_qty - item.expected_qty
    item.counted_at = now_moscow()
    await session.commit()
    return _item_to_out(item)


@router.post(
    '/sessions/{session_id}/complete/',
    response_model=InventorySessionOut,
    summary='Завершить инвентаризацию и обновить остатки по ячейкам',
)
async def complete_inventory_session(
    session_id: int,
    apply_adjustments: bool = Body(
        True, embed=True,
        description='Обновить StockByLocation '
                    'и создать StockMovement для расхождений',
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
            selectinload(InventorySession.items)
            .selectinload(InventoryItem.storage_location),
        )
    )
    inv_session = result.scalar_one_or_none()
    if not inv_session:
        raise HTTPException(status_code=404, detail='Сеанс не найден')
    if inv_session.status != InventoryStatus.ACTIVE:
        raise HTTPException(
            status_code=400,
            detail='Сеанс уже завершён или отменён'
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
                notes=f'Инвентаризация «{inv_session.name}»',
            )

    inv_session.status = InventoryStatus.COMPLETED
    inv_session.finished_at = now_moscow()
    await session.commit()

    return InventorySessionOut(
        id=inv_session.id, name=inv_session.name,
        started_at=inv_session.started_at,
        finished_at=inv_session.finished_at,
        status=inv_session.status, scope_type=inv_session.scope_type,
        scope_value=inv_session.scope_value, notes=inv_session.notes,
        items=[_item_to_out(it) for it in inv_session.items],
    )


@router.post(
    '/sessions/{session_id}/cancel/',
    response_model=InventorySessionListItem,
    summary='Отменить сеанс инвентаризации',
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
        raise HTTPException(status_code=404, detail='Сеанс не найден')
    if inv_session.status != InventoryStatus.ACTIVE:
        raise HTTPException(
            status_code=400,
            detail='Сеанс уже завершён или отменён'
        )
    inv_session.status = InventoryStatus.CANCELLED
    inv_session.finished_at = now_moscow()
    await session.commit()
    return InventorySessionListItem(
        id=inv_session.id, name=inv_session.name,
        started_at=inv_session.started_at,
        finished_at=inv_session.finished_at,
        status=inv_session.status, scope_type=inv_session.scope_type,
        scope_value=inv_session.scope_value, item_count=0, counted_count=0,
    )


# ─── StockMovement ──────────────────────────────────────────────────────────

@router.get(
    '/movements/',
    response_model=List[StockMovementOut],
    summary='История движений товара',
)
async def list_stock_movements(
    autopart_id: Optional[int] = Query(None),
    storage_location_id: Optional[int] = Query(None),
    movement_type: Optional[MovementType] = Query(None),
    offset: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=500),
    session: AsyncSession = Depends(get_session),
):
    stmt = (
        select(StockMovement)
        .options(
            selectinload(StockMovement.autopart),
            selectinload(StockMovement.storage_location),
        )
        .order_by(StockMovement.created_at.desc())
        .offset(offset).limit(limit)
    )
    if autopart_id:
        stmt = stmt.where(StockMovement.autopart_id == autopart_id)
    if storage_location_id:
        stmt = stmt.where(
            StockMovement.storage_location_id == storage_location_id
        )
    if movement_type:
        stmt = stmt.where(StockMovement.movement_type == movement_type)

    movements = (await session.execute(stmt)).scalars().all()
    return [
        _movement_to_out(m)
        for m in movements
    ]


@router.post(
    '/movements/',
    response_model=StockMovementOut,
    status_code=status.HTTP_201_CREATED,
    summary='Создать движение товара вручную',
)
async def create_stock_movement(
    data: StockMovementCreate,
    session: AsyncSession = Depends(get_session),
):
    if not (await session.get(AutoPart, data.autopart_id)):
        raise HTTPException(status_code=404, detail='Запчасть не найдена')
    if data.storage_location_id is None:
        raise HTTPException(
            status_code=400,
            detail='Для ручного движения требуется storage_location_id',
        )
    if not (await session.get(StorageLocation, data.storage_location_id)):
        raise HTTPException(
            status_code=404,
            detail='Место хранения не найдено',
        )

    if data.quantity == 0:
        raise HTTPException(
            status_code=400,
            detail='Количество движения не может быть 0',
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
                reference_type='manual_movement',
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
                reference_type='manual_movement',
                notes=data.notes,
            )
            if data.operation_uid and created_movements:
                created_movements[0].operation_uid = data.operation_uid
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    if not created_movements:
        raise HTTPException(
            status_code=400,
            detail='Не удалось создать движение с указанными параметрами',
        )

    movement_ids = [m.id for m in created_movements]
    result = await session.execute(
        select(StockMovement)
        .where(StockMovement.id.in_(movement_ids))
        .options(
            selectinload(StockMovement.autopart),
            selectinload(StockMovement.storage_location),
        )
        .order_by(StockMovement.created_at.desc(), StockMovement.id.desc())
    )
    movement = result.scalars().first()
    await session.commit()
    return _movement_to_out(movement)


# ─── Transfer ───────────────────────────────────────────────────────────────

@router.post(
    '/transfer/',
    response_model=TransferResult,
    status_code=status.HTTP_201_CREATED,
    summary='Переместить N единиц товара между ячейками',
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
            status_code=400,
            detail='Место-источник и назначение совпадают'
        )

    # ── Validate locations ──────────────────────────────────────────────────
    locs = (await session.execute(
        select(StorageLocation).where(
            StorageLocation.id.in_(
                [data.from_location_id, data.to_location_id]
            )
        )
    )).scalars().all()
    loc_map = {loc.id: loc for loc in locs}

    if data.from_location_id not in loc_map:
        raise HTTPException(
            status_code=404,
            detail='Место-источник не найдено'
        )
    if data.to_location_id not in loc_map:
        raise HTTPException(
            status_code=404,
            detail='Место-назначение не найдено'
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
                f'Недостаточно товара в '
                f'«{loc_map[data.from_location_id].name}»: '
                f'доступно {available}, запрошено {data.quantity}'
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
        current_sku_count = (await session.execute(
            select(func.count()).select_from(StockByLocation).where(
                StockByLocation.storage_location_id == data.to_location_id
            )
        )).scalar_one()
        if current_sku_count >= dest_loc.capacity:
            raise HTTPException(
                status_code=400,
                detail=(
                    f'Место «{dest_loc.name}» заполнено '
                    f'({current_sku_count}/{dest_loc.capacity} SKU)'
                ),
            )

    from_name = loc_map[data.from_location_id].name
    to_name = loc_map[data.to_location_id].name
    note = data.notes or f'Перемещение {from_name} → {to_name}'

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
        movement_out_id=transfer_result['movement_out_id'],
        movement_in_id=transfer_result['movement_in_id'],
    )


# ─── Stock Lots (GTD / партионный учёт) ────────────────────────────────────

def _lot_to_out(lot: StockLot) -> StockLotOut:
    loc = getattr(lot, 'storage_location', None)
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
    '/autoparts/{autopart_id}/lots',
    response_model=List[StockLotOut],
    summary='Партии (лоты) по артикулу',
)
async def get_autopart_lots(
    autopart_id: int,
    storage_location_id: Optional[int] = Query(default=None),
    only_active: bool = Query(
        default=False,
        description='Только с остатком > 0'
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
    '/lots',
    response_model=List[StockLotOut],
    summary='Список партий с фильтрацией',
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
        stmt = stmt.where(StockLot.gtd_number.ilike(f'%{gtd_number}%'))
    if source_receipt_id is not None:
        stmt = stmt.where(StockLot.source_receipt_id == source_receipt_id)
    if only_active:
        stmt = stmt.where(StockLot.remaining_quantity > 0)
    stmt = stmt.order_by(_asc(StockLot.received_at), _asc(StockLot.id))
    stmt = stmt.offset(offset).limit(limit)
    lots = (await session.execute(stmt)).scalars().all()
    return [_lot_to_out(lot) for lot in lots]


@router.get(
    '/lots/{lot_id}',
    response_model=StockLotOut,
    summary='Партия по ID',
)
async def get_stock_lot(
    lot_id: int,
    session: AsyncSession = Depends(get_session),
):
    lot = await session.get(StockLot, lot_id)
    if lot is None:
        raise HTTPException(status_code=404, detail='Партия не найдена')
    return _lot_to_out(lot)


# ─── StockDocument (ручные документы оприходования / списания) ─────────────

def _doc_item_to_out(item: StockDocumentItem) -> StockDocumentItemOut:
    ap = getattr(item, 'autopart', None)
    loc = getattr(item, 'storage_location', None)
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
    wh = getattr(doc, 'warehouse', None)
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


@router.get(
    '/documents/',
    response_model=List[StockDocumentListItem],
    summary='Список документов ручного оприходования / списания',
)
async def list_stock_documents(
    doc_type: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    warehouse_id: Optional[int] = Query(None),
    offset: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=200),
    db: AsyncSession = Depends(get_session),
):
    stmt = select(StockDocument).order_by(
        StockDocument.document_date.desc(), StockDocument.id.desc()
    ).offset(offset).limit(limit)
    if doc_type:
        stmt = stmt.where(StockDocument.doc_type == doc_type)
    if status:
        stmt = stmt.where(StockDocument.status == status)
    if warehouse_id:
        stmt = stmt.where(StockDocument.warehouse_id == warehouse_id)
    docs = (await db.execute(stmt)).scalars().all()

    result = []
    for doc in docs:
        wh = getattr(doc, 'warehouse', None)
        item_count = len(doc.items or [])
        result.append(StockDocumentListItem(
            id=doc.id,
            doc_type=doc.doc_type,
            status=doc.status,
            document_number=doc.document_number,
            document_date=doc.document_date,
            warehouse_id=doc.warehouse_id,
            warehouse_name=wh.name if wh else None,
            reason=doc.reason,
            item_count=item_count,
            created_at=doc.created_at,
            posted_at=doc.posted_at,
        ))
    return result


@router.post(
    '/documents/',
    response_model=StockDocumentOut,
    status_code=status.HTTP_201_CREATED,
    summary='Создать документ оприходования или списания (черновик)',
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
    '/documents/{doc_id}',
    response_model=StockDocumentOut,
    summary='Документ оприходования / списания по ID',
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
            selectinload(StockDocument.items)
            .selectinload(StockDocumentItem.storage_location),
        )
        .where(StockDocument.id == doc_id)
    )
    doc = (await db.execute(stmt)).scalar_one_or_none()
    if doc is None:
        raise HTTPException(status_code=404, detail='Документ не найден')
    return _doc_to_out(doc)


@router.patch(
    '/documents/{doc_id}',
    response_model=StockDocumentOut,
    summary='Обновить реквизиты черновика',
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
        raise HTTPException(status_code=404, detail='Документ не найден')
    if doc.status != StockDocumentStatus.DRAFT:
        raise HTTPException(
            status_code=400,
            detail='Редактировать можно только черновик',
        )
    for field, value in data.model_dump(exclude_unset=True).items():
        setattr(doc, field, value)
    await db.commit()
    return _doc_to_out(doc)


@router.post(
    '/documents/{doc_id}/items',
    response_model=StockDocumentItemOut,
    status_code=status.HTTP_201_CREATED,
    summary='Добавить строку в черновик',
)
async def add_document_item(
    doc_id: int,
    data: StockDocumentItemCreate,
    db: AsyncSession = Depends(get_session),
):
    doc = await db.get(StockDocument, doc_id)
    if doc is None:
        raise HTTPException(status_code=404, detail='Документ не найден')
    if doc.status != StockDocumentStatus.DRAFT:
        raise HTTPException(
            status_code=400,
            detail='Документ не в статусе черновика'
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
            selectinload(StockDocumentItem.autopart)
            .selectinload(AutoPart.brand),
            selectinload(StockDocumentItem.storage_location),
        )
        .where(StockDocumentItem.id == item.id)
    )
    item = (await db.execute(stmt)).scalar_one()
    await db.commit()
    return _doc_item_to_out(item)


@router.patch(
    '/documents/{doc_id}/items/{item_id}',
    response_model=StockDocumentItemOut,
    summary='Обновить строку черновика',
)
async def update_document_item(
    doc_id: int,
    item_id: int,
    data: StockDocumentItemUpdate,
    db: AsyncSession = Depends(get_session),
):
    doc = await db.get(StockDocument, doc_id)
    if doc is None:
        raise HTTPException(status_code=404, detail='Документ не найден')
    if doc.status != StockDocumentStatus.DRAFT:
        raise HTTPException(
            status_code=400,
            detail='Документ не в статусе черновика'
        )

    stmt = (
        select(StockDocumentItem)
        .options(
            selectinload(StockDocumentItem.autopart)
            .selectinload(AutoPart.brand),
            selectinload(StockDocumentItem.storage_location),
        )
        .where(
            StockDocumentItem.id == item_id,
            StockDocumentItem.document_id == doc_id,
        )
    )
    item = (await db.execute(stmt)).scalar_one_or_none()
    if item is None:
        raise HTTPException(status_code=404, detail='Строка не найдена')

    for field, value in data.model_dump(exclude_unset=True).items():
        setattr(item, field, value)
    await db.commit()
    return _doc_item_to_out(item)


@router.delete(
    '/documents/{doc_id}/items/{item_id}',
    status_code=status.HTTP_204_NO_CONTENT,
    summary='Удалить строку черновика',
)
async def delete_document_item(
    doc_id: int,
    item_id: int,
    db: AsyncSession = Depends(get_session),
):
    doc = await db.get(StockDocument, doc_id)
    if doc is None:
        raise HTTPException(status_code=404, detail='Документ не найден')
    if doc.status != StockDocumentStatus.DRAFT:
        raise HTTPException(
            status_code=400,
            detail='Документ не в статусе черновика'
        )

    item = await db.get(StockDocumentItem, item_id)
    if item is None or item.document_id != doc_id:
        raise HTTPException(status_code=404, detail='Строка не найдена')
    await db.delete(item)
    await db.commit()


@router.post(
    '/documents/{doc_id}/post',
    summary='Провести документ — обновить остатки',
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
    '/documents/{doc_id}/unpost',
    summary='Распровести документ — отменить изменения остатков',
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
    '/documents/{doc_id}',
    status_code=status.HTTP_204_NO_CONTENT,
    summary='Удалить черновик',
)
async def delete_stock_document(
    doc_id: int,
    db: AsyncSession = Depends(get_session),
):
    doc = await db.get(StockDocument, doc_id)
    if doc is None:
        raise HTTPException(status_code=404, detail='Документ не найден')
    if doc.status == StockDocumentStatus.POSTED:
        raise HTTPException(
            status_code=400,
            detail='Нельзя удалить проведённый документ. '
                   'Сначала распроведите.',
        )
    await db.delete(doc)
    await db.commit()


# ─── Admin / backfill ────────────────────────────────────────────────────────

@router.post(
    '/admin/backfill-lots',
    response_model=BackfillResult,
    summary='Backfill: создать opening_balance лоты для товара без партий',
    description=(
        'Одноразовая операция — создаёт лоты с source_type=opening_balance '
        'для всех позиций StockByLocation, у которых нет активных лотов. '
        'Безопасно запускать повторно: '
        'позиции с уже покрытым остатком пропускаются.'
    ),
)
async def run_backfill_lots(
    db: AsyncSession = Depends(get_session),
):
    result = await backfill_opening_balance_lots(db)
    await db.commit()
    return BackfillResult(**result)
