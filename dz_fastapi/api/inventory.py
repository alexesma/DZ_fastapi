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
                                         InventoryStatus, MovementType,
                                         StockByLocation, StockMovement)
from dz_fastapi.schemas.inventory import (InventoryItemCountUpdate,
                                          InventoryItemOut,
                                          InventorySessionCreate,
                                          InventorySessionListItem,
                                          InventorySessionOut,
                                          InventorySessionUpdate,
                                          StockByLocationOut,
                                          StockByLocationUpsert,
                                          StockMovementCreate,
                                          StockMovementOut, TransferRequest,
                                          TransferResult)

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


# ─── StockByLocation endpoints ──────────────────────────────────────────────

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

    sbl = await _ensure_sbl(
        session, data.autopart_id, data.storage_location_id, data.quantity
    )
    await session.commit()

    # Reload with relationships
    result = await session.execute(
        select(StockByLocation)
        .where(StockByLocation.id == sbl.id)
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

    # ── Get StockByLocation rows for these locations ────────────────────────
    if loc_ids:
        sbl_rows = (await session.execute(
            select(StockByLocation).where(
                StockByLocation.storage_location_id.in_(loc_ids)
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

    # Also include M2M links that don't have a StockByLocation row yet
    if loc_ids:
        assoc_rows = (await session.execute(
            select(
                autopart_storage_association.c.autopart_id,
                autopart_storage_association.c.storage_location_id,
            ).where(
                autopart_storage_association.c.storage_location_id.in_(
                    loc_ids
                )
            )
        )).all()
        for ap_id, loc_id in assoc_rows:
            key = (ap_id, loc_id)
            if key not in seen:
                seen.add(key)
                session.add(InventoryItem(
                    session_id=inv_session.id,
                    autopart_id=ap_id,
                    storage_location_id=loc_id,
                    expected_qty=0,
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
            # ── Update StockByLocation ──────────────────────────────────────
            sbl_result = await session.execute(
                select(StockByLocation).where(
                    StockByLocation.autopart_id == item.autopart_id,
                    StockByLocation.storage_location_id
                    == item.storage_location_id,
                )
            )
            sbl = sbl_result.scalar_one_or_none()
            qty_before = sbl.quantity if sbl else 0

            if sbl:
                sbl.quantity = item.actual_qty
                sbl.updated_at = now_moscow()
            elif item.actual_qty > 0:
                session.add(StockByLocation(
                    autopart_id=item.autopart_id,
                    storage_location_id=item.storage_location_id,
                    quantity=item.actual_qty,
                ))

            # ── StockMovement for non-zero discrepancies ────────────────────
            if item.discrepancy != 0:
                session.add(StockMovement(
                    autopart_id=item.autopart_id,
                    storage_location_id=item.storage_location_id,
                    movement_type=MovementType.INVENTORY,
                    quantity=item.discrepancy,
                    qty_before=qty_before,
                    qty_after=item.actual_qty,
                    reference_id=inv_session.id,
                    reference_type='inventory',
                    notes=f'Инвентаризация «{inv_session.name}»',
                ))

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
        StockMovementOut(
            id=m.id, autopart_id=m.autopart_id,
            storage_location_id=m.storage_location_id,
            movement_type=m.movement_type, quantity=m.quantity,
            qty_before=m.qty_before, qty_after=m.qty_after,
            reference_id=m.reference_id, reference_type=m.reference_type,
            notes=m.notes, created_at=m.created_at,
            autopart_oem=m.autopart.oem_number if m.autopart else None,
            autopart_name=m.autopart.name if m.autopart else None,
            storage_location_name=(
                m.storage_location.name if m.storage_location else None
            ),
        )
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
    movement = StockMovement(
        autopart_id=data.autopart_id,
        storage_location_id=data.storage_location_id,
        movement_type=data.movement_type,
        quantity=data.quantity,
        notes=data.notes,
    )
    session.add(movement)
    await session.flush()
    result = await session.execute(
        select(StockMovement)
        .where(StockMovement.id == movement.id)
        .options(selectinload(
            StockMovement.autopart),
            selectinload(StockMovement.storage_location)
        )
    )
    movement = result.scalar_one()
    await session.commit()
    return StockMovementOut(
        id=movement.id, autopart_id=movement.autopart_id,
        storage_location_id=movement.storage_location_id,
        movement_type=movement.movement_type, quantity=movement.quantity,
        qty_before=movement.qty_before, qty_after=movement.qty_after,
        reference_id=movement.reference_id,
        reference_type=movement.reference_type,
        notes=movement.notes, created_at=movement.created_at,
        autopart_oem=(
            movement.autopart.oem_number if movement.autopart else None
        ),
        autopart_name=movement.autopart.name if movement.autopart else None,
        storage_location_name=(
            movement.storage_location.name
            if movement.storage_location
            else None
        ),
    )


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

    qty_before_src = src_sbl.quantity
    qty_before_dst = dest_sbl.quantity if dest_sbl else 0

    # ── Update source ───────────────────────────────────────────────────────
    src_sbl.quantity -= data.quantity
    src_sbl.updated_at = now_moscow()
    qty_after_src = src_sbl.quantity

    if src_sbl.quantity == 0:
        # Remove from location entirely
        await session.delete(src_sbl)
        await session.execute(
            delete(autopart_storage_association).where(
                autopart_storage_association.c.autopart_id
                == data.autopart_id,
                autopart_storage_association.c.storage_location_id
                == data.from_location_id,
            )
        )

    # ── Update destination ──────────────────────────────────────────────────
    if dest_sbl:
        dest_sbl.quantity += data.quantity
        dest_sbl.updated_at = now_moscow()
        qty_after_dst = dest_sbl.quantity
    else:
        new_sbl = StockByLocation(
            autopart_id=data.autopart_id,
            storage_location_id=data.to_location_id,
            quantity=data.quantity,
        )
        session.add(new_sbl)
        qty_after_dst = data.quantity
        # Ensure M2M link
        assoc_exists = (await session.execute(
            select(autopart_storage_association).where(
                autopart_storage_association.c.autopart_id
                == data.autopart_id,
                autopart_storage_association.c.storage_location_id
                == data.to_location_id,
            )
        )).first()
        if not assoc_exists:
            await session.execute(
                insert(autopart_storage_association).values(
                    autopart_id=data.autopart_id,
                    storage_location_id=data.to_location_id,
                )
            )

    # ── StockMovement records ───────────────────────────────────────────────
    from_name = loc_map[data.from_location_id].name
    to_name = loc_map[data.to_location_id].name
    note = data.notes or f'Перемещение {from_name} → {to_name}'

    out_mv = StockMovement(
        autopart_id=data.autopart_id,
        storage_location_id=data.from_location_id,
        movement_type=MovementType.TRANSFER_OUT,
        quantity=-data.quantity,
        qty_before=qty_before_src,
        qty_after=qty_after_src,
        reference_type='transfer',
        notes=note,
    )
    in_mv = StockMovement(
        autopart_id=data.autopart_id,
        storage_location_id=data.to_location_id,
        movement_type=MovementType.TRANSFER_IN,
        quantity=data.quantity,
        qty_before=qty_before_dst,
        qty_after=qty_after_dst,
        reference_type='transfer',
        notes=note,
    )
    session.add(out_mv)
    session.add(in_mv)
    await session.flush()
    out_id, in_id = out_mv.id, in_mv.id
    await session.commit()

    return TransferResult(
        autopart_id=data.autopart_id,
        from_location_id=data.from_location_id,
        to_location_id=data.to_location_id,
        movement_out_id=out_id,
        movement_in_id=in_id,
    )
