"""Planning and posting of DragonZap production waves."""

from __future__ import annotations

from collections import defaultdict
from decimal import ROUND_HALF_UP, Decimal
from typing import Any, Iterable, Optional

from sqlalchemy import delete, func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from dz_fastapi.core.time import now_moscow
from dz_fastapi.models.autopart import AutoPart, LocationType, StorageLocation
from dz_fastapi.models.brand import Brand
from dz_fastapi.models.inventory import (
    DragonzapProductionGroup,
    LotSourceType,
    MarkingCodeStatus,
    MarkingMovementType,
    MovementType,
    ProductionWave,
    ProductionWaveAllocation,
    ProductionWaveDemand,
    ProductionWaveItem,
    ProductionWaveLabel,
    ProductionWaveLabelPrintEvent,
    ProductionWaveLabelStatus,
    ProductionWaveSource,
    ProductionWaveStatus,
    ProductMarkingCode,
    ProductMarkingCodeMovement,
    StockLot,
    StockLotRole,
    StockLotRoleSource,
    SyncStatus,
    Warehouse,
)
from dz_fastapi.models.partner import (
    STOCK_ORDER_STATUS,
    Customer,
    CustomerOrder,
    CustomerOrderItem,
    StockOrder,
    StockOrderItem,
)
from dz_fastapi.services.inventory_stock import (
    _apply_stock_delta,
    _create_stock_lot,
    ensure_default_warehouse,
)
from dz_fastapi.services.production_groups import list_production_groups, sync_production_groups

MONEY = Decimal("0.01")
UNIT_COST = Decimal("0.0001")
PRODUCTION_LOCATION_CODE = "PRODUCTION"
RESERVING_STATUSES = (
    ProductionWaveStatus.PLANNED,
    ProductionWaveStatus.IN_PROGRESS,
)


def _decimal(value: Any) -> Decimal:
    return Decimal(str(value or 0))


def _money(value: Decimal) -> Decimal:
    return value.quantize(MONEY, rounding=ROUND_HALF_UP)


def _unit_cost(value: Decimal) -> Decimal:
    return value.quantize(UNIT_COST, rounding=ROUND_HALF_UP)


def _enum_value(value: Any) -> str:
    return str(getattr(value, "value", value))


async def ensure_production_location(
    session: AsyncSession,
    warehouse: Warehouse,
) -> StorageLocation:
    location = (
        await session.execute(
            select(StorageLocation).where(
                StorageLocation.warehouse_id == warehouse.id,
                StorageLocation.system_code == PRODUCTION_LOCATION_CODE,
            )
        )
    ).scalar_one_or_none()
    if location is not None:
        return location
    location = StorageLocation(
        name=f"WH{warehouse.id} PRODUCTION",
        warehouse_id=warehouse.id,
        location_type=LocationType.OTHER,
        system_code=PRODUCTION_LOCATION_CODE,
    )
    session.add(location)
    await session.flush()
    return location


def _active_demand_subquery():
    return (
        select(
            ProductionWaveDemand.stock_order_item_id.label("stock_item_id"),
            func.sum(ProductionWaveDemand.quantity).label("allocated_qty"),
        )
        .join(
            ProductionWaveItem,
            ProductionWaveItem.id == ProductionWaveDemand.wave_item_id,
        )
        .join(ProductionWave, ProductionWave.id == ProductionWaveItem.wave_id)
        .where(ProductionWave.status != ProductionWaveStatus.CANCELLED)
        .group_by(ProductionWaveDemand.stock_order_item_id)
        .subquery()
    )


async def list_eligible_demands(
    session: AsyncSession,
    *,
    warehouse_id: Optional[int] = None,
    stock_order_item_ids: Optional[Iterable[int]] = None,
    limit: int = 200,
    offset: int = 0,
) -> tuple[list[dict[str, Any]], int]:
    """Return unallocated own-stock rows mapped to active DragonZap groups."""

    await sync_production_groups(session)
    await session.flush()

    allocated = _active_demand_subquery()
    allocated_qty = func.coalesce(allocated.c.allocated_qty, 0)
    remaining = StockOrderItem.quantity - StockOrderItem.picked_quantity - allocated_qty
    filters = [
        StockOrder.status == STOCK_ORDER_STATUS.NEW,
        DragonzapProductionGroup.is_active.is_(True),
        StockOrderItem.customer_order_item_id.is_not(None),
        remaining > 0,
    ]
    selected_ids = {int(value) for value in stock_order_item_ids or []}
    if selected_ids:
        filters.append(StockOrderItem.id.in_(selected_ids))

    base = (
        select(
            StockOrderItem,
            CustomerOrderItem,
            CustomerOrder,
            Customer,
            DragonzapProductionGroup,
            AutoPart,
            Brand,
            remaining.label("remaining_qty"),
        )
        .join(StockOrder, StockOrder.id == StockOrderItem.stock_order_id)
        .join(
            CustomerOrderItem,
            CustomerOrderItem.id == StockOrderItem.customer_order_item_id,
        )
        .join(CustomerOrder, CustomerOrder.id == CustomerOrderItem.order_id)
        .join(Customer, Customer.id == CustomerOrder.customer_id)
        .join(
            DragonzapProductionGroup,
            DragonzapProductionGroup.finished_autopart_id == StockOrderItem.autopart_id,
        )
        .join(AutoPart, AutoPart.id == StockOrderItem.autopart_id)
        .join(Brand, Brand.id == AutoPart.brand_id)
        .outerjoin(allocated, allocated.c.stock_item_id == StockOrderItem.id)
        .where(*filters)
    )
    total = int(
        (await session.execute(select(func.count()).select_from(base.subquery()))).scalar_one()
    )
    rows = (
        await session.execute(
            base.order_by(
                CustomerOrder.received_at,
                CustomerOrder.id,
                StockOrderItem.id,
            )
            .limit(limit)
            .offset(offset)
        )
    ).all()
    return [
        {
            "stock_order_item_id": stock_item.id,
            "stock_order_id": stock_item.stock_order_id,
            "customer_order_item_id": order_item.id,
            "customer_order_id": order.id,
            "customer_id": customer.id,
            "customer_name": customer.name,
            "order_number": order.order_number,
            "order_date": order.order_date,
            "requested_brand": order_item.brand,
            "requested_oem": order_item.oem,
            "requested_name": order_item.name,
            "quantity": int(remaining_qty),
            "production_group_id": group.id,
            "finished_autopart_id": part.id,
            "finished_brand": brand.name,
            "finished_oem_number": part.oem_number,
            "finished_name": part.name,
        }
        for (
            stock_item,
            order_item,
            order,
            customer,
            group,
            part,
            brand,
            remaining_qty,
        ) in rows
    ], total


async def _get_warehouse(
    session: AsyncSession,
    warehouse_id: Optional[int],
) -> Warehouse:
    if warehouse_id is None:
        return await ensure_default_warehouse(session)
    warehouse = await session.get(Warehouse, warehouse_id)
    if warehouse is None or not warehouse.is_active:
        raise LookupError("Активный склад не найден")
    return warehouse


async def create_production_wave(
    session: AsyncSession,
    *,
    stock_order_item_ids: list[int],
    user_id: Optional[int],
    warehouse_id: Optional[int] = None,
    cutoff_at=None,
    notes: Optional[str] = None,
    source: ProductionWaveSource = ProductionWaveSource.MANUAL,
) -> ProductionWave:
    selected_ids = {int(value) for value in stock_order_item_ids}
    if not selected_ids:
        raise ValueError("Выберите хотя бы одну строку заказа")
    locked_ids = set(
        (
            (
                await session.execute(
                    select(StockOrderItem.id)
                    .where(StockOrderItem.id.in_(selected_ids))
                    .order_by(StockOrderItem.id)
                    .with_for_update()
                )
            )
            .scalars()
            .all()
        )
    )
    if locked_ids != selected_ids:
        missing_ids = sorted(selected_ids - locked_ids)
        raise LookupError(f"Строки заказов не найдены: {', '.join(map(str, missing_ids))}")
    eligible, _ = await list_eligible_demands(
        session,
        warehouse_id=warehouse_id,
        stock_order_item_ids=selected_ids,
        limit=max(len(selected_ids), 1),
    )
    found_ids = {row["stock_order_item_id"] for row in eligible}
    missing = sorted(selected_ids - found_ids)
    if missing:
        raise ValueError(
            "Часть строк уже включена в другую волну или не относится к "
            f"активным группам DragonZap: {', '.join(map(str, missing))}"
        )

    warehouse = await _get_warehouse(session, warehouse_id)
    wave = ProductionWave(
        warehouse_id=warehouse.id,
        status=ProductionWaveStatus.DRAFT,
        source=source,
        cutoff_at=cutoff_at,
        notes=str(notes or "").strip() or None,
        created_by_user_id=user_id,
    )
    session.add(wave)
    await session.flush()
    wave.number = f"DZW-{now_moscow():%Y%m%d}-{wave.id:06d}"

    rows_by_group: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in eligible:
        rows_by_group[row["production_group_id"]].append(row)
    for group_id, rows in rows_by_group.items():
        item = ProductionWaveItem(
            wave_id=wave.id,
            production_group_id=group_id,
            finished_autopart_id=rows[0]["finished_autopart_id"],
            planned_quantity=sum(row["quantity"] for row in rows),
        )
        session.add(item)
        await session.flush()
        for row in rows:
            session.add(
                ProductionWaveDemand(
                    wave_item_id=item.id,
                    customer_order_item_id=row["customer_order_item_id"],
                    stock_order_item_id=row["stock_order_item_id"],
                    quantity=row["quantity"],
                    customer_id=row["customer_id"],
                    customer_name=row["customer_name"],
                    customer_order_id=row["customer_order_id"],
                    order_number=row["order_number"],
                    order_date=row["order_date"],
                    requested_brand=row["requested_brand"],
                    requested_oem=row["requested_oem"],
                    requested_name=row["requested_name"],
                )
            )
    await session.flush()
    await replan_production_wave(session, wave.id)
    return await get_production_wave(session, wave.id)


async def _reserved_by_other_waves(
    session: AsyncSession,
    *,
    lot_ids: list[int],
    wave_id: int,
) -> dict[int, int]:
    if not lot_ids:
        return {}
    return {
        int(lot_id): int(quantity or 0)
        for lot_id, quantity in (
            await session.execute(
                select(
                    ProductionWaveAllocation.stock_lot_id,
                    func.sum(
                        ProductionWaveAllocation.planned_quantity
                        - ProductionWaveAllocation.consumed_quantity
                    ),
                )
                .join(
                    ProductionWaveItem,
                    ProductionWaveItem.id == ProductionWaveAllocation.wave_item_id,
                )
                .join(
                    ProductionWave,
                    ProductionWave.id == ProductionWaveItem.wave_id,
                )
                .where(
                    ProductionWaveAllocation.stock_lot_id.in_(lot_ids),
                    ProductionWave.id != wave_id,
                    ProductionWave.status.in_(RESERVING_STATUSES),
                )
                .group_by(ProductionWaveAllocation.stock_lot_id)
            )
        ).all()
    }


async def _marking_codes_by_lot(
    session: AsyncSession,
    lot_ids: list[int],
) -> dict[int, list[str]]:
    result: dict[int, list[str]] = defaultdict(list)
    if not lot_ids:
        return result
    rows = (
        await session.execute(
            select(ProductMarkingCode.stock_lot_id, ProductMarkingCode.code)
            .where(
                ProductMarkingCode.stock_lot_id.in_(lot_ids),
                ProductMarkingCode.status.in_(
                    (MarkingCodeStatus.RECEIVED, MarkingCodeStatus.IN_STOCK)
                ),
            )
            .order_by(ProductMarkingCode.id)
        )
    ).all()
    for lot_id, code in rows:
        result[int(lot_id)].append(str(code))
    return result


async def _plan_item(
    session: AsyncSession,
    *,
    wave: ProductionWave,
    item: ProductionWaveItem,
) -> None:
    groups, _ = await list_production_groups(
        session,
        group_id=item.production_group_id,
        limit=1,
    )
    if not groups or not groups[0]["is_active"]:
        item.planning_error = "Группа выпуска отключена или удалена"
        item.shortage_quantity = item.planned_quantity
        return
    group = groups[0]
    if group["graph_truncated"]:
        item.planning_error = "Группа кроссов слишком велика и требует проверки"
        item.shortage_quantity = item.planned_quantity
        return
    materials = {
        row["autopart_id"]: int(row["priority"]) for row in group["materials"] if row["is_allowed"]
    }
    if not materials:
        item.planning_error = "В группе нет разрешённых материалов"
        item.shortage_quantity = item.planned_quantity
        return

    lot_ids = (
        (
            await session.execute(
                select(StockLot.id)
                .join(
                    StorageLocation,
                    StorageLocation.id == StockLot.storage_location_id,
                )
                .where(
                    StockLot.autopart_id.in_(materials),
                    StockLot.inventory_role == StockLotRole.DRAGONZAP_MATERIAL,
                    StockLot.remaining_quantity > 0,
                    StorageLocation.warehouse_id == wave.warehouse_id,
                )
                .order_by(StockLot.received_at, StockLot.id)
                .with_for_update(of=StockLot)
            )
        )
        .scalars()
        .all()
    )
    lots = (
        (
            await session.execute(
                select(StockLot)
                .where(StockLot.id.in_(lot_ids))
                .order_by(StockLot.received_at, StockLot.id)
            )
        )
        .scalars()
        .all()
    )
    reservations = await _reserved_by_other_waves(
        session,
        lot_ids=[lot.id for lot in lots],
        wave_id=wave.id,
    )
    codes_by_lot = await _marking_codes_by_lot(
        session,
        [lot.id for lot in lots],
    )
    candidates = []
    for lot in lots:
        available = max(
            0,
            int(lot.remaining_quantity) - reservations.get(lot.id, 0),
        )
        codes = codes_by_lot.get(lot.id, [])
        if codes:
            available = min(available, len(codes))
        if available:
            candidates.append((materials[lot.autopart_id], lot, available, codes))

    needed = int(item.planned_quantity)
    selected: list[tuple[StockLot, int, list[str]]] = []
    for priority in sorted({row[0] for row in candidates}):
        priority_rows = [row for row in candidates if row[0] == priority]
        covering = next(
            (row for row in priority_rows if row[2] >= needed),
            None,
        )
        rows = [covering] if covering is not None else priority_rows
        for _, lot, available, codes in rows:
            if needed <= 0:
                break
            quantity = min(available, needed)
            selected.append((lot, quantity, codes[:quantity] if codes else []))
            needed -= quantity
        if needed <= 0:
            break

    material_cost = Decimal("0")
    missing_cost = False
    for lot, quantity, codes in selected:
        unit = lot.cost_price
        if unit is None:
            missing_cost = True
            line_cost = Decimal("0")
        else:
            unit = _unit_cost(_decimal(unit))
            line_cost = _money(unit * Decimal(quantity))
            material_cost += line_cost
        session.add(
            ProductionWaveAllocation(
                wave_item_id=item.id,
                material_autopart_id=lot.autopart_id,
                stock_lot_id=lot.id,
                storage_location_id=lot.storage_location_id,
                planned_quantity=quantity,
                unit_material_cost=unit,
                total_material_cost=line_cost,
                gtd_number=lot.gtd_number,
                country_code=lot.country_code,
                country_name=lot.country_name,
                marking_codes=codes,
            )
        )

    item.shortage_quantity = needed
    errors = []
    if needed:
        errors.append(f"Не хватает материала: {needed} шт.")
    if missing_cost:
        errors.append("У одной или нескольких партий нет себестоимости")
    item.planning_error = "; ".join(errors) or None
    item.material_cost = _money(material_cost)
    item.packaging_cost = _money(_decimal(group["packaging_cost"]) * Decimal(item.planned_quantity))
    item.total_cost = _money(item.material_cost + item.packaging_cost)
    item.unit_cost = (
        _unit_cost(item.total_cost / Decimal(item.planned_quantity))
        if not errors and item.planned_quantity
        else None
    )


async def replan_production_wave(
    session: AsyncSession,
    wave_id: int,
) -> ProductionWave:
    wave = await session.get(ProductionWave, wave_id)
    if wave is None:
        raise LookupError("Производственная волна не найдена")
    if wave.status != ProductionWaveStatus.DRAFT:
        raise ValueError("Пересчитать можно только черновик волны")
    await session.execute(
        delete(ProductionWaveAllocation).where(
            ProductionWaveAllocation.wave_item_id.in_(
                select(ProductionWaveItem.id).where(ProductionWaveItem.wave_id == wave_id)
            )
        )
    )
    await session.flush()
    item_ids = (
        (
            await session.execute(
                select(ProductionWaveItem.id).where(ProductionWaveItem.wave_id == wave_id)
            )
        )
        .scalars()
        .all()
    )
    items = [await session.get(ProductionWaveItem, item_id) for item_id in item_ids]
    for item in items:
        await _plan_item(session, wave=wave, item=item)
    await session.flush()

    wave.total_planned_quantity = sum(item.planned_quantity for item in items)
    wave.total_material_cost = _money(sum((item.material_cost for item in items), Decimal("0")))
    wave.total_packaging_cost = _money(sum((item.packaging_cost for item in items), Decimal("0")))
    wave.total_finished_cost = _money(wave.total_material_cost + wave.total_packaging_cost)
    errors = [item.planning_error for item in items if item.planning_error]
    wave.error_message = " | ".join(errors) or None
    wave.sync_status = SyncStatus.PENDING
    await session.flush()
    return await get_production_wave(session, wave_id)


async def schedule_production_wave(
    session: AsyncSession,
    *,
    wave_id: int,
    user_id: Optional[int],
) -> ProductionWave:
    wave = await replan_production_wave(session, wave_id)
    if wave.error_message:
        raise ValueError("Волну нельзя запланировать: исправьте дефицит и ошибки расчёта")
    wave.status = ProductionWaveStatus.PLANNED
    wave.planned_by_user_id = user_id
    wave.planned_at = now_moscow()
    await _generate_wave_labels(session, wave)
    await session.flush()
    return await get_production_wave(session, wave_id)


async def _generate_wave_labels(
    session: AsyncSession,
    wave: ProductionWave,
) -> None:
    existing = int(
        (
            await session.execute(
                select(func.count(ProductionWaveLabel.id)).where(
                    ProductionWaveLabel.wave_id == wave.id
                )
            )
        ).scalar_one()
    )
    if existing:
        return
    demands = (
        (
            await session.execute(
                select(ProductionWaveDemand)
                .join(
                    ProductionWaveItem,
                    ProductionWaveItem.id == ProductionWaveDemand.wave_item_id,
                )
                .where(ProductionWaveItem.wave_id == wave.id)
                .order_by(ProductionWaveDemand.id)
            )
        )
        .scalars()
        .all()
    )
    for demand in demands:
        total_labels = int(demand.quantity)
        for sequence in range(1, total_labels + 1):
            session.add(
                ProductionWaveLabel(
                    wave_id=wave.id,
                    wave_item_id=demand.wave_item_id,
                    wave_demand_id=demand.id,
                    sequence_number=sequence,
                    total_labels=total_labels,
                    quantity=1,
                    requested_brand=demand.requested_brand,
                    requested_oem=demand.requested_oem,
                    requested_name=demand.requested_name,
                    customer_name=demand.customer_name,
                    order_number=demand.order_number,
                    order_date=demand.order_date,
                    barcode=(
                        f"DZW-{wave.id}-COI-{demand.customer_order_item_id}-" f"{sequence:04d}"
                    ),
                )
            )
    await session.flush()


async def create_scheduled_production_wave(
    session: AsyncSession,
    *,
    cutoff_at=None,
    warehouse_id: Optional[int] = None,
) -> Optional[ProductionWave]:
    """Create one aggregate wave from every currently eligible DragonZap row."""

    eligible, _ = await list_eligible_demands(
        session,
        warehouse_id=warehouse_id,
        limit=10000,
    )
    if not eligible:
        return None
    wave = await create_production_wave(
        session,
        stock_order_item_ids=[row["stock_order_item_id"] for row in eligible],
        user_id=None,
        warehouse_id=warehouse_id,
        cutoff_at=cutoff_at or now_moscow(),
        source=ProductionWaveSource.SCHEDULED,
        notes="Автоматически сформировано по настроенной отсечке",
    )
    if not wave.error_message:
        wave = await schedule_production_wave(
            session,
            wave_id=wave.id,
            user_id=None,
        )
    return wave


async def list_production_wave_labels(
    session: AsyncSession,
    wave_id: int,
) -> list[ProductionWaveLabel]:
    if await session.get(ProductionWave, wave_id) is None:
        raise LookupError("Производственная волна не найдена")
    return (
        (
            await session.execute(
                select(ProductionWaveLabel)
                .where(ProductionWaveLabel.wave_id == wave_id)
                .execution_options(populate_existing=True)
                .options(
                    selectinload(ProductionWaveLabel.last_printed_by_user),
                    selectinload(ProductionWaveLabel.print_events).selectinload(
                        ProductionWaveLabelPrintEvent.printed_by_user
                    ),
                )
                .order_by(
                    ProductionWaveLabel.wave_demand_id,
                    ProductionWaveLabel.sequence_number,
                )
            )
        )
        .scalars()
        .all()
    )


async def mark_production_wave_labels_printed(
    session: AsyncSession,
    *,
    wave_id: int,
    label_ids: Optional[list[int]],
    user_id: int,
    reason: Optional[str] = None,
) -> list[ProductionWaveLabel]:
    normalized_reason = str(reason or "").strip() or None
    filters = [ProductionWaveLabel.wave_id == wave_id]
    if label_ids:
        filters.append(ProductionWaveLabel.id.in_({int(value) for value in label_ids}))
    locked_ids = (
        (
            await session.execute(
                select(ProductionWaveLabel.id)
                .where(*filters)
                .order_by(ProductionWaveLabel.id)
                .with_for_update()
            )
        )
        .scalars()
        .all()
    )
    if not locked_ids:
        raise LookupError("Этикетки производственной волны не найдены")
    labels = (
        (
            await session.execute(
                select(ProductionWaveLabel)
                .where(ProductionWaveLabel.id.in_(locked_ids))
                .order_by(ProductionWaveLabel.id)
            )
        )
        .scalars()
        .all()
    )
    if any(label.print_count > 0 for label in labels) and not normalized_reason:
        raise ValueError("Для повторной печати укажите причину")
    printed_at = now_moscow()
    for label in labels:
        label.status = ProductionWaveLabelStatus.PRINTED
        label.print_count = int(label.print_count or 0) + 1
        label.last_printed_at = printed_at
        label.last_printed_by_user_id = user_id
        label.last_print_reason = normalized_reason
        session.add(
            ProductionWaveLabelPrintEvent(
                label_id=label.id,
                wave_id=wave_id,
                print_number=label.print_count,
                printed_by_user_id=user_id,
                printed_at=printed_at,
                reason=normalized_reason,
            )
        )
    await session.flush()
    return await list_production_wave_labels(session, wave_id)


async def _lock_wave(
    session: AsyncSession,
    wave_id: int,
) -> ProductionWave:
    locked_id = (
        await session.execute(
            select(ProductionWave.id).where(ProductionWave.id == wave_id).with_for_update()
        )
    ).scalar_one_or_none()
    if locked_id is None:
        raise LookupError("Производственная волна не найдена")
    return await session.get(ProductionWave, locked_id)


async def start_production_wave(
    session: AsyncSession,
    *,
    wave_id: int,
    user_id: int,
) -> ProductionWave:
    wave = await _lock_wave(session, wave_id)
    if wave.status != ProductionWaveStatus.PLANNED:
        raise ValueError("Запустить можно только запланированную волну")
    unprinted_labels = int(
        (
            await session.execute(
                select(func.count(ProductionWaveLabel.id)).where(
                    ProductionWaveLabel.wave_id == wave_id,
                    ProductionWaveLabel.print_count == 0,
                )
            )
        ).scalar_one()
    )
    if unprinted_labels:
        raise ValueError(f"Сначала напечатайте все этикетки волны: ожидают {unprinted_labels} шт.")
    wave.status = ProductionWaveStatus.IN_PROGRESS
    wave.started_by_user_id = user_id
    wave.started_at = now_moscow()
    await session.flush()
    return await get_production_wave(session, wave_id)


async def complete_production_wave(
    session: AsyncSession,
    *,
    wave_id: int,
    user_id: int,
) -> ProductionWave:
    wave = await _lock_wave(session, wave_id)
    if wave.status != ProductionWaveStatus.IN_PROGRESS:
        raise ValueError("Завершить можно только запущенную волну")
    warehouse = await session.get(Warehouse, wave.warehouse_id)
    location = await ensure_production_location(session, warehouse)
    items = (
        (
            await session.execute(
                select(ProductionWaveItem)
                .where(ProductionWaveItem.wave_id == wave_id)
                .options(selectinload(ProductionWaveItem.allocations))
                .order_by(ProductionWaveItem.id)
            )
        )
        .scalars()
        .unique()
        .all()
    )
    allocations = [allocation for item in items for allocation in item.allocations]
    lot_ids = sorted({allocation.stock_lot_id for allocation in allocations})
    locked_lot_ids = (
        (
            await session.execute(
                select(StockLot.id)
                .where(StockLot.id.in_(lot_ids))
                .order_by(StockLot.id)
                .with_for_update()
            )
        )
        .scalars()
        .all()
    )
    locked_lots = {lot_id: await session.get(StockLot, lot_id) for lot_id in locked_lot_ids}
    for allocation in allocations:
        lot = locked_lots.get(allocation.stock_lot_id)
        if lot is None or lot.inventory_role != StockLotRole.DRAGONZAP_MATERIAL:
            raise ValueError(f"Материальная партия {allocation.stock_lot_id} недоступна")
        if lot.remaining_quantity < allocation.planned_quantity:
            raise ValueError(
                f"В партии {lot.id} осталось {lot.remaining_quantity} шт., "
                f"требуется {allocation.planned_quantity} шт."
            )

    timestamp = now_moscow()
    for item in items:
        packaging_unit = _unit_cost(item.packaging_cost / Decimal(item.planned_quantity))
        for allocation in item.allocations:
            lot = locked_lots[allocation.stock_lot_id]
            quantity = int(allocation.planned_quantity)
            lot.remaining_quantity -= quantity
            await _apply_stock_delta(
                session,
                autopart_id=lot.autopart_id,
                storage_location_id=lot.storage_location_id,
                quantity_delta=-quantity,
                movement_type=MovementType.PRODUCTION_CONSUME,
                reference_id=wave.id,
                reference_type="production_wave",
                notes=f"Материал для волны {wave.number}",
                stock_lot_id=lot.id,
            )
            output_unit_cost = _unit_cost(_decimal(allocation.unit_material_cost) + packaging_unit)
            output_lot = await _create_stock_lot(
                session,
                autopart_id=item.finished_autopart_id,
                storage_location_id=location.id,
                quantity=quantity,
                source_type=LotSourceType.PRODUCTION,
                gtd_number=allocation.gtd_number,
                country_code=allocation.country_code,
                country_name=allocation.country_name,
                cost_price=output_unit_cost,
                inventory_role=StockLotRole.DRAGONZAP_FINISHED,
                role_source=StockLotRoleSource.PRODUCTION,
                role_rule_reference=f"production_wave:{wave.id}",
                role_change_reason=f"Выпуск по волне {wave.number}",
                marking_codes=list(allocation.marking_codes or []),
            )
            await _apply_stock_delta(
                session,
                autopart_id=item.finished_autopart_id,
                storage_location_id=location.id,
                quantity_delta=quantity,
                movement_type=MovementType.PRODUCTION_OUTPUT,
                reference_id=wave.id,
                reference_type="production_wave",
                notes=f"Выпуск готовой продукции по волне {wave.number}",
                stock_lot_id=output_lot.id,
            )
            codes = list(allocation.marking_codes or [])
            if codes:
                code_rows = (
                    (
                        await session.execute(
                            select(ProductMarkingCode).where(
                                ProductMarkingCode.code.in_(codes),
                                ProductMarkingCode.stock_lot_id == lot.id,
                            )
                        )
                    )
                    .scalars()
                    .all()
                )
                for code_row in code_rows:
                    code_row.autopart_id = item.finished_autopart_id
                    code_row.warehouse_id = warehouse.id
                    code_row.storage_location_id = location.id
                    code_row.stock_lot_id = output_lot.id
                    code_row.status = MarkingCodeStatus.IN_STOCK
                    session.add(
                        ProductMarkingCodeMovement(
                            marking_code_id=code_row.id,
                            movement_type=MarkingMovementType.PRODUCTION,
                            autopart_id=item.finished_autopart_id,
                            stock_lot_id=output_lot.id,
                            metadata_json={
                                "wave_id": wave.id,
                                "allocation_id": allocation.id,
                                "source_stock_lot_id": lot.id,
                            },
                        )
                    )
                lot.marking_codes = [
                    code for code in (lot.marking_codes or []) if code not in codes
                ]
            allocation.output_stock_lot_id = output_lot.id
            allocation.consumed_quantity = quantity
            allocation.consumed_at = timestamp
        item.produced_quantity = item.planned_quantity

    wave.status = ProductionWaveStatus.COMPLETED
    wave.total_produced_quantity = wave.total_planned_quantity
    wave.completed_by_user_id = user_id
    wave.completed_at = timestamp
    wave.error_message = None
    wave.sync_status = SyncStatus.PENDING
    await session.flush()
    from dz_fastapi.services.one_c_outbox import enqueue_production_wave_event

    await enqueue_production_wave_event(session, wave.id)
    return await get_production_wave(session, wave_id)


async def cancel_production_wave(
    session: AsyncSession,
    *,
    wave_id: int,
    user_id: int,
) -> ProductionWave:
    wave = await _lock_wave(session, wave_id)
    if wave.status in (
        ProductionWaveStatus.COMPLETED,
        ProductionWaveStatus.CANCELLED,
    ):
        raise ValueError("Завершённую или отменённую волну отменить нельзя")
    consumed = int(
        (
            await session.execute(
                select(func.coalesce(func.sum(ProductionWaveAllocation.consumed_quantity), 0))
                .join(
                    ProductionWaveItem,
                    ProductionWaveItem.id == ProductionWaveAllocation.wave_item_id,
                )
                .where(ProductionWaveItem.wave_id == wave_id)
            )
        ).scalar_one()
    )
    if consumed:
        raise ValueError("По волне уже есть фактическое списание материала")
    wave.status = ProductionWaveStatus.CANCELLED
    wave.cancelled_by_user_id = user_id
    wave.cancelled_at = now_moscow()
    await session.flush()
    return await get_production_wave(session, wave_id)


def _wave_options():
    return (
        selectinload(ProductionWave.items)
        .joinedload(ProductionWaveItem.finished_autopart)
        .joinedload(AutoPart.brand),
        selectinload(ProductionWave.items).selectinload(ProductionWaveItem.demands),
        selectinload(ProductionWave.items)
        .selectinload(ProductionWaveItem.allocations)
        .joinedload(ProductionWaveAllocation.material_autopart)
        .joinedload(AutoPart.brand),
        selectinload(ProductionWave.items)
        .selectinload(ProductionWaveItem.allocations)
        .selectinload(ProductionWaveAllocation.stock_lot),
        selectinload(ProductionWave.items)
        .selectinload(ProductionWaveItem.allocations)
        .selectinload(ProductionWaveAllocation.storage_location),
    )


async def _attach_part_snapshots(
    session: AsyncSession,
    waves: list[ProductionWave],
) -> None:
    part_ids = {
        part_id
        for wave in waves
        for item in wave.items
        for part_id in (
            [item.finished_autopart_id]
            + [allocation.material_autopart_id for allocation in item.allocations]
        )
    }
    snapshots = (
        {
            int(part_id): {
                "brand": brand,
                "oem_number": oem_number,
                "name": name,
            }
            for part_id, brand, oem_number, name in (
                await session.execute(
                    select(
                        AutoPart.id,
                        Brand.name,
                        AutoPart.oem_number,
                        AutoPart.name,
                    )
                    .join(Brand, Brand.id == AutoPart.brand_id)
                    .where(AutoPart.id.in_(part_ids))
                )
            ).all()
        }
        if part_ids
        else {}
    )
    for wave in waves:
        wave._production_part_snapshots = snapshots


async def get_production_wave(
    session: AsyncSession,
    wave_id: int,
) -> ProductionWave:
    wave = (
        (
            await session.execute(
                select(ProductionWave)
                .where(ProductionWave.id == wave_id)
                .options(*_wave_options())
                .execution_options(populate_existing=True)
            )
        )
        .scalars()
        .unique()
        .one_or_none()
    )
    if wave is None:
        raise LookupError("Производственная волна не найдена")
    await _attach_part_snapshots(session, [wave])
    return wave


async def list_production_waves(
    session: AsyncSession,
    *,
    status: Optional[ProductionWaveStatus] = None,
    limit: int = 50,
    offset: int = 0,
) -> tuple[list[ProductionWave], int]:
    filters = [ProductionWave.status == status] if status is not None else []
    total = int(
        (await session.execute(select(func.count(ProductionWave.id)).where(*filters))).scalar_one()
    )
    waves = (
        (
            await session.execute(
                select(ProductionWave)
                .where(*filters)
                .options(*_wave_options())
                .order_by(ProductionWave.created_at.desc(), ProductionWave.id.desc())
                .limit(limit)
                .offset(offset)
            )
        )
        .scalars()
        .unique()
        .all()
    )
    await _attach_part_snapshots(session, waves)
    return waves, total


def production_wave_to_dict(wave: ProductionWave) -> dict[str, Any]:
    def user_name(user) -> Optional[str]:
        return (user.name or user.email) if user else None

    part_snapshots = getattr(wave, "_production_part_snapshots", {})
    return {
        "id": wave.id,
        "number": wave.number,
        "warehouse_id": wave.warehouse_id,
        "warehouse_name": wave.warehouse.name if wave.warehouse else None,
        "status": _enum_value(wave.status),
        "source": _enum_value(wave.source),
        "cutoff_at": wave.cutoff_at,
        "notes": wave.notes,
        "error_message": wave.error_message,
        "total_planned_quantity": wave.total_planned_quantity,
        "total_produced_quantity": wave.total_produced_quantity,
        "total_material_cost": wave.total_material_cost,
        "total_packaging_cost": wave.total_packaging_cost,
        "total_finished_cost": wave.total_finished_cost,
        "created_by_name": user_name(wave.created_by_user),
        "planned_by_name": user_name(wave.planned_by_user),
        "started_by_name": user_name(wave.started_by_user),
        "completed_by_name": user_name(wave.completed_by_user),
        "cancelled_by_name": user_name(wave.cancelled_by_user),
        "created_at": wave.created_at,
        "planned_at": wave.planned_at,
        "started_at": wave.started_at,
        "completed_at": wave.completed_at,
        "cancelled_at": wave.cancelled_at,
        "external_id": wave.external_id,
        "sync_status": _enum_value(wave.sync_status),
        "items": [
            {
                "id": item.id,
                "production_group_id": item.production_group_id,
                "finished_autopart_id": item.finished_autopart_id,
                "finished_brand": part_snapshots.get(item.finished_autopart_id, {}).get("brand"),
                "finished_oem_number": part_snapshots.get(item.finished_autopart_id, {}).get(
                    "oem_number"
                ),
                "finished_name": part_snapshots.get(item.finished_autopart_id, {}).get("name"),
                "planned_quantity": item.planned_quantity,
                "produced_quantity": item.produced_quantity,
                "shortage_quantity": item.shortage_quantity,
                "planning_error": item.planning_error,
                "material_cost": item.material_cost,
                "packaging_cost": item.packaging_cost,
                "total_cost": item.total_cost,
                "unit_cost": item.unit_cost,
                "demands": [
                    {
                        "id": demand.id,
                        "stock_order_item_id": demand.stock_order_item_id,
                        "customer_order_item_id": demand.customer_order_item_id,
                        "customer_order_id": demand.customer_order_id,
                        "customer_id": demand.customer_id,
                        "customer_name": demand.customer_name,
                        "order_number": demand.order_number,
                        "order_date": demand.order_date,
                        "requested_brand": demand.requested_brand,
                        "requested_oem": demand.requested_oem,
                        "requested_name": demand.requested_name,
                        "quantity": demand.quantity,
                    }
                    for demand in item.demands
                ],
                "allocations": [
                    {
                        "id": allocation.id,
                        "material_autopart_id": allocation.material_autopart_id,
                        "material_brand": part_snapshots.get(
                            allocation.material_autopart_id, {}
                        ).get("brand"),
                        "material_oem_number": part_snapshots.get(
                            allocation.material_autopart_id, {}
                        ).get("oem_number"),
                        "material_name": part_snapshots.get(
                            allocation.material_autopart_id, {}
                        ).get("name"),
                        "stock_lot_id": allocation.stock_lot_id,
                        "storage_location_id": allocation.storage_location_id,
                        "storage_location_name": (
                            allocation.storage_location.name
                            if allocation.storage_location
                            else None
                        ),
                        "output_stock_lot_id": allocation.output_stock_lot_id,
                        "planned_quantity": allocation.planned_quantity,
                        "consumed_quantity": allocation.consumed_quantity,
                        "unit_material_cost": allocation.unit_material_cost,
                        "total_material_cost": allocation.total_material_cost,
                        "gtd_number": allocation.gtd_number,
                        "country_code": allocation.country_code,
                        "country_name": allocation.country_name,
                        "marking_codes_count": len(allocation.marking_codes or []),
                    }
                    for allocation in item.allocations
                ],
            }
            for item in wave.items
        ],
    }
