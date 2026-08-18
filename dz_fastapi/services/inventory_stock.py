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
from decimal import ROUND_HALF_UP, Decimal
from typing import Optional

from sqlalchemy import asc, func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload, selectinload

from dz_fastapi.core.time import now_moscow
from dz_fastapi.models.autopart import (
    AutoPart,
    LocationType,
    StorageLocation,
    autopart_storage_association,
)
from dz_fastapi.models.brand import Brand
from dz_fastapi.models.inventory import (
    LotSourceType,
    MovementType,
    ProductionWave,
    ProductionWaveAllocation,
    ProductionWaveItem,
    ProductionWaveStatus,
    ReserveStatus,
    ReturnDocumentStatus,
    ReturnFromCustomer,
    ReturnItem,
    ReturnToSupplier,
    ShipmentDocument,
    ShipmentDocumentItem,
    ShipmentDocumentItemLotAllocation,
    ShipmentDocumentStatus,
    StockByLocation,
    StockDocument,
    StockDocumentStatus,
    StockDocumentType,
    StockLot,
    StockLotRole,
    StockLotRoleChange,
    StockLotRoleSource,
    StockMovement,
    StockOrderPackage,
    StockReserve,
    SyncStatus,
    Warehouse,
)
from dz_fastapi.models.partner import (
    PROVIDER_INVENTORY_POLICY,
    STOCK_ORDER_STATUS,
    CustomerOrderItem,
    Provider,
    ProviderInventoryRoleRule,
    StockOrder,
    StockOrderItem,
    SupplierReceipt,
    SupplierReceiptItem,
)
from dz_fastapi.services.credit_control import (
    assert_shipment_credit_available,
    check_shipment_credit_policy,
)
from dz_fastapi.services.marking_codes import (
    allocate_marking_codes_for_shipment_allocation,
    register_receipt_marking_codes,
    release_marking_codes_for_shipment_allocation,
    return_marking_codes_from_customer,
    return_marking_codes_to_supplier,
)
from dz_fastapi.services.stock_order_packages import assert_stock_order_packing_ready

logger = logging.getLogger(__name__)

DEFAULT_WAREHOUSE_NAME = "Основной склад"
DEFAULT_WAREHOUSE_COMMENT = "Склад по умолчанию для входящих документов и первичного размещения."
RECEIVING_LOCATION_CODE = "RECEIVING"
UNIT_COST_PRECISION = Decimal("0.0001")
MONEY_PRECISION = Decimal("0.01")


def _to_decimal(value) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def _quantize_unit_cost(value: Decimal | None) -> Decimal | None:
    if value is None:
        return None
    return value.quantize(UNIT_COST_PRECISION, rounding=ROUND_HALF_UP)


def _quantize_money(value: Decimal | None) -> Decimal | None:
    if value is None:
        return None
    return value.quantize(MONEY_PRECISION, rounding=ROUND_HALF_UP)


def _derive_receipt_unit_cost(item: SupplierReceiptItem) -> Decimal | None:
    direct_price = _to_decimal(getattr(item, "price", None))
    if direct_price is not None:
        return _quantize_unit_cost(direct_price)

    total_with_vat = _to_decimal(getattr(item, "total_price_with_vat", None))
    received_quantity = int(getattr(item, "received_quantity", 0) or 0)
    if total_with_vat is None or received_quantity <= 0:
        return None
    return _quantize_unit_cost(total_with_vat / Decimal(received_quantity))


async def _infer_stock_lot_role_from_brand(
    session: AsyncSession,
    *,
    autopart_id: int,
) -> StockLotRole:
    brand_name = (
        await session.execute(
            select(Brand.name)
            .join(AutoPart, AutoPart.brand_id == Brand.id)
            .where(AutoPart.id == autopart_id)
        )
    ).scalar_one_or_none()
    return (
        StockLotRole.DRAGONZAP_FINISHED
        if str(brand_name or "").strip().casefold() == "dragonzap"
        else StockLotRole.ORIGINAL_GOOD
    )


async def resolve_receipt_inventory_role(
    session: AsyncSession,
    *,
    provider_id: int,
    autopart_id: int,
) -> tuple[StockLotRole, StockLotRoleSource, str, str]:
    """Resolve incoming role using item rule, provider policy, safe fallback."""
    rule = (
        await session.execute(
            select(ProviderInventoryRoleRule).where(
                ProviderInventoryRoleRule.provider_id == provider_id,
                ProviderInventoryRoleRule.autopart_id == autopart_id,
                ProviderInventoryRoleRule.is_active.is_(True),
            )
        )
    ).scalar_one_or_none()
    if rule is not None:
        return (
            StockLotRole(rule.inventory_role),
            StockLotRoleSource.ITEM_RULE,
            f"provider_inventory_role_rule:{rule.id}",
            rule.reason or "Роль назначена по точному правилу поставщика и номенклатуры",
        )

    provider_settings = (
        await session.execute(
            select(
                Provider.inventory_policy,
                Provider.inventory_policy_note,
            ).where(Provider.id == provider_id)
        )
    ).one_or_none()
    provider_policy = provider_settings[0] if provider_settings else None
    provider_note = str(provider_settings[1] or "").strip() if provider_settings else ""
    policy = PROVIDER_INVENTORY_POLICY(provider_policy or PROVIDER_INVENTORY_POLICY.ORIGINAL_GOODS)
    if policy == PROVIDER_INVENTORY_POLICY.DRAGONZAP_MATERIAL:
        role = StockLotRole.DRAGONZAP_MATERIAL
        reason = "Поставщик настроен как источник материала DragonZap"
    elif policy == PROVIDER_INVENTORY_POLICY.MIXED:
        role = StockLotRole.ORIGINAL_GOOD
        reason = (
            "Для смешанного поставщика нет точного правила: "
            "применён безопасный режим обычного товара"
        )
    else:
        role = StockLotRole.ORIGINAL_GOOD
        reason = "Поставщик настроен как источник обычного товара"
    if provider_note:
        reason = f"{reason}. Пояснение: {provider_note}"

    return (
        role,
        StockLotRoleSource.PROVIDER_POLICY,
        f"provider_inventory_policy:{provider_id}:{policy.value}",
        reason,
    )


def _weighted_average_cost_from_lots(lots: list[StockLot]) -> Decimal | None:
    total_quantity = 0
    total_cost = Decimal("0")
    for lot in lots:
        quantity = int(lot.remaining_quantity or 0)
        unit_cost = _quantize_unit_cost(_to_decimal(lot.cost_price))
        if quantity <= 0 or unit_cost is None:
            continue
        total_quantity += quantity
        total_cost += unit_cost * Decimal(quantity)
    if total_quantity <= 0:
        return None
    return _quantize_unit_cost(total_cost / Decimal(total_quantity))


async def _infer_autopart_cost_price(
    session: AsyncSession,
    *,
    autopart_id: int,
    storage_location_id: Optional[int],
) -> Decimal | None:
    """Infer an accounting unit cost for synthetic lots.

    Used for opening balances and positive inventory corrections where we
    create stock without a direct source receipt. We prefer live lots from the
    same location, then live lots globally, then the latest known historical
    cost for the part.
    """
    scoped_active_stmt = select(StockLot).where(
        StockLot.autopart_id == autopart_id,
        StockLot.remaining_quantity > 0,
        StockLot.cost_price.is_not(None),
    )
    if storage_location_id is not None:
        scoped_active_stmt = scoped_active_stmt.where(
            StockLot.storage_location_id == storage_location_id
        )
    scoped_active_lots = (
        (
            await session.execute(
                scoped_active_stmt.order_by(
                    asc(StockLot.received_at),
                    asc(StockLot.id),
                )
            )
        )
        .scalars()
        .all()
    )
    inferred = _weighted_average_cost_from_lots(scoped_active_lots)
    if inferred is not None:
        return inferred

    global_active_lots = (
        (
            await session.execute(
                select(StockLot)
                .where(
                    StockLot.autopart_id == autopart_id,
                    StockLot.remaining_quantity > 0,
                    StockLot.cost_price.is_not(None),
                )
                .order_by(asc(StockLot.received_at), asc(StockLot.id))
            )
        )
        .scalars()
        .all()
    )
    inferred = _weighted_average_cost_from_lots(global_active_lots)
    if inferred is not None:
        return inferred

    latest_known_stmt = select(StockLot.cost_price).where(
        StockLot.autopart_id == autopart_id,
        StockLot.cost_price.is_not(None),
    )
    if storage_location_id is not None:
        scoped_latest = (
            await session.execute(
                latest_known_stmt.where(StockLot.storage_location_id == storage_location_id)
                .order_by(StockLot.received_at.desc(), StockLot.id.desc())
                .limit(1)
            )
        ).scalar_one_or_none()
        scoped_latest_cost = _quantize_unit_cost(_to_decimal(scoped_latest))
        if scoped_latest_cost is not None:
            return scoped_latest_cost

    latest_known = (
        await session.execute(
            latest_known_stmt.order_by(
                StockLot.received_at.desc(),
                StockLot.id.desc(),
            ).limit(1)
        )
    ).scalar_one_or_none()
    return _quantize_unit_cost(_to_decimal(latest_known))


# ═══════════════════════════════════════════════════════════════════════════════
# Warehouse / location helpers
# ═══════════════════════════════════════════════════════════════════════════════


def _normalize_system_location_name(warehouse_id: int) -> str:
    return f"WH{int(warehouse_id)} RECEIVING"


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
        warehouse = await get_warehouse_by_id(session, int(explicit_warehouse_id))
        if warehouse is None:
            raise LookupError("Склад не найден")
        return warehouse

    if provider_id is not None:
        provider = await session.get(Provider, int(provider_id))
        if provider is not None and provider.default_warehouse_id is not None:
            warehouse = await get_warehouse_by_id(session, int(provider.default_warehouse_id))
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

    oem_number = str(item.oem_number or "").strip()
    if not oem_number:
        return None

    from dz_fastapi.models.autopart import AutoPart  # avoid circular import

    parts = (
        (
            await session.execute(
                select(AutoPart)
                .where(AutoPart.oem_number == oem_number)
                .options(selectinload(AutoPart.brand))
            )
        )
        .scalars()
        .all()
    )
    if not parts:
        return None
    if len(parts) == 1:
        return int(parts[0].id)

    brand_name = str(item.brand_name or "").strip()
    if not brand_name:
        return None

    normalized_brand = brand_name.casefold()
    for part in parts:
        brand = getattr(part, "brand", None)
        if brand and str(brand.name or "").strip().casefold() == normalized_brand:
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
        autopart_storage_association.c.storage_location_id == storage_location_id,
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
            f"Недостаточно остатка для движения: "
            f"autopart_id={autopart_id} location_id={storage_location_id} "
            f"before={qty_before} delta={quantity_delta}"
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
    cost_price: Optional[Decimal] = None,
    inventory_role: Optional[StockLotRole] = None,
    role_source: StockLotRoleSource = StockLotRoleSource.SYSTEM_DEFAULT,
    role_rule_reference: Optional[str] = None,
    role_change_reason: Optional[str] = None,
    marking_codes: Optional[list[str]] = None,
) -> StockLot:
    """Insert a new StockLot and return it (flushed, so .id is available)."""
    if inventory_role is None:
        inventory_role = await _infer_stock_lot_role_from_brand(
            session,
            autopart_id=autopart_id,
        )

    changed_at = now_moscow()
    change_reason = role_change_reason or ("Назначено автоматически по бренду номенклатуры")
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
        cost_price=_quantize_unit_cost(_to_decimal(cost_price)),
        inventory_role=inventory_role,
        role_source=role_source,
        role_rule_reference=role_rule_reference,
        role_changed_at=changed_at,
        role_change_reason=change_reason,
        marking_codes=list(marking_codes or []),
    )
    session.add(lot)
    await session.flush()
    session.add(
        StockLotRoleChange(
            stock_lot_id=lot.id,
            old_role=None,
            new_role=inventory_role,
            source=role_source,
            rule_reference=role_rule_reference,
            reason=change_reason,
            changed_at=changed_at,
        )
    )
    await session.flush()
    return lot


async def change_stock_lot_role(
    session: AsyncSession,
    *,
    lot_id: int,
    new_role: StockLotRole,
    changed_by_user_id: int,
    reason: str,
    rule_reference: Optional[str] = None,
) -> StockLot:
    """Change a lot role as an explicit audited warehouse operation."""
    normalized_reason = str(reason or "").strip()
    if len(normalized_reason) < 3:
        raise ValueError("Укажите причину изменения роли партии")

    locked_lot_id = (
        await session.execute(select(StockLot.id).where(StockLot.id == lot_id).with_for_update())
    ).scalar_one_or_none()
    if locked_lot_id is None:
        raise LookupError("Партия не найдена")
    lot = await session.get(StockLot, locked_lot_id)

    old_role = StockLotRole(lot.inventory_role)
    new_role = StockLotRole(new_role)
    if old_role == new_role:
        raise ValueError("У партии уже установлена выбранная роль")

    changed_at = now_moscow()
    normalized_reference = str(rule_reference or "").strip() or None
    lot.inventory_role = new_role
    lot.role_source = StockLotRoleSource.MANUAL
    lot.role_rule_reference = normalized_reference
    lot.role_changed_by_user_id = changed_by_user_id
    lot.role_changed_at = changed_at
    lot.role_change_reason = normalized_reason
    session.add(
        StockLotRoleChange(
            stock_lot_id=lot.id,
            old_role=old_role,
            new_role=new_role,
            source=StockLotRoleSource.MANUAL,
            rule_reference=normalized_reference,
            reason=normalized_reason,
            changed_by_user_id=changed_by_user_id,
            changed_at=changed_at,
        )
    )
    await session.flush()
    return lot


async def _consume_fifo(
    session: AsyncSession,
    *,
    autopart_id: int,
    storage_location_id: Optional[int],
    quantity: int,
    movement_type: MovementType,
    warehouse_id: Optional[int] = None,
    reference_id: Optional[int] = None,
    reference_type: Optional[str] = None,
    notes: Optional[str] = None,
) -> list[StockMovement]:
    """Internal FIFO engine.

    Deducts `quantity` units starting from the oldest lots.
    storage_location_id=None → FIFO across all matching locations.
    warehouse_id limits both lotted and legacy stock to one warehouse.
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
        lots_stmt = lots_stmt.where(StockLot.storage_location_id == storage_location_id)
    if warehouse_id is not None:
        lots_stmt = lots_stmt.join(
            StorageLocation,
            StorageLocation.id == StockLot.storage_location_id,
        ).where(StorageLocation.warehouse_id == warehouse_id)
    lots_stmt = lots_stmt.order_by(asc(StockLot.received_at), asc(StockLot.id))
    lots = (await session.execute(lots_stmt)).scalars().all()

    reserved_by_lot = (
        {
            int(lot_id): int(reserved or 0)
            for lot_id, reserved in (
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
                        ProductionWaveAllocation.stock_lot_id.in_([lot.id for lot in lots]),
                        ProductionWave.status.in_(
                            (
                                ProductionWaveStatus.PLANNED,
                                ProductionWaveStatus.IN_PROGRESS,
                            )
                        ),
                    )
                    .group_by(ProductionWaveAllocation.stock_lot_id)
                )
            ).all()
        }
        if lots
        else {}
    )

    free_lotted_quantity = sum(
        max(0, int(lot.remaining_quantity) - reserved_by_lot.get(lot.id, 0)) for lot in lots
    )
    stock_total_stmt = select(func.coalesce(func.sum(StockByLocation.quantity), 0)).where(
        StockByLocation.autopart_id == autopart_id
    )
    if storage_location_id is not None:
        stock_total_stmt = stock_total_stmt.where(
            StockByLocation.storage_location_id == storage_location_id
        )
    if warehouse_id is not None:
        stock_total_stmt = stock_total_stmt.join(
            StorageLocation,
            StorageLocation.id == StockByLocation.storage_location_id,
        ).where(StorageLocation.warehouse_id == warehouse_id)
    stock_total = int((await session.execute(stock_total_stmt)).scalar_one() or 0)
    lotted_total = sum(int(lot.remaining_quantity) for lot in lots)
    unlotted_quantity = max(0, stock_total - lotted_total)
    if reserved_by_lot and quantity > free_lotted_quantity + unlotted_quantity:
        raise ValueError(
            "Свободного остатка недостаточно: часть партий закреплена за "
            "производственной волной DragonZap"
        )

    remaining_to_consume = quantity
    movements: list[StockMovement] = []

    for lot in lots:
        if remaining_to_consume <= 0:
            break
        free_quantity = max(
            0,
            int(lot.remaining_quantity) - reserved_by_lot.get(lot.id, 0),
        )
        take = min(free_quantity, remaining_to_consume)
        if take <= 0:
            continue
        lot.remaining_quantity -= take
        remaining_to_consume -= take

        effective_location = (
            storage_location_id if storage_location_id is not None else lot.storage_location_id
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
            sbl_stmt = select(StockByLocation).where(
                StockByLocation.autopart_id == autopart_id,
                StockByLocation.quantity > 0,
            )
            if warehouse_id is not None:
                sbl_stmt = sbl_stmt.join(
                    StorageLocation,
                    StorageLocation.id == StockByLocation.storage_location_id,
                ).where(StorageLocation.warehouse_id == warehouse_id)
            sbl_stmt = sbl_stmt.order_by(StockByLocation.id).limit(1)
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
    doc_receiving_location = await ensure_receiving_location(session, doc_warehouse)

    multiplier = -1 if reverse else 1
    note_prefix = "Распроведение поступления" if reverse else "Поступление"
    note_suffix = (
        f" ({receipt.document_number})" if str(receipt.document_number or "").strip() else ""
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

        item_warehouse_id = getattr(item, "warehouse_id", None)
        if item_warehouse_id and item_warehouse_id != doc_warehouse.id:
            if item_warehouse_id not in _item_location_cache:
                item_wh = await get_warehouse_by_id(session, item_warehouse_id)
                if item_wh is not None:
                    _item_location_cache[item_warehouse_id] = await ensure_receiving_location(
                        session, item_wh
                    )
                else:
                    _item_location_cache[item_warehouse_id] = doc_receiving_location
            receiving_location = _item_location_cache[item_warehouse_id]
        else:
            receiving_location = doc_receiving_location

        lot_id: Optional[int] = None
        if not reverse:
            lot_cost_price = _derive_receipt_unit_cost(item)
            (
                inventory_role,
                role_source,
                role_rule_reference,
                role_change_reason,
            ) = await resolve_receipt_inventory_role(
                session,
                provider_id=receipt.provider_id,
                autopart_id=autopart_id,
            )
            lot = await _create_stock_lot(
                session,
                autopart_id=autopart_id,
                storage_location_id=receiving_location.id,
                quantity=quantity,
                source_type=LotSourceType.RECEIPT,
                gtd_number=getattr(item, "gtd_code", None),
                country_code=getattr(item, "country_code", None),
                country_name=getattr(item, "country_name", None),
                source_receipt_id=receipt.id,
                source_receipt_item_id=item.id,
                received_at=received_at,
                cost_price=lot_cost_price,
                inventory_role=inventory_role,
                role_source=role_source,
                role_rule_reference=role_rule_reference,
                role_change_reason=role_change_reason,
            )
            lot_id = lot.id
            await register_receipt_marking_codes(
                session,
                receipt_item=item,
                stock_lot=lot,
                codes=getattr(item, "marking_codes", None),
            )
        else:
            # Reverse: use the lot's actual
            # remaining qty (some may be consumed)
            # so we never try to make SBL go below zero.
            lot_stmt = select(StockLot).where(StockLot.source_receipt_item_id == item.id)
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
            reference_type="supplier_receipt",
            notes=f"{note_prefix} #{receipt.id}{note_suffix}",
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
        raise LookupError("Документ поступления не найден")
    if reverse:
        await _sync_receipt_customer_assembly(
            session,
            receipt=receipt,
            reverse=True,
        )
        await receive_stock(session, receipt=receipt, reverse=True)
        return

    await receive_stock(session, receipt=receipt, reverse=False)
    await _sync_receipt_customer_assembly(
        session,
        receipt=receipt,
        reverse=False,
    )


async def _sync_receipt_customer_assembly(
    session: AsyncSession,
    *,
    receipt: SupplierReceipt,
    reverse: bool,
) -> dict:
    """Add or remove posted cross-docking lines in customer assembly."""
    linked_items = [
        item
        for item in (receipt.items or [])
        if item.customer_order_item_id is not None and int(item.received_quantity or 0) > 0
    ]
    if not linked_items:
        return {"created": 0, "updated": 0, "removed": 0}

    receipt_item_ids = [int(item.id) for item in linked_items]
    existing_rows = (
        (
            await session.execute(
                select(StockOrderItem)
                .options(joinedload(StockOrderItem.stock_order))
                .where(StockOrderItem.supplier_receipt_item_id.in_(receipt_item_ids))
            )
        )
        .scalars()
        .all()
    )
    existing_by_receipt_item = {
        int(row.supplier_receipt_item_id): row
        for row in existing_rows
        if row.supplier_receipt_item_id is not None
    }

    if reverse:
        affected_order_ids: set[int] = set()
        for row in existing_rows:
            stock_order = row.stock_order
            if (
                stock_order is None
                or stock_order.status != STOCK_ORDER_STATUS.NEW
                or int(row.picked_quantity or 0) > 0
            ):
                raise ValueError(
                    "Нельзя распровести поступление: " "cross-docking уже подобран или отгружен"
                )
            affected_order_ids.add(int(row.stock_order_id))
            await session.delete(row)
        await session.flush()

        for stock_order_id in affected_order_ids:
            remaining = (
                (
                    await session.execute(
                        select(StockOrderItem).where(
                            StockOrderItem.stock_order_id == stock_order_id
                        )
                    )
                )
                .scalars()
                .all()
            )
            stock_order = await session.get(StockOrder, stock_order_id)
            if stock_order is None:
                continue
            if not remaining:
                await session.delete(stock_order)
                continue
            stock_order.status = (
                STOCK_ORDER_STATUS.COMPLETED
                if all(
                    int(item.picked_quantity or 0) >= int(item.quantity or 0) for item in remaining
                )
                else STOCK_ORDER_STATUS.NEW
            )
        await session.flush()
        return {"created": 0, "updated": 0, "removed": len(existing_rows)}

    created = 0
    updated = 0
    order_cache: dict[tuple[int, int], StockOrder] = {}

    for receipt_item in linked_items:
        autopart_id = await resolve_receipt_item_autopart_id(session, receipt_item)
        if autopart_id is None:
            raise ValueError(
                f"Не определена номенклатура cross-docking "
                f"для строки поступления #{receipt_item.id}"
            )
        receipt_item.autopart_id = int(autopart_id)

        existing = existing_by_receipt_item.get(int(receipt_item.id))
        lot_conditions = [
            StockLot.source_receipt_item_id == receipt_item.id,
        ]
        if existing is None:
            lot_conditions.append(StockLot.remaining_quantity > 0)
        preferred_lot_id = (
            await session.execute(
                select(StockLot.id)
                .where(*lot_conditions)
                .order_by(asc(StockLot.received_at), asc(StockLot.id))
                .limit(1)
            )
        ).scalar_one_or_none()
        if preferred_lot_id is None:
            raise ValueError(
                f"Не найдена партия cross-docking для " f"строки поступления #{receipt_item.id}"
            )

        customer_item = (
            await session.execute(
                select(CustomerOrderItem)
                .options(joinedload(CustomerOrderItem.order))
                .where(CustomerOrderItem.id == receipt_item.customer_order_item_id)
            )
        ).scalar_one_or_none()
        if customer_item is None or customer_item.order is None:
            raise ValueError(
                f"Не найден заказ клиента для " f"строки поступления #{receipt_item.id}"
            )
        customer_id = int(customer_item.order.customer_id)
        customer_order_id = int(customer_item.order_id)

        if existing is not None:
            unchanged = (
                int(existing.autopart_id or 0) == int(autopart_id)
                and int(existing.quantity or 0) == int(receipt_item.received_quantity)
                and int(existing.preferred_stock_lot_id or 0) == int(preferred_lot_id)
            )
            if unchanged:
                continue
            if int(existing.picked_quantity or 0) > 0:
                raise ValueError(
                    "Нельзя изменить поступление: " "строка cross-docking уже подобрана"
                )
            existing.autopart_id = int(autopart_id)
            existing.quantity = int(receipt_item.received_quantity)
            existing.preferred_stock_lot_id = int(preferred_lot_id)
            if existing.stock_order is not None:
                existing.stock_order.status = STOCK_ORDER_STATUS.NEW
            updated += 1
            continue

        cache_key = (customer_id, customer_order_id)
        stock_order = order_cache.get(cache_key)
        if stock_order is None:
            stock_order = (
                await session.execute(
                    select(StockOrder)
                    .join(StockOrderItem)
                    .join(
                        CustomerOrderItem,
                        CustomerOrderItem.id == StockOrderItem.customer_order_item_id,
                    )
                    .where(
                        CustomerOrderItem.order_id == customer_order_id,
                        StockOrder.status.in_(
                            (
                                STOCK_ORDER_STATUS.NEW,
                                STOCK_ORDER_STATUS.COMPLETED,
                            )
                        ),
                        StockOrder.shipment_document_id.is_(None),
                    )
                    .order_by(StockOrder.created_at.asc(), StockOrder.id.asc())
                    .limit(1)
                )
            ).scalar_one_or_none()
        if stock_order is None:
            stock_order = (
                await session.execute(
                    select(StockOrder)
                    .where(
                        StockOrder.customer_id == customer_id,
                        StockOrder.status.in_(
                            (
                                STOCK_ORDER_STATUS.NEW,
                                STOCK_ORDER_STATUS.COMPLETED,
                            )
                        ),
                        StockOrder.shipment_document_id.is_(None),
                    )
                    .order_by(StockOrder.created_at.asc(), StockOrder.id.asc())
                    .limit(1)
                )
            ).scalar_one_or_none()
        if stock_order is None:
            stock_order = StockOrder(
                customer_id=customer_id,
                status=STOCK_ORDER_STATUS.NEW,
                packing_required=True,
            )
            session.add(stock_order)
            await session.flush()
        order_cache[cache_key] = stock_order
        stock_order.packing_required = True
        stock_order.status = STOCK_ORDER_STATUS.NEW
        session.add(
            StockOrderItem(
                stock_order_id=stock_order.id,
                customer_order_item_id=customer_item.id,
                supplier_receipt_item_id=receipt_item.id,
                preferred_stock_lot_id=int(preferred_lot_id),
                autopart_id=int(autopart_id),
                quantity=int(receipt_item.received_quantity),
                picked_quantity=0,
            )
        )
        created += 1

    await session.flush()
    return {"created": created, "updated": updated, "removed": 0}


async def sync_posted_cross_docking_assemblies(
    session: AsyncSession,
) -> dict:
    """Backfill assembly lines for already posted linked receipts."""
    receipts = (
        (
            await session.execute(
                select(SupplierReceipt)
                .join(SupplierReceiptItem)
                .outerjoin(
                    StockOrderItem,
                    StockOrderItem.supplier_receipt_item_id == SupplierReceiptItem.id,
                )
                .where(
                    SupplierReceipt.posted_at.is_not(None),
                    SupplierReceiptItem.customer_order_item_id.is_not(None),
                    SupplierReceiptItem.received_quantity > 0,
                    StockOrderItem.id.is_(None),
                )
                .options(selectinload(SupplierReceipt.items))
                .order_by(SupplierReceipt.id.asc())
            )
        )
        .scalars()
        .unique()
        .all()
    )
    created = 0
    updated = 0
    errors: list[dict[str, str | int]] = []
    for receipt in receipts:
        try:
            async with session.begin_nested():
                result = await _sync_receipt_customer_assembly(
                    session,
                    receipt=receipt,
                    reverse=False,
                )
        except (LookupError, ValueError) as exc:
            errors.append(
                {
                    "receipt_id": int(receipt.id),
                    "error": str(exc),
                }
            )
            continue
        created += int(result.get("created", 0))
        updated += int(result.get("updated", 0))
    await session.flush()
    return {
        "receipts_processed": len(receipts),
        "items_created": created,
        "items_updated": updated,
        "receipts_skipped": len(errors),
        "errors": errors,
    }


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
    notes = reason or "Ручное списание"
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
        f"Коррекция инвентаризации #{inventory_session_id}"
        if inventory_session_id
        else "Коррекция инвентаризации"
    )

    if delta > 0:
        # Излишек — создаём лот
        inferred_cost_price = await _infer_autopart_cost_price(
            session,
            autopart_id=autopart_id,
            storage_location_id=storage_location_id,
        )
        lot = await _create_stock_lot(
            session,
            autopart_id=autopart_id,
            storage_location_id=storage_location_id,
            quantity=delta,
            source_type=LotSourceType.INVENTORY_CORRECTION,
            cost_price=inferred_cost_price,
        )
        mv = await _apply_stock_delta(
            session,
            autopart_id=autopart_id,
            storage_location_id=storage_location_id,
            quantity_delta=delta,
            movement_type=MovementType.INVENTORY,
            reference_id=inventory_session_id,
            reference_type="inventory",
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
            reference_type="inventory",
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
        raise ValueError("Количество должно быть > 0")

    note = notes or (f"Перемещение: loc#{from_location_id} → loc#{to_location_id}")

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
            StockLot.inventory_role == lot.inventory_role,
            StockLot.gtd_number == lot.gtd_number,
            StockLot.source_receipt_id == lot.source_receipt_id,
            StockLot.source_document_item_id == lot.source_document_item_id,
        )
        dest_lot = (await session.execute(dest_lot_stmt)).scalar_one_or_none()

        if dest_lot is not None:
            dest_lot.remaining_quantity += take
            dest_lot.initial_quantity += take
            if dest_lot.cost_price is None and lot.cost_price is not None:
                dest_lot.cost_price = lot.cost_price
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
                received_at=lot.received_at,  # preserve original date!
                cost_price=lot.cost_price,
                inventory_role=lot.inventory_role,
                role_source=lot.role_source,
                role_rule_reference=lot.role_rule_reference,
                role_change_reason=(f"Роль унаследована при перемещении партии #{lot.id}"),
            )

        transferred_lots.append(
            {
                "lot_id": lot.id,
                "gtd_number": lot.gtd_number,
                "quantity": take,
            }
        )

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
            reference_type="transfer",
            notes=note,
        )
        in_movement = await _apply_stock_delta(
            session,
            autopart_id=autopart_id,
            storage_location_id=to_location_id,
            quantity_delta=remaining,
            movement_type=MovementType.TRANSFER_IN,
            reference_type="transfer",
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
            reference_type="transfer",
            notes=note,
        )
        lot_in = await _apply_stock_delta(
            session,
            autopart_id=autopart_id,
            storage_location_id=to_location_id,
            quantity_delta=lot_qty,
            movement_type=MovementType.TRANSFER_IN,
            reference_type="transfer",
            notes=note,
        )
        if lot_out:
            out_movement = lot_out
        if lot_in:
            in_movement = lot_in

    await session.flush()

    return {
        "autopart_id": autopart_id,
        "from_location_id": from_location_id,
        "to_location_id": to_location_id,
        "quantity": quantity,
        "lots_transferred": transferred_lots,
        "movement_out_id": out_movement.id if out_movement else None,
        "movement_in_id": in_movement.id if in_movement else None,
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
        raise LookupError("Документ не найден")
    if doc.status != StockDocumentStatus.DRAFT:
        raise ValueError(f"Документ не в статусе DRAFT (текущий: {doc.status})")

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
                "StockDocument item id=%s: no storage_location — skipping",
                item.id,
            )
            continue

        if doc.doc_type == StockDocumentType.MANUAL_RECEIPT:
            lot = await _create_stock_lot(
                session,
                autopart_id=item.autopart_id,
                storage_location_id=item.storage_location_id,
                quantity=qty,
                cost_price=item.cost_price,
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
                reference_type="stock_document",
                notes=doc.reason or f"Ручное оприходование #{doc.id}",
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
                reference_type="stock_document",
                notes=doc.reason or f"Ручное списание #{doc.id}",
            )
            movements_created += len(mvs)

        processed += 1

    doc.status = StockDocumentStatus.POSTED
    doc.posted_at = now_moscow()
    doc.sync_status = SyncStatus.PENDING
    await session.flush()
    from dz_fastapi.services.one_c_outbox import enqueue_stock_document_event

    await enqueue_stock_document_event(session, doc.id)

    return {
        "document_id": document_id,
        "doc_type": doc.doc_type,
        "items_processed": processed,
        "movements_created": movements_created,
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
        raise LookupError("Документ не найден")
    if doc.status != StockDocumentStatus.POSTED:
        raise ValueError(f"Документ не проведён (текущий статус: {doc.status})")

    if doc.doc_type == StockDocumentType.MANUAL_WRITEOFF:
        raise ValueError(
            "Распроведение списания не поддерживается автоматически. "
            "Создайте документ оприходования для корректировки."
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
            reference_type="stock_document",
            notes=f"Распроведение ручного оприходования #{doc.id}",
        )
        processed += 1

    doc.status = StockDocumentStatus.CANCELLED
    doc.sync_status = SyncStatus.PENDING
    await session.flush()
    from dz_fastapi.services.one_c_outbox import EVENT_CANCELLED, enqueue_stock_document_event

    await enqueue_stock_document_event(session, doc.id, EVENT_CANCELLED)

    return {
        "document_id": document_id,
        "items_reversed": processed,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# Use-case: dispatch_stock_order
# ═══════════════════════════════════════════════════════════════════════════════


async def dispatch_stock_order(
    session: AsyncSession,
    *,
    stock_order_id: int,
) -> dict:
    """Create and post one shipment document for a completed stock order."""
    stmt = (
        select(StockOrder)
        .options(
            selectinload(StockOrder.items).selectinload(StockOrderItem.customer_order_item),
            selectinload(StockOrder.packages).selectinload(StockOrderPackage.items),
        )
        .where(StockOrder.id == stock_order_id)
        .with_for_update(of=StockOrder)
    )
    order = (await session.execute(stmt)).scalar_one_or_none()
    if order is None:
        raise LookupError("Складской заказ не найден")

    shipment = None
    if order.shipment_document_id is not None:
        shipment = await session.get(ShipmentDocument, order.shipment_document_id)
        if shipment is not None and shipment.status == ShipmentDocumentStatus.POSTED:
            order.status = STOCK_ORDER_STATUS.DISPATCHED
            await session.flush()
            return {
                "stock_order_id": stock_order_id,
                "shipment_document_id": shipment.id,
                "shipment_document_number": shipment.doc_number,
                "shipment_status": shipment.status,
                "processed_items": len(order.items or []),
                "movements_created": 0,
                "reserves_released": 0,
                "lot_ids": [],
                "credit_warning": None,
                "already_dispatched": True,
            }

    if order.status != STOCK_ORDER_STATUS.COMPLETED:
        raise ValueError("Перед отгрузкой заказ нужно полностью собрать")
    if not order.items:
        raise ValueError("В складском заказе нет позиций")

    incomplete_items = [
        item for item in order.items if int(item.picked_quantity or 0) != int(item.quantity or 0)
    ]
    if incomplete_items:
        raise ValueError("Не все позиции заказа собраны в полном количестве")
    if any(item.autopart_id is None for item in order.items):
        raise ValueError("В заказе есть строка без фактической номенклатуры")
    assert_stock_order_packing_ready(order)

    # A cancelled shipment remains in the audit trail; retry creates a new one.
    if shipment is None or shipment.status == ShipmentDocumentStatus.CANCELLED:
        default_warehouse = await ensure_default_warehouse(session)
        preferred_lot_ids = {
            int(item.preferred_stock_lot_id)
            for item in order.items
            if item.preferred_stock_lot_id is not None
        }
        preferred_warehouse_ids: set[int] = set()
        if preferred_lot_ids:
            preferred_lot_rows = (
                await session.execute(
                    select(StockLot.id, StorageLocation.warehouse_id)
                    .join(
                        StorageLocation,
                        StorageLocation.id == StockLot.storage_location_id,
                    )
                    .where(StockLot.id.in_(preferred_lot_ids))
                )
            ).all()
            if len(preferred_lot_rows) != len(preferred_lot_ids):
                raise ValueError("Не найдена закреплённая партия cross-docking")
            preferred_warehouse_ids = {
                int(warehouse_id)
                for _, warehouse_id in preferred_lot_rows
                if warehouse_id is not None
            }
        if len(preferred_warehouse_ids) > 1:
            raise ValueError(
                "Позиции cross-docking находятся на разных "
                "складах — сформируйте отдельные отгрузки"
            )
        has_regular_stock = any(item.preferred_stock_lot_id is None for item in order.items)
        preferred_warehouse_id = next(iter(preferred_warehouse_ids), default_warehouse.id)
        if has_regular_stock and preferred_warehouse_id != default_warehouse.id:
            raise ValueError(
                "Наш склад и cross-docking находятся на разных "
                "физических складах — объединённая отгрузка невозможна"
            )
        warehouse = (
            default_warehouse
            if preferred_warehouse_id == default_warehouse.id
            else await get_warehouse_by_id(session, preferred_warehouse_id)
        )
        if warehouse is None:
            raise ValueError("Склад cross-docking не найден")
        shipment_note = f"Создано из складского заказа #{stock_order_id}"
        previous_shipments = int(
            (
                await session.execute(
                    select(func.count(ShipmentDocument.id)).where(
                        ShipmentDocument.notes == shipment_note
                    )
                )
            ).scalar_one()
            or 0
        )
        shipment_number = f"SHP-SO-{stock_order_id:06d}"
        if previous_shipments:
            shipment_number = f"{shipment_number}-R{previous_shipments}"
        customer_order_ids = {
            int(item.customer_order_item.order_id)
            for item in order.items
            if item.customer_order_item is not None
        }
        customer_order_id = next(iter(customer_order_ids)) if len(customer_order_ids) == 1 else None
        shipment = ShipmentDocument(
            doc_number=shipment_number,
            status=ShipmentDocumentStatus.DRAFT,
            customer_id=order.customer_id,
            customer_order_id=customer_order_id,
            warehouse_id=warehouse.id,
            reason="Отгрузка собранного складского заказа",
            notes=shipment_note,
        )
        session.add(shipment)
        await session.flush()

        for item in order.items:
            customer_item = item.customer_order_item
            session.add(
                ShipmentDocumentItem(
                    document_id=shipment.id,
                    autopart_id=int(item.autopart_id),
                    customer_order_item_id=(
                        customer_item.id if customer_item is not None else None
                    ),
                    customer_oem=(customer_item.oem if customer_item is not None else None),
                    customer_brand=(customer_item.brand if customer_item is not None else None),
                    customer_name=(customer_item.name if customer_item is not None else None),
                    quantity=int(item.picked_quantity or 0),
                    preferred_lot_id=item.preferred_stock_lot_id,
                    price=(customer_item.requested_price if customer_item is not None else None),
                    vat_rate=Decimal("22.00"),
                    notes=f"Строка складского заказа #{item.id}",
                )
            )
        order.shipment_document_id = shipment.id
        await session.flush()
    elif shipment.status != ShipmentDocumentStatus.DRAFT:
        raise ValueError("Связанную накладную нельзя провести")

    post_result = await post_shipment_document(session, shipment.id)
    order.status = STOCK_ORDER_STATUS.DISPATCHED
    await session.flush()

    return {
        "stock_order_id": stock_order_id,
        "shipment_document_id": shipment.id,
        "shipment_document_number": shipment.doc_number,
        "shipment_status": shipment.status,
        "processed_items": len(order.items),
        "already_dispatched": False,
        **post_result,
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
        existing_qty = (await session.execute(existing_stmt)).scalar_one_or_none() or 0

        locations_processed += 1

        if int(existing_qty) >= int(sbl.quantity):
            # Already covered — skip
            autoparts_skipped += 1
            continue

        gap = int(sbl.quantity) - int(existing_qty)
        if gap <= 0:
            autoparts_skipped += 1
            continue

        inferred_cost_price = await _infer_autopart_cost_price(
            session,
            autopart_id=sbl.autopart_id,
            storage_location_id=sbl.storage_location_id,
        )
        await _create_stock_lot(
            session,
            autopart_id=sbl.autopart_id,
            storage_location_id=sbl.storage_location_id,
            quantity=gap,
            source_type=LotSourceType.OPENING_BALANCE,
            # No GTD for opening balance — unknown provenance
            cost_price=inferred_cost_price,
        )
        lots_created += 1

        logger.info(
            "backfill: created opening_balance lot " "autopart_id=%s location_id=%s qty=%s",
            sbl.autopart_id,
            sbl.storage_location_id,
            gap,
        )

    await session.flush()
    return {
        "lots_created": lots_created,
        "locations_processed": locations_processed,
        "autoparts_skipped": autoparts_skipped,
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
    stmt = (
        select(StockLot)
        .where(StockLot.autopart_id == autopart_id)
        .options(
            selectinload(StockLot.autopart).selectinload(AutoPart.brand),
            selectinload(StockLot.storage_location),
            selectinload(StockLot.role_changed_by_user),
        )
    )
    if storage_location_id is not None:
        stmt = stmt.where(StockLot.storage_location_id == storage_location_id)
    if only_active:
        stmt = stmt.where(StockLot.remaining_quantity > 0)
    stmt = stmt.order_by(asc(StockLot.received_at), asc(StockLot.id))
    return (await session.execute(stmt)).scalars().all()


# ═══════════════════════════════════════════════════════════════════════════════
# Резервы (StockReserve)
# ═══════════════════════════════════════════════════════════════════════════════


async def get_reserved_quantity(
    session: AsyncSession,
    *,
    autopart_id: int,
    storage_location_id: Optional[int] = None,
) -> int:
    """Сумма ACTIVE-резервов для запчасти (опционально по ячейке)."""
    stmt = select(func.coalesce(func.sum(StockReserve.quantity), 0)).where(
        StockReserve.autopart_id == autopart_id,
        StockReserve.status == ReserveStatus.ACTIVE,
    )
    if storage_location_id is not None:
        stmt = stmt.where(StockReserve.storage_location_id == storage_location_id)
    return int((await session.execute(stmt)).scalar_one())


async def get_physical_quantity(
    session: AsyncSession,
    *,
    autopart_id: int,
    storage_location_id: Optional[int] = None,
) -> int:
    """Физический остаток (сумма StockByLocation)."""
    stmt = select(func.coalesce(func.sum(StockByLocation.quantity), 0)).where(
        StockByLocation.autopart_id == autopart_id
    )
    if storage_location_id is not None:
        stmt = stmt.where(StockByLocation.storage_location_id == storage_location_id)
    return int((await session.execute(stmt)).scalar_one())


async def get_available_quantity(
    session: AsyncSession,
    *,
    autopart_id: int,
    storage_location_id: Optional[int] = None,
) -> int:
    """Свободный остаток = физический − зарезервированный."""
    physical = await get_physical_quantity(
        session,
        autopart_id=autopart_id,
        storage_location_id=storage_location_id,
    )
    reserved = await get_reserved_quantity(
        session,
        autopart_id=autopart_id,
        storage_location_id=storage_location_id,
    )
    return max(0, physical - reserved)


async def create_reserve(
    session: AsyncSession,
    *,
    autopart_id: int,
    quantity: int,
    storage_location_id: Optional[int] = None,
    customer_order_item_id: Optional[int] = None,
    stock_order_item_id: Optional[int] = None,
    expires_at=None,
    notes: Optional[str] = None,
    external_id: Optional[str] = None,
) -> StockReserve:
    """Создать резерв, проверив наличие свободного остатка.

    Raises ValueError если доступного остатка недостаточно.
    """
    available = await get_available_quantity(
        session,
        autopart_id=autopart_id,
        storage_location_id=storage_location_id,
    )
    if available < quantity:
        raise ValueError(
            f"Недостаточно свободного остатка: " f"доступно {available}, запрошено {quantity}"
        )

    reserve = StockReserve(
        autopart_id=autopart_id,
        storage_location_id=storage_location_id,
        quantity=quantity,
        status=ReserveStatus.ACTIVE,
        customer_order_item_id=customer_order_item_id,
        stock_order_item_id=stock_order_item_id,
        expires_at=expires_at,
        notes=notes,
        external_id=external_id,
    )
    session.add(reserve)
    await session.flush()
    return reserve


async def release_reserve(
    session: AsyncSession,
    reserve: StockReserve,
) -> None:
    """Снять резерв (перевести в RELEASED)."""
    if reserve.status != ReserveStatus.ACTIVE:
        return
    reserve.status = ReserveStatus.RELEASED
    reserve.released_at = now_moscow()
    await session.flush()


async def cancel_reserve(
    session: AsyncSession,
    reserve: StockReserve,
) -> None:
    """Отменить резерв (перевести в CANCELLED)."""
    if reserve.status != ReserveStatus.ACTIVE:
        return
    reserve.status = ReserveStatus.CANCELLED
    reserve.released_at = now_moscow()
    await session.flush()


# ═══════════════════════════════════════════════════════════════════════════════
# Накладная на отгрузку (ShipmentDocument)
# ═══════════════════════════════════════════════════════════════════════════════


async def post_shipment_document(
    session: AsyncSession,
    doc_id: int,
) -> dict:
    """Провести накладную на отгрузку.

    Для каждой строки:
      1. Снимает связанный резерв (→ RELEASED).
      2. Расходует FIFO-лоты (_consume_fifo).
      3. Проставляет lot_id в строку накладной.

    Возвращает dict с ключами: movements_created, reserves_released, lot_ids.
    Raises ValueError при попытке провести не-DRAFT документ или нехватке остатков.
    """
    result = await session.execute(
        select(ShipmentDocument)
        .where(ShipmentDocument.id == doc_id)
        .options(
            selectinload(ShipmentDocument.items).selectinload(ShipmentDocumentItem.allocations)
        )
    )
    doc = result.scalar_one_or_none()
    if doc is None:
        raise ValueError(f"Накладная {doc_id} не найдена")
    if doc.status != ShipmentDocumentStatus.DRAFT:
        raise ValueError(f"Накладная уже в статусе «{doc.status}» — провести нельзя")
    credit_check = await check_shipment_credit_policy(session, document=doc)
    await assert_shipment_credit_available(session, document=doc)
    credit_warning = (
        credit_check.to_detail() if credit_check is not None and credit_check.should_warn else None
    )

    movements_created = 0
    reserves_released = 0
    lot_ids: list[int] = []

    for item in doc.items:
        # 1. Снимаем резерв
        if item.reserve_id:
            reserve = await session.get(StockReserve, item.reserve_id)
            if reserve and reserve.status == ReserveStatus.ACTIVE:
                await release_reserve(session, reserve)
                reserves_released += 1

        # 2. Расходуем FIFO
        if item.preferred_lot_id is not None:
            movements = await _consume_preferred_lots(
                session,
                autopart_id=item.autopart_id,
                quantity=item.quantity,
                movement_type=MovementType.SHIPMENT,
                reference_id=doc.id,
                reference_type="shipment_document",
                notes=item.notes,
                preferred_lot_ids=[int(item.preferred_lot_id)],
                fallback_storage_location_id=item.storage_location_id,
                fallback_warehouse_id=doc.warehouse_id,
                allow_fallback=False,
            )
        else:
            movements = await _consume_fifo(
                session,
                autopart_id=item.autopart_id,
                storage_location_id=item.storage_location_id,
                warehouse_id=doc.warehouse_id,
                quantity=item.quantity,
                movement_type=MovementType.SHIPMENT,
                reference_id=doc.id,
                reference_type="shipment_document",
                notes=item.notes,
            )
        movements_created += len(movements)

        touched_lot_ids = [
            movement.stock_lot_id for movement in movements if movement.stock_lot_id is not None
        ]
        lot_map: dict[int, StockLot] = {}
        if touched_lot_ids:
            lots_result = await session.execute(
                select(StockLot)
                .options(selectinload(StockLot.source_receipt))
                .where(StockLot.id.in_(touched_lot_ids))
            )
            lot_map = {lot.id: lot for lot in lots_result.scalars().all() if lot.id}

        cost_total = Decimal("0.00")
        costed_quantity = 0
        has_known_cost = False

        for movement in movements:
            if movement.stock_lot_id is None:
                continue
            lot = lot_map.get(movement.stock_lot_id)
            quantity_taken = abs(int(movement.quantity or 0))
            source_receipt = None
            if lot is not None:
                source_receipt = lot.source_receipt
                if source_receipt is None and lot.source_receipt_id is not None:
                    source_receipt = await session.get(SupplierReceipt, lot.source_receipt_id)
            unit_cost = (
                _quantize_unit_cost(_to_decimal(lot.cost_price)) if lot is not None else None
            )
            total_cost = (
                _quantize_money(unit_cost * Decimal(quantity_taken))
                if unit_cost is not None
                else None
            )
            if total_cost is not None:
                cost_total += total_cost
                costed_quantity += quantity_taken
                has_known_cost = True

            allocation = ShipmentDocumentItemLotAllocation(
                shipment_document_item_id=item.id,
                stock_lot_id=movement.stock_lot_id,
                stock_movement_id=movement.id,
                provider_id=(source_receipt.provider_id if source_receipt is not None else None),
                quantity=quantity_taken,
                unit_cost_price=unit_cost,
                total_cost_price=total_cost,
            )
            session.add(allocation)
            await session.flush()
            await allocate_marking_codes_for_shipment_allocation(
                session,
                allocation=allocation,
                shipment_document_id=doc.id,
                shipment_document_item_id=item.id,
            )

        item.cost_total = _quantize_money(cost_total) if has_known_cost else None
        if costed_quantity == int(item.quantity or 0) and costed_quantity > 0:
            item.cost_price = _quantize_unit_cost(cost_total / Decimal(costed_quantity))
        else:
            item.cost_price = None

        # 3. Запоминаем первый задействованный лот в строке
        first_lot_id = next((m.stock_lot_id for m in movements if m.stock_lot_id), None)
        if first_lot_id and not item.lot_id:
            item.lot_id = first_lot_id
        lot_ids.extend(m.stock_lot_id for m in movements if m.stock_lot_id)

    doc.status = ShipmentDocumentStatus.POSTED
    doc.posted_at = now_moscow()
    doc.sync_status = SyncStatus.PENDING
    linked_stock_order = (
        await session.execute(select(StockOrder).where(StockOrder.shipment_document_id == doc.id))
    ).scalar_one_or_none()
    if linked_stock_order is not None:
        linked_stock_order.status = STOCK_ORDER_STATUS.DISPATCHED
    await session.flush()
    from dz_fastapi.services.one_c_outbox import enqueue_shipment_event

    await enqueue_shipment_event(session, doc.id)

    return {
        "movements_created": movements_created,
        "reserves_released": reserves_released,
        "lot_ids": list(dict.fromkeys(lot_ids)),  # dedupe, preserve order
        "credit_warning": credit_warning,
    }


async def unpost_shipment_document(
    session: AsyncSession,
    doc_id: int,
) -> dict:
    """Отменить проведённую накладную (обратные движения).

    Для каждой строки создаёт обратное StockMovement(RECEIPT),
    восстанавливая остатки и лоты.
    Raises ValueError если документ не POSTED.
    """
    result = await session.execute(
        select(ShipmentDocument)
        .where(ShipmentDocument.id == doc_id)
        .options(
            selectinload(ShipmentDocument.items)
            .selectinload(ShipmentDocumentItem.allocations)
            .selectinload(ShipmentDocumentItemLotAllocation.stock_lot)
        )
    )
    doc = result.scalar_one_or_none()
    if doc is None:
        raise ValueError(f"Накладная {doc_id} не найдена")
    if doc.status != ShipmentDocumentStatus.POSTED:
        raise ValueError(f"Накладная в статусе «{doc.status}» — отменить нельзя")

    movements_created = 0

    for item in doc.items:
        allocations = list(item.allocations or [])
        restored_quantity = 0

        if allocations:
            for allocation in allocations:
                lot = allocation.stock_lot
                if lot is not None and allocation.stock_lot_id is not None:
                    lot.remaining_quantity = min(
                        lot.remaining_quantity + allocation.quantity,
                        lot.initial_quantity,
                    )

                mv = await _apply_stock_delta(
                    session,
                    autopart_id=item.autopart_id,
                    storage_location_id=(
                        lot.storage_location_id if lot is not None else item.storage_location_id
                    ),
                    quantity_delta=allocation.quantity,
                    movement_type=MovementType.RECEIPT,
                    reference_id=doc.id,
                    reference_type="shipment_document_unpost",
                    notes=f"Отмена накладной #{doc_id}",
                    stock_lot_id=allocation.stock_lot_id,
                )
                if mv is not None:
                    movements_created += 1
                restored_quantity += int(allocation.quantity or 0)
                await release_marking_codes_for_shipment_allocation(
                    session,
                    allocation=allocation,
                )
                await session.delete(allocation)

        remaining_to_restore = int(item.quantity or 0) - restored_quantity
        if remaining_to_restore > 0:
            restore_lot = None
            if item.lot_id:
                restore_lot = await session.get(StockLot, item.lot_id)
                if restore_lot is not None:
                    restore_lot.remaining_quantity = min(
                        restore_lot.remaining_quantity + remaining_to_restore,
                        restore_lot.initial_quantity,
                    )

            mv = await _apply_stock_delta(
                session,
                autopart_id=item.autopart_id,
                storage_location_id=(
                    restore_lot.storage_location_id
                    if restore_lot is not None
                    else item.storage_location_id
                ),
                quantity_delta=remaining_to_restore,
                movement_type=MovementType.RECEIPT,
                reference_id=doc.id,
                reference_type="shipment_document_unpost",
                notes=f"Отмена накладной #{doc_id}",
                stock_lot_id=item.lot_id,
            )
            if mv is not None:
                movements_created += 1

        item.lot_id = None
        item.cost_price = None
        item.cost_total = None

    doc.status = ShipmentDocumentStatus.CANCELLED
    doc.posted_at = None
    doc.sync_status = SyncStatus.PENDING
    linked_stock_order = (
        await session.execute(select(StockOrder).where(StockOrder.shipment_document_id == doc.id))
    ).scalar_one_or_none()
    if linked_stock_order is not None:
        linked_stock_order.status = STOCK_ORDER_STATUS.COMPLETED
    await session.flush()
    from dz_fastapi.services.one_c_outbox import EVENT_CANCELLED, enqueue_shipment_event

    await enqueue_shipment_event(session, doc.id, EVENT_CANCELLED)

    return {"movements_created": movements_created}


# ═══════════════════════════════════════════════════════════════════════════════
# Use-case: returns
# ═══════════════════════════════════════════════════════════════════════════════


async def _resolve_return_warehouse(
    session: AsyncSession,
    *,
    explicit_warehouse_id: int | None = None,
    fallback_warehouse_id: int | None = None,
) -> Warehouse:
    for warehouse_id in (explicit_warehouse_id, fallback_warehouse_id):
        if warehouse_id is None:
            continue
        warehouse = await get_warehouse_by_id(session, int(warehouse_id))
        if warehouse is not None:
            return warehouse
    return await ensure_default_warehouse(session)


async def _consume_preferred_lots(
    session: AsyncSession,
    *,
    autopart_id: int,
    quantity: int,
    movement_type: MovementType,
    reference_id: int,
    reference_type: str,
    notes: str | None = None,
    preferred_lot_ids: list[int] | None = None,
    fallback_storage_location_id: int | None = None,
    fallback_warehouse_id: int | None = None,
    allow_fallback: bool = True,
) -> list[StockMovement]:
    """Consume stock from specific lots first, then fallback to FIFO.

    This is primarily used for supplier returns, where we try to return stock
    from the original receipt lots before falling back to generic FIFO.
    """
    remaining = int(quantity or 0)
    if remaining <= 0:
        return []

    preferred_lot_ids = list(dict.fromkeys(preferred_lot_ids or []))
    if not allow_fallback:
        available_quantity = 0
        for lot_id in preferred_lot_ids:
            lot = await session.get(StockLot, int(lot_id))
            if lot is None:
                continue
            if int(lot.autopart_id or 0) != int(autopart_id):
                continue
            available_quantity += max(int(lot.remaining_quantity or 0), 0)
        if available_quantity < remaining:
            raise ValueError(
                "В закреплённой партии cross-docking недостаточно "
                "товара; замена другой партией запрещена"
            )

    movements: list[StockMovement] = []
    seen_lot_ids: set[int] = set()

    for lot_id in preferred_lot_ids:
        if remaining <= 0:
            break
        if lot_id in seen_lot_ids:
            continue
        seen_lot_ids.add(lot_id)

        lot = await session.get(StockLot, int(lot_id))
        if lot is None:
            continue
        if int(lot.autopart_id or 0) != int(autopart_id):
            continue
        if int(lot.remaining_quantity or 0) <= 0:
            continue

        take = min(int(lot.remaining_quantity), remaining)
        lot.remaining_quantity -= take
        remaining -= take

        mv = await _apply_stock_delta(
            session,
            autopart_id=autopart_id,
            storage_location_id=lot.storage_location_id,
            quantity_delta=-take,
            movement_type=movement_type,
            reference_id=reference_id,
            reference_type=reference_type,
            notes=notes,
            stock_lot_id=lot.id,
        )
        if mv is not None:
            movements.append(mv)

    if remaining > 0 and not allow_fallback:
        raise ValueError(
            "В закреплённой партии cross-docking недостаточно "
            "товара; замена другой партией запрещена"
        )

    if remaining > 0:
        fallback = await _consume_fifo(
            session,
            autopart_id=autopart_id,
            storage_location_id=fallback_storage_location_id,
            quantity=remaining,
            movement_type=movement_type,
            warehouse_id=fallback_warehouse_id,
            reference_id=reference_id,
            reference_type=reference_type,
            notes=notes,
        )
        movements.extend(fallback)

    return movements


async def _load_customer_return(
    session: AsyncSession,
    doc_id: int,
) -> ReturnFromCustomer | None:
    result = await session.execute(
        select(ReturnFromCustomer)
        .where(ReturnFromCustomer.id == doc_id)
        .options(
            selectinload(ReturnFromCustomer.shipment_document),
            selectinload(ReturnFromCustomer.items).selectinload(ReturnItem.autopart),
            selectinload(ReturnFromCustomer.items).selectinload(ReturnItem.storage_location),
            selectinload(ReturnFromCustomer.items).selectinload(ReturnItem.lot),
            selectinload(ReturnFromCustomer.items)
            .selectinload(ReturnItem.shipment_item)
            .selectinload(ShipmentDocumentItem.lot),
        )
    )
    return result.scalar_one_or_none()


async def _load_supplier_return(
    session: AsyncSession,
    doc_id: int,
) -> ReturnToSupplier | None:
    result = await session.execute(
        select(ReturnToSupplier)
        .where(ReturnToSupplier.id == doc_id)
        .options(
            selectinload(ReturnToSupplier.supplier_receipt),
            selectinload(ReturnToSupplier.items).selectinload(ReturnItem.autopart),
            selectinload(ReturnToSupplier.items).selectinload(ReturnItem.storage_location),
            selectinload(ReturnToSupplier.items).selectinload(ReturnItem.lot),
            selectinload(ReturnToSupplier.items).selectinload(ReturnItem.supplier_receipt_item),
        )
    )
    return result.scalar_one_or_none()


async def approve_return_from_customer(
    session: AsyncSession,
    *,
    doc_id: int,
) -> ReturnFromCustomer:
    doc = await _load_customer_return(session, doc_id)
    if doc is None:
        raise LookupError("Возврат от клиента не найден")
    if doc.status != ReturnDocumentStatus.CREATED:
        raise ValueError("Согласовать можно только возврат в статусе CREATED")
    doc.status = ReturnDocumentStatus.APPROVED
    doc.approved_at = now_moscow()
    await session.flush()
    return doc


async def ship_return_from_customer(
    session: AsyncSession,
    *,
    doc_id: int,
) -> ReturnFromCustomer:
    doc = await _load_customer_return(session, doc_id)
    if doc is None:
        raise LookupError("Возврат от клиента не найден")
    if doc.status != ReturnDocumentStatus.APPROVED:
        raise ValueError("К отгрузке клиента можно перевести только APPROVED")
    doc.status = ReturnDocumentStatus.SHIPPED
    doc.shipped_at = now_moscow()
    await session.flush()
    return doc


async def confirm_return_from_customer(
    session: AsyncSession,
    *,
    doc_id: int,
) -> ReturnFromCustomer:
    doc = await _load_customer_return(session, doc_id)
    if doc is None:
        raise LookupError("Возврат от клиента не найден")
    if doc.status not in {
        ReturnDocumentStatus.APPROVED,
        ReturnDocumentStatus.SHIPPED,
    }:
        raise ValueError("Подтвердить приёмку можно только для APPROVED или SHIPPED")

    fallback_warehouse_id = (
        doc.shipment_document.warehouse_id if doc.shipment_document is not None else None
    )
    warehouse = await _resolve_return_warehouse(
        session,
        explicit_warehouse_id=doc.warehouse_id,
        fallback_warehouse_id=fallback_warehouse_id,
    )
    doc.warehouse_id = warehouse.id
    receiving_location = await ensure_receiving_location(session, warehouse)

    for item in doc.items or []:
        qty = int(item.quantity or 0)
        if qty <= 0:
            continue
        if item.lot_id is not None:
            continue

        source_item = item.shipment_item
        source_lot = getattr(source_item, "lot", None) if source_item is not None else None

        autopart_id = item.autopart_id or getattr(source_item, "autopart_id", None)
        if autopart_id is None:
            raise ValueError(f"Не удалось определить autopart для строки возврата #{item.id}")

        target_location_id = item.storage_location_id or receiving_location.id
        item.storage_location_id = target_location_id

        lot = await _create_stock_lot(
            session,
            autopart_id=int(autopart_id),
            storage_location_id=int(target_location_id),
            quantity=qty,
            source_type=LotSourceType.CUSTOMER_RETURN,
            gtd_number=item.gtd_number
            or (source_lot.gtd_number if source_lot is not None else None),
            country_code=item.country_code
            or (source_lot.country_code if source_lot is not None else None),
            country_name=item.country_name
            or (source_lot.country_name if source_lot is not None else None),
            inventory_role=(source_lot.inventory_role if source_lot is not None else None),
            role_source=StockLotRoleSource.CUSTOMER_RETURN,
            role_rule_reference=(f"stock_lot:{source_lot.id}" if source_lot is not None else None),
            role_change_reason=(
                "Роль восстановлена из исходной партии при возврате клиента"
                if source_lot is not None
                else "Роль назначена по номенклатуре при возврате клиента"
            ),
        )
        item.autopart_id = int(autopart_id)
        item.lot_id = lot.id

        await _apply_stock_delta(
            session,
            autopart_id=int(autopart_id),
            storage_location_id=int(target_location_id),
            quantity_delta=qty,
            movement_type=MovementType.CUSTOMER_RETURN,
            reference_id=doc.id,
            reference_type="return_from_customer",
            notes=item.notes or doc.reason or f"Возврат от клиента #{doc.id}",
            stock_lot_id=lot.id,
        )
        await return_marking_codes_from_customer(
            session,
            shipment_item_id=item.shipment_item_id,
            new_stock_lot=lot,
            quantity=qty,
            return_document_id=doc.id,
        )

    doc.status = ReturnDocumentStatus.CONFIRMED
    if doc.approved_at is None:
        doc.approved_at = now_moscow()
    doc.confirmed_at = now_moscow()
    await session.flush()
    return doc


async def reject_return_from_customer(
    session: AsyncSession,
    *,
    doc_id: int,
) -> ReturnFromCustomer:
    doc = await _load_customer_return(session, doc_id)
    if doc is None:
        raise LookupError("Возврат от клиента не найден")
    if doc.status == ReturnDocumentStatus.CONFIRMED:
        raise ValueError("Подтверждённый возврат отклонить нельзя")
    if doc.status == ReturnDocumentStatus.REJECTED:
        raise ValueError("Возврат уже отклонён")
    doc.status = ReturnDocumentStatus.REJECTED
    doc.rejected_at = now_moscow()
    await session.flush()
    return doc


async def approve_return_to_supplier(
    session: AsyncSession,
    *,
    doc_id: int,
) -> ReturnToSupplier:
    doc = await _load_supplier_return(session, doc_id)
    if doc is None:
        raise LookupError("Возврат поставщику не найден")
    if doc.status != ReturnDocumentStatus.CREATED:
        raise ValueError("Согласовать можно только возврат в статусе CREATED")
    doc.status = ReturnDocumentStatus.APPROVED
    doc.approved_at = now_moscow()
    await session.flush()
    return doc


async def ship_return_to_supplier(
    session: AsyncSession,
    *,
    doc_id: int,
) -> ReturnToSupplier:
    doc = await _load_supplier_return(session, doc_id)
    if doc is None:
        raise LookupError("Возврат поставщику не найден")
    if doc.status != ReturnDocumentStatus.APPROVED:
        raise ValueError("Отгрузить можно только возврат в статусе APPROVED")

    for item in doc.items or []:
        qty = int(item.quantity or 0)
        if qty <= 0:
            continue

        source_item = item.supplier_receipt_item
        autopart_id = item.autopart_id or getattr(source_item, "autopart_id", None)
        if autopart_id is None:
            raise ValueError(f"Не удалось определить autopart для строки возврата #{item.id}")

        preferred_lot_ids: list[int] = []
        if item.lot_id is not None:
            preferred_lot_ids.append(int(item.lot_id))
        elif item.supplier_receipt_item_id is not None:
            lot_rows = (
                (
                    await session.execute(
                        select(StockLot.id)
                        .where(
                            StockLot.source_receipt_item_id == item.supplier_receipt_item_id,
                            StockLot.remaining_quantity > 0,
                        )
                        .order_by(asc(StockLot.received_at), asc(StockLot.id))
                    )
                )
                .scalars()
                .all()
            )
            preferred_lot_ids.extend(int(lot_id) for lot_id in lot_rows)

        movements = await _consume_preferred_lots(
            session,
            autopart_id=int(autopart_id),
            quantity=qty,
            movement_type=MovementType.SUPPLIER_RETURN,
            reference_id=doc.id,
            reference_type="return_to_supplier",
            notes=item.notes or doc.reason or f"Возврат поставщику #{doc.id}",
            preferred_lot_ids=preferred_lot_ids,
            fallback_storage_location_id=item.storage_location_id,
        )
        if item.lot_id is None:
            first_lot_id = next(
                (mv.stock_lot_id for mv in movements if mv.stock_lot_id),
                None,
            )
            if first_lot_id is not None:
                item.lot_id = first_lot_id
        item.autopart_id = int(autopart_id)
        consumed_lot_ids = [mv.stock_lot_id for mv in movements if mv.stock_lot_id]
        await return_marking_codes_to_supplier(
            session,
            stock_lot_ids=consumed_lot_ids,
            quantity=qty,
            return_document_id=doc.id,
        )

    doc.status = ReturnDocumentStatus.SHIPPED
    doc.shipped_at = now_moscow()
    await session.flush()
    return doc


async def confirm_return_to_supplier(
    session: AsyncSession,
    *,
    doc_id: int,
) -> ReturnToSupplier:
    doc = await _load_supplier_return(session, doc_id)
    if doc is None:
        raise LookupError("Возврат поставщику не найден")
    if doc.status != ReturnDocumentStatus.SHIPPED:
        raise ValueError("Подтвердить можно только возврат в статусе SHIPPED")
    doc.status = ReturnDocumentStatus.CONFIRMED
    doc.confirmed_at = now_moscow()
    await session.flush()
    return doc


async def reject_return_to_supplier(
    session: AsyncSession,
    *,
    doc_id: int,
) -> ReturnToSupplier:
    doc = await _load_supplier_return(session, doc_id)
    if doc is None:
        raise LookupError("Возврат поставщику не найден")
    if doc.status not in {
        ReturnDocumentStatus.CREATED,
        ReturnDocumentStatus.APPROVED,
    }:
        raise ValueError("Отклонить можно только возврат до отгрузки")
    doc.status = ReturnDocumentStatus.REJECTED
    doc.rejected_at = now_moscow()
    await session.flush()
    return doc
