from __future__ import annotations

from typing import Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from dz_fastapi.core.time import now_moscow
from dz_fastapi.models.autopart import (LocationType, StorageLocation,
                                        autopart_storage_association)
from dz_fastapi.models.brand import Brand
from dz_fastapi.models.inventory import (MovementType, StockByLocation,
                                         StockMovement, Warehouse)
from dz_fastapi.models.partner import (Provider, SupplierReceipt,
                                       SupplierReceiptItem)

DEFAULT_WAREHOUSE_NAME = 'Основной склад'
DEFAULT_WAREHOUSE_COMMENT = (
    'Склад по умолчанию для входящих документов и первичного размещения.'
)
RECEIVING_LOCATION_CODE = 'RECEIVING'


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
                session,
                int(provider.default_warehouse_id),
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

    from dz_fastapi.models.autopart import \
        AutoPart  # local import to avoid cycles

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


async def apply_stock_delta(
    session: AsyncSession,
    *,
    autopart_id: int,
    storage_location_id: int,
    quantity_delta: int,
    movement_type: MovementType,
    reference_id: int | None = None,
    reference_type: str | None = None,
    notes: str | None = None,
) -> Optional[StockMovement]:
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
            'Недостаточно остатка для обратного движения по складу'
        )

    if stock_row is None:
        stock_row = StockByLocation(
            autopart_id=autopart_id,
            storage_location_id=storage_location_id,
            quantity=qty_after,
        )
        session.add(stock_row)
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
    )
    session.add(movement)
    return movement


async def apply_receipt_to_stock(
    session: AsyncSession,
    *,
    receipt: SupplierReceipt,
    reverse: bool = False,
) -> None:
    warehouse = await resolve_warehouse_for_provider(
        session,
        provider_id=receipt.provider_id,
        explicit_warehouse_id=receipt.warehouse_id,
    )
    receipt.warehouse_id = warehouse.id
    receiving_location = await ensure_receiving_location(session, warehouse)

    multiplier = -1 if reverse else 1
    note_prefix = 'Распроведение поступления' if reverse else 'Поступление'

    for item in receipt.items or []:
        autopart_id = await resolve_receipt_item_autopart_id(session, item)
        if autopart_id is None:
            continue
        quantity = max(int(item.received_quantity or 0), 0)
        if quantity <= 0:
            continue
        await apply_stock_delta(
            session,
            autopart_id=autopart_id,
            storage_location_id=receiving_location.id,
            quantity_delta=quantity * multiplier,
            movement_type=MovementType.RECEIPT,
            reference_id=receipt.id,
            reference_type='supplier_receipt',
            notes=(
                f'{note_prefix} #{receipt.id}'
                + (
                    f' ({receipt.document_number})'
                    if str(receipt.document_number or '').strip()
                    else ''
                )
            ),
        )


async def apply_receipt_to_stock_by_id(
    session: AsyncSession,
    *,
    receipt_id: int,
    reverse: bool = False,
) -> None:
    from sqlalchemy.orm import selectinload

    stmt = (
        select(SupplierReceipt)
        .options(selectinload(SupplierReceipt.items))
        .where(SupplierReceipt.id == receipt_id)
    )
    receipt = (await session.execute(stmt)).scalar_one_or_none()
    if receipt is None:
        raise LookupError('Документ поступления не найден')
    await apply_receipt_to_stock(session, receipt=receipt, reverse=reverse)
