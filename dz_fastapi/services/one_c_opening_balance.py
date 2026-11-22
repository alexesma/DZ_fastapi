"""Выгрузка начальных остатков склада в 1С одним документом оприходования.

Документ создаётся только для обмена: остатки и партии в нашей системе он
не меняет (иначе остатки удвоились бы). Состав берётся из фактических
остатков по ячейкам склада, себестоимость и ГТД — из партий (StockLot).
"""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from decimal import Decimal
from typing import Any, Optional
from zoneinfo import ZoneInfo

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.core.time import now_moscow
from dz_fastapi.models.autopart import StorageLocation
from dz_fastapi.models.inventory import (
    StockByLocation,
    StockDocument,
    StockDocumentItem,
    StockDocumentStatus,
    StockDocumentType,
    StockLot,
    SyncStatus,
    Warehouse,
)
from dz_fastapi.services.one_c_outbox import enqueue_stock_document_event

OPENING_REASON = "Начальные остатки (перенос из учётной системы)"
MAIN_WAREHOUSE_NAME = "Основной склад"
MSK = ZoneInfo("Europe/Moscow")


async def _main_warehouse(
    session: AsyncSession, warehouse_id: Optional[int]
) -> Warehouse:
    if warehouse_id is not None:
        warehouse = await session.get(Warehouse, warehouse_id)
    else:
        warehouse = (
            await session.execute(
                select(Warehouse).where(Warehouse.name == MAIN_WAREHOUSE_NAME)
            )
        ).scalar_one_or_none()
    if warehouse is None:
        raise LookupError("Основной склад не найден")
    return warehouse


async def build_opening_rows(
    session: AsyncSession, warehouse: Warehouse
) -> dict[str, Any]:
    """Строки документа: по каждой позиции остаток × (себестоимость, ГТД)."""
    stock_rows = (
        await session.execute(
            select(
                StockByLocation.autopart_id,
                func.sum(StockByLocation.quantity),
            )
            .join(
                StorageLocation,
                StorageLocation.id == StockByLocation.storage_location_id,
            )
            .where(
                StorageLocation.warehouse_id == warehouse.id,
                StockByLocation.quantity > 0,
            )
            .group_by(StockByLocation.autopart_id)
        )
    ).all()
    stock = {int(a): int(q) for a, q in stock_rows}

    lot_rows = (
        await session.execute(
            select(
                StockLot.autopart_id,
                StockLot.cost_price,
                StockLot.gtd_number,
                StockLot.country_code,
                StockLot.country_name,
                func.sum(StockLot.remaining_quantity),
            )
            .join(
                StorageLocation,
                StorageLocation.id == StockLot.storage_location_id,
            )
            .where(
                StorageLocation.warehouse_id == warehouse.id,
                StockLot.remaining_quantity > 0,
            )
            .group_by(
                StockLot.autopart_id,
                StockLot.cost_price,
                StockLot.gtd_number,
                StockLot.country_code,
                StockLot.country_name,
            )
            .order_by(StockLot.autopart_id)
        )
    ).all()
    lots: dict[int, list[list[Any]]] = defaultdict(list)
    for autopart_id, cost, gtd, code, cname, qty in lot_rows:
        lots[int(autopart_id)].append([cost, gtd, code, cname, int(qty)])

    rows: list[dict[str, Any]] = []
    without_cost = 0
    lots_short = 0
    lots_over = 0
    for autopart_id, total in sorted(stock.items()):
        parts = lots.get(autopart_id, [])
        lot_total = sum(p[4] for p in parts)
        known = [p for p in parts if p[0] is not None and p[4] > 0]
        avg = None
        if known:
            weight = sum(p[4] for p in known)
            avg = sum(Decimal(p[0]) * p[4] for p in known) / weight
        if lot_total > total:
            lots_over += 1
            surplus = lot_total - total
            for part in reversed(parts):
                cut = min(part[4], surplus)
                part[4] -= cut
                surplus -= cut
                if not surplus:
                    break
        elif lot_total < total:
            lots_short += 1
            parts.append([avg, None, None, None, total - lot_total])
        for cost, gtd, code, cname, qty in parts:
            if qty <= 0:
                continue
            if cost is None:
                cost = avg
            if cost is None:
                without_cost += qty
                cost = Decimal("0")
            rows.append(
                {
                    "autopart_id": autopart_id,
                    "quantity": qty,
                    "cost_price": Decimal(cost).quantize(Decimal("0.0001")),
                    "gtd_number": gtd,
                    "country_code": code,
                    "country_name": cname,
                }
            )
    amount = sum((r["quantity"] * r["cost_price"] for r in rows), Decimal(0))
    return {
        "rows": rows,
        "positions": len(stock),
        "lines": len(rows),
        "quantity": sum(r["quantity"] for r in rows),
        "amount": amount,
        "lines_with_gtd": sum(1 for r in rows if r["gtd_number"]),
        "quantity_without_cost": without_cost,
        "positions_lots_short": lots_short,
        "positions_lots_over": lots_over,
    }


async def _existing_opening(
    session: AsyncSession, warehouse: Warehouse
) -> Optional[StockDocument]:
    return (
        (
            await session.execute(
                select(StockDocument)
                .where(
                    StockDocument.reason == OPENING_REASON,
                    StockDocument.warehouse_id == warehouse.id,
                    StockDocument.status != StockDocumentStatus.CANCELLED,
                )
                .order_by(StockDocument.id.desc())
                .limit(1)
            )
        )
        .scalars()
        .first()
    )


async def preview_opening_balance(
    session: AsyncSession, *, warehouse_id: Optional[int] = None
) -> dict[str, Any]:
    warehouse = await _main_warehouse(session, warehouse_id)
    built = await build_opening_rows(session, warehouse)
    existing = await _existing_opening(session, warehouse)
    return {
        "warehouse": {"id": warehouse.id, "name": warehouse.name},
        "positions": built["positions"],
        "lines": built["lines"],
        "quantity": built["quantity"],
        "amount": str(built["amount"].quantize(Decimal("0.01"))),
        "lines_with_gtd": built["lines_with_gtd"],
        "quantity_without_cost": built["quantity_without_cost"],
        "positions_lots_short": built["positions_lots_short"],
        "positions_lots_over": built["positions_lots_over"],
        "existing_document_id": existing.id if existing else None,
        "existing_document_date": (
            existing.document_date.isoformat() if existing else None
        ),
    }


async def create_opening_balance_event(
    session: AsyncSession,
    *,
    document_date: datetime,
    warehouse_id: Optional[int] = None,
) -> dict[str, Any]:
    """Создаёт документ-основание и ставит его в очередь обмена с 1С."""
    warehouse = await _main_warehouse(session, warehouse_id)
    if await _existing_opening(session, warehouse) is not None:
        raise ValueError(
            "Документ начальных остатков уже создан; повторная выгрузка "
            "удвоила бы остатки в 1С"
        )
    built = await build_opening_rows(session, warehouse)
    if not built["rows"]:
        raise ValueError("Нет остатков для выгрузки")
    if document_date.tzinfo is None:
        document_date = document_date.replace(tzinfo=MSK)
    document = StockDocument(
        doc_type=StockDocumentType.MANUAL_RECEIPT,
        status=StockDocumentStatus.POSTED,
        document_date=document_date,
        warehouse_id=warehouse.id,
        reason=OPENING_REASON,
        notes="Только для выгрузки в 1С. Остатки и партии системы не меняет.",
        sync_status=SyncStatus.PENDING,
        posted_at=now_moscow(),
    )
    session.add(document)
    await session.flush()
    document.document_number = f"DZ-OPEN-{document.id}"
    for row in built["rows"]:
        session.add(
            StockDocumentItem(
                document_id=document.id,
                autopart_id=row["autopart_id"],
                quantity=row["quantity"],
                cost_price=row["cost_price"],
                gtd_number=row["gtd_number"],
                country_code=row["country_code"],
                country_name=row["country_name"],
            )
        )
    await session.flush()
    event = await enqueue_stock_document_event(session, document.id)
    return {
        "document_id": document.id,
        "event_id": event.id,
        "lines": built["lines"],
        "quantity": built["quantity"],
        "amount": str(built["amount"].quantize(Decimal("0.01"))),
    }
