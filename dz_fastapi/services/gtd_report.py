"""Отчёт по ГТД: сколько пришло / отгружено / осталось по каждой декларации
и по каким поставщикам номер ГТД не указан. Источник — партии склада."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any, Optional

from sqlalchemy import func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.core.time import MOSCOW_TZ
from dz_fastapi.models.inventory import StockLot
from dz_fastapi.models.partner import Provider, SupplierReceipt


def _bounds(
    date_from: Optional[date], date_to: Optional[date]
) -> tuple[Optional[datetime], Optional[datetime]]:
    start = (
        datetime.combine(date_from, datetime.min.time(), tzinfo=MOSCOW_TZ)
        if date_from
        else None
    )
    end = (
        datetime.combine(date_to, datetime.max.time(), tzinfo=MOSCOW_TZ)
        if date_to
        else None
    )
    return start, end


async def build_gtd_report(
    session: AsyncSession,
    *,
    gtd_number: Optional[str] = None,
    provider_id: Optional[int] = None,
    only_remaining: bool = False,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
    limit: int = 500,
    offset: int = 0,
) -> dict[str, Any]:
    start, end = _bounds(date_from, date_to)

    def _filters(stmt):
        stmt = stmt.where(StockLot.source_receipt_id.isnot(None))
        if provider_id is not None:
            stmt = stmt.where(SupplierReceipt.provider_id == provider_id)
        if start is not None:
            stmt = stmt.where(StockLot.received_at >= start)
        if end is not None:
            stmt = stmt.where(StockLot.received_at <= end)
        return stmt

    has_gtd = func.length(func.trim(func.coalesce(StockLot.gtd_number, ""))) > 0
    received = func.coalesce(func.sum(StockLot.initial_quantity), 0)
    remaining = func.coalesce(func.sum(StockLot.remaining_quantity), 0)

    items_stmt = _filters(
        select(
            StockLot.gtd_number,
            SupplierReceipt.provider_id,
            Provider.name.label("provider_name"),
            func.max(StockLot.country_name).label("country_name"),
            func.count(StockLot.id).label("lots"),
            received.label("received"),
            remaining.label("remaining"),
            func.min(StockLot.received_at).label("first_received_at"),
            func.max(StockLot.received_at).label("last_received_at"),
        )
        .join(SupplierReceipt, SupplierReceipt.id == StockLot.source_receipt_id)
        .join(Provider, Provider.id == SupplierReceipt.provider_id)
        .where(has_gtd)
        .group_by(
            StockLot.gtd_number, SupplierReceipt.provider_id, Provider.name
        )
    )
    if gtd_number:
        items_stmt = items_stmt.where(StockLot.gtd_number.ilike(f"%{gtd_number}%"))
    if only_remaining:
        items_stmt = items_stmt.having(remaining > 0)
    items_stmt = (
        items_stmt.order_by(func.max(StockLot.received_at).desc())
        .offset(offset)
        .limit(limit)
    )
    items = [
        {
            "gtd_number": row.gtd_number,
            "provider_id": int(row.provider_id),
            "provider_name": row.provider_name,
            "country_name": row.country_name,
            "lots": int(row.lots),
            "received": int(row.received),
            "shipped": int(row.received) - int(row.remaining),
            "remaining": int(row.remaining),
            "first_received_at": row.first_received_at,
            "last_received_at": row.last_received_at,
        }
        for row in (await session.execute(items_stmt)).all()
    ]

    missing_stmt = _filters(
        select(
            SupplierReceipt.provider_id,
            Provider.name.label("provider_name"),
            func.count(StockLot.id).label("lots"),
            received.label("received"),
            remaining.label("remaining"),
        )
        .join(SupplierReceipt, SupplierReceipt.id == StockLot.source_receipt_id)
        .join(Provider, Provider.id == SupplierReceipt.provider_id)
        .where(or_(StockLot.gtd_number.is_(None), ~has_gtd))
        .group_by(SupplierReceipt.provider_id, Provider.name)
        .order_by(func.count(StockLot.id).desc())
    )
    without_gtd = [
        {
            "provider_id": int(row.provider_id),
            "provider_name": row.provider_name,
            "lots": int(row.lots),
            "received": int(row.received),
            "remaining": int(row.remaining),
        }
        for row in (await session.execute(missing_stmt)).all()
    ]
    return {"items": items, "without_gtd": without_gtd}
