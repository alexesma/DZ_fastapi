"""Этап 6: статистика возвратов/рекламаций.

Агрегации для аналитики: кто из клиентов возвращает больше, у каких
поставщиков больше транзитных возвратов, какие бренды чаще в рекламациях,
разрезы по типу/статусу/решению и тренд по месяцам.
"""
import logging
from datetime import date, datetime
from typing import Any, Optional

from sqlalchemy import case, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.models.partner import Customer, Provider, Reclamation, ReclamationItem

logger = logging.getLogger("dz_fastapi")


def _apply_date_range(stmt, date_from: Optional[date], date_to: Optional[date]):
    if date_from is not None:
        stmt = stmt.where(
            Reclamation.created_at
            >= datetime.combine(date_from, datetime.min.time())
        )
    if date_to is not None:
        stmt = stmt.where(
            Reclamation.created_at
            <= datetime.combine(date_to, datetime.max.time())
        )
    return stmt


async def get_reclamation_statistics(
    session: AsyncSession,
    *,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
    top_limit: int = 10,
) -> dict[str, Any]:
    top_limit = max(1, min(int(top_limit or 10), 50))

    # ── Итоги ────────────────────────────────────────────────────────────
    total = (
        await session.execute(
            _apply_date_range(
                select(func.count()).select_from(Reclamation),
                date_from,
                date_to,
            )
        )
    ).scalar() or 0

    def _group_count(column):
        stmt = _apply_date_range(
            select(column, func.count()).select_from(Reclamation),
            date_from,
            date_to,
        ).group_by(column)
        return stmt

    by_status = {
        str(getattr(k, "value", k)): int(v)
        for k, v in (
            await session.execute(_group_count(Reclamation.status))
        ).all()
    }
    by_type = {
        str(getattr(k, "value", k)): int(v)
        for k, v in (
            await session.execute(_group_count(Reclamation.reclamation_type))
        ).all()
        if k is not None
    }
    by_resolution = {
        str(k): int(v)
        for k, v in (
            await session.execute(_group_count(Reclamation.resolution))
        ).all()
        if k is not None
    }

    # Среднее время до решения (в днях)
    avg_days_row = (
        await session.execute(
            _apply_date_range(
                select(
                    func.avg(
                        func.extract(
                            "epoch",
                            Reclamation.resolved_at - Reclamation.created_at,
                        )
                    )
                ).where(Reclamation.resolved_at.isnot(None)),
                date_from,
                date_to,
            )
        )
    ).scalar()
    avg_resolution_days = (
        round(float(avg_days_row) / 86400.0, 1)
        if avg_days_row is not None
        else None
    )

    # ── Топ клиентов ─────────────────────────────────────────────────────
    approved_case = func.sum(
        case((Reclamation.resolution == "approved", 1), else_=0)
    )
    rejected_case = func.sum(
        case((Reclamation.resolution == "rejected", 1), else_=0)
    )
    top_customers_stmt = _apply_date_range(
        select(
            Reclamation.customer_id,
            Customer.name.label("customer_name"),
            func.count().label("count"),
            approved_case.label("approved"),
            rejected_case.label("rejected"),
        )
        .select_from(Reclamation)
        .outerjoin(Customer, Customer.id == Reclamation.customer_id)
        .where(Reclamation.customer_id.isnot(None)),
        date_from,
        date_to,
    ).group_by(Reclamation.customer_id, Customer.name).order_by(
        func.count().desc()
    ).limit(top_limit)
    top_customers = [
        {
            "customer_id": row.customer_id,
            "customer_name": row.customer_name,
            "count": int(row.count),
            "approved": int(row.approved or 0),
            "rejected": int(row.rejected or 0),
        }
        for row in (await session.execute(top_customers_stmt)).all()
    ]

    # ── Топ поставщиков (транзитные позиции) ─────────────────────────────
    top_suppliers_stmt = _apply_date_range(
        select(
            ReclamationItem.source_provider_id,
            Provider.name.label("provider_name"),
            func.count(func.distinct(ReclamationItem.reclamation_id)).label(
                "reclamations"
            ),
            func.count().label("items"),
        )
        .select_from(ReclamationItem)
        .join(Reclamation, Reclamation.id == ReclamationItem.reclamation_id)
        .outerjoin(Provider, Provider.id == ReclamationItem.source_provider_id)
        .where(ReclamationItem.source_provider_id.isnot(None)),
        date_from,
        date_to,
    ).group_by(
        ReclamationItem.source_provider_id, Provider.name
    ).order_by(
        func.count(func.distinct(ReclamationItem.reclamation_id)).desc()
    ).limit(top_limit)
    top_suppliers = [
        {
            "provider_id": row.source_provider_id,
            "provider_name": row.provider_name,
            "reclamations": int(row.reclamations),
            "items": int(row.items),
        }
        for row in (await session.execute(top_suppliers_stmt)).all()
    ]

    # ── Топ брендов ──────────────────────────────────────────────────────
    top_brands_stmt = _apply_date_range(
        select(
            ReclamationItem.brand_name,
            func.count(func.distinct(ReclamationItem.reclamation_id)).label(
                "reclamations"
            ),
            func.coalesce(func.sum(ReclamationItem.quantity), 0).label("qty"),
        )
        .select_from(ReclamationItem)
        .join(Reclamation, Reclamation.id == ReclamationItem.reclamation_id)
        .where(ReclamationItem.brand_name.isnot(None)),
        date_from,
        date_to,
    ).group_by(ReclamationItem.brand_name).order_by(
        func.count(func.distinct(ReclamationItem.reclamation_id)).desc()
    ).limit(top_limit)
    top_brands = [
        {
            "brand_name": row.brand_name,
            "reclamations": int(row.reclamations),
            "quantity": int(row.qty or 0),
        }
        for row in (await session.execute(top_brands_stmt)).all()
    ]

    # ── Тренд по месяцам ─────────────────────────────────────────────────
    month_col = func.to_char(
        func.date_trunc("month", Reclamation.created_at), "YYYY-MM"
    ).label("month")
    monthly_stmt = _apply_date_range(
        select(month_col, func.count().label("count")).select_from(
            Reclamation
        ),
        date_from,
        date_to,
    ).group_by(month_col).order_by(month_col)
    by_month = [
        {"month": row.month, "count": int(row.count)}
        for row in (await session.execute(monthly_stmt)).all()
        if row.month is not None
    ]

    return {
        "total": int(total),
        "by_status": by_status,
        "by_type": by_type,
        "by_resolution": by_resolution,
        "avg_resolution_days": avg_resolution_days,
        "top_customers": top_customers,
        "top_suppliers": top_suppliers,
        "top_brands": top_brands,
        "by_month": by_month,
    }
