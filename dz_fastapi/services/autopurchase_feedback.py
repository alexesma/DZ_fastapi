"""Контур обратной связи автозаказа: «план vs факт».

При каждом завершённом расчёте пишем лёгкий снимок решения (прогноз
спроса, рекомендация, отправка). Через AUTOPURCHASE_FEEDBACK_DAYS
регламент сверяет прогноз с фактическим спросом (клиентские заказы,
последние версии, без ERROR) и классифицирует исход. Сводка показывает
точность прогноза (MAPE/смещение) и топ промахов — это данные для
осознанной подкрутки порогов автозаказа.
"""
from __future__ import annotations

import logging
import os
from datetime import timedelta
from typing import Any, Optional

from sqlalchemy import func, insert, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.core.time import now_moscow
from dz_fastapi.models.autopart import AutoPart, AutoPurchaseForecastSnapshot
from dz_fastapi.models.partner import (
    CUSTOMER_ORDER_STATUS,
    CustomerOrder,
    CustomerOrderItem,
    PriceList,
    PriceListAutoPartAssociation,
)
from dz_fastapi.services.placed_orders import _normalize_oem

logger = logging.getLogger("dz_fastapi")

# Через сколько дней после расчёта сверяем прогноз с фактом.
AUTOPURCHASE_FEEDBACK_DAYS = max(
    7,
    int(os.getenv("AUTOPURCHASE_FEEDBACK_DAYS", "30")),
)
# Не плодим снимки: если по OEM есть неоценённый снимок моложе N дней —
# новый не создаём (ночной запуск идёт каждый день).
SNAPSHOT_MIN_INTERVAL_DAYS = max(
    1,
    int(os.getenv("AUTOPURCHASE_FEEDBACK_SNAPSHOT_INTERVAL_DAYS", "7")),
)
# |ошибка прогноза| в пределах этого порога считаем точным попаданием.
FORECAST_ACCURACY_TOLERANCE_PCT = 40.0
# Оценённые снимки храним год.
SNAPSHOT_RETENTION_DAYS = 365

OUTCOME_ACCURATE = "accurate"
OUTCOME_OVERFORECAST = "overforecast"
OUTCOME_UNDERFORECAST = "underforecast"
OUTCOME_STOCKOUT_AGAIN = "stockout_again"
OUTCOME_NO_DEMAND = "no_demand"

_EVALUATE_CHUNK = 500


def classify_forecast_outcome(
    *,
    forecast_daily: Optional[float],
    actual_daily: float,
    current_quantity_at_eval: Optional[int],
) -> tuple[str, Optional[float]]:
    """Классификация исхода (чистая функция для юнит-теста).

    Возвращает (outcome, ошибка прогноза в % или None).
    """
    forecast = float(forecast_daily or 0)
    actual = max(float(actual_daily or 0), 0.0)

    if actual <= 0:
        if forecast <= 0:
            return OUTCOME_NO_DEMAND, None
        # Прогнозировали спрос — его не оказалось.
        return OUTCOME_OVERFORECAST, None

    error_pct = round((forecast - actual) / actual * 100.0, 1)

    # Спрос был, а остаток снова ноль — потерянные продажи, самый
    # дорогой исход независимо от численной точности прогноза.
    if current_quantity_at_eval is not None and current_quantity_at_eval <= 0:
        return OUTCOME_STOCKOUT_AGAIN, error_pct

    if abs(error_pct) <= FORECAST_ACCURACY_TOLERANCE_PCT:
        return OUTCOME_ACCURATE, error_pct
    if error_pct > 0:
        return OUTCOME_OVERFORECAST, error_pct
    return OUTCOME_UNDERFORECAST, error_pct


async def record_forecast_snapshots(
    session: AsyncSession,
    *,
    run_id: int,
    rows: list[dict[str, Any]],
) -> int:
    """Пишет снимки прогноза для строк с потребностью.

    Вызывается при персисте завершённого расчёта (в той же транзакции).
    """
    candidates = [
        row
        for row in rows
        if int(row.get("recommended_order_qty") or 0) > 0
        and str(row.get("oem_number") or "").strip()
    ]
    if not candidates:
        return 0

    oems = sorted(
        {_normalize_oem(row["oem_number"]) for row in candidates}
    )
    freshness_cutoff = now_moscow() - timedelta(
        days=SNAPSHOT_MIN_INTERVAL_DAYS
    )
    fresh_stmt = select(
        AutoPurchaseForecastSnapshot.oem_number
    ).where(
        AutoPurchaseForecastSnapshot.oem_number.in_(oems),
        AutoPurchaseForecastSnapshot.evaluated_at.is_(None),
        AutoPurchaseForecastSnapshot.created_at >= freshness_cutoff,
    )
    already_fresh = {
        _normalize_oem(row_oem)
        for row_oem in (await session.execute(fresh_stmt)).scalars().all()
    }

    created_at = now_moscow()
    insert_rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in candidates:
        oem = _normalize_oem(row["oem_number"])
        if not oem or oem in already_fresh or oem in seen:
            continue
        seen.add(oem)
        draft = row.get("draft_purchase_order") or {}
        insert_rows.append(
            {
                "created_at": created_at,
                "run_id": run_id,
                "autopart_id": row.get("autopart_id"),
                "oem_number": oem,
                "brand_name": row.get("brand_name"),
                "forecast_avg_daily": row.get("avg_daily_blended"),
                "recommended_qty": int(
                    row.get("recommended_order_qty") or 0
                ),
                "proposed_qty": draft.get("proposed_order_qty"),
                "sent_qty": None,
                "purchase_price": draft.get("price"),
                "current_quantity_at_run": int(
                    row.get("current_quantity") or 0
                ),
                "target_stock": row.get("target_stock"),
            }
        )
    if insert_rows:
        await session.execute(
            insert(AutoPurchaseForecastSnapshot), insert_rows
        )
    return len(insert_rows)


async def update_sent_snapshot_quantities(
    session: AsyncSession,
    *,
    run_id: int,
    sent_items: list[Any],
) -> None:
    """Фиксирует фактически отправленное количество в снимках run'а."""
    for item in sent_items:
        oem = _normalize_oem(getattr(item, "oem_number", None))
        if not oem:
            continue
        draft = dict(getattr(item, "draft_purchase_order", None) or {})
        sent_qty = int(
            draft.get("proposed_order_qty")
            or getattr(item, "recommended_order_qty", 0)
            or 0
        )
        await session.execute(
            update(AutoPurchaseForecastSnapshot)
            .where(
                AutoPurchaseForecastSnapshot.run_id == run_id,
                AutoPurchaseForecastSnapshot.oem_number == oem,
                AutoPurchaseForecastSnapshot.evaluated_at.is_(None),
            )
            .values(sent_qty=sent_qty)
        )


async def _load_actual_demand_by_oem(
    session: AsyncSession,
    oems: list[str],
    *,
    period_start,
    period_end,
) -> dict[str, int]:
    """Фактический спрос за период: клиентские заказы.

    Только последние версии заказов (без повторных загрузок того же
    номера) и без заказов со статусом ERROR — та же логика, что в
    расчёте спроса автозаказа.
    """
    from dz_fastapi.services.autopurchase import _load_superseded_customer_order_ids

    totals = {oem: 0 for oem in oems}
    if not oems:
        return totals
    superseded = await _load_superseded_customer_order_ids(
        session, cutoff=period_start
    )
    stmt = (
        select(
            CustomerOrderItem.oem,
            CustomerOrder.id,
            CustomerOrderItem.requested_qty,
        )
        .join(CustomerOrder, CustomerOrder.id == CustomerOrderItem.order_id)
        .where(
            CustomerOrder.received_at >= period_start,
            CustomerOrder.received_at <= period_end,
            CustomerOrder.status != CUSTOMER_ORDER_STATUS.ERROR,
            CustomerOrderItem.requested_qty.isnot(None),
            CustomerOrderItem.requested_qty > 0,
        )
    )
    allowed = set(oems)
    for oem_raw, order_id, qty in (await session.execute(stmt)).all():
        if int(order_id) in superseded:
            continue
        oem = _normalize_oem(oem_raw)
        if oem in allowed:
            totals[oem] += max(int(qty or 0), 0)
    return totals


async def _load_current_stock_by_oem(
    session: AsyncSession,
    oems: list[str],
) -> dict[str, int]:
    """Текущий остаток из последнего снапшота нашего прайса."""
    from dz_fastapi.services.autopurchase import _resolve_autopurchase_provider_config

    result = {oem: 0 for oem in oems}
    if not oems:
        return result
    try:
        config_row = await _resolve_autopurchase_provider_config(session)
    except ValueError:
        return result
    latest_pl_id = (
        await session.execute(
            select(PriceList.id)
            .where(
                PriceList.provider_config_id
                == int(config_row["provider_config_id"]),
                PriceList.is_active.is_(True),
            )
            .order_by(PriceList.date.desc().nullslast(), PriceList.id.desc())
            .limit(1)
        )
    ).scalar()
    if latest_pl_id is None:
        return result
    stmt = (
        select(
            AutoPart.oem_number,
            func.sum(PriceListAutoPartAssociation.quantity),
        )
        .join(AutoPart, AutoPart.id == PriceListAutoPartAssociation.autopart_id)
        .where(PriceListAutoPartAssociation.pricelist_id == latest_pl_id)
        .group_by(AutoPart.oem_number)
    )
    allowed = set(oems)
    for oem_raw, qty in (await session.execute(stmt)).all():
        oem = _normalize_oem(oem_raw)
        if oem in allowed:
            result[oem] = result.get(oem, 0) + int(qty or 0)
    return result


async def evaluate_due_forecast_snapshots(
    session: AsyncSession,
) -> dict[str, int]:
    """Оценивает созревшие снимки (возраст >= AUTOPURCHASE_FEEDBACK_DAYS)."""
    now = now_moscow()
    due_cutoff = now - timedelta(days=AUTOPURCHASE_FEEDBACK_DAYS)
    due_stmt = (
        select(AutoPurchaseForecastSnapshot)
        .where(
            AutoPurchaseForecastSnapshot.evaluated_at.is_(None),
            AutoPurchaseForecastSnapshot.created_at <= due_cutoff,
        )
        .order_by(AutoPurchaseForecastSnapshot.created_at.asc())
        .limit(_EVALUATE_CHUNK)
    )
    snapshots = list((await session.execute(due_stmt)).scalars().all())
    if not snapshots:
        return {"evaluated": 0}

    period_start = min(snap.created_at for snap in snapshots)
    oems = sorted({snap.oem_number for snap in snapshots})
    demand_by_oem = await _load_actual_demand_by_oem(
        session,
        oems,
        period_start=period_start,
        period_end=now,
    )
    stock_by_oem = await _load_current_stock_by_oem(session, oems)

    evaluated = 0
    outcome_counts: dict[str, int] = {}
    for snap in snapshots:
        # Фактический спрос ЗА ОКНО этого снимка (created_at → +N дней).
        # demand_by_oem посчитан от самого раннего снимка — для точности
        # окна отдельных снимков близки (регламент ежедневный), а спрос
        # усредняется в шт/день по фактической длительности.
        window_days = max(
            (now - snap.created_at).total_seconds() / 86400.0, 1.0
        )
        actual_sold = int(demand_by_oem.get(snap.oem_number, 0))
        actual_daily = actual_sold / window_days
        outcome, error_pct = classify_forecast_outcome(
            forecast_daily=(
                float(snap.forecast_avg_daily)
                if snap.forecast_avg_daily is not None
                else None
            ),
            actual_daily=actual_daily,
            current_quantity_at_eval=stock_by_oem.get(snap.oem_number),
        )
        snap.evaluated_at = now
        snap.actual_sold_qty = actual_sold
        snap.actual_avg_daily = round(actual_daily, 2)
        snap.forecast_error_pct = error_pct
        snap.current_quantity_at_eval = stock_by_oem.get(snap.oem_number)
        snap.outcome = outcome
        outcome_counts[outcome] = outcome_counts.get(outcome, 0) + 1
        evaluated += 1

    # Чистка совсем старых оценённых снимков.
    retention_cutoff = now - timedelta(days=SNAPSHOT_RETENTION_DAYS)
    from sqlalchemy import delete

    await session.execute(
        delete(AutoPurchaseForecastSnapshot).where(
            AutoPurchaseForecastSnapshot.evaluated_at.is_not(None),
            AutoPurchaseForecastSnapshot.created_at < retention_cutoff,
        )
    )
    await session.commit()
    logger.info(
        "Autopurchase feedback: evaluated %s snapshots (%s)",
        evaluated,
        outcome_counts,
    )
    return {"evaluated": evaluated, **outcome_counts}


def _snapshot_row_payload(
    snap: AutoPurchaseForecastSnapshot,
) -> dict[str, Any]:
    return {
        "oem_number": snap.oem_number,
        "brand_name": snap.brand_name,
        "created_at": snap.created_at,
        "forecast_avg_daily": (
            float(snap.forecast_avg_daily)
            if snap.forecast_avg_daily is not None
            else None
        ),
        "actual_avg_daily": (
            float(snap.actual_avg_daily)
            if snap.actual_avg_daily is not None
            else None
        ),
        "forecast_error_pct": (
            float(snap.forecast_error_pct)
            if snap.forecast_error_pct is not None
            else None
        ),
        "recommended_qty": int(snap.recommended_qty or 0),
        "sent_qty": snap.sent_qty,
        "actual_sold_qty": snap.actual_sold_qty,
        "current_quantity_at_eval": snap.current_quantity_at_eval,
        "outcome": snap.outcome,
    }


async def get_autopurchase_feedback_report(
    session: AsyncSession,
    *,
    top_limit: int = 15,
) -> dict[str, Any]:
    total = (
        await session.execute(
            select(func.count()).select_from(AutoPurchaseForecastSnapshot)
        )
    ).scalar() or 0
    pending = (
        await session.execute(
            select(func.count())
            .select_from(AutoPurchaseForecastSnapshot)
            .where(AutoPurchaseForecastSnapshot.evaluated_at.is_(None))
        )
    ).scalar() or 0

    outcome_rows = (
        await session.execute(
            select(
                AutoPurchaseForecastSnapshot.outcome,
                func.count(),
            )
            .where(AutoPurchaseForecastSnapshot.evaluated_at.is_not(None))
            .group_by(AutoPurchaseForecastSnapshot.outcome)
        )
    ).all()
    outcomes = {str(outcome): int(cnt) for outcome, cnt in outcome_rows}

    error_stats = (
        await session.execute(
            select(
                func.avg(
                    func.abs(AutoPurchaseForecastSnapshot.forecast_error_pct)
                ),
                func.avg(AutoPurchaseForecastSnapshot.forecast_error_pct),
            ).where(
                AutoPurchaseForecastSnapshot.forecast_error_pct.is_not(None)
            )
        )
    ).one()
    mape = round(float(error_stats[0]), 1) if error_stats[0] is not None else None
    bias = round(float(error_stats[1]), 1) if error_stats[1] is not None else None

    async def _top(outcome: str, order_by) -> list[dict[str, Any]]:
        rows = (
            await session.execute(
                select(AutoPurchaseForecastSnapshot)
                .where(AutoPurchaseForecastSnapshot.outcome == outcome)
                .order_by(order_by)
                .limit(top_limit)
            )
        ).scalars().all()
        return [_snapshot_row_payload(snap) for snap in rows]

    return {
        "generated_at": now_moscow(),
        "feedback_days": AUTOPURCHASE_FEEDBACK_DAYS,
        "accuracy_tolerance_pct": FORECAST_ACCURACY_TOLERANCE_PCT,
        "total_snapshots": int(total),
        "pending_snapshots": int(pending),
        "evaluated_snapshots": int(total) - int(pending),
        "mape_pct": mape,
        "bias_pct": bias,
        "outcomes": outcomes,
        "top_overforecast": await _top(
            OUTCOME_OVERFORECAST,
            AutoPurchaseForecastSnapshot.forecast_error_pct.desc().nulls_last(),
        ),
        "top_stockout_again": await _top(
            OUTCOME_STOCKOUT_AGAIN,
            AutoPurchaseForecastSnapshot.actual_avg_daily.desc().nulls_last(),
        ),
        "top_underforecast": await _top(
            OUTCOME_UNDERFORECAST,
            AutoPurchaseForecastSnapshot.forecast_error_pct.asc().nulls_last(),
        ),
    }
