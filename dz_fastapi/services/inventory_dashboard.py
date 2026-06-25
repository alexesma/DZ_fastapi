"""Аналитика контроля запасов для дашборда.

Считает по текущим данным (снапшоты нашего прайса, заказы клиентов,
поступления) — БЕЗ обращений к сайту Dragonzap. Видит ВСЕ позиции
нашего прайса, включая мёртвый сток, который автозаказ отфильтровывает.

История продаж из 1С (помесячно за 5 лет) подключается отдельным этапом —
тогда появятся «забытые чемпионы» и сезонность.
"""
from __future__ import annotations

import logging
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.core.time import now_moscow
from dz_fastapi.models.autopart import AutoPart
from dz_fastapi.models.brand import Brand
from dz_fastapi.models.partner import PriceList, PriceListAutoPartAssociation
from dz_fastapi.services.autopurchase import (
    AUTOPURCHASE_DEMAND_WINDOWS,
    _blend_average_daily_horizons,
    _calculate_in_stock_days,
    _calculate_snapshot_sales,
    _compute_availability_adjusted_daily,
    _load_customer_order_requested_by_oem_windows,
    _resolve_autopurchase_provider_config,
    _summarize_snapshot_rows,
)
from dz_fastapi.services.placed_orders import (
    _ACTIVE_ORDER_STATUSES,
    _compute_single_oem_abc_xyz_batch,
    _load_tracking_history_rows_for_oems,
    _normalize_oem,
)

logger = logging.getLogger("dz_fastapi")

# Пороги классификации состояния запаса (дни покрытия текущим остатком).
URGENT_DAYS_LEFT = 14
SLOW_COVER_DAYS = 90
OVERSTOCK_COVER_DAYS = 180
# Сколько строк показываем в каждой панели дашборда.
DASHBOARD_TOP_LIMIT = 100

INVENTORY_STATE_URGENT = "urgent"
INVENTORY_STATE_HEALTHY = "healthy"
INVENTORY_STATE_SLOW = "slow"
INVENTORY_STATE_DEAD = "dead"
INVENTORY_STATE_OVERSTOCK = "overstock"
INVENTORY_STATE_OOS_DEMAND = "out_of_stock_demand"
INVENTORY_STATE_OOS_NO_DEMAND = "out_of_stock_no_demand"


def resolve_inventory_unit_cost(
    catalog_unit_cost: Optional[float],
    own_pricelist_price: Optional[float],
) -> tuple[Optional[float], Optional[str]]:
    if catalog_unit_cost and catalog_unit_cost > 0:
        return catalog_unit_cost, "catalog_cost"
    if own_pricelist_price and own_pricelist_price > 0:
        return own_pricelist_price, "own_pricelist_estimate"
    return None, None


def classify_inventory_state(
    *,
    current_quantity: int,
    avg_daily: Optional[float],
    estimated_days_left: Optional[int],
    sold_last_365_days: int,
) -> str:
    """Чистая классификация состояния позиции (для юнит-теста).

    - нет остатка + есть спрос → потенциально упущенные продажи;
    - нет остатка, спроса нет → нейтрально;
    - остаток есть, спрос есть → срочно/норма/медленно/затоварено по
      числу дней покрытия;
    - остаток есть, спроса нет совсем → мёртвый сток.
    """
    has_demand = bool((avg_daily and avg_daily > 0) or sold_last_365_days > 0)
    if current_quantity <= 0:
        return (
            INVENTORY_STATE_OOS_DEMAND
            if has_demand
            else INVENTORY_STATE_OOS_NO_DEMAND
        )

    if not avg_daily or avg_daily <= 0:
        # Остаток лежит, продаж нет вообще — замороженные деньги.
        return INVENTORY_STATE_DEAD

    if estimated_days_left is not None and estimated_days_left <= URGENT_DAYS_LEFT:
        return INVENTORY_STATE_URGENT
    if estimated_days_left is not None and estimated_days_left > OVERSTOCK_COVER_DAYS:
        return INVENTORY_STATE_OVERSTOCK
    if estimated_days_left is not None and estimated_days_left > SLOW_COVER_DAYS:
        return INVENTORY_STATE_SLOW
    return INVENTORY_STATE_HEALTHY


async def _load_unit_cost_by_autopart(
    session: AsyncSession,
    autopart_ids: list[int],
) -> dict[int, float]:
    """Себестоимость для оценки замороженных денег.

    Берём purchase_price из карточки; если 0 — fallback на wholesale_price.
    """
    result: dict[int, float] = {}
    ids = sorted({int(item) for item in autopart_ids if item is not None})
    if not ids:
        return result
    stmt = select(
        AutoPart.id,
        AutoPart.purchase_price,
        AutoPart.wholesale_price,
    ).where(AutoPart.id.in_(ids))
    for ap_id, purchase_price, wholesale_price in (
        await session.execute(stmt)
    ).all():
        cost = float(purchase_price or 0) or float(wholesale_price or 0)
        if cost > 0:
            result[int(ap_id)] = cost
    return result


async def get_inventory_control_dashboard(
    session: AsyncSession,
    *,
    own_provider_config_id: Optional[int] = None,
) -> dict[str, Any]:
    config_row = await _resolve_autopurchase_provider_config(
        session,
        own_provider_config_id=own_provider_config_id,
    )
    provider_config_id = int(config_row["provider_config_id"])

    snapshot_stmt = (
        select(
            PriceList.id.label("pricelist_id"),
            PriceList.date.label("pricelist_date"),
            AutoPart.id.label("autopart_id"),
            AutoPart.oem_number.label("oem_number"),
            AutoPart.name.label("autopart_name"),
            AutoPart.minimum_balance.label("minimum_balance"),
            AutoPart.multiplicity.label("multiplicity"),
            AutoPart.min_balance_auto.label("min_balance_auto"),
            AutoPart.min_balance_user.label("min_balance_user"),
            Brand.name.label("brand_name"),
            PriceListAutoPartAssociation.quantity.label("quantity"),
            PriceListAutoPartAssociation.price.label("price"),
        )
        .select_from(PriceListAutoPartAssociation)
        .join(
            PriceList,
            PriceList.id == PriceListAutoPartAssociation.pricelist_id,
        )
        .join(AutoPart, AutoPart.id == PriceListAutoPartAssociation.autopart_id)
        .join(Brand, Brand.id == AutoPart.brand_id)
        .where(
            PriceList.provider_config_id == provider_config_id,
            PriceList.is_active.is_(True),
        )
        .order_by(
            PriceList.date.asc(),
            PriceList.id.asc(),
            AutoPart.oem_number.asc(),
        )
    )
    snapshot_rows = list(
        (await session.execute(snapshot_stmt)).mappings().all()
    )
    generated_at = now_moscow()
    empty_summary = {
        "total_skus": 0,
        "in_stock_skus": 0,
        "out_of_stock_skus": 0,
        "out_of_stock_with_demand_skus": 0,
        "urgent_count": 0,
        "dead_stock_skus": 0,
        "slow_stock_skus": 0,
        "healthy_skus": 0,
        "stock_value": 0.0,
        "dead_stock_value": 0.0,
        "service_level_pct": None,
        "inventory_turnover": None,
        "valuation_fallback_skus": 0,
    }
    history_note = (
        "«Забытые чемпионы» и сезонность появятся после загрузки истории "
        "продаж из 1С."
    )
    if not snapshot_rows:
        return {
            "generated_at": generated_at,
            "provider_config_id": provider_config_id,
            "provider_name": config_row.get("provider_name"),
            "summary": empty_summary,
            "abc_xyz_matrix": [],
            "urgent_to_order": [],
            "dead_stock": [],
            "slow_movers": [],
            "out_of_stock_with_demand": [],
            "history_pending_note": history_note,
        }

    snapshots_by_key, latest_known_rows_by_oem, latest_rows_by_oem = (
        _summarize_snapshot_rows(snapshot_rows)
    )
    snapshots = list(snapshots_by_key.values())
    normalized_oem_numbers = sorted(latest_known_rows_by_oem.keys())

    history_rows = await _load_tracking_history_rows_for_oems(
        session,
        normalized_oem_numbers=normalized_oem_numbers,
    )
    history_by_oem: dict[str, list[dict[str, Any]]] = {}
    for row in history_rows:
        normalized = _normalize_oem(row.get("oem_number"))
        if normalized:
            history_by_oem.setdefault(normalized, []).append(row)

    abc_xyz_by_oem = await _compute_single_oem_abc_xyz_batch(
        session,
        normalized_oem_numbers=normalized_oem_numbers,
        history_rows_by_oem=history_by_oem,
    )
    snapshot_demand_by_window = {
        days: _calculate_snapshot_sales(
            snapshots, normalized_oem_numbers, days=days
        )
        for days in AUTOPURCHASE_DEMAND_WINDOWS
    }
    in_stock_days_by_window = {
        days: _calculate_in_stock_days(
            snapshots, normalized_oem_numbers, days=days
        )
        for days in AUTOPURCHASE_DEMAND_WINDOWS
    }
    customer_requested_by_window = (
        await _load_customer_order_requested_by_oem_windows(
            session, normalized_oem_numbers, windows=AUTOPURCHASE_DEMAND_WINDOWS
        )
    )
    unit_cost_by_autopart = await _load_unit_cost_by_autopart(
        session,
        [
            int(row.get("autopart_id"))
            for row in latest_known_rows_by_oem.values()
            if row.get("autopart_id") is not None
        ],
    )

    rows: list[dict[str, Any]] = []
    abc_xyz_cells: dict[tuple[str, str], dict[str, Any]] = {}
    summary = dict(empty_summary)
    demand_skus = 0
    in_stock_demand_skus = 0
    total_annual_sales_value = 0.0
    total_avg_stock_value = 0.0

    for oem_number, known_row in latest_known_rows_by_oem.items():
        latest = latest_rows_by_oem.get(oem_number) or {
            **known_row,
            "current_quantity": 0,
        }
        current_quantity = int(latest.get("current_quantity") or 0)
        autopart_id = latest.get("autopart_id") or known_row.get("autopart_id")

        sold = {
            days: max(
                int(customer_requested_by_window[days].get(oem_number, 0)),
                int(snapshot_demand_by_window[days].get(oem_number, 0)),
            )
            for days in AUTOPURCHASE_DEMAND_WINDOWS
        }
        avg_daily = _blend_average_daily_horizons(
            _compute_availability_adjusted_daily(
                sold[30], 30, in_stock_days_by_window[30].get(oem_number, 30)
            ),
            _compute_availability_adjusted_daily(
                sold[90], 90, in_stock_days_by_window[90].get(oem_number, 90)
            ),
            _compute_availability_adjusted_daily(
                sold[180], 180,
                in_stock_days_by_window[180].get(oem_number, 180),
            ),
            _compute_availability_adjusted_daily(
                sold[365], 365,
                in_stock_days_by_window[365].get(oem_number, 365),
            ),
        )
        in_stock_days = {
            days: int(in_stock_days_by_window[days].get(oem_number, 0))
            for days in AUTOPURCHASE_DEMAND_WINDOWS
        }
        estimated_days_left = (
            int(current_quantity / avg_daily)
            if avg_daily and avg_daily > 0
            else None
        )
        in_transit_qty = max(
            sum(
                max(
                    int(row.get("ordered_quantity") or 0)
                    - int(row.get("received_quantity") or 0),
                    0,
                )
                for row in history_by_oem.get(oem_number, [])
                if row.get("current_status") in _ACTIVE_ORDER_STATUSES
            ),
            0,
        )

        abc_xyz = abc_xyz_by_oem.get(oem_number) or {}
        abc_class = str(abc_xyz.get("abc_class") or "").upper() or "—"
        xyz_class = str(abc_xyz.get("xyz_class") or "").upper() or "—"

        catalog_unit_cost = (
            unit_cost_by_autopart.get(int(autopart_id))
            if autopart_id is not None
            else None
        )
        sale_price = (
            float(latest.get("latest_price"))
            if latest.get("latest_price") is not None
            else None
        )
        unit_cost, unit_cost_source = resolve_inventory_unit_cost(
            catalog_unit_cost,
            sale_price,
        )
        if unit_cost_source == "own_pricelist_estimate":
            summary["valuation_fallback_skus"] += 1
        stock_value = (current_quantity * unit_cost) if unit_cost else 0.0
        annual_sales_value = (
            sold[365] * unit_cost if unit_cost else 0.0
        )

        state = classify_inventory_state(
            current_quantity=current_quantity,
            avg_daily=avg_daily,
            estimated_days_left=estimated_days_left,
            sold_last_365_days=sold[365],
        )

        # ── Сводка ───────────────────────────────────────────────────
        summary["total_skus"] += 1
        if current_quantity > 0:
            summary["in_stock_skus"] += 1
            summary["stock_value"] += stock_value
            total_avg_stock_value += stock_value
        else:
            summary["out_of_stock_skus"] += 1
        has_demand = bool((avg_daily and avg_daily > 0) or sold[365] > 0)
        if has_demand:
            demand_skus += 1
            if current_quantity > 0:
                in_stock_demand_skus += 1
        total_annual_sales_value += annual_sales_value

        if state == INVENTORY_STATE_URGENT:
            summary["urgent_count"] += 1
        elif state == INVENTORY_STATE_DEAD:
            summary["dead_stock_skus"] += 1
            summary["dead_stock_value"] += stock_value
        elif state in (INVENTORY_STATE_SLOW, INVENTORY_STATE_OVERSTOCK):
            summary["slow_stock_skus"] += 1
        elif state == INVENTORY_STATE_HEALTHY:
            summary["healthy_skus"] += 1
        elif state == INVENTORY_STATE_OOS_DEMAND:
            summary["out_of_stock_with_demand_skus"] += 1

        # ── ABC/XYZ-матрица (только позиции с классом) ───────────────
        if abc_class != "—" and xyz_class != "—":
            cell = abc_xyz_cells.setdefault(
                (abc_class, xyz_class),
                {
                    "abc_class": abc_class,
                    "xyz_class": xyz_class,
                    "sku_count": 0,
                    "stock_value": 0.0,
                    "annual_sales_value": 0.0,
                },
            )
            cell["sku_count"] += 1
            cell["stock_value"] += stock_value
            cell["annual_sales_value"] += annual_sales_value

        rows.append(
            {
                "oem_number": oem_number,
                "brand_name": latest.get("brand_name")
                or known_row.get("brand_name"),
                "autopart_name": latest.get("autopart_name")
                or known_row.get("autopart_name"),
                "autopart_id": autopart_id,
                "state": state,
                "current_quantity": current_quantity,
                "in_transit_qty": in_transit_qty,
                "avg_daily": avg_daily,
                "estimated_days_left": estimated_days_left,
                "sold_last_30_days": sold[30],
                "sold_last_90_days": sold[90],
                "sold_last_365_days": sold[365],
                "in_stock_days_30": in_stock_days[30],
                "in_stock_days_90": in_stock_days[90],
                "in_stock_days_180": in_stock_days[180],
                "in_stock_days_365": in_stock_days[365],
                "unit_cost": round(unit_cost, 2) if unit_cost else None,
                "unit_cost_source": unit_cost_source,
                "frozen_value": round(stock_value, 2) if stock_value else None,
                "sale_price": sale_price,
                "abc_class": abc_class if abc_class != "—" else None,
                "xyz_class": xyz_class if xyz_class != "—" else None,
            }
        )

    summary["stock_value"] = round(summary["stock_value"], 2)
    summary["dead_stock_value"] = round(summary["dead_stock_value"], 2)
    summary["service_level_pct"] = (
        round(in_stock_demand_skus / demand_skus * 100, 1)
        if demand_skus > 0
        else None
    )
    # Оборачиваемость ≈ годовая себестоимость продаж / средний сток (грубо
    # по текущему стоку — точную даст история из 1С).
    summary["inventory_turnover"] = (
        round(total_annual_sales_value / total_avg_stock_value, 2)
        if total_avg_stock_value > 0
        else None
    )

    def _top(state_names: set[str], sort_key) -> list[dict[str, Any]]:
        filtered = [row for row in rows if row["state"] in state_names]
        filtered.sort(key=sort_key)
        return filtered[:DASHBOARD_TOP_LIMIT]

    urgent_to_order = _top(
        {INVENTORY_STATE_URGENT},
        lambda row: (
            row["estimated_days_left"]
            if row["estimated_days_left"] is not None
            else 9_999
        ),
    )
    dead_stock = _top(
        {INVENTORY_STATE_DEAD},
        lambda row: -(row["frozen_value"] or 0),
    )
    slow_movers = _top(
        {INVENTORY_STATE_SLOW, INVENTORY_STATE_OVERSTOCK},
        lambda row: -(row["frozen_value"] or 0),
    )
    out_of_stock_with_demand = _top(
        {INVENTORY_STATE_OOS_DEMAND},
        lambda row: -row["sold_last_365_days"],
    )

    abc_xyz_matrix = sorted(
        (
            {
                **cell,
                "stock_value": round(cell["stock_value"], 2),
                "annual_sales_value": round(cell["annual_sales_value"], 2),
            }
            for cell in abc_xyz_cells.values()
        ),
        key=lambda cell: (cell["abc_class"], cell["xyz_class"]),
    )

    return {
        "generated_at": generated_at,
        "provider_config_id": provider_config_id,
        "provider_name": config_row.get("provider_name"),
        "summary": summary,
        "abc_xyz_matrix": abc_xyz_matrix,
        "urgent_to_order": urgent_to_order,
        "dead_stock": dead_stock,
        "slow_movers": slow_movers,
        "out_of_stock_with_demand": out_of_stock_with_demand,
        "history_pending_note": history_note,
    }
