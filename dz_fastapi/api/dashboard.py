from collections import defaultdict
from datetime import timedelta
from statistics import median
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import func, literal, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased

from dz_fastapi.api.deps import require_admin
from dz_fastapi.core.db import get_session
from dz_fastapi.core.time import now_moscow
from dz_fastapi.models.partner import (
    Customer,
    CustomerOrder,
    CustomerOrderItem,
    Order,
    OrderItem,
    PriceList,
    PriceListAutoPartAssociation,
    Provider,
    ProviderPriceListConfig,
    SupplierOrder,
    SupplierOrderItem,
)
from dz_fastapi.schemas.dashboard import (
    DashboardOrderDynamicsResponse,
    DashboardSupplierReliabilityResponse,
    InventoryDashboardResponse,
    SupplierPriceTrendPoint,
    SupplierPriceTrendResponse,
    SupplierPriceTrendSeries,
)
from dz_fastapi.services.inventory_dashboard import get_inventory_control_dashboard

router = APIRouter()


@router.get(
    "/dashboard/inventory-control",
    tags=["dashboard"],
    status_code=status.HTTP_200_OK,
    response_model=InventoryDashboardResponse,
    dependencies=[Depends(require_admin)],
)
async def get_inventory_control(
    own_provider_config_id: Optional[int] = Query(default=None),
    session: AsyncSession = Depends(get_session),
):
    try:
        return await get_inventory_control_dashboard(
            session=session,
            own_provider_config_id=own_provider_config_id,
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get(
    "/dashboard/order-dynamics",
    tags=["dashboard"],
    status_code=status.HTTP_200_OK,
    response_model=DashboardOrderDynamicsResponse,
    dependencies=[Depends(require_admin)],
)
async def get_order_dynamics(
    days: int = Query(default=14, ge=7, le=31),
    partner_limit: int = Query(default=10, ge=1, le=30),
    session: AsyncSession = Depends(get_session),
):
    today = now_moscow().date()
    start_date = today - timedelta(days=days - 1)
    customer_day = func.date(
        func.timezone("Europe/Moscow", CustomerOrder.received_at)
    )
    supplier_day = func.date(
        func.timezone("Europe/Moscow", SupplierOrder.created_at)
    )
    site_order_day = func.date(
        func.timezone("Europe/Moscow", Order.created_at)
    )
    customer_sum_expr = func.coalesce(
        func.sum(
            CustomerOrderItem.requested_qty
            * func.coalesce(
                CustomerOrderItem.requested_price,
                CustomerOrderItem.matched_price,
                0,
            )
        ),
        0,
    )
    supplier_sum_expr = func.coalesce(
        func.sum(
            SupplierOrderItem.quantity
            * func.coalesce(SupplierOrderItem.price, 0)
        ),
        0,
    )
    site_order_sum_expr = func.coalesce(
        func.sum(OrderItem.quantity * func.coalesce(OrderItem.price, 0)),
        0,
    )

    customer_daily_stmt = (
        select(
            customer_day.label("day"),
            func.count(func.distinct(CustomerOrder.id)).label("order_count"),
            func.count(CustomerOrderItem.id).label("position_count"),
            func.coalesce(func.sum(CustomerOrderItem.requested_qty), 0).label(
                "quantity"
            ),
            customer_sum_expr.label("total_sum"),
        )
        .select_from(CustomerOrder)
        .join(CustomerOrderItem, CustomerOrderItem.order_id == CustomerOrder.id)
        .where(customer_day >= start_date)
        .group_by(customer_day)
        .order_by(customer_day.asc())
    )
    supplier_daily_stmt = (
        select(
            supplier_day.label("day"),
            func.count(func.distinct(SupplierOrder.id)).label("order_count"),
            func.count(SupplierOrderItem.id).label("position_count"),
            func.coalesce(func.sum(SupplierOrderItem.quantity), 0).label(
                "quantity"
            ),
            supplier_sum_expr.label("total_sum"),
        )
        .select_from(SupplierOrder)
        .join(
            SupplierOrderItem,
            SupplierOrderItem.supplier_order_id == SupplierOrder.id,
        )
        .where(supplier_day >= start_date)
        .group_by(supplier_day)
        .order_by(supplier_day.asc())
    )
    site_order_daily_stmt = (
        select(
            site_order_day.label("day"),
            func.count(func.distinct(Order.id)).label("order_count"),
            func.count(OrderItem.id).label("position_count"),
            func.coalesce(func.sum(OrderItem.quantity), 0).label("quantity"),
            site_order_sum_expr.label("total_sum"),
        )
        .select_from(Order)
        .join(OrderItem, OrderItem.order_id == Order.id)
        .where(site_order_day >= start_date)
        .group_by(site_order_day)
        .order_by(site_order_day.asc())
    )
    customer_partner_stmt = (
        select(
            Customer.id.label("partner_id"),
            Customer.name.label("partner_name"),
            func.count(func.distinct(CustomerOrder.id)).label("order_count"),
            func.count(CustomerOrderItem.id).label("position_count"),
            func.coalesce(func.sum(CustomerOrderItem.requested_qty), 0).label(
                "quantity"
            ),
            customer_sum_expr.label("total_sum"),
        )
        .select_from(CustomerOrder)
        .join(Customer, Customer.id == CustomerOrder.customer_id)
        .join(CustomerOrderItem, CustomerOrderItem.order_id == CustomerOrder.id)
        .where(customer_day >= start_date)
        .group_by(Customer.id, Customer.name)
        .order_by(func.sum(CustomerOrderItem.requested_qty).desc())
        .limit(partner_limit)
    )
    supplier_partner_stmt = (
        select(
            Provider.id.label("partner_id"),
            Provider.name.label("partner_name"),
            func.count(func.distinct(SupplierOrder.id)).label("order_count"),
            func.count(SupplierOrderItem.id).label("position_count"),
            func.coalesce(func.sum(SupplierOrderItem.quantity), 0).label(
                "quantity"
            ),
            supplier_sum_expr.label("total_sum"),
        )
        .select_from(SupplierOrder)
        .join(Provider, Provider.id == SupplierOrder.provider_id)
        .join(
            SupplierOrderItem,
            SupplierOrderItem.supplier_order_id == SupplierOrder.id,
        )
        .where(supplier_day >= start_date)
        .group_by(Provider.id, Provider.name)
        .order_by(func.sum(SupplierOrderItem.quantity).desc())
        .limit(partner_limit)
    )
    site_order_partner_stmt = (
        select(
            Provider.id.label("partner_id"),
            Provider.name.label("partner_name"),
            func.count(func.distinct(Order.id)).label("order_count"),
            func.count(OrderItem.id).label("position_count"),
            func.coalesce(func.sum(OrderItem.quantity), 0).label("quantity"),
            site_order_sum_expr.label("total_sum"),
        )
        .select_from(Order)
        .join(Provider, Provider.id == Order.provider_id)
        .join(OrderItem, OrderItem.order_id == Order.id)
        .where(site_order_day >= start_date)
        .group_by(Provider.id, Provider.name)
    )

    customer_daily_rows = (await session.execute(customer_daily_stmt)).all()
    supplier_daily_rows = (await session.execute(supplier_daily_stmt)).all()
    site_order_daily_rows = (
        await session.execute(site_order_daily_stmt)
    ).all()
    customer_partner_rows = (
        await session.execute(customer_partner_stmt)
    ).all()
    supplier_partner_rows = (
        await session.execute(supplier_partner_stmt)
    ).all()
    site_order_partner_rows = (
        await session.execute(site_order_partner_stmt)
    ).all()

    def aggregate_map(*row_groups):
        result = {}
        for rows in row_groups:
            for row in rows:
                item = result.setdefault(
                    row.day,
                    {
                        "order_count": 0,
                        "position_count": 0,
                        "quantity": 0,
                        "total_sum": 0.0,
                    },
                )
                item["order_count"] += int(row.order_count or 0)
                item["position_count"] += int(row.position_count or 0)
                item["quantity"] += int(row.quantity or 0)
                item["total_sum"] += float(row.total_sum or 0)
        return result

    customer_by_day = aggregate_map(customer_daily_rows)
    supplier_by_day = aggregate_map(
        supplier_daily_rows,
        site_order_daily_rows,
    )
    daily = []
    for offset in range(days):
        day = start_date + timedelta(days=offset)
        customer_values = customer_by_day.get(day, {})
        supplier_values = supplier_by_day.get(day, {})
        daily.append(
            {
                "date": day,
                "customer_order_count": customer_values.get("order_count", 0),
                "customer_position_count": customer_values.get(
                    "position_count", 0
                ),
                "customer_qty": customer_values.get("quantity", 0),
                "customer_sum": customer_values.get("total_sum", 0.0),
                "supplier_order_count": supplier_values.get("order_count", 0),
                "supplier_position_count": supplier_values.get(
                    "position_count", 0
                ),
                "supplier_qty": supplier_values.get("quantity", 0),
                "supplier_sum": supplier_values.get("total_sum", 0.0),
            }
        )

    def partner_payload(*row_groups):
        result = {}
        for rows in row_groups:
            for row in rows:
                partner_id = int(row.partner_id)
                item = result.setdefault(
                    partner_id,
                    {
                        "partner_id": partner_id,
                        "partner_name": row.partner_name or f"#{partner_id}",
                        "order_count": 0,
                        "position_count": 0,
                        "quantity": 0,
                        "total_sum": 0.0,
                    },
                )
                item["order_count"] += int(row.order_count or 0)
                item["position_count"] += int(row.position_count or 0)
                item["quantity"] += int(row.quantity or 0)
                item["total_sum"] += float(row.total_sum or 0)
        return sorted(
            result.values(),
            key=lambda item: (-item["total_sum"], -item["quantity"]),
        )[:partner_limit]

    customer_order_count = sum(
        int(row["customer_order_count"]) for row in daily
    )
    customer_qty = sum(int(row["customer_qty"]) for row in daily)
    customer_sum = sum(float(row["customer_sum"]) for row in daily)
    supplier_order_count = sum(
        int(row["supplier_order_count"]) for row in daily
    )
    supplier_qty = sum(int(row["supplier_qty"]) for row in daily)
    supplier_sum = sum(float(row["supplier_sum"]) for row in daily)
    purchase_coverage_pct = (
        round((supplier_qty / customer_qty) * 100.0, 1)
        if customer_qty > 0
        else None
    )
    return {
        "generated_at": now_moscow(),
        "days": days,
        "summary": {
            "customer_order_count": customer_order_count,
            "customer_qty": customer_qty,
            "customer_sum": customer_sum,
            "supplier_order_count": supplier_order_count,
            "supplier_qty": supplier_qty,
            "supplier_sum": supplier_sum,
            "purchase_coverage_pct": purchase_coverage_pct,
        },
        "daily": daily,
        "customers": partner_payload(customer_partner_rows),
        "suppliers": partner_payload(
            supplier_partner_rows,
            site_order_partner_rows,
        ),
    }


def _build_supplier_reliability(rows, *, generated_at):
    grouped = {}
    for row in rows:
        provider_id = int(row.provider_id)
        item = grouped.setdefault(
            provider_id,
            {
                "provider_id": provider_id,
                "provider_name": row.provider_name or f"#{provider_id}",
                "order_ids": set(),
                "line_count": 0,
                "evaluated_line_count": 0,
                "ordered_qty": 0,
                "evaluated_qty": 0,
                "received_qty": 0,
                "pending_qty": 0,
                "ordered_sum": 0.0,
                "evaluated_sum": 0.0,
                "received_sum": 0.0,
                "pending_sum": 0.0,
                "deadline_line_count": 0,
                "on_time_line_count": 0,
                "late_line_count": 0,
                "lead_days": [],
            },
        )
        ordered_qty = max(int(row.quantity or 0), 0)
        received_qty = min(max(int(row.received_quantity or 0), 0), ordered_qty)
        unit_price = max(float(row.price or 0), 0.0)
        ordered_sum = ordered_qty * unit_price
        received_sum = received_qty * unit_price
        item["order_ids"].add(
            (getattr(row, "order_source", "supplier"), int(row.order_id))
        )
        item["line_count"] += 1
        item["ordered_qty"] += ordered_qty
        item["pending_qty"] += max(ordered_qty - received_qty, 0)
        item["ordered_sum"] += ordered_sum
        item["pending_sum"] += max(ordered_sum - received_sum, 0.0)

        created_at = row.created_at
        received_at = row.received_at
        max_delivery_day = row.max_delivery_day
        deadline = (
            created_at + timedelta(days=max(int(max_delivery_day), 0))
            if created_at is not None and max_delivery_day is not None
            else None
        )
        is_evaluated = received_at is not None or (
            deadline is not None and deadline <= generated_at
        )
        if is_evaluated:
            item["evaluated_line_count"] += 1
            item["evaluated_qty"] += ordered_qty
            item["received_qty"] += received_qty
            item["evaluated_sum"] += ordered_sum
            item["received_sum"] += received_sum

        if deadline is not None and is_evaluated:
            item["deadline_line_count"] += 1
            if received_at is not None and received_at <= deadline:
                item["on_time_line_count"] += 1
            else:
                item["late_line_count"] += 1

        if created_at is not None and received_at is not None:
            lead_days = max(
                (received_at - created_at).total_seconds() / 86400.0,
                0.0,
            )
            item["lead_days"].append(lead_days)

    result = []
    for item in grouped.values():
        evaluated_qty = item["evaluated_qty"]
        evaluated_sum = item["evaluated_sum"]
        deadline_lines = item["deadline_line_count"]
        lead_days = item["lead_days"]
        result.append(
            {
                "provider_id": item["provider_id"],
                "provider_name": item["provider_name"],
                "order_count": len(item["order_ids"]),
                "line_count": item["line_count"],
                "evaluated_line_count": item["evaluated_line_count"],
                "ordered_qty": item["ordered_qty"],
                "evaluated_qty": evaluated_qty,
                "received_qty": item["received_qty"],
                "pending_qty": item["pending_qty"],
                "ordered_sum": round(item["ordered_sum"], 2),
                "evaluated_sum": round(evaluated_sum, 2),
                "received_sum": round(item["received_sum"], 2),
                "pending_sum": round(item["pending_sum"], 2),
                "fill_rate_pct": (
                    round(item["received_sum"] / evaluated_sum * 100.0, 1)
                    if evaluated_sum > 0
                    else None
                ),
                "on_time_pct": (
                    round(
                        item["on_time_line_count"] / deadline_lines * 100.0,
                        1,
                    )
                    if deadline_lines > 0
                    else None
                ),
                "late_line_count": item["late_line_count"],
                "avg_lead_days": (
                    round(sum(lead_days) / len(lead_days), 1)
                    if lead_days
                    else None
                ),
            }
        )
    result.sort(
        key=lambda item: (
            item["fill_rate_pct"] is None,
            -(item["fill_rate_pct"] or 0),
            -(item["ordered_qty"] or 0),
        )
    )
    return result


@router.get(
    "/dashboard/supplier-reliability",
    tags=["dashboard"],
    status_code=status.HTTP_200_OK,
    response_model=DashboardSupplierReliabilityResponse,
    dependencies=[Depends(require_admin)],
)
async def get_supplier_reliability(
    days: int = Query(default=90, ge=30, le=365),
    session: AsyncSession = Depends(get_session),
):
    generated_at = now_moscow()
    date_from = generated_at - timedelta(days=days)
    stmt = (
        select(
            SupplierOrder.id.label("order_id"),
            literal("supplier").label("order_source"),
            SupplierOrder.provider_id,
            Provider.name.label("provider_name"),
            SupplierOrder.created_at,
            SupplierOrderItem.quantity,
            func.coalesce(
                SupplierOrderItem.response_price,
                SupplierOrderItem.price,
                0,
            ).label("price"),
            SupplierOrderItem.received_quantity,
            SupplierOrderItem.received_at,
            SupplierOrderItem.max_delivery_day,
        )
        .select_from(SupplierOrderItem)
        .join(
            SupplierOrder,
            SupplierOrder.id == SupplierOrderItem.supplier_order_id,
        )
        .join(Provider, Provider.id == SupplierOrder.provider_id)
        .where(SupplierOrder.created_at >= date_from)
        .order_by(SupplierOrder.created_at.desc())
    )
    site_stmt = (
        select(
            Order.id.label("order_id"),
            literal("site").label("order_source"),
            Order.provider_id,
            Provider.name.label("provider_name"),
            Order.created_at,
            OrderItem.quantity,
            func.coalesce(OrderItem.price, 0).label("price"),
            OrderItem.received_quantity,
            OrderItem.received_at,
            OrderItem.max_delivery_day,
        )
        .select_from(OrderItem)
        .join(Order, Order.id == OrderItem.order_id)
        .join(Provider, Provider.id == Order.provider_id)
        .where(Order.created_at >= date_from)
        .order_by(Order.created_at.desc())
    )
    rows = [
        *(await session.execute(stmt)).all(),
        *(await session.execute(site_stmt)).all(),
    ]
    return {
        "generated_at": generated_at,
        "days": days,
        "suppliers": _build_supplier_reliability(
            rows,
            generated_at=generated_at,
        ),
    }


def _to_float(value: object) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


async def _load_pair_stats(
    session: AsyncSession,
    prev_pricelist_id: int,
    curr_pricelist_id: int,
) -> tuple[int, Optional[float]]:
    curr_assoc = aliased(PriceListAutoPartAssociation)
    prev_assoc = aliased(PriceListAutoPartAssociation)
    stmt = (
        select(curr_assoc.price, prev_assoc.price)
        .join(
            prev_assoc,
            curr_assoc.autopart_id == prev_assoc.autopart_id,
        )
        .where(
            curr_assoc.pricelist_id == curr_pricelist_id,
            prev_assoc.pricelist_id == prev_pricelist_id,
            curr_assoc.quantity > 0,
            prev_assoc.quantity > 0,
            prev_assoc.price > 0,
        )
    )
    rows = (await session.execute(stmt)).all()
    if not rows:
        return 0, None

    ratios: list[float] = []
    for curr_price, prev_price in rows:
        curr_value = _to_float(curr_price)
        prev_value = _to_float(prev_price)
        if curr_value is None or prev_value is None or prev_value <= 0:
            continue
        ratios.append(((curr_value / prev_value) - 1.0) * 100.0)
    if not ratios:
        return 0, None
    return len(ratios), float(median(ratios))


def _rolling_median(
    values: list[Optional[float]],
    window: int,
) -> list[Optional[float]]:
    result: list[Optional[float]] = []
    for idx, value in enumerate(values):
        if value is None:
            result.append(None)
            continue
        start = max(0, idx - window + 1)
        segment = [
            item for item in values[start:idx + 1] if item is not None
        ]
        if not segment:
            result.append(None)
            continue
        result.append(float(median(segment)))
    return result


@router.get(
    "/dashboard/supplier-price-trends",
    tags=["dashboard"],
    status_code=status.HTTP_200_OK,
    response_model=SupplierPriceTrendResponse,
    dependencies=[Depends(require_admin)],
)
async def get_supplier_price_trends(
    days: int = Query(default=30, ge=1, le=365),
    points_limit: int = Query(default=10, ge=2, le=40),
    smooth_window: int = Query(default=3, ge=1, le=15),
    provider_config_ids: list[int] | None = Query(default=None),
    session: AsyncSession = Depends(get_session),
):
    start_date = now_moscow().date() - timedelta(days=days - 1)
    ranked_stmt = select(
        PriceList.id.label("pricelist_id"),
        PriceList.provider_config_id.label("provider_config_id"),
        PriceList.date.label("price_date"),
        func.row_number()
        .over(
            partition_by=PriceList.provider_config_id,
            order_by=(
                PriceList.date.desc().nullslast(),
                PriceList.id.desc(),
            ),
        )
        .label("rn"),
    ).where(
        PriceList.provider_config_id.is_not(None),
        PriceList.date >= start_date,
    )
    if provider_config_ids:
        ranked_stmt = ranked_stmt.where(
            PriceList.provider_config_id.in_(provider_config_ids)
        )
    ranked_subquery = ranked_stmt.subquery()

    points_stmt = (
        select(
            ranked_subquery.c.pricelist_id,
            ranked_subquery.c.provider_config_id,
            ranked_subquery.c.price_date,
        )
        .where(ranked_subquery.c.rn <= points_limit)
        .order_by(
            ranked_subquery.c.provider_config_id.asc(),
            ranked_subquery.c.price_date.asc(),
            ranked_subquery.c.pricelist_id.asc(),
        )
    )
    point_rows = (await session.execute(points_stmt)).all()
    if not point_rows:
        return SupplierPriceTrendResponse(
            generated_at=now_moscow(),
            days=days,
            points_limit=points_limit,
            smooth_window=smooth_window,
            series=[],
        )

    points_by_provider: dict[int, list[dict]] = defaultdict(list)
    pricelist_ids: list[int] = []
    for row in point_rows:
        pricelist_id = int(row.pricelist_id)
        provider_config_id = int(row.provider_config_id)
        points_by_provider[provider_config_id].append(
            {
                "pricelist_id": pricelist_id,
                "date": row.price_date,
            }
        )
        pricelist_ids.append(pricelist_id)

    metric_stmt = (
        select(
            PriceListAutoPartAssociation.pricelist_id,
            func.count()
            .filter(PriceListAutoPartAssociation.quantity > 0)
            .label("sku_count"),
            func.sum(PriceListAutoPartAssociation.quantity)
            .filter(PriceListAutoPartAssociation.quantity > 0)
            .label("stock_total_qty"),
            func.avg(PriceListAutoPartAssociation.price)
            .filter(PriceListAutoPartAssociation.quantity > 0)
            .label("avg_price"),
        )
        .where(PriceListAutoPartAssociation.pricelist_id.in_(pricelist_ids))
        .group_by(PriceListAutoPartAssociation.pricelist_id)
    )
    metric_rows = (await session.execute(metric_stmt)).all()
    metric_map = {
        int(row.pricelist_id): {
            "sku_count": int(row.sku_count or 0),
            "stock_total_qty": int(row.stock_total_qty or 0),
            "avg_price": _to_float(row.avg_price),
        }
        for row in metric_rows
    }

    provider_ids = list(points_by_provider.keys())
    provider_stmt = (
        select(
            ProviderPriceListConfig.id,
            ProviderPriceListConfig.name_price,
            Provider.id.label("provider_id"),
            Provider.name.label("provider_name"),
        )
        .join(Provider, Provider.id == ProviderPriceListConfig.provider_id)
        .where(ProviderPriceListConfig.id.in_(provider_ids))
    )
    provider_rows = (await session.execute(provider_stmt)).all()
    provider_map = {
        int(row.id): {
            "provider_id": int(row.provider_id),
            "provider_name": row.provider_name,
            "provider_config_name": row.name_price,
        }
        for row in provider_rows
    }

    pair_cache: dict[tuple[int, int], tuple[int, Optional[float]]] = {}
    series: list[SupplierPriceTrendSeries] = []
    for provider_config_id, raw_points in points_by_provider.items():
        ordered_points = sorted(
            raw_points,
            key=lambda item: (item["date"], item["pricelist_id"]),
        )
        points: list[SupplierPriceTrendPoint] = []
        prev_item = None
        for item in ordered_points:
            pricelist_id = int(item["pricelist_id"])
            metric = metric_map.get(pricelist_id, {})
            sku_count = int(metric.get("sku_count", 0))
            stock_total_qty = int(metric.get("stock_total_qty", 0))
            avg_price = metric.get("avg_price")
            step_index_pct = None
            coverage_pct = None
            overlap_count = None
            if prev_item is not None:
                prev_pricelist_id = int(prev_item["pricelist_id"])
                cache_key = (prev_pricelist_id, pricelist_id)
                if cache_key not in pair_cache:
                    pair_cache[cache_key] = await _load_pair_stats(
                        session=session,
                        prev_pricelist_id=prev_pricelist_id,
                        curr_pricelist_id=pricelist_id,
                    )
                overlap_count, step_index_pct = pair_cache[cache_key]
                prev_metric = metric_map.get(prev_pricelist_id, {})
                prev_sku_count = int(prev_metric.get("sku_count", 0))
                if prev_sku_count > 0 and overlap_count is not None:
                    coverage_pct = round(
                        (overlap_count / prev_sku_count) * 100,
                        2,
                    )
            points.append(
                SupplierPriceTrendPoint(
                    pricelist_id=pricelist_id,
                    date=item["date"],
                    sku_count=sku_count,
                    stock_total_qty=stock_total_qty,
                    avg_price=avg_price,
                    step_index_pct=(
                        round(step_index_pct, 2)
                        if step_index_pct is not None
                        else None
                    ),
                    coverage_pct=coverage_pct,
                    overlap_count=overlap_count,
                )
            )
            prev_item = item

        smooth_values = _rolling_median(
            [point.step_index_pct for point in points],
            smooth_window,
        )
        for point, smooth_value in zip(points, smooth_values):
            point.step_index_smooth_pct = (
                round(smooth_value, 2) if smooth_value is not None else None
            )

        provider_info = provider_map.get(provider_config_id, {})
        series.append(
            SupplierPriceTrendSeries(
                provider_config_id=provider_config_id,
                provider_id=provider_info.get("provider_id"),
                provider_name=provider_info.get("provider_name"),
                provider_config_name=provider_info.get("provider_config_name"),
                points=points,
            )
        )

    series.sort(
        key=lambda item: (
            item.provider_name or "",
            item.provider_config_name or "",
        )
    )
    return SupplierPriceTrendResponse(
        generated_at=now_moscow(),
        days=days,
        points_limit=points_limit,
        smooth_window=smooth_window,
        series=series,
    )
