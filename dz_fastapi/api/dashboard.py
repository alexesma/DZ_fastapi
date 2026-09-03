from collections import defaultdict
from datetime import datetime, time, timedelta
from statistics import median
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import Integer, and_, case, column, func, literal, select, text, values
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased

from dz_fastapi.api.deps import require_admin
from dz_fastapi.core.db import get_session
from dz_fastapi.core.time import now_moscow
from dz_fastapi.models.autopart import AutoPart, AutoPartPriceHistory
from dz_fastapi.models.partner import (
    CUSTOMER_ORDER_ITEM_STATUS,
    ORDER_TRACKING_SOURCE,
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
    DashboardOrderMarginResponse,
    DashboardSupplierReliabilityResponse,
    InventoryDashboardResponse,
    SupplierPriceTrendPoint,
    SupplierPriceTrendResponse,
    SupplierPriceTrendSeries,
)
from dz_fastapi.services.inventory_dashboard import get_inventory_control_dashboard

router = APIRouter()

# Отказы, при которых позиция была нам понятна: предложение существовало,
# но мы его не отдали (цена выше допустимой, фильтры источника или прайса,
# оффер не выбран). Такой отказ — наше решение, и он остаётся в адресуемом
# спросе. Всё остальное (NO_OFFER и отказы без кода) — позиции, которых нет
# в подключённых источниках; они спрос не характеризуют.
ADDRESSABLE_REJECT_CODES = frozenset(
    {
        "PRICE_TOO_HIGH",
        "FILTERED_BY_SOURCE_RULE",
        "FILTERED_BY_PRICE_CONFIG",
        "NONPOSITIVE_OFFER",
        "OFFER_MATCH_DIAGNOSTIC",
        "PARTIAL_STOCK",
    }
)


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
    partner_limit: int = Query(default=200, ge=1, le=1000),
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

    # Адресуемый спрос: строки, которые мы могли исполнить — принятые и
    # отказанные по нашему решению. NO_OFFER (позиции, которых нет ни в
    # одном подключённом источнике) сюда не входят, иначе покрытие
    # закупкой делится на заведомо неисполнимый спрос.
    addressable_condition = CustomerOrderItem.status.in_(
        (
            CUSTOMER_ORDER_ITEM_STATUS.OWN_STOCK,
            CUSTOMER_ORDER_ITEM_STATUS.SUPPLIER,
        )
    ) | func.upper(
        func.coalesce(CustomerOrderItem.reject_reason_code, "")
    ).in_(
        sorted(ADDRESSABLE_REJECT_CODES)
    )
    customer_addressable_qty_expr = func.coalesce(
        func.sum(
            case(
                (addressable_condition, CustomerOrderItem.requested_qty),
                else_=0,
            )
        ),
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
            customer_addressable_qty_expr.label("addressable_quantity"),
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
        .where(
            supplier_day >= start_date,
            func.coalesce(SupplierOrder.source_type, "")
            != ORDER_TRACKING_SOURCE.CUSTOMER_ORDER.value,
            SupplierOrderItem.customer_order_item_id.is_(None),
        )
        .group_by(Provider.id, Provider.name)
        .order_by(func.sum(SupplierOrderItem.quantity).desc())
    )
    cross_docking_partner_stmt = (
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
        .where(
            supplier_day >= start_date,
            (
                func.coalesce(SupplierOrder.source_type, "")
                == ORDER_TRACKING_SOURCE.CUSTOMER_ORDER.value
            )
            | SupplierOrderItem.customer_order_item_id.is_not(None),
        )
        .group_by(Provider.id, Provider.name)
        .order_by(func.sum(SupplierOrderItem.quantity).desc())
    )

    def _site_order_partner_stmt(cross_docking: bool):
        # Site-заказы Dragonzap делим по source_type так же, как заказы
        # поставщикам: CUSTOMER_ORDER = под клиента (cross-docking),
        # остальное (DRAGONZAP_SEARCH/SEARCH_OFFERS) = на склад.
        source_filter = (
            Order.source_type == ORDER_TRACKING_SOURCE.CUSTOMER_ORDER.value
            if cross_docking
            else func.coalesce(Order.source_type, "")
            != ORDER_TRACKING_SOURCE.CUSTOMER_ORDER.value
        )
        return (
            select(
                Provider.id.label("partner_id"),
                Provider.name.label("partner_name"),
                func.count(func.distinct(Order.id)).label("order_count"),
                func.count(OrderItem.id).label("position_count"),
                func.coalesce(func.sum(OrderItem.quantity), 0).label(
                    "quantity"
                ),
                site_order_sum_expr.label("total_sum"),
            )
            .select_from(Order)
            .join(Provider, Provider.id == Order.provider_id)
            .join(OrderItem, OrderItem.order_id == Order.id)
            .where(site_order_day >= start_date, source_filter)
            .group_by(Provider.id, Provider.name)
        )

    site_order_partner_stmt = _site_order_partner_stmt(cross_docking=False)
    site_order_cross_docking_stmt = _site_order_partner_stmt(
        cross_docking=True
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
    site_order_cross_docking_rows = (
        await session.execute(site_order_cross_docking_stmt)
    ).all()
    cross_docking_partner_rows = (
        await session.execute(cross_docking_partner_stmt)
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
                        "addressable_quantity": 0,
                        "total_sum": 0.0,
                    },
                )
                item["order_count"] += int(row.order_count or 0)
                item["position_count"] += int(row.position_count or 0)
                item["quantity"] += int(row.quantity or 0)
                # Колонка есть только у клиентских строк — у заказов
                # поставщикам понятия адресуемого спроса нет.
                item["addressable_quantity"] += int(
                    getattr(row, "addressable_quantity", 0) or 0
                )
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
                "customer_addressable_qty": customer_values.get(
                    "addressable_quantity", 0
                ),
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
    customer_addressable_qty = sum(
        int(row["customer_addressable_qty"]) for row in daily
    )
    customer_sum = sum(float(row["customer_sum"]) for row in daily)
    supplier_order_count = sum(
        int(row["supplier_order_count"]) for row in daily
    )
    supplier_qty = sum(int(row["supplier_qty"]) for row in daily)
    supplier_sum = sum(float(row["supplier_sum"]) for row in daily)
    # Делим на адресуемый спрос: закупка не может покрыть позиции, которых
    # нет ни в одном источнике. Значение выше 100% нормально — часть
    # закупки идёт на склад, а не под конкретный клиентский заказ.
    purchase_coverage_pct = (
        round((supplier_qty / customer_addressable_qty) * 100.0, 1)
        if customer_addressable_qty > 0
        else None
    )
    return {
        "generated_at": now_moscow(),
        "days": days,
        "summary": {
            "customer_order_count": customer_order_count,
            "customer_qty": customer_qty,
            "customer_addressable_qty": customer_addressable_qty,
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
            site_order_cross_docking_rows,
            cross_docking_partner_rows,
        ),
        "suppliers_warehouse": partner_payload(
            supplier_partner_rows,
            site_order_partner_rows,
        ),
        "suppliers_cross_docking": partner_payload(
            cross_docking_partner_rows,
            site_order_cross_docking_rows,
        ),
    }


async def _load_last_purchase_cost(
    session: AsyncSession,
    autopart_ids: set[int],
) -> dict[int, float]:
    """Фактическая закупочная цена по артикулу — последняя по времени.

    Карточка (AutoPart.purchase_price) в проекте не заполняется, а
    складского цикла (StockLot) ещё нет, поэтому единственный живой
    источник себестоимости — цены наших же заказов поставщикам и заказов
    на сайте. Берём последнюю закупку по каждому артикулу.

    wholesale_price сознательно не используется: это оптовая цена
    продажи, а не закупка, и подстановка её в себестоимость молча
    занижала бы маржу почти до нуля.
    """
    result: dict[int, float] = {}
    ids = sorted(autopart_ids)
    if not ids:
        return result

    supplier_stmt = (
        select(
            SupplierOrderItem.autopart_id,
            func.coalesce(
                SupplierOrderItem.response_price,
                SupplierOrderItem.price,
            ).label("price"),
            SupplierOrder.created_at.label("created_at"),
        )
        .select_from(SupplierOrderItem)
        .join(
            SupplierOrder,
            SupplierOrder.id == SupplierOrderItem.supplier_order_id,
        )
        .where(
            SupplierOrderItem.autopart_id.in_(ids),
            func.coalesce(
                SupplierOrderItem.response_price,
                SupplierOrderItem.price,
                0,
            )
            > 0,
        )
    )
    site_stmt = (
        select(
            OrderItem.autopart_id,
            OrderItem.price.label("price"),
            Order.created_at.label("created_at"),
        )
        .select_from(OrderItem)
        .join(Order, Order.id == OrderItem.order_id)
        .where(
            OrderItem.autopart_id.in_(ids),
            func.coalesce(OrderItem.price, 0) > 0,
        )
    )

    latest: dict[int, tuple[datetime, float]] = {}
    for rows in (
        (await session.execute(supplier_stmt)).all(),
        (await session.execute(site_stmt)).all(),
    ):
        for row in rows:
            autopart_id = int(row.autopart_id)
            price = float(row.price or 0)
            if price <= 0:
                continue
            created_at = row.created_at
            if created_at is None:
                continue
            known = latest.get(autopart_id)
            if known is None or created_at > known[0]:
                latest[autopart_id] = (created_at, price)
    for autopart_id, (_, price) in latest.items():
        result[autopart_id] = price
    return result


@router.get(
    "/dashboard/order-margin",
    tags=["dashboard"],
    status_code=status.HTTP_200_OK,
    response_model=DashboardOrderMarginResponse,
    dependencies=[Depends(require_admin)],
)
async def get_order_margin(
    days: int = Query(default=30, ge=1, le=365),
    session: AsyncSession = Depends(get_session),
):
    """Операционная оценка маржи для проектов без полного складского цикла.

    Строки заказов делятся на адресуемый спрос и позиции вне ассортимента:
    по адресуемым мы понимали позицию и имели предложение, поэтому отказ по
    ним — наше решение (цена, фильтры). NO_OFFER означает, что позиции нет
    ни в одном подключённом источнике, и такой спрос не показателен для
    конверсии и маржи.
    """
    generated_at = now_moscow()
    start_date = generated_at.date() - timedelta(days=days - 1)
    start_at = datetime.combine(start_date, time.min).replace(
        tzinfo=generated_at.tzinfo
    )
    stmt = (
        select(
            CustomerOrder.received_at,
            Customer.id.label("customer_id"),
            Customer.name.label("customer_name"),
            CustomerOrderItem.status,
            CustomerOrderItem.reject_reason_code,
            CustomerOrderItem.requested_qty,
            CustomerOrderItem.requested_price,
            CustomerOrderItem.ship_qty,
            CustomerOrderItem.reject_qty,
            CustomerOrderItem.matched_price,
            CustomerOrderItem.autopart_id,
            AutoPart.purchase_price,
        )
        .select_from(CustomerOrderItem)
        .join(CustomerOrder, CustomerOrder.id == CustomerOrderItem.order_id)
        .join(Customer, Customer.id == CustomerOrder.customer_id)
        .outerjoin(AutoPart, AutoPart.id == CustomerOrderItem.autopart_id)
        .where(CustomerOrder.received_at >= start_at)
        .order_by(CustomerOrder.received_at.asc())
    )
    order_rows = (await session.execute(stmt)).all()
    last_purchase_cost = await _load_last_purchase_cost(
        session,
        {
            int(row.autopart_id)
            for row in order_rows
            if row.autopart_id is not None
        },
    )

    grouped: dict[tuple[object, int], dict] = {}
    for row in order_rows:
        requested_qty = max(int(row.requested_qty or 0), 0)
        if requested_qty <= 0:
            continue
        received_at = row.received_at or generated_at
        if received_at.tzinfo is None:
            received_at = received_at.replace(tzinfo=generated_at.tzinfo)
        period_day = received_at.astimezone(generated_at.tzinfo).date()
        key = (period_day, int(row.customer_id))
        item = grouped.setdefault(
            key,
            {
                "period_start": datetime.combine(
                    period_day, time.min
                ).replace(tzinfo=generated_at.tzinfo),
                "customer_id": int(row.customer_id),
                "customer_name": row.customer_name or f"#{row.customer_id}",
                "ordered_quantity": 0,
                "order_total": 0.0,
                "unpriced_order_quantity": 0,
                "addressable_quantity": 0,
                "addressable_total": 0.0,
                "declined_quantity": 0,
                "declined_total": 0.0,
                "no_offer_quantity": 0,
                "no_offer_total": 0.0,
                "quantity": 0,
                "revenue_total": 0.0,
                "costed_revenue_total": 0.0,
                "cost_total": 0.0,
                "costed_quantity": 0,
                "uncosted_quantity": 0,
            },
        )
        order_price = (
            float(row.requested_price)
            if row.requested_price is not None
            else float(row.matched_price or 0)
        )
        line_total = requested_qty * order_price
        item["ordered_quantity"] += requested_qty
        item["order_total"] += line_total
        if order_price <= 0:
            item["unpriced_order_quantity"] += requested_qty

        is_accepted = row.status in (
            CUSTOMER_ORDER_ITEM_STATUS.OWN_STOCK,
            CUSTOMER_ORDER_ITEM_STATUS.SUPPLIER,
        )
        reject_code = (row.reject_reason_code or "").strip().upper()
        if is_accepted or reject_code in ADDRESSABLE_REJECT_CODES:
            item["addressable_quantity"] += requested_qty
            item["addressable_total"] += line_total
            if not is_accepted:
                item["declined_quantity"] += requested_qty
                item["declined_total"] += line_total
        else:
            item["no_offer_quantity"] += requested_qty
            item["no_offer_total"] += line_total

        if not is_accepted:
            continue
        reject_qty = max(int(row.reject_qty or 0), 0)
        quantity = (
            max(int(row.ship_qty or 0), 0)
            if row.ship_qty is not None
            else max(requested_qty - reject_qty, 0)
        )
        if quantity <= 0:
            continue
        # Выручка — только по цене клиента. Подставлять сюда matched_price
        # нельзя: это цена поставщика, то есть наша себестоимость.
        sale_price = float(row.requested_price or 0)
        supplier_cost = float(row.matched_price or 0)
        catalog_cost = float(row.purchase_price or 0)
        history_cost = float(
            last_purchase_cost.get(int(row.autopart_id), 0.0)
            if row.autopart_id is not None
            else 0.0
        )
        # Приоритет: цена поставщика по самой строке (точнее всего) →
        # учётная цена карточки как ручное переопределение → последняя
        # фактическая закупка этого артикула.
        if (
            row.status == CUSTOMER_ORDER_ITEM_STATUS.SUPPLIER
            and supplier_cost > 0
        ):
            unit_cost = supplier_cost
        elif catalog_cost > 0:
            unit_cost = catalog_cost
        else:
            unit_cost = history_cost

        item["quantity"] += quantity
        item["revenue_total"] += quantity * sale_price
        # Прибыль считаем, только когда известны обе стороны. Строка без
        # цены клиента — такой же пробел, как строка без себестоимости.
        if unit_cost > 0 and sale_price > 0:
            item["cost_total"] += quantity * unit_cost
            item["costed_revenue_total"] += quantity * sale_price
            item["costed_quantity"] += quantity
        else:
            item["uncosted_quantity"] += quantity

    rows = []
    for item in grouped.values():
        item["order_total"] = round(item["order_total"], 2)
        item["addressable_total"] = round(item["addressable_total"], 2)
        item["declined_total"] = round(item["declined_total"], 2)
        item["no_offer_total"] = round(item["no_offer_total"], 2)
        item["revenue_total"] = round(item["revenue_total"], 2)
        item["costed_revenue_total"] = round(item["costed_revenue_total"], 2)
        item["cost_total"] = round(item["cost_total"], 2)
        # Прибыль и маржа считаются по покрытой части: выручка берётся
        # только по тем же строкам, по которым известна себестоимость.
        # Раньше одна непокрытая строка обнуляла показатель за весь день.
        if item["costed_quantity"] > 0:
            gross_profit = round(
                item["costed_revenue_total"] - item["cost_total"], 2
            )
            item["gross_profit"] = gross_profit
            item["margin_percent"] = (
                round(
                    gross_profit / item["costed_revenue_total"] * 100, 2
                )
                if item["costed_revenue_total"] > 0
                else None
            )
        else:
            item["gross_profit"] = None
            item["margin_percent"] = None
        rows.append(item)
    rows.sort(key=lambda item: (item["period_start"], item["customer_name"]))
    return {
        "generated_at": generated_at,
        "source": "customer_orders_estimate",
        "note": (
            "Сумма заказов рассчитана по всем входящим строкам. "
            "Адресуемый спрос исключает позиции без предложения в "
            "подключённых источниках. Прибыль и маржа считаются по "
            "покрытой части: строки, где известны и цена клиента, и "
            "себестоимость (цена поставщика по строке, учётная цена "
            "карточки или последняя фактическая закупка артикула)."
        ),
        "rows": rows,
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
                # Медиана, а не среднее: один застрявший заказ не должен
                # портить оценку фактического срока поставки.
                "avg_lead_days": (
                    round(float(median(lead_days)), 1) if lead_days else None
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
    # Окно выравниваем по календарным суткам, как в остальных разделах
    # сводки: скользящее «now - days» давало несопоставимые периоды.
    date_from = datetime.combine(
        generated_at.date() - timedelta(days=days - 1),
        time.min,
    ).replace(tzinfo=generated_at.tzinfo)
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


async def _load_pair_stats_batch(
    session: AsyncSession,
    pairs: set[tuple[int, int]],
) -> dict[tuple[int, int], tuple[int, Optional[float], Optional[float]]]:
    """Статистика по всем парам прайсов за один запрос.

    Для каждой пары (предыдущий, текущий) возвращает: сколько позиций
    есть в обоих прайсах, медиану процентного изменения цены и долю
    позиций, у которых цена изменилась.

    Раньше на каждую пару шёл отдельный запрос, который тянул все общие
    позиции в Python (~12 тыс. строк на прайс). Теперь агрегация целиком
    на стороне Postgres: percentile_cont даёт ту же медиану с
    интерполяцией, что и statistics.median.

    Количества новых/ушедших позиций считаются вызывающим кодом из уже
    загруженных total-счётчиков, чтобы не делать лишние COUNT-запросы.
    """
    result: dict[
        tuple[int, int], tuple[int, Optional[float], Optional[float]]
    ] = {}
    if not pairs:
        return result

    # Один запрос на сотни пар создавал большой parallel hash в /dev/shm.
    # Небольшие последовательные пачки ограничивают пиковую память.
    await session.execute(text("SET LOCAL max_parallel_workers_per_gather = 0"))
    ordered_pairs = sorted(pairs)
    batch_size = 8
    for offset in range(0, len(ordered_pairs), batch_size):
        pair_values = values(
            column("prev_id", Integer),
            column("curr_id", Integer),
            name="pairs",
        ).data(ordered_pairs[offset: offset + batch_size])
        curr_assoc = aliased(PriceListAutoPartAssociation)
        prev_assoc = aliased(PriceListAutoPartAssociation)
        ratio = (
            curr_assoc.price / func.nullif(prev_assoc.price, 0) - 1
        ) * 100

        stmt = (
            select(
                pair_values.c.prev_id,
                pair_values.c.curr_id,
                func.count().label("overlap_count"),
                func.count(ratio).label("ratio_count"),
                func.percentile_cont(0.5)
                .within_group(ratio)
                .label("median_pct"),
                func.count()
                .filter(func.abs(ratio) > 0.01)
                .label("changed_count"),
            )
            .select_from(pair_values)
            .join(
                curr_assoc,
                curr_assoc.pricelist_id == pair_values.c.curr_id,
            )
            .join(
                prev_assoc,
                and_(
                    prev_assoc.pricelist_id == pair_values.c.prev_id,
                    prev_assoc.autopart_id == curr_assoc.autopart_id,
                ),
            )
            .group_by(pair_values.c.prev_id, pair_values.c.curr_id)
        )
        for row in (await session.execute(stmt)).all():
            ratio_count = int(row.ratio_count or 0)
            changed_share_pct = (
                round((int(row.changed_count or 0) / ratio_count) * 100.0, 1)
                if ratio_count
                else None
            )
            result[(int(row.prev_id), int(row.curr_id))] = (
                int(row.overlap_count or 0),
                _to_float(row.median_pct) if ratio_count else None,
                changed_share_pct,
            )
    # Пары без общих позиций Postgres не вернёт — заполняем нулями.
    for pair in pairs:
        result.setdefault(pair, (0, None, None))
    return result


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
            func.count().label("total_sku_count"),
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
            "total_sku_count": int(row.total_sku_count or 0),
            "sku_count": int(row.sku_count or 0),
            "stock_total_qty": int(row.stock_total_qty or 0),
            "avg_price": _to_float(row.avg_price),
        }
        for row in metric_rows
    }

    # Реальное время загрузки прайса берём из истории цен (created_at
    # пишется в момент загрузки) — у самого PriceList времени нет.
    uploaded_stmt = (
        select(
            AutoPartPriceHistory.pricelist_id,
            func.max(AutoPartPriceHistory.created_at).label("uploaded_at"),
        )
        .where(AutoPartPriceHistory.pricelist_id.in_(pricelist_ids))
        .group_by(AutoPartPriceHistory.pricelist_id)
    )
    uploaded_map = {
        int(row.pricelist_id): row.uploaded_at
        for row in (await session.execute(uploaded_stmt)).all()
    }

    # Базовый прайс для «цены к началу периода» — самая ранняя загрузка
    # внутри окна days, а не самая ранняя из показанных points_limit точек.
    # Иначе при days=30 и points_limit=8 подпись врала бы: сравнение шло
    # с 8-й с конца загрузкой, а не с началом периода.
    earliest_ranked_stmt = select(
        PriceList.id.label("pricelist_id"),
        PriceList.provider_config_id.label("provider_config_id"),
        func.row_number()
        .over(
            partition_by=PriceList.provider_config_id,
            order_by=(
                PriceList.date.asc().nullslast(),
                PriceList.id.asc(),
            ),
        )
        .label("rn"),
    ).where(
        PriceList.provider_config_id.is_not(None),
        PriceList.date >= start_date,
    )
    if provider_config_ids:
        earliest_ranked_stmt = earliest_ranked_stmt.where(
            PriceList.provider_config_id.in_(provider_config_ids)
        )
    earliest_subquery = earliest_ranked_stmt.subquery()
    earliest_map = {
        int(row.provider_config_id): int(row.pricelist_id)
        for row in (
            await session.execute(
                select(
                    earliest_subquery.c.pricelist_id,
                    earliest_subquery.c.provider_config_id,
                ).where(earliest_subquery.c.rn == 1)
            )
        ).all()
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

    # Все нужные пары собираем заранее и считаем одним запросом:
    # соседние точки каждой серии + база периода → последняя точка.
    ordered_by_provider: dict[int, list[dict]] = {}
    base_by_provider: dict[int, Optional[int]] = {}
    needed_pairs: set[tuple[int, int]] = set()
    for provider_config_id, raw_points in points_by_provider.items():
        ordered_points = sorted(
            raw_points,
            key=lambda item: (item["date"], item["pricelist_id"]),
        )
        ordered_by_provider[provider_config_id] = ordered_points
        base_pricelist_id = earliest_map.get(provider_config_id) or (
            int(ordered_points[0]["pricelist_id"]) if ordered_points else None
        )
        base_by_provider[provider_config_id] = base_pricelist_id
        for prev_item, curr_item in zip(ordered_points, ordered_points[1:]):
            needed_pairs.add(
                (
                    int(prev_item["pricelist_id"]),
                    int(curr_item["pricelist_id"]),
                )
            )
        if ordered_points and base_pricelist_id is not None:
            latest_id = int(ordered_points[-1]["pricelist_id"])
            if base_pricelist_id != latest_id:
                needed_pairs.add((base_pricelist_id, latest_id))
    pair_stats = await _load_pair_stats_batch(session, needed_pairs)

    series: list[SupplierPriceTrendSeries] = []
    for provider_config_id, ordered_points in ordered_by_provider.items():
        base_pricelist_id = base_by_provider.get(provider_config_id)
        points: list[SupplierPriceTrendPoint] = []
        prev_item = None
        for item in ordered_points:
            pricelist_id = int(item["pricelist_id"])
            metric = metric_map.get(pricelist_id, {})
            step_index_pct = None
            coverage_pct = None
            overlap_count = None
            new_positions = None
            removed_positions = None
            changed_share_pct = None
            if prev_item is not None:
                prev_pricelist_id = int(prev_item["pricelist_id"])
                (
                    overlap_count,
                    step_index_pct,
                    changed_share_pct,
                ) = pair_stats.get(
                    (prev_pricelist_id, pricelist_id), (0, None, None)
                )
                prev_total = int(
                    metric_map.get(prev_pricelist_id, {}).get(
                        "total_sku_count", 0
                    )
                )
                curr_total = int(metric.get("total_sku_count", 0))
                # Новые/ушедшие позиции — из уже загруженных total-счётчиков.
                new_positions = max(curr_total - overlap_count, 0)
                removed_positions = max(prev_total - overlap_count, 0)
                if prev_total > 0:
                    coverage_pct = round(
                        (overlap_count / prev_total) * 100, 2
                    )
            points.append(
                SupplierPriceTrendPoint(
                    pricelist_id=pricelist_id,
                    date=item["date"],
                    uploaded_at=uploaded_map.get(pricelist_id),
                    total_sku_count=int(metric.get("total_sku_count", 0)),
                    sku_count=int(metric.get("sku_count", 0)),
                    stock_total_qty=int(metric.get("stock_total_qty", 0)),
                    avg_price=metric.get("avg_price"),
                    step_index_pct=(
                        round(step_index_pct, 2)
                        if step_index_pct is not None
                        else None
                    ),
                    coverage_pct=coverage_pct,
                    overlap_count=overlap_count,
                    new_positions=new_positions,
                    removed_positions=removed_positions,
                    changed_share_pct=changed_share_pct,
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

        latest_point = points[-1] if points else None
        # Нетто-изменение цены: первая загрузка периода → последняя.
        net_price_change_pct = None
        if (
            latest_point is not None
            and base_pricelist_id is not None
            and base_pricelist_id != latest_point.pricelist_id
        ):
            net_price_change_pct = pair_stats.get(
                (base_pricelist_id, latest_point.pricelist_id),
                (0, None, None),
            )[1]
        latest_uploaded_at = (
            latest_point.uploaded_at if latest_point else None
        )
        provider_info = provider_map.get(provider_config_id, {})
        series.append(
            SupplierPriceTrendSeries(
                provider_config_id=provider_config_id,
                provider_id=provider_info.get("provider_id"),
                provider_name=provider_info.get("provider_name"),
                provider_config_name=provider_info.get("provider_config_name"),
                latest_uploaded_at=latest_uploaded_at,
                net_price_change_pct=net_price_change_pct,
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
