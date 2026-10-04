"""Просмотр и анализ загруженного прайса поставщика.

Строки выбранного прайса сравниваются с предыдущим прайсом той же
конфигурации (изменение цены и остатка), дополняются сводкой оборота
(наши продажи, оценка рекомендации, минимальная цена рынка) и
фильтруются/сортируются на стороне базы.
"""

from datetime import date
from typing import Optional

from pydantic import BaseModel, Field
from sqlalchemy import Float, and_, case, cast, func, literal, not_, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased

from dz_fastapi.models.autopart import AutoPart, AutoPartTurnoverSummary
from dz_fastapi.models.brand import Brand
from dz_fastapi.models.partner import PriceList, PriceListAutoPartAssociation

SORTABLE = {
    "oem": "oem_number",
    "brand": "brand_name",
    "price": "price",
    "quantity": "quantity",
    "price_change_pct": "price_change_pct",
    "quantity_change": "quantity_change",
    "sold_30d": "sold_qty_30d",
    "sold_90d": "sold_qty_90d",
    "score": "recommendation_score",
    "vs_market_pct": "vs_market_pct",
    "stock_value": "stock_value",
}


class ExplorerFilters(BaseModel):
    brand_ids: list[int] = Field(default_factory=list)
    q: Optional[str] = None
    price_min: Optional[float] = None
    price_max: Optional[float] = None
    qty_min: Optional[int] = None
    qty_max: Optional[int] = None
    in_stock: Optional[bool] = None
    # up / down / any / new / unchanged
    price_change: Optional[str] = None
    price_change_min_pct: Optional[float] = None
    # up / down / appeared / disappeared
    quantity_change: Optional[str] = None
    # Интерес: наши продажи и оценка рекомендации
    sold_30d_min: Optional[int] = None
    score_min: Optional[float] = None
    top_only: bool = False
    market_only: bool = False
    # with / without — были ли продажи за 90 дней
    demand: Optional[str] = None
    # Цена относительно минимальной у других поставщиков, %
    vs_market_min_pct: Optional[float] = None
    vs_market_max_pct: Optional[float] = None


async def resolve_pricelists(
    session: AsyncSession,
    provider_id: int,
    *,
    config_id: int | None,
    pricelist_id: int | None,
    compare_to: int | None,
) -> tuple[PriceList | None, PriceList | None]:
    """Выбранный прайс и прайс для сравнения (предыдущий по дате)."""
    base = select(PriceList).where(PriceList.provider_id == provider_id)
    if pricelist_id:
        current = (
            await session.execute(base.where(PriceList.id == pricelist_id))
        ).scalar_one_or_none()
    else:
        stmt = base
        if config_id:
            stmt = stmt.where(PriceList.provider_config_id == config_id)
        current = (
            await session.execute(
                stmt.order_by(PriceList.date.desc().nullslast(), PriceList.id.desc()).limit(1)
            )
        ).scalar_one_or_none()
    if current is None:
        return None, None
    if compare_to:
        previous = (
            await session.execute(
                select(PriceList).where(
                    PriceList.id == compare_to, PriceList.provider_id == provider_id
                )
            )
        ).scalar_one_or_none()
        return current, previous
    prev_stmt = select(PriceList).where(
        PriceList.provider_id == provider_id,
        PriceList.provider_config_id == current.provider_config_id,
        PriceList.id != current.id,
    )
    if current.date is not None:
        prev_stmt = prev_stmt.where(
            or_(
                PriceList.date < current.date,
                and_(PriceList.date == current.date, PriceList.id < current.id),
            )
        )
    else:
        prev_stmt = prev_stmt.where(PriceList.id < current.id)
    previous = (
        await session.execute(
            prev_stmt.order_by(PriceList.date.desc().nullslast(), PriceList.id.desc()).limit(1)
        )
    ).scalar_one_or_none()
    return current, previous


def _base_query(current_id: int, previous_id: int | None):
    cur = aliased(PriceListAutoPartAssociation)
    ts = AutoPartTurnoverSummary
    if previous_id:
        prev = aliased(PriceListAutoPartAssociation)
        prev_price = prev.price
        prev_qty = prev.quantity
    else:
        prev = None
        prev_price = literal(None)
        prev_qty = literal(None)

    price_change_pct = (
        case(
            (
                and_(prev_price.is_not(None), prev_price > 0),
                (cur.price - prev_price) / prev_price * 100.0,
            ),
            else_=None,
        )
        if previous_id
        else literal(None)
    )
    vs_market_pct = case(
        (
            and_(ts.min_purchase_price.is_not(None), ts.min_purchase_price > 0),
            (cur.price - ts.min_purchase_price) / ts.min_purchase_price * 100.0,
        ),
        else_=None,
    )
    stmt = (
        select(
            AutoPart.id.label("autopart_id"),
            AutoPart.oem_number.label("oem_number"),
            AutoPart.name.label("name"),
            Brand.id.label("brand_id"),
            Brand.name.label("brand_name"),
            cur.price.label("price"),
            cur.quantity.label("quantity"),
            cur.multiplicity.label("multiplicity"),
            (cur.price * cur.quantity).label("stock_value"),
            cast(prev_price, Float).label("prev_price"),
            prev_qty.label("prev_quantity"),
            cast(price_change_pct, Float).label("price_change_pct"),
            (cur.quantity - func.coalesce(prev_qty, 0)).label("quantity_change")
            if previous_id
            else literal(None).label("quantity_change"),
            (prev.autopart_id.is_(None) if previous_id else literal(False)).label("is_new"),
            func.coalesce(ts.sold_qty_30d, 0).label("sold_qty_30d"),
            func.coalesce(ts.sold_qty_90d, 0).label("sold_qty_90d"),
            func.coalesce(ts.recommendation_score, 0.0).label("recommendation_score"),
            func.coalesce(ts.is_top, False).label("is_top"),
            func.coalesce(ts.is_market_opportunity, False).label("is_market_opportunity"),
            cast(ts.min_purchase_price, Float).label("market_min_price"),
            ts.min_price_provider_name.label("market_min_provider"),
            ts.supplier_count.label("supplier_count"),
            cast(vs_market_pct, Float).label("vs_market_pct"),
        )
        .select_from(cur)
        .join(AutoPart, AutoPart.id == cur.autopart_id)
        .join(Brand, Brand.id == AutoPart.brand_id)
        .outerjoin(ts, ts.autopart_id == AutoPart.id)
        .where(cur.pricelist_id == current_id)
    )
    if previous_id:
        stmt = stmt.outerjoin(
            prev,
            and_(prev.pricelist_id == previous_id, prev.autopart_id == cur.autopart_id),
        )
    return stmt.subquery("rows")


def _apply_filters(stmt, rows, f: ExplorerFilters, has_previous: bool):
    c = rows.c
    if f.brand_ids:
        stmt = stmt.where(c.brand_id.in_(f.brand_ids))
    needle = (f.q or "").strip()
    if needle:
        pattern = f"%{needle}%"
        stmt = stmt.where(or_(c.oem_number.ilike(pattern), c.name.ilike(pattern)))
    if f.price_min is not None:
        stmt = stmt.where(c.price >= f.price_min)
    if f.price_max is not None:
        stmt = stmt.where(c.price <= f.price_max)
    if f.qty_min is not None:
        stmt = stmt.where(c.quantity >= f.qty_min)
    if f.qty_max is not None:
        stmt = stmt.where(c.quantity <= f.qty_max)
    if f.in_stock is True:
        stmt = stmt.where(c.quantity > 0)
    elif f.in_stock is False:
        stmt = stmt.where(c.quantity <= 0)

    if has_previous and f.price_change:
        threshold = abs(f.price_change_min_pct) if f.price_change_min_pct is not None else None
        if f.price_change == "up":
            stmt = stmt.where(c.price_change_pct > (threshold or 0))
        elif f.price_change == "down":
            stmt = stmt.where(c.price_change_pct < -(threshold or 0))
        elif f.price_change == "any":
            stmt = stmt.where(func.abs(c.price_change_pct) > (threshold or 0))
        elif f.price_change == "new":
            stmt = stmt.where(c.is_new.is_(True))
        elif f.price_change == "unchanged":
            stmt = stmt.where(c.is_new.is_(False), c.price_change_pct == 0)
    if has_previous and f.quantity_change:
        if f.quantity_change == "up":
            stmt = stmt.where(c.quantity_change > 0)
        elif f.quantity_change == "down":
            stmt = stmt.where(c.quantity_change < 0)
        elif f.quantity_change == "appeared":
            stmt = stmt.where(c.quantity > 0, func.coalesce(c.prev_quantity, 0) <= 0)
        elif f.quantity_change == "disappeared":
            stmt = stmt.where(c.quantity <= 0, func.coalesce(c.prev_quantity, 0) > 0)

    if f.sold_30d_min is not None:
        stmt = stmt.where(c.sold_qty_30d >= f.sold_30d_min)
    if f.score_min is not None:
        stmt = stmt.where(c.recommendation_score >= f.score_min)
    if f.top_only:
        stmt = stmt.where(c.is_top.is_(True))
    if f.market_only:
        stmt = stmt.where(c.is_market_opportunity.is_(True))
    if f.demand == "with":
        stmt = stmt.where(c.sold_qty_90d > 0)
    elif f.demand == "without":
        stmt = stmt.where(c.sold_qty_90d == 0)
    if f.vs_market_min_pct is not None:
        stmt = stmt.where(c.vs_market_pct >= f.vs_market_min_pct)
    if f.vs_market_max_pct is not None:
        stmt = stmt.where(c.vs_market_pct <= f.vs_market_max_pct)
    return stmt


async def explore_pricelist(
    session: AsyncSession,
    *,
    current: PriceList,
    previous: PriceList | None,
    filters: ExplorerFilters,
    sort_by: str,
    sort_dir: str,
    limit: int,
    offset: int,
) -> dict:
    rows = _base_query(current.id, previous.id if previous else None)
    has_previous = previous is not None
    c = rows.c

    filtered = _apply_filters(select(rows), rows, filters, has_previous)
    count_query = select(func.count()).select_from(filtered.subquery())
    total = (await session.execute(count_query)).scalar_one()

    column = getattr(c, SORTABLE.get(sort_by, "stock_value"))
    order = column.desc().nullslast() if sort_dir != "asc" else column.asc().nullslast()
    page = (
        await session.execute(filtered.order_by(order, c.autopart_id).limit(limit).offset(offset))
    ).mappings().all()

    sub = filtered.subquery()
    summary_row = (
        await session.execute(
            select(
                func.count().label("positions"),
                func.count().filter(sub.c.quantity > 0).label("in_stock"),
                func.coalesce(func.sum(sub.c.stock_value), 0).label("stock_value"),
                func.avg(sub.c.price).label("avg_price"),
                func.count().filter(sub.c.price_change_pct > 0).label("price_up"),
                func.count().filter(sub.c.price_change_pct < 0).label("price_down"),
                func.count().filter(sub.c.is_new.is_(True)).label("new_positions"),
                func.count().filter(sub.c.is_top.is_(True)).label("top_positions"),
                func.count().filter(sub.c.sold_qty_90d > 0).label("with_demand"),
            )
        )
    ).mappings().one()

    removed = 0
    if previous is not None:
        prev_assoc = PriceListAutoPartAssociation
        cur_assoc = aliased(PriceListAutoPartAssociation)
        removed = (
            await session.execute(
                select(func.count())
                .select_from(prev_assoc)
                .where(
                    prev_assoc.pricelist_id == previous.id,
                    not_(
                        select(literal(1))
                        .where(
                            cur_assoc.pricelist_id == current.id,
                            cur_assoc.autopart_id == prev_assoc.autopart_id,
                        )
                        .exists()
                    ),
                )
            )
        ).scalar_one()

    return {
        "total": total,
        "rows": [dict(r) for r in page],
        "summary": {**dict(summary_row), "removed_positions": removed},
    }


async def pricelist_brand_facets(
    session: AsyncSession, pricelist_id: int, limit: int = 300
) -> list[dict]:
    assoc = PriceListAutoPartAssociation
    stmt = (
        select(Brand.id, Brand.name, func.count().label("positions"))
        .select_from(assoc)
        .join(AutoPart, AutoPart.id == assoc.autopart_id)
        .join(Brand, Brand.id == AutoPart.brand_id)
        .where(assoc.pricelist_id == pricelist_id)
        .group_by(Brand.id, Brand.name)
        .order_by(func.count().desc(), Brand.name)
        .limit(limit)
    )
    return [
        {"id": row.id, "name": row.name, "positions": row.positions}
        for row in (await session.execute(stmt)).all()
    ]


async def list_provider_pricelists(
    session: AsyncSession, provider_id: int, per_config: int = 30
) -> list[dict]:
    stmt = (
        select(PriceList.id, PriceList.date, PriceList.provider_config_id, PriceList.is_active)
        .where(PriceList.provider_id == provider_id)
        .order_by(PriceList.date.desc().nullslast(), PriceList.id.desc())
        .limit(per_config * 20)
    )
    counts: dict[int | None, int] = {}
    result = []
    for row in (await session.execute(stmt)).all():
        n = counts.get(row.provider_config_id, 0)
        if n >= per_config:
            continue
        counts[row.provider_config_id] = n + 1
        result.append(
            {
                "id": row.id,
                "date": row.date.isoformat() if isinstance(row.date, date) else None,
                "config_id": row.provider_config_id,
                "is_active": bool(row.is_active),
            }
        )
    return result
