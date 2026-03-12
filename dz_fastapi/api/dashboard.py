from collections import defaultdict
from datetime import timedelta
from statistics import median
from typing import Optional

from fastapi import APIRouter, Depends, Query, status
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased

from dz_fastapi.api.deps import require_admin
from dz_fastapi.core.db import get_session
from dz_fastapi.core.time import now_moscow
from dz_fastapi.models.partner import (PriceList, PriceListAutoPartAssociation,
                                       Provider, ProviderPriceListConfig)
from dz_fastapi.schemas.dashboard import (SupplierPriceTrendPoint,
                                          SupplierPriceTrendResponse,
                                          SupplierPriceTrendSeries)

router = APIRouter()


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
            item for item in values[start: idx + 1]
            if item is not None
        ]
        if not segment:
            result.append(None)
            continue
        result.append(float(median(segment)))
    return result


@router.get(
    '/dashboard/supplier-price-trends',
    tags=['dashboard'],
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
        PriceList.id.label('pricelist_id'),
        PriceList.provider_config_id.label('provider_config_id'),
        PriceList.date.label('price_date'),
        func.row_number().over(
            partition_by=PriceList.provider_config_id,
            order_by=(
                PriceList.date.desc().nullslast(),
                PriceList.id.desc(),
            ),
        ).label('rn'),
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
                'pricelist_id': pricelist_id,
                'date': row.price_date,
            }
        )
        pricelist_ids.append(pricelist_id)

    metric_stmt = (
        select(
            PriceListAutoPartAssociation.pricelist_id,
            func.count().filter(
                PriceListAutoPartAssociation.quantity > 0
            ).label('sku_count'),
            func.sum(PriceListAutoPartAssociation.quantity).filter(
                PriceListAutoPartAssociation.quantity > 0
            ).label('stock_total_qty'),
            func.avg(PriceListAutoPartAssociation.price).filter(
                PriceListAutoPartAssociation.quantity > 0
            ).label('avg_price'),
        )
        .where(PriceListAutoPartAssociation.pricelist_id.in_(pricelist_ids))
        .group_by(PriceListAutoPartAssociation.pricelist_id)
    )
    metric_rows = (await session.execute(metric_stmt)).all()
    metric_map = {
        int(row.pricelist_id): {
            'sku_count': int(row.sku_count or 0),
            'stock_total_qty': int(row.stock_total_qty or 0),
            'avg_price': _to_float(row.avg_price),
        }
        for row in metric_rows
    }

    provider_ids = list(points_by_provider.keys())
    provider_stmt = (
        select(
            ProviderPriceListConfig.id,
            ProviderPriceListConfig.name_price,
            Provider.id.label('provider_id'),
            Provider.name.label('provider_name'),
        )
        .join(Provider, Provider.id == ProviderPriceListConfig.provider_id)
        .where(ProviderPriceListConfig.id.in_(provider_ids))
    )
    provider_rows = (await session.execute(provider_stmt)).all()
    provider_map = {
        int(row.id): {
            'provider_id': int(row.provider_id),
            'provider_name': row.provider_name,
            'provider_config_name': row.name_price,
        }
        for row in provider_rows
    }

    pair_cache: dict[tuple[int, int], tuple[int, Optional[float]]] = {}
    series: list[SupplierPriceTrendSeries] = []
    for provider_config_id, raw_points in points_by_provider.items():
        ordered_points = sorted(
            raw_points,
            key=lambda item: (item['date'], item['pricelist_id']),
        )
        points: list[SupplierPriceTrendPoint] = []
        prev_item = None
        for item in ordered_points:
            pricelist_id = int(item['pricelist_id'])
            metric = metric_map.get(pricelist_id, {})
            sku_count = int(metric.get('sku_count', 0))
            stock_total_qty = int(metric.get('stock_total_qty', 0))
            avg_price = metric.get('avg_price')
            step_index_pct = None
            coverage_pct = None
            overlap_count = None
            if prev_item is not None:
                prev_pricelist_id = int(prev_item['pricelist_id'])
                cache_key = (prev_pricelist_id, pricelist_id)
                if cache_key not in pair_cache:
                    pair_cache[cache_key] = await _load_pair_stats(
                        session=session,
                        prev_pricelist_id=prev_pricelist_id,
                        curr_pricelist_id=pricelist_id,
                    )
                overlap_count, step_index_pct = pair_cache[cache_key]
                prev_metric = metric_map.get(prev_pricelist_id, {})
                prev_sku_count = int(prev_metric.get('sku_count', 0))
                if prev_sku_count > 0 and overlap_count is not None:
                    coverage_pct = round(
                        (overlap_count / prev_sku_count) * 100,
                        2,
                    )
            points.append(
                SupplierPriceTrendPoint(
                    pricelist_id=pricelist_id,
                    date=item['date'],
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
                round(smooth_value, 2)
                if smooth_value is not None
                else None
            )

        provider_info = provider_map.get(provider_config_id, {})
        series.append(
            SupplierPriceTrendSeries(
                provider_config_id=provider_config_id,
                provider_id=provider_info.get('provider_id'),
                provider_name=provider_info.get('provider_name'),
                provider_config_name=provider_info.get(
                    'provider_config_name'
                ),
                points=points,
            )
        )

    series.sort(
        key=lambda item: (
            item.provider_name or '',
            item.provider_config_name or '',
        )
    )
    return SupplierPriceTrendResponse(
        generated_at=now_moscow(),
        days=days,
        points_limit=points_limit,
        smooth_window=smooth_window,
        series=series,
    )
