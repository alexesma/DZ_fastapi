import logging
import statistics
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.api.validators import normalize_brand_name
from dz_fastapi.models.autopart import AutoPart, preprocess_oem_number
from dz_fastapi.models.brand import Brand
from dz_fastapi.models.notification import AppNotificationLevel
from dz_fastapi.models.partner import (
    PriceList,
    PriceListAutoPartAssociation,
    Provider,
    ProviderPriceListConfig,
)
from dz_fastapi.services.notifications import create_admin_notifications
from dz_fastapi.services.utils import normalize_mixed_cyrillic

logger = logging.getLogger("dz_fastapi")

ROW_CHANGE_LIMIT = 0.25
MIN_OVERLAP_RATIO = 0.50
MEDIAN_PRICE_CHANGE_LIMIT = 0.10
ITEM_PRICE_CHANGE_LIMIT = 0.10
CHANGED_ITEMS_SHARE_LIMIT = 0.20

PRICELIST_ALERT_TITLE_PREFIX = "Прайс заблокирован:"


@dataclass(frozen=True)
class PricelistAnomalyResult:
    blocked: bool
    reasons: list[str]
    metrics: dict[str, Any]


def _normalise_key(brand: object, oem: object) -> tuple[str, str] | None:
    brand_value = normalize_brand_name(
        normalize_mixed_cyrillic(str(brand or ""))
    )
    oem_value = preprocess_oem_number(str(oem or ""))
    if not brand_value or not oem_value:
        return None
    return brand_value, oem_value


def _money_float(value: object) -> float | None:
    try:
        result = float(Decimal(str(value)))
    except (ArithmeticError, TypeError, ValueError):
        return None
    if result <= 0:
        return None
    return result


def build_candidate_price_map(items: list[dict]) -> dict[tuple[str, str], float]:
    result: dict[tuple[str, str], float] = {}
    for item in items:
        key = _normalise_key(item.get("brand"), item.get("oem_number"))
        price = _money_float(item.get("price"))
        if key is None or price is None:
            continue
        current = result.get(key)
        if current is None or price < current:
            result[key] = price
    return result


def calculate_pricelist_anomaly(
    previous_prices: dict[tuple[str, str], float],
    candidate_prices: dict[tuple[str, str], float],
) -> PricelistAnomalyResult:
    previous_count = len(previous_prices)
    candidate_count = len(candidate_prices)
    if previous_count == 0:
        return PricelistAnomalyResult(
            blocked=False,
            reasons=[],
            metrics={
                "previous_positions": previous_count,
                "candidate_positions": candidate_count,
            },
        )
    if candidate_count == 0:
        return PricelistAnomalyResult(
            blocked=True,
            reasons=["После очистки в прайсе не осталось допустимых позиций."],
            metrics={
                "previous_positions": previous_count,
                "candidate_positions": 0,
                "positions_change_percent": -100.0,
                "overlap_percent": 0.0,
            },
        )

    row_change = (candidate_count - previous_count) / previous_count
    common_keys = previous_prices.keys() & candidate_prices.keys()
    overlap_ratio = len(common_keys) / previous_count

    previous_common_prices = [previous_prices[key] for key in common_keys]
    candidate_common_prices = [candidate_prices[key] for key in common_keys]
    previous_median = (
        statistics.median(previous_common_prices)
        if previous_common_prices
        else 0.0
    )
    candidate_median = (
        statistics.median(candidate_common_prices)
        if candidate_common_prices
        else 0.0
    )
    median_change = (
        (candidate_median - previous_median) / previous_median
        if previous_median
        else 0.0
    )

    changed_count = 0
    increased_count = 0
    paired_changes: list[float] = []
    for key in common_keys:
        previous_price = previous_prices[key]
        if previous_price <= 0:
            continue
        change = (candidate_prices[key] - previous_price) / previous_price
        paired_changes.append(change)
        if abs(change) > ITEM_PRICE_CHANGE_LIMIT:
            changed_count += 1
        if change > ITEM_PRICE_CHANGE_LIMIT:
            increased_count += 1

    compared_count = len(paired_changes)
    changed_share = changed_count / compared_count if compared_count else 0.0
    increased_share = (
        increased_count / compared_count if compared_count else 0.0
    )
    paired_median_change = (
        statistics.median(paired_changes) if paired_changes else 0.0
    )

    reasons: list[str] = []
    if abs(row_change) > ROW_CHANGE_LIMIT:
        reasons.append(
            "Количество позиций изменилось на "
            f"{row_change * 100:+.1f}% (допустимо ±25%)."
        )
    if overlap_ratio < MIN_OVERLAP_RATIO:
        reasons.append(
            "Пересечение номенклатуры с предыдущим прайсом "
            f"{overlap_ratio * 100:.1f}% (минимум 50%)."
        )
    if abs(median_change) > MEDIAN_PRICE_CHANGE_LIMIT:
        reasons.append(
            "Медианная цена всего прайса изменилась на "
            f"{median_change * 100:+.1f}% (допустимо ±10%)."
        )
    if paired_median_change > MEDIAN_PRICE_CHANGE_LIMIT:
        reasons.append(
            "Медианный рост цен совпадающих позиций составил "
            f"{paired_median_change * 100:.1f}% (максимум 10%)."
        )
    if changed_share > CHANGED_ITEMS_SHARE_LIMIT:
        reasons.append(
            f"{changed_share * 100:.1f}% совпадающих позиций изменили "
            "цену более чем на 10% (максимум 20%)."
        )

    return PricelistAnomalyResult(
        blocked=bool(reasons),
        reasons=reasons,
        metrics={
            "previous_positions": previous_count,
            "candidate_positions": candidate_count,
            "positions_change_percent": round(row_change * 100, 2),
            "overlap_percent": round(overlap_ratio * 100, 2),
            "previous_median_price": round(previous_median, 2),
            "candidate_median_price": round(candidate_median, 2),
            "median_price_change_percent": round(median_change * 100, 2),
            "paired_median_price_change_percent": round(
                paired_median_change * 100,
                2,
            ),
            "changed_items_percent": round(changed_share * 100, 2),
            "increased_items_percent": round(increased_share * 100, 2),
            "compared_positions": compared_count,
        },
    )


async def _load_previous_price_map(
    session: AsyncSession,
    provider_config_id: int,
) -> tuple[int | None, dict[tuple[str, str], float]]:
    latest_id = (
        await session.execute(
            select(PriceList.id)
            .where(PriceList.provider_config_id == provider_config_id)
            .order_by(PriceList.date.desc().nullslast(), PriceList.id.desc())
            .limit(1)
        )
    ).scalar_one_or_none()
    if latest_id is None:
        return None, {}

    rows = (
        await session.execute(
            select(
                Brand.name,
                AutoPart.oem_number,
                PriceListAutoPartAssociation.price,
            )
            .join(
                AutoPart,
                AutoPart.id == PriceListAutoPartAssociation.autopart_id,
            )
            .join(Brand, Brand.id == AutoPart.brand_id)
            .where(
                PriceListAutoPartAssociation.pricelist_id == latest_id
            )
        )
    ).all()
    prices: dict[tuple[str, str], float] = {}
    for brand, oem, price in rows:
        key = _normalise_key(brand, oem)
        price_value = _money_float(price)
        if key is not None and price_value is not None:
            prices[key] = price_value
    return int(latest_id), prices


async def guard_automatic_provider_pricelist(
    *,
    session: AsyncSession,
    provider: Provider,
    provider_config: ProviderPriceListConfig,
    items: list[dict],
    source_filename: str | None = None,
) -> PricelistAnomalyResult:
    previous_id, previous_prices = await _load_previous_price_map(
        session,
        int(provider_config.id),
    )
    candidate_prices = build_candidate_price_map(items)
    result = calculate_pricelist_anomaly(previous_prices, candidate_prices)
    result.metrics["previous_pricelist_id"] = previous_id
    result.metrics["source_filename"] = source_filename

    if not result.blocked:
        return result

    reason_text = "\n".join(f"• {reason}" for reason in result.reasons)
    message = (
        f"Поставщик: {provider.name}\n"
        f"Конфигурация: {provider_config.name_price or provider_config.id}\n"
        f"Файл: {source_filename or 'не указан'}\n\n"
        f"{reason_text}\n\n"
        "Файл не опубликован, история цен и остатков не изменена. "
        "После проверки его можно загрузить вручную."
    )
    await create_admin_notifications(
        session,
        title=(
            f"{PRICELIST_ALERT_TITLE_PREFIX} "
            f"{provider.name} / "
            f"{provider_config.name_price or provider_config.id}"
        ),
        message=message,
        level=AppNotificationLevel.ERROR,
        link=f"/providers/{provider.id}/edit",
        commit=True,
    )
    logger.warning(
        "Blocked anomalous provider pricelist: provider_id=%s config_id=%s "
        "source=%s metrics=%s reasons=%s",
        provider.id,
        provider_config.id,
        source_filename,
        result.metrics,
        result.reasons,
    )
    return result
