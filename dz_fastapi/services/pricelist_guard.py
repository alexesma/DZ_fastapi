import asyncio
import hashlib
import logging
import os
import re
import statistics
from dataclasses import dataclass
from decimal import Decimal
from typing import Any
from uuid import uuid4

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.api.validators import normalize_brand_name
from dz_fastapi.core.time import now_moscow
from dz_fastapi.models.autopart import AutoPart, preprocess_oem_number
from dz_fastapi.models.brand import Brand
from dz_fastapi.models.notification import AppNotification, AppNotificationLevel
from dz_fastapi.models.partner import (
    PriceList,
    PriceListAutoPartAssociation,
    Provider,
    ProviderPriceListConfig,
    ProviderPricelistReview,
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
PRICELIST_REVIEW_DIR = os.path.join("uploads", "pricelist_reviews")


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


def build_review_examples(
    items: list[dict],
    previous_prices: dict[tuple[str, str], float],
    *,
    limit: int = 10,
) -> list[dict[str, Any]]:
    """Pick useful examples, preferring new positions and distinct brands."""
    candidates: list[dict[str, Any]] = []
    seen_keys: set[tuple[str, str]] = set()
    for item in items:
        key = _normalise_key(item.get("brand"), item.get("oem_number"))
        price = _money_float(item.get("price"))
        if key is None or price is None or key in seen_keys:
            continue
        seen_keys.add(key)
        previous_price = previous_prices.get(key)
        change_percent = None
        if previous_price:
            change_percent = round(
                (price - previous_price) / previous_price * 100,
                2,
            )
        candidates.append(
            {
                "brand": str(item.get("brand") or "").strip(),
                "oem_number": str(item.get("oem_number") or "").strip(),
                "name": str(item.get("name") or "").strip() or None,
                "quantity": int(item.get("quantity") or 0),
                "price": round(price, 2),
                "previous_price": previous_price,
                "price_change_percent": change_percent,
                "change_type": (
                    "new"
                    if previous_price is None
                    else "price_changed"
                    if change_percent
                    else "unchanged"
                ),
            }
        )

    ordered = sorted(
        candidates,
        key=lambda row: (
            0 if row["change_type"] == "new" else 1,
            -abs(row.get("price_change_percent") or 0),
            row["brand"],
            row["oem_number"],
        ),
    )
    result: list[dict[str, Any]] = []
    selected_ids: set[tuple[str, str]] = set()
    selected_brands: set[str] = set()

    for row in ordered:
        brand_key = row["brand"].casefold()
        if brand_key in selected_brands:
            continue
        result.append(row)
        selected_brands.add(brand_key)
        selected_ids.add((brand_key, row["oem_number"]))
        if len(result) >= limit:
            return result

    for row in ordered:
        row_id = (row["brand"].casefold(), row["oem_number"])
        if row_id in selected_ids:
            continue
        result.append(row)
        selected_ids.add(row_id)
        if len(result) >= limit:
            break
    return result


def _safe_source_filename(filename: str | None, extension: str | None) -> str:
    fallback_extension = str(extension or "bin").strip(".").lower() or "bin"
    raw = os.path.basename(str(filename or "").strip())
    if not raw:
        raw = f"pricelist.{fallback_extension}"
    safe = re.sub(r"[^A-Za-zА-Яа-я0-9._ -]", "_", raw)
    return safe[:240] or f"pricelist.{fallback_extension}"


async def _store_review_file(path: str, payload: bytes) -> None:
    def _write() -> None:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "wb") as file_handle:
            file_handle.write(payload)

    await asyncio.to_thread(_write)


async def _create_pricelist_review(
    *,
    session: AsyncSession,
    provider: Provider,
    provider_config: ProviderPriceListConfig,
    result: PricelistAnomalyResult,
    items: list[dict],
    previous_prices: dict[tuple[str, str], float],
    file_content: bytes,
    file_extension: str | None,
    source_filename: str | None,
) -> tuple[ProviderPricelistReview, bool]:
    # Serialize review creation per configuration. This prevents two emails
    # processed almost simultaneously from leaving two current reviews.
    await session.execute(
        select(ProviderPriceListConfig.id)
        .where(ProviderPriceListConfig.id == provider_config.id)
        .with_for_update()
    )
    checksum = hashlib.sha256(file_content).hexdigest()
    existing = (
        await session.execute(
            select(ProviderPricelistReview)
            .where(
                ProviderPricelistReview.provider_config_id
                == provider_config.id,
                ProviderPricelistReview.file_sha256 == checksum,
                ProviderPricelistReview.status.in_(
                    ("pending", "processing")
                ),
            )
            .order_by(ProviderPricelistReview.id.desc())
            .limit(1)
        )
    ).scalar_one_or_none()
    if existing is not None:
        return existing, False

    # От одной конфигурации нужен только самый свежий файл. Если поставщик
    # прислал ночью пять версий, первые четыре уже не имеют смысла проверять:
    # новая версия заменяет все предыдущие ожидающие решения.
    previous_pending = (
        (
            await session.execute(
                select(ProviderPricelistReview)
                .where(
                    ProviderPricelistReview.provider_config_id
                    == provider_config.id,
                    ProviderPricelistReview.status == "pending",
                )
                .with_for_update()
            )
        )
        .scalars()
        .all()
    )
    superseded_ids: list[int] = []
    decided_at = now_moscow()
    for pending in previous_pending:
        pending.status = "superseded"
        pending.decision_reason = (
            "Заменён более свежим прайсом этой конфигурации"
        )
        pending.decided_at = decided_at
        pending.processing_error = None
        session.add(pending)
        superseded_ids.append(int(pending.id))

    if superseded_ids:
        old_links = [
            f"/providers/{provider.id}/edit?pricelist_review={review_id}"
            for review_id in superseded_ids
        ]
        await session.execute(
            update(AppNotification)
            .where(
                AppNotification.link.in_(old_links),
                AppNotification.read_at.is_(None),
            )
            .values(read_at=decided_at)
        )

    safe_filename = _safe_source_filename(source_filename, file_extension)
    relative_path = os.path.join(
        PRICELIST_REVIEW_DIR,
        str(provider.id),
        f"{uuid4().hex}_{safe_filename}",
    )
    await _store_review_file(relative_path, file_content)
    review = ProviderPricelistReview(
        provider_id=provider.id,
        provider_config_id=provider_config.id,
        previous_pricelist_id=result.metrics.get("previous_pricelist_id"),
        source_filename=safe_filename,
        file_path=relative_path,
        file_extension=(
            str(file_extension or os.path.splitext(safe_filename)[1])
            .strip(".")
            .lower()
            or "bin"
        ),
        file_sha256=checksum,
        status="pending",
        reasons=list(result.reasons),
        metrics=dict(result.metrics),
        examples=build_review_examples(items, previous_prices),
    )
    session.add(review)
    await session.flush()
    return review, True


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
            .join(
                PriceListAutoPartAssociation,
                PriceListAutoPartAssociation.pricelist_id == PriceList.id,
            )
            .where(PriceList.provider_config_id == provider_config_id)
            # ID reflects publication order. A reviewed file may be approved
            # after a newer business date was written by an automatic import,
            # so ordering by date could keep the old baseline indefinitely.
            .order_by(PriceList.id.desc())
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
    file_content: bytes | None = None,
    file_extension: str | None = None,
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

    review = None
    review_created = False
    if file_content is not None:
        review, review_created = await _create_pricelist_review(
            session=session,
            provider=provider,
            provider_config=provider_config,
            result=result,
            items=items,
            previous_prices=previous_prices,
            file_content=file_content,
            file_extension=file_extension,
            source_filename=source_filename,
        )

    if review is not None and not review_created:
        logger.warning(
            "Blocked duplicate anomalous provider pricelist: "
            "provider_id=%s config_id=%s review_id=%s source=%s",
            provider.id,
            provider_config.id,
            review.id,
            source_filename,
        )
        return result

    reason_text = "\n".join(f"• {reason}" for reason in result.reasons)
    next_step = (
        "Откройте проверку в карточке поставщика: исходный файл можно "
        "скачать, затем принять и опубликовать либо отклонить с причиной."
        if review is not None
        else "После проверки файл можно загрузить вручную."
    )
    message = (
        f"Поставщик: {provider.name}\n"
        f"Конфигурация: {provider_config.name_price or provider_config.id}\n"
        f"Файл: {source_filename or 'не указан'}\n\n"
        f"{reason_text}\n\n"
        "Файл не опубликован, история цен и остатков не изменена. "
        f"{next_step}"
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
        link=(
            f"/providers/{provider.id}/edit"
            + (f"?pricelist_review={review.id}" if review else "")
        ),
        payload=(
            {
                "notification_type": "provider_pricelist_review",
                "review_id": int(review.id),
                "provider_id": int(provider.id),
                "provider_config_id": int(provider_config.id),
                "provider_name": provider.name,
                "config_name": provider_config.name_price or str(provider_config.id),
                "source_filename": review.source_filename,
                "reasons": list(review.reasons or []),
                "metrics": dict(review.metrics or {}),
                "examples": list(review.examples or []),
            }
            if review is not None
            else None
        ),
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
