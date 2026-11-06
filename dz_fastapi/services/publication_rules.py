"""Предпросмотр, массовое задание и импорт из Excel правил публикации.

Здесь живут только «простые» правила, у которых нет кроссов: скрыть позицию
или поменять её цену (``price_only``). Правила с кроссами создаются поштучно,
потому что для каждой позиции кроссы свои.
"""

from __future__ import annotations

import io
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import ROUND_CEILING, ROUND_HALF_UP, Decimal
from typing import Any, Optional

import pandas as pd
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from dz_fastapi.core.time import now_moscow
from dz_fastapi.models.autopart import AutoPart, preprocess_oem_number
from dz_fastapi.models.brand import Brand
from dz_fastapi.models.partner import (
    CustomerPriceListPublicationRule,
    CustomerPriceListSource,
    PriceList,
    PriceListAutoPartAssociation,
    ProviderPriceListConfig,
)

BULK_POSITIONS_LIMIT = 5000
IMPORT_ROWS_LIMIT = 5000
IMPORT_TEMPLATE_HEADERS = [
    "Бренд",
    "Артикул",
    "Действие",
    "Тип цены",
    "Значение",
    "Прайс",
    "Округление",
    "Действует с",
    "Действует по",
]
_ACTIONS = {"скрыть": "hide", "hide": "hide", "цена": "price_only", "price": "price_only"}
_PRICE_KINDS = {
    "фиксированная": "fixed",
    "фикс": "fixed",
    "fixed": "fixed",
    "процент": "percent",
    "%": "percent",
    "percent": "percent",
}


def source_label(source: Optional[CustomerPriceListSource]) -> Optional[str]:
    if source is None:
        return None
    provider_config = source.provider_config
    provider = getattr(provider_config, "provider", None)
    parts = [
        str(getattr(provider, "name", "") or "").strip(),
        str(getattr(provider_config, "name_price", "") or "").strip(),
    ]
    return " · ".join(part for part in parts if part) or f"Источник {source.id}"


def compute_rule_price(
    *,
    base_price: float,
    price_mode: str,
    fixed_price: Optional[float],
    price_percent: Optional[float],
    rounding_step: Optional[int],
) -> float:
    """Цена по правилу; общая для предпросмотра и сохранения."""
    if price_mode == "fixed" and fixed_price is not None:
        return float(fixed_price)
    if price_mode != "percent" or price_percent is None:
        return float(base_price)
    price = Decimal(str(base_price)) * Decimal(str(price_percent)) / Decimal(100)
    step = int(rounding_step or 0)
    if step > 1:
        price = (price / step).to_integral_value(rounding=ROUND_CEILING) * step
    return float(price.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


async def load_config_sources(
    session: AsyncSession, config_id: int
) -> list[CustomerPriceListSource]:
    return list(
        (
            await session.execute(
                select(CustomerPriceListSource)
                .options(
                    selectinload(CustomerPriceListSource.provider_config).selectinload(
                        ProviderPriceListConfig.provider
                    )
                )
                .where(
                    CustomerPriceListSource.customer_config_id == config_id,
                    CustomerPriceListSource.enabled.is_(True),
                )
                .order_by(CustomerPriceListSource.id)
            )
        ).scalars()
    )


async def _latest_pricelist_id(
    session: AsyncSession, source: CustomerPriceListSource
) -> Optional[int]:
    return (
        await session.execute(
            select(func.max(PriceList.id)).where(
                PriceList.provider_config_id == source.provider_config_id,
                PriceList.is_active.is_(True),
            )
        )
    ).scalar_one_or_none()


async def source_prices_for_autopart(
    session: AsyncSession, sources: list[CustomerPriceListSource], autopart_id: int
) -> list[dict[str, Any]]:
    """Цена позиции в свежем прайсе каждого источника с наценкой источника."""
    result: list[dict[str, Any]] = []
    for source in sources:
        latest = await _latest_pricelist_id(session, source)
        if latest is None:
            continue
        price = (
            await session.execute(
                select(func.min(PriceListAutoPartAssociation.price)).where(
                    PriceListAutoPartAssociation.pricelist_id == latest,
                    PriceListAutoPartAssociation.autopart_id == autopart_id,
                )
            )
        ).scalar_one_or_none()
        if price is None:
            continue
        markup = float(source.markup or 1.0)
        result.append(
            {
                "source_id": source.id,
                "source_name": source_label(source),
                "price": float(price),
                "price_with_markup": round(float(price) * markup, 2),
            }
        )
    return result


@dataclass
class SimpleRuleSpec:
    """Параметры простого правила: скрыть позицию или поменять её цену."""

    mode: str  # hide | price_only
    price_mode: str = "default"  # fixed | percent
    fixed_price: Optional[float] = None
    price_percent: Optional[float] = None
    price_source_id: Optional[int] = None
    price_rounding_step: Optional[int] = None
    valid_from: Optional[date] = None
    valid_to: Optional[date] = None
    is_active: bool = True


@dataclass
class BulkReport:
    created: int = 0
    updated: int = 0
    skipped: int = 0
    errors: list[dict[str, Any]] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {
            "created": self.created,
            "updated": self.updated,
            "skipped": self.skipped,
            "errors": self.errors,
        }


async def upsert_simple_rule(
    session: AsyncSession,
    *,
    config_id: int,
    autopart_id: int,
    spec: SimpleRuleSpec,
    user_id: Optional[int],
    overwrite_cross_rules: bool = False,
) -> str:
    """Создаёт или обновляет правило; ``created``, ``updated`` или ``skipped``."""
    existing = (
        await session.execute(
            select(CustomerPriceListPublicationRule)
            .options(selectinload(CustomerPriceListPublicationRule.targets))
            .where(
                CustomerPriceListPublicationRule.config_id == config_id,
                CustomerPriceListPublicationRule.source_autopart_id == autopart_id,
            )
        )
    ).scalar_one_or_none()
    outcome = "updated"
    if existing is None:
        existing = CustomerPriceListPublicationRule(
            config_id=config_id,
            source_autopart_id=autopart_id,
            created_by_user_id=user_id,
        )
        outcome = "created"
    elif existing.targets and not overwrite_cross_rules:
        return "skipped"  # правило с кроссами не затираем без явного согласия
    elif existing.targets:
        existing.targets.clear()
        await session.flush()
    is_priced = spec.mode == "price_only"
    existing.target_autopart_id = None
    existing.mode = spec.mode
    existing.price_mode = spec.price_mode if is_priced else "default"
    existing.fixed_price = spec.fixed_price if is_priced and spec.price_mode == "fixed" else None
    is_percent = is_priced and spec.price_mode == "percent"
    existing.price_percent = spec.price_percent if is_percent else None
    existing.price_source_id = spec.price_source_id if is_percent else None
    existing.price_rounding_step = spec.price_rounding_step if is_percent else None
    existing.valid_from = spec.valid_from
    existing.valid_to = spec.valid_to
    existing.is_active = spec.is_active
    existing.updated_by_user_id = user_id
    existing.updated_at = now_moscow()
    session.add(existing)
    return outcome


async def brand_positions_in_sources(
    session: AsyncSession,
    sources: list[CustomerPriceListSource],
    brand_id: int,
) -> list[int]:
    """Позиции бренда, которые есть в свежих прайсах источников конфигурации."""
    latest_ids = []
    for source in sources:
        latest = await _latest_pricelist_id(session, source)
        if latest is not None:
            latest_ids.append(latest)
    if not latest_ids:
        return []
    rows = (
        await session.execute(
            select(AutoPart.id)
            .join(
                PriceListAutoPartAssociation,
                PriceListAutoPartAssociation.autopart_id == AutoPart.id,
            )
            .where(
                AutoPart.brand_id == brand_id,
                PriceListAutoPartAssociation.pricelist_id.in_(latest_ids),
            )
            .group_by(AutoPart.id)
            .order_by(AutoPart.id)
            .limit(BULK_POSITIONS_LIMIT + 1)
        )
    ).all()
    return [row[0] for row in rows]


def _clean(value: Any) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    return str(value).strip()


def _as_date(value: Any) -> Optional[date]:
    text = _clean(value)
    if not text:
        return None
    if isinstance(value, (datetime, pd.Timestamp)):
        return value.date()
    for pattern in ("%d.%m.%Y", "%Y-%m-%d", "%d.%m.%y"):
        try:
            return datetime.strptime(text[:10], pattern).date()
        except ValueError:
            continue
    raise ValueError(f"Не удалось разобрать дату «{text}»")


def _as_number(value: Any) -> Optional[float]:
    text = _clean(value).replace(" ", "").replace(" ", "").replace(",", ".")
    if not text:
        return None
    try:
        return float(text)
    except ValueError as exc:
        raise ValueError(f"Не удалось разобрать число «{text}»") from exc


def build_import_template() -> bytes:
    sample = pd.DataFrame(
        [
            ["TOYOTA", "9091603107", "цена", "процент", 95, "", 5, "", ""],
            [
                "TOYOTA", "9091603108", "цена", "фиксированная", 1250, "", "",
                "01.12.2026", "31.12.2026",
            ],
            ["HONDA", "52245SBAA21", "скрыть", "", "", "", "", "", ""],
        ],
        columns=IMPORT_TEMPLATE_HEADERS,
    )
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        sample.to_excel(writer, index=False, sheet_name="Правила")
    return buffer.getvalue()


def _row_spec(row: Any, sources_by_label: dict[str, CustomerPriceListSource]) -> SimpleRuleSpec:
    action = _ACTIONS.get(_clean(row.get("действие")).lower())
    if action is None:
        raise ValueError("Действие должно быть «скрыть» или «цена»")
    spec = SimpleRuleSpec(
        mode=action,
        valid_from=_as_date(row.get("действует с")),
        valid_to=_as_date(row.get("действует по")),
    )
    if spec.valid_from and spec.valid_to and spec.valid_to < spec.valid_from:
        raise ValueError("Дата «по» раньше даты «с»")
    if action == "hide":
        return spec
    kind = _PRICE_KINDS.get(_clean(row.get("тип цены")).lower())
    value = _as_number(row.get("значение"))
    if kind is None:
        raise ValueError("Тип цены должен быть «фиксированная» или «процент»")
    if value is None or value <= 0:
        raise ValueError("Не указано значение цены или процента")
    spec.price_mode = kind
    if kind == "fixed":
        spec.fixed_price = value
        return spec
    if value > 1000:
        raise ValueError("Процент не может быть больше 1000")
    spec.price_percent = value
    rounding = _as_number(row.get("округление"))
    spec.price_rounding_step = int(rounding) if rounding else None
    source_name = _clean(row.get("прайс")).lower()
    if source_name:
        source = sources_by_label.get(source_name) or next(
            (item for label, item in sources_by_label.items() if source_name in label),
            None,
        )
        if source is None:
            raise ValueError(f"Прайс «{_clean(row.get('прайс'))}» не подключён к конфигурации")
        spec.price_source_id = source.id
    return spec


async def import_rules_from_excel(
    session: AsyncSession,
    *,
    config_id: int,
    content: bytes,
    user_id: Optional[int],
    overwrite_cross_rules: bool = False,
) -> BulkReport:
    """Импорт простых правил из xlsx; строки с ошибками пропускаются и описываются."""
    report = BulkReport()
    try:
        frame = pd.read_excel(io.BytesIO(content), dtype=object)
    except Exception as exc:  # noqa: BLE001 — любой битый файл сводим к понятной ошибке
        raise ValueError("Не удалось прочитать файл: нужен Excel (.xlsx)") from exc
    frame.columns = [_clean(name).lower() for name in frame.columns]
    missing = {"бренд", "артикул", "действие"} - set(frame.columns)
    if missing:
        raise ValueError("В файле нет обязательных колонок: " + ", ".join(sorted(missing)))
    if len(frame) > IMPORT_ROWS_LIMIT:
        raise ValueError(f"В файле больше {IMPORT_ROWS_LIMIT} строк")

    sources = await load_config_sources(session, config_id)
    sources_by_label = {(source_label(item) or "").lower(): item for item in sources}
    brands = {
        name.lower(): brand_id
        for brand_id, name in (await session.execute(select(Brand.id, Brand.name))).all()
    }
    for index, row in frame.iterrows():
        line = int(index) + 2  # строка в Excel: заголовок + нумерация с 1
        try:
            brand_name, oem = _clean(row.get("бренд")), _clean(row.get("артикул"))
            if not brand_name or not oem:
                raise ValueError("Не указан бренд или артикул")
            brand_id = brands.get(brand_name.lower())
            if brand_id is None:
                raise ValueError(f"Бренд «{brand_name}» не найден")
            autopart_id = (
                await session.execute(
                    select(AutoPart.id).where(
                        AutoPart.brand_id == brand_id,
                        AutoPart.oem_number == preprocess_oem_number(oem),
                    )
                )
            ).scalar_one_or_none()
            if autopart_id is None:
                raise ValueError(f"Позиция {brand_name} {oem} не найдена в номенклатуре")
            outcome = await upsert_simple_rule(
                session,
                config_id=config_id,
                autopart_id=autopart_id,
                spec=_row_spec(row, sources_by_label),
                user_id=user_id,
                overwrite_cross_rules=overwrite_cross_rules,
            )
            setattr(report, outcome, getattr(report, outcome) + 1)
        except ValueError as exc:
            report.errors.append({"row": line, "message": str(exc)})
    await session.commit()
    return report
