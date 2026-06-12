from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from datetime import date, datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal
from math import ceil
from typing import Any, Optional

import httpx
from sqlalchemy import delete, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.core.constants import URL_DZ_SEARCH
from dz_fastapi.core.time import now_moscow
from dz_fastapi.crud.partner import crud_provider
from dz_fastapi.http.dz_site_client import DZSiteClient
from dz_fastapi.models.autopart import AutoPart, AutoPurchaseRun, AutoPurchaseRunItem
from dz_fastapi.models.brand import Brand
from dz_fastapi.models.cross import AutoPartCross, AutoPartInvalidCross
from dz_fastapi.models.partner import (
    CUSTOMER_ORDER_ITEM_STATUS,
    STOCK_ORDER_STATUS,
    SUPPLIER_ORDER_STATUS,
    CustomerOrder,
    CustomerOrderItem,
    PriceList,
    PriceListAutoPartAssociation,
    Provider,
    ProviderPriceListConfig,
    StockOrder,
    StockOrderItem,
    SupplierOrder,
    SupplierOrderItem,
    SupplierReceipt,
    SupplierReceiptItem,
)
from dz_fastapi.services.placed_orders import (
    _ACTIVE_ORDER_STATUSES,
    _build_tracking_exceptions,
    _compute_purchase_price_stats,
    _compute_single_oem_abc_xyz_batch,
    _load_tracking_history_rows_for_oems,
    _normalize_oem,
    _prioritize_tracking_exceptions,
    _round_stat,
    _to_decimal,
)
from dz_fastapi.services.site_brand_search import (
    expand_site_query_brands,
    merge_site_offers,
    resolve_fallback_site_brand,
)

AUTOPURCHASE_MODE_DRAFT_ONLY = "draft_only"
AUTOPURCHASE_MODE_AUTO_APPROVE_SAFE = "auto_approve_safe"
AUTOPURCHASE_MODE_DISABLED = "disabled"

AUTOPURCHASE_RUN_STATUS_QUEUED = "queued"
AUTOPURCHASE_RUN_STATUS_RUNNING = "running"
AUTOPURCHASE_RUN_STATUS_COMPLETED = "completed"
AUTOPURCHASE_RUN_STATUS_FAILED = "failed"

AUTOPURCHASE_STATUS_AUTO_APPROVED = "auto_approved"
AUTOPURCHASE_STATUS_NEEDS_REVIEW = "needs_review"
AUTOPURCHASE_STATUS_BLOCKED = "blocked"

AUTOPURCHASE_MANUAL_REASON_CODES = {
    "autopurchase_manual_auto_approved",
    "autopurchase_manual_needs_review",
    "autopurchase_manual_blocked",
}

FILL_RATE_THRESHOLD_DRAFT = 60.0
FILL_RATE_THRESHOLD_AUTO_APPROVE = 80.0

SAFETY_STOCK_DAYS_BY_ABC_XYZ: dict[tuple[str, str], int] = {
    ("A", "X"): 14,
    ("A", "Y"): 10,
    ("A", "Z"): 7,
    ("B", "X"): 10,
    ("B", "Y"): 7,
    ("B", "Z"): 5,
    ("C", "X"): 7,
    ("C", "Y"): 5,
    ("C", "Z"): 0,
}

MAX_LEAD_DAYS_BY_ABC_XYZ: dict[tuple[str, str], int] = {
    ("A", "X"): 21,
    ("A", "Y"): 28,
    ("B", "X"): 30,
    ("B", "Y"): 35,
}
ABC_CLASS_PRIORITY: dict[str, int] = {
    "A": 0,
    "B": 1,
    "C": 2,
}
XYZ_CLASS_PRIORITY: dict[str, int] = {
    "X": 0,
    "Y": 1,
    "Z": 2,
}

DRAGONZAP_BRAND_NAME = "DRAGONZAP"
# Бренды-кроссы, которые предпочитаем при подборе замены для Dragonzap:
# Dragonzap производится из деталей этих марок.
DRAGONZAP_PREFERRED_CROSS_BRANDS: tuple[str, ...] = tuple(
    item.strip().upper()
    for item in os.getenv(
        "DRAGONZAP_PREFERRED_CROSS_BRANDS",
        "CHERY,HAVAL,GEELY,LIFAN,JAC,CHANGAN",
    ).split(",")
    if item.strip()
)
# Сколько кроссов максимум проверяем на сайте для одной Dragonzap-позиции
# (кроссов может быть десятки — ограничиваем количество запросов).
AUTOPURCHASE_MAX_CROSS_QUERIES = max(
    1,
    int(os.getenv("AUTOPURCHASE_MAX_CROSS_QUERIES", "8")),
)
# Контроль закупочной цены относительно нашей продажной цены:
# жёсткий потолок — закупка не дороже 90% продажи (иначе блокируем),
# целевой уровень — 70–80% (дороже 80% → только ручная проверка).
AUTOPURCHASE_MAX_PURCHASE_TO_SALE_RATIO = max(
    0.1,
    float(os.getenv("AUTOPURCHASE_MAX_PURCHASE_TO_SALE_RATIO", "0.9")),
)
AUTOPURCHASE_TARGET_PURCHASE_TO_SALE_RATIO = max(
    0.1,
    float(os.getenv("AUTOPURCHASE_TARGET_PURCHASE_TO_SALE_RATIO", "0.8")),
)

SITE_API_KEY = os.getenv("KEY_FOR_WEBSITE")
AUTOPURCHASE_REASONS_LIMIT = 8
AUTOPURCHASE_MAX_LIMIT = 1000
AUTOPURCHASE_SITE_FETCH_CONCURRENCY = max(
    int(os.getenv("AUTOPURCHASE_SITE_FETCH_CONCURRENCY", "6")),
    1,
)
AUTOPURCHASE_RUN_LOCK_KEY = int(
    os.getenv("AUTOPURCHASE_RUN_LOCK_KEY", "92025001")
)
AUTOPURCHASE_FINISHED_HISTORY_LIMIT = max(
    0,
    int(os.getenv("AUTOPURCHASE_FINISHED_HISTORY_LIMIT", "3")),
)
AUTOPURCHASE_AI_ENABLED = (
    str(os.getenv("AUTOPURCHASE_AI_ENABLED", "1")).strip().lower()
    in {"1", "true", "yes", "on"}
)
AUTOPURCHASE_AI_MODEL = (
    str(os.getenv("AUTOPURCHASE_AI_MODEL", "gpt-4o-mini")).strip()
    or "gpt-4o-mini"
)
AUTOPURCHASE_AI_BASE_URL = (
    str(
        os.getenv("AUTOPURCHASE_AI_BASE_URL")
        or os.getenv("OPENAI_BASE_URL")
        or "https://api.openai.com/v1"
    ).strip()
    or "https://api.openai.com/v1"
)
AUTOPURCHASE_AI_API_KEY = str(os.getenv("OPENAI_API_KEY", "")).strip()
AUTOPURCHASE_AI_TIMEOUT_SEC = max(
    5,
    int(os.getenv("AUTOPURCHASE_AI_TIMEOUT_SEC", "45")),
)
# Срок поставки по умолчанию (дней), используется как fallback
# когда фактическая история заказов отсутствует, но есть минимальный остаток.
AUTOPURCHASE_DEFAULT_LEAD_DAYS_FALLBACK = max(
    1,
    int(os.getenv("AUTOPURCHASE_DEFAULT_LEAD_DAYS_FALLBACK", "7")),
)
AUTOPURCHASE_DEMAND_WINDOWS = (30, 90, 180, 365)
# Целевой запас: на сколько дней спроса заказываем (1,5 месяца для A
# и позиций без класса; для B и C — меньше, чтобы не замораживать деньги
# в медленных позициях).
AUTOPURCHASE_TARGET_COVER_DAYS = max(
    1,
    int(os.getenv("AUTOPURCHASE_TARGET_COVER_DAYS", "45")),
)
AUTOPURCHASE_TARGET_COVER_DAYS_B = max(
    1,
    int(os.getenv("AUTOPURCHASE_TARGET_COVER_DAYS_B", "30")),
)
AUTOPURCHASE_TARGET_COVER_DAYS_C = max(
    1,
    int(os.getenv("AUTOPURCHASE_TARGET_COVER_DAYS_C", "21")),
)
# Максимум, во сколько раз корректировка «спрос на день наличия» может
# поднять календарную среднюю продаж.
AUTOPURCHASE_AVAILABILITY_BOOST_CAP = max(
    1.0,
    float(os.getenv("AUTOPURCHASE_AVAILABILITY_BOOST_CAP", "3")),
)
# Сколько дней наблюдений наличия нужно, чтобы полностью доверять
# поправке «спрос на день наличия» (меньше дней — ближе к календарной).
AUTOPURCHASE_AVAILABILITY_CONFIDENCE_DAYS = max(
    1,
    int(os.getenv("AUTOPURCHASE_AVAILABILITY_CONFIDENCE_DAYS", "14")),
)
# Ценовые сигналы: «нет продаж при наличии» и «всплеск спроса на минимуме».
AUTOPURCHASE_NO_SALES_MIN_STOCK_DAYS = 14
AUTOPURCHASE_PRICE_HIGH_FACTOR = 1.2
AUTOPURCHASE_DEMAND_SPIKE_FACTOR = 3.0
AUTOPURCHASE_DEMAND_SPIKE_MIN_QTY = 10
# Кэш предложений сайта между запусками (сек); 0 — выключить.
AUTOPURCHASE_SITE_CACHE_TTL_SEC = max(
    0,
    int(os.getenv("AUTOPURCHASE_SITE_CACHE_TTL_SEC", "3600")),
)
_SITE_OFFERS_CACHE: dict[
    tuple[str, str, bool], tuple[float, list[dict[str, Any]]]
] = {}
_SITE_OFFERS_CACHE_MAX_ENTRIES = 20000
# Сколько лучших предложений сайта сохраняем для ручного выбора.
AUTOPURCHASE_TOP_OFFERS_LIMIT = max(
    1,
    int(os.getenv("AUTOPURCHASE_TOP_OFFERS_LIMIT", "10")),
)
AUTOPURCHASE_RECOVERY_STOCKOUT_DAYS = max(
    1,
    int(os.getenv("AUTOPURCHASE_RECOVERY_STOCKOUT_DAYS", "45")),
)

logger = logging.getLogger("dz_fastapi")


def _quantize_float(value: Optional[float], digits: str = "0.01") -> Optional[float]:
    if value is None:
        return None
    return float(
        Decimal(str(value)).quantize(
            Decimal(digits), rounding=ROUND_HALF_UP
        )
    )


def _format_money_value(value: Optional[float]) -> str:
    normalized = _quantize_float(value)
    if normalized is None:
        return "0.00"
    return f"{normalized:.2f}"


def _get_safety_stock_days(
    abc_class: Optional[str],
    xyz_class: Optional[str],
) -> int:
    return SAFETY_STOCK_DAYS_BY_ABC_XYZ.get(
        (str(abc_class or "").upper(), str(xyz_class or "").upper()),
        5,
    )


def _get_max_allowed_lead_days(
    abc_class: Optional[str],
    xyz_class: Optional[str],
) -> Optional[int]:
    return MAX_LEAD_DAYS_BY_ABC_XYZ.get(
        (str(abc_class or "").upper(), str(xyz_class or "").upper())
    )


def _compute_average_daily(sold_qty: int, days: int) -> Optional[float]:
    if sold_qty <= 0 or days <= 0:
        return None
    return _quantize_float(sold_qty / days)


def _compute_availability_adjusted_daily(
    sold_qty: int,
    window_days: int,
    in_stock_days: int,
) -> Optional[float]:
    """Спрос в день НАЛИЧИЯ товара, а не календарный.

    Если товар был в наличии 10 дней из 30 и продано 10 шт, реальный
    спрос ~1 шт/день, а не 0.33 — иначе система сама занижает заказ
    после каждого stockout. Три защиты от взрывных оценок:
    - нижняя граница знаменателя (7 дней);
    - доверие к поправке пропорционально числу дней наблюдений:
      3 дня наличия — почти не доверяем (остаёмся у календарной
      средней), AUTOPURCHASE_AVAILABILITY_CONFIDENCE_DAYS и больше —
      доверяем полностью;
    - итог не может превышать календарную среднюю больше чем в
      AUTOPURCHASE_AVAILABILITY_BOOST_CAP раз.
    """
    if sold_qty <= 0 or window_days <= 0:
        return None
    floor_days = min(window_days, 7)
    effective_days = max(min(int(in_stock_days), window_days), floor_days)
    adjusted_daily = sold_qty / effective_days
    calendar_daily = sold_qty / window_days
    confidence = min(
        max(int(in_stock_days), 0)
        / AUTOPURCHASE_AVAILABILITY_CONFIDENCE_DAYS,
        1.0,
    )
    blended_daily = (
        calendar_daily + (adjusted_daily - calendar_daily) * confidence
    )
    capped_daily = min(
        blended_daily,
        calendar_daily * AUTOPURCHASE_AVAILABILITY_BOOST_CAP,
    )
    return _quantize_float(capped_daily)


def _get_target_cover_days(abc_class: Optional[str]) -> int:
    normalized = str(abc_class or "").strip().upper()
    if normalized == "B":
        return AUTOPURCHASE_TARGET_COVER_DAYS_B
    if normalized == "C":
        return AUTOPURCHASE_TARGET_COVER_DAYS_C
    return AUTOPURCHASE_TARGET_COVER_DAYS


async def _get_site_offers_cached(
    client: DZSiteClient,
    *,
    oem: str,
    brand: str,
    without_cross: bool,
) -> list[dict[str, Any]]:
    """get_offers с TTL-кэшем: повторный пересчёт не бомбит сайт заново."""
    cache_key = (
        str(oem or "").strip().upper(),
        str(brand or "").strip().upper(),
        bool(without_cross),
    )
    if AUTOPURCHASE_SITE_CACHE_TTL_SEC > 0:
        cached = _SITE_OFFERS_CACHE.get(cache_key)
        if (
            cached is not None
            and (time.monotonic() - cached[0]) < AUTOPURCHASE_SITE_CACHE_TTL_SEC
        ):
            return [dict(item) for item in cached[1]]

    offers = await client.get_offers(
        oem=oem,
        brand=brand,
        without_cross=without_cross,
    )
    normalized_offers = [
        item for item in (offers or []) if isinstance(item, dict)
    ]
    if AUTOPURCHASE_SITE_CACHE_TTL_SEC > 0:
        if len(_SITE_OFFERS_CACHE) >= _SITE_OFFERS_CACHE_MAX_ENTRIES:
            _SITE_OFFERS_CACHE.clear()
        _SITE_OFFERS_CACHE[cache_key] = (
            time.monotonic(),
            [dict(item) for item in normalized_offers],
        )
    return normalized_offers


def _blend_average_daily(
    avg_daily_30: Optional[float],
    avg_daily_90: Optional[float],
) -> Optional[float]:
    if avg_daily_30 is not None and avg_daily_90 is not None:
        return _quantize_float(avg_daily_30 * 0.7 + avg_daily_90 * 0.3)
    return avg_daily_30 if avg_daily_30 is not None else avg_daily_90


def _blend_average_daily_horizons(
    avg_daily_30: Optional[float],
    avg_daily_90: Optional[float],
    avg_daily_180: Optional[float],
    avg_daily_365: Optional[float],
) -> Optional[float]:
    weighted_sum = 0.0
    total_weight = 0.0
    for value, weight in (
        (avg_daily_30, 0.45),
        (avg_daily_90, 0.30),
        (avg_daily_180, 0.15),
        (avg_daily_365, 0.10),
    ):
        if value is None or value <= 0:
            continue
        weighted_sum += float(value) * weight
        total_weight += weight
    if total_weight <= 0:
        return None
    return _quantize_float(weighted_sum / total_weight)


def _build_reason(
    *,
    code: str,
    severity: str,
    title: str,
    description: str,
) -> dict[str, str]:
    return {
        "code": code,
        "severity": severity,
        "title": title,
        "description": description,
    }


def _build_autopurchase_diagnostic_metric(
    *,
    code: str,
    title: str,
    value: int,
    description: Optional[str] = None,
) -> dict[str, Any]:
    return {
        "code": code,
        "title": title,
        "value": int(value),
        "description": description,
    }


def _limit_reasons(reasons: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        dict(reason)
        for reason in _prioritize_tracking_exceptions(
            list(reasons or []),
            limit=AUTOPURCHASE_REASONS_LIMIT,
        )
    ]


def _to_decimal_value(value: Any) -> Optional[Decimal]:
    if value is None:
        return None
    return Decimal(str(value))


def _to_json_safe(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Decimal):
        if value == value.to_integral_value():
            return int(value)
        return float(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, dict):
        return {
            str(key): _to_json_safe(item_value)
            for key, item_value in value.items()
        }
    if isinstance(value, (list, tuple, set)):
        return [_to_json_safe(item) for item in value]
    return str(value)


def _get_abc_xyz_priority(
    abc_xyz: Optional[dict[str, Any]],
) -> tuple[int, int]:
    payload = dict(abc_xyz or {})
    abc_class = str(payload.get("abc_class") or "").strip().upper()
    xyz_class = str(payload.get("xyz_class") or "").strip().upper()
    return (
        ABC_CLASS_PRIORITY.get(abc_class, 9),
        XYZ_CLASS_PRIORITY.get(xyz_class, 9),
    )


def _get_autopurchase_priority_key(
    item: dict[str, Any],
) -> tuple[int, int, int, int, int, int, str]:
    abc_priority, xyz_priority = _get_abc_xyz_priority(item.get("abc_xyz"))
    estimated_days_left = item.get("estimated_days_left_30_days")
    open_customer_backlog_qty = int(item.get("open_customer_backlog_qty") or 0)
    return (
        (
            -1
            if open_customer_backlog_qty > 0
            else (
                int(estimated_days_left)
                if estimated_days_left is not None
                else 9_999
            )
        ),
        -open_customer_backlog_qty,
        abc_priority,
        xyz_priority,
        -int(item.get("recommended_order_qty") or 0),
        -int(item.get("sold_last_30_days") or 0),
        str(item.get("oem_number") or ""),
    )


def _serialize_autopurchase_run(run: AutoPurchaseRun) -> dict[str, Any]:
    summary_snapshot = dict(run.summary_snapshot or {})
    return {
        "id": int(run.id),
        "provider_config_id": int(run.provider_config_id),
        "provider_id": int(run.provider_id),
        "provider_name": summary_snapshot.get("provider_name") or "",
        "provider_config_name": summary_snapshot.get("provider_config_name"),
        "initiated_by_user_id": run.initiated_by_user_id,
        "started_at": run.started_at,
        "finished_at": run.finished_at,
        "status": run.status,
        "mode": run.mode,
        "trigger_source": run.trigger_source,
        "supplier_source": "site",
        "settings_snapshot": dict(run.settings_snapshot or {}),
        "summary_snapshot": summary_snapshot,
        "total_items": int(summary_snapshot.get("total_items") or 0),
        "auto_approved_count": int(
            summary_snapshot.get("auto_approved_count") or 0
        ),
        "needs_review_count": int(
            summary_snapshot.get("needs_review_count") or 0
        ),
        "blocked_count": int(summary_snapshot.get("blocked_count") or 0),
        "sent_count": int(summary_snapshot.get("sent_count") or 0),
    }


async def _prune_old_finished_autopurchase_runs(
    session: AsyncSession,
) -> int:
    if AUTOPURCHASE_FINISHED_HISTORY_LIMIT <= 0:
        return 0

    prune_stmt = (
        select(AutoPurchaseRun.id)
        .where(AutoPurchaseRun.finished_at.is_not(None))
        .order_by(
            AutoPurchaseRun.finished_at.desc(),
            AutoPurchaseRun.started_at.desc(),
            AutoPurchaseRun.id.desc(),
        )
        .offset(AUTOPURCHASE_FINISHED_HISTORY_LIMIT)
    )
    prune_ids = [
        int(run_id)
        for run_id in (await session.execute(prune_stmt)).scalars().all()
    ]
    if not prune_ids:
        return 0

    await session.execute(
        delete(AutoPurchaseRunItem).where(
            AutoPurchaseRunItem.run_id.in_(prune_ids)
        )
    )
    await session.execute(
        delete(AutoPurchaseRun).where(AutoPurchaseRun.id.in_(prune_ids))
    )
    return len(prune_ids)


def _serialize_autopurchase_run_item(
    item: AutoPurchaseRunItem,
) -> dict[str, Any]:
    return {
        "id": int(item.id),
        "run_id": int(item.run_id),
        "selected_supplier_id": item.selected_supplier_id,
        "oem_number": item.oem_number,
        "brand_name": item.brand_name,
        "autopart_name": item.autopart_name,
        "autopart_id": item.autopart_id,
        "current_quantity": int(item.current_quantity or 0),
        "latest_price": float(item.latest_price) if item.latest_price is not None else None,
        "minimum_balance": int(item.minimum_balance or 0),
        "multiplicity": int(item.multiplicity or 1),
        "in_transit_qty": int(item.in_transit_qty or 0),
        "open_customer_backlog_qty": int(
            (item.draft_purchase_order or {}).get("open_customer_backlog_qty")
            or 0
        ),
        "last_receipt_price": (
            float(
                (item.draft_purchase_order or {}).get("last_receipt_price")
            )
            if (item.draft_purchase_order or {}).get("last_receipt_price")
            is not None
            else None
        ),
        "sold_last_30_days": int(item.sold_last_30_days or 0),
        "sold_last_90_days": int(item.sold_last_90_days or 0),
        "avg_daily_30": float(item.avg_daily_30) if item.avg_daily_30 is not None else None,
        "avg_daily_90": float(item.avg_daily_90) if item.avg_daily_90 is not None else None,
        "avg_daily_blended": (
            float(item.avg_daily_blended)
            if item.avg_daily_blended is not None
            else None
        ),
        "estimated_days_left_30_days": item.estimated_days_left_30_days,
        "average_actual_lead_days": (
            float(item.average_actual_lead_days)
            if item.average_actual_lead_days is not None
            else None
        ),
        "lead_time_days_used": (
            float(item.lead_time_days_used)
            if item.lead_time_days_used is not None
            else None
        ),
        "safety_stock_days": item.safety_stock_days,
        "safety_stock_qty": (
            float(item.safety_stock_qty)
            if item.safety_stock_qty is not None
            else None
        ),
        "reorder_point": float(item.reorder_point) if item.reorder_point is not None else None,
        "target_stock": item.target_stock,
        "recommended_order_qty": int(item.recommended_order_qty or 0),
        "decision_status": item.decision_status,
        "autopurchase_mode": item.autopurchase_mode,
        "missing_in_latest_pricelist": bool(item.missing_in_latest_pricelist),
        "reason_codes": list(item.reason_codes or []),
        "reason_titles": list(item.reason_titles or []),
        "reasons": list(item.reasons or []),
        "abc_xyz": dict(item.abc_xyz or {}) if item.abc_xyz else None,
        "best_supplier_by_price": (
            dict(item.best_supplier_by_price or {})
            if item.best_supplier_by_price
            else None
        ),
        "best_supplier_by_lead_time": (
            dict(item.best_supplier_by_lead_time or {})
            if item.best_supplier_by_lead_time
            else None
        ),
        "recommended_supplier": (
            dict(item.recommended_supplier or {})
            if item.recommended_supplier
            else None
        ),
        "draft_purchase_order": (
            dict(item.draft_purchase_order or {})
            if item.draft_purchase_order
            else None
        ),
        "top_site_offers": list(item.top_site_offers or []),
        "cross_group": (
            dict(item.cross_group or {}) if item.cross_group else None
        ),
        "sent_to_site_at": item.sent_to_site_at,
        "sent_order_id": item.sent_order_id,
        "sent_order_number": item.sent_order_number,
        "sent_customer_id": item.sent_customer_id,
        "send_result_snapshot": (
            dict(item.send_result_snapshot or {})
            if item.send_result_snapshot
            else {}
        ),
    }


def _extract_text_from_ai_message_content(content: Any) -> str:
    if isinstance(content, str):
        return content.strip()
    if isinstance(content, list):
        chunks: list[str] = []
        for item in content:
            if isinstance(item, dict):
                text = str(item.get("text", "")).strip()
                if text:
                    chunks.append(text)
            else:
                text = str(item or "").strip()
                if text:
                    chunks.append(text)
        return "\n".join(chunks).strip()
    return str(content or "").strip()


def _extract_json_object(text: str) -> Optional[dict[str, Any]]:
    payload_text = str(text or "").strip()
    if not payload_text:
        return None
    try:
        parsed = json.loads(payload_text)
        if isinstance(parsed, dict):
            return parsed
    except (TypeError, ValueError):
        pass
    start = payload_text.find("{")
    end = payload_text.rfind("}")
    if start < 0 or end <= start:
        return None
    candidate = payload_text[start:end + 1]
    try:
        parsed = json.loads(candidate)
    except (TypeError, ValueError):
        return None
    if isinstance(parsed, dict):
        return parsed
    return None


def _build_autopurchase_ai_fallback(
    *,
    run: AutoPurchaseRun,
    item: AutoPurchaseRunItem,
    warning_code: Optional[str] = None,
    warning_message: Optional[str] = None,
) -> dict[str, Any]:
    supplier = dict(item.recommended_supplier or {})
    supplier_name = str(supplier.get("provider_name") or "Dragonzap").strip()
    current_qty = int(item.current_quantity or 0)
    in_transit_qty = int(item.in_transit_qty or 0)
    recommended_qty = int(item.recommended_order_qty or 0)
    fill_rate = supplier.get("fill_rate")
    effective_lead_days = supplier.get("effective_lead_days")
    reasons = list(item.reason_titles or [])
    short_reasons = ", ".join(reasons[:3]) if reasons else "без дополнительных флагов"
    human_explanation = (
        f"Позиция {item.brand_name or '—'} {item.oem_number} попала в автозаказ, "
        f"потому что текущий остаток {current_qty} шт, в пути {in_transit_qty} шт, "
        f"а расчёт рекомендует заказать {recommended_qty} шт."
    )
    risk_parts = []
    if item.missing_in_latest_pricelist:
        risk_parts.append("позиция выпала из последнего нашего прайса")
    if fill_rate is None:
        risk_parts.append("по выбранному site-поставщику нет истории fill rate")
    elif float(fill_rate) < FILL_RATE_THRESHOLD_AUTO_APPROVE:
        risk_parts.append(
            f"fill rate поставщика ниже safe-порога ({fill_rate}%)"
        )
    if effective_lead_days is not None:
        risk_parts.append(f"ожидаемый срок {effective_lead_days} дн.")
    if not risk_parts:
        risk_parts.append("явных критичных рисков не найдено")
    manager_note = (
        f"Проверь строку в режиме {run.mode}. "
        f"Сайт рекомендует поставщика {supplier_name}. "
        f"Основные причины: {short_reasons}."
    )
    return {
        "run_id": int(run.id),
        "item_id": int(item.id),
        "model": AUTOPURCHASE_AI_MODEL,
        "generated_at": now_moscow(),
        "source": "fallback",
        "warning_code": warning_code,
        "warning_message": warning_message,
        "human_explanation": human_explanation,
        "risk_summary": "; ".join(risk_parts),
        "manager_note": manager_note,
        "supplier_message_draft": (
            f"Добрый день. Просим подтвердить наличие и срок поставки по позиции "
            f"{item.brand_name or ''} {item.oem_number} в количестве "
            f"{recommended_qty} шт."
        ).strip(),
        "confidence": 0.45,
        "requires_human_review": item.decision_status
        != AUTOPURCHASE_STATUS_AUTO_APPROVED,
    }


def _resolve_autopurchase_ai_warning(
    exc: Exception | None = None,
) -> tuple[str, str]:
    if not AUTOPURCHASE_AI_ENABLED:
        return (
            "ai_disabled",
            "AI-пояснения отключены в настройках сервера.",
        )
    if not AUTOPURCHASE_AI_API_KEY:
        return (
            "missing_api_key",
            "Не настроен OPENAI_API_KEY, поэтому AI-пояснение недоступно.",
        )
    if exc is None:
        return (
            "ai_unavailable",
            "AI-пояснение временно недоступно, показано резервное объяснение по правилам системы.",
        )

    if isinstance(exc, httpx.HTTPStatusError):
        status_code = int(exc.response.status_code)
        detail = ""
        try:
            payload = exc.response.json()
        except Exception:
            payload = None
        if isinstance(payload, dict):
            error_obj = payload.get("error")
            if isinstance(error_obj, dict):
                detail = str(
                    error_obj.get("code")
                    or error_obj.get("type")
                    or error_obj.get("message")
                    or ""
                ).strip()
            elif payload.get("detail"):
                detail = str(payload.get("detail") or "").strip()
        normalized = detail.lower()
        if status_code == 401:
            return (
                "invalid_api_key",
                "OpenAI отклонил ключ API. Проверь OPENAI_API_KEY.",
            )
        if status_code == 429 and (
            "insufficient_quota" in normalized
            or "billing" in normalized
            or "quota" in normalized
        ):
            return (
                "insufficient_quota",
                "OpenAI недоступен из-за квоты или оплаты. Проверь billing и лимиты API.",
            )
        if status_code == 429:
            return (
                "rate_limited",
                "OpenAI временно ограничил запросы. Попробуй повторить чуть позже.",
            )
        if 500 <= status_code <= 599:
            return (
                "provider_unavailable",
                "OpenAI временно недоступен. Показано резервное пояснение по правилам системы.",
            )
    if isinstance(exc, httpx.TimeoutException):
        return (
            "ai_timeout",
            "OpenAI не ответил вовремя. Показано резервное пояснение по правилам системы.",
        )
    if isinstance(exc, httpx.RequestError):
        return (
            "network_error",
            "Не удалось связаться с OpenAI. Проверь сеть или base URL API.",
        )
    return (
        "ai_unavailable",
        "AI-пояснение временно недоступно, показано резервное объяснение по правилам системы.",
    )


async def _generate_autopurchase_ai_payload(
    *,
    run: AutoPurchaseRun,
    item: AutoPurchaseRunItem,
) -> dict[str, Any]:
    if not AUTOPURCHASE_AI_ENABLED or not AUTOPURCHASE_AI_API_KEY:
        warning_code, warning_message = _resolve_autopurchase_ai_warning()
        return _build_autopurchase_ai_fallback(
            run=run,
            item=item,
            warning_code=warning_code,
            warning_message=warning_message,
        )

    fallback = _build_autopurchase_ai_fallback(run=run, item=item)

    supplier = dict(item.recommended_supplier or {})
    draft = dict(item.draft_purchase_order or {})
    user_payload = {
        "run": {
            "id": int(run.id),
            "mode": run.mode,
            "provider_name": (run.summary_snapshot or {}).get("provider_name"),
            "supplier_source": "site",
        },
        "item": {
            "id": int(item.id),
            "brand_name": item.brand_name,
            "oem_number": item.oem_number,
            "autopart_name": item.autopart_name,
            "current_quantity": int(item.current_quantity or 0),
            "in_transit_qty": int(item.in_transit_qty or 0),
            "sold_last_30_days": int(item.sold_last_30_days or 0),
            "sold_last_90_days": int(item.sold_last_90_days or 0),
            "estimated_days_left_30_days": item.estimated_days_left_30_days,
            "reorder_point": (
                float(item.reorder_point) if item.reorder_point is not None else None
            ),
            "target_stock": item.target_stock,
            "recommended_order_qty": int(item.recommended_order_qty or 0),
            "decision_status": item.decision_status,
            "missing_in_latest_pricelist": bool(item.missing_in_latest_pricelist),
            "abc_xyz": dict(item.abc_xyz or {}),
            "reason_titles": list(item.reason_titles or []),
            "reasons": list(item.reasons or []),
            "recommended_supplier": supplier,
            "draft_purchase_order": draft,
        },
    }
    system_prompt = (
        "Ты помогаешь менеджеру закупок понять строку автозаказа. "
        "Верни только JSON без markdown с полями: "
        "human_explanation, risk_summary, manager_note, "
        "supplier_message_draft, confidence, requires_human_review. "
        "Пиши по-русски, коротко и по делу. Не меняй математику расчёта, "
        "а объясняй уже готовое решение системы."
    )
    payload = {
        "model": AUTOPURCHASE_AI_MODEL,
        "temperature": 0.2,
        "messages": [
            {"role": "system", "content": system_prompt},
            {
                "role": "user",
                "content": json.dumps(user_payload, ensure_ascii=False),
            },
        ],
    }
    url = AUTOPURCHASE_AI_BASE_URL.rstrip("/") + "/chat/completions"
    try:
        async with httpx.AsyncClient(timeout=AUTOPURCHASE_AI_TIMEOUT_SEC) as client:
            response = await client.post(
                url,
                headers={
                    "Authorization": "Bearer " + AUTOPURCHASE_AI_API_KEY,
                    "Content-Type": "application/json",
                },
                json=payload,
            )
        response.raise_for_status()
        data = response.json()
        choices = data.get("choices") or []
        if not choices:
            return fallback
        content = _extract_text_from_ai_message_content(
            choices[0].get("message", {}).get("content")
        )
        parsed = _extract_json_object(content)
        if not parsed:
            return fallback
        return {
            "run_id": int(run.id),
            "item_id": int(item.id),
            "model": AUTOPURCHASE_AI_MODEL,
            "generated_at": now_moscow(),
            "source": "ai",
            "human_explanation": str(
                parsed.get("human_explanation") or fallback["human_explanation"]
            )[:4000],
            "risk_summary": str(
                parsed.get("risk_summary") or fallback["risk_summary"]
            )[:4000],
            "manager_note": str(
                parsed.get("manager_note") or fallback["manager_note"]
            )[:4000],
            "supplier_message_draft": str(
                parsed.get("supplier_message_draft")
                or fallback["supplier_message_draft"]
            )[:4000],
            "confidence": min(
                max(float(parsed.get("confidence") or fallback["confidence"]), 0.0),
                1.0,
            ),
            "requires_human_review": bool(
                parsed.get(
                    "requires_human_review",
                    fallback["requires_human_review"],
                )
            ),
        }
    except Exception as exc:
        warning_code, warning_message = _resolve_autopurchase_ai_warning(exc)
        logger.warning(
            "Autopurchase AI explanation failed run_id=%s item_id=%s: %s",
            run.id,
            item.id,
            exc,
        )
        return _build_autopurchase_ai_fallback(
            run=run,
            item=item,
            warning_code=warning_code,
            warning_message=warning_message,
        )


def _build_autopurchase_group_ai_fallback(
    *,
    run: AutoPurchaseRun,
    group: dict[str, Any],
    warning_code: Optional[str] = None,
    warning_message: Optional[str] = None,
) -> dict[str, Any]:
    items = list(group.get("items") or [])
    provider_name = str(group.get("provider_name") or "Dragonzap").strip()
    total_items = int(group.get("total_items") or 0)
    total_quantity = int(group.get("total_quantity") or 0)
    total_sum = _quantize_float(group.get("total_sum"))
    partial_count = sum(
        1 for item in items if int(item.get("remaining_gap_qty") or 0) > 0
    )
    top_positions = ", ".join(
        f"{item.get('brand_name') or '—'} {item.get('oem_number')}"
        for item in items[:3]
    ) or "без расшифровки позиций"
    human_explanation = (
        f"Группа поставщика {provider_name} содержит {total_items} поз. "
        f"к заказу на {total_quantity} шт"
        f"{f' и сумму {total_sum:.2f} руб.' if total_sum is not None else '.'}"
    )
    risk_parts = []
    if partial_count > 0:
        risk_parts.append(
            f"по {partial_count} поз. сайт даёт только частичное количество"
        )
    if not risk_parts:
        risk_parts.append("явных критичных ограничений по группе не видно")
    manager_note = (
        f"Run #{run.id}, поставщик {provider_name}. "
        f"Проверь приоритетные позиции: {top_positions}."
    )
    supplier_message_draft = (
        f"Добрый день. Просим подтвердить заказ по группе автозаказа run #{run.id}. "
        f"Поставщик: {provider_name}. Всего позиций: {total_items}, "
        f"общее количество: {total_quantity} шт."
    )
    return {
        "run_id": int(run.id),
        "supplier_key": str(group.get("supplier_key") or ""),
        "provider_name": provider_name,
        "total_items": total_items,
        "total_quantity": total_quantity,
        "total_sum": total_sum,
        "model": AUTOPURCHASE_AI_MODEL,
        "generated_at": now_moscow(),
        "source": "fallback",
        "warning_code": warning_code,
        "warning_message": warning_message,
        "human_explanation": human_explanation,
        "risk_summary": "; ".join(risk_parts),
        "manager_note": manager_note,
        "supplier_message_draft": supplier_message_draft,
        "confidence": 0.45,
        "requires_human_review": partial_count > 0,
    }


async def _generate_autopurchase_group_ai_payload(
    *,
    run: AutoPurchaseRun,
    group: dict[str, Any],
) -> dict[str, Any]:
    if not AUTOPURCHASE_AI_ENABLED or not AUTOPURCHASE_AI_API_KEY:
        warning_code, warning_message = _resolve_autopurchase_ai_warning()
        return _build_autopurchase_group_ai_fallback(
            run=run,
            group=group,
            warning_code=warning_code,
            warning_message=warning_message,
        )

    fallback = _build_autopurchase_group_ai_fallback(run=run, group=group)

    items = list(group.get("items") or [])
    user_payload = {
        "run": {
            "id": int(run.id),
            "mode": run.mode,
            "provider_name": (run.summary_snapshot or {}).get("provider_name"),
            "supplier_source": "site",
        },
        "group": {
            "supplier_key": str(group.get("supplier_key") or ""),
            "provider_name": str(group.get("provider_name") or ""),
            "provider_config_name": group.get("provider_config_name"),
            "total_items": int(group.get("total_items") or 0),
            "total_quantity": int(group.get("total_quantity") or 0),
            "total_sum": _quantize_float(group.get("total_sum")),
            "items": [
                {
                    "item_id": int(group_item.get("item_id") or 0),
                    "brand_name": group_item.get("brand_name"),
                    "oem_number": group_item.get("oem_number"),
                    "autopart_name": group_item.get("autopart_name"),
                    "recommended_order_qty": int(
                        group_item.get("recommended_order_qty") or 0
                    ),
                    "proposed_order_qty": int(
                        group_item.get("proposed_order_qty") or 0
                    ),
                    "remaining_gap_qty": int(
                        group_item.get("remaining_gap_qty") or 0
                    ),
                    "price": group_item.get("price"),
                    "line_total": group_item.get("line_total"),
                    "reason": group_item.get("reason"),
                }
                for group_item in items[:15]
            ],
        },
    }
    system_prompt = (
        "Ты помогаешь менеджеру закупок понять группу строк автозаказа "
        "по одному поставщику. Верни только JSON без markdown с полями: "
        "human_explanation, risk_summary, manager_note, "
        "supplier_message_draft, confidence, requires_human_review. "
        "Пиши по-русски, коротко и по делу. Объясняй уже готовую группу "
        "к отправке на сайт и не меняй сам расчёт."
    )
    payload = {
        "model": AUTOPURCHASE_AI_MODEL,
        "temperature": 0.2,
        "messages": [
            {"role": "system", "content": system_prompt},
            {
                "role": "user",
                "content": json.dumps(user_payload, ensure_ascii=False),
            },
        ],
    }
    url = AUTOPURCHASE_AI_BASE_URL.rstrip("/") + "/chat/completions"
    try:
        async with httpx.AsyncClient(timeout=AUTOPURCHASE_AI_TIMEOUT_SEC) as client:
            response = await client.post(
                url,
                headers={
                    "Authorization": "Bearer " + AUTOPURCHASE_AI_API_KEY,
                    "Content-Type": "application/json",
                },
                json=payload,
            )
        response.raise_for_status()
        data = response.json()
        choices = data.get("choices") or []
        if not choices:
            return fallback
        content = _extract_text_from_ai_message_content(
            choices[0].get("message", {}).get("content")
        )
        parsed = _extract_json_object(content)
        if not parsed:
            return fallback
        return {
            "run_id": int(run.id),
            "supplier_key": str(group.get("supplier_key") or ""),
            "provider_name": str(group.get("provider_name") or ""),
            "total_items": int(group.get("total_items") or 0),
            "total_quantity": int(group.get("total_quantity") or 0),
            "total_sum": _quantize_float(group.get("total_sum")),
            "model": AUTOPURCHASE_AI_MODEL,
            "generated_at": now_moscow(),
            "source": "ai",
            "human_explanation": str(
                parsed.get("human_explanation") or fallback["human_explanation"]
            )[:4000],
            "risk_summary": str(
                parsed.get("risk_summary") or fallback["risk_summary"]
            )[:4000],
            "manager_note": str(
                parsed.get("manager_note") or fallback["manager_note"]
            )[:4000],
            "supplier_message_draft": str(
                parsed.get("supplier_message_draft")
                or fallback["supplier_message_draft"]
            )[:4000],
            "confidence": min(
                max(float(parsed.get("confidence") or fallback["confidence"]), 0.0),
                1.0,
            ),
            "requires_human_review": bool(
                parsed.get(
                    "requires_human_review",
                    fallback["requires_human_review"],
                )
            ),
        }
    except Exception as exc:
        warning_code, warning_message = _resolve_autopurchase_ai_warning(exc)
        logger.warning(
            "Autopurchase AI group explanation failed run_id=%s supplier_key=%s: %s",
            run.id,
            group.get("supplier_key"),
            exc,
        )
        return _build_autopurchase_group_ai_fallback(
            run=run,
            group=group,
            warning_code=warning_code,
            warning_message=warning_message,
        )


def _strip_manual_override_reasons(
    *,
    reason_codes: list[str] | None,
    reason_titles: list[str] | None,
    reasons: list[dict[str, Any]] | None,
) -> tuple[list[str], list[str], list[dict[str, Any]]]:
    filtered_reasons = [
        dict(reason)
        for reason in (reasons or [])
        if str(reason.get("code") or "") not in AUTOPURCHASE_MANUAL_REASON_CODES
    ]
    filtered_codes = [
        code
        for code in (reason_codes or [])
        if str(code) not in AUTOPURCHASE_MANUAL_REASON_CODES
    ]
    filtered_titles = [
        title
        for title in (reason_titles or [])
        if title
        and title
        not in {
            "Менеджер подтвердил вручную",
            "Менеджер вернул на ручную проверку",
            "Менеджер заблокировал строку",
        }
    ]
    return filtered_codes, filtered_titles, filtered_reasons


def _build_manual_override_reason(
    *,
    decision_status: str,
    comment: Optional[str],
) -> dict[str, str]:
    normalized_status = str(decision_status or "").strip().lower()
    if normalized_status == AUTOPURCHASE_STATUS_AUTO_APPROVED:
        return _build_reason(
            code="autopurchase_manual_auto_approved",
            severity="info",
            title="Менеджер подтвердил вручную",
            description=(
                f"Строка вручную переведена в автоутверждённые."
                f"{f' Комментарий: {comment}' if comment else ''}"
            ),
        )
    if normalized_status == AUTOPURCHASE_STATUS_BLOCKED:
        return _build_reason(
            code="autopurchase_manual_blocked",
            severity="warning",
            title="Менеджер заблокировал строку",
            description=(
                "Строка снята с автозаказа вручную."
                f"{f' Комментарий: {comment}' if comment else ''}"
            ),
        )
    return _build_reason(
        code="autopurchase_manual_needs_review",
        severity="info",
        title="Менеджер вернул на ручную проверку",
        description=(
            "Строка требует ручного решения менеджера."
            f"{f' Комментарий: {comment}' if comment else ''}"
        ),
    )


def _refresh_run_summary_snapshot(
    run: AutoPurchaseRun,
    items: list[AutoPurchaseRunItem],
) -> None:
    summary_snapshot = dict(run.summary_snapshot or {})
    summary_snapshot["total_items"] = len(items)
    summary_snapshot["auto_approved_count"] = sum(
        1
        for item in items
        if item.decision_status == AUTOPURCHASE_STATUS_AUTO_APPROVED
    )
    summary_snapshot["needs_review_count"] = sum(
        1
        for item in items
        if item.decision_status == AUTOPURCHASE_STATUS_NEEDS_REVIEW
    )
    summary_snapshot["blocked_count"] = sum(
        1
        for item in items
        if item.decision_status == AUTOPURCHASE_STATUS_BLOCKED
    )
    summary_snapshot["sent_count"] = sum(
        1 for item in items if item.sent_to_site_at is not None
    )
    run.summary_snapshot = summary_snapshot


def _validate_autopurchase_decision_status(decision_status: str) -> str:
    normalized_status = str(decision_status or "").strip().lower()
    if normalized_status not in {
        AUTOPURCHASE_STATUS_AUTO_APPROVED,
        AUTOPURCHASE_STATUS_NEEDS_REVIEW,
        AUTOPURCHASE_STATUS_BLOCKED,
    }:
        raise ValueError("Неизвестный статус строки автозаказа")
    return normalized_status


def _apply_autopurchase_item_status_override(
    item: AutoPurchaseRunItem,
    *,
    decision_status: str,
    comment: Optional[str] = None,
) -> None:
    normalized_status = _validate_autopurchase_decision_status(decision_status)
    recommended_supplier = dict(item.recommended_supplier or {})
    draft_purchase_order = dict(item.draft_purchase_order or {})
    if normalized_status == AUTOPURCHASE_STATUS_AUTO_APPROVED:
        if not recommended_supplier.get("provider_name"):
            raise ValueError(
                "Нельзя подтвердить строку без найденного site-поставщика"
            )
        if not draft_purchase_order:
            raise ValueError(
                "Нельзя подтвердить строку без подготовленного черновика заказа"
            )

    reason_codes, reason_titles, reasons = _strip_manual_override_reasons(
        reason_codes=list(item.reason_codes or []),
        reason_titles=list(item.reason_titles or []),
        reasons=list(item.reasons or []),
    )
    manual_reason = _build_manual_override_reason(
        decision_status=normalized_status,
        comment=comment,
    )
    reason_codes.append(manual_reason["code"])
    reason_titles.append(manual_reason["title"])
    reasons.append(manual_reason)

    item.decision_status = normalized_status
    item.reason_codes = reason_codes
    item.reason_titles = reason_titles
    item.reasons = _limit_reasons(reasons)


def _normalize_site_supplier_id(value: Any) -> Optional[int]:
    try:
        if value is None or str(value).strip() == "":
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


async def _resolve_existing_site_provider_id(
    session: AsyncSession,
    *,
    supplier_id: Optional[int],
    supplier_name: Optional[str],
    provider_cache: dict[str, int],
) -> Optional[int]:
    normalized_supplier_name = str(supplier_name or "").strip()
    id_cache_key = (
        f"DRAGONZAP:id:{int(supplier_id)}"
        if supplier_id is not None
        else None
    )
    if id_cache_key:
        cached_provider_id = provider_cache.get(id_cache_key)
        if cached_provider_id is not None:
            return cached_provider_id
        reference = await crud_provider.get_external_reference_by_source_supplier(
            source_system="DRAGONZAP",
            external_supplier_id=int(supplier_id),
            session=session,
        )
        if reference is not None and reference.is_active:
            provider_cache[id_cache_key] = int(reference.provider_id)
            if normalized_supplier_name:
                provider_cache[
                    f"DRAGONZAP:name:{normalized_supplier_name.casefold()}"
                ] = int(reference.provider_id)
            return int(reference.provider_id)

    if normalized_supplier_name:
        name_cache_key = f"DRAGONZAP:name:{normalized_supplier_name.casefold()}"
        cached_provider_id = provider_cache.get(name_cache_key)
        if cached_provider_id is not None:
            return cached_provider_id
        provider = await crud_provider.get_provider_or_none(
            normalized_supplier_name,
            session,
        )
        if provider is not None:
            provider_cache[name_cache_key] = int(provider.id)
            if id_cache_key:
                provider_cache[id_cache_key] = int(provider.id)
            return int(provider.id)

    return None


def _get_site_history_provider_key(
    *,
    provider_id: Optional[int],
    provider_name: Optional[str],
) -> Optional[str]:
    if provider_id is not None:
        return f"id:{int(provider_id)}"
    normalized_provider_name = str(provider_name or "").strip().casefold()
    if normalized_provider_name:
        return f"name:{normalized_provider_name}"
    return None


def _build_site_history_stats_by_provider(
    history_rows: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    provider_rows: dict[str, list[dict[str, Any]]] = {}
    for row in history_rows:
        if str(row.get("source_type") or "").strip().lower() != "site":
            continue
        provider_key = _get_site_history_provider_key(
            provider_id=row.get("provider_id"),
            provider_name=row.get("provider_name"),
        )
        if not provider_key:
            continue
        provider_rows.setdefault(provider_key, []).append(row)

    result: dict[str, dict[str, Any]] = {}
    for provider_key, rows in provider_rows.items():
        total_ordered = sum(int(row.get("ordered_quantity") or 0) for row in rows)
        total_received = sum(int(row.get("received_quantity") or 0) for row in rows)
        fill_rate = (
            _round_stat(total_received / total_ordered * 100, 1)
            if total_ordered > 0
            else None
        )
        lead_values = [
            int(row["actual_lead_days"])
            for row in rows
            if row.get("actual_lead_days") is not None
        ]
        avg_lead_days = (
            _round_stat(sum(lead_values) / len(lead_values), 1)
            if lead_values
            else None
        )
        price_values = [
            float(price_value)
            for price_value in (_to_decimal(row.get("price")) for row in rows)
            if price_value is not None
        ]
        avg_price = (
            _round_stat(sum(price_values) / len(price_values), 2)
            if price_values
            else None
        )
        last_ordered_at = max(
            (
                row.get("created_at")
                for row in rows
                if row.get("created_at") is not None
            ),
            default=None,
        )
        result[provider_key] = {
            "order_count": len(rows),
            "fill_rate": fill_rate,
            "avg_lead_days": avg_lead_days,
            "avg_price": avg_price,
            "last_ordered_at": last_ordered_at,
        }
    return result


def _build_site_supplier_stat(
    raw: dict[str, Any],
    *,
    provider_id: Optional[int] = None,
    history_stat: Optional[dict[str, Any]] = None,
) -> Optional[dict[str, Any]]:
    try:
        price = float(raw.get("cost"))
        quantity = int(raw.get("qnt") or 0)
        min_delivery = int(raw.get("min_delivery_day") or 1)
        max_delivery = int(raw.get("max_delivery_day") or min_delivery)
    except (TypeError, ValueError):
        return None

    provider_name = str(raw.get("sup_logo") or "").strip() or "Dragonzap"
    price_name = str(raw.get("price_name") or "").strip() or None
    effective_lead = _round_stat((min_delivery + max_delivery) / 2, 1)
    external_supplier_id = _normalize_site_supplier_id(raw.get("supplier_id"))
    return {
        "provider_id": provider_id,
        "provider_name": provider_name,
        "external_supplier_id": external_supplier_id,
        "order_count": int((history_stat or {}).get("order_count") or 0),
        "fill_rate": (history_stat or {}).get("fill_rate"),
        "avg_lead_days": (history_stat or {}).get("avg_lead_days"),
        "effective_lead_days": (
            (history_stat or {}).get("avg_lead_days")
            if (history_stat or {}).get("avg_lead_days") is not None
            else effective_lead
        ),
        "avg_price": (history_stat or {}).get("avg_price"),
        "last_ordered_at": (history_stat or {}).get("last_ordered_at"),
        "current_price": price,
        "current_qty": quantity,
        "current_min_delivery": min_delivery,
        "current_max_delivery": max_delivery,
        "current_oem_number": _normalize_oem(raw.get("oem")) or "",
        "current_brand_name": raw.get("make_name"),
        "current_autopart_name": raw.get("detail_name"),
        "current_autopart_id": None,
        "current_provider_config_id": None,
        "current_provider_config_name": price_name,
        "source_type": "site",
        "sup_logo": str(raw.get("sup_logo") or "").strip() or None,
        "hash_key": raw.get("hash_key"),
        "system_hash": raw.get("system_hash"),
        "current_min_qnt": int(raw.get("min_qnt") or 1),
        "is_own_price": False,
        "score": None,
    }


def _select_best_site_supplier_by_price(
    supplier_stats: list[dict[str, Any]],
) -> Optional[dict[str, Any]]:
    candidates = [
        item
        for item in supplier_stats
        if item.get("current_price") is not None
        and int(item.get("current_qty") or 0) > 0
    ]
    if not candidates:
        return None
    return min(
        candidates,
        key=lambda item: (
            float(item.get("current_price") or 9_999_999),
            float(item.get("effective_lead_days") or 9_999),
            -int(item.get("current_qty") or 0),
        ),
    )


def _select_best_site_supplier_by_lead_time(
    supplier_stats: list[dict[str, Any]],
) -> Optional[dict[str, Any]]:
    candidates = [
        item
        for item in supplier_stats
        if item.get("current_price") is not None
        and item.get("effective_lead_days") is not None
        and int(item.get("current_qty") or 0) > 0
    ]
    if not candidates:
        return None
    return min(
        candidates,
        key=lambda item: (
            float(item.get("effective_lead_days") or 9_999),
            float(item.get("current_price") or 9_999_999),
            -int(item.get("current_qty") or 0),
        ),
    )


def _normalize_brand_key(value: Any) -> str:
    return "".join(ch for ch in str(value or "").upper() if ch.isalnum())


def _is_dragonzap_brand(brand_name: Optional[str]) -> bool:
    return _normalize_brand_key(brand_name) == DRAGONZAP_BRAND_NAME


def _brand_matches_allowed(
    make_name: Any,
    allowed_brand_keys: set[str],
) -> bool:
    make_key = _normalize_brand_key(make_name)
    if not make_key or not allowed_brand_keys:
        # Сайт не вернул бренд предложения — доверяем серверной
        # фильтрации по make_name в самом запросе get_offers.
        return True
    return any(
        make_key == allowed_key
        or make_key in allowed_key
        or allowed_key in make_key
        for allowed_key in allowed_brand_keys
        if allowed_key
    )


def _get_cross_brand_priority(brand_name: str) -> int:
    normalized = _normalize_brand_key(brand_name)
    for index, preferred in enumerate(DRAGONZAP_PREFERRED_CROSS_BRANDS):
        preferred_key = _normalize_brand_key(preferred)
        if preferred_key and (
            normalized == preferred_key
            or preferred_key in normalized
            or normalized in preferred_key
        ):
            return index
    return len(DRAGONZAP_PREFERRED_CROSS_BRANDS)


async def _load_dragonzap_cross_targets(
    session: AsyncSession,
    *,
    autopart_id: int,
    limit: int = AUTOPURCHASE_MAX_CROSS_QUERIES,
) -> list[dict[str, Any]]:
    """Кроссы Dragonzap-позиции из нашей системы для поиска на сайте.

    Dragonzap производится из деталей других брендов, поэтому замену ищем
    по кроссам (AutoPartCross), отдавая приоритет брендам
    DRAGONZAP_PREFERRED_CROSS_BRANDS. Явно ошибочные кроссы
    (AutoPartInvalidCross) исключаются.
    """
    cross_stmt = (
        select(
            AutoPartCross.cross_oem_number,
            AutoPartCross.cross_brand_id,
            AutoPartCross.priority,
            Brand.name.label("cross_brand_name"),
        )
        .join(Brand, Brand.id == AutoPartCross.cross_brand_id)
        .where(AutoPartCross.source_autopart_id == autopart_id)
    )
    cross_rows = (await session.execute(cross_stmt)).all()
    if not cross_rows:
        return []

    invalid_stmt = select(
        AutoPartInvalidCross.invalid_brand_id,
        AutoPartInvalidCross.invalid_oem_number,
    ).where(AutoPartInvalidCross.source_autopart_id == autopart_id)
    invalid_pairs = {
        (row.invalid_brand_id, _normalize_oem(row.invalid_oem_number))
        for row in (await session.execute(invalid_stmt)).all()
    }

    targets: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for row in cross_rows:
        brand_name = str(row.cross_brand_name or "").strip()
        oem_number = _normalize_oem(row.cross_oem_number) or ""
        if not brand_name or not oem_number:
            continue
        if _is_dragonzap_brand(brand_name):
            continue
        if (row.cross_brand_id, oem_number) in invalid_pairs:
            continue
        dedupe_key = (_normalize_brand_key(brand_name), oem_number)
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        targets.append(
            {
                "oem_number": oem_number,
                "brand_name": brand_name,
                "brand_priority": _get_cross_brand_priority(brand_name),
                "cross_priority": int(row.priority or 100),
            }
        )

    targets.sort(
        key=lambda item: (
            item["brand_priority"],
            item["cross_priority"],
            item["brand_name"],
            item["oem_number"],
        )
    )
    return targets[: max(int(limit), 1)]


def _sum_active_outstanding_qty(history_rows: list[dict[str, Any]]) -> int:
    return max(
        sum(
            max(
                int(row.get("ordered_quantity") or 0)
                - int(row.get("received_quantity") or 0),
                0,
            )
            for row in history_rows
            if row.get("current_status") in _ACTIVE_ORDER_STATUSES
        ),
        0,
    )


async def _load_dragonzap_cross_stock_map(
    session: AsyncSession,
    *,
    latest_known_rows_by_oem: dict[str, dict[str, Any]],
    latest_rows_by_oem: dict[str, dict[str, Any]],
    history_by_oem: dict[str, list[dict[str, Any]]],
) -> dict[str, dict[str, Any]]:
    """Сводное наличие Dragonzap-позиции по кроссам бренда Dragonzap.

    Пример: DZ123 (3 шт) имеет кросс DZ122 (3 шт) → доступно 6 шт.
    Кроссы других брендов (CHERY/HAVAL/...) сюда не входят — по ним
    идёт поиск закупки на сайте.
    """
    dz_autopart_to_oem: dict[int, str] = {}
    for oem, known_row in latest_known_rows_by_oem.items():
        if not _is_dragonzap_brand(known_row.get("brand_name")):
            continue
        autopart_id = known_row.get("autopart_id")
        if autopart_id is not None:
            dz_autopart_to_oem[int(autopart_id)] = oem
    if not dz_autopart_to_oem:
        return {}

    dz_ids = sorted(dz_autopart_to_oem.keys())
    related_oems_by_oem: dict[str, set[str]] = {}

    def _link(source_oem: Optional[str], related_oem: Optional[str]) -> None:
        if not source_oem or not related_oem or source_oem == related_oem:
            return
        related_oems_by_oem.setdefault(source_oem, set()).add(related_oem)
        related_oems_by_oem.setdefault(related_oem, set()).add(source_oem)

    # Кроссы, где наша Dragonzap-позиция — источник, а кросс тоже Dragonzap.
    forward_stmt = (
        select(
            AutoPartCross.source_autopart_id,
            AutoPartCross.cross_oem_number,
        )
        .join(Brand, Brand.id == AutoPartCross.cross_brand_id)
        .where(
            AutoPartCross.source_autopart_id.in_(dz_ids),
            func.upper(Brand.name) == DRAGONZAP_BRAND_NAME,
        )
    )
    for source_autopart_id, cross_oem_raw in (
        await session.execute(forward_stmt)
    ).all():
        _link(
            dz_autopart_to_oem.get(int(source_autopart_id)),
            _normalize_oem(cross_oem_raw),
        )

    # Обратное направление: наша позиция указана как кросс у другой
    # Dragonzap-позиции.
    reverse_stmt = (
        select(
            AutoPartCross.cross_autopart_id,
            AutoPart.oem_number,
        )
        .join(AutoPart, AutoPart.id == AutoPartCross.source_autopart_id)
        .join(Brand, Brand.id == AutoPart.brand_id)
        .where(
            AutoPartCross.cross_autopart_id.in_(dz_ids),
            func.upper(Brand.name) == DRAGONZAP_BRAND_NAME,
        )
    )
    for cross_autopart_id, source_oem_raw in (
        await session.execute(reverse_stmt)
    ).all():
        _link(
            dz_autopart_to_oem.get(int(cross_autopart_id)),
            _normalize_oem(source_oem_raw),
        )

    result: dict[str, dict[str, Any]] = {}
    for oem, related_oems in related_oems_by_oem.items():
        if oem not in latest_known_rows_by_oem:
            continue
        items: list[dict[str, Any]] = []
        total_quantity = 0
        total_in_transit = 0
        for related_oem in sorted(related_oems):
            related_row = latest_rows_by_oem.get(related_oem)
            related_known = latest_known_rows_by_oem.get(related_oem)
            if related_row is None and related_known is None:
                # Кросс не из нашего прайса — наличие неизвестно.
                continue
            related_qty = int((related_row or {}).get("current_quantity") or 0)
            related_in_transit = _sum_active_outstanding_qty(
                history_by_oem.get(related_oem, [])
            )
            items.append(
                {
                    "oem_number": related_oem,
                    "brand_name": DRAGONZAP_BRAND_NAME,
                    "quantity": related_qty,
                    "in_transit_qty": related_in_transit,
                    "autopart_name": (
                        (related_row or related_known or {}).get(
                            "autopart_name"
                        )
                    ),
                }
            )
            total_quantity += related_qty
            total_in_transit += related_in_transit
        if items:
            result[oem] = {
                "items": items,
                "cross_quantity": total_quantity,
                "cross_in_transit_qty": total_in_transit,
            }
    return result


async def _load_last_receipt_price_by_autopart(
    session: AsyncSession,
    autopart_ids: list[int],
    *,
    receipts_limit: int = 3,
) -> dict[int, dict[str, Any]]:
    """Ориентир закупочной цены из документов прихода.

    Средневзвешенная по количеству цена последних 2–3 поступлений:
    одна аномальная поставка не должна сбивать ориентир.
    """
    result: dict[int, dict[str, Any]] = {}
    normalized_ids = sorted(
        {int(item) for item in autopart_ids if item is not None}
    )
    if not normalized_ids:
        return result

    stmt = (
        select(
            SupplierReceiptItem.autopart_id,
            SupplierReceiptItem.price,
            SupplierReceiptItem.received_quantity,
            SupplierReceipt.document_date,
            SupplierReceipt.created_at,
        )
        .join(
            SupplierReceipt,
            SupplierReceipt.id == SupplierReceiptItem.receipt_id,
        )
        .where(
            SupplierReceiptItem.autopart_id.in_(normalized_ids),
            SupplierReceiptItem.price.is_not(None),
            SupplierReceiptItem.price > 0,
        )
        .order_by(
            SupplierReceiptItem.autopart_id.asc(),
            SupplierReceipt.document_date.desc().nulls_last(),
            SupplierReceipt.created_at.desc().nulls_last(),
            SupplierReceiptItem.id.desc(),
        )
    )
    rows_by_autopart: dict[int, list[Any]] = {}
    for row in (await session.execute(stmt)).all():
        autopart_id = int(row.autopart_id)
        bucket = rows_by_autopart.setdefault(autopart_id, [])
        if len(bucket) < max(int(receipts_limit), 1):
            bucket.append(row)

    for autopart_id, rows in rows_by_autopart.items():
        weighted_sum = 0.0
        weight_total = 0.0
        for row in rows:
            weight = max(float(row.received_quantity or 0), 1.0)
            weighted_sum += float(row.price) * weight
            weight_total += weight
        result[autopart_id] = {
            "price": _quantize_float(weighted_sum / weight_total),
            "document_date": rows[0].document_date,
        }
    return result


async def _fetch_site_supplier_stats_for_oem(
    session: AsyncSession,
    *,
    oem_number: str,
    brand_name: Optional[str],
    history_rows: Optional[list[dict[str, Any]]] = None,
    autopart_id: Optional[int] = None,
) -> tuple[list[dict[str, Any]], list[str], bool, int, int]:
    if not SITE_API_KEY:
        return [], [], False, 0, 0

    query_brands = await expand_site_query_brands(session, brand_name)
    if not query_brands:
        return [], [], False, 0, 0

    # Бизнес-правило подбора бренда:
    # - Dragonzap мы производим из других брендов, поэтому замену ищем
    #   через кроссы нашей системы (приоритет — CHERY/HAVAL/GEELY/LIFAN/
    #   JAC/CHANGAN), на сайте запрашиваем кроссы и берём самый дешёвый
    #   вариант; синонимы бренда и fallback по брендам сайта — запасные пути;
    # - все остальные бренды заказываем строго тем брендом, который был
    #   в нашем прайсе — без подмены на аналоги других производителей.
    requested_is_dragonzap = _is_dragonzap_brand(brand_name)

    cross_targets: list[dict[str, Any]] = []
    if requested_is_dragonzap and autopart_id is not None:
        try:
            cross_targets = await _load_dragonzap_cross_targets(
                session,
                autopart_id=int(autopart_id),
            )
        except Exception as exc:
            logger.warning(
                "Не удалось загрузить кроссы Dragonzap для autopart_id=%s: %s",
                autopart_id,
                exc,
            )
            cross_targets = []

    used_fallback_brand = False
    searched_queries: list[str] = []
    offers_by_brand: list[list[dict[str, Any]]] = []
    provider_cache: dict[str, int] = {}
    site_history_stats_by_provider = _build_site_history_stats_by_provider(
        list(history_rows or [])
    )

    async with DZSiteClient(
        base_url=URL_DZ_SEARCH,
        api_key=SITE_API_KEY,
        verify_ssl=False,
    ) as client:
        # 1) Кроссы из нашей системы (только Dragonzap): ищем по номеру
        #    кросса под его брендом, разрешая сайту добавлять свои кроссы.
        for cross_target in cross_targets:
            cross_offers = await _get_site_offers_cached(
                client,
                oem=cross_target["oem_number"],
                brand=cross_target["brand_name"],
                without_cross=False,
            )
            searched_queries.append(
                f"{cross_target['brand_name']} {cross_target['oem_number']}"
            )
            if not cross_offers:
                continue
            for item in cross_offers:
                item.setdefault("query_brand", cross_target["brand_name"])
            offers_by_brand.append(cross_offers)

        # 2) Прямой поиск по исходному номеру (для Dragonzap — по всем
        #    синонимам бренда, для остальных — строго по бренду из прайса).
        for query_brand in query_brands:
            direct_offers = await _get_site_offers_cached(
                client,
                oem=oem_number,
                brand=query_brand,
                without_cross=True,
            )
            searched_queries.append(f"{query_brand} {oem_number}")
            if not direct_offers:
                continue
            for item in direct_offers:
                item.setdefault("query_brand", query_brand)
            offers_by_brand.append(direct_offers)

        if not offers_by_brand and requested_is_dragonzap:
            (
                fallback_offers,
                _site_brand_candidates,
                fallback_brand,
            ) = await resolve_fallback_site_brand(
                client,
                oem=oem_number,
                exclude_brands=query_brands,
                without_cross=True,
            )
            if fallback_brand:
                offers_by_brand = fallback_offers
                query_brands = [fallback_brand]
                searched_queries.append(f"{fallback_brand} {oem_number}")
                used_fallback_brand = True

    merged = merge_site_offers(offers_by_brand)
    # Для Dragonzap допустимы любые бренды из кроссов (своих и сайта),
    # поэтому фильтр по бренду применяем только к строгим брендам.
    allowed_brand_keys = (
        set()
        if requested_is_dragonzap
        else {_normalize_brand_key(brand) for brand in query_brands}
    )
    filtered_other_brand_count = 0
    supplier_stats: list[dict[str, Any]] = []
    for raw in merged:
        if not _brand_matches_allowed(raw.get("make_name"), allowed_brand_keys):
            filtered_other_brand_count += 1
            logger.debug(
                "Отфильтровано предложение чужого бренда %s для OEM=%s "
                "(разрешённые бренды: %s)",
                raw.get("make_name"),
                oem_number,
                ", ".join(query_brands),
            )
            continue
        provider_name = str(raw.get("sup_logo") or "").strip() or "Dragonzap"
        external_supplier_id = _normalize_site_supplier_id(raw.get("supplier_id"))
        provider_id = await _resolve_existing_site_provider_id(
            session,
            supplier_id=external_supplier_id,
            supplier_name=provider_name,
            provider_cache=provider_cache,
        )
        history_stat = site_history_stats_by_provider.get(
            _get_site_history_provider_key(
                provider_id=provider_id,
                provider_name=provider_name,
            )
            or ""
        )
        site_stat = _build_site_supplier_stat(
            raw,
            provider_id=provider_id,
            history_stat=history_stat,
        )
        if site_stat:
            supplier_stats.append(site_stat)

    unique_searched_queries: list[str] = []
    seen_queries: set[str] = set()
    for query_label in searched_queries:
        if query_label in seen_queries:
            continue
        seen_queries.add(query_label)
        unique_searched_queries.append(query_label)

    return (
        supplier_stats,
        unique_searched_queries,
        used_fallback_brand,
        filtered_other_brand_count,
        len(cross_targets),
    )


def _plan_auto_allocations(
    offers: list[dict[str, Any]],
    *,
    needed_qty: int,
    max_allowed_price: Optional[float] = None,
) -> tuple[list[dict[str, Any]], int]:
    """Жадно закрывает потребность по самым дешёвым предложениям.

    Каждое предложение заказывается только целыми партиями (min_qnt —
    кратность, округление вверх). Возвращает (распределения, закрыто шт).
    """
    if needed_qty <= 0:
        return [], 0

    allocations: list[dict[str, Any]] = []
    remaining = int(needed_qty)
    for offer in offers:
        if remaining <= 0:
            break
        price = offer.get("current_price")
        if price is None or offer.get("is_own_price"):
            continue
        if (
            max_allowed_price is not None
            and float(price) > float(max_allowed_price)
        ):
            continue
        lot = max(int(offer.get("current_min_qnt") or 1), 1)
        orderable_cap = _round_down_to_lot(
            int(offer.get("current_qty") or 0),
            lot,
        )
        if orderable_cap <= 0:
            continue
        take_qty = min(
            _round_up_to_multiplicity(remaining, lot),
            orderable_cap,
        )
        if take_qty <= 0:
            continue
        allocations.append(_build_allocation_from_offer(offer, take_qty))
        remaining -= take_qty

    covered_qty = sum(
        int(allocation["quantity"]) for allocation in allocations
    )
    return allocations, covered_qty


def _select_autopurchase_supplier(
    supplier_stats: list[dict[str, Any]],
    *,
    fill_rate_threshold: float,
    max_allowed_lead_days: Optional[int],
    max_allowed_price: Optional[float] = None,
) -> Optional[dict[str, Any]]:
    candidates = [
        item
        for item in supplier_stats
        if item.get("current_price") is not None
        and not item.get("is_own_price")
        and int(item.get("current_qty") or 0) > 0
    ]
    # Жёсткий потолок закупочной цены: закупать дороже max_allowed_price
    # (доля от нашей продажной цены) нельзя — иначе маржа исчезает.
    if max_allowed_price is not None:
        candidates = [
            item
            for item in candidates
            if float(item.get("current_price") or 0) <= float(max_allowed_price)
        ]
    if not candidates:
        return None
    base_candidates = candidates

    filtered_by_fill = []
    for item in base_candidates:
        fill_rate = item.get("fill_rate")
        if fill_rate is not None and float(fill_rate) >= float(fill_rate_threshold):
            filtered_by_fill.append(item)
    base_candidates = filtered_by_fill or base_candidates

    if max_allowed_lead_days is not None:
        filtered_by_lead = []
        for item in base_candidates:
            effective_lead_days = item.get("effective_lead_days")
            if (
                effective_lead_days is None
                or float(effective_lead_days) <= float(max_allowed_lead_days)
            ):
                filtered_by_lead.append(item)
        base_candidates = filtered_by_lead or base_candidates

    return min(
        base_candidates,
        key=lambda item: (
            float(item.get("current_price") or 9_999_999),
            -float(item.get("fill_rate") or 0),
            float(item.get("effective_lead_days") or 9_999),
            -int(item.get("current_qty") or 0),
        ),
    )


def _round_up_to_multiplicity(quantity: int, multiplicity: int) -> int:
    if quantity <= 0:
        return 0
    if multiplicity <= 1:
        return quantity
    return int(ceil(quantity / multiplicity) * multiplicity)


def _round_down_to_lot(quantity: int, lot: int) -> int:
    """Максимум, который можно заказать целыми партиями поставщика."""
    if quantity <= 0:
        return 0
    if lot <= 1:
        return quantity
    return (int(quantity) // int(lot)) * int(lot)


def _summarize_snapshot_rows(
    snapshot_rows: list[dict[str, Any]],
) -> tuple[
    dict[tuple[date, int], dict[str, Any]],
    dict[str, dict[str, Any]],
    dict[str, dict[str, Any]],
]:
    snapshots_by_key: dict[tuple[date, int], dict[str, Any]] = {}
    latest_known_rows_by_oem: dict[str, dict[str, Any]] = {}

    for row in snapshot_rows:
        snapshot_key = (row["pricelist_date"], row["pricelist_id"])
        snapshot = snapshots_by_key.setdefault(
            snapshot_key,
            {
                "pricelist_date": row["pricelist_date"],
                "pricelist_id": row["pricelist_id"],
                "qty_by_oem": {},
            },
        )
        oem_number = _normalize_oem(row.get("oem_number"))
        if not oem_number:
            continue
        qty = int(row.get("quantity") or 0)
        snapshot["qty_by_oem"][oem_number] = (
            snapshot["qty_by_oem"].get(oem_number, 0) + qty
        )

        latest_known_rows_by_oem[oem_number] = {
            "oem_number": oem_number,
            "autopart_id": row.get("autopart_id"),
            "brand_name": row.get("brand_name"),
            "autopart_name": row.get("autopart_name"),
            "latest_price": _to_decimal(row.get("price")),
            "last_seen_pricelist_date": row.get("pricelist_date"),
            "last_seen_pricelist_id": row.get("pricelist_id"),
            "minimum_balance": int(row.get("minimum_balance") or 0),
            "multiplicity": int(row.get("multiplicity") or 1),
            "min_balance_auto": bool(row.get("min_balance_auto") or False),
            "min_balance_user": bool(row.get("min_balance_user") or False),
        }

    latest_rows_by_oem: dict[str, dict[str, Any]] = {}
    if snapshots_by_key:
        latest_snapshot_key = max(snapshots_by_key.keys())
        latest_snapshot_rows = [
            row
            for row in snapshot_rows
            if (row["pricelist_date"], row["pricelist_id"]) == latest_snapshot_key
        ]
        for row in latest_snapshot_rows:
            oem_number = _normalize_oem(row.get("oem_number"))
            if not oem_number:
                continue
            entry = latest_rows_by_oem.setdefault(
                oem_number,
                {
                    "oem_number": oem_number,
                    "autopart_id": row.get("autopart_id"),
                    "brand_name": row.get("brand_name"),
                    "autopart_name": row.get("autopart_name"),
                    "current_quantity": 0,
                    "latest_price": None,
                    "minimum_balance": int(row.get("minimum_balance") or 0),
                    "multiplicity": int(row.get("multiplicity") or 1),
                    "min_balance_auto": bool(row.get("min_balance_auto") or False),
                    "min_balance_user": bool(row.get("min_balance_user") or False),
                },
            )
            qty = int(row.get("quantity") or 0)
            entry["current_quantity"] += qty
            price_value = _to_decimal(row.get("price"))
            if entry["latest_price"] is None or (
                price_value is not None and price_value < entry["latest_price"]
            ):
                entry["latest_price"] = price_value
                entry["autopart_id"] = row.get("autopart_id")
                entry["brand_name"] = row.get("brand_name")
                entry["autopart_name"] = row.get("autopart_name")

    return snapshots_by_key, latest_known_rows_by_oem, latest_rows_by_oem


def _build_received_qty_by_oem_and_date(
    history_rows: list[dict[str, Any]],
) -> dict[str, dict[date, int]]:
    received_qty_by_oem_and_date: dict[str, dict[date, int]] = {}
    for row in history_rows:
        oem_number = _normalize_oem(row.get("oem_number"))
        received_at = row.get("received_at")
        received_quantity = int(row.get("received_quantity") or 0)
        if not oem_number or received_at is None or received_quantity <= 0:
            continue
        received_date = received_at.date()
        per_date = received_qty_by_oem_and_date.setdefault(oem_number, {})
        per_date[received_date] = per_date.get(received_date, 0) + received_quantity
    return received_qty_by_oem_and_date


def _get_received_qty_between(
    received_qty_by_date: dict[date, int],
    *,
    previous_date: date,
    current_date: date,
) -> int:
    return sum(
        qty
        for received_date, qty in received_qty_by_date.items()
        if previous_date < received_date <= current_date
    )


def _estimate_coverable_in_transit_qty(
    history_rows: list[dict[str, Any]],
    *,
    lead_time_days_used: Optional[float],
) -> int:
    if lead_time_days_used is None:
        return 0

    planning_horizon_days = max(int(ceil(float(lead_time_days_used))), 0)
    planning_horizon_date = now_moscow().date() + timedelta(days=planning_horizon_days)
    coverable_qty = 0

    for row in history_rows:
        if row.get("current_status") not in _ACTIVE_ORDER_STATUSES:
            continue
        outstanding_qty = max(
            int(row.get("ordered_quantity") or 0)
            - int(row.get("received_quantity") or 0),
            0,
        )
        if outstanding_qty <= 0:
            continue
        created_at = row.get("created_at")
        if created_at is None:
            continue
        row_lead_days_raw = (
            row.get("max_delivery_day")
            or row.get("min_delivery_day")
            or lead_time_days_used
        )
        try:
            row_lead_days = max(int(ceil(float(row_lead_days_raw))), 0)
        except (TypeError, ValueError):
            row_lead_days = planning_horizon_days
        expected_arrival_date = created_at.date() + timedelta(days=row_lead_days)
        if expected_arrival_date <= planning_horizon_date:
            coverable_qty += outstanding_qty

    return max(coverable_qty, 0)


async def _try_acquire_autopurchase_run_lock(session: AsyncSession) -> bool:
    result = await session.execute(
        select(func.pg_try_advisory_xact_lock(AUTOPURCHASE_RUN_LOCK_KEY))
    )
    return bool(result.scalar())


async def _get_active_autopurchase_run(
    session: AsyncSession,
) -> Optional[AutoPurchaseRun]:
    stmt = (
        select(AutoPurchaseRun)
        .where(
            AutoPurchaseRun.finished_at.is_(None),
            AutoPurchaseRun.status.in_(
                [
                    AUTOPURCHASE_RUN_STATUS_QUEUED,
                    AUTOPURCHASE_RUN_STATUS_RUNNING,
                ]
            ),
        )
        .order_by(AutoPurchaseRun.started_at.desc(), AutoPurchaseRun.id.desc())
        .limit(1)
    )
    return (await session.execute(stmt)).scalar_one_or_none()


def _build_run_summary_message(status: str) -> str:
    if status == AUTOPURCHASE_RUN_STATUS_QUEUED:
        return "Запуск автозаказа поставлен в очередь"
    if status == AUTOPURCHASE_RUN_STATUS_RUNNING:
        return "Расчёт автозаказа выполняется"
    if status == AUTOPURCHASE_RUN_STATUS_COMPLETED:
        return "Расчёт автозаказа завершён"
    if status == AUTOPURCHASE_RUN_STATUS_FAILED:
        return "Расчёт автозаказа завершился с ошибкой"
    return "Статус запуска обновлён"


def _build_run_settings(
    *,
    own_provider_config_id: Optional[int],
    mode: str,
    limit: int,
    budget_limit: Optional[float],
    position_limit: Optional[int],
) -> dict[str, Any]:
    return {
        "own_provider_config_id": own_provider_config_id,
        "mode": mode,
        "limit": limit,
        "budget_limit": budget_limit,
        "position_limit": position_limit,
        "supplier_source": "site",
    }


def _build_completed_run_summary_message(preview: dict[str, Any]) -> str:
    total_items = int(preview.get("total_items") or 0)
    auto_approved_count = int(preview.get("auto_approved_count") or 0)
    needs_review_count = int(preview.get("needs_review_count") or 0)
    blocked_count = int(preview.get("blocked_count") or 0)

    if total_items <= 0:
        return "Расчёт завершён: позиции с потребностью к заказу не найдены"
    if auto_approved_count <= 0 and needs_review_count > 0:
        return (
            "Расчёт завершён: готовых строк к заказу не найдено, "
            f"на ручную проверку вынесено {needs_review_count} поз."
        )
    if auto_approved_count > 0:
        return (
            "Расчёт завершён: "
            f"готово к заказу {auto_approved_count} поз., "
            f"на проверку {needs_review_count} поз., "
            f"заблокировано {blocked_count} поз."
        )
    return "Расчёт автозаказа завершён"


def _build_initial_run_summary(config_row: dict[str, Any], *, status: str) -> dict[str, Any]:
    return {
        "provider_name": config_row.get("provider_name"),
        "provider_config_name": config_row.get("provider_config_name"),
        "generated_at": None,
        "total_items": 0,
        "auto_approved_count": 0,
        "needs_review_count": 0,
        "blocked_count": 0,
        "sent_count": 0,
        "diagnostics": [],
        "message": _build_run_summary_message(status),
    }


async def _persist_autopurchase_preview(
    session: AsyncSession,
    *,
    run_id: int,
    preview: dict[str, Any],
) -> None:
    persisted_run = await session.get(AutoPurchaseRun, run_id)
    if persisted_run is None:
        raise ValueError("Запуск автозаказа не найден")

    finished_at = now_moscow()
    persisted_run.finished_at = finished_at
    persisted_run.status = AUTOPURCHASE_RUN_STATUS_COMPLETED
    persisted_run.mode = str(preview["mode"])
    persisted_run.summary_snapshot = {
        "provider_name": preview["provider_name"],
        "provider_config_name": preview.get("provider_config_name"),
        "generated_at": preview["generated_at"].isoformat()
        if preview.get("generated_at")
        else None,
        "total_items": preview["total_items"],
        "auto_approved_count": preview["auto_approved_count"],
        "needs_review_count": preview["needs_review_count"],
        "blocked_count": preview["blocked_count"],
        "sent_count": 0,
        "diagnostics": _to_json_safe(list(preview.get("diagnostics") or [])),
        "message": _build_completed_run_summary_message(preview),
    }

    await session.execute(
        delete(AutoPurchaseRunItem).where(AutoPurchaseRunItem.run_id == run_id)
    )

    for row in preview.get("rows", []):
        recommended_supplier = row.get("recommended_supplier") or {}
        item = AutoPurchaseRunItem(
            run_id=run_id,
            autopart_id=row.get("autopart_id"),
            selected_supplier_id=recommended_supplier.get("provider_id"),
            oem_number=row["oem_number"],
            brand_name=row.get("brand_name"),
            autopart_name=row.get("autopart_name"),
            current_quantity=int(row.get("current_quantity") or 0),
            latest_price=_to_decimal_value(row.get("latest_price")),
            minimum_balance=int(row.get("minimum_balance") or 0),
            multiplicity=int(row.get("multiplicity") or 1),
            in_transit_qty=int(row.get("in_transit_qty") or 0),
            sold_last_30_days=int(row.get("sold_last_30_days") or 0),
            sold_last_90_days=int(row.get("sold_last_90_days") or 0),
            avg_daily_30=_to_decimal_value(row.get("avg_daily_30")),
            avg_daily_90=_to_decimal_value(row.get("avg_daily_90")),
            avg_daily_blended=_to_decimal_value(row.get("avg_daily_blended")),
            estimated_days_left_30_days=row.get("estimated_days_left_30_days"),
            average_actual_lead_days=_to_decimal_value(
                row.get("average_actual_lead_days")
            ),
            lead_time_days_used=_to_decimal_value(
                row.get("lead_time_days_used")
            ),
            safety_stock_days=row.get("safety_stock_days"),
            safety_stock_qty=_to_decimal_value(row.get("safety_stock_qty")),
            reorder_point=_to_decimal_value(row.get("reorder_point")),
            target_stock=row.get("target_stock"),
            recommended_order_qty=int(row.get("recommended_order_qty") or 0),
            decision_status=row["decision_status"],
            autopurchase_mode=row["autopurchase_mode"],
            missing_in_latest_pricelist=bool(
                row.get("missing_in_latest_pricelist") or False
            ),
            reason_codes=list(row.get("reason_codes") or []),
            reason_titles=list(row.get("reason_titles") or []),
            reasons=_to_json_safe(list(row.get("reasons") or [])),
            abc_xyz=_to_json_safe(row.get("abc_xyz") or {}),
            best_supplier_by_price=_to_json_safe(
                row.get("best_supplier_by_price") or {}
            ),
            best_supplier_by_lead_time=_to_json_safe(
                row.get("best_supplier_by_lead_time") or {}
            ),
            recommended_supplier=_to_json_safe(recommended_supplier or {}),
            draft_purchase_order=_to_json_safe(
                row.get("draft_purchase_order") or {}
            ),
            top_site_offers=_to_json_safe(
                list(row.get("top_site_offers") or [])
            ),
            cross_group=_to_json_safe(row.get("cross_group") or {}),
        )
        session.add(item)

    await _prune_old_finished_autopurchase_runs(session)


def _calculate_snapshot_sales(
    snapshots: list[dict[str, Any]],
    normalized_oem_numbers: list[str],
    *,
    days: int,
    received_qty_by_oem_and_date: Optional[dict[str, dict[date, int]]] = None,
) -> dict[str, int]:
    sold_by_oem = {oem: 0 for oem in normalized_oem_numbers}
    cutoff = now_moscow().date() - timedelta(days=days)
    for previous_snapshot, current_snapshot in zip(snapshots, snapshots[1:]):
        if current_snapshot["pricelist_date"] < cutoff:
            continue
        previous_qty_by_oem = previous_snapshot["qty_by_oem"]
        current_qty_by_oem = current_snapshot["qty_by_oem"]
        for oem in normalized_oem_numbers:
            prev_qty = int(previous_qty_by_oem.get(oem, 0))
            curr_qty = int(current_qty_by_oem.get(oem, 0))
            received_qty = _get_received_qty_between(
                (received_qty_by_oem_and_date or {}).get(oem, {}),
                previous_date=previous_snapshot["pricelist_date"],
                current_date=current_snapshot["pricelist_date"],
            )
            sold_by_oem[oem] += max((prev_qty + received_qty) - curr_qty, 0)
    return sold_by_oem


def _calculate_in_stock_days(
    snapshots: list[dict[str, Any]],
    normalized_oem_numbers: list[str],
    *,
    days: int,
) -> dict[str, int]:
    """Сколько дней окна товар реально был в наличии (по снапшотам)."""
    in_stock_by_oem = {oem: 0 for oem in normalized_oem_numbers}
    if not snapshots:
        return in_stock_by_oem
    today = now_moscow().date()
    cutoff = today - timedelta(days=days)

    def _add_span(qty_by_oem: dict[str, int], start: date, end: date) -> None:
        span_days = (end - max(start, cutoff)).days
        if span_days <= 0:
            return
        for oem in normalized_oem_numbers:
            if int(qty_by_oem.get(oem, 0) or 0) > 0:
                in_stock_by_oem[oem] += span_days

    for previous_snapshot, current_snapshot in zip(snapshots, snapshots[1:]):
        if current_snapshot["pricelist_date"] < cutoff:
            continue
        _add_span(
            previous_snapshot["qty_by_oem"],
            previous_snapshot["pricelist_date"],
            current_snapshot["pricelist_date"],
        )
    # Хвост от последнего снапшота до сегодня.
    _add_span(
        snapshots[-1]["qty_by_oem"],
        snapshots[-1]["pricelist_date"],
        today,
    )
    return {
        oem: min(value, days) for oem, value in in_stock_by_oem.items()
    }


async def _load_customer_order_requested_by_oem_windows(
    session: AsyncSession,
    normalized_oem_numbers: list[str],
    *,
    windows: tuple[int, ...] = AUTOPURCHASE_DEMAND_WINDOWS,
) -> dict[int, dict[str, int]]:
    normalized_windows = tuple(sorted({max(int(day), 1) for day in windows}))
    totals = {
        days: {oem: 0 for oem in normalized_oem_numbers}
        for days in normalized_windows
    }
    if not normalized_oem_numbers:
        return totals

    current_time = now_moscow()
    cutoffs = {
        days: current_time - timedelta(days=days)
        for days in normalized_windows
    }
    max_cutoff = min(cutoffs.values())
    stmt = (
        select(
            CustomerOrderItem.oem,
            CustomerOrder.received_at,
            CustomerOrderItem.requested_qty,
        )
        .join(CustomerOrder, CustomerOrder.id == CustomerOrderItem.order_id)
        .where(
            CustomerOrder.received_at >= max_cutoff,
            CustomerOrderItem.requested_qty.isnot(None),
            CustomerOrderItem.requested_qty > 0,
        )
    )
    result = await session.execute(stmt)
    allowed_oems = set(normalized_oem_numbers)
    for oem_raw, received_at, requested_qty in result.fetchall():
        normalized = _normalize_oem(oem_raw)
        if (
            not normalized
            or normalized not in allowed_oems
            or received_at is None
        ):
            continue
        qty = max(int(requested_qty or 0), 0)
        if qty <= 0:
            continue
        for days, cutoff in cutoffs.items():
            if received_at >= cutoff:
                totals[days][normalized] = totals[days].get(normalized, 0) + qty
    return totals


async def _load_open_customer_backlog_by_oem(
    session: AsyncSession,
    normalized_oem_numbers: list[str],
) -> dict[str, int]:
    backlog_by_oem = {oem: 0 for oem in normalized_oem_numbers}
    if not normalized_oem_numbers:
        return backlog_by_oem

    allowed_oems = set(normalized_oem_numbers)

    def _add_backlog(oem_raw: Any, qty: int) -> None:
        normalized = _normalize_oem(oem_raw)
        if not normalized or normalized not in allowed_oems:
            return
        backlog_by_oem[normalized] = backlog_by_oem.get(normalized, 0) + max(
            int(qty or 0),
            0,
        )

    new_items_stmt = (
        select(
            CustomerOrderItem.oem,
            func.sum(CustomerOrderItem.requested_qty).label("qty"),
        )
        .where(
            CustomerOrderItem.status == CUSTOMER_ORDER_ITEM_STATUS.NEW,
            CustomerOrderItem.requested_qty > 0,
        )
        .group_by(CustomerOrderItem.oem)
    )
    for oem_raw, qty in (await session.execute(new_items_stmt)).fetchall():
        _add_backlog(oem_raw, int(qty or 0))

    stock_items_stmt = (
        select(
            CustomerOrderItem.oem,
            func.sum(StockOrderItem.quantity).label("qty"),
        )
        .select_from(StockOrderItem)
        .join(StockOrder, StockOrder.id == StockOrderItem.stock_order_id)
        .join(
            CustomerOrderItem,
            CustomerOrderItem.id == StockOrderItem.customer_order_item_id,
        )
        .where(
            StockOrder.status == STOCK_ORDER_STATUS.NEW,
            StockOrderItem.quantity > 0,
        )
        .group_by(CustomerOrderItem.oem)
    )
    for oem_raw, qty in (await session.execute(stock_items_stmt)).fetchall():
        _add_backlog(oem_raw, int(qty or 0))

    supplier_items_stmt = (
        select(
            CustomerOrderItem.oem,
            SupplierOrderItem.quantity,
            SupplierOrderItem.received_quantity,
        )
        .select_from(SupplierOrderItem)
        .join(SupplierOrder, SupplierOrder.id == SupplierOrderItem.supplier_order_id)
        .join(
            CustomerOrderItem,
            CustomerOrderItem.id == SupplierOrderItem.customer_order_item_id,
        )
        .where(
            SupplierOrder.status != SUPPLIER_ORDER_STATUS.REMOVED,
            SupplierOrderItem.quantity > 0,
        )
    )
    for oem_raw, ordered_qty, received_qty in (
        await session.execute(supplier_items_stmt)
    ).fetchall():
        outstanding_qty = max(
            int(ordered_qty or 0) - int(received_qty or 0),
            0,
        )
        if outstanding_qty > 0:
            _add_backlog(oem_raw, outstanding_qty)

    return backlog_by_oem


async def _load_customer_order_period_metrics_by_oem(
    session: AsyncSession,
    normalized_oem_numbers: list[str],
    *,
    windows: tuple[int, ...] = AUTOPURCHASE_DEMAND_WINDOWS,
) -> dict[str, dict[str, Any]]:
    normalized_windows = tuple(sorted({max(int(day), 1) for day in windows}))
    metrics_by_oem = {
        oem: {
            **{
                f"order_count_{days}_days": 0
                for days in normalized_windows
            },
            **{
                f"min_sale_price_{days}_days": None
                for days in normalized_windows
            },
        }
        for oem in normalized_oem_numbers
    }
    if not normalized_oem_numbers:
        return metrics_by_oem

    current_time = now_moscow()
    cutoffs = {
        days: current_time - timedelta(days=days)
        for days in normalized_windows
    }
    max_cutoff = min(cutoffs.values())
    order_ids_by_oem_and_window = {
        oem: {days: set() for days in normalized_windows}
        for oem in normalized_oem_numbers
    }
    allowed_oems = set(normalized_oem_numbers)

    stmt = (
        select(
            CustomerOrderItem.oem,
            CustomerOrder.id,
            CustomerOrder.received_at,
            CustomerOrderItem.requested_price,
        )
        .join(CustomerOrder, CustomerOrder.id == CustomerOrderItem.order_id)
        .where(
            CustomerOrder.received_at >= max_cutoff,
            CustomerOrderItem.requested_qty.isnot(None),
            CustomerOrderItem.requested_qty > 0,
        )
    )
    result = await session.execute(stmt)
    for oem_raw, order_id, received_at, requested_price in result.fetchall():
        normalized = _normalize_oem(oem_raw)
        if (
            not normalized
            or normalized not in allowed_oems
            or received_at is None
            or order_id is None
        ):
            continue
        price_value = None
        if requested_price is not None:
            try:
                candidate_price = float(requested_price)
            except (TypeError, ValueError):
                candidate_price = None
            if candidate_price is not None and candidate_price > 0:
                price_value = _quantize_float(candidate_price)

        for days, cutoff in cutoffs.items():
            if received_at < cutoff:
                continue
            order_ids_by_oem_and_window[normalized][days].add(int(order_id))
            price_key = f"min_sale_price_{days}_days"
            if price_value is not None:
                current_min_price = metrics_by_oem[normalized][price_key]
                if current_min_price is None or price_value < current_min_price:
                    metrics_by_oem[normalized][price_key] = price_value

    for oem_number, window_map in order_ids_by_oem_and_window.items():
        for days, order_ids in window_map.items():
            metrics_by_oem[oem_number][f"order_count_{days}_days"] = len(order_ids)

    return metrics_by_oem


def _estimate_consecutive_stockout_days(
    snapshots: list[dict[str, Any]],
    *,
    oem_number: str,
) -> int:
    if not snapshots:
        return 0
    latest_snapshot = snapshots[-1]
    latest_qty = int(latest_snapshot["qty_by_oem"].get(oem_number, 0) or 0)
    if latest_qty > 0:
        return 0
    latest_date = latest_snapshot["pricelist_date"]
    stockout_started_at = latest_date
    for snapshot in reversed(snapshots):
        qty = int(snapshot["qty_by_oem"].get(oem_number, 0) or 0)
        if qty > 0:
            break
        stockout_started_at = snapshot["pricelist_date"]
    return max((latest_date - stockout_started_at).days, 0)


def _apply_recovery_mode(
    *,
    current_quantity: int,
    consecutive_stockout_days: int,
    avg_daily_planning: Optional[float],
    avg_daily_180: Optional[float],
    avg_daily_365: Optional[float],
) -> tuple[Optional[float], bool]:
    if (
        current_quantity > 0
        or consecutive_stockout_days < AUTOPURCHASE_RECOVERY_STOCKOUT_DAYS
    ):
        return avg_daily_planning, False

    recovery_floor = max(
        [
            float(value)
            for value in (avg_daily_180, avg_daily_365)
            if value is not None and value > 0
        ],
        default=None,
    )
    if recovery_floor is None:
        return avg_daily_planning, False
    if avg_daily_planning is None or recovery_floor > float(avg_daily_planning):
        return _quantize_float(recovery_floor), True
    return avg_daily_planning, False


def _build_autopurchase_draft(
    *,
    supplier: Optional[dict[str, Any]],
    available_qty: int,
    in_transit_qty: int,
    target_qty: Optional[int],
    recommended_qty: int,
    lead_time_days_used: Optional[float],
    reason: Optional[str],
    open_customer_backlog_qty: int = 0,
    last_receipt_price: Optional[float] = None,
) -> Optional[dict[str, Any]]:
    if not supplier or not supplier.get("provider_name"):
        return None
    if supplier.get("current_price") is None:
        return None
    if recommended_qty <= 0:
        return None

    supplier_available_qty = max(int(supplier.get("current_qty") or 0), 0)
    if supplier_available_qty <= 0:
        return None
    # min_qnt поставщика — это КРАТНОСТЬ (размер партии): заказывать можно
    # только целыми партиями, округляя потребность ВВЕРХ.
    # Пример: нужно 57, партия 20 → заказываем 60.
    supplier_lot = max(int(supplier.get("current_min_qnt") or 1), 1)
    orderable_cap_qty = _round_down_to_lot(supplier_available_qty, supplier_lot)
    if orderable_cap_qty <= 0:
        # У поставщика нет даже одной целой партии.
        return None
    proposed_order_qty = min(
        _round_up_to_multiplicity(recommended_qty, supplier_lot),
        orderable_cap_qty,
    )
    if proposed_order_qty <= 0:
        return None
    remaining_gap_qty = max(recommended_qty - proposed_order_qty, 0)

    return {
        "provider_id": (
            int(supplier["provider_id"])
            if supplier.get("provider_id") is not None
            else None
        ),
        "external_supplier_id": supplier.get("external_supplier_id"),
        "provider_name": supplier.get("provider_name") or "—",
        "provider_config_id": supplier.get("current_provider_config_id"),
        "provider_config_name": supplier.get("current_provider_config_name"),
        "autopart_id": supplier.get("current_autopart_id"),
        "oem_number": supplier.get("current_oem_number") or "",
        "brand_name": supplier.get("current_brand_name"),
        "autopart_name": supplier.get("current_autopart_name"),
        "price": supplier.get("current_price"),
        "last_receipt_price": last_receipt_price,
        "available_qty": available_qty,
        "in_transit_qty": int(in_transit_qty or 0),
        "open_customer_backlog_qty": max(int(open_customer_backlog_qty or 0), 0),
        "target_qty": target_qty,
        "recommended_qty": recommended_qty,
        "supplier_available_qty": supplier_available_qty,
        "proposed_order_qty": proposed_order_qty,
        "remaining_gap_qty": remaining_gap_qty,
        "lead_days_used": lead_time_days_used,
        "reason": reason,
        "source_type": supplier.get("source_type"),
        "sup_logo": supplier.get("sup_logo"),
        "hash_key": supplier.get("hash_key"),
        "system_hash": supplier.get("system_hash"),
        "min_qnt": supplier.get("current_min_qnt"),
        "min_delivery_day": supplier.get("current_min_delivery"),
        "max_delivery_day": supplier.get("current_max_delivery"),
    }


async def _resolve_autopurchase_provider_config(
    session: AsyncSession,
    *,
    own_provider_config_id: Optional[int] = None,
) -> dict[str, Any]:
    config_stmt = (
        select(
            ProviderPriceListConfig.id.label("provider_config_id"),
            ProviderPriceListConfig.provider_id.label("provider_id"),
            ProviderPriceListConfig.name_price.label("provider_config_name"),
            Provider.name.label("provider_name"),
        )
        .select_from(ProviderPriceListConfig)
        .join(Provider, Provider.id == ProviderPriceListConfig.provider_id)
        .where(Provider.is_own_price.is_(True))
    )
    if own_provider_config_id is not None:
        config_stmt = config_stmt.where(
            ProviderPriceListConfig.id == own_provider_config_id
        )
    else:
        config_stmt = config_stmt.where(
            ProviderPriceListConfig.use_for_order_insights.is_(True)
        )
    config_stmt = config_stmt.order_by(ProviderPriceListConfig.id.asc()).limit(1)
    config_row = (await session.execute(config_stmt)).mappings().first()
    if not config_row:
        raise ValueError("Не найден конфиг нашего прайса для автозаказа")
    return dict(config_row)


async def get_autopurchase_preview(
    session: AsyncSession,
    *,
    own_provider_config_id: Optional[int] = None,
    mode: str = AUTOPURCHASE_MODE_DRAFT_ONLY,
    decision_status: Optional[str] = None,
    search: Optional[str] = None,
    limit: int = 200,
) -> dict[str, Any]:
    requested_mode = str(mode or AUTOPURCHASE_MODE_DRAFT_ONLY).strip().lower()
    normalized_limit = max(min(int(limit or 200), AUTOPURCHASE_MAX_LIMIT), 1)
    if requested_mode not in {
        AUTOPURCHASE_MODE_DRAFT_ONLY,
        AUTOPURCHASE_MODE_AUTO_APPROVE_SAFE,
        AUTOPURCHASE_MODE_DISABLED,
    }:
        raise ValueError("Неизвестный режим автозаказа")

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
    snapshot_rows = list((await session.execute(snapshot_stmt)).mappings().all())
    if not snapshot_rows:
        return {
            "provider_config_id": provider_config_id,
            "provider_id": int(config_row["provider_id"]),
            "provider_name": config_row["provider_name"],
            "provider_config_name": config_row.get("provider_config_name"),
            "generated_at": now_moscow(),
            "mode": requested_mode,
            "supplier_source": "site",
            "total_items": 0,
            "auto_approved_count": 0,
            "needs_review_count": 0,
            "blocked_count": 0,
            "rows": [],
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

    received_qty_by_oem_and_date = _build_received_qty_by_oem_and_date(history_rows)
    abc_xyz_by_oem = await _compute_single_oem_abc_xyz_batch(
        session,
        normalized_oem_numbers=normalized_oem_numbers,
        history_rows_by_oem=history_by_oem,
    )
    snapshot_demand_by_window = {
        days: _calculate_snapshot_sales(
            snapshots,
            normalized_oem_numbers,
            days=days,
            received_qty_by_oem_and_date=received_qty_by_oem_and_date,
        )
        for days in AUTOPURCHASE_DEMAND_WINDOWS
    }
    in_stock_days_by_window = {
        days: _calculate_in_stock_days(
            snapshots,
            normalized_oem_numbers,
            days=days,
        )
        for days in AUTOPURCHASE_DEMAND_WINDOWS
    }
    customer_requested_by_window = await _load_customer_order_requested_by_oem_windows(
        session,
        normalized_oem_numbers,
        windows=AUTOPURCHASE_DEMAND_WINDOWS,
    )
    open_customer_backlog_by_oem = await _load_open_customer_backlog_by_oem(
        session,
        normalized_oem_numbers,
    )
    dragonzap_cross_stock_by_oem = await _load_dragonzap_cross_stock_map(
        session,
        latest_known_rows_by_oem=latest_known_rows_by_oem,
        latest_rows_by_oem=latest_rows_by_oem,
        history_by_oem=history_by_oem,
    )

    decision_filter = str(decision_status or "").strip().lower() or None
    search_filter = str(search or "").strip().lower()
    decision_rank = {
        AUTOPURCHASE_STATUS_BLOCKED: 0,
        AUTOPURCHASE_STATUS_NEEDS_REVIEW: 1,
        AUTOPURCHASE_STATUS_AUTO_APPROVED: 2,
    }
    base_rows: list[dict[str, Any]] = []
    diagnostics = {
        "oems_in_own_pricelist_count": len(normalized_oem_numbers),
        "oems_with_sales_signal_count": 0,
        "oems_with_open_backlog_count": 0,
        "recovery_mode_used_count": 0,
        "fallback_lead_time_used_count": 0,
        "fallback_lead_time_sales_only_count": 0,
        "manual_min_balance_fallback_count": 0,
        "excluded_missing_without_activity_count": 0,
        "excluded_zero_need_count": 0,
        "excluded_zero_need_with_sales_count": 0,
        "rows_missing_in_latest_pricelist_count": 0,
        "rows_without_supplier_count": 0,
        "rows_without_draft_count": 0,
        "rows_with_partial_supplier_qty_count": 0,
        "rows_ready_for_draft_count": 0,
    }

    for _oem_idx, (oem_number, known_row) in enumerate(latest_known_rows_by_oem.items()):
        # Каждые 50 OEM отдаём управление event loop — без этого при большом прайсе
        # цикл блокирует loop на несколько секунд и вызывает TimeoutError в других задачах.
        if _oem_idx % 50 == 0 and _oem_idx > 0:
            await asyncio.sleep(0)

        latest = latest_rows_by_oem.get(oem_number) or {
            **known_row,
            "current_quantity": 0,
        }
        oem_history = history_by_oem.get(oem_number, [])
        # Открытый backlog НЕ должен попадать в спрос: незакрытые заявки
        # учитываются отдельно (вычитаются из свободного остатка), иначе
        # потребность задваивается — один раз через раздутый спрос/цель
        # и второй раз через вычет backlog из наличия.
        _open_backlog_for_demand = int(
            open_customer_backlog_by_oem.get(oem_number, 0)
        )
        requested_last_30_days = max(
            int(customer_requested_by_window[30].get(oem_number, 0))
            - _open_backlog_for_demand,
            0,
        )
        requested_last_90_days = max(
            int(customer_requested_by_window[90].get(oem_number, 0))
            - _open_backlog_for_demand,
            0,
        )
        requested_last_180_days = max(
            int(customer_requested_by_window[180].get(oem_number, 0))
            - _open_backlog_for_demand,
            0,
        )
        requested_last_365_days = max(
            int(customer_requested_by_window[365].get(oem_number, 0))
            - _open_backlog_for_demand,
            0,
        )
        snapshot_last_30_days = int(
            snapshot_demand_by_window[30].get(oem_number, 0)
        )
        snapshot_last_90_days = int(
            snapshot_demand_by_window[90].get(oem_number, 0)
        )
        snapshot_last_180_days = int(
            snapshot_demand_by_window[180].get(oem_number, 0)
        )
        snapshot_last_365_days = int(
            snapshot_demand_by_window[365].get(oem_number, 0)
        )
        sold_last_30_days = max(requested_last_30_days, snapshot_last_30_days)
        sold_last_90_days = max(requested_last_90_days, snapshot_last_90_days)
        sold_last_180_days = max(requested_last_180_days, snapshot_last_180_days)
        sold_last_365_days = max(requested_last_365_days, snapshot_last_365_days)
        # Спрос считаем на день НАЛИЧИЯ: продажи делим на дни, когда товар
        # реально был на складе, иначе stockout занижает прогноз.
        avg_daily_30 = _compute_availability_adjusted_daily(
            sold_last_30_days,
            30,
            in_stock_days_by_window[30].get(oem_number, 30),
        )
        avg_daily_90 = _compute_availability_adjusted_daily(
            sold_last_90_days,
            90,
            in_stock_days_by_window[90].get(oem_number, 90),
        )
        avg_daily_180 = _compute_availability_adjusted_daily(
            sold_last_180_days,
            180,
            in_stock_days_by_window[180].get(oem_number, 180),
        )
        avg_daily_365 = _compute_availability_adjusted_daily(
            sold_last_365_days,
            365,
            in_stock_days_by_window[365].get(oem_number, 365),
        )
        current_quantity = int(latest.get("current_quantity") or 0)
        minimum_balance = int(latest.get("minimum_balance") or 0)
        multiplicity = max(int(latest.get("multiplicity") or 1), 1)
        missing_in_latest_pricelist = oem_number not in latest_rows_by_oem
        open_customer_backlog_qty = int(
            open_customer_backlog_by_oem.get(oem_number, 0)
        )
        consecutive_stockout_days = _estimate_consecutive_stockout_days(
            snapshots,
            oem_number=oem_number,
        )
        avg_daily_blended = _blend_average_daily_horizons(
            avg_daily_30,
            avg_daily_90,
            avg_daily_180,
            avg_daily_365,
        )
        avg_daily_blended, recovery_mode_applied = _apply_recovery_mode(
            current_quantity=current_quantity,
            consecutive_stockout_days=consecutive_stockout_days,
            avg_daily_planning=avg_daily_blended,
            avg_daily_180=avg_daily_180,
            avg_daily_365=avg_daily_365,
        )
        has_sales_signal = bool(
            (avg_daily_blended and avg_daily_blended > 0)
            or sold_last_30_days > 0
            or sold_last_90_days > 0
            or sold_last_180_days > 0
            or sold_last_365_days > 0
            or open_customer_backlog_qty > 0
        )
        if has_sales_signal:
            diagnostics["oems_with_sales_signal_count"] += 1
        if open_customer_backlog_qty > 0:
            diagnostics["oems_with_open_backlog_count"] += 1
        if recovery_mode_applied:
            diagnostics["recovery_mode_used_count"] += 1
        lead_values = [
            int(row["actual_lead_days"])
            for row in oem_history
            if row.get("actual_lead_days") is not None
        ]
        average_actual_lead_days = (
            _round_stat(sum(lead_values) / len(lead_values), 1)
            if lead_values
            else None
        )
        in_transit_qty = max(
            sum(
                max(
                    int(row.get("ordered_quantity") or 0)
                    - int(row.get("received_quantity") or 0),
                    0,
                )
                for row in oem_history
                if row.get("current_status") in _ACTIVE_ORDER_STATUSES
            ),
            0,
        )

        abc_xyz = abc_xyz_by_oem.get(oem_number)
        abc_class = abc_xyz.get("abc_class") if abc_xyz else None
        xyz_class = abc_xyz.get("xyz_class") if abc_xyz else None
        safety_stock_days = _get_safety_stock_days(abc_class, xyz_class)
        lead_time_days_used = (
            float(average_actual_lead_days)
            if average_actual_lead_days is not None
            else None
        )

        manual_min_balance_fallback = False
        if lead_time_days_used is None and has_sales_signal:
            lead_time_days_used = float(AUTOPURCHASE_DEFAULT_LEAD_DAYS_FALLBACK)
            diagnostics["fallback_lead_time_used_count"] += 1
            if minimum_balance <= 0:
                diagnostics["fallback_lead_time_sales_only_count"] += 1
        elif lead_time_days_used is None and minimum_balance > 0:
            manual_min_balance_fallback = True
            diagnostics["manual_min_balance_fallback_count"] += 1

        coverable_in_transit_qty = _estimate_coverable_in_transit_qty(
            oem_history,
            lead_time_days_used=lead_time_days_used,
        )
        # Наличие Dragonzap-позиции считаем вместе с кроссами бренда
        # Dragonzap (свой остаток + остатки и «в пути» кросс-артикулов).
        cross_group_raw = dragonzap_cross_stock_by_oem.get(oem_number)
        cross_stock_qty = (
            int(cross_group_raw.get("cross_quantity") or 0)
            if cross_group_raw
            else 0
        )
        cross_in_transit_qty = (
            int(cross_group_raw.get("cross_in_transit_qty") or 0)
            if cross_group_raw
            else 0
        )
        group_available_quantity = current_quantity + cross_stock_qty
        cross_group = (
            {
                "own_quantity": current_quantity,
                "cross_quantity": cross_stock_qty,
                "group_quantity": group_available_quantity,
                "cross_in_transit_qty": cross_in_transit_qty,
                "items": list(cross_group_raw.get("items") or []),
            }
            if cross_group_raw
            else None
        )
        available_qty_for_planning = (
            current_quantity
            + coverable_in_transit_qty
            + cross_stock_qty
            + cross_in_transit_qty
        )
        net_available_qty_for_planning = (
            available_qty_for_planning - open_customer_backlog_qty
        )

        lead_time_demand = (
            _quantize_float(avg_daily_blended * lead_time_days_used)
            if avg_daily_blended is not None and lead_time_days_used is not None
            else None
        )
        safety_stock_qty = (
            _quantize_float(avg_daily_blended * safety_stock_days)
            if avg_daily_blended is not None
            else None
        )
        reorder_point = (
            _quantize_float((lead_time_demand or 0) + (safety_stock_qty or 0), "0.1")
            if lead_time_demand is not None
            else (
                float(minimum_balance)
                if minimum_balance > 0 and manual_min_balance_fallback
                else None
            )
        )
        # Целевой запас — дни покрытия по классу ABC (A=45 «1,5 месяца»,
        # B=30, C=21), чтобы не замораживать деньги в медленных позициях.
        # Точка дозаказа (reorder_point) остаётся нижней границей на случай,
        # когда срок поставки + страховой запас превышают целевое покрытие.
        target_cover_days = _get_target_cover_days(abc_class)
        target_stock = None
        if avg_daily_blended is not None and avg_daily_blended > 0:
            target_stock = int(
                ceil(avg_daily_blended * target_cover_days)
            )
            if reorder_point is not None:
                target_stock = max(target_stock, int(ceil(float(reorder_point))))
        elif reorder_point is not None:
            target_stock = int(ceil(float(reorder_point)))
        elif minimum_balance > 0:
            target_stock = int(minimum_balance)
        if target_stock is not None:
            target_stock = max(target_stock, int(minimum_balance or 0))

        recommended_order_qty = 0
        planning_target_qty = int(target_stock or 0)
        if target_stock is not None or open_customer_backlog_qty > 0:
            recommended_order_qty = max(
                planning_target_qty - net_available_qty_for_planning,
                0,
            )
            recommended_order_qty = _round_up_to_multiplicity(
                recommended_order_qty,
                multiplicity,
            )

        # Для расчёта дней запаса предпочитаем 30-дневную среднюю,
        # но при её отсутствии используем бленд, чтобы не терять приоритет
        # у товаров, у которых последние 30 дней не было продаж.
        _avg_for_days_left = (
            avg_daily_30
            if avg_daily_30 and avg_daily_30 > 0
            else avg_daily_blended
        )
        # Дни запаса считаем по сводному наличию (для Dragonzap — вместе
        # с остатками кросс-артикулов Dragonzap).
        free_current_quantity = max(
            group_available_quantity - open_customer_backlog_qty, 0
        )
        estimated_days_left_30_days = (
            int(free_current_quantity / _avg_for_days_left)
            if _avg_for_days_left and _avg_for_days_left > 0
            else None
        )
        if (
            missing_in_latest_pricelist
            and sold_last_30_days <= 0
            and sold_last_90_days <= 0
            and sold_last_180_days <= 0
            and sold_last_365_days <= 0
            and in_transit_qty <= 0
            and open_customer_backlog_qty <= 0
            and minimum_balance <= 0
        ):
            diagnostics["excluded_missing_without_activity_count"] += 1
            continue
        if (
            recommended_order_qty <= 0
            and not missing_in_latest_pricelist
            and minimum_balance <= 0
        ):
            diagnostics["excluded_zero_need_count"] += 1
            if (
                sold_last_30_days > 0
                or sold_last_90_days > 0
                or sold_last_180_days > 0
                or sold_last_365_days > 0
                or open_customer_backlog_qty > 0
            ):
                diagnostics["excluded_zero_need_with_sales_count"] += 1
            continue

        search_haystack = " ".join(
            [
                str(oem_number or ""),
                str(latest.get("brand_name") or known_row.get("brand_name") or ""),
                str(latest.get("autopart_name") or known_row.get("autopart_name") or ""),
            ]
        ).lower()
        if search_filter and search_filter not in search_haystack:
            continue

        base_rows.append(
            {
                "autopart_id": latest.get("autopart_id") or known_row.get("autopart_id"),
                "oem_number": oem_number,
                "brand_name": latest.get("brand_name") or known_row.get("brand_name"),
                "autopart_name": (
                    latest.get("autopart_name") or known_row.get("autopart_name")
                ),
                "current_quantity": current_quantity,
                "latest_price": (
                    float(latest.get("latest_price"))
                    if latest.get("latest_price") is not None
                    else (
                        float(known_row.get("latest_price"))
                        if known_row.get("latest_price") is not None
                        else None
                    )
                ),
                "minimum_balance": minimum_balance,
                "multiplicity": multiplicity,
                "in_transit_qty": in_transit_qty,
                "coverable_in_transit_qty": coverable_in_transit_qty,
                "available_qty_for_planning": available_qty_for_planning,
                "cross_group": cross_group,
                "group_available_quantity": group_available_quantity,
                "open_customer_backlog_qty": open_customer_backlog_qty,
                "sold_last_30_days": sold_last_30_days,
                "sold_last_90_days": sold_last_90_days,
                "sold_last_180_days": sold_last_180_days,
                "sold_last_365_days": sold_last_365_days,
                "in_stock_days_90": int(
                    in_stock_days_by_window[90].get(oem_number, 0)
                ),
                "avg_daily_30": avg_daily_30,
                "avg_daily_90": avg_daily_90,
                "avg_daily_blended": avg_daily_blended,
                "estimated_days_left_30_days": estimated_days_left_30_days,
                "consecutive_stockout_days": consecutive_stockout_days,
                "recovery_mode_applied": recovery_mode_applied,
                "average_actual_lead_days": average_actual_lead_days,
                "lead_time_days_used": lead_time_days_used,
                "safety_stock_days": safety_stock_days,
                "safety_stock_qty": safety_stock_qty,
                "reorder_point": reorder_point,
                "target_stock": target_stock,
                "recommended_order_qty": recommended_order_qty,
                "autopurchase_mode": requested_mode,
                "missing_in_latest_pricelist": missing_in_latest_pricelist,
                "abc_xyz": abc_xyz,
                "history_rows": oem_history,
            }
        )

    base_rows.sort(key=_get_autopurchase_priority_key)
    candidate_rows = base_rows[:normalized_limit]

    site_results_by_oem: dict[
        str, tuple[list[dict[str, Any]], list[str], bool, int, int]
    ] = {}
    last_receipt_by_autopart: dict[int, dict[str, Any]] = {}
    candidate_period_metrics: dict[str, dict[str, Any]] = {}
    if candidate_rows:
        # Цена последнего поступления — ориентир закупочной цены
        # из загруженных документов прихода.
        last_receipt_by_autopart = await _load_last_receipt_price_by_autopart(
            session,
            [
                int(candidate_row["autopart_id"])
                for candidate_row in candidate_rows
                if candidate_row.get("autopart_id") is not None
            ],
        )
        # Исторические цены продаж и число заказов по окнам —
        # для ценовых сигналов и блока «Заказы и цены».
        candidate_period_metrics = (
            await _load_customer_order_period_metrics_by_oem(
                session,
                [
                    str(candidate_row["oem_number"])
                    for candidate_row in candidate_rows
                ],
            )
        )
        site_fetch_semaphore = asyncio.Semaphore(AUTOPURCHASE_SITE_FETCH_CONCURRENCY)

        async def _load_site_result(
            candidate_row: dict[str, Any],
        ) -> tuple[str, tuple[list[dict[str, Any]], list[str], bool, int, int]]:
            candidate_oem_number = str(candidate_row["oem_number"])
            try:
                async with site_fetch_semaphore:
                    site_result = await _fetch_site_supplier_stats_for_oem(
                        session,
                        oem_number=candidate_oem_number,
                        brand_name=candidate_row.get("brand_name"),
                        history_rows=list(candidate_row.get("history_rows") or []),
                        autopart_id=candidate_row.get("autopart_id"),
                    )
            except Exception as exc:
                logger.warning(
                    "Ошибка загрузки site-поставщиков для OEM=%s: %s",
                    candidate_oem_number,
                    exc,
                )
                site_result = ([], [], False, 0, 0)
            return candidate_oem_number, site_result

        site_results_by_oem = dict(
            await asyncio.gather(
                *[_load_site_result(candidate_row) for candidate_row in candidate_rows]
            )
        )

    rows: list[dict[str, Any]] = []
    for _row_idx, candidate_row in enumerate(candidate_rows):
        if _row_idx % 50 == 0 and _row_idx > 0:
            await asyncio.sleep(0)

        oem_number = str(candidate_row["oem_number"])
        (
            site_supplier_stats,
            site_query_brands,
            used_site_fallback_brand,
            filtered_other_brand_count,
            cross_targets_count,
        ) = site_results_by_oem.get(oem_number) or ([], [], False, 0, 0)
        best_supplier_by_price = _select_best_site_supplier_by_price(
            site_supplier_stats
        )
        best_supplier_by_lead_time = _select_best_site_supplier_by_lead_time(
            site_supplier_stats
        )
        # Топ предложений сайта по цене — для ручного выбора менеджером.
        top_site_offers = sorted(
            [
                item
                for item in site_supplier_stats
                if item.get("current_price") is not None
                and not item.get("is_own_price")
                and int(item.get("current_qty") or 0) > 0
            ],
            key=lambda item: (
                float(item.get("current_price") or 9_999_999),
                float(item.get("effective_lead_days") or 9_999),
                -int(item.get("current_qty") or 0),
            ),
        )[:AUTOPURCHASE_TOP_OFFERS_LIMIT]

        abc_xyz = candidate_row.get("abc_xyz")
        abc_class = abc_xyz.get("abc_class") if abc_xyz else None
        xyz_class = abc_xyz.get("xyz_class") if abc_xyz else None
        max_allowed_lead_days = _get_max_allowed_lead_days(abc_class, xyz_class)
        supplier_fill_threshold = (
            FILL_RATE_THRESHOLD_AUTO_APPROVE
            if requested_mode == AUTOPURCHASE_MODE_AUTO_APPROVE_SAFE
            else FILL_RATE_THRESHOLD_DRAFT
        )
        # Контроль закупки: latest_price в нашем прайсе — это ПРОДАЖНАЯ
        # цена, закупка обязана быть ниже (потолок 90%, цель 70–80%).
        own_sale_price = candidate_row.get("latest_price")
        max_allowed_purchase_price = (
            _quantize_float(
                float(own_sale_price)
                * AUTOPURCHASE_MAX_PURCHASE_TO_SALE_RATIO
            )
            if own_sale_price is not None and float(own_sale_price) > 0
            else None
        )
        candidate_autopart_id = candidate_row.get("autopart_id")
        last_receipt = (
            last_receipt_by_autopart.get(int(candidate_autopart_id))
            if candidate_autopart_id is not None
            else None
        )
        last_receipt_price = (
            float(last_receipt["price"]) if last_receipt else None
        )
        selected_supplier = _select_autopurchase_supplier(
            site_supplier_stats,
            fill_rate_threshold=supplier_fill_threshold,
            max_allowed_lead_days=max_allowed_lead_days,
            max_allowed_price=max_allowed_purchase_price,
        )
        purchase_stats = _compute_purchase_price_stats(
            candidate_row.get("history_rows") or []
        )
        reasons = list(
            _build_tracking_exceptions(
                own_price_analysis={
                    "current_quantity": candidate_row.get("current_quantity"),
                    "estimated_days_left_30_days": candidate_row.get(
                        "estimated_days_left_30_days"
                    ),
                },
                in_transit_qty=int(candidate_row.get("coverable_in_transit_qty") or 0),
                reorder_point=candidate_row.get("reorder_point"),
                price_trend=purchase_stats.get("price_trend"),
                price_trend_pct=purchase_stats.get("price_trend_pct"),
                best_supplier=selected_supplier,
                best_supplier_by_price=best_supplier_by_price,
                best_supplier_by_lead_time=best_supplier_by_lead_time,
                missing_in_latest_pricelist=bool(
                    candidate_row.get("missing_in_latest_pricelist") or False
                ),
            )
        )
        only_non_positive_site_qty = bool(site_supplier_stats) and all(
            int(item.get("current_qty") or 0) <= 0 for item in site_supplier_stats
        )

        if not SITE_API_KEY:
            reasons.append(
                _build_reason(
                    code="site_api_key_missing",
                    severity="critical",
                    title="Не настроен API-ключ сайта",
                    description=(
                        "Для автозаказа по сайту нужен KEY_FOR_WEBSITE "
                        "в настройках окружения backend."
                    ),
                )
            )

        row_brand_is_dragonzap = _is_dragonzap_brand(
            candidate_row.get("brand_name")
        )
        cheapest_site_price = min(
            (
                float(item["current_price"])
                for item in site_supplier_stats
                if item.get("current_price") is not None
                and not item.get("is_own_price")
                and int(item.get("current_qty") or 0) > 0
            ),
            default=None,
        )
        blocked_by_price_cap = bool(
            not selected_supplier
            and cheapest_site_price is not None
            and max_allowed_purchase_price is not None
            and cheapest_site_price > max_allowed_purchase_price
        )
        if not selected_supplier:
            diagnostics["rows_without_supplier_count"] += 1
            if blocked_by_price_cap:
                reasons.append(
                    _build_reason(
                        code="site_price_above_sale_price",
                        severity="critical",
                        title="Закупка дороже нашей продажной цены",
                        description=(
                            "Самое дешёвое предложение сайта "
                            f"{_format_money_value(cheapest_site_price)} руб. "
                            "превышает потолок закупки "
                            f"{_format_money_value(max_allowed_purchase_price)} руб. "
                            f"({int(AUTOPURCHASE_MAX_PURCHASE_TO_SALE_RATIO * 100)}% "
                            "от нашей продажной цены "
                            f"{_format_money_value(float(own_sale_price))} руб.). "
                            "Заказывать по такой цене невыгодно."
                        ),
                    )
                )
            else:
                if only_non_positive_site_qty:
                    not_found_description = (
                        "Dragonzap вернул предложения, но у них остаток 0 или -1, "
                        "поэтому автозаказ не использует их."
                    )
                elif row_brand_is_dragonzap:
                    not_found_description = (
                        "Dragonzap не вернул подходящее актуальное предложение "
                        "ни по кроссам позиции, ни по самому номеру."
                    )
                else:
                    not_found_description = (
                        "Dragonzap не вернул подходящее актуальное предложение "
                        "по этому OEM строго по бренду "
                        f"{candidate_row.get('brand_name') or '—'}. "
                        "Подмена на другие бренды запрещена правилами закупки."
                    )
                reasons.append(
                    _build_reason(
                        code=(
                            "site_suppliers_without_positive_qty"
                            if only_non_positive_site_qty
                            else "site_supplier_not_found"
                        ),
                        severity="critical",
                        title=(
                            "Сайт вернул только предложения без доступного остатка"
                            if only_non_positive_site_qty
                            else "Сайт не дал подходящего поставщика"
                        ),
                        description=not_found_description,
                    )
                )
        elif selected_supplier.get("fill_rate") is None:
            reasons.append(
                _build_reason(
                    code="supplier_fill_rate_unknown",
                    severity="warning",
                    title="Нет истории исполнения по site-поставщику",
                    description=(
                        "По выбранному site-поставщику ещё нет накопленной "
                        "истории исполнения заказов. Строка требует ручной проверки."
                    ),
                )
            )

        if row_brand_is_dragonzap and cross_targets_count > 0:
            reasons.append(
                _build_reason(
                    code="dragonzap_crosses_used",
                    severity="info",
                    title="Поиск выполнен по кроссам Dragonzap",
                    description=(
                        f"Проверено кроссов: {cross_targets_count}. "
                        "Запросы на сайт (с учётом кроссов сайта): "
                        + ", ".join(site_query_brands[:10])
                        + ". Выбирается самый дешёвый подходящий вариант."
                    ),
                )
            )
        elif row_brand_is_dragonzap and cross_targets_count == 0:
            reasons.append(
                _build_reason(
                    code="dragonzap_no_crosses",
                    severity="warning",
                    title="У Dragonzap-позиции нет кроссов в системе",
                    description=(
                        "Кроссы для этой позиции не заведены, поэтому поиск "
                        "шёл только по бренду и синонимам: "
                        + ", ".join(site_query_brands[:8])
                        + ". Добавьте кроссы (CHERY/HAVAL/GEELY/LIFAN/JAC/"
                        "CHANGAN), чтобы автозаказ находил замену дешевле."
                    ),
                )
            )
        elif used_site_fallback_brand and site_query_brands:
            reasons.append(
                _build_reason(
                    code="site_brand_fallback_used",
                    severity="warning",
                    title="Поиск на сайте ушёл в fallback-бренд",
                    description=(
                        "Синонимы Dragonzap не дали результата, поэтому сайт "
                        "проверяли через бренд: "
                        + ", ".join(site_query_brands[:8])
                        + ". Такая строка не автоутверждается и требует "
                        "ручной проверки."
                    ),
                )
            )

        if filtered_other_brand_count > 0:
            reasons.append(
                _build_reason(
                    code="site_offers_other_brand_filtered",
                    severity="info",
                    title="Отфильтрованы предложения чужих брендов",
                    description=(
                        f"Сайт вернул {filtered_other_brand_count} предложений "
                        "других брендов — они исключены, потому что для этой "
                        "позиции разрешён только бренд из нашего прайса"
                        + (
                            " и его синонимы Dragonzap."
                            if row_brand_is_dragonzap
                            else "."
                        )
                    ),
                )
            )

        open_customer_backlog_qty = int(
            candidate_row.get("open_customer_backlog_qty") or 0
        )
        if open_customer_backlog_qty > 0:
            reasons.append(
                _build_reason(
                    code="open_customer_backlog",
                    severity="info",
                    title="Есть открытый клиентский backlog",
                    description=(
                        "По OEM есть незакрытая потребность клиентов на "
                        f"{open_customer_backlog_qty} шт. "
                        "Расчёт вычитает этот backlog из свободного остатка."
                    ),
                )
            )

        if candidate_row.get("recovery_mode_applied"):
            reasons.append(
                _build_reason(
                    code="recovery_mode_applied",
                    severity="info",
                    title="Включён recovery mode",
                    description=(
                        "Позиция долго была без остатка, поэтому расчёт поднял "
                        "спрос по длинной истории 180/365 дней, чтобы вернуть её "
                        "в оборот."
                    ),
                )
            )

        recommended_order_qty = int(candidate_row.get("recommended_order_qty") or 0)
        # План закрытия потребности: если у лучшего поставщика не хватает
        # количества, добираем у следующих по цене (целыми партиями).
        auto_allocations, covered_supply_qty = _plan_auto_allocations(
            top_site_offers,
            needed_qty=recommended_order_qty,
            max_allowed_price=max_allowed_purchase_price,
        )
        if (
            selected_supplier
            and int(selected_supplier.get("current_qty") or 0) < recommended_order_qty
        ):
            diagnostics["rows_with_partial_supplier_qty_count"] += 1
            if covered_supply_qty >= recommended_order_qty and len(auto_allocations) > 1:
                reasons.append(
                    _build_reason(
                        code="need_split_across_suppliers",
                        severity="info",
                        title="Потребность закрывается несколькими поставщиками",
                        description=(
                            f"У лучшего поставщика только "
                            f"{int(selected_supplier.get('current_qty') or 0)} шт, "
                            f"поэтому {covered_supply_qty} шт распределены на "
                            f"{len(auto_allocations)} предложений "
                            "(по возрастанию цены, целыми партиями)."
                        ),
                    )
                )
            else:
                reasons.append(
                    _build_reason(
                        code="site_qty_less_than_required",
                        severity="warning",
                        title="У лучшего site-поставщика не хватает количества",
                        description=(
                            f"Сайт даёт только {int(selected_supplier.get('current_qty') or 0)} "
                            f"шт при потребности {recommended_order_qty} шт, "
                            f"всеми предложениями закрывается {covered_supply_qty} шт. "
                            "Позиция требует ручного решения или дополнительного дозаказа."
                        ),
                    )
                )

        # Маржинальность: закупка дороже целевых 70–80% от продажи —
        # автоутверждение запрещаем, оставляем менеджеру.
        purchase_margin_below_target = False
        selected_price = (
            float(selected_supplier["current_price"])
            if selected_supplier
            and selected_supplier.get("current_price") is not None
            else None
        )
        if (
            selected_price is not None
            and own_sale_price is not None
            and float(own_sale_price) > 0
            and selected_price
            > float(own_sale_price) * AUTOPURCHASE_TARGET_PURCHASE_TO_SALE_RATIO
        ):
            purchase_margin_below_target = True
            margin_pct = (
                (float(own_sale_price) - selected_price)
                / float(own_sale_price)
                * 100
            )
            reasons.append(
                _build_reason(
                    code="purchase_margin_below_target",
                    severity="warning",
                    title="Маржа ниже целевых 20–30%",
                    description=(
                        f"Закупка {_format_money_value(selected_price)} руб. "
                        "при нашей продажной цене "
                        f"{_format_money_value(float(own_sale_price))} руб. — "
                        f"маржа всего {margin_pct:.0f}%. Целевая закупка не "
                        f"дороже {int(AUTOPURCHASE_TARGET_PURCHASE_TO_SALE_RATIO * 100)}% "
                        "от продажной цены."
                    ),
                )
            )
        if (
            selected_price is not None
            and last_receipt_price is not None
            and last_receipt_price > 0
            and selected_price > last_receipt_price * 1.1
        ):
            reasons.append(
                _build_reason(
                    code="purchase_above_last_receipt",
                    severity="warning",
                    title="Дороже последнего поступления",
                    description=(
                        f"Закупка {_format_money_value(selected_price)} руб. "
                        "дороже цены последнего поступления "
                        f"{_format_money_value(last_receipt_price)} руб. "
                        "более чем на 10%."
                    ),
                )
            )

        # ── Ценовые сигналы ──────────────────────────────────────────
        # Отсутствие продаж или их всплеск может объясняться нашей ценой,
        # а не реальным спросом — предупреждаем менеджера.
        period_metrics = candidate_period_metrics.get(oem_number, {})
        min_sale_price_365 = period_metrics.get("min_sale_price_365_days")
        sold_30 = int(candidate_row.get("sold_last_30_days") or 0)
        sold_90 = int(candidate_row.get("sold_last_90_days") or 0)
        sold_365 = int(candidate_row.get("sold_last_365_days") or 0)
        in_stock_days_90 = int(candidate_row.get("in_stock_days_90") or 0)

        if (
            sold_90 <= 0
            and sold_365 > 0
            and in_stock_days_90 >= AUTOPURCHASE_NO_SALES_MIN_STOCK_DAYS
            and own_sale_price is not None
            and min_sale_price_365 is not None
            and float(own_sale_price)
            > float(min_sale_price_365) * AUTOPURCHASE_PRICE_HIGH_FACTOR
        ):
            price_over_pct = (
                (float(own_sale_price) - float(min_sale_price_365))
                / float(min_sale_price_365)
                * 100
            )
            reasons.append(
                _build_reason(
                    code="no_sales_price_suspect",
                    severity="warning",
                    title="Нет продаж при наличии — возможно, мешает цена",
                    description=(
                        f"Товар был в наличии {in_stock_days_90} дн за "
                        "последние 90 дней, но продаж нет, хотя раньше "
                        f"продавался ({sold_365} шт за год). Текущая цена "
                        f"{_format_money_value(float(own_sale_price))} руб. "
                        "выше исторической цены продаж "
                        f"{_format_money_value(float(min_sale_price_365))} руб. "
                        f"на {price_over_pct:.0f}% — проверьте цену перед "
                        "дозаказом."
                    ),
                )
            )

        demand_spike_price_suspect = False
        if (
            sold_30 >= AUTOPURCHASE_DEMAND_SPIKE_MIN_QTY
            and sold_365 > 0
            and (sold_30 / 30.0)
            >= AUTOPURCHASE_DEMAND_SPIKE_FACTOR * (sold_365 / 365.0)
        ):
            spike_factor = (sold_30 / 30.0) / (sold_365 / 365.0)
            price_at_low = bool(
                own_sale_price is not None
                and min_sale_price_365 is not None
                and float(own_sale_price) <= float(min_sale_price_365)
            )
            demand_spike_price_suspect = price_at_low
            reasons.append(
                _build_reason(
                    code=(
                        "demand_spike_price_low"
                        if price_at_low
                        else "demand_spike"
                    ),
                    severity="warning" if price_at_low else "info",
                    title=(
                        "Всплеск спроса при цене на минимуме"
                        if price_at_low
                        else "Всплеск спроса"
                    ),
                    description=(
                        f"Продажи за 30 дней ({sold_30} шт) в "
                        f"{spike_factor:.1f} раза выше среднегодового темпа."
                        + (
                            " Текущая цена на историческом минимуме — "
                            "проверьте, не занижена ли цена (возможна "
                            "ошибка), прежде чем заказывать под такой "
                            "спрос."
                            if price_at_low
                            else " Если это сезон или акция — учтите при "
                            "подтверждении объёма."
                        )
                    ),
                )
            )

        decision_value = AUTOPURCHASE_STATUS_NEEDS_REVIEW
        if requested_mode == AUTOPURCHASE_MODE_DISABLED:
            decision_value = AUTOPURCHASE_STATUS_BLOCKED
            reasons.append(
                _build_reason(
                    code="autopurchase_disabled",
                    severity="info",
                    title="Автозаказ отключён",
                    description="Запуск выполнен в режиме без автоутверждения строк.",
                )
            )
        elif not selected_supplier:
            decision_value = AUTOPURCHASE_STATUS_BLOCKED
        elif bool(candidate_row.get("missing_in_latest_pricelist") or False):
            decision_value = AUTOPURCHASE_STATUS_NEEDS_REVIEW
        elif used_site_fallback_brand:
            # Предложение найдено по fallback-бренду, которого нет среди
            # подтверждённых синонимов — только ручное решение менеджера.
            decision_value = AUTOPURCHASE_STATUS_NEEDS_REVIEW
        elif purchase_margin_below_target:
            # Маржа ниже целевой — автоутверждение запрещено.
            decision_value = AUTOPURCHASE_STATUS_NEEDS_REVIEW
        elif demand_spike_price_suspect:
            # Всплеск спроса при цене на минимуме — возможно, цена занижена
            # ошибочно; заказ под такой спрос только после ручной проверки.
            decision_value = AUTOPURCHASE_STATUS_NEEDS_REVIEW
        elif (
            requested_mode == AUTOPURCHASE_MODE_AUTO_APPROVE_SAFE
            and (
                # Стандартный путь: накопленный fill_rate выше порога
                (
                    selected_supplier.get("fill_rate") is not None
                    and float(selected_supplier.get("fill_rate") or 0)
                    >= FILL_RATE_THRESHOLD_AUTO_APPROVE
                )
                # Bootstrap-путь: нет истории вообще (новая система/поставщик),
                # но поставщик найден и готов дать нужное количество.
                # Заказываем с предупреждением — fill_rate накопится после первых заказов.
                or selected_supplier.get("fill_rate") is None
            )
            and (
                max_allowed_lead_days is None
                or selected_supplier.get("effective_lead_days") is None
                or float(selected_supplier.get("effective_lead_days") or 0)
                <= float(max_allowed_lead_days)
            )
            # Потребность закрыта: одним поставщиком или авто-распределением
            # по нескольким (целыми партиями).
            and covered_supply_qty >= recommended_order_qty
        ):
            decision_value = AUTOPURCHASE_STATUS_AUTO_APPROVED

        if decision_filter and decision_value != decision_filter:
            continue

        limited_reasons = _limit_reasons(reasons)

        draft_purchase_order = _build_autopurchase_draft(
            supplier=selected_supplier,
            available_qty=int(candidate_row.get("available_qty_for_planning") or 0),
            in_transit_qty=int(candidate_row.get("in_transit_qty") or 0),
            target_qty=candidate_row.get("target_stock"),
            recommended_qty=recommended_order_qty,
            lead_time_days_used=candidate_row.get("lead_time_days_used"),
            open_customer_backlog_qty=open_customer_backlog_qty,
            last_receipt_price=last_receipt_price,
            reason=next(
                (
                    reason["title"]
                    for reason in reasons
                    if reason.get("severity") == "critical"
                ),
                None,
            ),
        )
        # Авто-распределение по нескольким поставщикам: если один лучший
        # не закрывает потребность, черновик содержит разбивку.
        if (
            draft_purchase_order
            and len(auto_allocations) > 1
            and covered_supply_qty
            > int(draft_purchase_order.get("proposed_order_qty") or 0)
        ):
            draft_purchase_order["allocations"] = auto_allocations
            draft_purchase_order["proposed_order_qty"] = covered_supply_qty
            draft_purchase_order["remaining_gap_qty"] = max(
                recommended_order_qty - covered_supply_qty, 0
            )

        if candidate_row.get("missing_in_latest_pricelist"):
            diagnostics["rows_missing_in_latest_pricelist_count"] += 1
        if draft_purchase_order:
            diagnostics["rows_ready_for_draft_count"] += 1
        else:
            diagnostics["rows_without_draft_count"] += 1

        rows.append(
            {
                "autopart_id": candidate_row.get("autopart_id"),
                "oem_number": oem_number,
                "brand_name": candidate_row.get("brand_name"),
                "autopart_name": candidate_row.get("autopart_name"),
                "current_quantity": int(candidate_row.get("current_quantity") or 0),
                "latest_price": candidate_row.get("latest_price"),
                "last_receipt_price": last_receipt_price,
                "max_allowed_purchase_price": max_allowed_purchase_price,
                "minimum_balance": int(candidate_row.get("minimum_balance") or 0),
                "multiplicity": int(candidate_row.get("multiplicity") or 1),
                "in_transit_qty": int(candidate_row.get("in_transit_qty") or 0),
                "coverable_in_transit_qty": int(
                    candidate_row.get("coverable_in_transit_qty") or 0
                ),
                "open_customer_backlog_qty": open_customer_backlog_qty,
                "consecutive_stockout_days": int(
                    candidate_row.get("consecutive_stockout_days") or 0
                ),
                "recovery_mode_applied": bool(
                    candidate_row.get("recovery_mode_applied") or False
                ),
                "sold_last_30_days": int(candidate_row.get("sold_last_30_days") or 0),
                "sold_last_90_days": int(candidate_row.get("sold_last_90_days") or 0),
                "sold_last_180_days": int(
                    candidate_row.get("sold_last_180_days") or 0
                ),
                "sold_last_365_days": int(
                    candidate_row.get("sold_last_365_days") or 0
                ),
                **{
                    metric_key: metric_value
                    for metric_key, metric_value in period_metrics.items()
                },
                "avg_daily_30": candidate_row.get("avg_daily_30"),
                "avg_daily_90": candidate_row.get("avg_daily_90"),
                "avg_daily_blended": candidate_row.get("avg_daily_blended"),
                "estimated_days_left_30_days": candidate_row.get(
                    "estimated_days_left_30_days"
                ),
                "average_actual_lead_days": candidate_row.get(
                    "average_actual_lead_days"
                ),
                "lead_time_days_used": candidate_row.get("lead_time_days_used"),
                "safety_stock_days": candidate_row.get("safety_stock_days"),
                "safety_stock_qty": candidate_row.get("safety_stock_qty"),
                "reorder_point": candidate_row.get("reorder_point"),
                "target_stock": candidate_row.get("target_stock"),
                "recommended_order_qty": recommended_order_qty,
                "decision_status": decision_value,
                "autopurchase_mode": requested_mode,
                "missing_in_latest_pricelist": bool(
                    candidate_row.get("missing_in_latest_pricelist") or False
                ),
                "reason_codes": [item["code"] for item in limited_reasons],
                "reason_titles": [item["title"] for item in limited_reasons],
                "reasons": limited_reasons,
                "abc_xyz": abc_xyz,
                "best_supplier_by_price": best_supplier_by_price,
                "best_supplier_by_lead_time": best_supplier_by_lead_time,
                "recommended_supplier": selected_supplier,
                "draft_purchase_order": draft_purchase_order,
                "top_site_offers": top_site_offers,
                "cross_group": candidate_row.get("cross_group"),
                "site_query_brands": site_query_brands,
                "used_site_fallback_brand": used_site_fallback_brand,
            }
        )

    rows.sort(
        key=lambda item: (
            decision_rank.get(item["decision_status"], 99),
            *_get_autopurchase_priority_key(item),
        )
    )
    return {
        "provider_config_id": provider_config_id,
        "provider_id": int(config_row["provider_id"]),
        "provider_name": config_row["provider_name"],
        "provider_config_name": config_row.get("provider_config_name"),
        "generated_at": now_moscow(),
        "mode": requested_mode,
        "supplier_source": "site",
        "total_items": len(rows),
        "auto_approved_count": sum(
            1
            for row in rows
            if row["decision_status"] == AUTOPURCHASE_STATUS_AUTO_APPROVED
        ),
        "needs_review_count": sum(
            1
            for row in rows
            if row["decision_status"] == AUTOPURCHASE_STATUS_NEEDS_REVIEW
        ),
        "blocked_count": sum(
            1
            for row in rows
            if row["decision_status"] == AUTOPURCHASE_STATUS_BLOCKED
        ),
        "diagnostics": [
            _build_autopurchase_diagnostic_metric(
                code="oems_in_own_pricelist_count",
                title="OEM в нашем прайсе",
                value=diagnostics["oems_in_own_pricelist_count"],
            ),
            _build_autopurchase_diagnostic_metric(
                code="oems_with_sales_signal_count",
                title="Позиции с сигналом спроса",
                value=diagnostics["oems_with_sales_signal_count"],
                description=(
                    "Позиции, по которым расчёт увидел клиентский спрос по "
                    "заказам и/или движение товара по нашим снапшотам "
                    "за окна 30/90/180/365 дней."
                ),
            ),
            _build_autopurchase_diagnostic_metric(
                code="oems_with_open_backlog_count",
                title="Позиции с открытым backlog",
                value=diagnostics["oems_with_open_backlog_count"],
                description=(
                    "Позиции, по которым есть незакрытые клиентские потребности, "
                    "уменьшающие свободный остаток для планирования."
                ),
            ),
            _build_autopurchase_diagnostic_metric(
                code="recovery_mode_used_count",
                title="Recovery mode включался",
                value=diagnostics["recovery_mode_used_count"],
                description=(
                    "Позиции, которые долго были без остатка и для которых "
                    "расчёт поднял спрос по длинной истории 180/365 дней."
                ),
            ),
            _build_autopurchase_diagnostic_metric(
                code="fallback_lead_time_used_count",
                title="Использован fallback по сроку поставки",
                value=diagnostics["fallback_lead_time_used_count"],
                description=(
                    "Позиции со спросом, где не было истории фактического срока "
                    "поставки и применён дефолтный lead time."
                ),
            ),
            _build_autopurchase_diagnostic_metric(
                code="fallback_lead_time_sales_only_count",
                title="Fallback сработал без min balance",
                value=diagnostics["fallback_lead_time_sales_only_count"],
                description=(
                    "Позиции со спросом, для которых потребность удалось посчитать "
                    "даже при нулевом минимальном остатке."
                ),
            ),
            _build_autopurchase_diagnostic_metric(
                code="excluded_zero_need_count",
                title="Отсечено с нулевой потребностью",
                value=diagnostics["excluded_zero_need_count"],
            ),
            _build_autopurchase_diagnostic_metric(
                code="excluded_zero_need_with_sales_count",
                title="Нулевая потребность при наличии продаж",
                value=diagnostics["excluded_zero_need_with_sales_count"],
                description=(
                    "Самый важный индикатор для диагностики: позиции имели спрос "
                    "или backlog, но расчёт всё равно не вывел их в потребность."
                ),
            ),
            _build_autopurchase_diagnostic_metric(
                code="rows_without_supplier_count",
                title="Строк без site-поставщика",
                value=diagnostics["rows_without_supplier_count"],
            ),
            _build_autopurchase_diagnostic_metric(
                code="rows_without_draft_count",
                title="Строк без готового черновика",
                value=diagnostics["rows_without_draft_count"],
            ),
            _build_autopurchase_diagnostic_metric(
                code="rows_ready_for_draft_count",
                title="Строк, готовых к черновику",
                value=diagnostics["rows_ready_for_draft_count"],
            ),
        ],
        "rows": rows,
    }


async def create_autopurchase_run(
    session: AsyncSession,
    *,
    initiated_by_user_id: Optional[int],
    own_provider_config_id: Optional[int] = None,
    mode: str = AUTOPURCHASE_MODE_DRAFT_ONLY,
    limit: int = 1000,
    budget_limit: Optional[float] = None,
    position_limit: Optional[int] = None,
    trigger_source: str = "manual",
) -> dict[str, Any]:
    """Create a queued run record and return immediately.
    The scheduler container picks it up and executes the heavy calculation.
    """
    requested_mode = str(mode or AUTOPURCHASE_MODE_DRAFT_ONLY).strip().lower()
    normalized_budget_limit = _normalize_optional_positive_float(budget_limit)
    normalized_position_limit = _normalize_optional_positive_int(position_limit)

    if session.in_transaction():
        await session.rollback()

    async with session.begin():
        lock_acquired = await _try_acquire_autopurchase_run_lock(session)
        if not lock_acquired:
            raise ValueError(
                "Сейчас уже выполняется расчёт автозаказа. "
                "Дождитесь завершения текущего запуска и попробуйте снова."
            )

        active_run = await _get_active_autopurchase_run(session)
        if active_run is not None:
            raise ValueError(
                "Сейчас уже выполняется расчёт автозаказа. "
                "Дождитесь завершения текущего запуска и попробуйте снова."
            )

        config_row = await _resolve_autopurchase_provider_config(
            session,
            own_provider_config_id=own_provider_config_id,
        )
        run = AutoPurchaseRun(
            provider_config_id=int(config_row["provider_config_id"]),
            provider_id=int(config_row["provider_id"]),
            initiated_by_user_id=initiated_by_user_id,
            started_at=now_moscow(),
            finished_at=None,
            status=AUTOPURCHASE_RUN_STATUS_QUEUED,
            mode=requested_mode,
            trigger_source=str(trigger_source or "manual"),
            used_local_prices_only=False,
            settings_snapshot=_build_run_settings(
                own_provider_config_id=own_provider_config_id,
                mode=requested_mode,
                limit=limit,
                budget_limit=normalized_budget_limit,
                position_limit=normalized_position_limit,
            ),
            summary_snapshot=_build_initial_run_summary(
                config_row,
                status=AUTOPURCHASE_RUN_STATUS_QUEUED,
            ),
        )
        session.add(run)
        await session.flush()
        run_id = int(run.id)

    run_record = await session.get(AutoPurchaseRun, run_id)
    return _serialize_autopurchase_run(run_record)


async def _send_autopurchase_run_telegram_summary(
    *,
    run_id: int,
    preview: dict[str, Any],
) -> None:
    from dz_fastapi.services.telegram import send_message_to_telegram

    approved_sum = 0.0
    approved_qty = 0
    for row in preview.get("rows", []):
        if row.get("decision_status") != AUTOPURCHASE_STATUS_AUTO_APPROVED:
            continue
        draft = row.get("draft_purchase_order") or {}
        qty = int(draft.get("proposed_order_qty") or 0)
        price = float(draft.get("price") or 0)
        approved_qty += qty
        approved_sum += qty * price

    lines = [
        f"🤖 Автозаказ: ночной расчёт #{run_id} готов.",
        f"Позиции с потребностью: {int(preview.get('total_items') or 0)}",
        (
            f"Готово к заказу: {int(preview.get('auto_approved_count') or 0)} поз. "
            f"на {approved_qty} шт ≈ {_format_money_value(approved_sum)} руб."
        ),
        f"На ручную проверку: {int(preview.get('needs_review_count') or 0)} поз.",
        f"Заблокировано: {int(preview.get('blocked_count') or 0)} поз.",
        "Открой раздел «Автозаказ», проверь и отправь черновики.",
    ]
    await send_message_to_telegram("\n".join(lines))


async def execute_next_autopurchase_run(
    session: AsyncSession,
) -> Optional[int]:
    """Claim and execute the next queued/running unfinished autopurchase run."""
    if session.in_transaction():
        await session.rollback()

    run_id: Optional[int] = None
    own_provider_config_id: Optional[int] = None
    mode = AUTOPURCHASE_MODE_DRAFT_ONLY
    limit = AUTOPURCHASE_MAX_LIMIT
    run_trigger_source = "manual"

    async with session.begin():
        lock_acquired = await _try_acquire_autopurchase_run_lock(session)
        if not lock_acquired:
            return None

        run = (
            await session.execute(
                select(AutoPurchaseRun)
                .where(
                    AutoPurchaseRun.finished_at.is_(None),
                    AutoPurchaseRun.status.in_(
                        [
                            AUTOPURCHASE_RUN_STATUS_QUEUED,
                            AUTOPURCHASE_RUN_STATUS_RUNNING,
                        ]
                    ),
                )
                .order_by(AutoPurchaseRun.started_at.asc(), AutoPurchaseRun.id.asc())
                .limit(1)
            )
        ).scalar_one_or_none()
        if run is None:
            return None

        run_id = int(run.id)
        run_trigger_source = str(run.trigger_source or "manual")
        run.status = AUTOPURCHASE_RUN_STATUS_RUNNING
        summary_snapshot = dict(run.summary_snapshot or {})
        summary_snapshot["message"] = _build_run_summary_message(
            AUTOPURCHASE_RUN_STATUS_RUNNING
        )
        run.summary_snapshot = summary_snapshot

        settings = dict(run.settings_snapshot or {})
        own_provider_config_id = settings.get("own_provider_config_id")
        mode = str(settings.get("mode") or run.mode or AUTOPURCHASE_MODE_DRAFT_ONLY)
        try:
            limit = int(settings.get("limit") or AUTOPURCHASE_MAX_LIMIT)
        except (TypeError, ValueError):
            limit = AUTOPURCHASE_MAX_LIMIT

    if run_id is None:
        return None

    try:
        preview = await get_autopurchase_preview(
            session,
            own_provider_config_id=own_provider_config_id,
            mode=mode,
            limit=limit,
        )

        if session.in_transaction():
            await session.rollback()
        async with session.begin():
            await _persist_autopurchase_preview(
                session,
                run_id=run_id,
                preview=preview,
            )

        logger.info("Autopurchase run %s completed successfully", run_id)

        # Утренняя сводка по ночному (scheduled) расчёту в Telegram.
        if run_trigger_source == "scheduled":
            try:
                await _send_autopurchase_run_telegram_summary(
                    run_id=run_id,
                    preview=preview,
                )
            except Exception as notify_exc:
                logger.warning(
                    "Не удалось отправить Telegram-сводку автозаказа "
                    "run_id=%s: %s",
                    run_id,
                    notify_exc,
                )
        return run_id

    except Exception as exc:
        logger.exception("Autopurchase run failed run_id=%s: %s", run_id, exc)
        failure_message = str(exc).strip()
        if failure_message:
            failure_message = f"{exc.__class__.__name__}: {failure_message}"
        else:
            failure_message = exc.__class__.__name__
        if session.in_transaction():
            await session.rollback()
        async with session.begin():
            failed_run = await session.get(AutoPurchaseRun, run_id)
            if failed_run is not None:
                failed_summary = dict(failed_run.summary_snapshot or {})
                failed_summary["message"] = failure_message
                failed_run.summary_snapshot = failed_summary
                failed_run.finished_at = now_moscow()
                failed_run.status = AUTOPURCHASE_RUN_STATUS_FAILED
                await _prune_old_finished_autopurchase_runs(session)
        return run_id


async def list_autopurchase_runs(
    session: AsyncSession,
    *,
    limit: int = 50,
) -> list[dict[str, Any]]:
    normalized_limit = max(1, int(limit or 50))

    active_stmt = (
        select(AutoPurchaseRun)
        .where(AutoPurchaseRun.finished_at.is_(None))
        .order_by(AutoPurchaseRun.started_at.desc(), AutoPurchaseRun.id.desc())
        .limit(normalized_limit)
    )
    active_runs = list((await session.execute(active_stmt)).scalars().all())

    remaining_limit = max(normalized_limit - len(active_runs), 0)
    finished_runs: list[AutoPurchaseRun] = []
    if remaining_limit > 0 and AUTOPURCHASE_FINISHED_HISTORY_LIMIT > 0:
        finished_stmt = (
            select(AutoPurchaseRun)
            .where(AutoPurchaseRun.finished_at.is_not(None))
            .order_by(
                AutoPurchaseRun.finished_at.desc(),
                AutoPurchaseRun.started_at.desc(),
                AutoPurchaseRun.id.desc(),
            )
            .limit(min(remaining_limit, AUTOPURCHASE_FINISHED_HISTORY_LIMIT))
        )
        finished_runs = list(
            (await session.execute(finished_stmt)).scalars().all()
        )

    runs = sorted(
        [*active_runs, *finished_runs],
        key=lambda run: (
            run.finished_at or run.started_at or now_moscow(),
            run.started_at or now_moscow(),
            int(run.id),
        ),
        reverse=True,
    )[:normalized_limit]
    return [_serialize_autopurchase_run(run) for run in runs]


async def get_autopurchase_run(
    session: AsyncSession,
    *,
    run_id: int,
) -> dict[str, Any]:
    stmt = select(AutoPurchaseRun).where(AutoPurchaseRun.id == run_id)
    run = (await session.execute(stmt)).scalar_one_or_none()
    if run is None:
        raise ValueError("Запуск автозаказа не найден")
    return _serialize_autopurchase_run(run)


async def get_autopurchase_run_items(
    session: AsyncSession,
    *,
    run_id: int,
    decision_status: Optional[str] = None,
    search: Optional[str] = None,
    limit: int = 1000,
) -> dict[str, Any]:
    run_stmt = select(AutoPurchaseRun).where(AutoPurchaseRun.id == run_id)
    run = (await session.execute(run_stmt)).scalar_one_or_none()
    if run is None:
        raise ValueError("Запуск автозаказа не найден")

    stmt = select(AutoPurchaseRunItem).where(AutoPurchaseRunItem.run_id == run_id)
    items = (await session.execute(stmt)).scalars().all()
    item_oem_numbers = sorted(
        {
            _normalize_oem(item.oem_number)
            for item in items
            if _normalize_oem(item.oem_number)
        }
    )
    customer_order_period_metrics = await _load_customer_order_period_metrics_by_oem(
        session,
        item_oem_numbers,
    )
    # Backlog считаем по живым данным, потому что в снапшоте строки он не
    # сохраняется, а от него зависит приоритет вывода.
    open_backlog_by_oem = await _load_open_customer_backlog_by_oem(
        session,
        item_oem_numbers,
    )

    decision_filter = str(decision_status or "").strip().lower() or None
    search_filter = str(search or "").strip().lower()
    filtered_rows: list[dict[str, Any]] = []
    decision_rank = {
        AUTOPURCHASE_STATUS_AUTO_APPROVED: 0,
        AUTOPURCHASE_STATUS_NEEDS_REVIEW: 1,
        AUTOPURCHASE_STATUS_BLOCKED: 2,
    }
    for item in items:
        serialized = _serialize_autopurchase_run_item(item)
        serialized.update(
            customer_order_period_metrics.get(
                _normalize_oem(serialized.get("oem_number")),
                {},
            )
        )
        serialized["open_customer_backlog_qty"] = int(
            open_backlog_by_oem.get(
                _normalize_oem(serialized.get("oem_number")) or "",
                serialized.get("open_customer_backlog_qty") or 0,
            )
        )
        if decision_filter and serialized["decision_status"] != decision_filter:
            continue
        if search_filter:
            search_blob = " ".join(
                [
                    str(serialized.get("brand_name") or ""),
                    str(serialized.get("oem_number") or ""),
                    str(serialized.get("autopart_name") or ""),
                    str(
                        (serialized.get("recommended_supplier") or {}).get(
                            "provider_name"
                        )
                        or ""
                    ),
                ]
            ).lower()
            if search_filter not in search_blob:
                continue
        filtered_rows.append(serialized)

    filtered_rows.sort(
        key=lambda item: (
            decision_rank.get(item["decision_status"], 99),
            *_get_autopurchase_priority_key(item),
        )
    )

    return {
        "run": _serialize_autopurchase_run(run),
        "total_items": len(filtered_rows),
        "rows": filtered_rows[:limit],
    }


async def get_autopurchase_run_item_ai_explanation(
    session: AsyncSession,
    *,
    run_id: int,
    item_id: int,
) -> dict[str, Any]:
    run_stmt = select(AutoPurchaseRun).where(AutoPurchaseRun.id == run_id)
    run = (await session.execute(run_stmt)).scalar_one_or_none()
    if run is None:
        raise ValueError("Запуск автозаказа не найден")

    item_stmt = select(AutoPurchaseRunItem).where(
        AutoPurchaseRunItem.run_id == run_id,
        AutoPurchaseRunItem.id == item_id,
    )
    item = (await session.execute(item_stmt)).scalar_one_or_none()
    if item is None:
        raise ValueError("Строка автозаказа не найдена")

    return await _generate_autopurchase_ai_payload(run=run, item=item)


async def update_autopurchase_run_item_status(
    session: AsyncSession,
    *,
    run_id: int,
    item_id: int,
    decision_status: str,
    comment: Optional[str] = None,
) -> dict[str, Any]:
    normalized_status = _validate_autopurchase_decision_status(decision_status)

    run_stmt = select(AutoPurchaseRun).where(AutoPurchaseRun.id == run_id)
    run = (await session.execute(run_stmt)).scalar_one_or_none()
    if run is None:
        raise ValueError("Запуск автозаказа не найден")

    item_stmt = select(AutoPurchaseRunItem).where(
        AutoPurchaseRunItem.run_id == run_id,
        AutoPurchaseRunItem.id == item_id,
    )
    item = (await session.execute(item_stmt)).scalar_one_or_none()
    if item is None:
        raise ValueError("Строка автозаказа не найдена")

    _apply_autopurchase_item_status_override(
        item,
        decision_status=normalized_status,
        comment=comment,
    )

    items_stmt = select(AutoPurchaseRunItem).where(
        AutoPurchaseRunItem.run_id == run_id
    )
    all_items = list((await session.execute(items_stmt)).scalars().all())
    _refresh_run_summary_snapshot(run, all_items)
    await session.commit()
    return {
        "run": _serialize_autopurchase_run(run),
        "item": _serialize_autopurchase_run_item(item),
    }


async def update_autopurchase_run_items_status(
    session: AsyncSession,
    *,
    run_id: int,
    item_ids: list[int],
    decision_status: str,
    comment: Optional[str] = None,
) -> dict[str, Any]:
    normalized_status = _validate_autopurchase_decision_status(decision_status)
    normalized_item_ids = sorted(
        {
            int(item_id)
            for item_id in (item_ids or [])
            if str(item_id).strip()
        }
    )
    if not normalized_item_ids:
        raise ValueError("Не переданы item_ids для массового обновления")

    run_stmt = select(AutoPurchaseRun).where(AutoPurchaseRun.id == run_id)
    run = (await session.execute(run_stmt)).scalar_one_or_none()
    if run is None:
        raise ValueError("Запуск автозаказа не найден")

    items_stmt = select(AutoPurchaseRunItem).where(
        AutoPurchaseRunItem.run_id == run_id,
        AutoPurchaseRunItem.id.in_(normalized_item_ids),
    )
    items = list((await session.execute(items_stmt)).scalars().all())
    found_item_ids = {int(item.id) for item in items}
    missing_item_ids = [
        item_id for item_id in normalized_item_ids if item_id not in found_item_ids
    ]
    if missing_item_ids:
        raise ValueError(
            "Не найдены строки автозаказа: "
            + ", ".join(str(item_id) for item_id in missing_item_ids)
        )

    for item in items:
        _apply_autopurchase_item_status_override(
            item,
            decision_status=normalized_status,
            comment=comment,
        )

    all_items_stmt = select(AutoPurchaseRunItem).where(
        AutoPurchaseRunItem.run_id == run_id
    )
    all_items = list((await session.execute(all_items_stmt)).scalars().all())
    _refresh_run_summary_snapshot(run, all_items)
    await session.commit()

    return {
        "run": _serialize_autopurchase_run(run),
        "updated_items": [
            _serialize_autopurchase_run_item(item) for item in items
        ],
    }


def _build_allocation_from_offer(
    offer: dict[str, Any],
    quantity: int,
) -> dict[str, Any]:
    return {
        "provider_id": offer.get("provider_id"),
        "external_supplier_id": offer.get("external_supplier_id"),
        "provider_name": offer.get("provider_name") or "—",
        "provider_config_name": offer.get("current_provider_config_name"),
        "source_type": offer.get("source_type"),
        "sup_logo": offer.get("sup_logo"),
        "price": offer.get("current_price"),
        "quantity": int(quantity),
        "supplier_available_qty": int(offer.get("current_qty") or 0),
        "min_qnt": offer.get("current_min_qnt"),
        "min_delivery_day": offer.get("current_min_delivery"),
        "max_delivery_day": offer.get("current_max_delivery"),
        "hash_key": offer.get("hash_key"),
        "system_hash": offer.get("system_hash"),
        "oem_number": offer.get("current_oem_number") or "",
        "brand_name": offer.get("current_brand_name"),
        "autopart_name": offer.get("current_autopart_name"),
    }


async def update_autopurchase_run_item_allocations(
    session: AsyncSession,
    *,
    run_id: int,
    item_id: int,
    allocations: list[dict[str, Any]],
    comment: Optional[str] = None,
) -> dict[str, Any]:
    """Ручной выбор предложений из топ-10 (одно или несколько).

    Менеджер может заменить выбор автозаказа или распределить количество
    между несколькими предложениями. Строка после этого считается
    подтверждённой вручную.
    """
    run_stmt = select(AutoPurchaseRun).where(AutoPurchaseRun.id == run_id)
    run = (await session.execute(run_stmt)).scalar_one_or_none()
    if run is None:
        raise ValueError("Запуск автозаказа не найден")

    item_stmt = select(AutoPurchaseRunItem).where(
        AutoPurchaseRunItem.run_id == run_id,
        AutoPurchaseRunItem.id == item_id,
    )
    item = (await session.execute(item_stmt)).scalar_one_or_none()
    if item is None:
        raise ValueError("Строка автозаказа не найдена")
    if item.sent_to_site_at is not None:
        raise ValueError(
            "Строка уже отправлена на сайт — изменить выбор нельзя"
        )

    offers = list(item.top_site_offers or [])
    if not offers:
        raise ValueError(
            "По строке нет сохранённых предложений сайта для выбора"
        )

    normalized_allocations: list[dict[str, Any]] = []
    for raw in allocations or []:
        try:
            offer_index = int(raw.get("offer_index"))
            quantity = int(raw.get("quantity"))
        except (TypeError, ValueError) as exc:
            raise ValueError(
                "Каждое распределение должно содержать offer_index и quantity"
            ) from exc
        if offer_index < 0 or offer_index >= len(offers):
            raise ValueError(f"Неизвестное предложение #{offer_index + 1}")
        if quantity <= 0:
            raise ValueError("Количество в распределении должно быть больше 0")
        offer = dict(offers[offer_index] or {})
        supplier_available = int(offer.get("current_qty") or 0)
        # min_qnt — кратность (размер партии): заказ только целыми
        # партиями, запрошенное количество округляем ВВЕРХ.
        lot = max(int(offer.get("current_min_qnt") or 1), 1)
        orderable_cap = _round_down_to_lot(supplier_available, lot)
        if orderable_cap <= 0:
            raise ValueError(
                f"У предложения #{offer_index + 1} нет даже одной целой "
                f"партии ({lot} шт), доступно {supplier_available} шт"
            )
        if quantity > orderable_cap:
            raise ValueError(
                f"У предложения #{offer_index + 1} доступно только "
                f"{orderable_cap} шт целыми партиями по {lot} шт"
            )
        quantity = min(
            _round_up_to_multiplicity(quantity, lot),
            orderable_cap,
        )
        normalized_allocations.append(
            _build_allocation_from_offer(offer, quantity)
        )

    if not normalized_allocations:
        raise ValueError("Не передано ни одного распределения")

    primary_offer = dict(offers[int(allocations[0]["offer_index"])] or {})
    total_qty = sum(
        int(allocation["quantity"]) for allocation in normalized_allocations
    )

    item.recommended_supplier = _to_json_safe(primary_offer)
    item.selected_supplier_id = primary_offer.get("provider_id")

    draft = dict(item.draft_purchase_order or {})
    draft.update(
        {
            "provider_id": primary_offer.get("provider_id"),
            "external_supplier_id": primary_offer.get("external_supplier_id"),
            "provider_name": primary_offer.get("provider_name") or "—",
            "price": primary_offer.get("current_price"),
            "oem_number": primary_offer.get("current_oem_number") or "",
            "brand_name": primary_offer.get("current_brand_name"),
            "autopart_name": primary_offer.get("current_autopart_name"),
            "supplier_available_qty": int(
                primary_offer.get("current_qty") or 0
            ),
            "proposed_order_qty": total_qty,
            "recommended_qty": int(item.recommended_order_qty or 0),
            "remaining_gap_qty": max(
                int(item.recommended_order_qty or 0) - total_qty, 0
            ),
            "min_qnt": primary_offer.get("current_min_qnt"),
            "min_delivery_day": primary_offer.get("current_min_delivery"),
            "max_delivery_day": primary_offer.get("current_max_delivery"),
            "hash_key": primary_offer.get("hash_key"),
            "system_hash": primary_offer.get("system_hash"),
            "sup_logo": primary_offer.get("sup_logo"),
            "source_type": primary_offer.get("source_type"),
            "reason": "Выбор менеджера из топ-предложений",
            "allocations": _to_json_safe(normalized_allocations),
        }
    )
    item.draft_purchase_order = _to_json_safe(draft)

    _apply_autopurchase_item_status_override(
        item,
        decision_status=AUTOPURCHASE_STATUS_AUTO_APPROVED,
        comment=comment
        or (
            "Выбрано вручную: "
            + ", ".join(
                f"{allocation['provider_name']} × {allocation['quantity']} шт"
                for allocation in normalized_allocations
            )
        ),
    )

    items_stmt = select(AutoPurchaseRunItem).where(
        AutoPurchaseRunItem.run_id == run_id
    )
    all_items = list((await session.execute(items_stmt)).scalars().all())
    _refresh_run_summary_snapshot(run, all_items)
    await session.commit()
    return {
        "run": _serialize_autopurchase_run(run),
        "item": _serialize_autopurchase_run_item(item),
    }


async def mark_autopurchase_run_items_sent(
    session: AsyncSession,
    *,
    run_id: int,
    item_ids: list[int],
    order_id: Optional[int] = None,
    order_number: Optional[str] = None,
    customer_id: Optional[int] = None,
    send_result_snapshot: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    normalized_item_ids = sorted(
        {
            int(item_id)
            for item_id in (item_ids or [])
            if str(item_id).strip()
        }
    )
    if not normalized_item_ids:
        raise ValueError("Не переданы item_ids для пометки как отправленные")

    run_stmt = select(AutoPurchaseRun).where(AutoPurchaseRun.id == run_id)
    run = (await session.execute(run_stmt)).scalar_one_or_none()
    if run is None:
        raise ValueError("Запуск автозаказа не найден")

    items_stmt = select(AutoPurchaseRunItem).where(
        AutoPurchaseRunItem.run_id == run_id,
        AutoPurchaseRunItem.id.in_(normalized_item_ids),
    )
    items = list((await session.execute(items_stmt)).scalars().all())
    found_item_ids = {int(item.id) for item in items}
    missing_item_ids = [
        item_id for item_id in normalized_item_ids if item_id not in found_item_ids
    ]
    if missing_item_ids:
        raise ValueError(
            "Не найдены строки автозаказа: "
            + ", ".join(str(item_id) for item_id in missing_item_ids)
        )

    sent_at = now_moscow()
    for item in items:
        item.sent_to_site_at = sent_at
        item.sent_order_id = order_id
        item.sent_order_number = order_number
        item.sent_customer_id = customer_id
        item.send_result_snapshot = _to_json_safe(
            dict(send_result_snapshot or {})
        )

    all_items_stmt = select(AutoPurchaseRunItem).where(
        AutoPurchaseRunItem.run_id == run_id
    )
    all_items = list((await session.execute(all_items_stmt)).scalars().all())
    _refresh_run_summary_snapshot(run, all_items)
    await session.commit()

    return {
        "run": _serialize_autopurchase_run(run),
        "updated_items": [
            _serialize_autopurchase_run_item(item) for item in items
        ],
    }


def _get_supplier_identity_group_key(
    supplier: dict[str, Any],
) -> tuple[str, str, str, str]:
    provider_identity = (
        f"provider:{int(supplier['provider_id'])}"
        if supplier.get("provider_id") is not None
        else (
            f"external:{int(supplier['external_supplier_id'])}"
            if supplier.get("external_supplier_id") is not None
            else f"name:{str(supplier.get('provider_name') or '').strip().casefold()}"
        )
    )
    return (
        provider_identity,
        str(supplier.get("provider_name") or "").strip(),
        str(supplier.get("current_provider_config_name") or "").strip(),
        str(supplier.get("source_type") or "").strip(),
    )


def _get_draft_group_key(item: AutoPurchaseRunItem) -> tuple[str, str, str, str]:
    return _get_supplier_identity_group_key(
        dict(item.recommended_supplier or {})
    )


def _normalize_optional_positive_int(value: Any) -> Optional[int]:
    if value in (None, "", 0):
        return None
    try:
        normalized = int(value)
    except (TypeError, ValueError):
        return None
    return normalized if normalized > 0 else None


def _normalize_optional_positive_float(value: Any) -> Optional[float]:
    if value in (None, "", 0):
        return None
    try:
        normalized = float(value)
    except (TypeError, ValueError):
        return None
    if normalized <= 0:
        return None
    return _quantize_float(normalized)


def _get_draft_limits_from_run(
    run: AutoPurchaseRun,
) -> tuple[Optional[float], Optional[int]]:
    settings = dict(run.settings_snapshot or {})
    budget_limit = _normalize_optional_positive_float(
        settings.get("budget_limit")
    )
    position_limit = _normalize_optional_positive_int(
        settings.get("position_limit")
    )
    return budget_limit, position_limit


def _get_draft_item_priority(
    item: AutoPurchaseRunItem,
) -> tuple[int, int, int, int, int, int, str, int]:
    abc_priority, xyz_priority = _get_abc_xyz_priority(dict(item.abc_xyz or {}))
    open_customer_backlog_qty = int(
        (item.draft_purchase_order or {}).get("open_customer_backlog_qty") or 0
    )
    return (
        (
            -1
            if open_customer_backlog_qty > 0
            else (
                int(item.estimated_days_left_30_days)
                if item.estimated_days_left_30_days is not None
                else 9_999
            )
        ),
        -open_customer_backlog_qty,
        abc_priority,
        xyz_priority,
        -int(item.recommended_order_qty or 0),
        -int(item.sold_last_30_days or 0),
        str(item.oem_number or ""),
        int(item.id or 0),
    )


def _get_draft_item_profit_score(item: AutoPurchaseRunItem) -> float:
    """Прибыльность позиции: маржинальность × скорость продаж.

    Используется при ограниченном бюджете: лимит в первую очередь
    тратим на то, что быстрее и прибыльнее всего вернёт деньги.
    """
    draft = dict(item.draft_purchase_order or {})
    purchase_price = float(draft.get("price") or 0)
    sale_price = float(item.latest_price or 0)
    if purchase_price <= 0 or sale_price <= 0:
        return 0.0
    margin_ratio = max((sale_price - purchase_price) / purchase_price, 0.0)
    daily_sales = max(int(item.sold_last_30_days or 0), 0) / 30.0
    return margin_ratio * daily_sales


def _get_draft_item_budget_priority(
    item: AutoPurchaseRunItem,
) -> tuple[int, float, tuple]:
    draft = dict(item.draft_purchase_order or {})
    open_customer_backlog_qty = int(
        draft.get("open_customer_backlog_qty") or 0
    )
    return (
        0 if open_customer_backlog_qty > 0 else 1,
        -_get_draft_item_profit_score(item),
        _get_draft_item_priority(item),
    )


async def get_autopurchase_run_draft_orders(
    session: AsyncSession,
    *,
    run_id: int,
) -> dict[str, Any]:
    run_stmt = select(AutoPurchaseRun).where(AutoPurchaseRun.id == run_id)
    run = (await session.execute(run_stmt)).scalar_one_or_none()
    if run is None:
        raise ValueError("Запуск автозаказа не найден")

    items_stmt = (
        select(AutoPurchaseRunItem)
        .where(
            AutoPurchaseRunItem.run_id == run_id,
            AutoPurchaseRunItem.decision_status
            == AUTOPURCHASE_STATUS_AUTO_APPROVED,
        )
        .order_by(
            AutoPurchaseRunItem.estimated_days_left_30_days.asc().nulls_last(),
            AutoPurchaseRunItem.recommended_order_qty.desc(),
            AutoPurchaseRunItem.sold_last_30_days.desc(),
            AutoPurchaseRunItem.oem_number.asc(),
            AutoPurchaseRunItem.id.asc(),
        )
    )
    items = list((await session.execute(items_stmt)).scalars().all())

    budget_limit, position_limit = _get_draft_limits_from_run(run)
    if budget_limit is not None or position_limit is not None:
        # Лимит режет хвост списка — сортируем по отдаче на вложенный
        # рубль (backlog всегда первым), а не только по дням запаса.
        items.sort(key=_get_draft_item_budget_priority)
    else:
        items.sort(key=_get_draft_item_priority)
    selected_total_sum = Decimal("0")
    selected_total_items = 0

    draft_groups: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    skipped_items: list[dict[str, Any]] = []

    for item in items:
        supplier = dict(item.recommended_supplier or {})
        draft = dict(item.draft_purchase_order or {})
        if item.sent_to_site_at is not None:
            sent_suffix = (
                f" в заказ {item.sent_order_number}"
                if item.sent_order_number
                else ""
            )
            skipped_items.append(
                {
                    "item_id": int(item.id),
                    "oem_number": item.oem_number,
                    "brand_name": item.brand_name,
                    "reason": "Уже отправлено на сайт" + sent_suffix,
                }
            )
            continue
        provider_name = str(supplier.get("provider_name") or "").strip()
        if not provider_name or not draft:
            skipped_items.append(
                {
                    "item_id": int(item.id),
                    "oem_number": item.oem_number,
                    "brand_name": item.brand_name,
                    "reason": "Нет готового site-поставщика или черновика строки",
                }
            )
            continue

        # Одна строка может быть распределена менеджером на несколько
        # предложений (allocations) — тогда формируем несколько строк
        # отправки, каждая в группе своего поставщика.
        manual_allocations = list(draft.get("allocations") or [])
        if manual_allocations:
            line_specs = [
                {
                    "supplier": {
                        "provider_id": allocation.get("provider_id"),
                        "external_supplier_id": allocation.get(
                            "external_supplier_id"
                        ),
                        "provider_name": allocation.get("provider_name"),
                        "current_provider_config_name": allocation.get(
                            "provider_config_name"
                        ),
                        "source_type": allocation.get("source_type"),
                        "sup_logo": allocation.get("sup_logo"),
                    },
                    "price": allocation.get("price"),
                    "qty": int(allocation.get("quantity") or 0),
                    "supplier_available_qty": int(
                        allocation.get("supplier_available_qty") or 0
                    ),
                    "site_brand_name": allocation.get("brand_name"),
                    "site_oem_number": allocation.get("oem_number"),
                    "site_autopart_name": allocation.get("autopart_name"),
                    "min_qnt": allocation.get("min_qnt"),
                    "min_delivery_day": allocation.get("min_delivery_day"),
                    "max_delivery_day": allocation.get("max_delivery_day"),
                    "hash_key": allocation.get("hash_key"),
                    "system_hash": allocation.get("system_hash"),
                    "reason": "Выбор менеджера из топ-предложений",
                    "allow_budget_trim": False,
                }
                for allocation in manual_allocations
            ]
        else:
            line_specs = [
                {
                    "supplier": supplier,
                    "price": draft.get("price"),
                    "qty": int(
                        draft.get("proposed_order_qty")
                        or draft.get("recommended_qty")
                        or item.recommended_order_qty
                        or 0
                    ),
                    "supplier_available_qty": int(
                        draft.get("supplier_available_qty") or 0
                    ),
                    "site_brand_name": draft.get("brand_name"),
                    "site_oem_number": draft.get("oem_number"),
                    "site_autopart_name": draft.get("autopart_name"),
                    "min_qnt": draft.get("min_qnt"),
                    "min_delivery_day": draft.get("min_delivery_day"),
                    "max_delivery_day": draft.get("max_delivery_day"),
                    "hash_key": draft.get("hash_key"),
                    "system_hash": draft.get("system_hash"),
                    "reason": draft.get("reason"),
                    "allow_budget_trim": True,
                }
            ]

        item_proposed_total = 0
        for spec in line_specs:
            spec_supplier = dict(spec.get("supplier") or {})
            spec_provider_name = str(
                spec_supplier.get("provider_name") or ""
            ).strip()
            unit_price = _quantize_float(spec.get("price"))
            proposed_order_qty = int(spec.get("qty") or 0)
            if (
                proposed_order_qty <= 0
                or unit_price is None
                or not spec_provider_name
            ):
                skipped_items.append(
                    {
                        "item_id": int(item.id),
                        "oem_number": item.oem_number,
                        "brand_name": item.brand_name,
                        "reason": (
                            "Строка не даёт ненулевой объём заказа "
                            "по site-поставщику"
                        ),
                    }
                )
                continue

            line_total = _quantize_float(unit_price * proposed_order_qty)
            if (
                position_limit is not None
                and selected_total_items >= position_limit
            ):
                skipped_items.append(
                    {
                        "item_id": int(item.id),
                        "oem_number": item.oem_number,
                        "brand_name": item.brand_name,
                        "reason": (
                            f"Не вошло в отправку: достигнут лимит позиций "
                            f"({position_limit} шт.)"
                        ),
                    }
                )
                continue

            if budget_limit is not None and line_total is not None:
                remaining_budget = max(
                    float(budget_limit) - float(selected_total_sum),
                    0.0,
                )
                if float(line_total) > remaining_budget:
                    if not spec.get("allow_budget_trim"):
                        skipped_items.append(
                            {
                                "item_id": int(item.id),
                                "oem_number": item.oem_number,
                                "brand_name": item.brand_name,
                                "reason": (
                                    "Не вошло в отправку: ручное "
                                    "распределение превышает остаток "
                                    "лимита суммы "
                                    f"({_format_money_value(remaining_budget)}"
                                    " руб.)"
                                ),
                            }
                        )
                        continue
                    # Пробуем заказать частичное количество по остатку
                    # бюджета. Заказ возможен только целыми партиями
                    # поставщика (min_qnt = кратность), поэтому подрезаем
                    # до целого числа партий, но не меньше одной партии —
                    # небольшой выход за бюджет допустим. Строку пропускаем
                    # только если остатка не хватает даже на одну штуку.
                    multiplicity = max(int(item.multiplicity or 1), 1)
                    supplier_lot = max(int(spec.get("min_qnt") or 1), 1)
                    lot_step = max(supplier_lot, multiplicity)
                    max_qty_by_budget = int(
                        remaining_budget // float(unit_price)
                    )
                    if max_qty_by_budget >= 1:
                        budget_qty = _round_down_to_lot(
                            max_qty_by_budget,
                            lot_step,
                        )
                        # Меньше одной партии заказать нельзя — берём её,
                        # даже если чуть выходим за бюджет.
                        budget_qty = max(budget_qty, lot_step)
                        proposed_order_qty = min(
                            budget_qty, proposed_order_qty
                        )
                        line_total = _quantize_float(
                            unit_price * proposed_order_qty
                        )
                    else:
                        skipped_items.append(
                            {
                                "item_id": int(item.id),
                                "oem_number": item.oem_number,
                                "brand_name": item.brand_name,
                                "reason": (
                                    "Не вошло в отправку: остатка лимита "
                                    "суммы "
                                    f"({_format_money_value(remaining_budget)}"
                                    " руб.) не хватает даже на 1 шт по цене "
                                    f"{_format_money_value(float(unit_price))}"
                                    " руб."
                                ),
                            }
                        )
                        continue

            group_key = _get_supplier_identity_group_key(spec_supplier)
            group = draft_groups.get(group_key)
            if group is None:
                group = {
                    "supplier_key": "|".join(group_key),
                    "provider_id": spec_supplier.get("provider_id"),
                    "external_supplier_id": spec_supplier.get(
                        "external_supplier_id"
                    ),
                    "provider_name": spec_provider_name,
                    "provider_config_name": spec_supplier.get(
                        "current_provider_config_name"
                    ),
                    "source_type": spec_supplier.get("source_type"),
                    "sup_logo": spec_supplier.get("sup_logo"),
                    "total_items": 0,
                    "total_quantity": 0,
                    "total_sum": 0.0,
                    "items": [],
                }
                draft_groups[group_key] = group

            item_proposed_total += proposed_order_qty
            # Пересчитываем дефицит от фактически выбранного количества
            # по всем строкам этой позиции.
            remaining_gap_qty = max(
                int(item.recommended_order_qty or 0) - item_proposed_total,
                0,
            )
            group["items"].append(
                {
                    "item_id": int(item.id),
                    "autopart_id": item.autopart_id,
                    "oem_number": item.oem_number,
                    "brand_name": item.brand_name,
                    "autopart_name": item.autopart_name,
                    # Реквизиты предложения сайта: заказ должен уходить
                    # с брендом/номером найденного предложения (для
                    # Dragonzap-позиций это бренд кросса), а не с нашим.
                    "site_brand_name": (
                        spec.get("site_brand_name") or item.brand_name
                    ),
                    "site_oem_number": (
                        spec.get("site_oem_number") or item.oem_number
                    ),
                    "site_autopart_name": (
                        spec.get("site_autopart_name") or item.autopart_name
                    ),
                    "open_customer_backlog_qty": int(
                        draft.get("open_customer_backlog_qty") or 0
                    ),
                    "decision_status": item.decision_status,
                    "recommended_order_qty": int(
                        item.recommended_order_qty or 0
                    ),
                    "proposed_order_qty": proposed_order_qty,
                    "remaining_gap_qty": remaining_gap_qty,
                    "supplier_available_qty": int(
                        spec.get("supplier_available_qty") or 0
                    ),
                    "price": unit_price,
                    "line_total": line_total,
                    "provider_id": spec_supplier.get("provider_id"),
                    "external_supplier_id": spec_supplier.get(
                        "external_supplier_id"
                    ),
                    "min_qnt": spec.get("min_qnt"),
                    "min_delivery_day": spec.get("min_delivery_day"),
                    "max_delivery_day": spec.get("max_delivery_day"),
                    "hash_key": spec.get("hash_key"),
                    "system_hash": spec.get("system_hash"),
                    "reason": spec.get("reason"),
                }
            )
            group["total_items"] += 1
            group["total_quantity"] += proposed_order_qty
            # Накапливаем как Decimal, чтобы избежать плавающих ошибок
            # округления.
            group["total_sum"] = Decimal(str(group["total_sum"])) + Decimal(
                str(line_total or 0)
            )
            selected_total_items += 1
            selected_total_sum += Decimal(str(line_total or 0))

    # Финализируем суммы групп: квантизируем Decimal → float для JSON-сериализации.
    for group in draft_groups.values():
        group["total_sum"] = _quantize_float(float(group["total_sum"])) or 0.0

    groups = sorted(
        draft_groups.values(),
        key=lambda item: (
            -float(item.get("total_sum") or 0),
            str(item.get("provider_name") or ""),
        ),
    )

    return {
        "run": _serialize_autopurchase_run(run),
        "total_groups": len(groups),
        "total_items": sum(int(group["total_items"]) for group in groups),
        "total_quantity": sum(int(group["total_quantity"]) for group in groups),
        "total_sum": _quantize_float(float(selected_total_sum)),
        "applied_budget_limit": budget_limit,
        "applied_position_limit": position_limit,
        "groups": groups,
        "skipped_items": skipped_items,
    }


async def get_autopurchase_run_draft_group_ai_explanation(
    session: AsyncSession,
    *,
    run_id: int,
    supplier_key: str,
) -> dict[str, Any]:
    run_stmt = select(AutoPurchaseRun).where(AutoPurchaseRun.id == run_id)
    run = (await session.execute(run_stmt)).scalar_one_or_none()
    if run is None:
        raise ValueError("Запуск автозаказа не найден")

    draft_payload = await get_autopurchase_run_draft_orders(
        session,
        run_id=run_id,
    )
    normalized_supplier_key = str(supplier_key or "").strip()
    if not normalized_supplier_key:
        raise ValueError("Не передан supplier_key группы автозаказа")

    group = next(
        (
            item
            for item in list(draft_payload.get("groups") or [])
            if str(item.get("supplier_key") or "") == normalized_supplier_key
        ),
        None,
    )
    if group is None:
        raise ValueError("Группа черновика автозаказа не найдена")

    return await _generate_autopurchase_group_ai_payload(
        run=run,
        group=group,
    )
