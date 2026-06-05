from __future__ import annotations

import asyncio
import json
import logging
import os
from datetime import date, timedelta
from decimal import ROUND_HALF_UP, Decimal
from math import ceil
from typing import Any, Optional

import httpx
from sqlalchemy import delete, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.core.constants import URL_DZ_SEARCH
from dz_fastapi.core.time import now_moscow
from dz_fastapi.crud.brand import brand_crud
from dz_fastapi.crud.partner import crud_provider
from dz_fastapi.http.dz_site_client import DZSiteClient
from dz_fastapi.models.autopart import AutoPart, AutoPurchaseRun, AutoPurchaseRunItem
from dz_fastapi.models.brand import Brand
from dz_fastapi.models.partner import (
    PriceList,
    PriceListAutoPartAssociation,
    Provider,
    ProviderPriceListConfig,
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


def _blend_average_daily(
    avg_daily_30: Optional[float],
    avg_daily_90: Optional[float],
) -> Optional[float]:
    if avg_daily_30 is not None and avg_daily_90 is not None:
        return _quantize_float(avg_daily_30 * 0.7 + avg_daily_90 * 0.3)
    return avg_daily_30 if avg_daily_30 is not None else avg_daily_90


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


async def _expand_site_query_brands(
    session: AsyncSession,
    brand_name: Optional[str],
) -> list[str]:
    normalized_input = str(brand_name or "").strip().upper()
    if not normalized_input:
        return []

    expanded = [normalized_input]
    try:
        main_brand = await brand_crud.get_brand_by_name_or_none(
            brand_name=normalized_input,
            session=session,
        )
        if not main_brand:
            return expanded

        related = await brand_crud.get_all_synonyms_bi_directional(
            brand=main_brand,
            session=session,
        )
        candidates = [str(main_brand.name).strip().upper()]
        candidates.extend(
            str(item.name).strip().upper()
            for item in related
            if str(getattr(item, "name", "")).strip()
        )
        candidates.append(normalized_input)

        unique: list[str] = []
        seen: set[str] = set()
        for candidate in candidates:
            if not candidate or candidate in seen:
                continue
            seen.add(candidate)
            unique.append(candidate)
        return unique
    except Exception as exc:
        logger.warning(
            "Не удалось расширить бренд для site-запроса brand=%s: %s",
            normalized_input,
            exc,
        )
        return expanded


def _prepare_site_brand_candidates(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        rows = [item for item in payload if isinstance(item, dict)]
    elif isinstance(payload, dict):
        raw_rows = payload.get("data")
        rows = (
            [item for item in raw_rows if isinstance(item, dict)]
            if isinstance(raw_rows, list)
            else []
        )
    else:
        rows = []

    deduped: dict[str, dict[str, Any]] = {}
    for row in rows:
        brand_name = str(row.get("brand") or "").strip().upper()
        if not brand_name:
            continue
        try:
            rate = int(row.get("rate") or 0)
        except (TypeError, ValueError):
            rate = 0
        current = deduped.get(brand_name)
        normalized_row = {
            "brand": brand_name,
            "number": row.get("number"),
            "des_text": row.get("des_text"),
            "rate": rate,
        }
        if current is None or rate > int(current.get("rate") or 0):
            deduped[brand_name] = normalized_row

    return sorted(
        deduped.values(),
        key=lambda item: (-int(item.get("rate") or 0), item["brand"]),
    )


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


def _merge_site_offers(offers_by_brand: list[list[dict[str, Any]]]) -> list[dict[str, Any]]:
    merged: list[dict[str, Any]] = []
    seen = set()
    for offers in offers_by_brand:
        for raw in offers or []:
            key = (
                raw.get("system_hash")
                or raw.get("hash_key")
                or (
                    raw.get("oem"),
                    raw.get("make_name"),
                    raw.get("cost"),
                    raw.get("qnt"),
                    raw.get("price_name"),
                    raw.get("sup_logo"),
                    raw.get("min_delivery_day"),
                    raw.get("max_delivery_day"),
                )
            )
            if key in seen:
                continue
            seen.add(key)
            merged.append(raw)
    return merged


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
        item for item in supplier_stats if item.get("current_price") is not None
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


async def _fetch_site_supplier_stats_for_oem(
    session: AsyncSession,
    *,
    oem_number: str,
    brand_name: Optional[str],
    history_rows: Optional[list[dict[str, Any]]] = None,
) -> tuple[list[dict[str, Any]], list[str], bool]:
    if not SITE_API_KEY:
        return [], [], False

    query_brands = await _expand_site_query_brands(session, brand_name)
    if not query_brands:
        return [], [], False

    site_brand_candidates: list[dict[str, Any]] = []
    used_fallback_brand = False
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
        for query_brand in query_brands:
            offers = await client.get_offers(
                oem=oem_number,
                brand=query_brand,
                without_cross=True,
            )
            if not offers:
                continue
            for item in offers:
                if isinstance(item, dict):
                    item.setdefault("query_brand", query_brand)
            offers_by_brand.append(offers)

        if not offers_by_brand:
            site_brand_candidates = _prepare_site_brand_candidates(
                await client.get_brands(oem_number)
            )
            tried_brands = set(query_brands)
            for candidate in site_brand_candidates:
                fallback_brand = candidate["brand"]
                if fallback_brand in tried_brands:
                    continue
                offers = await client.get_offers(
                    oem=oem_number,
                    brand=fallback_brand,
                    without_cross=True,
                )
                if not offers:
                    continue
                for item in offers:
                    if isinstance(item, dict):
                        item.setdefault("query_brand", fallback_brand)
                offers_by_brand.append(offers)
                query_brands = [fallback_brand]
                used_fallback_brand = True
                break

    merged = _merge_site_offers(offers_by_brand)
    supplier_stats: list[dict[str, Any]] = []
    for raw in merged:
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
    return supplier_stats, query_brands, used_fallback_brand


def _select_autopurchase_supplier(
    supplier_stats: list[dict[str, Any]],
    *,
    fill_rate_threshold: float,
    max_allowed_lead_days: Optional[int],
) -> Optional[dict[str, Any]]:
    candidates = [
        item
        for item in supplier_stats
        if item.get("current_price") is not None and not item.get("is_own_price")
    ]
    if not candidates:
        return None

    in_stock_candidates = [
        item for item in candidates if int(item.get("current_qty") or 0) > 0
    ]
    base_candidates = in_stock_candidates or candidates

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
        "message": _build_run_summary_message(AUTOPURCHASE_RUN_STATUS_COMPLETED),
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
            reasons=list(row.get("reasons") or []),
            abc_xyz=row.get("abc_xyz") or {},
            best_supplier_by_price=row.get("best_supplier_by_price") or {},
            best_supplier_by_lead_time=row.get("best_supplier_by_lead_time") or {},
            recommended_supplier=recommended_supplier or {},
            draft_purchase_order=row.get("draft_purchase_order") or {},
        )
        session.add(item)


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


def _build_autopurchase_draft(
    *,
    supplier: Optional[dict[str, Any]],
    available_qty: int,
    in_transit_qty: int,
    target_qty: Optional[int],
    recommended_qty: int,
    lead_time_days_used: Optional[float],
    reason: Optional[str],
) -> Optional[dict[str, Any]]:
    if not supplier or not supplier.get("provider_name"):
        return None
    if supplier.get("current_price") is None:
        return None
    if recommended_qty <= 0:
        return None

    supplier_available_qty = max(int(supplier.get("current_qty") or 0), 0)
    proposed_order_qty = min(recommended_qty, supplier_available_qty)
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
        "available_qty": available_qty,
        "in_transit_qty": int(in_transit_qty or 0),
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
    sold_last_30_by_oem = _calculate_snapshot_sales(
        snapshots,
        normalized_oem_numbers,
        days=30,
        received_qty_by_oem_and_date=received_qty_by_oem_and_date,
    )
    sold_last_90_by_oem = _calculate_snapshot_sales(
        snapshots,
        normalized_oem_numbers,
        days=90,
        received_qty_by_oem_and_date=received_qty_by_oem_and_date,
    )

    decision_filter = str(decision_status or "").strip().lower() or None
    search_filter = str(search or "").strip().lower()
    decision_rank = {
        AUTOPURCHASE_STATUS_BLOCKED: 0,
        AUTOPURCHASE_STATUS_NEEDS_REVIEW: 1,
        AUTOPURCHASE_STATUS_AUTO_APPROVED: 2,
    }
    base_rows: list[dict[str, Any]] = []

    for oem_number, known_row in latest_known_rows_by_oem.items():
        latest = latest_rows_by_oem.get(oem_number) or {
            **known_row,
            "current_quantity": 0,
        }
        oem_history = history_by_oem.get(oem_number, [])
        sold_last_30_days = int(sold_last_30_by_oem.get(oem_number, 0))
        sold_last_90_days = int(sold_last_90_by_oem.get(oem_number, 0))
        avg_daily_30 = _compute_average_daily(sold_last_30_days, 30)
        avg_daily_90 = _compute_average_daily(sold_last_90_days, 90)
        avg_daily_blended = _blend_average_daily(avg_daily_30, avg_daily_90)
        current_quantity = int(latest.get("current_quantity") or 0)
        minimum_balance = int(latest.get("minimum_balance") or 0)
        multiplicity = max(int(latest.get("multiplicity") or 1), 1)
        missing_in_latest_pricelist = oem_number not in latest_rows_by_oem
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
        if lead_time_days_used is None and minimum_balance > 0 and avg_daily_blended:
            lead_time_days_used = 7.0
        elif lead_time_days_used is None and minimum_balance > 0:
            manual_min_balance_fallback = True

        coverable_in_transit_qty = _estimate_coverable_in_transit_qty(
            oem_history,
            lead_time_days_used=lead_time_days_used,
        )
        available_qty_for_planning = current_quantity + coverable_in_transit_qty

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
        review_period_days = (
            max(7, int(ceil(lead_time_days_used)))
            if lead_time_days_used is not None
            else 7
        )
        demand_for_review_period = (
            _quantize_float(avg_daily_blended * review_period_days)
            if avg_daily_blended is not None
            else None
        )
        target_stock = None
        if reorder_point is not None:
            target_stock = int(
                ceil(float(reorder_point) + float(demand_for_review_period or 0))
            )
        elif minimum_balance > 0:
            target_stock = int(minimum_balance)
        if target_stock is not None:
            target_stock = max(target_stock, int(minimum_balance or 0))

        recommended_order_qty = 0
        if target_stock is not None:
            recommended_order_qty = max(target_stock - available_qty_for_planning, 0)
            recommended_order_qty = _round_up_to_multiplicity(
                recommended_order_qty,
                multiplicity,
            )

        estimated_days_left_30_days = (
            int(current_quantity / avg_daily_30)
            if avg_daily_30 and avg_daily_30 > 0
            else None
        )
        if (
            missing_in_latest_pricelist
            and sold_last_30_days <= 0
            and sold_last_90_days <= 0
            and in_transit_qty <= 0
            and minimum_balance <= 0
        ):
            continue
        if (
            recommended_order_qty <= 0
            and not missing_in_latest_pricelist
            and minimum_balance <= 0
        ):
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
                "sold_last_30_days": sold_last_30_days,
                "sold_last_90_days": sold_last_90_days,
                "avg_daily_30": avg_daily_30,
                "avg_daily_90": avg_daily_90,
                "avg_daily_blended": avg_daily_blended,
                "estimated_days_left_30_days": estimated_days_left_30_days,
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

    base_rows.sort(
        key=lambda item: (
            0 if item.get("missing_in_latest_pricelist") else 1,
            item["estimated_days_left_30_days"]
            if item["estimated_days_left_30_days"] is not None
            else 9_999,
            -int(item.get("recommended_order_qty") or 0),
            item["oem_number"],
        )
    )
    candidate_rows = base_rows[:normalized_limit]

    site_results_by_oem: dict[str, tuple[list[dict[str, Any]], list[str], bool]] = {}
    if candidate_rows:
        site_fetch_semaphore = asyncio.Semaphore(AUTOPURCHASE_SITE_FETCH_CONCURRENCY)

        async def _load_site_result(
            candidate_row: dict[str, Any],
        ) -> tuple[str, tuple[list[dict[str, Any]], list[str], bool]]:
            candidate_oem_number = str(candidate_row["oem_number"])
            try:
                async with site_fetch_semaphore:
                    site_result = await _fetch_site_supplier_stats_for_oem(
                        session,
                        oem_number=candidate_oem_number,
                        brand_name=candidate_row.get("brand_name"),
                        history_rows=list(candidate_row.get("history_rows") or []),
                    )
            except Exception as exc:
                logger.warning(
                    "Ошибка загрузки site-поставщиков для OEM=%s: %s",
                    candidate_oem_number,
                    exc,
                )
                site_result = ([], [], False)
            return candidate_oem_number, site_result

        site_results_by_oem = dict(
            await asyncio.gather(
                *[_load_site_result(candidate_row) for candidate_row in candidate_rows]
            )
        )

    rows: list[dict[str, Any]] = []
    for candidate_row in candidate_rows:
        oem_number = str(candidate_row["oem_number"])
        site_supplier_stats, site_query_brands, used_site_fallback_brand = (
            site_results_by_oem.get(oem_number) or ([], [], False)
        )
        best_supplier_by_price = _select_best_site_supplier_by_price(
            site_supplier_stats
        )
        best_supplier_by_lead_time = _select_best_site_supplier_by_lead_time(
            site_supplier_stats
        )

        abc_xyz = candidate_row.get("abc_xyz")
        abc_class = abc_xyz.get("abc_class") if abc_xyz else None
        xyz_class = abc_xyz.get("xyz_class") if abc_xyz else None
        max_allowed_lead_days = _get_max_allowed_lead_days(abc_class, xyz_class)
        supplier_fill_threshold = (
            FILL_RATE_THRESHOLD_AUTO_APPROVE
            if requested_mode == AUTOPURCHASE_MODE_AUTO_APPROVE_SAFE
            else FILL_RATE_THRESHOLD_DRAFT
        )
        selected_supplier = _select_autopurchase_supplier(
            site_supplier_stats,
            fill_rate_threshold=supplier_fill_threshold,
            max_allowed_lead_days=max_allowed_lead_days,
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

        if not selected_supplier:
            reasons.append(
                _build_reason(
                    code="site_supplier_not_found",
                    severity="critical",
                    title="Сайт не дал подходящего поставщика",
                    description=(
                        "Dragonzap не вернул подходящее актуальное предложение "
                        "по этому OEM."
                    ),
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

        recommended_order_qty = int(candidate_row.get("recommended_order_qty") or 0)
        if (
            selected_supplier
            and int(selected_supplier.get("current_qty") or 0) < recommended_order_qty
        ):
            reasons.append(
                _build_reason(
                    code="site_qty_less_than_required",
                    severity="warning",
                    title="У лучшего site-поставщика не хватает количества",
                    description=(
                        f"Сайт даёт только {int(selected_supplier.get('current_qty') or 0)} "
                        f"шт при потребности {recommended_order_qty} шт. "
                        "Позиция требует ручного решения или дополнительного дозаказа."
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
        elif (
            requested_mode == AUTOPURCHASE_MODE_AUTO_APPROVE_SAFE
            and selected_supplier.get("fill_rate") is not None
            and float(selected_supplier.get("fill_rate") or 0)
            >= FILL_RATE_THRESHOLD_AUTO_APPROVE
            and (
                max_allowed_lead_days is None
                or selected_supplier.get("effective_lead_days") is None
                or float(selected_supplier.get("effective_lead_days") or 0)
                <= float(max_allowed_lead_days)
            )
            and int(selected_supplier.get("current_qty") or 0)
            >= recommended_order_qty
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
            reason=next(
                (
                    reason["title"]
                    for reason in reasons
                    if reason.get("severity") == "critical"
                ),
                None,
            ),
        )

        rows.append(
            {
                "autopart_id": candidate_row.get("autopart_id"),
                "oem_number": oem_number,
                "brand_name": candidate_row.get("brand_name"),
                "autopart_name": candidate_row.get("autopart_name"),
                "current_quantity": int(candidate_row.get("current_quantity") or 0),
                "latest_price": candidate_row.get("latest_price"),
                "minimum_balance": int(candidate_row.get("minimum_balance") or 0),
                "multiplicity": int(candidate_row.get("multiplicity") or 1),
                "in_transit_qty": int(candidate_row.get("in_transit_qty") or 0),
                "sold_last_30_days": int(candidate_row.get("sold_last_30_days") or 0),
                "sold_last_90_days": int(candidate_row.get("sold_last_90_days") or 0),
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
                "site_query_brands": site_query_brands,
                "used_site_fallback_brand": used_site_fallback_brand,
            }
        )

    rows.sort(
        key=lambda item: (
            decision_rank.get(item["decision_status"], 99),
            item["estimated_days_left_30_days"]
            if item["estimated_days_left_30_days"] is not None
            else 9_999,
            -int(item.get("recommended_order_qty") or 0),
            item["oem_number"],
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
            trigger_source="manual",
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
        return run_id


async def list_autopurchase_runs(
    session: AsyncSession,
    *,
    limit: int = 50,
) -> list[dict[str, Any]]:
    stmt = (
        select(AutoPurchaseRun)
        .order_by(AutoPurchaseRun.started_at.desc(), AutoPurchaseRun.id.desc())
        .limit(limit)
    )
    runs = (await session.execute(stmt)).scalars().all()
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

    stmt = (
        select(AutoPurchaseRunItem)
        .where(AutoPurchaseRunItem.run_id == run_id)
        .order_by(
            AutoPurchaseRunItem.decision_status.asc(),
            AutoPurchaseRunItem.recommended_order_qty.desc(),
            AutoPurchaseRunItem.oem_number.asc(),
            AutoPurchaseRunItem.id.asc(),
        )
    )
    items = (await session.execute(stmt)).scalars().all()

    decision_filter = str(decision_status or "").strip().lower() or None
    search_filter = str(search or "").strip().lower()
    filtered_rows: list[dict[str, Any]] = []
    for item in items:
        serialized = _serialize_autopurchase_run_item(item)
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
        item.send_result_snapshot = dict(send_result_snapshot or {})

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


def _get_draft_group_key(item: AutoPurchaseRunItem) -> tuple[str, str, str, str]:
    supplier = dict(item.recommended_supplier or {})
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


def _get_draft_item_priority(item: AutoPurchaseRunItem) -> tuple[int, int, int, str, int]:
    return (
        int(item.estimated_days_left_30_days)
        if item.estimated_days_left_30_days is not None
        else 9_999,
        -int(item.recommended_order_qty or 0),
        -int(item.sold_last_30_days or 0),
        str(item.oem_number or ""),
        int(item.id or 0),
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
    items.sort(key=_get_draft_item_priority)

    budget_limit, position_limit = _get_draft_limits_from_run(run)
    selected_total_sum = 0.0
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

        unit_price = _quantize_float(draft.get("price"))
        proposed_order_qty = int(
            draft.get("proposed_order_qty")
            or draft.get("recommended_qty")
            or item.recommended_order_qty
            or 0
        )
        if proposed_order_qty <= 0 or unit_price is None:
            skipped_items.append(
                {
                    "item_id": int(item.id),
                    "oem_number": item.oem_number,
                    "brand_name": item.brand_name,
                    "reason": "Строка не даёт ненулевой объём заказа по site-поставщику",
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
                skipped_items.append(
                    {
                        "item_id": int(item.id),
                        "oem_number": item.oem_number,
                        "brand_name": item.brand_name,
                        "reason": (
                            "Не вошло в отправку: строка превышает остаток "
                            f"лимита суммы ({_format_money_value(line_total)} руб. > "
                            f"{_format_money_value(remaining_budget)} руб.)"
                        ),
                    }
                )
                continue

        group_key = _get_draft_group_key(item)
        group = draft_groups.get(group_key)
        if group is None:
            group = {
                "supplier_key": "|".join(group_key),
                "provider_id": supplier.get("provider_id"),
                "external_supplier_id": supplier.get("external_supplier_id"),
                "provider_name": provider_name,
                "provider_config_name": supplier.get(
                    "current_provider_config_name"
                ),
                "source_type": supplier.get("source_type"),
                "sup_logo": supplier.get("sup_logo"),
                "total_items": 0,
                "total_quantity": 0,
                "total_sum": 0.0,
                "items": [],
            }
            draft_groups[group_key] = group

        remaining_gap_qty = int(draft.get("remaining_gap_qty") or 0)
        group["items"].append(
            {
                "item_id": int(item.id),
                "autopart_id": item.autopart_id,
                "oem_number": item.oem_number,
                "brand_name": item.brand_name,
                "autopart_name": item.autopart_name,
                "decision_status": item.decision_status,
                "recommended_order_qty": int(item.recommended_order_qty or 0),
                "proposed_order_qty": proposed_order_qty,
                "remaining_gap_qty": remaining_gap_qty,
                "supplier_available_qty": int(
                    draft.get("supplier_available_qty") or 0
                ),
                "price": unit_price,
                "line_total": line_total,
                "provider_id": supplier.get("provider_id"),
                "external_supplier_id": supplier.get("external_supplier_id"),
                "min_qnt": draft.get("min_qnt"),
                "min_delivery_day": draft.get("min_delivery_day"),
                "max_delivery_day": draft.get("max_delivery_day"),
                "hash_key": draft.get("hash_key"),
                "system_hash": draft.get("system_hash"),
                "reason": draft.get("reason"),
            }
        )
        group["total_items"] += 1
        group["total_quantity"] += proposed_order_qty
        group["total_sum"] = _quantize_float(
            float(group["total_sum"]) + float(line_total or 0)
        ) or 0.0
        selected_total_items += 1
        selected_total_sum = _quantize_float(
            float(selected_total_sum) + float(line_total or 0)
        ) or 0.0

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
        "total_sum": _quantize_float(
            sum(float(group["total_sum"]) for group in groups)
        ),
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
