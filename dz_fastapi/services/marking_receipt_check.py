"""Сверка кодов маркировки (КИЗ) при приёмке от поставщиков.

Для каждой строки поступления с кодами из УПД (почта или Диадок) делаем
локальные проверки (количество, дубли, формат, реестр, привязка GTIN) и,
если есть действующий токен ГИС МТ, сверяем коды с Честным знаком:
статус, владелец, GTIN. Результат хранится в
``SupplierReceiptItem.marking_check`` и показывается в документе и списке.
Проведение поступления расхождения не блокируют — решает администратор.
"""
from __future__ import annotations

import hashlib
import json
import logging
import re
from collections import Counter
from typing import Any, Iterable, Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.core.time import now_moscow
from dz_fastapi.models.autopart import ScannedCodeAlias
from dz_fastapi.models.inventory import MarkingCodeStatus, ProductMarkingCode
from dz_fastapi.models.partner import SupplierReceipt, SupplierReceiptItem
from dz_fastapi.models.settings import DiadocIntegrationSettings
from dz_fastapi.services.marking_code_parse import parse_marking_code

logger = logging.getLogger("dz_fastapi")

CHECK_OK = "ok"
CHECK_WARNING = "warning"
CHECK_ERROR = "error"
CHECK_UNCHECKED = "unchecked"
CHECK_NONE = "none"

SEVERITY_ERROR = "error"
SEVERITY_WARNING = "warning"

GIS_OK_STATUS = "INTRODUCED"
GIS_STATUS_TEXT = {
    "EMITTED": "эмитирован, но не введён в оборот",
    "APPLIED": "нанесён, но не введён в оборот",
    "APPLIED_NOT_PAID": "нанесён, эмиссия не оплачена",
    "WITHDRAWN": "выведен из оборота",
    "RETIRED": "выбыл из оборота (продан)",
    "REMARK_RETIRED": "выбыл (перемаркирован)",
    "WRITTEN_OFF": "списан",
    "DISAGGREGATION": "расформирована упаковка",
    "FOREIGN_EMISSION": "иностранная эмиссия",
    "FOREIGN_SHIP": "отгружен за рубеж",
    "EXPORTED": "экспортирован",
}
# Коды, уже числящиеся у нас в этих статусах, повторно принимать нельзя
_REGISTRY_BUSY = {
    MarkingCodeStatus.RECEIVED,
    MarkingCodeStatus.IN_STOCK,
    MarkingCodeStatus.RESERVED,
    MarkingCodeStatus.SHIPPED,
}
_REGISTRY_STATUS_TEXT = {
    MarkingCodeStatus.RECEIVED: "уже принят",
    MarkingCodeStatus.IN_STOCK: "уже числится на нашем складе",
    MarkingCodeStatus.RESERVED: "уже зарезервирован у нас",
    MarkingCodeStatus.SHIPPED: "уже отгружен клиенту",
}
_DIGITS_RE = re.compile(r"\D+")
_MAX_ISSUES_PER_ITEM = 200


def _digits(value: object) -> str:
    return _DIGITS_RE.sub("", str(value or ""))


def _issue(
    reason: str,
    message: str,
    *,
    code: Optional[str] = None,
    severity: str = SEVERITY_ERROR,
) -> dict[str, Any]:
    return {
        "code": code,
        "reason": reason,
        "severity": severity,
        "message": message,
    }


def receipt_item_fingerprint(item: SupplierReceiptItem) -> str:
    """Отпечаток входных данных строки: при смене кодов проверка устаревает."""
    payload = json.dumps(
        [
            list(item.marking_codes or []),
            int(item.received_quantity or 0),
            item.autopart_id,
        ],
        ensure_ascii=False,
    )
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()[:16]


def _parse_gis_item(item: Any) -> dict[str, str]:
    payload = item if isinstance(item, dict) else {}
    info = payload.get("cisInfo")
    if isinstance(info, dict):
        payload = info
    return {
        "cis": str(
            payload.get("requestedCis") or payload.get("cis") or ""
        ).strip(),
        "status": str(payload.get("status") or "").strip().upper(),
        "owner_inn": _digits(payload.get("ownerInn")),
        "gtin": _digits(payload.get("gtin")),
    }


def _local_issues(
    item: SupplierReceiptItem,
    *,
    codes: list[str],
    duplicates_other_items: set[str],
    registry: dict[str, ProductMarkingCode],
    receipt_id: int,
    alias_by_gtin: dict[str, int],
) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    quantity = int(item.received_quantity or 0)
    if quantity > 0 and len(codes) != quantity:
        issues.append(
            _issue(
                "quantity_mismatch",
                f"Кодов {len(codes)}, а в строке {quantity} шт. — "
                "количество кодов должно совпадать с количеством товара",
            )
        )
    counts = Counter(str(c) for c in (item.marking_codes or []))
    for code, count in counts.items():
        if count > 1:
            issues.append(
                _issue(
                    "duplicate_in_item",
                    f"Код указан в строке {count} раза(з)",
                    code=code,
                )
            )
    for code in codes:
        if code in duplicates_other_items:
            issues.append(
                _issue(
                    "duplicate_in_document",
                    "Этот код повторяется в другой строке документа",
                    code=code,
                )
            )
        parsed = parse_marking_code(code)
        if parsed is None:
            issues.append(
                _issue(
                    "invalid_format",
                    "Не похоже на код маркировки (нет GTIN и серийного номера)",
                    code=code,
                )
            )
            continue
        row = registry.get(code)
        if (
            row is not None
            and row.status in _REGISTRY_BUSY
            and row.supplier_receipt_id != receipt_id
        ):
            issues.append(
                _issue(
                    "already_in_registry",
                    "Код "
                    + _REGISTRY_STATUS_TEXT.get(row.status, "уже учтён")
                    + (
                        f" (поступление #{row.supplier_receipt_id})"
                        if row.supplier_receipt_id
                        else ""
                    ),
                    code=code,
                )
            )
        bound = alias_by_gtin.get(parsed.gtin)
        if bound is not None and item.autopart_id and bound != item.autopart_id:
            issues.append(
                _issue(
                    "gtin_mismatch",
                    f"GTIN {parsed.gtin} привязан к другому товару "
                    f"(id {bound}), а в строке товар id {item.autopart_id}",
                    code=code,
                )
            )
    return issues


def _gis_issues(
    codes: list[str],
    *,
    gis_by_code: dict[str, dict[str, str]],
    allowed_owner_inns: set[str],
    supplier_inn_known: bool,
) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    owner_unknown_reported = False
    for code in codes:
        info = gis_by_code.get(code)
        if info is None:
            issues.append(
                _issue(
                    "gis_not_found",
                    "ГИС МТ не вернул данных по коду — такого кода нет "
                    "в системе или он записан с ошибкой",
                    code=code,
                )
            )
            continue
        status = info["status"]
        if status != GIS_OK_STATUS:
            text = GIS_STATUS_TEXT.get(status, status or "нет статуса")
            issues.append(
                _issue(
                    "gis_status",
                    f"Статус в ГИС МТ: {text} — ожидается «в обороте» "
                    f"(INTRODUCED), получен {status or '—'}",
                    code=code,
                )
            )
        owner = info["owner_inn"]
        if owner and allowed_owner_inns and owner not in allowed_owner_inns:
            issues.append(
                _issue(
                    "gis_owner",
                    f"Владелец кода в ГИС МТ — ИНН {owner}, а не поставщик "
                    "и не мы",
                    code=code,
                )
            )
        elif owner and not supplier_inn_known and not owner_unknown_reported:
            owner_unknown_reported = True
            issues.append(
                _issue(
                    "supplier_inn_missing",
                    "У поставщика не заполнен ИНН — владельца кодов "
                    "проверить нельзя",
                    severity=SEVERITY_WARNING,
                )
            )
        parsed = parse_marking_code(code)
        if (
            parsed
            and info["gtin"]
            and info["gtin"].lstrip("0") != parsed.gtin.lstrip("0")
        ):
            issues.append(
                _issue(
                    "gis_gtin",
                    f"GTIN в ГИС МТ ({info['gtin']}) не совпадает с GTIN "
                    f"в коде ({parsed.gtin})",
                    code=code,
                )
            )
    return issues


async def _own_inn(session: AsyncSession) -> str:
    row = (
        await session.execute(select(DiadocIntegrationSettings).limit(1))
    ).scalars().first()
    return _digits(getattr(row, "organization_inn", None))


async def _fetch_gis_info(
    session: AsyncSession, codes: list[str]
) -> tuple[dict[str, dict[str, str]], Optional[str]]:
    """(коды → данные ГИС, причина недоступности)."""
    from dz_fastapi.http.gis_mt_client import GisMtClient
    from dz_fastapi.services.gis_mt import GIS_CISES_INFO_CHUNK, get_or_create_gis_mt_settings

    settings_row = await get_or_create_gis_mt_settings(session)
    client = GisMtClient(token=str(settings_row.token))
    result: dict[str, dict[str, str]] = {}
    try:
        for offset in range(0, len(codes), GIS_CISES_INFO_CHUNK):
            chunk = codes[offset:offset + GIS_CISES_INFO_CHUNK]
            for raw in await client.get_cises_info(chunk):
                parsed = _parse_gis_item(raw)
                if parsed["cis"]:
                    result[parsed["cis"]] = parsed
    except Exception as exc:  # noqa: BLE001
        logger.warning("GIS MT receipt marking check failed: %s", exc)
        return {}, str(exc)[:300]
    return result, None


def _item_status(issues: list[dict[str, Any]]) -> str:
    if any(i["severity"] == SEVERITY_ERROR for i in issues):
        return CHECK_ERROR
    if issues:
        return CHECK_WARNING
    return CHECK_OK


async def check_receipt_marking(
    session: AsyncSession,
    receipt: SupplierReceipt,
    *,
    use_gis: bool = True,
    force: bool = False,
) -> bool:
    """Сверяет КИЗ строк поступления, результат пишет в строки.

    ``use_gis=False`` — только локальные проверки (дёшево, для показа в
    списке). Проверка пересчитывается, если изменились коды/количество,
    либо если ГИС МТ теперь доступен, а прошлая проверка шла без него.
    Без действующего токена ГИС МТ проверка остаётся локальной.
    Коммит — на вызывающем. Возвращает True, если что-то записали.
    """
    from dz_fastapi.services.marking_codes import normalize_marking_codes

    items = [i for i in (receipt.items or []) if i.marking_codes]
    if not items:
        return False

    gis_enabled = False
    if use_gis:
        from dz_fastapi.services.gis_mt import get_or_create_gis_mt_settings, gis_mt_token_is_active

        gis_enabled = gis_mt_token_is_active(
            await get_or_create_gis_mt_settings(session)
        )

    stale: list[SupplierReceiptItem] = []
    for item in items:
        current = item.marking_check or {}
        fresh = current.get("fingerprint") == receipt_item_fingerprint(item)
        if force or not fresh or (gis_enabled and not current.get("gis_checked")):
            stale.append(item)
    if not stale:
        return False

    codes_by_item: dict[int, list[str]] = {
        item.id: normalize_marking_codes(item.marking_codes) for item in items
    }
    all_codes = sorted({c for codes in codes_by_item.values() for c in codes})
    registry_rows = (
        await session.execute(
            select(ProductMarkingCode).where(
                ProductMarkingCode.code.in_(all_codes)
            )
        )
    ).scalars().all()
    registry = {str(row.code): row for row in registry_rows}

    gtins = {
        parsed.gtin
        for parsed in (parse_marking_code(c) for c in all_codes)
        if parsed
    }
    alias_by_gtin: dict[str, int] = {}
    if gtins:
        alias_rows = (
            await session.execute(
                select(ScannedCodeAlias).where(
                    ScannedCodeAlias.code.in_({f"GTIN:{g}" for g in gtins})
                )
            )
        ).scalars().all()
        alias_by_gtin = {
            str(row.code)[len("GTIN:"):]: int(row.autopart_id)
            for row in alias_rows
        }

    gis_by_code: dict[str, dict[str, str]] = {}
    gis_error: Optional[str] = None
    if gis_enabled:
        stale_codes = sorted(
            {c for item in stale for c in codes_by_item[item.id]}
        )
        gis_by_code, gis_error = await _fetch_gis_info(session, stale_codes)

    provider = getattr(receipt, "provider", None)
    supplier_inn = _digits(getattr(provider, "inn", None))
    allowed_inns = {
        value for value in (supplier_inn, await _own_inn(session)) if value
    }
    now = now_moscow().isoformat()

    for item in stale:
        codes = codes_by_item[item.id]
        other_codes: set[str] = set()
        for other in items:
            if other.id != item.id:
                other_codes.update(codes_by_item[other.id])
        issues = _local_issues(
            item,
            codes=codes,
            duplicates_other_items={c for c in codes if c in other_codes},
            registry=registry,
            receipt_id=int(receipt.id),
            alias_by_gtin=alias_by_gtin,
        )
        gis_checked = gis_enabled and gis_error is None
        if gis_checked:
            issues.extend(
                _gis_issues(
                    codes,
                    gis_by_code=gis_by_code,
                    allowed_owner_inns=allowed_inns,
                    supplier_inn_known=bool(supplier_inn),
                )
            )
        elif gis_error:
            issues.append(
                _issue(
                    "gis_unavailable",
                    "ГИС МТ не ответил, сверка по Честному знаку не "
                    f"выполнена: {gis_error}",
                    severity=SEVERITY_WARNING,
                )
            )
        item.marking_check = {
            "fingerprint": receipt_item_fingerprint(item),
            "checked_at": now,
            "gis_checked": gis_checked,
            "codes_count": len(codes),
            "status": _item_status(issues),
            "issues": issues[:_MAX_ISSUES_PER_ITEM],
            "issues_total": len(issues),
        }
    await session.flush()
    return True


async def check_receipts_marking_local(
    session: AsyncSession, receipts: Iterable[SupplierReceipt]
) -> None:
    """Ленивая локальная сверка для показа: пишет только недостающее."""
    changed = False
    for receipt in receipts:
        if await check_receipt_marking(session, receipt, use_gis=False):
            changed = True
    if changed:
        await session.commit()


def summarize_receipt_marking(receipt: SupplierReceipt) -> dict[str, Any]:
    """Итог по документу для бейджа: статус и счётчики."""
    items = [i for i in (receipt.items or []) if i.marking_codes]
    if not items:
        return {
            "marking_status": CHECK_NONE,
            "marking_error_count": 0,
            "marking_warning_count": 0,
            "marking_gis_checked": False,
        }
    errors = warnings = 0
    gis_checked = True
    unchecked = False
    for item in items:
        check = item.marking_check or {}
        if not check:
            unchecked = True
            gis_checked = False
            continue
        gis_checked = gis_checked and bool(check.get("gis_checked"))
        for issue in check.get("issues") or []:
            if issue.get("severity") == SEVERITY_ERROR:
                errors += 1
            else:
                warnings += 1
    if errors:
        status = CHECK_ERROR
    elif warnings:
        status = CHECK_WARNING
    elif unchecked:
        status = CHECK_UNCHECKED
    else:
        status = CHECK_OK
    return {
        "marking_status": status,
        "marking_error_count": errors,
        "marking_warning_count": warnings,
        "marking_gis_checked": gis_checked,
    }
