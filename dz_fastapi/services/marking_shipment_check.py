"""Проверка кодов маркировки отгрузки клиенту перед исходящим УПД.

Коды в УПД берутся из ``allocation.marking_codes`` — их закрепляет
``allocate_marking_codes_for_shipment_allocation`` при проведении отгрузки
(FIFO по партиям, статус кода IN_STOCK → SHIPPED). Здесь сверяем: по
каждой строке кодов столько же, сколько штук (если товар маркируемый), коды
закреплены именно за этой отгрузкой и — при выполненном входе в ГИС МТ —
в ГИС МТ они в обороте (INTRODUCED) и принадлежат нам, иначе передача
права собственности покупателю не пройдёт. Нехватка кодов и коды, не
закреплённые за отгрузкой, блокируют формализованный УПД: иначе покупатель
получит товар без передачи права собственности на код.
"""
from __future__ import annotations

from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.models.inventory import MarkingCodeStatus, ProductMarkingCode, ShipmentDocument
from dz_fastapi.services.marking_receipt_check import (
    GIS_OK_STATUS,
    GIS_STATUS_TEXT,
    SEVERITY_ERROR,
    _digits,
    _fetch_gis_info,
    _issue,
    _own_inn,
)

# Расхождения, с которыми формализованный УПД отправлять нельзя.
# Причины по ГИС МТ появляются, только если сверка с ним выполнена.
BLOCKING_REASONS = frozenset(
    {
        "quantity_mismatch",
        "registry_state",
        "gis_not_found",
        "gis_status",
        "gis_owner",
    }
)


def split_marking_issues(
    issues: list[dict[str, Any]],
) -> tuple[list[str], list[str]]:
    """Делит замечания на блокирующие отправку УПД и предупреждения."""
    blockers: list[str] = []
    warnings: list[str] = []
    for issue in issues:
        text = f"КИЗ: {issue['message']}"
        if (
            issue.get("severity") == SEVERITY_ERROR
            and issue.get("reason") in BLOCKING_REASONS
        ):
            blockers.append(text)
        else:
            warnings.append(text)
    return blockers, warnings


async def check_shipment_marking(
    session: AsyncSession,
    document: ShipmentDocument,
    *,
    use_gis: bool = False,
) -> dict[str, Any]:
    from dz_fastapi.services.diadoc_outgoing import _collect_shipment_item_marking_codes

    items = list(getattr(document, "items", []) or [])
    codes_by_item = {
        item.id: _collect_shipment_item_marking_codes(item) for item in items
    }
    all_codes = sorted({c for codes in codes_by_item.values() for c in codes})
    autopart_ids = {
        int(item.autopart_id) for item in items if item.autopart_id
    }

    marked_autoparts: set[int] = set()
    if autopart_ids:
        marked_autoparts = set(
            (
                await session.execute(
                    select(ProductMarkingCode.autopart_id)
                    .where(ProductMarkingCode.autopart_id.in_(autopart_ids))
                    .distinct()
                )
            ).scalars().all()
        )
    from dz_fastapi.services.marking_codes import autoparts_marked_by_category

    marked_autoparts |= await autoparts_marked_by_category(session, autopart_ids)
    registry: dict[str, ProductMarkingCode] = {}
    if all_codes:
        registry = {
            str(row.code): row
            for row in (
                await session.execute(
                    select(ProductMarkingCode).where(
                        ProductMarkingCode.code.in_(all_codes)
                    )
                )
            ).scalars().all()
        }

    gis_by_code: dict[str, dict[str, str]] = {}
    gis_checked = False
    gis_error = None
    if use_gis and all_codes:
        from dz_fastapi.services.gis_mt import get_or_create_gis_mt_settings, gis_mt_token_is_active

        if gis_mt_token_is_active(await get_or_create_gis_mt_settings(session)):
            gis_by_code, gis_error = await _fetch_gis_info(session, all_codes)
            gis_checked = gis_error is None
    own_inn = await _own_inn(session) if gis_checked else ""

    issues: list[dict[str, Any]] = []
    for index, item in enumerate(items, start=1):
        codes = codes_by_item[item.id]
        quantity = int(item.quantity or 0)
        label = f"Строка {index}"
        is_marked = bool(codes) or (
            item.autopart_id is not None and item.autopart_id in marked_autoparts
        )
        if is_marked and len(codes) != quantity:
            issues.append(
                _issue(
                    "quantity_mismatch",
                    f"{label}: в УПД пойдёт {len(codes)} кодов маркировки "
                    f"из {quantity} шт. — товар маркируемый, нужно по коду "
                    "на каждую штуку",
                )
            )
        for code in codes:
            row = registry.get(code)
            if row is not None and (
                row.status != MarkingCodeStatus.SHIPPED
                or row.shipment_document_id != document.id
            ):
                issues.append(
                    _issue(
                        "registry_state",
                        f"{label}: код не закреплён за этой отгрузкой "
                        f"(статус в учёте: {getattr(row.status, 'value', row.status)})",
                        code=code,
                    )
                )
            if not gis_checked:
                continue
            info = gis_by_code.get(code)
            if info is None:
                issues.append(
                    _issue(
                        "gis_not_found",
                        f"{label}: ГИС МТ не вернул данных по коду",
                        code=code,
                    )
                )
                continue
            if info["status"] != GIS_OK_STATUS:
                text = GIS_STATUS_TEXT.get(info["status"], info["status"] or "нет статуса")
                issues.append(
                    _issue(
                        "gis_status",
                        f"{label}: статус в ГИС МТ — {text}, передать "
                        "покупателю нельзя (нужен INTRODUCED)",
                        code=code,
                    )
                )
            elif own_inn and info["owner_inn"] and info["owner_inn"] != _digits(own_inn):
                issues.append(
                    _issue(
                        "gis_owner",
                        f"{label}: владелец кода в ГИС МТ — ИНН "
                        f"{info['owner_inn']}, а не мы: передать нельзя",
                        code=code,
                    )
                )
    if gis_error:
        issues.append(
            _issue(
                "gis_unavailable",
                f"ГИС МТ не ответил, сверка не выполнена: {gis_error}",
                severity="warning",
            )
        )
    errors = sum(1 for i in issues if i["severity"] == SEVERITY_ERROR)
    return {
        "shipment_id": int(document.id),
        "codes_total": len(all_codes),
        "gis_checked": gis_checked,
        "status": (
            "none" if not all_codes and not issues
            else "error" if errors else "warning" if issues else "ok"
        ),
        "issues": issues,
    }
