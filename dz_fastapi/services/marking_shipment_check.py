"""Сверка кодов исходящего УПД со сканами сборки, складским реестром и ГИС МТ."""
from __future__ import annotations

from collections import defaultdict
from decimal import Decimal, InvalidOperation
from typing import Any
from xml.etree import ElementTree as ET

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.models.inventory import (
    MarkingCodeStatus,
    ProductMarkingCode,
    ShipmentDocument,
    ShipmentDocumentStatus,
    ShipmentMarkingScan,
)
from dz_fastapi.models.settings import GisMtSettings
from dz_fastapi.services.gis_mt import gis_mt_token_is_active
from dz_fastapi.services.marking_receipt_check import (
    GIS_OK_STATUS,
    GIS_STATUS_TEXT,
    SEVERITY_ERROR,
    _fetch_gis_info,
    _issue,
    _own_inn,
)

# Расхождения, с которыми формализованный УПД отправлять нельзя.
BLOCKING_REASONS = frozenset(
    {
        "quantity_mismatch",
        "registry_state",
        "scan_mismatch",
        "gis_not_found",
        "gis_status",
        "gis_owner",
        "gis_unavailable",
        "gis_auth",
        "gis_seller",
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


class ShipmentMarkingBlocked(ValueError):
    """Маркировка отгрузки препятствует передаче УПД."""


async def check_shipment_marking(
    session: AsyncSession,
    document: ShipmentDocument,
    *,
    use_gis: bool = False,
    title_seller_inn: str | None = None,
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
    from dz_fastapi.services.marking_codes import (
        autoparts_marked_by_category,
        shipment_line_has_marked_lot,
    )

    marked_autoparts |= await autoparts_marked_by_category(session, autopart_ids)
    registry: dict[str, ProductMarkingCode] = {}
    if all_codes:
        registry = {
            str(row.code): row
            for row in (
                await session.execute(
                    select(ProductMarkingCode)
                    .where(ProductMarkingCode.code.in_(all_codes))
                    .execution_options(populate_existing=True)
                )
            )
            .scalars()
            .all()
        }

    scans_by_item = defaultdict(dict)
    for item_id, code, registry_id in await session.execute(
        select(
            ShipmentMarkingScan.shipment_document_item_id,
            ShipmentMarkingScan.code,
            ShipmentMarkingScan.marking_code_id,
        ).where(ShipmentMarkingScan.shipment_document_id == document.id)
    ):
        scans_by_item[item_id][code] = registry_id

    gis_by_code: dict[str, dict[str, str]] = {}
    gis_checked = False
    issues: list[dict[str, Any]] = []
    own_inn = ""
    if use_gis and all_codes:
        settings = await session.scalar(select(GisMtSettings).limit(1))
        if settings is None or not gis_mt_token_is_active(settings):
            issues.append(_issue(
                "gis_auth", "Войдите в ГИС МТ для проверки кодов перед отправкой УПД."
            ))
        else:
            own_inn = await _own_inn(session)
            if len(own_inn) not in (10, 12):
                issues.append(_issue(
                    "gis_seller",
                    "Заполните ИНН продавца в настройках Диадока для сверки владельца кодов.",
                ))
            elif title_seller_inn is not None and title_seller_inn != own_inn:
                issues.append(_issue(
                    "gis_seller",
                    "ИНН продавца в файле УПД отличается от ИНН организации Диадока",
                ))
            else:
                gis_by_code, gis_error = await _fetch_gis_info(session, all_codes)
                gis_checked = gis_error is None
                if not gis_checked:
                    issues.append(_issue(
                        "gis_unavailable", f"Не удалось проверить коды в ГИС МТ. {gis_error}"
                    ))

    for index, item in enumerate(items, start=1):
        codes = codes_by_item[item.id]
        quantity = int(item.quantity or 0)
        label = f"Строка {index}"
        scans = scans_by_item[item.id]
        is_marked = (
            bool(codes or scans)
            or item.autopart_id in marked_autoparts
            or shipment_line_has_marked_lot(item)
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
        if set(codes) != scans.keys():
            issues.append(
                _issue(
                    "scan_mismatch",
                    f"{label}: назначенные коды отличаются от сканов сборки. "
                    f"Совпало {len(set(codes).intersection(scans))} из {quantity}. "
                    "Сверьте коды собранных экземпляров в накладной.",
                )
            )
        for code in codes:
            row = registry.get(code)
            if row is None:
                issues.append(
                    _issue(
                        "registry_state", f"{label}: код отсутствует в складском реестре", code=code
                    )
                )
            elif (
                row.status != MarkingCodeStatus.SHIPPED
                or row.shipment_document_id != document.id
                or row.shipment_document_item_id != item.id
                or row.autopart_id != item.autopart_id
                or (code in scans and scans[code] != row.id)
            ):
                issues.append(
                    _issue(
                        "registry_state",
                        f"{label}: код не закреплён за этой строкой отгрузки "
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
            elif not info["owner_inn"]:
                issues.append(_issue(
                    "gis_owner", f"{label}: ГИС МТ не сообщил ИНН владельца кода", code=code
                ))
            elif info["owner_inn"] != own_inn:
                issues.append(
                    _issue(
                        "gis_owner",
                        f"{label}: владелец кода в ГИС МТ — ИНН "
                        f"{info['owner_inn']}, а не мы: передать нельзя",
                        code=code,
                    )
                )
    if not all_codes:
        gis_state = "no_codes"
    elif not use_gis:
        gis_state = "not_requested"
    elif not gis_checked:
        gis_state = "deferred"
    elif any(issue["reason"].startswith("gis_") for issue in issues):
        gis_state = "rejected"
    else:
        gis_state = "verified"
    errors = sum(1 for i in issues if i["severity"] == SEVERITY_ERROR)
    return {
        "shipment_id": int(document.id),
        "codes_total": len(all_codes),
        "gis_checked": gis_checked,
        "gis_state": gis_state,
        "status": (
            "none"
            if not all_codes and not issues
            else "error" if errors else "warning" if issues else "ok"
        ),
        "issues": issues,
    }


async def validate_shipment_upd_marking(
    session: AsyncSession, shipment_id: int, content: bytes
) -> None:
    """Сверяет фактический титул продавца в момент передачи в Диадок."""
    from dz_fastapi.services.diadoc_outgoing import (
        _collect_shipment_item_marking_codes,
        _load_shipment_document,
    )

    document = await _load_shipment_document(session, shipment_id=shipment_id, lock=True)
    if document is None or document.status != ShipmentDocumentStatus.POSTED:
        raise ShipmentMarkingBlocked("Для отправки УПД нужна проведённая отгрузка")
    try:
        root = ET.fromstring(content)
    except (ET.ParseError, LookupError, ValueError) as exc:
        raise ShipmentMarkingBlocked("Не удалось прочитать XML УПД для сверки маркировки") from exc
    from dz_fastapi.services.upd_xml import _extract_seller

    _, seller_inn = _extract_seller(root)
    marking = await check_shipment_marking(
        session, document, use_gis=True, title_seller_inn=seller_inn or ""
    )
    blockers, _ = split_marking_issues(marking["issues"])
    if blockers:
        raise ShipmentMarkingBlocked("; ".join(blockers[:10]))
    groups = root.findall(".//{*}НомСредИдентТов")
    if not groups and not marking["codes_total"]:
        return
    expected = {
        n: sorted(_collect_shipment_item_marking_codes(item))
        for n, item in enumerate(document.items, start=1)
    }
    actual = {}
    checked_groups = 0
    for line in root.findall(".//{*}СведТов"):
        number = int(line.get("НомСтр") or 0)
        if number in actual:
            raise ShipmentMarkingBlocked("В УПД повторяется номер товарной строки")
        if expected.get(number):
            try:
                quantity = Decimal(line.get("КолТов") or "")
            except InvalidOperation as exc:
                raise ShipmentMarkingBlocked(
                    "В файле УПД отсутствует количество маркированного товара"
                ) from exc
            if quantity != len(expected[number]):
                raise ShipmentMarkingBlocked(
                    "Количество в файле УПД отличается от количества сканов строки"
                )
        codes = []
        for group in line.findall(".//{*}НомСредИдентТов"):
            checked_groups += 1
            # Поэкземплярный УПД содержит индивидуальные КИЗ. Любая другая
            # форма идентификатора требует отдельного сопоставления.
            units = group.findall("{*}КИЗ")
            if len(group) != len(units) or any(key != "КИЗ" for key in group.attrib):
                raise ShipmentMarkingBlocked("В УПД указан код упаковки вместо кода экземпляра")
            values = [u.text for u in units] + [group.get("КИЗ"), group.text]
            codes.extend(value.strip() for value in values if value and value.strip())
        actual[number] = sorted(codes)
    if actual != expected or checked_groups != len(groups):
        raise ShipmentMarkingBlocked(
            "Коды в файле УПД отличаются от сканов по строкам. Проверьте черновик Диадока."
        )
