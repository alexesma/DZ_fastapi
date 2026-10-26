"""Печатные формы реализации: УПД, упаковочный лист, транспортная накладная.

Формы собираются в HTML (A4) для печати из браузера или через релей Windows.

Правовая основа (проверено на 25.10.2026, см. LEGAL_NOTES ниже):

* УПД — счёт-фактура по форме Приложения № 1 к постановлению Правительства
  РФ от 26.12.2011 № 1137 в редакции постановления Правительства РФ от
  23.01.2026 № 26 (действует с 01.04.2026; новая строка 5б «К счёту-фактуре,
  выставленному на аванс», ОГРНИП и дата присвоения, ставка НДС 22 %) плюс
  передаточный документ (статус 1) или только передаточный документ
  (статус 2). Рекомендация ФНС — письмо от 21.10.2013 № ММВ-20-3/96@.
* Упаковочный лист — унифицированной формы нет; реквизиты определяются
  договором и таможенной практикой, форма свободная.
* Транспортная накладная — форма Приложения № 4 к Правилам перевозок грузов
  автомобильным транспортом (постановление Правительства РФ от 21.12.2020
  № 2200). С 01.09.2026 действует обязательное электронное оформление (ЭТрН,
  ФЗ от 07.06.2025 № 140-ФЗ) через оператора ЭДО; бумажная накладная
  допускается только в случаях, установленных Минтрансом. Новая редакция
  правил и формы на дату проверки не утверждена, поэтому печатается форма
  Приложения № 4 с предупреждением на листе.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path
from typing import Any

from jinja2 import Environment, FileSystemLoader, select_autoescape
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.core.time import now_moscow
from dz_fastapi.models.inventory import ShipmentDocument

LEGAL_NOTES = {
    "upd": (
        "Приложение № 1 к постановлению Правительства РФ от 26.12.2011 "
        "№ 1137 в редакции постановления Правительства РФ от 23.01.2026 "
        "№ 26 (с 01.04.2026)"
    ),
    "packing_list": "Свободная форма (унифицированной формы нет)",
    "transport_waybill": (
        "Приложение № 4 к Правилам перевозок грузов автомобильным "
        "транспортом (постановление Правительства РФ от 21.12.2020 № 2200)"
    ),
}

DOCUMENT_KINDS = {
    "upd": "УПД",
    "packing_list": "Упаковочный лист",
    "transport_waybill": "Транспортная накладная",
}

_TEMPLATES_DIR = Path(__file__).parent.parent / "templates" / "shipment"
_env = Environment(
    loader=FileSystemLoader(str(_TEMPLATES_DIR)),
    autoescape=select_autoescape(["html"]),
)

CENT = Decimal("0.01")
UNIT_PIECE_OKEI = "796"
UNIT_PIECE_NAME = "шт"
EMPTY = "—"


def _dec(value: Any, default: str = "0") -> Decimal:
    try:
        return Decimal(str(value if value is not None else default))
    except Exception:  # noqa: BLE001
        return Decimal(default)


def _money(value: Decimal) -> str:
    return f"{value.quantize(CENT, rounding=ROUND_HALF_UP):,.2f}".replace(
        ",", " "
    ).replace(".", ",")


def _fmt_date(value: date | datetime | None) -> str:
    if not value:
        return ""
    return value.strftime("%d.%m.%Y")


def _fmt_qty(value: Any) -> str:
    number = _dec(value)
    if number == number.to_integral_value():
        return str(int(number))
    return str(number.normalize())


def _party(name: str | None, inn: str | None, kpp: str | None,
           address: str | None) -> dict[str, str]:
    return {
        "name": (name or "").strip(),
        "inn": (inn or "").strip(),
        "kpp": (kpp or "").strip(),
        "address": (address or "").strip(),
    }


async def load_seller(session: AsyncSession) -> dict[str, Any]:
    """Реквизиты продавца из «Настройки → Реквизиты организации»."""
    from dz_fastapi.services.company_profile import load_company_requisites

    return await load_company_requisites(session)


def _buyer(customer: Any) -> dict[str, str]:
    if customer is None:
        return _party("", "", "", "")
    name = (
        getattr(customer, "legal_name", None) or getattr(customer, "name", None)
    )
    buyer = _party(
        name,
        getattr(customer, "inn", None),
        getattr(customer, "kpp", None),
        getattr(customer, "legal_address", None),
    )
    buyer["postal_address"] = (
        getattr(customer, "postal_address", None) or buyer["address"]
    )
    buyer["phone"] = getattr(customer, "phone", None) or ""
    return buyer


def _item_origin(item: Any) -> tuple[str, str, str]:
    """Страна происхождения и номер ГТД по партиям, из которых списано."""
    codes: list[str] = []
    names: list[str] = []
    gtds: list[str] = []
    lots = [
        getattr(allocation, "stock_lot", None)
        for allocation in (getattr(item, "allocations", None) or [])
    ]
    lot = getattr(item, "lot", None)
    if lot is not None:
        lots.append(lot)
    for lot in lots:
        if lot is None:
            continue
        for bucket, value in (
            (codes, getattr(lot, "country_code", None)),
            (names, getattr(lot, "country_name", None)),
            (gtds, getattr(lot, "gtd_number", None)),
        ):
            value = (value or "").strip()
            if value and value not in bucket:
                bucket.append(value)
    return (
        ", ".join(codes) or "--",
        ", ".join(names) or "--",
        ", ".join(gtds) or "--",
    )


def _item_title(item: Any) -> str:
    brand = (
        getattr(item, "customer_brand", None)
        or getattr(getattr(getattr(item, "autopart", None), "brand", None),
                   "name", None)
        or ""
    )
    oem = (
        getattr(item, "customer_oem", None)
        or getattr(getattr(item, "autopart", None), "oem_number", None)
        or ""
    )
    name = (
        getattr(item, "customer_name", None)
        or getattr(getattr(item, "autopart", None), "name", None)
        or ""
    )
    return " ".join(part for part in (brand, oem, name) if part).strip()


def _item_code(item: Any) -> str:
    return (
        getattr(item, "customer_oem", None)
        or getattr(getattr(item, "autopart", None), "oem_number", None)
        or ""
    )


def build_upd_rows(doc: ShipmentDocument) -> tuple[list[dict], dict]:
    """Строки УПД: цена в накладной — с НДС, расчёт ведём от неё."""
    rows: list[dict] = []
    total_net = total_vat = total_gross = Decimal("0")
    for index, item in enumerate(doc.items or [], start=1):
        qty = _dec(item.quantity)
        gross_unit = _dec(item.price)
        rate = _dec(item.vat_rate, "22")
        divisor = Decimal("1") + rate / Decimal("100")
        net_unit = (gross_unit / divisor).quantize(
            CENT, rounding=ROUND_HALF_UP
        )
        net_total = (net_unit * qty).quantize(CENT, rounding=ROUND_HALF_UP)
        gross_total = (gross_unit * qty).quantize(CENT, rounding=ROUND_HALF_UP)
        vat_total = gross_total - net_total
        country_code, country_name, gtd = _item_origin(item)
        rows.append({
            "n": index,
            "code": _item_code(item),
            "title": _item_title(item),
            "unit_code": UNIT_PIECE_OKEI,
            "unit_name": UNIT_PIECE_NAME,
            "qty": _fmt_qty(qty),
            "price": _money(net_unit),
            "net": _money(net_total),
            "vat_rate": f"{_fmt_qty(rate)}%",
            "vat": _money(vat_total),
            "gross": _money(gross_total),
            "country_code": country_code,
            "country_name": country_name,
            "gtd": gtd,
        })
        total_net += net_total
        total_vat += vat_total
        total_gross += gross_total
    totals = {
        "net": _money(total_net),
        "vat": _money(total_vat),
        "gross": _money(total_gross),
        "count": len(rows),
    }
    return rows, totals


def _doc_number(doc: ShipmentDocument) -> str:
    return doc.doc_number or str(doc.id)


async def _base_context(
    session: AsyncSession, doc: ShipmentDocument
) -> dict[str, Any]:
    seller = await load_seller(session)
    customer = getattr(doc, "customer", None)
    return {
        "doc": doc,
        "doc_number": _doc_number(doc),
        "doc_date": _fmt_date(doc.doc_date),
        "seller": seller,
        "buyer": _buyer(customer),
        "generated_at": now_moscow().strftime("%d.%m.%Y %H:%M"),
        "legal_basis": "",
        "empty": EMPTY,
    }


async def render_upd(
    session: AsyncSession, doc: ShipmentDocument, *, status: int = 1
) -> str:
    context = await _base_context(session, doc)
    rows, totals = build_upd_rows(doc)
    context.update({
        "rows": rows,
        "totals": totals,
        "status": 2 if int(status) == 2 else 1,
        "legal_basis": LEGAL_NOTES["upd"],
        "order_numbers": ", ".join(sorted({
            str(item.customer_order_item.order_id)
            for item in (doc.items or [])
            if getattr(item, "customer_order_item", None) is not None
        })),
    })
    return _env.get_template("upd.html").render(**context)


def _packages(doc: ShipmentDocument) -> list[dict]:
    by_item = {item.id: item for item in (doc.items or [])}
    boxes: list[dict] = []
    for package in sorted(doc.packages or [], key=lambda row: row.number):
        lines = []
        weight = Decimal("0")
        for entry in package.items or []:
            item = by_item.get(entry.shipment_item_id)
            if item is None:
                continue
            unit_weight = _dec(
                getattr(getattr(item, "autopart", None), "weight", None)
            )
            weight += unit_weight * _dec(entry.quantity)
            lines.append({
                "code": _item_code(item),
                "title": _item_title(item),
                "qty": _fmt_qty(entry.quantity),
            })
        boxes.append({
            "number": package.number,
            "lines": lines,
            "qty": sum(int(line["qty"]) for line in lines),
            "weight": (
                f"{weight.quantize(Decimal('0.001'), rounding=ROUND_HALF_UP)}"
                if weight else ""
            ),
        })
    return boxes


async def render_packing_list(
    session: AsyncSession, doc: ShipmentDocument
) -> str:
    context = await _base_context(session, doc)
    boxes = _packages(doc)
    unboxed: dict[int, int] = defaultdict(int)
    boxed: dict[int, int] = defaultdict(int)
    for package in doc.packages or []:
        for entry in package.items or []:
            boxed[entry.shipment_item_id] += int(entry.quantity or 0)
    for item in doc.items or []:
        left = int(item.quantity or 0) - boxed.get(item.id, 0)
        if left > 0:
            unboxed[item.id] = left
    loose = [
        {"code": _item_code(item), "title": _item_title(item),
         "qty": str(unboxed[item.id])}
        for item in (doc.items or []) if item.id in unboxed
    ]
    total_weight = sum(
        (_dec(box["weight"]) for box in boxes if box["weight"]), Decimal("0")
    )
    context.update({
        "boxes": boxes,
        "loose": loose,
        "total_places": len(boxes),
        "total_qty": sum(int(item.quantity or 0) for item in (doc.items or [])),
        "total_weight": (
            f"{total_weight.quantize(Decimal('0.001'), rounding=ROUND_HALF_UP)}"
            if total_weight else ""
        ),
        "legal_basis": LEGAL_NOTES["packing_list"],
    })
    return _env.get_template("packing_list.html").render(**context)


async def render_transport_waybill(
    session: AsyncSession, doc: ShipmentDocument
) -> str:
    context = await _base_context(session, doc)
    items = list(doc.items or [])
    boxes = _packages(doc)
    total_weight = sum(
        (_dec(box["weight"]) for box in boxes if box["weight"]), Decimal("0")
    )
    total_qty = sum(int(item.quantity or 0) for item in items)
    total_sum = sum(
        (_dec(item.price) * _dec(item.quantity) for item in items),
        Decimal("0"),
    )
    context.update({
        "places": len(boxes),
        "total_qty": total_qty,
        "total_weight": (
            f"{total_weight.quantize(Decimal('0.001'), rounding=ROUND_HALF_UP)}"
            if total_weight else ""
        ),
        "total_sum": _money(total_sum),
        "warehouse_name": getattr(
            getattr(doc, "warehouse", None), "name", ""
        ) or "",
        "legal_basis": LEGAL_NOTES["transport_waybill"],
    })
    return _env.get_template("transport_waybill.html").render(**context)
