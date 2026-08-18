"""Обмен с 1С.

Два канала:
1. Стандартный протокол «Обмен с сайтом» (CommerceML 2) — 1С:УТ сама
   ходит на наш URL по расписанию и забирает проведённые отгрузки как
   документы «Заказ товара» (бухгалтер создаёт реализацию «на
   основании»). Выгруженные отгрузки помечаются sync_status=SYNCED.
2. Ручные выгрузки файлами (Excel/CommerceML XML) за период: реализации,
   поступления, контрагенты, номенклатура — под типовую «Загрузку данных
   из табличного документа».
"""

from __future__ import annotations

import asyncio
import logging
import xml.etree.ElementTree as ET
from datetime import date, datetime
from decimal import Decimal
from io import BytesIO
from types import SimpleNamespace
from typing import Any, Optional, Sequence

import pandas as pd
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from dz_fastapi.core.time import now_moscow
from dz_fastapi.models.autopart import AutoPart
from dz_fastapi.models.brand import Brand
from dz_fastapi.models.inventory import (
    ShipmentDocument,
    ShipmentDocumentItem,
    ShipmentDocumentStatus,
    SyncStatus,
)
from dz_fastapi.models.partner import Customer, Provider, SupplierReceipt

logger = logging.getLogger("dz_fastapi")

COMMERCEML_SCHEMA_VERSION = "2.05"
DEFAULT_VAT_RATE = "20"
SALE_QUERY_LIMIT = 100


def _fmt_money(value: Any) -> str:
    if value is None:
        return "0.00"
    return f"{Decimal(str(value)):.2f}"


def _shipment_document_number(document: ShipmentDocument) -> str:
    number = str(document.doc_number or "").strip()
    return number or f"DZ-{document.id}"


async def list_shipments_for_1c(
    session: AsyncSession,
    *,
    only_pending: bool = True,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
    limit: Optional[int] = None,
) -> list[ShipmentDocument]:
    stmt = (
        select(ShipmentDocument)
        .where(ShipmentDocument.status == ShipmentDocumentStatus.POSTED)
        .options(
            selectinload(ShipmentDocument.items)
            .selectinload(ShipmentDocumentItem.autopart)
            .selectinload(AutoPart.brand),
            selectinload(ShipmentDocument.customer),
        )
        .order_by(ShipmentDocument.doc_date.asc(), ShipmentDocument.id.asc())
    )
    if only_pending:
        stmt = stmt.where(ShipmentDocument.sync_status == SyncStatus.PENDING)
    if date_from is not None:
        stmt = stmt.where(
            ShipmentDocument.doc_date >= datetime.combine(date_from, datetime.min.time())
        )
    if date_to is not None:
        stmt = stmt.where(
            ShipmentDocument.doc_date <= datetime.combine(date_to, datetime.max.time())
        )
    if limit:
        stmt = stmt.limit(int(limit))
    return list((await session.execute(stmt)).scalars().all())


def build_commerceml_sale_xml(
    shipments: Sequence[ShipmentDocument],
    *,
    vat_rate: str = DEFAULT_VAT_RATE,
    formed_at: Optional[datetime] = None,
) -> bytes:
    """Отгрузки → CommerceML «КоммерческаяИнформация» с документами."""
    root = ET.Element(
        "КоммерческаяИнформация",
        {
            "ВерсияСхемы": COMMERCEML_SCHEMA_VERSION,
            "ДатаФормирования": (formed_at or now_moscow()).strftime("%Y-%m-%dT%H:%M:%S"),
        },
    )
    for document in shipments:
        customer = getattr(document, "customer", None)
        doc_el = ET.SubElement(root, "Документ")
        ET.SubElement(doc_el, "Ид").text = f"dz-shipment-{document.id}"
        ET.SubElement(doc_el, "Номер").text = _shipment_document_number(document)
        doc_date = document.doc_date or document.created_at
        ET.SubElement(doc_el, "Дата").text = doc_date.strftime("%Y-%m-%d")
        ET.SubElement(doc_el, "ХозОперация").text = "Заказ товара"
        ET.SubElement(doc_el, "Роль").text = "Продавец"
        ET.SubElement(doc_el, "Валюта").text = "руб"
        ET.SubElement(doc_el, "Курс").text = "1"
        total = sum(
            (Decimal(str(item.price or 0)) * int(item.quantity or 0))
            for item in (document.items or [])
        )
        ET.SubElement(doc_el, "Сумма").text = _fmt_money(total)

        counterparties = ET.SubElement(doc_el, "Контрагенты")
        counterparty = ET.SubElement(counterparties, "Контрагент")
        customer_id = getattr(customer, "id", None) or 0
        name = str(getattr(customer, "name", "") or "").strip()
        ET.SubElement(counterparty, "Ид").text = f"dz-customer-{customer_id}"
        ET.SubElement(counterparty, "Наименование").text = name or "Розничный покупатель"
        ET.SubElement(counterparty, "ПолноеНаименование").text = name or "Розничный покупатель"
        inn = str(getattr(customer, "inn", "") or "").strip()
        kpp = str(getattr(customer, "kpp", "") or "").strip()
        if inn:
            ET.SubElement(counterparty, "ИНН").text = inn
        if kpp:
            ET.SubElement(counterparty, "КПП").text = kpp
        ET.SubElement(counterparty, "Роль").text = "Покупатель"

        ET.SubElement(doc_el, "Время").text = doc_date.strftime("%H:%M:%S")
        if document.notes:
            ET.SubElement(doc_el, "Комментарий").text = str(document.notes)[:1000]

        goods = ET.SubElement(doc_el, "Товары")
        for item in document.items or []:
            autopart = getattr(item, "autopart", None)
            brand = getattr(autopart, "brand", None)
            client_oem = str(
                getattr(item, "customer_oem", None) or getattr(autopart, "oem_number", "") or ""
            ).strip()
            client_brand = str(
                getattr(item, "customer_brand", None) or getattr(brand, "name", "") or ""
            ).strip()
            client_name = str(
                getattr(item, "customer_name", None) or getattr(autopart, "name", "") or ""
            ).strip()
            good = ET.SubElement(goods, "Товар")
            ET.SubElement(good, "Ид").text = f"dz-autopart-{item.autopart_id}"
            ET.SubElement(good, "Артикул").text = client_oem
            part_name = " ".join(
                part
                for part in (
                    client_brand,
                    client_oem,
                    client_name,
                )
                if part
            )
            ET.SubElement(good, "Наименование").text = part_name or f"Запчасть #{item.autopart_id}"
            ET.SubElement(
                good,
                "БазоваяЕдиница",
                {"Код": "796", "НаименованиеПолное": "Штука"},
            ).text = "шт"
            ET.SubElement(good, "ЦенаЗаЕдиницу").text = _fmt_money(item.price)
            ET.SubElement(good, "Количество").text = str(int(item.quantity or 0))
            ET.SubElement(good, "Сумма").text = _fmt_money(
                Decimal(str(item.price or 0)) * int(item.quantity or 0)
            )
            taxes = ET.SubElement(good, "СтавкиНалогов")
            tax = ET.SubElement(taxes, "СтавкаНалога")
            ET.SubElement(tax, "Наименование").text = "НДС"
            ET.SubElement(tax, "Ставка").text = str(vat_rate)

        requisites = ET.SubElement(doc_el, "ЗначенияРеквизитов")
        for req_name, req_value in (
            ("Проведен", "true"),
            ("Метод оплаты", ""),
            (
                "Заказ клиента",
                str(document.customer_order_id or ""),
            ),
        ):
            req = ET.SubElement(requisites, "ЗначениеРеквизита")
            ET.SubElement(req, "Наименование").text = req_name
            ET.SubElement(req, "Значение").text = req_value

    return ET.tostring(root, encoding="utf-8", xml_declaration=True)


def build_commerceml_sale_xml_from_snapshots(
    snapshots: Sequence[dict[str, Any]],
    *,
    formed_at: Optional[datetime] = None,
) -> bytes:
    """Build CommerceML from immutable outbox snapshots, not live rows."""
    shipments = []
    for snapshot in snapshots:
        customer_data = snapshot.get("customer") or {}
        customer = SimpleNamespace(**customer_data) if customer_data else None
        items = []
        for row in snapshot.get("items") or []:
            physical = row.get("physical") or {}
            brand_name = physical.get("brand")
            autopart = SimpleNamespace(
                oem_number=physical.get("oem"),
                name=physical.get("name"),
                brand=(SimpleNamespace(name=brand_name) if brand_name else None),
            )
            items.append(
                SimpleNamespace(
                    autopart_id=physical.get("autopart_id"),
                    autopart=autopart,
                    customer_oem=row.get("customer_oem"),
                    customer_brand=row.get("customer_brand"),
                    customer_name=row.get("customer_name"),
                    quantity=row.get("quantity"),
                    price=row.get("price"),
                )
            )
        raw_date = snapshot.get("document_date")
        try:
            document_date = datetime.fromisoformat(str(raw_date))
        except (TypeError, ValueError):
            document_date = now_moscow()
        shipments.append(
            SimpleNamespace(
                id=snapshot.get("document_id"),
                doc_number=snapshot.get("document_number"),
                doc_date=document_date,
                created_at=document_date,
                notes=snapshot.get("notes"),
                customer=customer,
                customer_order_id=snapshot.get("customer_order_id"),
                items=items,
            )
        )
    return build_commerceml_sale_xml(shipments, formed_at=formed_at)


async def mark_shipments_synced(
    session: AsyncSession,
    shipment_ids: Sequence[int],
) -> int:
    if not shipment_ids:
        return 0
    rows = (
        (
            await session.execute(
                select(ShipmentDocument).where(ShipmentDocument.id.in_(list(shipment_ids)))
            )
        )
        .scalars()
        .all()
    )
    for row in rows:
        row.sync_status = SyncStatus.SYNCED
        session.add(row)
    await session.commit()
    return len(rows)


async def reset_shipments_sync_status(
    session: AsyncSession,
    *,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
) -> int:
    """Возвращает отгрузки периода в очередь выгрузки (PENDING)."""
    stmt = select(ShipmentDocument).where(
        ShipmentDocument.status == ShipmentDocumentStatus.POSTED,
        ShipmentDocument.sync_status == SyncStatus.SYNCED,
    )
    if date_from is not None:
        stmt = stmt.where(
            ShipmentDocument.doc_date >= datetime.combine(date_from, datetime.min.time())
        )
    if date_to is not None:
        stmt = stmt.where(
            ShipmentDocument.doc_date <= datetime.combine(date_to, datetime.max.time())
        )
    rows = (await session.execute(stmt)).scalars().all()
    for row in rows:
        row.sync_status = SyncStatus.PENDING
        session.add(row)
    await session.commit()
    return len(rows)


def _rows_to_xlsx_bytes(rows: list[dict], sheet_name: str) -> bytes:
    """CPU-bound (pandas+openpyxl): вызывать только через to_thread,
    иначе генерация большого файла блокирует весь event loop."""
    df = pd.DataFrame(rows)
    buffer = BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
    return buffer.getvalue()


async def build_shipments_xlsx(
    session: AsyncSession,
    *,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
    only_pending: bool = False,
) -> bytes:
    """Реализации за период — строками по позициям."""
    shipments = await list_shipments_for_1c(
        session,
        only_pending=only_pending,
        date_from=date_from,
        date_to=date_to,
    )
    rows: list[dict[str, Any]] = []
    for document in shipments:
        customer = getattr(document, "customer", None)
        doc_date = document.doc_date or document.created_at
        for item in document.items or []:
            autopart = getattr(item, "autopart", None)
            brand = getattr(autopart, "brand", None)
            quantity = int(item.quantity or 0)
            price = Decimal(str(item.price or 0))
            rows.append(
                {
                    "Дата": doc_date.strftime("%d.%m.%Y"),
                    "Номер": _shipment_document_number(document),
                    "Контрагент": getattr(customer, "name", "") or "",
                    "ИНН": getattr(customer, "inn", "") or "",
                    "КПП": getattr(customer, "kpp", "") or "",
                    "Артикул": getattr(autopart, "oem_number", "") or "",
                    "Бренд": getattr(brand, "name", "") or "",
                    "Номенклатура": getattr(autopart, "name", "") or "",
                    "Количество": quantity,
                    "Цена": float(price),
                    "Сумма": float(price * quantity),
                    "Себестоимость": (
                        float(item.cost_total) if item.cost_total is not None else None
                    ),
                    "Выгружен в 1С": ("да" if document.sync_status == SyncStatus.SYNCED else "нет"),
                }
            )
    return await asyncio.to_thread(_rows_to_xlsx_bytes, rows, "Реализации")


async def build_receipts_xlsx(
    session: AsyncSession,
    *,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
) -> bytes:
    """Поступления за период — строками по позициям."""
    stmt = (
        select(SupplierReceipt)
        .where(SupplierReceipt.posted_at.is_not(None))
        .options(
            selectinload(SupplierReceipt.items),
            selectinload(SupplierReceipt.provider),
        )
        .order_by(SupplierReceipt.document_date.asc(), SupplierReceipt.id.asc())
    )
    if date_from is not None:
        stmt = stmt.where(SupplierReceipt.document_date >= date_from)
    if date_to is not None:
        stmt = stmt.where(SupplierReceipt.document_date <= date_to)
    receipts = (await session.execute(stmt)).scalars().all()
    rows: list[dict[str, Any]] = []
    for receipt in receipts:
        provider = getattr(receipt, "provider", None)
        for item in receipt.items or []:
            quantity = int(item.received_quantity or 0)
            price = Decimal(str(item.price or 0))
            rows.append(
                {
                    "Дата": (
                        receipt.document_date.strftime("%d.%m.%Y") if receipt.document_date else ""
                    ),
                    "Номер": receipt.document_number or f"R-{receipt.id}",
                    "Поставщик": getattr(provider, "name", "") or "",
                    "ИНН": getattr(provider, "inn", "") or "",
                    "КПП": getattr(provider, "kpp", "") or "",
                    "Артикул": item.oem_number or "",
                    "Бренд": item.brand_name or "",
                    "Номенклатура": item.autopart_name or "",
                    "Количество": quantity,
                    "Цена": float(price),
                    "Сумма": float(price * quantity),
                    "Сумма с НДС": (
                        float(item.total_price_with_vat)
                        if item.total_price_with_vat is not None
                        else None
                    ),
                    "ГТД": item.gtd_code or "",
                    "Страна": item.country_name or item.country_code or "",
                }
            )
    return await asyncio.to_thread(_rows_to_xlsx_bytes, rows, "Поступления")


async def build_counterparties_xlsx(session: AsyncSession) -> bytes:
    customers = (
        (await session.execute(select(Customer).order_by(Customer.name.asc()))).scalars().all()
    )
    providers = (
        (await session.execute(select(Provider).order_by(Provider.name.asc()))).scalars().all()
    )
    rows: list[dict[str, Any]] = []
    for customer in customers:
        rows.append(
            {
                "Тип": "Покупатель",
                "Наименование": customer.name,
                "ИНН": customer.inn or "",
                "КПП": customer.kpp or "",
                "Email": customer.email_contact or "",
                "Юр. адрес": getattr(customer, "legal_address", "") or "",
                "Почтовый адрес": (getattr(customer, "postal_address", "") or ""),
            }
        )
    for provider in providers:
        rows.append(
            {
                "Тип": "Поставщик",
                "Наименование": provider.name,
                "ИНН": provider.inn or "",
                "КПП": provider.kpp or "",
                "Email": provider.email_contact or "",
                "Юр. адрес": "",
                "Почтовый адрес": "",
            }
        )
    return await asyncio.to_thread(_rows_to_xlsx_bytes, rows, "Контрагенты")


async def build_nomenclature_xlsx(session: AsyncSession) -> bytes:
    stmt = (
        select(
            AutoPart.id,
            AutoPart.oem_number,
            Brand.name.label("brand_name"),
            AutoPart.name,
            AutoPart.barcode,
        )
        .join(Brand, Brand.id == AutoPart.brand_id)
        .order_by(Brand.name.asc(), AutoPart.oem_number.asc())
    )
    rows = (await session.execute(stmt)).mappings().all()
    payload_rows = [
        {
            "Код": f"dz-autopart-{row['id']}",
            "Артикул": row["oem_number"],
            "Бренд": row["brand_name"],
            "Наименование": row["name"] or "",
            "Штрихкод": row["barcode"] or "",
            "Ед.": "шт",
        }
        for row in rows
    ]
    return await asyncio.to_thread(_rows_to_xlsx_bytes, payload_rows, "Номенклатура")


async def get_one_c_exchange_status(
    session: AsyncSession,
) -> dict[str, Any]:
    from sqlalchemy import func

    pending = (
        await session.execute(
            select(func.count())
            .select_from(ShipmentDocument)
            .where(
                ShipmentDocument.status == ShipmentDocumentStatus.POSTED,
                ShipmentDocument.sync_status == SyncStatus.PENDING,
            )
        )
    ).scalar() or 0
    synced = (
        await session.execute(
            select(func.count())
            .select_from(ShipmentDocument)
            .where(
                ShipmentDocument.status == ShipmentDocumentStatus.POSTED,
                ShipmentDocument.sync_status == SyncStatus.SYNCED,
            )
        )
    ).scalar() or 0
    return {
        "pending_shipments": int(pending),
        "synced_shipments": int(synced),
    }


# ── История продаж из 1С (разовая загрузка) ─────────────────────────────

_SALES_HEADER_ALIASES = {
    "period": ("период", "месяц", "дата"),
    "oem": ("артикул", "oem", "номенклатура.артикул", "код"),
    "brand": ("бренд", "производитель", "марка"),
    "quantity": ("количество", "кол-во", "колво", "шт"),
    "revenue": ("выручка", "сумма", "сумма продажи", "оборот"),
}


def _resolve_sales_columns(df: pd.DataFrame) -> dict[str, Any]:
    """Ищет колонки по русским заголовкам (без учёта регистра)."""
    resolved: dict[str, Any] = {}
    normalized = {str(column).strip().lower(): column for column in df.columns}
    for key, aliases in _SALES_HEADER_ALIASES.items():
        for alias in aliases:
            if alias in normalized:
                resolved[key] = normalized[alias]
                break
    return resolved


def _parse_sales_period(value: Any) -> Optional[date]:
    """Дата/строка → первое число месяца."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if isinstance(value, datetime):
        return value.date().replace(day=1)
    if isinstance(value, date):
        return value.replace(day=1)
    text = str(value).strip()
    if not text:
        return None
    for fmt in ("%d.%m.%Y", "%m.%Y", "%Y-%m-%d", "%Y-%m", "%d.%m.%y"):
        try:
            return datetime.strptime(text, fmt).date().replace(day=1)
        except ValueError:
            continue
    try:
        parsed = pd.to_datetime(text, dayfirst=True, errors="coerce")
    except Exception:  # noqa: BLE001
        return None
    if pd.isna(parsed):
        return None
    return parsed.date().replace(day=1)


def _parse_sales_history_payload(
    payload: bytes,
) -> tuple[dict[tuple, dict[str, Any]], int, int]:
    """CPU-bound разбор Excel: вызывать через to_thread.

    Возвращает (агрегат по (период, артикул, бренд), пропущено, строк).
    """
    from dz_fastapi.models.autopart import preprocess_oem_number

    df = pd.read_excel(BytesIO(payload))
    columns = _resolve_sales_columns(df)
    missing = [key for key in ("period", "oem", "quantity") if key not in columns]
    if missing:
        raise ValueError(
            "Не найдены обязательные колонки: "
            + ", ".join("/".join(_SALES_HEADER_ALIASES[key]) for key in missing)
        )

    aggregated: dict[tuple, dict[str, Any]] = {}
    skipped = 0
    for _, row in df.iterrows():
        period = _parse_sales_period(row.get(columns["period"]))
        oem = preprocess_oem_number(str(row.get(columns["oem"]) or ""))
        try:
            quantity = int(float(row.get(columns["quantity"]) or 0))
        except (TypeError, ValueError):
            quantity = 0
        if not period or not oem or quantity <= 0:
            skipped += 1
            continue
        brand = ""
        if "brand" in columns:
            raw_brand = row.get(columns["brand"])
            if raw_brand is not None and not pd.isna(raw_brand):
                brand = str(raw_brand).strip().upper()
        revenue = None
        if "revenue" in columns:
            raw_revenue = row.get(columns["revenue"])
            if raw_revenue is not None and not pd.isna(raw_revenue):
                try:
                    revenue = float(str(raw_revenue).replace(",", ".").replace(" ", ""))
                except (TypeError, ValueError):
                    revenue = None
        key = (period, oem, brand)
        bucket = aggregated.setdefault(key, {"quantity": 0, "revenue": None})
        bucket["quantity"] += quantity
        if revenue is not None:
            bucket["revenue"] = (bucket["revenue"] or 0.0) + revenue
    return aggregated, skipped, int(len(df))


async def import_sales_history_xlsx(
    session: AsyncSession,
    payload: bytes,
) -> dict[str, Any]:
    """Загружает месячную историю продаж из Excel (выгрузка 1С).

    Ожидаемые колонки (регистр не важен): Период/Месяц/Дата,
    Артикул/OEM, Бренд/Производитель, Количество/Кол-во,
    Выручка/Сумма (необязательно). Данные агрегируются в месяц и
    апсертятся по (период, артикул, бренд) — повторная загрузка того
    же файла безопасна. Разбор файла идёт в отдельном потоке,
    существующие записи выбираются одним запросом по периодам.
    """
    from dz_fastapi.models.partner import SalesHistoryMonthly

    aggregated, skipped, rows_in_file = await asyncio.to_thread(
        _parse_sales_history_payload, payload
    )

    periods = sorted({key[0] for key in aggregated})
    existing_by_key: dict[tuple, SalesHistoryMonthly] = {}
    if periods:
        existing_rows = (
            (
                await session.execute(
                    select(SalesHistoryMonthly).where(SalesHistoryMonthly.period.in_(periods))
                )
            )
            .scalars()
            .all()
        )
        existing_by_key = {
            (
                row.period,
                row.oem_number,
                str(row.brand_name or ""),
            ): row
            for row in existing_rows
        }

    created = 0
    updated = 0
    for (period, oem, brand), values in aggregated.items():
        existing = existing_by_key.get((period, oem, brand))
        if existing is None:
            session.add(
                SalesHistoryMonthly(
                    period=period,
                    oem_number=oem,
                    brand_name=brand or None,
                    quantity=int(values["quantity"]),
                    revenue=values["revenue"],
                )
            )
            created += 1
        else:
            existing.quantity = int(values["quantity"])
            existing.revenue = values["revenue"]
            session.add(existing)
            updated += 1
    await session.commit()
    return {
        "rows_in_file": rows_in_file,
        "created": created,
        "updated": updated,
        "skipped": skipped,
    }


async def get_sales_history_summary(
    session: AsyncSession,
) -> dict[str, Any]:
    from sqlalchemy import func as sa_func

    from dz_fastapi.models.partner import SalesHistoryMonthly

    row = (
        await session.execute(
            select(
                sa_func.count(),
                sa_func.min(SalesHistoryMonthly.period),
                sa_func.max(SalesHistoryMonthly.period),
                sa_func.count(sa_func.distinct(SalesHistoryMonthly.oem_number)),
                sa_func.coalesce(sa_func.sum(SalesHistoryMonthly.quantity), 0),
            )
        )
    ).one()
    total_rows, period_from, period_to, unique_oems, total_qty = row
    return {
        "rows": int(total_rows or 0),
        "period_from": period_from,
        "period_to": period_to,
        "unique_oems": int(unique_oems or 0),
        "total_quantity": int(total_qty or 0),
    }
