"""Инвентаризация из файла старой 1С: остатки и места хранения.

Файл (xlsx «Инвентаризация» из 1С) — источник правды по физическому остатку:
по каждой позиции известны количество, цена и место хранения. Загрузка:

* сопоставляет бренд и артикул с номенклатурой (недостающее создаёт);
* приводит названия мест к нашему формату (заглавные латинские, цифры,
  пробел, «/», «-»; кириллица транслитерируется);
* для каждой позиции приводит остаток к файлу: сначала ПЕРЕМЕЩАЕТ товар между
  местами с сохранением партий и ГТД, затем излишек оприходует по цене файла,
  недостачу списывает по FIFO;
* позиции, которых в файле нет, обнуляет (полная инвентаризация);
* удаляет места, которых в файле нет и где нет остатка.

Операция идемпотентна: цели абсолютные, повторный запуск ничего не удвоит.
"""

from __future__ import annotations

import logging
import re
import xml.etree.ElementTree as ET
import zipfile
from collections import defaultdict
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from typing import Any, Optional

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.api.validators import normalize_brand_name
from dz_fastapi.core.time import now_moscow
from dz_fastapi.crud.brand import brand_crud
from dz_fastapi.models.autopart import (
    AutoPart,
    StorageLocation,
    autopart_storage_association,
    preprocess_oem_number,
)
from dz_fastapi.models.brand import Brand
from dz_fastapi.models.inventory import (
    InventoryItem,
    InventoryScopeType,
    InventorySession,
    InventoryStatus,
    StockByLocation,
    Warehouse,
)
from dz_fastapi.services.inventory_stock import (
    reconcile_stock_absolute,
    transfer_stock_with_lot_trace,
)

logger = logging.getLogger("dz_fastapi")

NO_LOCATION_NAME = "BEZ MESTA"
LOCATION_MAX = 20

_TRANSLIT = {
    "а": "A",
    "б": "B",
    "в": "V",
    "г": "G",
    "д": "D",
    "е": "E",
    "ё": "E",
    "ж": "ZH",
    "з": "Z",
    "и": "I",
    "й": "Y",
    "к": "K",
    "л": "L",
    "м": "M",
    "н": "N",
    "о": "O",
    "п": "P",
    "р": "R",
    "с": "S",
    "т": "T",
    "у": "U",
    "ф": "F",
    "х": "H",
    "ц": "C",
    "ч": "CH",
    "ш": "SH",
    "щ": "SH",
    "ъ": "",
    "ы": "Y",
    "ь": "",
    "э": "E",
    "ю": "YU",
    "я": "YA",
}


def normalize_location_name(raw: Optional[str]) -> str:
    """Название места в формате базы: ``^[A-Z0-9 /-]+$``, не длиннее 20."""
    text = "".join(_TRANSLIT.get(ch.lower(), ch) for ch in (raw or "").strip())
    text = re.sub(r"[^A-Za-z0-9 /\-]", " ", text).upper()
    text = re.sub(r"\s+", " ", text).strip()
    return text[:LOCATION_MAX].strip() or NO_LOCATION_NAME


@dataclass
class FileRow:
    brand: str
    article: str
    name: str
    location: str
    quantity: int
    price: Optional[Decimal]


# ── Разбор xlsx (1С кладёт SharedStrings с заглавной S, openpyxl такое не читает)


def _ns(tag: str) -> str:
    return "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}" + tag


def _col_index(ref: str) -> int:
    letters = re.match(r"[A-Z]+", ref).group(0)
    number = 0
    for ch in letters:
        number = number * 26 + ord(ch) - 64
    return number


def read_xlsx_rows(content: bytes) -> list[list[str]]:
    import io

    archive = zipfile.ZipFile(io.BytesIO(content))
    names = {name.lower(): name for name in archive.namelist()}
    strings: list[str] = []
    shared = names.get("xl/sharedstrings.xml")
    if shared:
        for si in ET.fromstring(archive.read(shared)).findall(_ns("si")):
            strings.append("".join(t.text or "" for t in si.iter(_ns("t"))))
    sheet_name = names.get("xl/worksheets/sheet1.xml") or next(
        name for low, name in sorted(names.items()) if low.startswith("xl/worksheets/sheet")
    )
    sheet = ET.fromstring(archive.read(sheet_name))
    rows: list[list[str]] = []
    for row in sheet.find(_ns("sheetData")).findall(_ns("row")):
        cells: dict[int, str] = {}
        for cell in row.findall(_ns("c")):
            value = cell.find(_ns("v"))
            kind = cell.get("t")
            if kind == "s" and value is not None:
                text = strings[int(value.text)]
            elif kind == "inlineStr":
                text = "".join(t.text or "" for t in cell.iter(_ns("t")))
            else:
                text = value.text if value is not None and value.text else ""
            cells[_col_index(cell.get("r"))] = text
        width = max(cells) if cells else 0
        rows.append([cells.get(i, "") for i in range(1, width + 1)])
    return rows


def _decimal(value: str) -> Optional[Decimal]:
    try:
        return Decimal(str(value).replace(",", ".").strip())
    except (InvalidOperation, ValueError):
        return None


def parse_inventory_file(content: bytes) -> list[FileRow]:
    rows = read_xlsx_rows(content)
    if not rows:
        return []
    header = [cell.strip().lower() for cell in rows[0]]

    def col(*names: str) -> Optional[int]:
        for name in names:
            if name in header:
                return header.index(name)
        return None

    brand_i = col("каталог", "бренд")
    article_i = col("артикул")
    name_i = col("номенклатура", "наименование")
    qty_i = col("количество")
    price_i = col("цена")
    place_i = col("новое место хранения", "место (тек)", "старое место хранения")
    if None in (brand_i, article_i, qty_i):
        raise ValueError("В файле нет колонок «Каталог», «Артикул», «Количество»")
    result: list[FileRow] = []
    for row in rows[1:]:

        def cell(i: Optional[int]) -> str:
            return row[i].strip() if i is not None and i < len(row) else ""

        article = cell(article_i)
        quantity = _decimal(cell(qty_i))
        if not article or quantity is None or quantity <= 0:
            continue
        place = cell(place_i)
        if not place and place_i is not None:
            # если «новое» пусто — берём «текущее»
            alt = col("место (тек)", "старое место хранения")
            place = cell(alt)
        result.append(
            FileRow(
                brand=cell(brand_i),
                article=article,
                name=cell(name_i),
                location=place,
                quantity=int(quantity),
                price=_decimal(cell(price_i)) if price_i is not None else None,
            )
        )
    return result


# ── План ────────────────────────────────────────────────────────────────────


@dataclass
class Target:
    brand_key: str
    brand_name: str
    oem: str
    name: str
    qty_by_location: dict[str, int] = field(default_factory=dict)
    price: Optional[Decimal] = None


@dataclass
class Plan:
    targets: dict[tuple[str, str], Target]
    file_rows: int
    report: dict[str, Any] = field(default_factory=dict)
    autopart_ids: dict[tuple[str, str], int] = field(default_factory=dict)
    brand_ids: dict[str, int] = field(default_factory=dict)
    location_ids: dict[str, int] = field(default_factory=dict)
    stock: dict[int, dict[int, int]] = field(default_factory=dict)  # ap -> loc -> qty


def aggregate_rows(rows: list[FileRow]) -> dict[tuple[str, str], Target]:
    targets: dict[tuple[str, str], Target] = {}
    for row in rows:
        brand_key = normalize_brand_name(row.brand) or row.brand.strip().upper()
        oem = preprocess_oem_number(row.article)
        if not oem:
            continue
        target = targets.setdefault(
            (brand_key, oem),
            Target(brand_key=brand_key, brand_name=row.brand.strip(), oem=oem, name=row.name),
        )
        place = normalize_location_name(row.location)
        target.qty_by_location[place] = target.qty_by_location.get(place, 0) + row.quantity
        if row.price is not None:
            target.price = row.price
    return targets


async def build_plan(session: AsyncSession, rows: list[FileRow]) -> Plan:
    targets = aggregate_rows(rows)
    plan = Plan(targets=targets, file_rows=len(rows))

    brands = await brand_crud.get_multi_with_synonyms(session)
    canonical = brand_crud.build_canonical_brand_map(brands)
    brand_id_by_key: dict[str, int] = {}
    for brand in brands:
        main = canonical.get(brand.id, brand)
        brand_id_by_key[normalize_brand_name(brand.name)] = main.id
        brand_id_by_key[normalize_brand_name(main.name)] = main.id
    plan.brand_ids = {
        key: brand_id_by_key[key]
        for key in {t.brand_key for t in targets.values()}
        if key in brand_id_by_key
    }
    missing_brands = sorted({t.brand_key for t in targets.values()} - set(plan.brand_ids))

    oems = sorted({t.oem for t in targets.values()})
    existing: dict[tuple[int, str], int] = {}
    for i in range(0, len(oems), 5000):
        end = i + 5000
        chunk = oems[i:end]
        for ap_id, brand_id, oem in (
            await session.execute(
                select(AutoPart.id, AutoPart.brand_id, AutoPart.oem_number).where(
                    AutoPart.oem_number.in_(chunk)
                )
            )
        ).all():
            existing[(brand_id, oem)] = ap_id
    for key, target in targets.items():
        brand_id = plan.brand_ids.get(target.brand_key)
        if brand_id is not None and (brand_id, target.oem) in existing:
            plan.autopart_ids[key] = existing[(brand_id, target.oem)]

    locations = {
        loc.name: loc.id for loc in (await session.execute(select(StorageLocation))).scalars().all()
    }
    plan.location_ids = locations
    stock_rows = (
        await session.execute(
            select(
                StockByLocation.autopart_id,
                StockByLocation.storage_location_id,
                StockByLocation.quantity,
            ).where(StockByLocation.quantity > 0)
        )
    ).all()
    for ap_id, loc_id, qty in stock_rows:
        plan.stock.setdefault(ap_id, {})[loc_id] = int(qty)

    file_locations = {place for t in targets.values() for place in t.qty_by_location}
    matched_ap = set(plan.autopart_ids.values())
    file_qty = sum(q for t in targets.values() for q in t.qty_by_location.values())
    current_qty = sum(q for locs in plan.stock.values() for q in locs.values())
    zeroed = [ap for ap in plan.stock if ap not in matched_ap]
    system_names = {
        loc.name
        for loc in (await session.execute(select(StorageLocation))).scalars().all()
        if loc.system_code
    }
    old_locations = [
        name for name in locations if name not in file_locations and name not in system_names
    ]
    report = {
        "file_rows": len(rows),
        "positions": len(targets),
        "matched_positions": len(plan.autopart_ids),
        "new_autoparts": len(targets) - len(plan.autopart_ids),
        "new_brands": missing_brands[:50],
        "new_brands_count": len(missing_brands),
        "file_quantity": file_qty,
        "current_quantity": current_qty,
        "file_locations": len(file_locations),
        "new_locations": len(file_locations - set(locations)),
        "old_locations_to_delete": len(old_locations),
        "positions_zeroed": len(zeroed),
        "quantity_zeroed": sum(sum(plan.stock[ap].values()) for ap in zeroed),
        "no_location_rows": sum(
            1 for t in targets.values() if NO_LOCATION_NAME in t.qty_by_location
        ),
    }
    plan.report = report
    return plan


# ── Применение ──────────────────────────────────────────────────────────────


async def apply_plan(
    session: AsyncSession,
    plan: Plan,
    *,
    name: str,
    progress: Optional[dict] = None,
    chunk_size: int = 100,
) -> dict[str, Any]:
    stats = defaultdict(int)
    inv = InventorySession(
        name=name,
        scope_type=InventoryScopeType.FULL,
        notes="Загружено из файла инвентаризации старой 1С",
    )
    session.add(inv)
    await session.flush()

    # Бренды, места, номенклатура
    for key in sorted({t.brand_key for t in plan.targets.values()} - set(plan.brand_ids)):
        sample = next(t for t in plan.targets.values() if t.brand_key == key)
        brand = Brand(name=(sample.brand_name or key)[:100])
        session.add(brand)
        await session.flush()
        plan.brand_ids[key] = brand.id
        stats["brands_created"] += 1
    file_locations = {place for t in plan.targets.values() for place in t.qty_by_location}
    # Новые места — на основном (первом) складе.
    warehouse_id = await session.scalar(select(Warehouse.id).order_by(Warehouse.id).limit(1))
    for place in sorted(file_locations - set(plan.location_ids)):
        location = StorageLocation(name=place, warehouse_id=warehouse_id)
        session.add(location)
        await session.flush()
        plan.location_ids[place] = location.id
        stats["locations_created"] += 1
    for key, target in plan.targets.items():
        if key in plan.autopart_ids:
            continue
        part = AutoPart(
            brand_id=plan.brand_ids[target.brand_key],
            oem_number=target.oem,
            name=(target.name or target.oem)[:250],
        )
        session.add(part)
        await session.flush()
        plan.autopart_ids[key] = part.id
        stats["autoparts_created"] += 1
    await session.commit()

    # Цели по позициям: файл + обнуление остальных
    goals: dict[int, tuple[dict[int, int], Optional[Decimal]]] = {}
    for key, target in plan.targets.items():
        goals[plan.autopart_ids[key]] = (
            {plan.location_ids[place]: qty for place, qty in target.qty_by_location.items()},
            target.price,
        )
    for ap_id in plan.stock:
        goals.setdefault(ap_id, ({}, None))

    total = len(goals)
    done = 0
    for ap_id, (target_locs, price) in goals.items():
        current = dict(plan.stock.get(ap_id, {}))
        surplus = [
            [loc, qty - target_locs.get(loc, 0)]
            for loc, qty in current.items()
            if qty > target_locs.get(loc, 0)
        ]
        deficit = [
            [loc, qty - current.get(loc, 0)]
            for loc, qty in target_locs.items()
            if qty > current.get(loc, 0)
        ]
        for dest in deficit:
            for src in surplus:
                if dest[1] <= 0:
                    break
                take = min(src[1], dest[1])
                if take <= 0:
                    continue
                await transfer_stock_with_lot_trace(
                    session,
                    autopart_id=ap_id,
                    from_location_id=src[0],
                    to_location_id=dest[0],
                    quantity=take,
                    notes=f"Инвентаризация «{name}»: перенос на новое место",
                )
                src[1] -= take
                dest[1] -= take
                stats["transfers"] += 1
        for loc in set(current) | set(target_locs):
            wanted = target_locs.get(loc, 0)
            before = current.get(loc, 0)
            movement = await reconcile_stock_absolute(
                session,
                autopart_id=ap_id,
                storage_location_id=loc,
                target_quantity=wanted,
                inventory_session_id=inv.id,
                notes=f"Инвентаризация «{name}»",
                unit_cost=price,
            )
            session.add(
                InventoryItem(
                    session_id=inv.id,
                    autopart_id=ap_id,
                    storage_location_id=loc,
                    expected_qty=before,
                    actual_qty=wanted,
                    discrepancy=wanted - before,
                    counted_at=now_moscow(),
                )
            )
            if movement is not None:
                stats["adjustments"] += 1
        done += 1
        if done % chunk_size == 0:
            await session.commit()
            if progress is not None:
                progress.update(done=done, total=total, stats=dict(stats))
    await session.commit()

    # Старые места: удаляем те, которых нет в файле и где нет остатка
    left_stock = {
        loc
        for (loc,) in (
            await session.execute(
                select(StockByLocation.storage_location_id).where(StockByLocation.quantity > 0)
            )
        ).all()
    }
    for location in (await session.execute(select(StorageLocation))).scalars().all():
        if location.name in file_locations or location.system_code or location.id in left_stock:
            continue
        try:
            # Core-удаление: у связи location.autoparts стоит cascade delete,
            # ORM-удаление могло бы задеть номенклатуру.
            async with session.begin_nested():
                await session.execute(
                    delete(autopart_storage_association).where(
                        autopart_storage_association.c.storage_location_id == location.id
                    )
                )
                await session.execute(
                    delete(StorageLocation).where(StorageLocation.id == location.id)
                )
            stats["locations_deleted"] += 1
        except Exception as exc:  # noqa: BLE001
            logger.warning("Location %s not deleted: %s", location.name, exc)
            stats["locations_kept"] += 1
    inv.status = InventoryStatus.COMPLETED
    inv.finished_at = now_moscow()
    await session.commit()
    if progress is not None:
        progress.update(done=total, total=total, stats=dict(stats), finished=True)
    return {"session_id": inv.id, **dict(stats)}


# ── Фоновое выполнение (запрос не должен держаться минутами) ────────────────

JOBS: dict[str, dict[str, Any]] = {}


async def _run_job(job_id: str, content: bytes, name: str) -> None:
    from dz_fastapi.core.db import get_async_session

    job = JOBS[job_id]
    try:
        async with get_async_session()() as session:
            plan = await build_plan(session, parse_inventory_file(content))
            job["report"] = plan.report
            job["state"] = "running"
            result = await apply_plan(session, plan, name=name, progress=job)
            job["result"] = result
            job["state"] = "done"
    except Exception as exc:  # noqa: BLE001
        logger.exception("Inventory file import failed")
        job["state"] = "error"
        job["error"] = str(exc)[:500]


def start_import_job(content: bytes, name: str) -> str:
    import asyncio
    import uuid

    job_id = uuid.uuid4().hex[:12]
    JOBS[job_id] = {"state": "starting", "done": 0, "total": 0}
    asyncio.get_running_loop().create_task(_run_job(job_id, content, name))
    return job_id
