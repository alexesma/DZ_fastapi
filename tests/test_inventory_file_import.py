import io
import zipfile
from decimal import Decimal

import pytest
from sqlalchemy import select

from dz_fastapi.models.autopart import AutoPart, StorageLocation
from dz_fastapi.models.brand import Brand
from dz_fastapi.models.inventory import (
    InventorySession,
    InventoryStatus,
    StockByLocation,
    StockLot,
    Warehouse,
)
from dz_fastapi.services.inventory_file_import import (
    NO_LOCATION_NAME,
    apply_plan,
    build_plan,
    normalize_location_name,
    parse_inventory_file,
)
from dz_fastapi.services.inventory_stock import reconcile_stock_absolute

HEADER = [
    "№",
    "Каталог",
    "Артикул",
    "Номенклатура",
    "Место (тек)",
    "Отклонение",
    "Количество",
    "Ед.",
    "К.",
    "Учет. количество ",
    "Цена",
    "Сумма",
    "Учет. сумма",
    "Качество",
    "Сумма (регл.)",
    "Старое место хранения",
    "Новое место хранения",
]


def make_xlsx(data_rows) -> bytes:
    """xlsx как у 1С: SharedStrings.xml с заглавной S."""
    strings: list[str] = []

    def idx(text):
        if text not in strings:
            strings.append(text)
        return strings.index(text)

    ns = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
    rows_xml = []
    for r, row in enumerate([HEADER] + data_rows, start=1):
        cells = "".join(
            f'<c r="{chr(65 + c)}{r}" t="s"><v>{idx(str(v))}</v></c>' for c, v in enumerate(row)
        )
        rows_xml.append(f'<row r="{r}">{cells}</row>')
    sheet = f'<worksheet xmlns="{ns}"><sheetData>{"".join(rows_xml)}</sheetData></worksheet>'
    shared = f'<sst xmlns="{ns}">' + "".join(f"<si><t>{s}</t></si>" for s in strings) + "</sst>"
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("xl/SharedStrings.xml", shared)
        archive.writestr("xl/worksheets/sheet1.xml", sheet)
    return buffer.getvalue()


def line(n, brand, article, name, place, qty, price):
    return [
        n,
        brand,
        article,
        name,
        place,
        0,
        qty,
        "шт",
        1,
        qty,
        price,
        0,
        0,
        "Новый",
        0,
        place,
        place,
    ]


def test_location_names_are_normalized_to_database_format():
    assert normalize_location_name("Q-MD 29/1") == "Q-MD 29/1"
    assert normalize_location_name("J-MD 35/1 Honda") == "J-MD 35/1 HONDA"
    assert normalize_location_name("арки 10") == "ARKI 10"
    assert normalize_location_name("Q-MD 12 (perfect)") == "Q-MD 12 PERFECT"
    assert normalize_location_name("  ") == NO_LOCATION_NAME
    assert len(normalize_location_name("Стеллаж Сергей Очень Длинное Название")) <= 20


def test_parse_reads_1c_xlsx_and_uses_new_location():
    content = make_xlsx(
        [
            line(1, "TOYOTA", "130413806006", "Вкладыши", "Q-MD 29/1", 5, "660"),
            line(2, "DRAGONZAP", "DZ1", "Деталь", "", 0, "10"),  # нулевое пропускается
        ]
    )
    rows = parse_inventory_file(content)
    assert len(rows) == 1
    assert (rows[0].brand, rows[0].article, rows[0].quantity) == ("TOYOTA", "130413806006", 5)
    assert rows[0].price == Decimal("660")


@pytest.mark.asyncio
async def test_apply_moves_stock_keeps_lots_creates_missing_and_deletes_old_places(test_session):
    warehouse = Warehouse(name="Основной")
    brand = Brand(name="TOYOTA")
    test_session.add_all([warehouse, brand])
    await test_session.flush()
    old_a = StorageLocation(name="A 1", warehouse_id=warehouse.id)
    old_b = StorageLocation(name="A 2", warehouse_id=warehouse.id)
    part_a = AutoPart(brand_id=brand.id, oem_number="130413806006", name="Вкладыши")
    part_b = AutoPart(brand_id=brand.id, oem_number="ZZZ999", name="Лишнее")
    test_session.add_all([old_a, old_b, part_a, part_b])
    await test_session.flush()
    await reconcile_stock_absolute(
        test_session, autopart_id=part_a.id, storage_location_id=old_a.id, target_quantity=5
    )
    await reconcile_stock_absolute(
        test_session, autopart_id=part_b.id, storage_location_id=old_b.id, target_quantity=3
    )
    await test_session.commit()

    content = make_xlsx(
        [
            line(1, "TOYOTA", "130413806006", "Вкладыши", "Q-MD 29/1", 5, "660"),
            line(2, "НОВЫЙБРЕНД", "ABC 123", "Новая деталь", "арки 1", 2, "100"),
        ]
    )
    plan = await build_plan(test_session, parse_inventory_file(content))
    assert plan.report["matched_positions"] == 1
    assert plan.report["new_autoparts"] == 1
    assert plan.report["positions_zeroed"] == 1
    assert plan.report["old_locations_to_delete"] == 2

    result = await apply_plan(test_session, plan, name="тест")

    assert result["transfers"] == 1
    assert result["autoparts_created"] == 1 and result["brands_created"] == 1
    stock = {
        (row.autopart_id, row.storage_location.name): row.quantity
        for row in (await test_session.execute(select(StockByLocation))).scalars().all()
        if row.quantity
    }
    new_part = (
        await test_session.execute(select(AutoPart).where(AutoPart.oem_number == "ABC123"))
    ).scalar_one()
    assert stock == {(part_a.id, "Q-MD 29/1"): 5, (new_part.id, "ARKI 1"): 2}
    # Перенесённая партия сохранилась (не списана и не пересоздана «из воздуха»).
    lots = (
        (await test_session.execute(select(StockLot).where(StockLot.autopart_id == part_a.id)))
        .scalars()
        .all()
    )
    assert sum(lot.remaining_quantity for lot in lots) == 5
    # Цена файла стала себестоимостью оприходованного излишка.
    new_lot = (
        await test_session.execute(select(StockLot).where(StockLot.autopart_id == new_part.id))
    ).scalar_one()
    assert new_lot.cost_price == Decimal("100")
    names = {loc.name for loc in (await test_session.execute(select(StorageLocation))).scalars()}
    assert names == {"Q-MD 29/1", "ARKI 1"}
    session_row = (await test_session.execute(select(InventorySession))).scalar_one()
    assert session_row.status == InventoryStatus.COMPLETED

    # Повторный запуск ничего не удваивает.
    plan2 = await build_plan(test_session, parse_inventory_file(content))
    again = await apply_plan(test_session, plan2, name="повтор")
    assert again.get("transfers", 0) == 0 and again.get("adjustments", 0) == 0


def test_brand_aliases_and_junk_brands():
    from dz_fastapi.services.inventory_file_import import aggregate_rows, resolve_brand_key

    assert resolve_brand_key("HYUNDAI KIA") == "HYUNDAI-KIA"
    assert resolve_brand_key("MAHLE ORIGINAL") == "MAHLE"
    assert resolve_brand_key("OJD (QUICK BRAKE)") == "QUICK BRAKE"
    assert resolve_brand_key("TOYOTA") == "TOYOTA"

    rows = parse_inventory_file(
        make_xlsx(
            [
                line(1, "1 680", "1AP1220003", "Патрон", "A 1", 1, "10"),
                line(2, "RAF FILTER", "RST002", "Фильтр", "A 2", 1, "10"),
            ]
        )
    )
    skipped: list[dict] = []
    targets = aggregate_rows(rows, skipped)
    assert [item["brand"] for item in skipped] == ["1 680"]
    assert list(targets) == [("RAF", "RST002")]
