"""УПД реализации в формате Excel — для отправки клиенту на почту.

Данные те же, что в печатной форме УПД (``shipment_print_documents``):
шапка счёта-фактуры, таблица товаров с НДС, итоги и блок передачи.
"""

from __future__ import annotations

from io import BytesIO
from typing import Any

from openpyxl import Workbook
from openpyxl.styles import Alignment, Border, Font, Side
from openpyxl.utils import get_column_letter
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.models.inventory import ShipmentDocument
from dz_fastapi.services.shipment_print_documents import _base_context, build_upd_rows

_THIN = Side(style="thin", color="000000")
_BORDER = Border(left=_THIN, right=_THIN, top=_THIN, bottom=_THIN)
_BOLD = Font(bold=True)
_SMALL = Font(size=8)
_WRAP = Alignment(wrap_text=True, vertical="top")
_CENTER = Alignment(wrap_text=True, vertical="center", horizontal="center")
_RIGHT = Alignment(vertical="top", horizontal="right")

# (заголовок, номер графы УПД, ключ строки, ширина, числовая ли графа)
_COLUMNS = [
    ("№ п/п", "А", "n", 6, False),
    ("Код товара", "Б", "code", 16, False),
    ("Наименование товара", "1", "title", 46, False),
    ("Код ед.", "2", "unit_code", 7, False),
    ("Ед. изм.", "2а", "unit_name", 7, False),
    ("Количество", "3", "qty", 11, True),
    ("Цена без НДС", "4", "price", 13, True),
    ("Стоимость без НДС", "5", "net", 15, True),
    ("Ставка НДС", "7", "vat_rate", 9, False),
    ("Сумма НДС", "8", "vat", 13, True),
    ("Стоимость с НДС", "9", "gross", 15, True),
    ("Код страны", "10", "country_code", 8, False),
    ("Страна происхождения", "10а", "country_name", 16, False),
    ("Номер таможенной декларации", "11", "gtd", 24, False),
]
_LAST = len(_COLUMNS)


def _number(value: Any) -> Any:
    """Сумма числом, чтобы в Excel её можно было складывать."""
    text = str(value or "").replace(" ", "").replace(" ", "").replace(",", ".")
    try:
        return float(text)
    except ValueError:
        return value


def _inn_kpp(party: dict[str, Any]) -> str:
    return "/".join(part for part in (party.get("inn"), party.get("kpp")) if part)


def _with_address(party: dict[str, Any]) -> str:
    return ", ".join(
        part for part in (party.get("name"), party.get("postal_address")) if part
    )


def build_upd_xlsx(context: dict[str, Any], rows: list[dict], totals: dict) -> bytes:
    seller, buyer = context["seller"], context["buyer"]
    book = Workbook()
    sheet = book.active
    sheet.title = "УПД"
    sheet.page_setup.orientation = "landscape"
    sheet.page_setup.paperSize = sheet.PAPERSIZE_A4
    sheet.page_setup.fitToWidth = 1
    sheet.page_setup.fitToHeight = 0
    sheet.sheet_properties.pageSetUpPr.fitToPage = True
    for index, (_title, _graph, _key, width, _numeric) in enumerate(_COLUMNS, start=1):
        sheet.column_dimensions[get_column_letter(index)].width = width

    line = 1

    def wide(text: str, *, bold: bool = False, small: bool = False) -> None:
        nonlocal line
        sheet.merge_cells(start_row=line, start_column=1, end_row=line, end_column=_LAST)
        cell = sheet.cell(row=line, column=1, value=text)
        cell.alignment = _WRAP
        if bold:
            cell.font = _BOLD
        if small:
            cell.font = _SMALL
        line += 1

    def pair(label: str, value: Any, mark: str = "") -> None:
        nonlocal line
        sheet.merge_cells(start_row=line, start_column=1, end_row=line, end_column=3)
        sheet.merge_cells(start_row=line, start_column=4, end_row=line, end_column=_LAST - 1)
        sheet.cell(row=line, column=1, value=label).font = _BOLD
        sheet.cell(row=line, column=4, value=value or context["empty"]).alignment = _WRAP
        sheet.cell(row=line, column=_LAST, value=mark).font = _SMALL
        line += 1

    wide("Универсальный передаточный документ", bold=True)
    wide(
        "Статус: 1 — счёт-фактура и передаточный документ (акт)",
        small=True,
    )
    wide(
        f"Счёт-фактура № {context['doc_number']} от {context['doc_date']}",
        bold=True,
    )
    line += 1
    pair("Продавец", seller.get("name"), "(2)")
    pair("Адрес", seller.get("address"), "(2а)")
    pair("ИНН/КПП продавца", _inn_kpp(seller), "(2б)")
    pair("Грузоотправитель и его адрес", _with_address(seller), "(3)")
    pair("Грузополучатель и его адрес", _with_address(buyer), "(4)")
    pair("Покупатель", buyer.get("name"), "(6)")
    pair("Адрес", buyer.get("address"), "(6а)")
    pair("ИНН/КПП покупателя", _inn_kpp(buyer), "(6б)")
    pair("Валюта", "Российский рубль, 643", "(7)")
    line += 1

    header_row = line
    for index, (title, graph, _key, _width, _numeric) in enumerate(_COLUMNS, start=1):
        head = sheet.cell(row=line, column=index, value=title)
        head.font, head.alignment, head.border = _BOLD, _CENTER, _BORDER
        number = sheet.cell(row=line + 1, column=index, value=graph)
        number.font, number.alignment, number.border = _SMALL, _CENTER, _BORDER
    sheet.row_dimensions[header_row].height = 42
    line += 2

    for row in rows:
        for index, (_title, _graph, key, _width, numeric) in enumerate(_COLUMNS, start=1):
            value = row.get(key)
            cell = sheet.cell(
                row=line, column=index, value=_number(value) if numeric else value
            )
            cell.border = _BORDER
            cell.alignment = _RIGHT if numeric else _WRAP
            if numeric and key != "qty":
                cell.number_format = "#,##0.00"
        line += 1

    sheet.merge_cells(start_row=line, start_column=1, end_row=line, end_column=7)
    total = sheet.cell(row=line, column=1, value="Всего к оплате")
    total.font = _BOLD
    for index, (_title, _graph, key, _width, _numeric) in enumerate(_COLUMNS, start=1):
        cell = sheet.cell(row=line, column=index)
        cell.border = _BORDER
        if key in ("net", "vat", "gross"):
            cell.value = _number(totals[key])
            cell.number_format = "#,##0.00"
            cell.font = _BOLD
            cell.alignment = _RIGHT
    line += 2

    signer = " ".join(
        part for part in (seller.get("signer_position"), seller.get("signer_name")) if part
    )
    wide(f"Руководитель организации или иное уполномоченное лицо: {signer} ____________")
    if seller.get("accountant_name"):
        wide(f"Главный бухгалтер: {seller['accountant_name']} ____________")
    line += 1
    wide(
        f"Товар передал: {signer} ____________        "
        "Товар получил: ________________________ ____________"
    )
    wide(
        "Дата отгрузки, передачи: "
        f"{context['doc_date']}        Дата получения: «___» ____________ 20__ г."
    )
    wide(
        "Наименование экономического субъекта — составителя документа: "
        + ", ".join(
            part
            for part in (seller.get("name"), f"ИНН {seller['inn']}" if seller.get("inn") else "")
            if part
        ),
        small=True,
    )
    wide(
        "Наименование экономического субъекта — получателя: "
        + ", ".join(
            part
            for part in (buyer.get("name"), f"ИНН {buyer['inn']}" if buyer.get("inn") else "")
            if part
        ),
        small=True,
    )
    wide(f"Документ сформирован {context['generated_at']}", small=True)

    stream = BytesIO()
    book.save(stream)
    return stream.getvalue()


async def render_upd_xlsx(session: AsyncSession, doc: ShipmentDocument) -> bytes:
    context = await _base_context(session, doc)
    rows, totals = build_upd_rows(doc)
    return build_upd_xlsx(context, rows, totals)
