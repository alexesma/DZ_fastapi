from io import BytesIO
from types import SimpleNamespace

import pandas as pd
from openpyxl import Workbook, load_workbook

from dz_fastapi.models.partner import CUSTOMER_ORDER_SHIP_MODE
from dz_fastapi.services.customer_orders import (_apply_response_updates_csv,
                                                 _apply_response_updates_excel)


def test_apply_response_updates_excel_writes_ship_price_when_configured():
    wb = Workbook()
    ws = wb.active
    ws.cell(row=2, column=2).value = 5
    ws.cell(row=2, column=4).value = None

    source = BytesIO()
    wb.save(source)
    source.seek(0)

    config = SimpleNamespace(
        qty_col=1,
        ship_qty_col=None,
        ship_price_col=3,
        reject_qty_col=None,
        ship_mode=CUSTOMER_ORDER_SHIP_MODE.REPLACE_QTY,
    )
    item = SimpleNamespace(
        row_index=2,
        ship_qty=3,
        reject_qty=0,
        requested_price=3131.0,
    )

    output = _apply_response_updates_excel(source, config, [item])
    result = load_workbook(output)
    result_ws = result.active

    assert result_ws.cell(row=2, column=2).value == 3
    assert result_ws.cell(row=2, column=4).value == 3131.0


def test_apply_response_updates_csv_leaves_ship_price_blank_for_reject():
    df = pd.DataFrame(
        [
            ['OEM1', 2, '', ''],
            ['OEM2', 1, '', ''],
        ]
    )
    source = BytesIO()
    df.to_csv(source, index=False, header=False)
    source.seek(0)

    config = SimpleNamespace(
        qty_col=1,
        ship_qty_col=2,
        ship_price_col=3,
        reject_qty_col=None,
        ship_mode=CUSTOMER_ORDER_SHIP_MODE.WRITE_SHIP_QTY,
    )
    item = SimpleNamespace(
        row_index=1,
        ship_qty=0,
        reject_qty=1,
        requested_price=2500.0,
    )

    output = _apply_response_updates_csv(source, config, [item])
    result = pd.read_csv(output, header=None, keep_default_na=False)

    assert float(result.iat[1, 2]) == 0.0
    assert result.iat[1, 3] == ''
