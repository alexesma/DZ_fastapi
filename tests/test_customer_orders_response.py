from io import BytesIO
from types import SimpleNamespace

import pandas as pd
from openpyxl import Workbook, load_workbook

from dz_fastapi.models.partner import CUSTOMER_ORDER_SHIP_MODE
from dz_fastapi.services.customer_orders import (
    _apply_response_updates_csv, _apply_response_updates_excel,
    _build_order_reply_recipients, _customer_order_auto_reply_enabled,
    _customer_order_reply_override_email)


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


def test_customer_order_auto_reply_enabled_with_default_stub(monkeypatch):
    monkeypatch.delenv('CUSTOMER_ORDER_AUTO_REPLY_ENABLED', raising=False)
    monkeypatch.delenv('CUSTOMER_ORDER_REPLY_OVERRIDE_EMAIL', raising=False)
    assert _customer_order_reply_override_email() == 'info@dragonzap.ru'
    assert _customer_order_auto_reply_enabled() is True


def test_customer_order_auto_reply_can_be_fully_disabled(monkeypatch):
    monkeypatch.setenv('CUSTOMER_ORDER_AUTO_REPLY_ENABLED', '0')
    monkeypatch.setenv('CUSTOMER_ORDER_REPLY_OVERRIDE_EMAIL', '')
    assert _customer_order_reply_override_email() is None
    assert _customer_order_auto_reply_enabled() is False


def test_build_order_reply_recipients_uses_stub_override(monkeypatch):
    monkeypatch.setenv(
        'CUSTOMER_ORDER_REPLY_OVERRIDE_EMAIL', 'info@dragonzap.ru'
    )
    config = SimpleNamespace(
        order_reply_emails=['client@example.com', 'sales@example.com']
    )
    recipients = _build_order_reply_recipients(
        'sender@example.com',
        config,
    )
    original = _build_order_reply_recipients(
        'sender@example.com',
        config,
        use_override=False,
    )

    assert recipients == 'info@dragonzap.ru'
    assert original == (
        'client@example.com,sales@example.com,sender@example.com'
    )
