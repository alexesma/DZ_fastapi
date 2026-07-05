from datetime import datetime
from decimal import Decimal
from io import BytesIO
from zoneinfo import ZoneInfo

import pytest
from openpyxl import load_workbook

from dz_fastapi.models.partner import (
    CustomerOrder,
    CustomerOrderItem,
    PriceList,
    PriceListAutoPartAssociation,
    Provider,
    ProviderPriceListConfig,
)
from dz_fastapi.services.autopurchase_top import (
    build_customer_order_period_report_data,
    build_customer_order_period_report_xlsx,
)


@pytest.mark.asyncio
async def test_customer_order_period_report_uses_requested_qty_and_stock(
    test_session,
    created_autopart,
    created_customers,
):
    provider = Provider(
        name="Own price",
        email_contact="own@example.com",
        email_incoming_price="own-price@example.com",
        type_prices="Retail",
        is_own_price=True,
    )
    test_session.add(provider)
    await test_session.flush()
    config = ProviderPriceListConfig(
        provider_id=provider.id,
        start_row=1,
        oem_col=0,
        brand_col=1,
        name_col=2,
        qty_col=3,
        price_col=4,
        use_for_order_insights=True,
    )
    test_session.add(config)
    await test_session.flush()
    pricelist = PriceList(
        date=datetime(2026, 7, 1).date(),
        provider_id=provider.id,
        provider_config_id=config.id,
        is_active=True,
    )
    test_session.add(pricelist)
    await test_session.flush()
    test_session.add(
        PriceListAutoPartAssociation(
            pricelist_id=pricelist.id,
            autopart_id=created_autopart.id,
            quantity=12,
            price=Decimal("100.00"),
            multiplicity=1,
        )
    )

    tz = ZoneInfo("Europe/Moscow")
    order_period_1 = CustomerOrder(
        customer_id=created_customers[0].id,
        received_at=datetime(2025, 6, 10, 12, 0, tzinfo=tz),
    )
    order_period_2 = CustomerOrder(
        customer_id=created_customers[0].id,
        received_at=datetime(2026, 2, 3, 12, 0, tzinfo=tz),
    )
    test_session.add_all([order_period_1, order_period_2])
    await test_session.flush()
    test_session.add_all(
        [
            CustomerOrderItem(
                order_id=order_period_1.id,
                oem=created_autopart.oem_number,
                brand=created_autopart.brand.name,
                name=created_autopart.name,
                requested_qty=2,
                requested_price=Decimal("100.00"),
            ),
            CustomerOrderItem(
                order_id=order_period_1.id,
                oem=created_autopart.oem_number,
                brand=created_autopart.brand.name,
                name=created_autopart.name,
                requested_qty=3,
                requested_price=Decimal("116.6667"),
            ),
            CustomerOrderItem(
                order_id=order_period_2.id,
                oem=created_autopart.oem_number,
                brand=created_autopart.brand.name,
                name=created_autopart.name,
                requested_qty=7,
                requested_price=Decimal("140.00"),
            ),
        ]
    )
    await test_session.commit()

    report_data = await build_customer_order_period_report_data(
        test_session,
        period1_from=datetime(2025, 6, 1).date(),
        period1_to=datetime(2025, 12, 31).date(),
        period2_from=datetime(2026, 1, 1).date(),
        period2_to=datetime(2026, 7, 1).date(),
    )
    assert report_data["summary"]["period1_qty"] == 5
    assert report_data["summary"]["period2_qty"] == 7
    assert report_data["summary"]["total_qty"] == 12
    assert report_data["summary"]["stock_qty"] == 12
    assert report_data["total_items"] == 1
    assert report_data["rows"][0]["oem_number"] == created_autopart.oem_number
    assert report_data["rows"][0]["current_quantity"] == 12
    assert report_data["rows"][0]["period1_qty"] == 5
    assert report_data["rows"][0]["period2_qty"] == 7
    assert report_data["rows"][0]["total_qty"] == 12
    assert report_data["rows"][0]["period1_avg_price"] == pytest.approx(
        110.0,
        abs=0.01,
    )

    content = await build_customer_order_period_report_xlsx(
        test_session,
        period1_from=datetime(2025, 6, 1).date(),
        period1_to=datetime(2025, 12, 31).date(),
        period2_from=datetime(2026, 1, 1).date(),
        period2_to=datetime(2026, 7, 1).date(),
    )

    workbook = load_workbook(BytesIO(content), data_only=True)
    sheet = workbook["TDSheet"]

    assert sheet["A1"].value == "Заказы клиентов по периодам с остатками"
    assert "Период 1: Июнь 2025 г. - Декабрь 2025 г." in sheet["A2"].value
    assert sheet["A4"].value == "Артикул"
    assert sheet["E4"].value == "Кол-во заказанных клиентами позиций за период 1"
    assert sheet["A5"].value == created_autopart.oem_number
    assert sheet["D5"].value == 12
    assert sheet["E5"].value == 5
    assert sheet["F5"].value == 7
    assert sheet["G5"].value == pytest.approx(110.0, abs=0.01)
