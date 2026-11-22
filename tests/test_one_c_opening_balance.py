from decimal import Decimal

import pytest
from sqlalchemy import func, select

from dz_fastapi.models.autopart import AutoPart, StorageLocation
from dz_fastapi.models.brand import Brand
from dz_fastapi.models.inventory import StockByLocation, StockDocument, Warehouse
from dz_fastapi.services.inventory_stock import reconcile_stock_absolute
from dz_fastapi.services.one_c_opening_balance import (
    create_opening_balance_event,
    preview_opening_balance,
)


@pytest.mark.asyncio
async def test_opening_balance_event_does_not_change_stock_and_is_single(test_session):
    main = Warehouse(name="Основной склад")
    other = Warehouse(name="Склад Заказа")
    brand = Brand(name="TOYOTA")
    test_session.add_all([main, other, brand])
    await test_session.flush()
    cell = StorageLocation(name="A 1", warehouse_id=main.id)
    far = StorageLocation(name="B 1", warehouse_id=other.id)
    part = AutoPart(brand_id=brand.id, oem_number="X1", name="Деталь")
    part2 = AutoPart(brand_id=brand.id, oem_number="X2", name="Другая")
    test_session.add_all([cell, far, part, part2])
    await test_session.flush()
    await reconcile_stock_absolute(
        test_session,
        autopart_id=part.id,
        storage_location_id=cell.id,
        target_quantity=5,
        unit_cost=Decimal("100"),
    )
    await reconcile_stock_absolute(
        test_session,
        autopart_id=part2.id,
        storage_location_id=far.id,
        target_quantity=7,
        unit_cost=Decimal("10"),
    )
    await test_session.commit()

    preview = await preview_opening_balance(test_session)
    assert preview["positions"] == 1
    assert preview["quantity"] == 5
    assert preview["amount"] == "500.00"
    assert preview["existing_document_id"] is None

    before = (
        await test_session.execute(select(func.sum(StockByLocation.quantity)))
    ).scalar_one()
    from datetime import datetime

    result = await create_opening_balance_event(
        test_session, document_date=datetime(2026, 11, 21, 9, 0)
    )
    await test_session.commit()
    after = (
        await test_session.execute(select(func.sum(StockByLocation.quantity)))
    ).scalar_one()
    assert before == after
    assert result["lines"] == 1

    document = await test_session.get(StockDocument, result["document_id"])
    assert document.document_number.startswith("DZ-OPEN-")

    from dz_fastapi.services.one_c_enterprise_data_export import preview_outbox_event
    from dz_fastapi.services.one_c_outbox import build_stock_document_snapshot

    payload = await build_stock_document_snapshot(test_session, result["document_id"])
    xml = preview_outbox_event(
        "stock_document",
        "posted",
        payload,
        {
            "short_name": "ООО Тест",
            "full_name": "ООО Тест",
            "inn": "7701234567",
            "kpp": "770101001",
            "is_individual_entrepreneur": False,
        },
    )
    text = xml.decode("utf-8")
    assert "ОприходованиеТоваров" in text
    assert "<ВидОперации>ОприходованиеСобственныхТоваров</ВидОперации>" in text
    assert "<ТипЗапасов>СобственныеТовары</ТипЗапасов>" in text
    assert "</Склад><Комментарий>" not in text and "<СтавкаНДС>" not in text

    with pytest.raises(ValueError):
        await create_opening_balance_event(
            test_session, document_date=datetime(2026, 11, 21, 9, 0)
        )
