"""Customer order screen: system stock, supplier price-list qty, ЧЗ category."""

import pytest

from dz_fastapi.api.customer_order import _item_reference_info
from dz_fastapi.models.autopart import StorageLocation
from dz_fastapi.models.inventory import ReserveStatus, StockByLocation, StockReserve, Warehouse
from dz_fastapi.models.nomenclature import HonestSignCategory
from dz_fastapi.models.partner import CUSTOMER_ORDER_ITEM_STATUS, CustomerOrder, CustomerOrderItem


@pytest.mark.asyncio
async def test_reference_info_uses_system_stock_minus_reserves(
    test_session, created_autopart, created_customers, created_storage,
):
    main = Warehouse(name="Основной склад")
    other = Warehouse(name="Склад Заказа")
    test_session.add_all([main, other])
    await test_session.flush()
    created_storage.warehouse_id = main.id
    order_location = StorageLocation(name="WH2 RECEIVING", warehouse_id=other.id)
    test_session.add(order_location)
    await test_session.flush()
    category = HonestSignCategory(name="Шины")
    test_session.add(category)
    await test_session.flush()
    created_autopart.honest_sign_categories = [category]
    order = CustomerOrder(customer_id=created_customers[0].id, status="PROCESSED")
    test_session.add(order)
    await test_session.flush()
    item = CustomerOrderItem(
        order_id=order.id, oem="X1", brand="B", requested_qty=3,
        status=CUSTOMER_ORDER_ITEM_STATUS.NEW,
        autopart_id=created_autopart.id,
    )
    test_session.add_all([
        item,
        StockByLocation(
            autopart_id=created_autopart.id,
            storage_location_id=created_storage.id, quantity=5,
        ),
        StockByLocation(
            autopart_id=created_autopart.id,
            storage_location_id=order_location.id, quantity=100,
        ),
        StockReserve(
            autopart_id=created_autopart.id,
            storage_location_id=created_storage.id, quantity=2,
            status=ReserveStatus.ACTIVE,
        ),
    ])
    await test_session.flush()

    info = (await _item_reference_info(test_session, [item]))[item.id]

    assert info["stock_physical"] == 5
    assert info["stock_free"] == 3
    assert info["supplier_stock_qty"] is None
    assert info["honest_sign_category"] == "Шины"
