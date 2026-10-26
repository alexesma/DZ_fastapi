from decimal import Decimal

import pytest
from sqlalchemy import select

from dz_fastapi.core.time import now_moscow
from dz_fastapi.models.inventory import (
    CrossDockingLabel,
    InventoryItem,
    InventoryScopeType,
    InventorySession,
    StockByLocation,
)
from dz_fastapi.models.partner import (
    CustomerOrder,
    CustomerOrderItem,
    StockOrder,
    StockOrderItem,
    SupplierReceipt,
    SupplierReceiptItem,
)
from dz_fastapi.models.user import User, UserRole, UserStatus
from dz_fastapi.services.auth import get_password_hash


@pytest.mark.asyncio
async def test_own_stock_shortage_rejects_rest_and_creates_inventory(
    async_client, test_session, created_customers, created_autopart,
    created_storage,
):
    order = CustomerOrder(
        customer_id=created_customers[0].id, status="PROCESSED",
        order_number="SHORT-1",
    )
    test_session.add(order)
    await test_session.flush()
    item = CustomerOrderItem(
        order_id=order.id, row_index=1, oem=created_autopart.oem_number,
        brand="TEST", requested_qty=3, requested_price=Decimal("100.00"),
        ship_qty=3, reject_qty=0, status="OWN_STOCK",
        autopart_id=created_autopart.id,
    )
    stock_order = StockOrder(customer_id=created_customers[0].id)
    test_session.add_all([item, stock_order])
    await test_session.flush()
    stock_item = StockOrderItem(
        stock_order_id=stock_order.id, customer_order_item_id=item.id,
        autopart_id=created_autopart.id, quantity=3,
    )
    test_session.add_all([
        stock_item,
        StockByLocation(autopart_id=created_autopart.id,
                        storage_location_id=created_storage.id, quantity=3),
    ])
    await test_session.commit()
    storage_name = created_storage.name
    autopart_id = created_autopart.id
    item_id, stock_item_id, order_id = item.id, stock_item.id, order.id

    too_many = await async_client.post(
        f"/customer-orders/assembly/own-stock/{stock_item_id}/shortage",
        json={"found_quantity": 4},
    )
    assert too_many.status_code == 409

    done = await async_client.post(
        f"/customer-orders/assembly/own-stock/{stock_item_id}/shortage",
        json={"found_quantity": 1, "comment": "пустая ячейка"},
    )
    assert done.status_code == 200, done.text
    data = done.json()
    assert data["rejected_quantity"] == 2
    assert data["found"] == 1
    assert data["location"] == storage_name

    test_session.expire_all()
    refreshed = await test_session.get(CustomerOrderItem, item_id)
    assert (refreshed.ship_qty, refreshed.requested_qty) == (1, 1)
    refreshed_stock = await test_session.get(StockOrderItem, stock_item_id)
    assert refreshed_stock.quantity == 1
    rejected = (await test_session.execute(
        select(CustomerOrderItem).where(
            CustomerOrderItem.order_id == order_id,
            CustomerOrderItem.status == "REJECTED",
        )
    )).scalars().all()
    assert [(row.reject_qty, row.ship_qty, row.row_index, row.reject_reason_code)
            for row in rejected] == [(2, 0, 1, "OWN_STOCK_SHORTAGE")]

    session_row = await test_session.get(
        InventorySession, data["inventory_session_id"]
    )
    assert session_row.scope_type == InventoryScopeType.LOCATION
    assert session_row.scope_value == storage_name
    inventory_items = (await test_session.execute(
        select(InventoryItem).where(InventoryItem.session_id == session_row.id)
    )).scalars().all()
    assert [row.autopart_id for row in inventory_items] == [autopart_id]

    # повторно: недостачи уже нет, количество совпадает с найденным
    again = await async_client.post(
        f"/customer-orders/assembly/own-stock/{stock_item_id}/shortage",
        json={"found_quantity": 1},
    )
    assert again.status_code == 409


@pytest.mark.asyncio
async def test_printing_supplier_label_accepts_the_received_quantity(
    async_client, test_session, created_customers, created_providers,
    created_autopart,
):
    user = User(
        email="accept-label@example.com",
        password_hash=get_password_hash("secret123"),
        role=UserRole.MANAGER, status=UserStatus.ACTIVE,
    )
    order = CustomerOrder(
        customer_id=created_customers[0].id, status="PROCESSED",
        order_number="ACCEPT-1",
    )
    test_session.add_all([user, order])
    await test_session.flush()
    item = CustomerOrderItem(
        order_id=order.id, oem=created_autopart.oem_number, brand="TEST",
        requested_qty=2, ship_qty=2, status="SUPPLIER",
        autopart_id=created_autopart.id, supplier_id=created_providers[0].id,
    )
    receipt = SupplierReceipt(provider_id=created_providers[0].id,
                              posted_at=now_moscow())
    stock_order = StockOrder(customer_id=created_customers[0].id)
    test_session.add_all([item, receipt, stock_order])
    await test_session.flush()
    receipt_item = SupplierReceiptItem(
        receipt_id=receipt.id, customer_order_item_id=item.id,
        autopart_id=created_autopart.id, received_quantity=2,
    )
    test_session.add(receipt_item)
    await test_session.flush()
    stock_item = StockOrderItem(
        stock_order_id=stock_order.id, customer_order_item_id=item.id,
        supplier_receipt_item_id=receipt_item.id,
        autopart_id=created_autopart.id, quantity=2,
    )
    label = CrossDockingLabel(
        supplier_receipt_item_id=receipt_item.id,
        customer_order_item_id=item.id, quantity=2,
        requested_brand=item.brand, requested_oem=item.oem,
        barcode="ACCEPT-LABEL-1",
    )
    test_session.add_all([stock_item, label])
    await test_session.commit()
    stock_item_id, label_id, receipt_id = stock_item.id, label.id, receipt.id
    user_email = user.email
    assert stock_item.picked_quantity == 0
    login = await async_client.post(
        "/auth/login", json={"email": user_email, "password": "secret123"}
    )
    assert login.status_code == 200

    printed = await async_client.post(
        f"/customer-orders/supplier-receipts/{receipt_id}/cross-docking-labels/print",
        json={"label_ids": [label_id]},
    )
    assert printed.status_code == 200, printed.text

    test_session.expire_all()
    assert (await test_session.get(StockOrderItem, stock_item_id)).picked_quantity == 2
    listed = await async_client.get("/customer-orders/assembly")
    row = next(row for row in listed.json() if row["number"] == "ACCEPT-1")
    assert "accept_receipt" not in row["actions"]
