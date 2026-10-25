"""Work queue pagination, realization preview and atomic receipt picking."""

from datetime import timedelta

import pytest

from dz_fastapi.core.time import now_moscow
from dz_fastapi.models.inventory import ShipmentDocument, ShipmentDocumentItem
from dz_fastapi.models.partner import (
    CustomerOrder,
    CustomerOrderItem,
    StockOrder,
    StockOrderItem,
    SupplierReceipt,
    SupplierReceiptItem,
)
from dz_fastapi.models.user import User, UserRole, UserStatus


@pytest.mark.asyncio
async def test_old_open_orders_remain_in_paginated_work_queue(
    async_client, test_session, created_customers, created_autopart,
):
    orders = []
    for number in range(3):
        order = CustomerOrder(
            customer_id=created_customers[0].id,
            order_number=f"OLD-{number}", status="PROCESSED",
            received_at=now_moscow() - timedelta(days=20),
        )
        test_session.add(order)
        await test_session.flush()
        item = CustomerOrderItem(
            order_id=order.id, oem=created_autopart.oem_number,
            brand="TEST", requested_qty=2, ship_qty=2,
            status="SUPPLIER", autopart_id=created_autopart.id,
        )
        test_session.add(item)
        await test_session.flush()
        orders.append((order, item))
    completed_doc = ShipmentDocument(customer_id=created_customers[0].id)
    test_session.add(completed_doc)
    await test_session.flush()
    test_session.add(ShipmentDocumentItem(
        document_id=completed_doc.id, autopart_id=created_autopart.id,
        customer_order_item_id=orders[0][1].id, quantity=2,
    ))
    await test_session.commit()

    first = await async_client.get("/customer-orders/assembly/page", params={
        "view": "work", "page": 1, "page_size": 1,
        "customer_group": "wholesale",
    })
    assert first.status_code == 200, first.text
    assert first.json()["total"] == 2
    assert len(first.json()["items"]) == 1
    second = await async_client.get("/customer-orders/assembly/page", params={
        "view": "work", "page": 2, "page_size": 1,
        "customer_group": "wholesale",
    })
    assert second.status_code == 200, second.text
    ids = {first.json()["items"][0]["id"], second.json()["items"][0]["id"]}
    assert ids == {orders[1][0].id, orders[2][0].id}

    summary = await async_client.get("/customer-orders/summary-page", params={
        "customer_group": "wholesale",
    })
    assert summary.status_code == 200, summary.text
    pending_summary = next(row for row in summary.json()["items"]
                           if row["id"] == orders[1][0].id)
    assert pending_summary["fulfillment"]["ordered"] == 2
    assert "await_supplier" in pending_summary["next_actions"]

    dated = await async_client.get("/customer-orders/assembly/page", params={
        "view": "dates", "date_from": now_moscow().date().isoformat(),
        "date_to": now_moscow().date().isoformat(),
    })
    assert dated.status_code == 200, dated.text
    assert dated.json()["total"] == 0

    preview = await async_client.post(
        "/customer-orders/assembly/realizations/preview",
        json={"order_ids": [orders[1][0].id]},
    )
    assert preview.status_code == 200, preview.text
    assert preview.json()["ready_total"] == 0
    assert preview.json()["remaining_total"] == 2


@pytest.mark.asyncio
async def test_bulk_pick_rejects_invalid_row_without_partial_update(
    async_client, test_session, created_customers, created_providers,
    created_autopart,
):
    test_session.add(User(
        id=1, email="bulk-pick@example.com", password_hash="unused",
        role=UserRole.ADMIN, status=UserStatus.ACTIVE,
    ))
    receipt = SupplierReceipt(provider_id=created_providers[0].id,
                              posted_at=now_moscow())
    stock_order = StockOrder(customer_id=created_customers[0].id)
    test_session.add_all([receipt, stock_order])
    await test_session.flush()
    rows = []
    for _ in range(2):
        receipt_item = SupplierReceiptItem(
            receipt_id=receipt.id, autopart_id=created_autopart.id,
            received_quantity=2,
        )
        test_session.add(receipt_item)
        await test_session.flush()
        row = StockOrderItem(
            stock_order_id=stock_order.id,
            supplier_receipt_item_id=receipt_item.id,
            autopart_id=created_autopart.id, quantity=2,
        )
        test_session.add(row)
        await test_session.flush()
        rows.append(row)
    await test_session.commit()

    invalid = await async_client.post(
        "/customer-orders/assembly/cross-picks/bulk",
        json={"items": [
            {"id": rows[0].id, "picked_quantity": 2},
            {"id": rows[1].id, "picked_quantity": 3},
        ]},
    )
    assert invalid.status_code == 409, invalid.text
    for row in rows:
        await test_session.refresh(row)
        assert row.picked_quantity == 0

    valid = await async_client.post(
        "/customer-orders/assembly/cross-picks/bulk",
        json={"items": [
            {"id": row.id, "picked_quantity": 2} for row in rows
        ]},
    )
    assert valid.status_code == 200, valid.text
    for row in rows:
        await test_session.refresh(row)
        assert row.picked_quantity == 2
    await test_session.refresh(stock_order)
    assert stock_order.status.value == "COMPLETED"
