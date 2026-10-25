from datetime import timedelta
from decimal import Decimal

import pytest
from sqlalchemy import update

from dz_fastapi.core.time import now_moscow
from dz_fastapi.models.autopart import StorageLocation
from dz_fastapi.models.inventory import CrossDockingLabel, StockByLocation, StockLot
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
async def test_assembly_partial_realizations_and_box_limits(
    async_client, test_session, created_customers, created_providers, created_autopart,
    created_storage,
):
    user = User(
        email="assembly-manager@example.com",
        password_hash=get_password_hash("secret123"),
        role=UserRole.MANAGER,
        status=UserStatus.ACTIVE,
    )
    test_session.add(user)
    await test_session.flush()
    orders = []
    stock_items = []
    for number, quantity in (("ORDER-A", 3), ("ORDER-B", 2)):
        order = CustomerOrder(
            customer_id=created_customers[0].id,
            status="PROCESSED",
            order_number=number,
        )
        test_session.add(order)
        await test_session.flush()
        item = CustomerOrderItem(
            order_id=order.id,
            oem=created_autopart.oem_number,
            brand="TEST",
            requested_qty=quantity,
            requested_price=Decimal("100.00"),
            ship_qty=quantity,
            status="OWN_STOCK",
            autopart_id=created_autopart.id,
        )
        test_session.add(item)
        stock_order = StockOrder(customer_id=created_customers[0].id)
        test_session.add(stock_order)
        await test_session.flush()
        stock_item = StockOrderItem(
            stock_order_id=stock_order.id,
            customer_order_item_id=item.id,
            autopart_id=created_autopart.id,
            quantity=quantity,
        )
        test_session.add(stock_item)
        await test_session.flush()
        orders.append(order)
        stock_items.append(stock_item)
    unrelated_order = CustomerOrder(
        customer_id=created_customers[1].id,
        status="PROCESSED",
    )
    test_session.add(unrelated_order)
    await test_session.flush()
    unrelated_item = CustomerOrderItem(
        order_id=unrelated_order.id,
        oem=created_autopart.oem_number,
        brand="TEST", requested_qty=2, ship_qty=2,
        status="SUPPLIER", autopart_id=created_autopart.id,
        supplier_id=created_providers[0].id,
    )
    receipt = SupplierReceipt(provider_id=created_providers[0].id,
                              posted_at=now_moscow())
    test_session.add_all([unrelated_item, receipt])
    await test_session.flush()
    receipt_item = SupplierReceiptItem(
        receipt_id=receipt.id,
        customer_order_item_id=unrelated_item.id,
        autopart_id=created_autopart.id,
        received_quantity=2,
    )
    test_session.add(receipt_item)
    await test_session.flush()
    linked_lot = StockLot(
        autopart_id=created_autopart.id,
        storage_location_id=created_storage.id,
        source_receipt_id=receipt.id,
        source_receipt_item_id=receipt_item.id,
        initial_quantity=2,
        remaining_quantity=2,
        received_at=now_moscow() - timedelta(days=1),
    )
    test_session.add(StockByLocation(
        autopart_id=created_autopart.id,
        storage_location_id=created_storage.id,
        quantity=9,
    ))
    test_session.add(linked_lot)
    test_session.add(StockLot(
        autopart_id=created_autopart.id,
        storage_location_id=created_storage.id,
        initial_quantity=7,
        remaining_quantity=7,
    ))
    await test_session.commit()

    login = await async_client.post(
        "/auth/login", json={"email": user.email, "password": "secret123"}
    )
    assert login.status_code == 200

    labels = await async_client.post(
        "/customer-orders/assembly/own-labels/print",
        json={"stock_item_ids": [row.id for row in stock_items]},
    )
    assert labels.status_code == 200, labels.text
    assert sorted(row["quantity"] for row in labels.json()) == [2, 3]
    assert {row["location"] for row in labels.json()} == {created_storage.name}
    old_dispatch = await async_client.post(
        f"/customer-orders/stock/orders/{stock_items[0].stock_order_id}/dispatch"
    )
    assert old_dispatch.status_code == 400

    assembly = await async_client.get("/customer-orders/assembly")
    assert assembly.status_code == 200, assembly.text
    by_number = {row["number"]: row for row in assembly.json()}
    assert by_number["ORDER-A"]["counts"]["ready"] == 3
    assert by_number["ORDER-B"]["counts"]["ready"] == 2

    created = await async_client.post(
        "/customer-orders/assembly/realizations",
        json={"order_ids": [row.id for row in orders]},
    )
    assert created.status_code == 201, created.text
    shipment_id = created.json()["id"]
    shipment = await async_client.get(f"/inventory/shipments/{shipment_id}/")
    assert shipment.status_code == 200, shipment.text
    assert shipment.json()["assembly_managed"] is True
    assert shipment.json()["customer_order_id"] is None
    assert len(shipment.json()["items"]) == 2
    listed = await async_client.get(
        "/inventory/shipments/", params={"doc_number": created.json()["doc_number"]}
    )
    assert listed.status_code == 200, listed.text
    assert listed.headers["x-total-count"] == "1"
    assert listed.json()[0]["customer_order_ids"] == sorted(row.id for row in orders)
    second_order_list = await async_client.get(
        "/inventory/shipments/", params={"customer_order_id": orders[1].id}
    )
    assert shipment_id in [row["id"] for row in second_order_list.json()]
    first = next(row for row in shipment.json()["items"]
                 if row["source_customer_order_id"] == orders[0].id)
    second = next(row for row in shipment.json()["items"]
                  if row["source_customer_order_id"] == orders[1].id)
    edited = await async_client.patch(
        f"/inventory/shipments/{shipment_id}/items/{first['id']}/",
        json={"quantity": 1},
    )
    assert edited.status_code == 200, edited.text

    next_draft = await async_client.post(
        "/customer-orders/assembly/realizations",
        json={"order_ids": [orders[0].id]},
    )
    assert next_draft.status_code == 201, next_draft.text
    first_page = await async_client.get(
        "/inventory/shipments/", params={"offset": 0, "limit": 1}
    )
    second_page = await async_client.get(
        "/inventory/shipments/", params={"offset": 1, "limit": 1}
    )
    assert first_page.headers["x-total-count"] == "2"
    assert second_page.headers["x-total-count"] == "2"
    assert {first_page.json()[0]["id"], second_page.json()[0]["id"]} == {
        shipment_id, next_draft.json()["id"],
    }
    next_doc = await async_client.get(
        f"/inventory/shipments/{next_draft.json()['id']}/"
    )
    assert next_doc.json()["items"][0]["quantity"] == 2
    too_many = await async_client.patch(
        f"/inventory/shipments/{next_draft.json()['id']}/items/"
        f"{next_doc.json()['items'][0]['id']}/",
        json={"quantity": 3},
    )
    assert too_many.status_code == 409

    box = await async_client.post(
        f"/customer-orders/assembly/realizations/{shipment_id}/boxes"
    )
    assert box.status_code == 201, box.text
    contents = await async_client.put(
        f"/customer-orders/assembly/realizations/{shipment_id}/boxes/{box.json()['id']}",
        json={"items": [
            {"shipment_item_id": first["id"], "quantity": 1},
            {"shipment_item_id": second["id"], "quantity": 2},
        ]},
    )
    assert contents.status_code == 200, contents.text
    blocked_reduce = await async_client.patch(
        f"/inventory/shipments/{shipment_id}/items/{second['id']}/",
        json={"quantity": 1},
    )
    assert blocked_reduce.status_code == 409
    sealed = await async_client.post(
        f"/customer-orders/assembly/realizations/{shipment_id}/boxes/{box.json()['id']}/seal"
    )
    assert sealed.status_code == 200, sealed.text
    posted = await async_client.post(
        f"/inventory/shipments/{shipment_id}/post/"
    )
    assert posted.status_code == 200, posted.text
    await test_session.refresh(linked_lot)
    assert linked_lot.remaining_quantity == 2
    after_post = await async_client.get("/customer-orders/assembly")
    by_number = {row["number"]: row for row in after_post.json()}
    assert by_number["ORDER-A"]["counts"]["ready"] == 0
    assert by_number["ORDER-B"]["counts"]["ready"] == 0
    cancelled = await async_client.post(
        f"/inventory/shipments/{shipment_id}/unpost/"
    )
    assert cancelled.status_code == 200, cancelled.text
    after_cancel = await async_client.get("/customer-orders/assembly")
    by_number = {row["number"]: row for row in after_cancel.json()}
    assert by_number["ORDER-A"]["counts"]["ready"] == 1
    assert by_number["ORDER-B"]["counts"]["ready"] == 2


@pytest.mark.asyncio
async def test_cross_docking_requires_posted_receipt_and_printed_label(
    async_client, test_session, created_customers, created_providers,
    created_autopart, created_storage,
):
    user = User(
        email="assembly-cross@example.com",
        password_hash=get_password_hash("secret123"),
        role=UserRole.MANAGER,
        status=UserStatus.ACTIVE,
    )
    test_session.add(user)
    order = CustomerOrder(
        customer_id=created_customers[0].id,
        status="PROCESSED",
        order_number="CROSS-A",
    )
    test_session.add(order)
    await test_session.flush()
    item = CustomerOrderItem(
        order_id=order.id,
        oem=created_autopart.oem_number,
        brand="TEST",
        requested_qty=3,
        requested_price=Decimal("150.00"),
        ship_qty=3,
        status="SUPPLIER",
        autopart_id=created_autopart.id,
        supplier_id=created_providers[0].id,
    )
    receipt = SupplierReceipt(provider_id=created_providers[0].id,
                              posted_at=now_moscow())
    test_session.add_all([item, receipt])
    await test_session.flush()
    receipt_item = SupplierReceiptItem(
        receipt_id=receipt.id,
        customer_order_item_id=item.id,
        autopart_id=created_autopart.id,
        received_quantity=2,
    )
    test_session.add(receipt_item)
    await test_session.flush()
    first_lot = StockLot(
        autopart_id=created_autopart.id,
        storage_location_id=created_storage.id,
        source_receipt_id=receipt.id,
        source_receipt_item_id=receipt_item.id,
        initial_quantity=1,
        remaining_quantity=1,
    )
    second_lot = StockLot(
        autopart_id=created_autopart.id,
        storage_location_id=created_storage.id,
        source_receipt_id=receipt.id,
        source_receipt_item_id=receipt_item.id,
        initial_quantity=1,
        remaining_quantity=1,
    )
    test_session.add_all([first_lot, second_lot])
    await test_session.commit()
    login = await async_client.post(
        "/auth/login", json={"email": user.email, "password": "secret123"}
    )
    assert login.status_code == 200

    before = await async_client.get("/customer-orders/assembly")
    row = next(row for row in before.json() if row["number"] == "CROSS-A")
    assert row["counts"]["received"] == 2
    assert row["counts"]["ready"] == 0

    label = CrossDockingLabel(
        supplier_receipt_item_id=receipt_item.id,
        customer_order_item_id=item.id,
        quantity=2,
        requested_brand=item.brand,
        requested_oem=item.oem,
        barcode="TEST-CROSS-1",
        print_count=1,
    )
    test_session.add(label)
    await test_session.commit()
    after = await async_client.get("/customer-orders/assembly")
    row = next(row for row in after.json() if row["number"] == "CROSS-A")
    assert row["counts"]["cross_labeled"] == 2
    assert row["counts"]["ready"] == 2

    created = await async_client.post(
        "/customer-orders/assembly/realizations",
        json={"order_ids": [order.id]},
    )
    assert created.status_code == 201, created.text
    shipment = await async_client.get(
        f"/inventory/shipments/{created.json()['id']}/"
    )
    assert len(shipment.json()["items"]) == 2
    assert {row["cross_docking_label_id"] for row in shipment.json()["items"]} == {label.id}
    assert {row["preferred_lot_id"] for row in shipment.json()["items"]} == {
        first_lot.id, second_lot.id,
    }


@pytest.mark.asyncio
async def test_own_label_uses_first_sufficient_location_and_reprint_does_not_pick_again(
    async_client, test_session, created_customers, created_autopart,
    created_storage,
):
    user = User(
        email="assembly-location@example.com",
        password_hash=get_password_hash("secret123"),
        role=UserRole.MANAGER,
        status=UserStatus.ACTIVE,
    )
    order = CustomerOrder(customer_id=created_customers[0].id,
                          status="PROCESSED")
    test_session.add_all([user, order])
    await test_session.flush()
    item = CustomerOrderItem(
        order_id=order.id, oem=created_autopart.oem_number,
        brand="TEST", requested_qty=3, ship_qty=3,
        status="OWN_STOCK", autopart_id=created_autopart.id,
    )
    stock_order = StockOrder(customer_id=created_customers[0].id)
    second_location = StorageLocation(name="BB 1")
    test_session.add_all([item, stock_order, second_location])
    await test_session.flush()
    stock_item = StockOrderItem(
        stock_order_id=stock_order.id,
        customer_order_item_id=item.id,
        autopart_id=created_autopart.id,
        quantity=3,
    )
    test_session.add_all([
        stock_item,
        StockByLocation(autopart_id=created_autopart.id,
                        storage_location_id=created_storage.id, quantity=1),
        StockByLocation(autopart_id=created_autopart.id,
                        storage_location_id=second_location.id, quantity=3),
    ])
    await test_session.commit()
    login = await async_client.post(
        "/auth/login", json={"email": user.email, "password": "secret123"}
    )
    assert login.status_code == 200

    printed = await async_client.post(
        "/customer-orders/assembly/own-labels/print",
        json={"stock_item_ids": [stock_item.id]},
    )
    assert printed.status_code == 200, printed.text
    assert len(printed.json()) == 1
    assert printed.json()[0]["location"] == "BB 1"
    label_id = printed.json()[0]["id"]
    repeated = await async_client.post(
        f"/customer-orders/assembly/own-labels/{label_id}/reprint"
    )
    assert repeated.status_code == 200, repeated.text
    assert repeated.json()["print_count"] == 2
    assembly = await async_client.get("/customer-orders/assembly")
    assert assembly.json()[0]["counts"]["own_labeled"] == 3

    cancelled = await async_client.post(
        f"/customer-orders/assembly/own-labels/{label_id}/cancel"
    )
    assert cancelled.status_code == 200, cancelled.text
    after_cancel = await async_client.get("/customer-orders/assembly")
    assert after_cancel.json()[0]["counts"]["own_labeled"] == 0
    await test_session.execute(
        update(StockByLocation)
        .where(StockByLocation.storage_location_id == second_location.id)
        .values(quantity=2)
    )
    await test_session.commit()
    split = await async_client.post(
        "/customer-orders/assembly/own-labels/print",
        json={"stock_item_ids": [stock_item.id]},
    )
    assert split.status_code == 200, split.text
    assert [(row["location"], row["quantity"]) for row in split.json()] == [
        (created_storage.name, 1), (second_location.name, 2),
    ]
    for row in split.json():
        response = await async_client.post(
            f"/customer-orders/assembly/own-labels/{row['id']}/cancel"
        )
        assert response.status_code == 200
    first_location = await async_client.post(
        "/customer-orders/assembly/own-labels/print",
        json={"stock_item_ids": [stock_item.id],
              "storage_location_id": created_storage.id},
    )
    assert first_location.status_code == 200, first_location.text
    assert [(row["location"], row["quantity"]) for row in first_location.json()] == [
        (created_storage.name, 1),
    ]
    second_location_print = await async_client.post(
        "/customer-orders/assembly/own-labels/print",
        json={"stock_item_ids": [stock_item.id],
              "storage_location_id": second_location.id},
    )
    assert second_location_print.status_code == 200, second_location_print.text
    assert [(row["location"], row["quantity"]) for row in second_location_print.json()] == [
        (second_location.name, 2),
    ]


@pytest.mark.asyncio
async def test_label_uses_custom_label_name_when_set(
    async_client, test_session, created_customers, created_autopart,
    created_storage,
):
    customer = created_customers[0]
    user = User(
        email="assembly-labelname@example.com",
        password_hash=get_password_hash("secret123"),
        role=UserRole.MANAGER,
        status=UserStatus.ACTIVE,
    )
    order = CustomerOrder(customer_id=customer.id, status="PROCESSED")
    test_session.add_all([user, order])
    await test_session.flush()
    item = CustomerOrderItem(
        order_id=order.id, oem=created_autopart.oem_number, brand="TEST",
        requested_qty=1, ship_qty=1, status="OWN_STOCK",
        autopart_id=created_autopart.id,
    )
    stock_order = StockOrder(customer_id=customer.id)
    test_session.add_all([item, stock_order])
    await test_session.flush()
    stock_item = StockOrderItem(
        stock_order_id=stock_order.id, customer_order_item_id=item.id,
        autopart_id=created_autopart.id, quantity=1,
    )
    test_session.add_all([
        stock_item,
        StockByLocation(autopart_id=created_autopart.id,
                        storage_location_id=created_storage.id, quantity=1),
    ])
    await test_session.commit()
    login = await async_client.post(
        "/auth/login", json={"email": user.email, "password": "secret123"}
    )
    assert login.status_code == 200

    printed = await async_client.post(
        "/customer-orders/assembly/own-labels/print",
        json={"stock_item_ids": [stock_item.id]},
    )
    assert printed.status_code == 200, printed.text
    label = printed.json()[0]
    default_name = label["customer_name"]
    assert default_name

    saved = await async_client.patch(
        f"/customers/{customer.id}/", json={"name": customer.name, "label_name": "  Мотор  "}
    )
    assert saved.status_code == 200, saved.text
    assert saved.json()["label_name"] == "Мотор"
    listed = (await async_client.get("/customer-orders/assembly")).json()[0]
    assert listed["customer_label_name"] == "Мотор"
    assert listed["customer_name"] != "Мотор"

    cleared = await async_client.patch(
        f"/customers/{customer.id}/", json={"name": customer.name, "label_name": ""}
    )
    assert cleared.json()["label_name"] is None
    listed = (await async_client.get("/customer-orders/assembly")).json()[0]
    assert listed["customer_label_name"] == default_name
