from datetime import date, datetime, timedelta
from decimal import Decimal

import pytest
from sqlalchemy import select

from dz_fastapi.core.time import now_moscow
from dz_fastapi.models.inventory import (StockByLocation, StockMovement,
                                         Warehouse)
from dz_fastapi.models.partner import (ORDER_TRACKING_SOURCE,
                                       SUPPLIER_ORDER_STATUS,
                                       TYPE_ORDER_ITEM_STATUS,
                                       TYPE_STATUS_ORDER, CustomerOrder,
                                       CustomerOrderItem, Order, OrderItem,
                                       StockOrder, StockOrderItem,
                                       SupplierOrder, SupplierOrderItem)
from dz_fastapi.models.user import User, UserRole, UserStatus
from dz_fastapi.services.auth import get_password_hash


async def _create_user(session, email: str, role: UserRole):
    user = User(
        email=email,
        password_hash=get_password_hash("secret123"),
        role=role,
        status=UserStatus.ACTIVE,
    )
    session.add(user)
    await session.commit()
    await session.refresh(user)
    return user


async def _login(async_client, email: str):
    response = await async_client.post(
        "/auth/login",
        json={"email": email, "password": "secret123"},
    )
    assert response.status_code == 200


@pytest.mark.asyncio
async def test_customer_order_config_crud(
    async_client, test_session, created_customers
):
    await _create_user(test_session, "manager@example.com", UserRole.MANAGER)
    await _login(async_client, "manager@example.com")

    customer = created_customers[0]
    payload = {
        "customer_id": customer.id,
        "order_email": "orders@client.com",
        "oem_col": 1,
        "brand_col": 2,
        "qty_col": 3,
        "price_tolerance_pct": 2,
        "price_warning_pct": 5,
    }

    response = await async_client.post("/customer-orders/config", json=payload)
    assert response.status_code == 201
    config = response.json()
    assert config["customer_id"] == customer.id
    assert config["oem_col"] == 1
    assert config["brand_col"] == 2
    assert config["qty_col"] == 3

    response = await async_client.get(f"/customer-orders/config/{customer.id}")
    assert response.status_code == 200

    response = await async_client.put(
        f"/customer-orders/config/{customer.id}",
        json={"order_subject_pattern": "test"},
    )
    assert response.status_code == 200
    assert response.json()["order_subject_pattern"] == "test"


def _shift_to_previous_month(value: datetime) -> datetime:
    year = value.year
    month = value.month - 1
    if month == 0:
        month = 12
        year -= 1
    return value.replace(year=year, month=month)


@pytest.mark.asyncio
async def test_customer_order_item_stats_monthly_breakdown(
    async_client, test_session, created_customers
):
    await _create_user(test_session, "stats@example.com", UserRole.MANAGER)
    await _login(async_client, "stats@example.com")

    customer = created_customers[0]
    other_customer = created_customers[1]
    now = now_moscow().replace(day=15, hour=10, minute=0, second=0)
    previous_month = _shift_to_previous_month(now)

    order_one = CustomerOrder(
        customer_id=customer.id,
        status="SENT",
        received_at=now,
    )
    order_two = CustomerOrder(
        customer_id=customer.id,
        status="SENT",
        received_at=previous_month,
    )
    order_three = CustomerOrder(
        customer_id=other_customer.id,
        status="SENT",
        received_at=previous_month,
    )
    test_session.add_all([order_one, order_two, order_three])
    await test_session.flush()

    test_session.add_all(
        [
            CustomerOrderItem(
                order_id=order_one.id,
                oem="SH0113TM3",
                brand="MAZDA",
                requested_qty=2,
                requested_price=Decimal("2325.00"),
                ship_qty=2,
                status="SUPPLIER",
            ),
            CustomerOrderItem(
                order_id=order_two.id,
                oem="SH0113TM3",
                brand="MAZDA",
                requested_qty=1,
                requested_price=Decimal("2100.00"),
                ship_qty=0,
                reject_qty=1,
                status="REJECTED",
            ),
            CustomerOrderItem(
                order_id=order_three.id,
                oem="SH0113TM3",
                brand="MAZDA",
                requested_qty=4,
                requested_price=Decimal("2200.00"),
                ship_qty=4,
                status="SUPPLIER",
            ),
        ]
    )
    await test_session.commit()

    response = await async_client.get(
        "/customer-orders/item-stats",
        params={
            "kind": "oem",
            "value": "SH0113TM3",
            "customer_id": customer.id,
            "months": 3,
        },
    )
    assert response.status_code == 200, response.text
    payload = response.json()

    assert payload["kind"] == "oem"
    assert payload["value"] == "SH0113TM3"
    assert payload["current_customer_id"] == customer.id
    assert payload["current_customer_summary"]["orders_count"] == 2
    assert payload["current_customer_summary"]["rows_count"] == 2
    assert payload["current_customer_summary"]["total_requested_qty"] == 3
    assert payload["all_customers_summary"]["orders_count"] == 3
    assert payload["all_customers_summary"]["total_requested_qty"] == 7
    assert len(payload["current_customer_monthly"]) == 3
    assert len(payload["all_customers_monthly"]) == 3
    assert (
        payload["current_customer_recent"][0]["requested_price"]
        == "2325.00"
    )
    assert payload["all_customers_recent"][0]["customer_id"] == customer.id

    monthly_all = {
        row["month"]: row for row in payload["all_customers_monthly"]
    }
    assert (
        monthly_all[date(now.year, now.month, 1).isoformat()]["orders_count"]
        == 1
    )
    assert (
        monthly_all[
            date(previous_month.year, previous_month.month, 1).isoformat()
        ]["orders_count"]
        == 2
    )


@pytest.mark.asyncio
async def test_customer_order_summary_includes_partial_reject_qty(
    async_client, test_session, created_customers
):
    await _create_user(test_session, "summary@example.com", UserRole.MANAGER)
    await _login(async_client, "summary@example.com")

    customer = created_customers[0]
    order = CustomerOrder(
        customer_id=customer.id,
        status="PROCESSED",
        received_at=now_moscow(),
    )
    test_session.add(order)
    await test_session.flush()

    test_session.add(
        CustomerOrderItem(
            order_id=order.id,
            oem="16626AD200",
            brand="NISSAN",
            requested_qty=16,
            requested_price=Decimal("100.00"),
            ship_qty=10,
            reject_qty=6,
            status="OWN_STOCK",
        )
    )
    await test_session.commit()

    response = await async_client.get(
        "/customer-orders/summary",
        params={"customer_id": customer.id},
    )
    assert response.status_code == 200, response.text
    rows = response.json()
    row = next((item for item in rows if item["id"] == order.id), None)
    assert row is not None
    assert row["stock_sum"] == pytest.approx(1000.0)
    assert row["rejected_sum"] == pytest.approx(600.0)
    assert row["total_sum"] == pytest.approx(1600.0)
    assert row["rejected_pct"] == pytest.approx(37.5)


@pytest.mark.asyncio
async def test_supplier_order_list_rejected_pct_uses_order_value_base(
    async_client,
    test_session,
    created_customers,
    created_providers,
):
    await _create_user(
        test_session, "supplier-list@example.com", UserRole.MANAGER
    )
    await _login(async_client, "supplier-list@example.com")

    customer_order = CustomerOrder(
        customer_id=created_customers[0].id,
        status="PROCESSED",
        received_at=now_moscow(),
    )
    test_session.add(customer_order)
    await test_session.flush()

    customer_item = CustomerOrderItem(
        order_id=customer_order.id,
        oem="SMD359158",
        brand="CHERY",
        requested_qty=10,
        requested_price=Decimal("140.00"),
        ship_qty=10,
        status="SUPPLIER",
        matched_price=Decimal("100.00"),
    )
    test_session.add(customer_item)
    await test_session.flush()

    supplier_order = SupplierOrder(
        provider_id=created_providers[0].id,
        status=SUPPLIER_ORDER_STATUS.SENT,
        created_at=now_moscow(),
        sent_at=now_moscow(),
    )
    test_session.add(supplier_order)
    await test_session.flush()

    # Order value base: 10 * 100 = 1000
    # Rejected value: (10 - 7) * 100 = 300 => 30%
    # response_price must not affect rejection percent base.
    test_session.add(
        SupplierOrderItem(
            supplier_order_id=supplier_order.id,
            customer_order_item_id=customer_item.id,
            quantity=10,
            price=Decimal("100.00"),
            confirmed_quantity=7,
            response_price=Decimal("50.00"),
        )
    )
    await test_session.commit()

    response = await async_client.get("/customer-orders/supplier/list")
    assert response.status_code == 200, response.text
    payload = response.json()
    row = next((item for item in payload if item["id"] == supplier_order.id))

    assert row is not None
    assert row["supplier_sum"] == pytest.approx(1000.0)
    assert row["total_sum"] == pytest.approx(1000.0)
    assert row["rejected_sum"] == pytest.approx(300.0)
    assert row["rejected_pct"] == pytest.approx(30.0)


@pytest.mark.asyncio
async def test_stock_order_pick_endpoint_updates_progress(
    async_client,
    test_session,
    created_autopart,
    created_customers,
    created_storage,
):
    await _create_user(test_session, "picker@example.com", UserRole.MANAGER)
    await _login(async_client, "picker@example.com")

    created_autopart.storage_locations.append(created_storage)
    stock_order = StockOrder(customer_id=created_customers[0].id)
    test_session.add(stock_order)
    await test_session.flush()

    stock_item = StockOrderItem(
        stock_order_id=stock_order.id,
        autopart_id=created_autopart.id,
        quantity=3,
    )
    test_session.add(stock_item)
    await test_session.commit()

    response = await async_client.patch(
        f"/customer-orders/stock/items/{stock_item.id}/pick",
        json={"increment": 1, "scan_code": created_autopart.barcode},
    )
    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["picked_quantity"] == 1
    assert payload["stock_order_status"] == "NEW"
    assert payload["pick_last_scan_code"] == created_autopart.barcode
    assert payload["picked_by_email"] == "picker@example.com"

    response = await async_client.patch(
        f"/customer-orders/stock/items/{stock_item.id}/pick",
        json={"picked_quantity": 3},
    )
    assert response.status_code == 200, response.text
    assert response.json()["stock_order_status"] == "COMPLETED"

    response = await async_client.get("/customer-orders/stock/list")
    assert response.status_code == 200, response.text
    rows = response.json()
    assert rows[0]["customer_name"] == created_customers[0].name
    assert rows[0]["items"][0]["picked_quantity"] == 3
    assert rows[0]["items"][0]["picked_by_email"] == "picker@example.com"


@pytest.mark.asyncio
async def test_supplier_receipt_candidates_and_create_receipt(
    async_client,
    test_session,
    created_autopart,
    created_customers,
    created_providers,
):
    await _create_user(test_session, "receiver@example.com", UserRole.MANAGER)
    await _login(async_client, "receiver@example.com")

    customer_order = CustomerOrder(
        customer_id=created_customers[0].id,
        order_number="CO-1001",
        status="SENT",
        received_at=now_moscow(),
    )
    test_session.add(customer_order)
    await test_session.flush()

    customer_order_item = CustomerOrderItem(
        order_id=customer_order.id,
        oem=created_autopart.oem_number,
        brand="TEST BRAND",
        name=created_autopart.name,
        requested_qty=4,
        requested_price=Decimal("120.00"),
        ship_qty=4,
        status="SUPPLIER",
        autopart_id=created_autopart.id,
    )
    test_session.add(customer_order_item)
    await test_session.flush()

    supplier_order = SupplierOrder(
        provider_id=created_providers[0].id,
        status=SUPPLIER_ORDER_STATUS.SENT,
        created_at=now_moscow(),
        sent_at=now_moscow(),
    )
    test_session.add(supplier_order)
    await test_session.flush()

    supplier_item = SupplierOrderItem(
        supplier_order_id=supplier_order.id,
        customer_order_item_id=customer_order_item.id,
        autopart_id=created_autopart.id,
        oem_number=created_autopart.oem_number,
        brand_name="TEST BRAND",
        autopart_name=created_autopart.name,
        quantity=4,
        confirmed_quantity=3,
        price=Decimal("118.50"),
        response_price=Decimal("119.00"),
        response_status_raw="готово",
    )
    test_session.add(supplier_item)
    await test_session.commit()

    response = await async_client.get(
        "/customer-orders/supplier-receipts/candidates",
        params={"provider_id": created_providers[0].id},
    )
    assert response.status_code == 200, response.text
    rows = response.json()
    assert len(rows) == 1
    assert rows[0]["pending_quantity"] == 3
    assert rows[0]["customer_name"] == created_customers[0].name
    assert rows[0]["response_status_raw"] == "готово"

    response = await async_client.post(
        "/customer-orders/supplier-receipts",
        json={
            "provider_id": created_providers[0].id,
            "document_number": "RC-77",
            "items": [
                {
                    "supplier_order_item_id": supplier_item.id,
                    "received_quantity": 2,
                    "comment": "Частичное поступление",
                }
            ],
        },
    )
    assert response.status_code == 201, response.text
    payload = response.json()
    assert payload["provider_id"] == created_providers[0].id
    assert payload["document_number"] == "RC-77"
    assert payload["items"][0]["received_quantity"] == 2
    receipt_id = payload["id"]

    await test_session.refresh(supplier_item)
    assert supplier_item.received_quantity == 2
    assert supplier_item.received_at is not None

    response = await async_client.get(
        "/customer-orders/supplier-receipts/candidates",
        params={"provider_id": created_providers[0].id},
    )
    assert response.status_code == 200, response.text
    rows = response.json()
    assert rows == []

    response = await async_client.post(
        f"/customer-orders/supplier-receipts/{receipt_id}/post",
    )
    assert response.status_code == 200, response.text
    assert response.json()["posted_at"] is not None

    response = await async_client.post(
        f"/customer-orders/supplier-receipts/{receipt_id}/unpost",
    )
    assert response.status_code == 200, response.text
    assert response.json()["posted_at"] is None

    response = await async_client.delete(
        f"/customer-orders/supplier-receipts/{receipt_id}",
    )
    assert response.status_code == 200, response.text
    assert response.json()["deleted"] is True

    response = await async_client.get(
        "/customer-orders/supplier-receipts/candidates",
        params={"provider_id": created_providers[0].id},
    )
    assert response.status_code == 200, response.text
    rows = response.json()
    assert len(rows) == 1
    assert rows[0]["pending_quantity"] == 3


@pytest.mark.asyncio
async def test_supplier_receipt_zero_quantity_marks_explicit_refusal(
    async_client,
    test_session,
    created_autopart,
    created_customers,
    created_providers,
):
    await _create_user(
        test_session, "receiver-refusal@example.com", UserRole.MANAGER
    )
    await _login(async_client, "receiver-refusal@example.com")

    customer_order = CustomerOrder(
        customer_id=created_customers[0].id,
        order_number="CO-REF-1",
        status="PROCESSED",
        received_at=now_moscow(),
    )
    test_session.add(customer_order)
    await test_session.flush()

    customer_order_item = CustomerOrderItem(
        order_id=customer_order.id,
        oem=created_autopart.oem_number,
        brand="TEST BRAND",
        name=created_autopart.name,
        requested_qty=5,
        requested_price=Decimal("140.00"),
        ship_qty=5,
        status="SUPPLIER",
        autopart_id=created_autopart.id,
    )
    test_session.add(customer_order_item)
    await test_session.flush()

    supplier_order = SupplierOrder(
        provider_id=created_providers[0].id,
        status=SUPPLIER_ORDER_STATUS.SENT,
        created_at=now_moscow(),
        sent_at=now_moscow(),
    )
    test_session.add(supplier_order)
    await test_session.flush()

    supplier_item = SupplierOrderItem(
        supplier_order_id=supplier_order.id,
        customer_order_item_id=customer_order_item.id,
        autopart_id=created_autopart.id,
        oem_number=created_autopart.oem_number,
        brand_name="TEST BRAND",
        autopart_name=created_autopart.name,
        quantity=5,
        price=Decimal("100.00"),
    )
    test_session.add(supplier_item)
    await test_session.commit()

    response = await async_client.post(
        "/customer-orders/supplier-receipts",
        json={
            "provider_id": created_providers[0].id,
            "document_number": "RC-REF-1",
            "items": [
                {
                    "supplier_order_item_id": supplier_item.id,
                    "received_quantity": 0,
                    "comment": "Явный отказ",
                }
            ],
        },
    )
    assert response.status_code == 201, response.text
    payload = response.json()
    assert payload["items"][0]["received_quantity"] == 0
    assert payload["items"][0]["comment"] == "Явный отказ"

    await test_session.refresh(supplier_item)
    assert supplier_item.confirmed_quantity == 0

    response = await async_client.get(
        "/customer-orders/supplier/list",
        params={"provider_id": created_providers[0].id},
    )
    assert response.status_code == 200, response.text
    rows = response.json()
    row = next((item for item in rows if item["id"] == supplier_order.id))
    assert row is not None
    assert row["supplier_sum"] == pytest.approx(500.0)
    assert row["rejected_sum"] == pytest.approx(500.0)
    assert row["rejected_pct"] == pytest.approx(100.0)


@pytest.mark.asyncio
async def test_manual_supplier_receipt_auto_links_site_tracking_item(
    async_client,
    test_session,
    created_autopart,
    created_customers,
    created_providers,
):
    await _create_user(
        test_session, "receiver-site@example.com", UserRole.MANAGER
    )
    await _login(async_client, "receiver-site@example.com")

    order = Order(
        provider_id=created_providers[0].id,
        customer_id=created_customers[0].id,
        source_type=ORDER_TRACKING_SOURCE.DRAGONZAP_SEARCH.value,
        status=TYPE_STATUS_ORDER.ORDERED,
    )
    test_session.add(order)
    await test_session.flush()

    order_item = OrderItem(
        order_id=order.id,
        autopart_id=created_autopart.id,
        oem_number=created_autopart.oem_number,
        brand_name="TEST BRAND",
        autopart_name=created_autopart.name,
        quantity=5,
        price=Decimal("100.00"),
        status=TYPE_ORDER_ITEM_STATUS.SENT,
        tracking_uuid="site-receipt-auto-link",
    )
    test_session.add(order_item)
    await test_session.commit()

    response = await async_client.post(
        "/customer-orders/supplier-receipts/manual",
        json={
            "provider_id": created_providers[0].id,
            "document_number": "MAN-SITE-1",
            "items": [
                {
                    "oem_number": created_autopart.oem_number,
                    "brand_name": "TEST BRAND",
                    "autopart_name": created_autopart.name,
                    "received_quantity": 2,
                }
            ],
        },
    )
    assert response.status_code == 201, response.text
    payload = response.json()
    assert payload["items"][0]["order_item_id"] == order_item.id

    await test_session.refresh(order_item)
    await test_session.refresh(order)
    assert order_item.received_quantity == 2
    assert order_item.status == TYPE_ORDER_ITEM_STATUS.IN_PROGRESS
    assert order.status == TYPE_STATUS_ORDER.PROCESSING

    response = await async_client.post(
        "/customer-orders/supplier-receipts/manual",
        json={
            "provider_id": created_providers[0].id,
            "document_number": "MAN-SITE-2",
            "items": [
                {
                    "oem_number": created_autopart.oem_number,
                    "brand_name": "TEST BRAND",
                    "autopart_name": created_autopart.name,
                    "received_quantity": 3,
                }
            ],
        },
    )
    assert response.status_code == 201, response.text
    payload = response.json()
    assert payload["items"][0]["order_item_id"] == order_item.id

    await test_session.refresh(order_item)
    await test_session.refresh(order)
    assert order_item.received_quantity == 5
    assert order_item.status == TYPE_ORDER_ITEM_STATUS.DELIVERED
    assert order.status == TYPE_STATUS_ORDER.ARRIVED


@pytest.mark.asyncio
async def test_manual_supplier_receipt_post_creates_stock_in_default_warehouse(
    async_client,
    test_session,
    created_autopart,
    created_providers,
):
    await _create_user(
        test_session, "receiver-stock@example.com", UserRole.MANAGER
    )
    await _login(async_client, "receiver-stock@example.com")

    response = await async_client.post(
        "/customer-orders/supplier-receipts/manual",
        json={
            "provider_id": created_providers[0].id,
            "document_number": "WH-MAN-1",
            "post_now": True,
            "items": [
                {
                    "autopart_id": created_autopart.id,
                    "oem_number": created_autopart.oem_number,
                    "brand_name": "TEST BRAND",
                    "autopart_name": created_autopart.name,
                    "received_quantity": 4,
                }
            ],
        },
    )

    assert response.status_code == 201, response.text
    payload = response.json()
    assert payload["posted_at"] is not None
    assert payload["warehouse_id"] is not None
    assert payload["warehouse_name"] == "Основной склад"

    warehouse = await test_session.get(Warehouse, payload["warehouse_id"])
    assert warehouse is not None
    assert warehouse.name == "Основной склад"

    stock_row = (
        await test_session.execute(
            select(StockByLocation).where(
                StockByLocation.autopart_id == created_autopart.id
            )
        )
    ).scalar_one_or_none()
    assert stock_row is not None
    assert stock_row.quantity == 4

    movement = (
        await test_session.execute(
            select(StockMovement)
            .where(
                StockMovement.autopart_id == created_autopart.id,
                StockMovement.reference_type == "supplier_receipt",
                StockMovement.reference_id == payload["id"],
            )
            .order_by(StockMovement.id.desc())
        )
    ).scalars().first()
    assert movement is not None
    assert movement.quantity == 4


@pytest.mark.asyncio
async def test_process_supplier_responses_endpoint(
    async_client,
    test_session,
):
    await _create_user(test_session, "sync@example.com", UserRole.MANAGER)
    await _login(async_client, "sync@example.com")

    async def fake_process_supplier_response_messages(
        session,
        *,
        provider_id=None,
        supplier_response_config_id=None,
        date_from=None,
        date_to=None,
    ):
        assert provider_id == 77
        assert supplier_response_config_id is None
        assert str(date_from) == "2026-04-03"
        assert str(date_to) == "2026-04-04"
        return {
            "fetched_messages": 3,
            "processed_messages": 2,
            "matched_orders": 1,
            "stored_attachments": 1,
            "parsed_response_files": 1,
            "parsed_text_positions": 0,
            "recognized_positions": 2,
            "unresolved_positions": 1,
            "unresolved_examples": ["ABC123: строка заказа не найдена"],
            "updated_items": 2,
            "updated_orders": 1,
            "unmapped_statuses": 1,
            "skipped_messages": 1,
        }

    from dz_fastapi.api import customer_order as customer_order_api

    original = customer_order_api.process_supplier_response_messages
    customer_order_api.process_supplier_response_messages = (
        fake_process_supplier_response_messages
    )
    try:
        response = await async_client.post(
            "/customer-orders/supplier/process-responses",
            params={
                "provider_id": 77,
                "date_from": "2026-04-03",
                "date_to": "2026-04-04",
            },
        )
    finally:
        customer_order_api.process_supplier_response_messages = original

    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["processed_messages"] == 2
    assert payload["updated_items"] == 2


@pytest.mark.asyncio
async def test_supplier_receipt_providers_endpoint_filters_by_period(
    async_client,
    test_session,
    created_providers,
):
    await _create_user(
        test_session,
        "providers-filter@example.com",
        UserRole.MANAGER,
    )
    await _login(async_client, "providers-filter@example.com")

    now_dt = now_moscow()
    recent_provider = created_providers[0]
    old_provider = created_providers[1]

    recent_order = SupplierOrder(
        provider_id=recent_provider.id,
        status=SUPPLIER_ORDER_STATUS.SENT,
        created_at=now_dt,
        sent_at=now_dt,
    )
    old_order = SupplierOrder(
        provider_id=old_provider.id,
        status=SUPPLIER_ORDER_STATUS.SENT,
        created_at=now_dt - timedelta(days=15),
        sent_at=now_dt - timedelta(days=15),
    )
    test_session.add_all([recent_order, old_order])
    await test_session.flush()

    # The providers endpoint returns only providers with pending receipt items.
    test_session.add_all(
        [
            SupplierOrderItem(
                supplier_order_id=recent_order.id,
                quantity=2,
            ),
            SupplierOrderItem(
                supplier_order_id=old_order.id,
                quantity=1,
            ),
        ]
    )
    await test_session.commit()

    response = await async_client.get(
        "/customer-orders/supplier-receipts/providers",
        params={
            "date_from": (now_dt.date() - timedelta(days=2)).isoformat(),
            "date_to": now_dt.date().isoformat(),
        },
    )
    assert response.status_code == 200, response.text
    payload = response.json()
    provider_ids = {row["provider_id"] for row in payload}
    assert recent_provider.id in provider_ids
    assert old_provider.id not in provider_ids
