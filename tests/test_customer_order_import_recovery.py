from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from dz_fastapi.models.email_account import EmailAccount
from dz_fastapi.models.partner import (
    CUSTOMER_ORDER_STATUS,
    CustomerOrder,
    CustomerOrderConfig,
    CustomerOrderItem,
)
from dz_fastapi.services import customer_orders as customer_order_service
from dz_fastapi.services.customer_orders import (
    ParsedOrderRow,
    _fetch_order_messages,
    _find_partssoft_order_duplicate,
    _is_resumable_import_stub,
    _is_same_file_delivery,
)


def test_same_file_hash_on_later_day_is_a_new_order_without_stable_identity():
    received_at = datetime(2026, 9, 18, 8, 0, tzinfo=timezone.utc)
    order = CustomerOrder(
        received_at=received_at,
        source_email="orders@example.com",
        source_uid=100,
        order_number="#",
    )
    repeated_template = SimpleNamespace(
        uid=200,
        received_at=received_at + timedelta(days=1),
    )

    assert not _is_same_file_delivery(
        order,
        repeated_template,
        sender="orders@example.com",
        order_number_hint="#",
    )


def test_same_file_hash_with_same_order_number_is_a_duplicate_on_later_day():
    received_at = datetime(2026, 9, 18, 8, 0, tzinfo=timezone.utc)
    order = CustomerOrder(
        received_at=received_at,
        source_email="orders@example.com",
        source_uid=100,
        order_number="ORDER-42",
    )
    redelivery = SimpleNamespace(
        uid=200,
        received_at=received_at + timedelta(days=1),
    )

    assert _is_same_file_delivery(
        order,
        redelivery,
        sender="orders@example.com",
        order_number_hint="Заказ ORDER-42",
    )


@pytest.mark.asyncio
async def test_fetch_order_messages_combines_new_and_recovery_uids(monkeypatch):
    captured = {}

    class FakeFolder:
        def set(self, folder):
            captured["folder"] = folder

    class FakeMailbox:
        folder = FakeFolder()

        def login(self, email, password):
            return self

        def fetch(self, criteria, **kwargs):
            captured["criteria"] = criteria
            return []

        def logout(self):
            return None

    monkeypatch.setattr(
        customer_order_service,
        "_create_mailbox",
        lambda *args, **kwargs: FakeMailbox(),
    )

    messages = await _fetch_order_messages(
        "imap.example.com",
        "orders@example.com",
        "password",
        "INBOX",
        customer_order_service.now_moscow().date(),
        False,
        last_uid=1300,
        additional_uids={1210, 1205},
    )

    assert messages == []
    assert captured["folder"] == "INBOX"
    assert captured["criteria"] == "UID 1205,1210,1301:*"


@pytest.mark.asyncio
async def test_new_email_import_without_items_is_resumable(
    test_session,
    created_customers,
):
    order = CustomerOrder(
        customer_id=created_customers[0].id,
        status=CUSTOMER_ORDER_STATUS.NEW,
        source_filename="order.xlsx",
        file_hash="a" * 64,
    )
    test_session.add(order)
    await test_session.flush()

    assert await _is_resumable_import_stub(test_session, order) is True


@pytest.mark.asyncio
async def test_new_import_with_items_is_not_resumable(
    test_session,
    created_customers,
):
    order = CustomerOrder(
        customer_id=created_customers[0].id,
        status=CUSTOMER_ORDER_STATUS.NEW,
        source_filename="order.xlsx",
        file_hash="b" * 64,
    )
    test_session.add(order)
    await test_session.flush()
    test_session.add(
        CustomerOrderItem(
            order_id=order.id,
            row_index=1,
            oem="TEST-1",
            brand="TEST",
            requested_qty=1,
        )
    )
    await test_session.flush()

    assert await _is_resumable_import_stub(test_session, order) is False


@pytest.mark.asyncio
async def test_email_import_finds_existing_partssoft_order(
    test_session,
    created_customers,
):
    order = CustomerOrder(
        customer_id=created_customers[0].id,
        external_source="PARTS_SOFT",
        external_order_id="9004",
        order_number="WEB-9004",
        order_date=date(2026, 9, 9),
        status=CUSTOMER_ORDER_STATUS.PROCESSED,
    )
    test_session.add(order)
    await test_session.flush()
    test_session.add(
        CustomerOrderItem(
            order_id=order.id,
            row_index=1,
            oem="ABC-123",
            brand="HAVAL",
            requested_qty=2,
            requested_price=1500.50,
        )
    )
    await test_session.commit()

    duplicate = await _find_partssoft_order_duplicate(
        test_session,
        customer_id=created_customers[0].id,
        rows=[
            ParsedOrderRow(
                row_index=1,
                oem="abc-123",
                brand="haval",
                name=None,
                requested_qty=2,
                requested_price=1500.5,
            )
        ],
        order_number="WEB-9004",
        order_date=date(2026, 9, 9),
    )

    assert duplicate is not None
    assert duplicate.id == order.id


@pytest.mark.asyncio
async def test_email_import_matches_verbose_partssoft_order_number(
    test_session,
    created_customers,
):
    order = CustomerOrder(
        customer_id=created_customers[0].id,
        external_source="PARTS_SOFT",
        external_order_id="217300",
        order_number="Заказ № 37137 от 16.09.2026 8:59:40",
        order_date=date(2026, 9, 16),
        status=CUSTOMER_ORDER_STATUS.PROCESSED,
    )
    test_session.add(order)
    await test_session.commit()

    duplicate = await _find_partssoft_order_duplicate(
        test_session,
        customer_id=created_customers[0].id,
        rows=[
            ParsedOrderRow(
                row_index=1,
                oem="different-source-format",
                brand="",
                name=None,
                requested_qty=1,
                requested_price=None,
            )
        ],
        order_number="№ 37137 от",
        order_date=date(2026, 9, 16),
    )

    assert duplicate is not None
    assert duplicate.id == order.id


@pytest.mark.asyncio
async def test_email_import_matches_numberless_partssoft_order_by_content(
    test_session,
    created_customers,
):
    received_at = customer_order_service.now_moscow()
    order = CustomerOrder(
        customer_id=created_customers[0].id,
        external_source="PARTS_SOFT",
        external_order_id="217301",
        order_number="#",
        order_date=received_at.date(),
        received_at=received_at,
        status=CUSTOMER_ORDER_STATUS.PROCESSED,
    )
    test_session.add(order)
    await test_session.flush()
    test_session.add(
        CustomerOrderItem(
            order_id=order.id,
            row_index=1,
            oem="BH-3888-E",
            brand="NOK ORIGINAL",
            requested_qty=2,
            requested_price=5056,
        )
    )
    await test_session.commit()

    duplicate = await _find_partssoft_order_duplicate(
        test_session,
        customer_id=created_customers[0].id,
        rows=[
            ParsedOrderRow(
                row_index=1,
                oem="BH3888E",
                brand="NOK",
                name=None,
                requested_qty=2,
                requested_price=5056,
            )
        ],
        order_number="4072",
        order_date=received_at.date(),
        received_at=received_at,
    )

    assert duplicate is not None
    assert duplicate.id == order.id


@pytest.mark.asyncio
async def test_targeted_recovery_fetches_interrupted_order_uid(
    test_session,
    created_customers,
    monkeypatch,
):
    customer = created_customers[0]
    account = EmailAccount(
        name="Recovery inbox",
        email="orders-recovery@example.com",
        password="test-password",
        transport="smtp",
        imap_host="imap.example.com",
        imap_port=993,
        imap_folder="INBOX",
        purposes=["orders_in"],
        is_active=True,
    )
    test_session.add(account)
    await test_session.flush()

    config = CustomerOrderConfig(
        customer_id=customer.id,
        order_email="client-orders@example.com",
        order_emails=[],
        email_account_id=account.id,
        order_start_row=1,
        oem_col=0,
        brand_col=1,
        qty_col=2,
        is_active=True,
    )
    test_session.add(config)
    await test_session.flush()
    test_session.add(
        CustomerOrder(
            customer_id=customer.id,
            order_config_id=config.id,
            status=CUSTOMER_ORDER_STATUS.NEW,
            source_email="client-orders@example.com",
            source_uid=1205,
            source_filename="order.xlsx",
            file_hash="c" * 64,
        )
    )
    await test_session.commit()

    captured = {}

    async def fake_fetch_order_messages(*args, **kwargs):
        captured.update(kwargs)
        return []

    monkeypatch.setattr(
        customer_order_service,
        "_fetch_order_messages",
        fake_fetch_order_messages,
    )

    await customer_order_service.process_customer_orders(
        test_session,
        customer_id=customer.id,
        config_id=config.id,
    )

    assert captured["last_uid"] is None
    assert captured["additional_uids"] == {1205}
    assert captured["from_email"] == "client-orders@example.com"


@pytest.mark.asyncio
async def test_regular_run_fetches_all_interrupted_order_uids(
    test_session,
    created_customers,
    monkeypatch,
):
    account = EmailAccount(
        name="Shared recovery inbox",
        email="orders-all@example.com",
        password="test-password",
        transport="smtp",
        imap_host="imap.example.com",
        imap_port=993,
        imap_folder="INBOX",
        purposes=["orders_in"],
        is_active=True,
    )
    test_session.add(account)
    await test_session.flush()

    configs = []
    for index, customer in enumerate(created_customers, start=1):
        config = CustomerOrderConfig(
            customer_id=customer.id,
            order_email=f"client-{index}@example.com",
            order_emails=[],
            email_account_id=account.id,
            order_start_row=1,
            oem_col=0,
            brand_col=1,
            qty_col=2,
            last_uid=1300,
            is_active=True,
        )
        test_session.add(config)
        configs.append(config)
    await test_session.flush()

    for config, source_uid in zip(configs, (1205, 1210)):
        test_session.add(
            CustomerOrder(
                customer_id=config.customer_id,
                order_config_id=config.id,
                status=CUSTOMER_ORDER_STATUS.NEW,
                source_email=config.order_email,
                source_uid=source_uid,
                source_filename=f"order-{source_uid}.xlsx",
                file_hash=str(source_uid).zfill(64),
            )
        )
    await test_session.commit()

    captured = {}

    async def fake_fetch_order_messages(*args, **kwargs):
        captured.update(kwargs)
        return []

    monkeypatch.setattr(
        customer_order_service,
        "_fetch_order_messages",
        fake_fetch_order_messages,
    )

    await customer_order_service.process_customer_orders(test_session)

    assert captured["last_uid"] == 1300
    assert captured["additional_uids"] == {1205, 1210}


def _order_xlsx(rows):
    from io import BytesIO

    from openpyxl import Workbook

    workbook = Workbook()
    sheet = workbook.active
    for row in rows:
        sheet.append(row)
    buffer = BytesIO()
    workbook.save(buffer)
    return buffer.getvalue()


@pytest.mark.asyncio
async def test_one_failing_email_does_not_block_the_rest_of_the_batch(
    test_session,
    created_customers,
    monkeypatch,
):
    """Письмо, на котором обработка падает, не должно ломать остальные.

    После rollback все загруженные объекты «просрочены», и прежний обработчик
    ошибки падал на ``config.customer_id`` (greenlet_spawn). Письмо не
    записывалось, отметка не двигалась, а вся остальная пачка падала следом.
    """
    from sqlalchemy import select

    from dz_fastapi.models.partner import CustomerPriceListConfig

    account = EmailAccount(
        name="orders-inbox",
        email="orders-inbox@example.com",
        password="secret",
        imap_host="imap.example.com",
        imap_folder="INBOX",
        purposes=["orders_in"],
        is_active=True,
    )
    test_session.add(account)
    await test_session.flush()

    configs = []
    for index, customer in enumerate(created_customers[:2], start=1):
        # У прайса нет ни одной опубликованной версии: подбор по заказу упадёт
        price_config = CustomerPriceListConfig(
            customer_id=customer.id,
            name=f"price-{index}",
            general_markup=1.0,
            own_price_list_markup=1.0,
            third_party_markup=1.0,
            emails=[],
            additional_filters={},
        )
        test_session.add(price_config)
        await test_session.flush()
        config = CustomerOrderConfig(
            customer_id=customer.id,
            order_email=f"client-{index}@example.com",
            order_emails=[],
            email_account_id=account.id,
            pricelist_config_id=price_config.id,
            order_start_row=1,
            oem_col=0,
            brand_col=1,
            qty_col=2,
            last_uid=100,
            is_active=True,
        )
        test_session.add(config)
        configs.append(config)
    await test_session.commit()
    config_ids = [config.id for config in configs]
    customer_ids = [customer.id for customer in created_customers[:2]]

    payload = _order_xlsx([["A100", "BRAND", 1], ["A200", "BRAND", 2]])
    messages = [
        SimpleNamespace(
            uid=str(200 + index),
            subject=f"Order {index}",
            from_=f"client-{index}@example.com",
            attachments=[SimpleNamespace(filename=f"order-{index}.xlsx", payload=payload)],
            received_at=datetime(2026, 10, 18, 8, index, tzinfo=timezone.utc),
            date=datetime(2026, 10, 18, 8, index, tzinfo=timezone.utc),
            text="",
            html="",
            folder_name="INBOX",
            email_account_id=account.id,
        )
        for index in (1, 2)
    ]

    async def fake_fetch_order_messages(*args, **kwargs):
        return list(messages)

    monkeypatch.setattr(customer_order_service, "_fetch_order_messages", fake_fetch_order_messages)

    await customer_order_service.process_customer_orders(test_session)

    orders = (
        (
            await test_session.execute(
                select(CustomerOrder).where(CustomerOrder.customer_id.in_(customer_ids))
            )
        )
        .scalars()
        .all()
    )
    assert sorted(order.source_uid for order in orders) == [201, 202]
    assert all(order.status == CUSTOMER_ORDER_STATUS.ERROR for order in orders)
    assert all("опубликованного прайса" in (order.error_details or "") for order in orders)
    mark_rows = await test_session.execute(
        select(CustomerOrderConfig.last_uid).where(CustomerOrderConfig.id.in_(config_ids))
    )
    marks = mark_rows.scalars().all()
    assert sorted(marks) == [201, 202]
