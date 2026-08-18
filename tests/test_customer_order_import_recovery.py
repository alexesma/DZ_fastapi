import pytest

from dz_fastapi.models.email_account import EmailAccount
from dz_fastapi.models.partner import (
    CUSTOMER_ORDER_STATUS,
    CustomerOrder,
    CustomerOrderConfig,
    CustomerOrderItem,
)
from dz_fastapi.services import customer_orders as customer_order_service
from dz_fastapi.services.customer_orders import _fetch_order_messages, _is_resumable_import_stub


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
