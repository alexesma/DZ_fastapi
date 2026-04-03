from datetime import timedelta

import pytest

from dz_fastapi.core.time import now_moscow
from dz_fastapi.models.autopart import AutoPart
from dz_fastapi.models.brand import Brand
from dz_fastapi.models.partner import (ORDER_TRACKING_SOURCE,
                                       SUPPLIER_ORDER_STATUS,
                                       TYPE_ORDER_ITEM_STATUS,
                                       TYPE_STATUS_ORDER, Customer, Order,
                                       OrderItem, Provider, SupplierOrder,
                                       SupplierOrderItem)
from dz_fastapi.models.user import User, UserRole, UserStatus
from dz_fastapi.services.auth import get_password_hash
from dz_fastapi.services.placed_orders import (cleanup_old_tracking_history,
                                               list_tracking_history,
                                               sync_site_tracking_statuses,
                                               update_tracking_item)


async def _create_user(test_session, email='manager@example.com'):
    user = User(
        email=email,
        password_hash=get_password_hash('secret123'),
        role=UserRole.MANAGER,
        status=UserStatus.ACTIVE,
    )
    test_session.add(user)
    await test_session.commit()
    await test_session.refresh(user)
    return user


@pytest.mark.asyncio
async def test_list_tracking_history_returns_site_and_supplier_rows(
    test_session,
):
    user = await _create_user(test_session)
    provider = Provider(
        name='Provider One',
        email_contact='provider@example.com',
        email_incoming_price='prices@example.com',
        type_prices='Wholesale',
    )
    customer = Customer(
        name='Customer One',
        email_contact='customer@example.com',
        email_outgoing_price='out@example.com',
        type_prices='Wholesale',
    )
    brand = Brand(name='TEST')
    test_session.add_all([provider, customer, brand])
    await test_session.flush()

    autopart = AutoPart(
        name='Widget',
        brand_id=brand.id,
        oem_number='OEM123',
    )
    test_session.add(autopart)
    await test_session.flush()

    site_order = Order(
        provider_id=provider.id,
        customer_id=customer.id,
        source_type=ORDER_TRACKING_SOURCE.DRAGONZAP_SEARCH.value,
        created_by_user_id=user.id,
        status=TYPE_STATUS_ORDER.ORDERED,
    )
    supplier_order = SupplierOrder(
        provider_id=provider.id,
        source_type=ORDER_TRACKING_SOURCE.SEARCH_OFFERS.value,
        created_by_user_id=user.id,
        status=SUPPLIER_ORDER_STATUS.SENT,
    )
    test_session.add_all([site_order, supplier_order])
    await test_session.flush()

    test_session.add_all(
        [
            OrderItem(
                order_id=site_order.id,
                autopart_id=autopart.id,
                oem_number='OEM123',
                brand_name='TEST',
                autopart_name='Widget',
                quantity=2,
                price=100,
                min_delivery_day=2,
                max_delivery_day=3,
                status=TYPE_ORDER_ITEM_STATUS.SENT,
            ),
            SupplierOrderItem(
                supplier_order_id=supplier_order.id,
                autopart_id=autopart.id,
                oem_number='OEM123',
                brand_name='TEST',
                autopart_name='Widget',
                quantity=1,
                price=90,
            ),
        ]
    )
    await test_session.commit()

    rows = await list_tracking_history(
        test_session,
        oem_number='OEM123',
    )

    assert len(rows) == 2
    assert {row['source_type'] for row in rows} == {'site', 'supplier'}
    assert all(row['ordered_by_email'] == user.email for row in rows)


@pytest.mark.asyncio
async def test_update_tracking_item_updates_site_status_and_received_qty(
    test_session,
):
    provider = Provider(
        name='Provider Two',
        email_contact='provider2@example.com',
        email_incoming_price='prices2@example.com',
        type_prices='Wholesale',
    )
    customer = Customer(
        name='Customer Two',
        email_contact='customer2@example.com',
        email_outgoing_price='out2@example.com',
        type_prices='Wholesale',
    )
    test_session.add_all([provider, customer])
    await test_session.flush()

    order = Order(
        provider_id=provider.id,
        customer_id=customer.id,
        source_type=ORDER_TRACKING_SOURCE.DRAGONZAP_SEARCH.value,
        status=TYPE_STATUS_ORDER.ORDERED,
    )
    test_session.add(order)
    await test_session.flush()

    item = OrderItem(
        order_id=order.id,
        oem_number='OEM555',
        brand_name='TEST',
        autopart_name='Part',
        quantity=2,
        price=50,
        status=TYPE_ORDER_ITEM_STATUS.SENT,
    )
    test_session.add(item)
    await test_session.commit()
    await test_session.refresh(order)
    await test_session.refresh(item)

    result = await update_tracking_item(
        test_session,
        source_type='site',
        item_id=item.id,
        status='SHIPPED',
        received_quantity=2,
    )

    await test_session.refresh(order)
    await test_session.refresh(item)
    assert result['status'] == 'SHIPPED'
    assert order.status == TYPE_STATUS_ORDER.SHIPPED
    assert item.status == TYPE_ORDER_ITEM_STATUS.DELIVERED
    assert item.received_quantity == 2
    assert item.received_at is not None


@pytest.mark.asyncio
async def test_cleanup_old_tracking_history_keeps_recent_and_customer_flow(
    test_session,
):
    provider = Provider(
        name='Provider Three',
        email_contact='provider3@example.com',
        email_incoming_price='prices3@example.com',
        type_prices='Wholesale',
    )
    customer = Customer(
        name='Customer Three',
        email_contact='customer3@example.com',
        email_outgoing_price='out3@example.com',
        type_prices='Wholesale',
    )
    test_session.add_all([provider, customer])
    await test_session.flush()

    old_time = now_moscow() - timedelta(days=400)

    old_site_order = Order(
        provider_id=provider.id,
        customer_id=customer.id,
        source_type=ORDER_TRACKING_SOURCE.DRAGONZAP_SEARCH.value,
        created_at=old_time,
        status=TYPE_STATUS_ORDER.ORDERED,
    )
    old_supplier_order = SupplierOrder(
        provider_id=provider.id,
        source_type=ORDER_TRACKING_SOURCE.SEARCH_OFFERS.value,
        created_at=old_time,
        status=SUPPLIER_ORDER_STATUS.SENT,
    )
    keep_supplier_order = SupplierOrder(
        provider_id=provider.id,
        source_type=ORDER_TRACKING_SOURCE.CUSTOMER_ORDER.value,
        created_at=old_time,
        status=SUPPLIER_ORDER_STATUS.SENT,
    )
    test_session.add_all(
        [old_site_order, old_supplier_order, keep_supplier_order]
    )
    await test_session.flush()
    test_session.add_all(
        [
            OrderItem(
                order_id=old_site_order.id,
                oem_number='OLD1',
                brand_name='TEST',
                autopart_name='Old site',
                quantity=1,
                price=10,
                status=TYPE_ORDER_ITEM_STATUS.SENT,
            ),
            SupplierOrderItem(
                supplier_order_id=old_supplier_order.id,
                oem_number='OLD2',
                brand_name='TEST',
                autopart_name='Old supplier',
                quantity=1,
                price=10,
            ),
            SupplierOrderItem(
                supplier_order_id=keep_supplier_order.id,
                oem_number='KEEP',
                brand_name='TEST',
                autopart_name='Keep supplier',
                quantity=1,
                price=10,
            ),
        ]
    )
    await test_session.commit()

    summary = await cleanup_old_tracking_history(test_session)

    assert summary['orders_deleted'] == 1
    assert summary['supplier_orders_deleted'] == 1

    remaining_rows = await list_tracking_history(test_session, limit=50)
    assert remaining_rows == []


@pytest.mark.asyncio
async def test_sync_site_tracking_statuses_updates_order_from_site(
    test_session,
    monkeypatch,
):
    provider = Provider(
        name='Provider Four',
        email_contact='provider4@example.com',
        email_incoming_price='prices4@example.com',
        type_prices='Wholesale',
    )
    customer = Customer(
        name='Customer Four',
        email_contact='customer4@example.com',
        email_outgoing_price='out4@example.com',
        type_prices='Wholesale',
    )
    test_session.add_all([provider, customer])
    await test_session.flush()

    order = Order(
        provider_id=provider.id,
        customer_id=customer.id,
        source_type=ORDER_TRACKING_SOURCE.DRAGONZAP_SEARCH.value,
        status=TYPE_STATUS_ORDER.ORDERED,
    )
    test_session.add(order)
    await test_session.flush()

    item = OrderItem(
        order_id=order.id,
        oem_number='OEMSYNC',
        brand_name='TEST',
        autopart_name='Part sync',
        quantity=2,
        price=55,
        tracking_uuid='site-sync-uuid',
        status=TYPE_ORDER_ITEM_STATUS.SENT,
    )
    test_session.add(item)
    await test_session.commit()

    class FakeDZSiteClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return None

        async def get_order_items(self, **kwargs):
            assert kwargs['search_comment_eq'] == 'site-sync-uuid'
            return {
                'data': [
                    {
                        'comment': 'site-sync-uuid',
                        'status_code': 'arrived',
                    }
                ]
            }

    monkeypatch.setattr(
        'dz_fastapi.services.placed_orders.SITE_API_KEY',
        'test-key',
    )
    monkeypatch.setattr(
        'dz_fastapi.services.placed_orders.DZSiteClient',
        FakeDZSiteClient,
    )

    summary = await sync_site_tracking_statuses(
        test_session,
        oem_number='OEMSYNC',
    )

    await test_session.refresh(order)
    await test_session.refresh(item)
    assert summary['checked'] == 1
    assert summary['updated'] == 1
    assert order.status == TYPE_STATUS_ORDER.ARRIVED
    assert item.status == TYPE_ORDER_ITEM_STATUS.IN_PROGRESS
    assert item.received_quantity == 2
    assert item.received_at is not None
