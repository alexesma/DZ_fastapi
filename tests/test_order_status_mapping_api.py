import pytest

from dz_fastapi.api.deps import get_current_user
from dz_fastapi.main import app
from dz_fastapi.models.order_status_mapping import ExternalStatusUnmapped
from dz_fastapi.models.partner import (ORDER_TRACKING_SOURCE,
                                       TYPE_ORDER_ITEM_STATUS,
                                       TYPE_STATUS_ORDER, Customer, Order,
                                       OrderItem, Provider)
from dz_fastapi.models.user import User, UserRole, UserStatus
from dz_fastapi.services.auth import get_password_hash
from dz_fastapi.services.order_status_mapping import \
    EXTERNAL_STATUS_SOURCE_DRAGONZAP


async def _create_admin(test_session):
    user = User(
        email='status-admin@example.com',
        password_hash=get_password_hash('secret123'),
        role=UserRole.ADMIN,
        status=UserStatus.ACTIVE,
    )
    test_session.add(user)
    await test_session.commit()
    await test_session.refresh(user)
    return user


@pytest.mark.asyncio
async def test_create_mapping_applies_to_existing_tracking_items(
    async_client,
    test_session,
):
    admin = await _create_admin(test_session)

    async def override_current_user():
        return admin

    app.dependency_overrides[get_current_user] = override_current_user

    provider = Provider(
        name='Mapping Provider',
        email_contact='mapping-provider@example.com',
        email_incoming_price='mapping-prices@example.com',
        type_prices='Wholesale',
    )
    customer = Customer(
        name='Mapping Customer',
        email_contact='mapping-customer@example.com',
        email_outgoing_price='mapping-out@example.com',
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
        oem_number='OEM-MAP-1',
        brand_name='TEST',
        autopart_name='Mapped later',
        quantity=1,
        price=25,
        tracking_uuid='map-later-uuid',
        status=TYPE_ORDER_ITEM_STATUS.SENT,
        external_status_source=EXTERNAL_STATUS_SOURCE_DRAGONZAP,
        external_status_raw='Manual Review',
        external_status_normalized='manual review',
    )
    await test_session.flush()

    unresolved = ExternalStatusUnmapped(
        source_key=EXTERNAL_STATUS_SOURCE_DRAGONZAP,
        provider_id=provider.id,
        raw_status='Manual Review',
        normalized_status='manual review',
        seen_count=1,
        sample_order_id=order.id,
        sample_item_id=item.id,
        is_resolved=False,
    )
    test_session.add_all([item, unresolved])
    await test_session.commit()

    response = await async_client.post(
        '/admin/order-status-mappings',
        json={
            'source_key': EXTERNAL_STATUS_SOURCE_DRAGONZAP,
            'provider_id': provider.id,
            'raw_status': 'manual review',
            'match_mode': 'CONTAINS',
            'internal_order_status': 'PROCESSING',
            'internal_item_status': 'IN_PROGRESS',
            'priority': 10,
            'is_active': True,
            'apply_existing': True,
        },
    )

    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload['source_key'] == EXTERNAL_STATUS_SOURCE_DRAGONZAP
    assert payload['provider_id'] == provider.id
    assert payload['internal_order_status'] == 'PROCESSING'
    assert payload['internal_item_status'] == 'IN_PROGRESS'

    await test_session.refresh(order)
    await test_session.refresh(item)
    await test_session.refresh(unresolved)

    assert order.status == TYPE_STATUS_ORDER.PROCESSING
    assert item.status == TYPE_ORDER_ITEM_STATUS.IN_PROGRESS
    assert item.external_status_mapping_id == payload['id']
    assert unresolved.is_resolved is True
    assert unresolved.mapping_id == payload['id']


@pytest.mark.asyncio
async def test_list_unmapped_statuses_returns_only_unresolved(
    async_client,
    test_session,
):
    admin = await _create_admin(test_session)

    async def override_current_user():
        return admin

    app.dependency_overrides[get_current_user] = override_current_user

    unresolved = ExternalStatusUnmapped(
        source_key=EXTERNAL_STATUS_SOURCE_DRAGONZAP,
        raw_status='Manual Review',
        normalized_status='manual review',
        seen_count=2,
        is_resolved=False,
    )
    resolved = ExternalStatusUnmapped(
        source_key=EXTERNAL_STATUS_SOURCE_DRAGONZAP,
        raw_status='Done',
        normalized_status='done',
        seen_count=1,
        is_resolved=True,
    )
    test_session.add_all([unresolved, resolved])
    await test_session.commit()

    response = await async_client.get(
        '/admin/order-status-mappings/unmapped'
    )

    assert response.status_code == 200, response.text
    payload = response.json()
    assert len(payload) == 1
    assert payload[0]['raw_status'] == 'Manual Review'
