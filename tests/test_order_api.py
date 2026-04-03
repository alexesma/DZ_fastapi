import pytest
from sqlalchemy import select

from dz_fastapi.api.deps import get_current_user
from dz_fastapi.main import app
from dz_fastapi.models.partner import Order, OrderItem, Provider
from dz_fastapi.models.user import User, UserRole, UserStatus
from dz_fastapi.services.auth import get_password_hash


async def _create_user(session, email: str, role: UserRole):
    user = User(
        email=email,
        password_hash=get_password_hash('secret123'),
        role=role,
        status=UserStatus.ACTIVE,
    )
    session.add(user)
    await session.commit()
    await session.refresh(user)
    return user


class _FakeDZSiteClient:
    def __init__(self, *args, **kwargs):
        self._comments = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def add_autopart_in_basket(
        self,
        *,
        comment,
        **kwargs,
    ):
        self._comments.append(comment)
        return True

    async def get_basket(self, api_key=None):
        return {
            'data': [{'comment': comment} for comment in self._comments]
        }

    async def order_basket(self, api_key=None, comment=None):
        return True


class _FailOrderDZSiteClient(_FakeDZSiteClient):
    clean_called = False

    async def order_basket(self, api_key=None, comment=None):
        return False

    async def clean_basket(self, api_key=None):
        type(self).clean_called = True
        self._comments = []
        return True


@pytest.mark.asyncio
async def test_send_api_resolves_supplier_by_name_when_id_external(
    async_client, test_session, created_customers, monkeypatch
):
    current_user = await _create_user(
        test_session,
        'orders-admin@example.com',
        UserRole.ADMIN,
    )

    async def override_current_user():
        return current_user

    app.dependency_overrides[get_current_user] = override_current_user

    monkeypatch.setattr(
        'dz_fastapi.api.order.DZSiteClient',
        _FakeDZSiteClient,
    )

    response = await async_client.post(
        f'/order/send_api?customer_id={created_customers[0].id}',
        json=[
            {
                'autopart_id': None,
                'oem_number': 'TEST-123',
                'brand_name': 'SITE',
                'autopart_name': 'Site item',
                'supplier_id': 9731061118,
                'supplier_name': 'Dragonzap Test Supplier',
                'quantity': 1,
                'confirmed_price': 100.0,
                'min_delivery_day': 1,
                'max_delivery_day': 2,
                'status': 'Send',
                'tracking_uuid': 'dragonzap:test:1',
                'hash_key': 'hash-123',
                'system_hash': 'system-123',
            }
        ],
    )

    assert response.status_code == 201, response.text
    payload = response.json()
    assert payload['successful_items'] == 1
    assert payload['failed_items'] == 0

    provider = (
        await test_session.execute(
            select(Provider).where(Provider.name == 'Dragonzap Test Supplier')
        )
    ).scalar_one()
    assert provider.is_virtual is True

    order = (
        await test_session.execute(
            select(Order).where(Order.provider_id == provider.id)
        )
    ).scalar_one()
    assert order.customer_id == created_customers[0].id


@pytest.mark.asyncio
async def test_send_api_normalizes_long_tracking_uuid(
    async_client, test_session, created_customers, monkeypatch
):
    current_user = await _create_user(
        test_session,
        'orders-tracking@example.com',
        UserRole.ADMIN,
    )

    async def override_current_user():
        return current_user

    app.dependency_overrides[get_current_user] = override_current_user

    monkeypatch.setattr(
        'dz_fastapi.api.order.DZSiteClient',
        _FakeDZSiteClient,
    )

    long_tracking_uuid = (
        'dragonzap:Ivers (http://ivers.ru/):'
        'PRV2|13840068|60c3c730-563c-47ff-a1d7-15612efb19d8-ITEM568|S1'
        ':9011610175'
    )
    response = await async_client.post(
        f'/order/send_api?customer_id={created_customers[0].id}',
        json=[
            {
                'autopart_id': None,
                'oem_number': '9011610175',
                'brand_name': 'TOYOTA-LEXUS',
                'autopart_name': 'Шпилька',
                'supplier_id': 9731061118,
                'supplier_name': 'Ivers (http://ivers.ru/)',
                'quantity': 10,
                'confirmed_price': 101.0,
                'min_delivery_day': 1,
                'max_delivery_day': 0,
                'status': 'Send',
                'tracking_uuid': long_tracking_uuid,
                'hash_key': (
                    'PRV2|13840068|60c3c730-563c-47ff-a1d7-15612efb19d8'
                    '-ITEM568|S1'
                ),
                'system_hash': (
                    'PRV2|13840068|60c3c730-563c-47ff-a1d7-15612efb19d8'
                    '-ITEM568|S1'
                ),
            }
        ],
    )

    assert response.status_code == 201, response.text
    payload = response.json()
    assert payload['successful_items'] == 1
    assert payload['results'][0]['request_tracking_uuid'] == long_tracking_uuid
    assert len(payload['results'][0]['tracking_uuid']) <= 36
    assert payload['results'][0]['tracking_uuid'] != long_tracking_uuid

    order_item = (
        await test_session.execute(select(OrderItem))
    ).scalar_one()
    assert len(order_item.tracking_uuid) <= 36
    assert order_item.tracking_uuid == payload['results'][0]['tracking_uuid']


@pytest.mark.asyncio
async def test_send_api_does_not_create_local_order_when_site_order_fails(
    async_client, test_session, created_customers, monkeypatch
):
    current_user = await _create_user(
        test_session,
        'orders-failed-site@example.com',
        UserRole.ADMIN,
    )

    async def override_current_user():
        return current_user

    app.dependency_overrides[get_current_user] = override_current_user
    _FailOrderDZSiteClient.clean_called = False

    monkeypatch.setattr(
        'dz_fastapi.api.order.DZSiteClient',
        _FailOrderDZSiteClient,
    )

    response = await async_client.post(
        f'/order/send_api?customer_id={created_customers[0].id}',
        json=[
            {
                'autopart_id': None,
                'oem_number': '9011610175',
                'brand_name': 'TOYOTA-LEXUS',
                'autopart_name': 'Шпилька',
                'supplier_id': 9731061118,
                'supplier_name': 'Ivers (http://ivers.ru/)',
                'quantity': 10,
                'confirmed_price': 101.0,
                'min_delivery_day': 1,
                'max_delivery_day': 0,
                'status': 'Send',
                'tracking_uuid': 'dragonzap:test-fail:9011610175',
                'hash_key': 'hash-site-fail',
                'system_hash': 'system-site-fail',
            }
        ],
    )

    assert response.status_code == 201, response.text
    payload = response.json()
    assert payload['successful_items'] == 0
    assert payload['failed_items'] == 1
    assert payload['order_id'] is None
    assert payload['results'][0]['status'] == 'error'
    assert _FailOrderDZSiteClient.clean_called is True

    order_count = (
        await test_session.execute(select(Order))
    ).scalars().all()
    assert order_count == []
