import pytest

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


async def _login(async_client, email: str):
    response = await async_client.post(
        '/auth/login',
        json={'email': email, 'password': 'secret123'},
    )
    assert response.status_code == 200


@pytest.mark.asyncio
async def test_customer_order_config_crud(
    async_client, test_session, created_customers
):
    await _create_user(test_session, 'manager@example.com', UserRole.MANAGER)
    await _login(async_client, 'manager@example.com')

    customer = created_customers[0]
    payload = {
        'customer_id': customer.id,
        'order_email': 'orders@client.com',
        'oem_col': 0,
        'brand_col': 1,
        'qty_col': 2,
        'price_tolerance_pct': 2,
        'price_warning_pct': 5,
    }

    response = await async_client.post('/customer-orders/config', json=payload)
    assert response.status_code == 201
    config = response.json()
    assert config['customer_id'] == customer.id

    response = await async_client.get(
        f'/customer-orders/config/{customer.id}'
    )
    assert response.status_code == 200

    response = await async_client.put(
        f'/customer-orders/config/{customer.id}',
        json={'order_subject_pattern': 'test'},
    )
    assert response.status_code == 200
    assert response.json()['order_subject_pattern'] == 'test'
