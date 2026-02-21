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
async def test_email_accounts_requires_admin(async_client, test_session):
    await _create_user(test_session, 'manager@example.com', UserRole.MANAGER)
    await _login(async_client, 'manager@example.com')
    response = await async_client.get('/email-accounts/')
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_email_accounts_crud(async_client, test_session):
    await _create_user(test_session, 'admin@example.com', UserRole.ADMIN)
    await _login(async_client, 'admin@example.com')

    payload = {
        'name': 'Orders Inbox',
        'email': 'orders@example.com',
        'password': 'pass123',
        'imap_host': 'imap.example.com',
        'purposes': ['orders_in'],
        'is_active': True,
    }
    response = await async_client.post('/email-accounts/', json=payload)
    assert response.status_code == 201
    account = response.json()
    account_id = account['id']

    response = await async_client.get('/email-accounts/')
    assert response.status_code == 200
    assert any(item['id'] == account_id for item in response.json())

    response = await async_client.patch(
        f'/email-accounts/{account_id}',
        json={
            'name': 'Orders Inbox 2', 'purposes': ['orders_in', 'orders_out']
        },
    )
    assert response.status_code == 200
    assert response.json()['name'] == 'Orders Inbox 2'

    response = await async_client.delete(f'/email-accounts/{account_id}')
    assert response.status_code == 204
