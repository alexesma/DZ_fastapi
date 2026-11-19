import pytest

from dz_fastapi.models.email_account import EmailAccount
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
@pytest.mark.no_auth_override
async def test_email_accounts_requires_admin(async_client, test_session):
    await _create_user(test_session, "manager@example.com", UserRole.MANAGER)
    await _login(async_client, "manager@example.com")
    response = await async_client.get("/email-accounts/")
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_email_accounts_crud(async_client, test_session):
    await _create_user(test_session, "admin@example.com", UserRole.ADMIN)
    await _login(async_client, "admin@example.com")

    payload = {
        "name": "Orders Inbox",
        "email": "orders@example.com",
        "password": "pass123",
        "imap_host": "imap.example.com",
        "smtp_host": "smtp.mail.selcloud.ru",
        "smtp_port": 1127,
        "smtp_username": "selectel-login",
        "smtp_via_relay": False,
        "purposes": ["orders_in"],
        "is_active": True,
    }
    response = await async_client.post("/email-accounts/", json=payload)
    assert response.status_code == 201
    account = response.json()
    account_id = account["id"]
    assert "password" not in account
    assert "resend_api_key" not in account
    assert account["has_password"] is True
    assert account["smtp_username"] == "selectel-login"
    assert account["smtp_via_relay"] is False

    response = await async_client.get("/email-accounts/")
    assert response.status_code == 200
    assert any(item["id"] == account_id for item in response.json())

    response = await async_client.patch(
        f"/email-accounts/{account_id}",
        json={"name": "Orders Inbox 2", "purposes": ["orders_in", "orders_out"]},
    )
    assert response.status_code == 200
    assert response.json()["name"] == "Orders Inbox 2"

    stored = await test_session.get(EmailAccount, account_id)
    assert stored.password == "pass123"

    response = await async_client.patch(
        f"/email-accounts/{account_id}",
        json={"password": "", "resend_api_key": ""},
    )
    assert response.status_code == 200
    await test_session.refresh(stored)
    assert stored.password == "pass123"

    response = await async_client.delete(f"/email-accounts/{account_id}")
    assert response.status_code == 204


@pytest.mark.asyncio
async def test_migrate_resend_outgoing_to_selectel_keeps_inbound(
    async_client,
    test_session,
):
    await _create_user(test_session, "selectel-admin@example.com", UserRole.ADMIN)
    await _login(async_client, "selectel-admin@example.com")
    create_response = await async_client.post(
        "/email-accounts/",
        json={
            "name": "Orders Resend",
            "email": "orders@dragonzap.online",
            "transport": "resend_api",
            "resend_api_key": "re_inbound",
            "purposes": ["orders_in", "orders_out"],
        },
    )
    assert create_response.status_code == 201
    account_id = create_response.json()["id"]

    response = await async_client.post(
        "/email-accounts/migrate-resend-to-selectel",
        json={
            "account_ids": [account_id],
            "smtp_username": "selectel-resource",
            "smtp_password": "selectel-secret",
        },
    )

    assert response.status_code == 200
    migrated = response.json()[0]
    assert migrated["transport"] == "resend_api"
    assert migrated["outbound_transport"] == "smtp"
    assert migrated["smtp_host"] == "smtp.mail.selcloud.ru"
    assert migrated["smtp_port"] == 1127
    assert migrated["smtp_use_ssl"] is True
    assert migrated["smtp_username"] == "selectel-resource"
    assert migrated["smtp_via_relay"] is False
    stored = await test_session.get(EmailAccount, account_id)
    await test_session.refresh(stored)
    assert stored.password == "selectel-secret"
    assert stored.resend_api_key == "re_inbound"
