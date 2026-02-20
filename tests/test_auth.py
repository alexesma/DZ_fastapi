import pytest

from dz_fastapi.models.user import User, UserRole, UserStatus
from dz_fastapi.services.auth import get_password_hash


async def _create_user(
    session,
    email: str,
    password: str,
    role: UserRole = UserRole.MANAGER,
    status: UserStatus = UserStatus.PENDING,
):
    user = User(
        email=email.lower().strip(),
        password_hash=get_password_hash(password),
        role=role,
        status=status,
    )
    session.add(user)
    await session.commit()
    await session.refresh(user)
    return user


@pytest.mark.asyncio
async def test_register_success(async_client, test_session):
    payload = {"email": "newuser@example.com", "password": "secret123"}
    response = await async_client.post("/auth/register", json=payload)
    assert response.status_code == 200
    data = response.json()
    assert data["email"] == payload["email"]
    assert data["status"] == "pending"

    # duplicate
    response = await async_client.post("/auth/register", json=payload)
    assert response.status_code == 400


@pytest.mark.asyncio
async def test_register_invalid_password(async_client):
    payload = {"email": "shortpass@example.com", "password": "123"}
    response = await async_client.post("/auth/register", json=payload)
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_login_pending_denied(async_client, test_session):
    await _create_user(
        test_session,
        email="pending@example.com",
        password="secret123",
        status=UserStatus.PENDING,
    )
    response = await async_client.post(
        "/auth/login",
        json={"email": "pending@example.com", "password": "secret123"},
    )
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_login_invalid_credentials(async_client, test_session):
    await _create_user(
        test_session,
        email="active@example.com",
        password="secret123",
        status=UserStatus.ACTIVE,
    )
    response = await async_client.post(
        "/auth/login",
        json={"email": "active@example.com", "password": "wrong"},
    )
    assert response.status_code == 401

    response = await async_client.post(
        "/auth/login",
        json={"email": "missing@example.com", "password": "secret123"},
    )
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_login_me_logout_flow(async_client, test_session):
    await _create_user(
        test_session,
        email="active@example.com",
        password="secret123",
        status=UserStatus.ACTIVE,
    )
    response = await async_client.post(
        "/auth/login",
        json={"email": "active@example.com", "password": "secret123"},
    )
    assert response.status_code == 200

    response = await async_client.get("/auth/me")
    assert response.status_code == 200
    assert response.json()["email"] == "active@example.com"

    response = await async_client.post("/auth/logout")
    assert response.status_code == 200

    response = await async_client.get("/auth/me")
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_me_requires_auth(async_client):
    response = await async_client.get("/auth/me")
    assert response.status_code == 401

    async_client.cookies.set("access_token", "invalid-token")
    response = await async_client.get("/auth/me")
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_admin_endpoints_require_admin(async_client, test_session):
    await _create_user(
        test_session,
        email="admin@example.com",
        password="secret123",
        role=UserRole.ADMIN,
        status=UserStatus.ACTIVE,
    )
    await _create_user(
        test_session,
        email="manager@example.com",
        password="secret123",
        role=UserRole.MANAGER,
        status=UserStatus.ACTIVE,
    )

    # manager login
    await async_client.post(
        "/auth/login",
        json={"email": "manager@example.com", "password": "secret123"},
    )
    response = await async_client.get("/admin/users")
    assert response.status_code == 403

    # admin login
    async_client.cookies.clear()
    await async_client.post(
        "/auth/login",
        json={"email": "admin@example.com", "password": "secret123"},
    )
    response = await async_client.get("/admin/users")
    assert response.status_code == 200
    data = response.json()
    assert any(u["email"] == "admin@example.com" for u in data)


@pytest.mark.asyncio
async def test_admin_requires_auth(async_client):
    response = await async_client.get("/admin/users")
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_admin_approve_disable_and_role(async_client, test_session):
    await _create_user(
        test_session,
        email="admin@example.com",
        password="secret123",
        role=UserRole.ADMIN,
        status=UserStatus.ACTIVE,
    )
    pending = await _create_user(
        test_session,
        email="pending@example.com",
        password="secret123",
        role=UserRole.MANAGER,
        status=UserStatus.PENDING,
    )

    # login as admin
    await async_client.post(
        "/auth/login",
        json={"email": "admin@example.com", "password": "secret123"},
    )

    # list pending
    response = await async_client.get(
        "/admin/users",
        params={"status": "pending"},
    )
    assert response.status_code == 200
    assert any(u["id"] == pending.id for u in response.json())

    # approve
    response = await async_client.post(f"/admin/users/{pending.id}/approve")
    assert response.status_code == 200
    assert response.json()["status"] == "active"

    # role update
    response = await async_client.post(
        f"/admin/users/{pending.id}/role",
        json={"role": "admin"},
    )
    assert response.status_code == 200
    assert response.json()["role"] == "admin"

    # disable
    response = await async_client.post(f"/admin/users/{pending.id}/disable")
    assert response.status_code == 200
    assert response.json()["status"] == "disabled"


@pytest.mark.asyncio
async def test_disabled_user_login_denied(async_client, test_session):
    await _create_user(
        test_session,
        email="disabled@example.com",
        password="secret123",
        status=UserStatus.DISABLED,
    )
    response = await async_client.post(
        "/auth/login",
        json={"email": "disabled@example.com", "password": "secret123"},
    )
    assert response.status_code == 403
