import pytest

from dz_fastapi.models.user import User, UserStatus
from dz_fastapi.services.auth import get_password_hash

pytestmark = pytest.mark.no_auth_override

PRIVATE_ENDPOINTS = [
    ("GET", "/providers/"),
    ("GET", "/brand/"),
    ("GET", "/crosses/"),
    ("GET", "/finance/debtors"),
    ("GET", "/dashboard/order-dynamics"),
    ("GET", "/settings/price-check"),
    ("GET", "/email-accounts/"),
    ("GET", "/diadoc/status"),
    ("GET", "/order/debug/basket"),
    ("GET", "/watchlist"),
]


@pytest.mark.asyncio
@pytest.mark.parametrize(("method", "path"), PRIVATE_ENDPOINTS)
async def test_business_api_requires_auth_cookie(async_client, method, path):
    response = await async_client.request(method, path)
    assert response.status_code == 401, response.text


@pytest.mark.asyncio
async def test_health_and_auth_login_stay_public(async_client):
    health = await async_client.get("/health")
    assert health.status_code == 200

    login = await async_client.post(
        "/auth/login",
        json={"email": "missing@example.com", "password": "secret123"},
    )
    assert login.status_code == 401
    assert login.json()["detail"] == "Invalid credentials"


@pytest.mark.asyncio
async def test_authenticated_user_can_access_business_api(
    async_client,
    test_session,
):
    user = User(
        name="API User",
        email="api-user@example.com",
        password_hash=get_password_hash("secret123"),
        status=UserStatus.ACTIVE,
    )
    test_session.add(user)
    await test_session.commit()

    login = await async_client.post(
        "/auth/login",
        json={"email": "api-user@example.com", "password": "secret123"},
    )
    assert login.status_code == 200, login.text

    response = await async_client.get("/providers/")
    assert response.status_code != 401, response.text
