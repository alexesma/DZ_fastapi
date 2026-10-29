import pytest

from dz_fastapi.models.user import User, UserRole, UserStatus
from dz_fastapi.services.auth import get_password_hash


@pytest.mark.asyncio
async def test_scan_resolve_autopart_and_unknown(
    async_client, test_session, created_autopart,
):
    user = User(
        email="scan-user@example.com",
        password_hash=get_password_hash("secret123"),
        role=UserRole.MANAGER,
        status=UserStatus.ACTIVE,
    )
    test_session.add(user)
    await test_session.commit()
    login = await async_client.post(
        "/auth/login", json={"email": user.email, "password": "secret123"}
    )
    assert login.status_code == 200

    found = await async_client.get(
        "/scan/resolve", params={"code": created_autopart.barcode}
    )
    assert found.status_code == 200, found.text
    assert found.json()["kind"] == "autopart"
    assert found.json()["autopart_id"] == created_autopart.id

    missing = await async_client.get("/scan/resolve", params={"code": "DZ-NOPE"})
    assert missing.status_code == 404

    fake_box = await async_client.get(
        "/scan/resolve", params={"code": "DZ-BOX-999999-1"}
    )
    assert fake_box.status_code == 404
