import pytest

from dz_fastapi.models.user import User, UserRole, UserStatus
from dz_fastapi.services.auth import get_password_hash


@pytest.mark.asyncio
async def test_saved_print_form_is_returned_fresh_and_uncached(
    async_client, test_session
):
    admin = User(
        email="print-forms-admin@example.com",
        password_hash=get_password_hash("secret123"),
        role=UserRole.ADMIN,
        status=UserStatus.ACTIVE,
    )
    test_session.add(admin)
    await test_session.commit()
    login = await async_client.post(
        "/auth/login", json={"email": admin.email, "password": "secret123"}
    )
    assert login.status_code == 200

    def layout(x):
        return {
            "width_mm": 58, "height_mm": 40,
            "blocks": [{"id": "oem", "kind": "text", "field": "oem",
                        "x": x, "y": 2, "width": 40, "height": 6}],
        }

    for x in (3, 9):
        saved = await async_client.put("/print-forms/own_stock", json=layout(x))
        assert saved.status_code == 200, saved.text
        listed = await async_client.get("/print-forms")
        assert listed.headers["cache-control"] == "no-store"
        row = next(r for r in listed.json() if r["key"] == "own_stock")
        assert row["blocks"][0]["x"] == x
