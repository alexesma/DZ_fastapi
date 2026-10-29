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


def test_code_keys_handle_original_part_formats():
    from dz_fastapi.services.scan_codes import code_keys

    assert code_keys("52245-SBA-A21") == ["52245SBAA21"]
    assert code_keys("52245SBAA21") == ["52245SBAA21"]
    keys = code_keys("90916-03107\t  3")
    assert "9091603107" in keys
    assert code_keys("90916-03107   X")[-1] == "9091603107"
    assert code_keys("12\t") == []
    assert code_keys("A\x1dB5678-9012")[-1] == "B56789012"


@pytest.mark.asyncio
async def test_scan_resolve_by_original_code_and_alias(
    async_client, test_session, created_autopart,
):
    user = User(
        email="scan-alias@example.com",
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
    oem = created_autopart.oem_number
    dashed = "-".join([oem[:3], oem[3:6], oem[6:]]) if len(oem) > 6 else oem
    by_oem = await async_client.get(
        "/scan/resolve", params={"code": f"{dashed}\t  7"}
    )
    assert by_oem.status_code == 200, by_oem.text
    assert by_oem.json()["autopart_id"] == created_autopart.id

    unknown = await async_client.get(
        "/scan/resolve", params={"code": "ZZ-UNKNOWN-ORIG-1"})
    assert unknown.status_code == 404
    bound = await async_client.post("/scan/alias", json={
        "code": "ZZ-UNKNOWN-ORIG-1", "autopart_id": created_autopart.id})
    assert bound.status_code == 201, bound.text
    again = await async_client.get(
        "/scan/resolve", params={"code": "ZZ-UNKNOWN-ORIG-1"})
    assert again.status_code == 200
    assert again.json()["autopart_id"] == created_autopart.id
