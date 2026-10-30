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
    assert code_keys("1310175200  A") == ["1310175200A", "1310175200"]
    assert code_keys("12\t") == []
    assert code_keys("A\x1dB5678-9012")[-1] == "B56789012"


def test_code_keys_split_semicolon_label_and_keep_marking_code_first():
    from dz_fastapi.services.scan_codes import code_keys

    keys = code_keys("00HB;;75722-TL0-G01;1;HB5M19;CHINA;CN;156;;;;")
    assert keys.index("75722TL0G01") < keys.index("HB5M19")
    assert "1" not in keys and "CN" not in keys and "156" not in keys

    marking = "0104610014828864215hnn7i3eekru3\x1d91ee1o\x1d92abcd;efgh12="
    keys = code_keys(marking)
    assert keys[0].startswith("010461001482886421")


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


@pytest.mark.asyncio
async def test_scan_resolve_location(async_client, test_session, created_storage):
    user = User(
        email="scan-loc@example.com",
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
        "/scan/resolve", params={"code": created_storage.name.lower()}
    )
    assert found.status_code == 200, found.text
    assert found.json()["kind"] == "location"
    assert found.json()["storage_location_id"] == created_storage.id


@pytest.mark.asyncio
async def test_scan_log_and_catalog(async_client, test_session, created_autopart,
                                    created_storage):
    admin = User(
        email="scan-admin@example.com",
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

    await async_client.get("/scan/resolve",
                           params={"code": created_autopart.barcode})
    await async_client.get("/scan/resolve", params={"code": "MISS-1234"})
    sent = await async_client.post("/scan/log", json={"events": [
        {"code": "LOCAL-1", "found": True, "kind": "autopart"}]})
    assert sent.status_code == 204
    log = await async_client.get("/scan/log")
    assert log.status_code == 200, log.text
    codes = {row["code"]: row for row in log.json()}
    assert codes[created_autopart.barcode]["found"] is True
    assert codes["MISS-1234"]["found"] is False
    assert codes["LOCAL-1"]["source"] == "local"
    missed = await async_client.get("/scan/log", params={"only_missed": True})
    assert [row["code"] for row in missed.json()] == ["MISS-1234"]

    catalog = await async_client.get("/scan/catalog")
    assert catalog.status_code == 200, catalog.text
    ids = [row[0] for row in catalog.json()["parts"]]
    assert created_autopart.id in ids
    assert [created_storage.id, created_storage.name] in catalog.json()["locations"]
