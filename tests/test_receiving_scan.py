import pytest
from sqlalchemy import select

from dz_fastapi.models.inventory import StockByLocation
from dz_fastapi.models.partner import SupplierReceipt, SupplierReceiptItem
from dz_fastapi.models.user import User, UserRole, UserStatus
from dz_fastapi.services.auth import get_password_hash


@pytest.mark.asyncio
async def test_receiving_count_and_place(
    async_client, test_session, created_providers, created_autopart,
    created_storage,
):
    user = User(
        email="receiving@example.com",
        password_hash=get_password_hash("secret123"),
        role=UserRole.MANAGER,
        status=UserStatus.ACTIVE,
    )
    receipt = SupplierReceipt(provider_id=created_providers[0].id,
                              document_number="UPD-1")
    test_session.add_all([user, receipt])
    await test_session.flush()
    test_session.add(SupplierReceiptItem(
        receipt_id=receipt.id, autopart_id=created_autopart.id,
        received_quantity=3,
    ))
    await test_session.commit()
    login = await async_client.post(
        "/auth/login", json={"email": user.email, "password": "secret123"}
    )
    assert login.status_code == 200

    plan = await async_client.get(f"/receiving-scan/receipts/{receipt.id}")
    assert plan.status_code == 200, plan.text
    assert plan.json()["lines"][0]["document_qty"] == 3
    assert plan.json()["posted"] is False

    unposted = await async_client.post(
        f"/receiving-scan/receipts/{receipt.id}/place",
        json={"autopart_id": created_autopart.id,
              "storage_location_id": created_storage.id, "quantity": 1},
    )
    assert unposted.status_code == 409

    first = await async_client.post(
        f"/receiving-scan/receipts/{receipt.id}/count",
        json={"autopart_id": created_autopart.id, "quantity": 2},
    )
    assert first.status_code == 200, first.text
    second = await async_client.post(
        f"/receiving-scan/receipts/{receipt.id}/count",
        json={"autopart_id": created_autopart.id, "quantity": 1},
    )
    assert second.json()["accepted_qty"] == 3
    missing = await async_client.post(
        f"/receiving-scan/receipts/{receipt.id}/count",
        json={"autopart_id": 999999, "quantity": 1},
    )
    assert missing.status_code == 404

    posted = await async_client.post(
        f"/customer-orders/supplier-receipts/{receipt.id}/post"
    )
    assert posted.status_code == 200, posted.text
    too_many = await async_client.post(
        f"/receiving-scan/receipts/{receipt.id}/place",
        json={"autopart_id": created_autopart.id,
              "storage_location_id": created_storage.id, "quantity": 9},
    )
    assert too_many.status_code == 409
    placed = await async_client.post(
        f"/receiving-scan/receipts/{receipt.id}/place",
        json={"autopart_id": created_autopart.id,
              "storage_location_id": created_storage.id, "quantity": 2},
    )
    assert placed.status_code == 200, placed.text
    assert placed.json()["line"]["placed_qty"] == 2
    rows = (await test_session.execute(
        select(StockByLocation).where(
            StockByLocation.storage_location_id == created_storage.id,
            StockByLocation.autopart_id == created_autopart.id)
    )).scalars().all()
    assert sum(row.quantity for row in rows) == 2
