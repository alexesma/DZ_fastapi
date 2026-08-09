from __future__ import annotations

import pytest
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.models.autopart import AutoPart, StorageLocation
from dz_fastapi.models.brand import Brand
from dz_fastapi.models.inventory import (
    LotSourceType,
    MovementType,
    StockLot,
    StockLotRole,
    StockLotRoleChange,
    StockLotRoleSource,
)
from dz_fastapi.models.user import User, UserRole, UserStatus
from dz_fastapi.services.auth import get_password_hash
from dz_fastapi.services.inventory_stock import (
    _apply_stock_delta,
    _create_stock_lot,
    transfer_stock_with_lot_trace,
)


@pytest.mark.asyncio
async def test_new_lot_role_is_inferred_from_autopart_brand(
    test_session: AsyncSession,
    created_autopart: AutoPart,
    created_storage: StorageLocation,
):
    dragonzap_brand = Brand(name="DragonZap")
    test_session.add(dragonzap_brand)
    await test_session.flush()
    dragonzap_part = AutoPart(
        brand_id=dragonzap_brand.id,
        oem_number="DZ-ROLE-1",
        name="Готовая позиция DragonZap",
        barcode="DZ-ROLE-1",
    )
    test_session.add(dragonzap_part)
    await test_session.flush()

    original_lot = await _create_stock_lot(
        test_session,
        autopart_id=created_autopart.id,
        storage_location_id=created_storage.id,
        quantity=2,
        source_type=LotSourceType.MANUAL,
    )
    finished_lot = await _create_stock_lot(
        test_session,
        autopart_id=dragonzap_part.id,
        storage_location_id=created_storage.id,
        quantity=3,
        source_type=LotSourceType.MANUAL,
    )

    assert original_lot.inventory_role == StockLotRole.ORIGINAL_GOOD
    assert finished_lot.inventory_role == StockLotRole.DRAGONZAP_FINISHED
    assert finished_lot.role_source == StockLotRoleSource.SYSTEM_DEFAULT

    changes = (
        (
            await test_session.execute(
                select(StockLotRoleChange).order_by(StockLotRoleChange.stock_lot_id)
            )
        )
        .scalars()
        .all()
    )
    assert len(changes) == 2
    assert all(change.old_role is None for change in changes)
    assert [change.new_role for change in changes] == [
        StockLotRole.ORIGINAL_GOOD,
        StockLotRole.DRAGONZAP_FINISHED,
    ]


@pytest.mark.asyncio
async def test_admin_can_change_lot_role_with_audit_history(
    async_client: AsyncClient,
    test_session: AsyncSession,
    created_autopart: AutoPart,
    created_storage: StorageLocation,
):
    test_session.add(
        User(
            id=1,
            name="Test Admin",
            email="test-admin@example.com",
            password_hash="not-used",
            role=UserRole.ADMIN,
            status=UserStatus.ACTIVE,
        )
    )
    await test_session.commit()

    movement_response = await async_client.post(
        "/inventory/movements/",
        json={
            "autopart_id": created_autopart.id,
            "storage_location_id": created_storage.id,
            "movement_type": "manual",
            "quantity": 5,
        },
    )
    assert movement_response.status_code == 201, movement_response.text
    lot_id = movement_response.json()["stock_lot_id"]

    update_response = await async_client.patch(
        f"/inventory/lots/{lot_id}/role",
        json={
            "inventory_role": "dragonzap_material",
            "reason": "Партия предназначена для переупаковки",
        },
    )
    assert update_response.status_code == 200, update_response.text
    updated = update_response.json()
    assert updated["inventory_role"] == "dragonzap_material"
    assert updated["role_source"] == "manual"
    assert updated["role_changed_by_name"] == "Test Admin"
    assert updated["autopart_oem"] == created_autopart.oem_number

    history_response = await async_client.get(f"/inventory/lots/{lot_id}/role-history")
    assert history_response.status_code == 200, history_response.text
    history = history_response.json()
    assert len(history) == 2
    assert history[0]["old_role"] == "original_good"
    assert history[0]["new_role"] == "dragonzap_material"
    assert history[0]["reason"] == "Партия предназначена для переупаковки"
    assert history[1]["old_role"] is None


@pytest.mark.asyncio
async def test_transfer_preserves_material_role(
    test_session: AsyncSession,
    created_autopart: AutoPart,
    created_storage: StorageLocation,
):
    destination = StorageLocation(name="ROLE DESTINATION")
    test_session.add(destination)
    await test_session.flush()
    source_lot = await _create_stock_lot(
        test_session,
        autopart_id=created_autopart.id,
        storage_location_id=created_storage.id,
        quantity=4,
        source_type=LotSourceType.MANUAL,
        inventory_role=StockLotRole.DRAGONZAP_MATERIAL,
        role_source=StockLotRoleSource.MANUAL,
        role_change_reason="Материал определён до перемещения",
    )
    await _apply_stock_delta(
        test_session,
        autopart_id=created_autopart.id,
        storage_location_id=created_storage.id,
        quantity_delta=4,
        movement_type=MovementType.MANUAL,
        stock_lot_id=source_lot.id,
    )

    await transfer_stock_with_lot_trace(
        test_session,
        autopart_id=created_autopart.id,
        from_location_id=created_storage.id,
        to_location_id=destination.id,
        quantity=2,
    )

    destination_lot = (
        await test_session.execute(
            select(StockLot).where(
                StockLot.storage_location_id == destination.id
            )
        )
    ).scalar_one()
    assert destination_lot.inventory_role == StockLotRole.DRAGONZAP_MATERIAL
    assert destination_lot.role_source == StockLotRoleSource.MANUAL
    assert destination_lot.role_change_reason == (
        f"Роль унаследована при перемещении партии #{source_lot.id}"
    )


@pytest.mark.no_auth_override
@pytest.mark.asyncio
async def test_manager_cannot_change_lot_role(
    async_client: AsyncClient,
    test_session: AsyncSession,
    created_autopart: AutoPart,
    created_storage: StorageLocation,
):
    manager = User(
        email="stock-manager@example.com",
        password_hash=get_password_hash("secret123"),
        role=UserRole.MANAGER,
        status=UserStatus.ACTIVE,
    )
    test_session.add(manager)
    lot = await _create_stock_lot(
        test_session,
        autopart_id=created_autopart.id,
        storage_location_id=created_storage.id,
        quantity=1,
        source_type=LotSourceType.MANUAL,
    )
    await test_session.commit()

    login_response = await async_client.post(
        "/auth/login",
        json={"email": manager.email, "password": "secret123"},
    )
    assert login_response.status_code == 200, login_response.text
    response = await async_client.patch(
        f"/inventory/lots/{lot.id}/role",
        json={
            "inventory_role": "dragonzap_material",
            "reason": "Попытка менеджера",
        },
    )
    assert response.status_code == 403
