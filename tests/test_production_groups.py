from __future__ import annotations

from decimal import Decimal

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.models.autopart import AutoPart, StorageLocation
from dz_fastapi.models.brand import Brand
from dz_fastapi.models.cross import AutoPartCross
from dz_fastapi.models.inventory import LotSourceType, StockLot, StockLotRole
from dz_fastapi.models.user import User, UserRole, UserStatus


async def _production_catalog(session: AsyncSession):
    dragonzap = Brand(name="DRAGONZAP")
    toyota = Brand(name="TOYOTA")
    geely = Brand(name="GEELY")
    session.add_all([dragonzap, toyota, geely])
    await session.flush()

    finished = AutoPart(
        brand_id=dragonzap.id,
        oem_number="DZ123",
        name="Готовая позиция DragonZap",
    )
    direct_material = AutoPart(
        brand_id=toyota.id,
        oem_number="123",
        name="Материал Toyota",
    )
    indirect_material = AutoPart(
        brand_id=geely.id,
        oem_number="G123",
        name="Материал Geely",
    )
    session.add_all([finished, direct_material, indirect_material])
    await session.flush()
    direct_cross = AutoPartCross(
        source_autopart_id=finished.id,
        cross_brand_id=toyota.id,
        cross_oem_number=direct_material.oem_number,
        cross_autopart_id=direct_material.id,
        priority=15,
    )
    indirect_cross = AutoPartCross(
        source_autopart_id=direct_material.id,
        cross_brand_id=geely.id,
        cross_oem_number=indirect_material.oem_number,
        cross_autopart_id=indirect_material.id,
        priority=30,
    )
    session.add_all([direct_cross, indirect_cross])
    await session.commit()
    return finished, direct_material, indirect_material, direct_cross


@pytest.mark.asyncio
async def test_production_group_is_derived_from_crosses_and_material_lots(
    async_client: AsyncClient,
    test_session: AsyncSession,
):
    finished, direct_material, indirect_material, _ = await _production_catalog(
        test_session
    )
    location = StorageLocation(name="PRODUCTION A")
    test_session.add(location)
    await test_session.flush()
    test_session.add_all(
        [
            StockLot(
                autopart_id=direct_material.id,
                storage_location_id=location.id,
                source_type=LotSourceType.RECEIPT,
                inventory_role=StockLotRole.DRAGONZAP_MATERIAL,
                initial_quantity=7,
                remaining_quantity=7,
            ),
            StockLot(
                autopart_id=direct_material.id,
                storage_location_id=location.id,
                source_type=LotSourceType.RECEIPT,
                inventory_role=StockLotRole.ORIGINAL_GOOD,
                initial_quantity=100,
                remaining_quantity=100,
            ),
            StockLot(
                autopart_id=indirect_material.id,
                storage_location_id=location.id,
                source_type=LotSourceType.RECEIPT,
                inventory_role=StockLotRole.DRAGONZAP_MATERIAL,
                initial_quantity=4,
                remaining_quantity=4,
            ),
        ]
    )
    await test_session.commit()

    response = await async_client.get("/inventory/production-groups")
    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["total"] == 1
    assert payload["synced_groups"] == 1
    group = payload["items"][0]
    assert group["finished_autopart_id"] == finished.id
    assert group["candidates_count"] == 2
    assert group["available_material_quantity"] == 11

    by_id = {row["autopart_id"]: row for row in group["materials"]}
    assert by_id[direct_material.id]["priority"] == 15
    assert by_id[direct_material.id]["available_material_quantity"] == 7
    assert by_id[indirect_material.id]["priority"] == 100
    assert by_id[indirect_material.id]["available_material_quantity"] == 4


@pytest.mark.asyncio
async def test_material_override_changes_priority_and_available_total(
    async_client: AsyncClient,
    test_session: AsyncSession,
):
    test_session.add(
        User(
            id=1,
            name="Production Admin",
            email="production-admin@example.com",
            password_hash="not-used",
            role=UserRole.ADMIN,
            status=UserStatus.ACTIVE,
        )
    )
    _, direct_material, _, _ = await _production_catalog(test_session)
    location = StorageLocation(name="PRODUCTION B")
    test_session.add(location)
    await test_session.flush()
    test_session.add(
        StockLot(
            autopart_id=direct_material.id,
            storage_location_id=location.id,
            source_type=LotSourceType.RECEIPT,
            inventory_role=StockLotRole.DRAGONZAP_MATERIAL,
            initial_quantity=9,
            remaining_quantity=9,
        )
    )
    await test_session.commit()

    listing = await async_client.get("/inventory/production-groups")
    group_id = listing.json()["items"][0]["id"]
    group_settings = await async_client.patch(
        f"/inventory/production-groups/{group_id}",
        json={
            "packaging_cost": 12.5,
            "packaging_description": "Коробка и этикетка 58x40",
            "notes": "Тестовая настройка выпуска",
        },
    )
    assert group_settings.status_code == 200, group_settings.text
    assert Decimal(str(group_settings.json()["packaging_cost"])) == Decimal("12.5")
    assert group_settings.json()["updated_by_name"] == "Production Admin"

    response = await async_client.put(
        f"/inventory/production-groups/{group_id}/materials/{direct_material.id}",
        json={
            "priority": 5,
            "is_allowed": False,
            "reason": "Этот источник поставляется как оригинальный товар",
        },
    )
    assert response.status_code == 200, response.text
    group = response.json()
    material = next(
        row for row in group["materials"] if row["autopart_id"] == direct_material.id
    )
    assert material["priority"] == 5
    assert material["is_allowed"] is False
    assert material["has_override"] is True
    assert material["updated_by_name"] == "Production Admin"
    assert group["available_material_quantity"] == 0


@pytest.mark.asyncio
async def test_removed_cross_cannot_be_restored_by_saved_override(
    async_client: AsyncClient,
    test_session: AsyncSession,
):
    test_session.add(
        User(
            id=1,
            name="Production Admin",
            email="production-admin@example.com",
            password_hash="not-used",
            role=UserRole.ADMIN,
            status=UserStatus.ACTIVE,
        )
    )
    _, direct_material, _, direct_cross = await _production_catalog(test_session)
    listing = await async_client.get("/inventory/production-groups")
    group_id = listing.json()["items"][0]["id"]
    configured = await async_client.put(
        f"/inventory/production-groups/{group_id}/materials/{direct_material.id}",
        json={"priority": 1, "is_allowed": True, "reason": "Приоритетная партия"},
    )
    assert configured.status_code == 200, configured.text

    await test_session.delete(direct_cross)
    await test_session.commit()
    response = await async_client.get(f"/inventory/production-groups/{group_id}")
    assert response.status_code == 200, response.text
    material_ids = {row["autopart_id"] for row in response.json()["materials"]}
    assert direct_material.id not in material_ids

    stale_update = await async_client.put(
        f"/inventory/production-groups/{group_id}/materials/{direct_material.id}",
        json={"priority": 1, "is_allowed": True},
    )
    assert stale_update.status_code == 400


@pytest.mark.asyncio
async def test_one_way_cross_does_not_create_production_group(
    async_client: AsyncClient,
    test_session: AsyncSession,
):
    _, _, _, direct_cross = await _production_catalog(test_session)
    direct_cross.is_bidirectional = False
    await test_session.commit()

    response = await async_client.get("/inventory/production-groups")
    assert response.status_code == 200, response.text
    assert response.json()["total"] == 0
    assert response.json()["items"] == []
