from datetime import date

import pytest
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.models.autopart import AutoPart
from dz_fastapi.models.inventory import StockLot, StockLotRole, StockLotRoleSource
from dz_fastapi.models.partner import (
    PROVIDER_INVENTORY_POLICY,
    Provider,
    ProviderInventoryRoleRule,
    SupplierReceipt,
    SupplierReceiptItem,
)
from dz_fastapi.models.user import User, UserRole, UserStatus
from dz_fastapi.services.auth import get_password_hash
from dz_fastapi.services.inventory_stock import receive_stock


async def _receive_one_part(
    session: AsyncSession,
    *,
    provider: Provider,
    autopart: AutoPart,
) -> StockLot:
    receipt = SupplierReceipt(
        provider_id=provider.id,
        document_number=f"ROLE-{provider.id}",
        document_date=date.today(),
    )
    receipt.items = [
        SupplierReceiptItem(
            autopart_id=autopart.id,
            received_quantity=2,
        )
    ]
    session.add(receipt)
    await session.flush()
    await receive_stock(session, receipt=receipt)
    await session.flush()
    return (
        await session.execute(
            select(StockLot).where(StockLot.source_receipt_id == receipt.id)
        )
    ).scalar_one()


@pytest.mark.asyncio
async def test_provider_material_policy_classifies_receipt_lot(
    test_session: AsyncSession,
    created_providers: list[Provider],
    created_autopart: AutoPart,
):
    provider = created_providers[0]
    provider.inventory_policy = PROVIDER_INVENTORY_POLICY.DRAGONZAP_MATERIAL
    await test_session.flush()

    lot = await _receive_one_part(
        test_session,
        provider=provider,
        autopart=created_autopart,
    )

    assert lot.inventory_role == StockLotRole.DRAGONZAP_MATERIAL
    assert lot.role_source == StockLotRoleSource.PROVIDER_POLICY
    assert lot.role_rule_reference == (
        f"provider_inventory_policy:{provider.id}:dragonzap_material"
    )


@pytest.mark.asyncio
async def test_item_rule_overrides_mixed_provider_and_fallback_is_safe(
    test_session: AsyncSession,
    created_providers: list[Provider],
    created_autopart: AutoPart,
):
    mixed_provider = created_providers[0]
    mixed_provider.inventory_policy = PROVIDER_INVENTORY_POLICY.MIXED
    await test_session.flush()

    fallback_lot = await _receive_one_part(
        test_session,
        provider=mixed_provider,
        autopart=created_autopart,
    )
    assert fallback_lot.inventory_role == StockLotRole.ORIGINAL_GOOD
    assert fallback_lot.role_source == StockLotRoleSource.PROVIDER_POLICY
    assert "нет точного правила" in fallback_lot.role_change_reason

    rule = ProviderInventoryRoleRule(
        provider_id=created_providers[1].id,
        autopart_id=created_autopart.id,
        inventory_role=StockLotRole.DRAGONZAP_MATERIAL,
        reason="Эта позиция всегда поступает как материал",
    )
    test_session.add(rule)
    await test_session.flush()
    override_lot = await _receive_one_part(
        test_session,
        provider=created_providers[1],
        autopart=created_autopart,
    )

    assert override_lot.inventory_role == StockLotRole.DRAGONZAP_MATERIAL
    assert override_lot.role_source == StockLotRoleSource.ITEM_RULE
    assert override_lot.role_rule_reference == (
        f"provider_inventory_role_rule:{rule.id}"
    )
    assert override_lot.role_change_reason == rule.reason


@pytest.mark.asyncio
async def test_admin_manages_provider_inventory_role_rule(
    async_client: AsyncClient,
    test_session: AsyncSession,
    created_providers: list[Provider],
    created_autopart: AutoPart,
):
    test_session.add(
        User(
            id=1,
            name="Test Admin",
            email="provider-role-admin@example.com",
            password_hash="not-used",
            role=UserRole.ADMIN,
            status=UserStatus.ACTIVE,
        )
    )
    await test_session.commit()

    create_response = await async_client.post(
        f"/providers/{created_providers[0].id}/inventory-role-rules",
        json={
            "autopart_id": created_autopart.id,
            "inventory_role": "dragonzap_material",
            "reason": "Материал для переупаковки",
        },
    )
    assert create_response.status_code == 201, create_response.text
    payload = create_response.json()
    assert payload["autopart_oem"] == created_autopart.oem_number
    assert payload["inventory_role"] == "dragonzap_material"
    assert payload["created_by_name"] == "Test Admin"

    list_response = await async_client.get(
        f"/providers/{created_providers[0].id}/inventory-role-rules"
    )
    assert list_response.status_code == 200, list_response.text
    assert [item["id"] for item in list_response.json()] == [payload["id"]]

    update_response = await async_client.patch(
        (
            f"/providers/{created_providers[0].id}/inventory-role-rules/"
            f"{payload['id']}"
        ),
        json={
            "inventory_role": "original_good",
            "reason": "Проверено как оригинальный товар",
        },
    )
    assert update_response.status_code == 200, update_response.text
    assert update_response.json()["inventory_role"] == "original_good"

    invalid_response = await async_client.post(
        f"/providers/{created_providers[1].id}/inventory-role-rules",
        json={
            "autopart_id": created_autopart.id,
            "inventory_role": "dragonzap_finished",
        },
    )
    assert invalid_response.status_code == 422, invalid_response.text


@pytest.mark.no_auth_override
@pytest.mark.asyncio
async def test_manager_cannot_change_provider_inventory_policy(
    async_client: AsyncClient,
    test_session: AsyncSession,
    created_providers: list[Provider],
    created_autopart: AutoPart,
):
    manager = User(
        email="provider-role-manager@example.com",
        password_hash=get_password_hash("secret123"),
        role=UserRole.MANAGER,
        status=UserStatus.ACTIVE,
    )
    test_session.add(manager)
    await test_session.commit()
    login_response = await async_client.post(
        "/auth/login",
        json={"email": manager.email, "password": "secret123"},
    )
    assert login_response.status_code == 200, login_response.text

    policy_response = await async_client.patch(
        f"/providers/{created_providers[0].id}/",
        json={"inventory_policy": "dragonzap_material"},
    )
    assert policy_response.status_code == 403, policy_response.text

    rule_response = await async_client.post(
        f"/providers/{created_providers[0].id}/inventory-role-rules",
        json={
            "autopart_id": created_autopart.id,
            "inventory_role": "dragonzap_material",
        },
    )
    assert rule_response.status_code == 403, rule_response.text
