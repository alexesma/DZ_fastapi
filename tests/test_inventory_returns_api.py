from __future__ import annotations

from datetime import date

import pytest
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.models.autopart import AutoPart, StorageLocation
from dz_fastapi.models.brand import Brand
from dz_fastapi.models.diadoc import DiadocOutgoingDocument
from dz_fastapi.models.inventory import (
    LotSourceType,
    MovementType,
    ReturnFromCustomer,
    StockByLocation,
    StockLot,
    StockMovement,
)
from dz_fastapi.models.partner import Provider, SupplierReceipt, SupplierReceiptItem
from dz_fastapi.services.inventory_stock import receive_stock


async def _stock_row(
    session: AsyncSession,
    *,
    autopart_id: int,
    storage_location_id: int,
) -> StockByLocation | None:
    return (
        await session.execute(
            select(StockByLocation).where(
                StockByLocation.autopart_id == autopart_id,
                StockByLocation.storage_location_id == storage_location_id,
            )
        )
    ).scalar_one_or_none()


@pytest.mark.asyncio
async def test_customer_return_confirm_creates_new_lot_and_receipt_movement(
    async_client: AsyncClient,
    test_session: AsyncSession,
    created_autopart: AutoPart,
    created_storage: StorageLocation,
    created_customers,
):
    seed_response = await async_client.post(
        "/inventory/movements/",
        json={
            "autopart_id": created_autopart.id,
            "storage_location_id": created_storage.id,
            "movement_type": "manual",
            "quantity": 5,
            "notes": "seed for customer return",
        },
    )
    assert seed_response.status_code == 201, seed_response.text

    shipment_response = await async_client.post(
        "/inventory/shipments/",
        json={
            "customer_id": created_customers[0].id,
            "reason": "Отгрузка под будущий возврат",
            "items": [
                {
                    "autopart_id": created_autopart.id,
                    "storage_location_id": created_storage.id,
                    "quantity": 2,
                    "price": "150.00",
                }
            ],
        },
    )
    assert shipment_response.status_code == 201, shipment_response.text
    shipment = shipment_response.json()
    shipment_id = shipment["id"]
    shipment_item_id = shipment["items"][0]["id"]

    post_response = await async_client.post(f"/inventory/shipments/{shipment_id}/post/")
    assert post_response.status_code == 200, post_response.text

    return_response = await async_client.post(
        "/inventory/customer-returns/",
        json={
            "customer_id": created_customers[0].id,
            "shipment_document_id": shipment_id,
            "reason": "Не подошло",
        },
    )
    assert return_response.status_code == 201, return_response.text
    customer_return = return_response.json()
    return_id = customer_return["id"]

    add_item_response = await async_client.post(
        f"/inventory/customer-returns/{return_id}/items/",
        json={
            "shipment_item_id": shipment_item_id,
            "quantity": 2,
            "notes": "Клиент вернул товар",
        },
    )
    assert add_item_response.status_code == 201, add_item_response.text

    approve_response = await async_client.post(f"/inventory/customer-returns/{return_id}/approve/")
    assert approve_response.status_code == 200, approve_response.text

    ship_response = await async_client.post(f"/inventory/customer-returns/{return_id}/ship/")
    assert ship_response.status_code == 200, ship_response.text

    confirm_response = await async_client.post(f"/inventory/customer-returns/{return_id}/confirm/")
    assert confirm_response.status_code == 200, confirm_response.text
    confirmed = confirm_response.json()
    assert confirmed["status"] == "confirmed"
    assert confirmed["items"][0]["lot_id"] is not None

    movement = (
        await test_session.execute(
            select(StockMovement).where(
                StockMovement.reference_id == return_id,
                StockMovement.reference_type == "return_from_customer",
                StockMovement.movement_type == MovementType.CUSTOMER_RETURN,
            )
        )
    ).scalar_one()
    assert movement.quantity == 2
    assert movement.stock_lot_id is not None

    created_lot = await test_session.get(StockLot, movement.stock_lot_id)
    assert created_lot is not None
    assert created_lot.source_type == LotSourceType.CUSTOMER_RETURN
    assert created_lot.remaining_quantity == 2

    receiving_stock = await _stock_row(
        test_session,
        autopart_id=created_autopart.id,
        storage_location_id=created_lot.storage_location_id,
    )
    assert receiving_stock is not None
    assert receiving_stock.quantity == 2


@pytest.mark.asyncio
async def test_supplier_return_ship_consumes_receipt_lot(
    async_client: AsyncClient,
    test_session: AsyncSession,
    created_autopart: AutoPart,
    created_brand: Brand,
    created_providers: list[Provider],
):
    receipt = SupplierReceipt(
        provider_id=created_providers[0].id,
        document_number="RET-SUP-1",
        document_date=date.today(),
    )
    receipt.items = [
        SupplierReceiptItem(
            autopart_id=created_autopart.id,
            oem_number=created_autopart.oem_number,
            brand_name=created_brand.name,
            autopart_name=created_autopart.name,
            received_quantity=4,
            price="250.00",
            gtd_code="GTD-RET-001",
            country_code="CN",
            country_name="China",
        )
    ]
    test_session.add(receipt)
    await test_session.flush()

    await receive_stock(test_session, receipt=receipt, reverse=False)
    await test_session.commit()

    source_item = receipt.items[0]
    receipt_lot = (
        await test_session.execute(
            select(StockLot).where(StockLot.source_receipt_item_id == source_item.id)
        )
    ).scalar_one()
    assert receipt_lot.remaining_quantity == 4

    create_response = await async_client.post(
        "/inventory/supplier-returns/",
        json={
            "provider_id": created_providers[0].id,
            "supplier_receipt_id": receipt.id,
            "reason": "Возврат по браку",
        },
    )
    assert create_response.status_code == 201, create_response.text
    supplier_return = create_response.json()
    return_id = supplier_return["id"]

    add_item_response = await async_client.post(
        f"/inventory/supplier-returns/{return_id}/items/",
        json={
            "supplier_receipt_item_id": source_item.id,
            "quantity": 3,
            "notes": "Возврат поставщику",
        },
    )
    assert add_item_response.status_code == 201, add_item_response.text

    approve_response = await async_client.post(f"/inventory/supplier-returns/{return_id}/approve/")
    assert approve_response.status_code == 200, approve_response.text

    ship_response = await async_client.post(f"/inventory/supplier-returns/{return_id}/ship/")
    assert ship_response.status_code == 200, ship_response.text
    shipped = ship_response.json()
    assert shipped["status"] == "shipped"
    assert shipped["items"][0]["lot_id"] == receipt_lot.id

    movement = (
        await test_session.execute(
            select(StockMovement).where(
                StockMovement.reference_id == return_id,
                StockMovement.reference_type == "return_to_supplier",
                StockMovement.movement_type == MovementType.SUPPLIER_RETURN,
            )
        )
    ).scalar_one()
    assert movement.quantity == -3
    assert movement.stock_lot_id == receipt_lot.id

    await test_session.refresh(receipt_lot)
    assert receipt_lot.remaining_quantity == 1

    stock_row = await _stock_row(
        test_session,
        autopart_id=created_autopart.id,
        storage_location_id=receipt_lot.storage_location_id,
    )
    assert stock_row is not None
    assert stock_row.quantity == 1

    confirm_response = await async_client.post(f"/inventory/supplier-returns/{return_id}/confirm/")
    assert confirm_response.status_code == 200, confirm_response.text
    assert confirm_response.json()["status"] == "confirmed"


@pytest.mark.asyncio
async def test_customer_return_rejects_invalid_confirm_transition(
    async_client: AsyncClient,
    created_customers,
):
    create_response = await async_client.post(
        "/inventory/customer-returns/",
        json={
            "customer_id": created_customers[0].id,
            "reason": "Проверка переходов",
        },
    )
    assert create_response.status_code == 201, create_response.text
    return_id = create_response.json()["id"]

    confirm_response = await async_client.post(f"/inventory/customer-returns/{return_id}/confirm/")
    assert confirm_response.status_code == 400, confirm_response.text
    assert "APPROVED" in confirm_response.json()["detail"]


@pytest.mark.asyncio
async def test_customer_return_list_includes_diadoc_outgoing_status(
    async_client: AsyncClient,
    test_session: AsyncSession,
    created_customers,
):
    outgoing = DiadocOutgoingDocument(
        environment="staging",
        from_box_id_guid="box-from",
        to_box_id_guid="box-to",
        customer_id=created_customers[0].id,
        source_type="return_from_customer",
        type_named_id="UniversalCorrectionDocument",
        file_name="ukd.xml",
        local_file_path="/tmp/ukd.xml",
        status="sent",
    )
    test_session.add(outgoing)
    await test_session.flush()

    create_response = await async_client.post(
        "/inventory/customer-returns/",
        json={
            "customer_id": created_customers[0].id,
            "reason": "Тестовая связка с Диадоком",
        },
    )
    assert create_response.status_code == 201, create_response.text
    created_return = create_response.json()

    customer_return = await test_session.get(
        ReturnFromCustomer,
        created_return["id"],
    )
    customer_return.diadoc_outgoing_document_id = outgoing.id
    await test_session.commit()

    list_response = await async_client.get("/inventory/customer-returns/")
    assert list_response.status_code == 200, list_response.text
    payload = list_response.json()
    target_row = next(row for row in payload if row["id"] == created_return["id"])
    assert target_row["diadoc_outgoing_document_id"] == outgoing.id
    assert target_row["diadoc_outgoing_status"] == "sent"
