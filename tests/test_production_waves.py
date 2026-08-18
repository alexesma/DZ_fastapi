from __future__ import annotations

from datetime import timedelta
from decimal import Decimal

import pytest
from httpx import AsyncClient
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.core.time import now_moscow
from dz_fastapi.models.autopart import AutoPart, LocationType, StorageLocation
from dz_fastapi.models.brand import Brand
from dz_fastapi.models.cross import AutoPartCross
from dz_fastapi.models.inventory import (
    LotSourceType,
    MovementType,
    ProductionWaveAllocation,
    ProductionWaveLabel,
    ProductionWaveSource,
    ProductionWaveStatus,
    StockLot,
    StockLotRole,
    Warehouse,
)
from dz_fastapi.models.partner import (
    CUSTOMER_ORDER_ITEM_STATUS,
    Customer,
    CustomerOrder,
    CustomerOrderItem,
    StockOrder,
    StockOrderItem,
)
from dz_fastapi.models.user import User, UserRole, UserStatus
from dz_fastapi.services.inventory_stock import _apply_stock_delta, _consume_fifo, _create_stock_lot
from dz_fastapi.services.production_waves import create_scheduled_production_wave


async def _wave_scenario(
    session: AsyncSession,
    *,
    requested_quantity: int = 6,
    available_quantities: tuple[int, ...] = (2, 4),
):
    session.add(
        User(
            id=1,
            name="Wave Admin",
            email="wave-admin@example.com",
            password_hash="unused",
            role=UserRole.ADMIN,
            status=UserStatus.ACTIVE,
        )
    )
    dz_brand = Brand(name="DRAGONZAP")
    material_brand = Brand(name="GEELY")
    session.add_all([dz_brand, material_brand])
    await session.flush()
    finished = AutoPart(
        brand_id=dz_brand.id,
        oem_number="DZ1014003218",
        name="Готовая позиция DragonZap",
    )
    material = AutoPart(
        brand_id=material_brand.id,
        oem_number="1014003218",
        name="Материал Geely",
    )
    session.add_all([finished, material])
    await session.flush()
    session.add(
        AutoPartCross(
            source_autopart_id=finished.id,
            cross_autopart_id=material.id,
            cross_brand_id=material_brand.id,
            cross_oem_number=material.oem_number,
            priority=10,
        )
    )
    warehouse = Warehouse(name="WAVE WAREHOUSE", is_active=True)
    session.add(warehouse)
    await session.flush()
    location = StorageLocation(
        name="WAVE MATERIAL",
        warehouse_id=warehouse.id,
        location_type=LocationType.OTHER,
    )
    session.add(location)
    customer = Customer(
        name="Wave Customer",
        email_contact="wave-customer@example.com",
    )
    session.add(customer)
    await session.flush()
    order = CustomerOrder(
        customer_id=customer.id,
        order_number="WAVE-ORDER-1",
    )
    session.add(order)
    await session.flush()
    order_item = CustomerOrderItem(
        order_id=order.id,
        brand="DragonZap",
        oem="DZ1014003218",
        name="Позиция клиента",
        requested_qty=requested_quantity,
        ship_qty=requested_quantity,
        status=CUSTOMER_ORDER_ITEM_STATUS.OWN_STOCK,
        autopart_id=finished.id,
    )
    session.add(order_item)
    stock_order = StockOrder(customer_id=customer.id)
    session.add(stock_order)
    await session.flush()
    stock_item = StockOrderItem(
        stock_order_id=stock_order.id,
        customer_order_item_id=order_item.id,
        autopart_id=finished.id,
        quantity=requested_quantity,
    )
    session.add(stock_item)
    await session.flush()

    lots = []
    for index, quantity in enumerate(available_quantities):
        lot = await _create_stock_lot(
            session,
            autopart_id=material.id,
            storage_location_id=location.id,
            quantity=quantity,
            source_type=LotSourceType.RECEIPT,
            gtd_number=f"GTD-{index + 1}",
            country_code="CN",
            cost_price=Decimal(10 + index * 2),
            received_at=now_moscow() - timedelta(days=10 - index),
            inventory_role=StockLotRole.DRAGONZAP_MATERIAL,
        )
        await _apply_stock_delta(
            session,
            autopart_id=material.id,
            storage_location_id=location.id,
            quantity_delta=quantity,
            movement_type=MovementType.RECEIPT,
            stock_lot_id=lot.id,
        )
        lots.append(lot)
    await session.commit()
    return warehouse, finished, material, stock_item, lots


@pytest.mark.asyncio
async def test_production_wave_plans_fifo_and_posts_finished_lots(
    async_client: AsyncClient,
    test_session: AsyncSession,
):
    warehouse, finished, material, stock_item, source_lots = await _wave_scenario(test_session)
    groups = await async_client.get("/inventory/production-groups")
    assert groups.status_code == 200, groups.text
    group_id = groups.json()["items"][0]["id"]
    configured = await async_client.patch(
        f"/inventory/production-groups/{group_id}",
        json={"packaging_cost": 2},
    )
    assert configured.status_code == 200, configured.text

    eligible = await async_client.get("/inventory/production-waves/eligible")
    assert eligible.status_code == 200, eligible.text
    assert eligible.json()["items"][0]["stock_order_item_id"] == stock_item.id

    created = await async_client.post(
        "/inventory/production-waves",
        json={
            "stock_order_item_ids": [stock_item.id],
            "warehouse_id": warehouse.id,
        },
    )
    assert created.status_code == 201, created.text
    wave = created.json()
    assert wave["status"] == "draft"
    assert wave["total_planned_quantity"] == 6
    allocations = wave["items"][0]["allocations"]
    assert [row["planned_quantity"] for row in allocations] == [2, 4]
    assert [row["gtd_number"] for row in allocations] == ["GTD-1", "GTD-2"]
    assert Decimal(str(wave["total_finished_cost"])) == Decimal("80.00")

    planned = await async_client.post(f"/inventory/production-waves/{wave['id']}/plan")
    assert planned.status_code == 200, planned.text
    assert planned.json()["status"] == "planned"
    labels_response = await async_client.get(
        f"/inventory/production-waves/{wave['id']}/labels"
    )
    assert labels_response.status_code == 200, labels_response.text
    labels = labels_response.json()
    assert len(labels) == 6
    assert {label["requested_oem"] for label in labels} == {"DZ1014003218"}
    assert [label["sequence_number"] for label in labels] == list(range(1, 7))
    assert all(label["quantity"] == 1 for label in labels)

    printed = await async_client.post(
        f"/inventory/production-waves/{wave['id']}/labels/printed",
        json={"label_ids": [labels[0]["id"]]},
    )
    assert printed.status_code == 200, printed.text
    assert printed.json()[0]["print_count"] == 1
    reprint_without_reason = await async_client.post(
        f"/inventory/production-waves/{wave['id']}/labels/printed",
        json={"label_ids": [labels[0]["id"]]},
    )
    assert reprint_without_reason.status_code == 400
    reprinted = await async_client.post(
        f"/inventory/production-waves/{wave['id']}/labels/printed",
        json={
            "label_ids": [labels[0]["id"]],
            "reason": "Повреждена при упаковке",
        },
    )
    assert reprinted.status_code == 200, reprinted.text
    assert reprinted.json()[0]["print_count"] == 2
    assert reprinted.json()[0]["last_print_reason"] == "Повреждена при упаковке"
    assert [event["print_number"] for event in reprinted.json()[0]["print_history"]] == [
        1,
        2,
    ]
    assert reprinted.json()[0]["print_history"][1]["reason"] == (
        "Повреждена при упаковке"
    )
    blocked_start = await async_client.post(
        f"/inventory/production-waves/{wave['id']}/start"
    )
    assert blocked_start.status_code == 400
    assert "напечатайте все этикетки" in blocked_start.json()["detail"]
    remaining_printed = await async_client.post(
        f"/inventory/production-waves/{wave['id']}/labels/printed",
        json={"label_ids": [label["id"] for label in labels[1:]]},
    )
    assert remaining_printed.status_code == 200, remaining_printed.text
    with pytest.raises(ValueError, match="производственной волной"):
        await _consume_fifo(
            test_session,
            autopart_id=material.id,
            storage_location_id=source_lots[0].storage_location_id,
            quantity=1,
            movement_type=MovementType.SHIPMENT,
        )
    await test_session.rollback()
    started = await async_client.post(f"/inventory/production-waves/{wave['id']}/start")
    assert started.status_code == 200, started.text
    completed = await async_client.post(f"/inventory/production-waves/{wave['id']}/complete")
    assert completed.status_code == 200, completed.text
    payload = completed.json()
    assert payload["status"] == "completed"
    assert payload["total_produced_quantity"] == 6

    for source_lot in source_lots:
        await test_session.refresh(source_lot)
        assert source_lot.remaining_quantity == 0
    output_lots = (
        (
            await test_session.execute(
                select(StockLot)
                .where(
                    StockLot.autopart_id == finished.id,
                    StockLot.source_type == LotSourceType.PRODUCTION,
                )
                .order_by(StockLot.id)
            )
        )
        .scalars()
        .all()
    )
    assert [lot.initial_quantity for lot in output_lots] == [2, 4]
    assert [lot.gtd_number for lot in output_lots] == ["GTD-1", "GTD-2"]
    assert [lot.cost_price for lot in output_lots] == [
        Decimal("12.0000"),
        Decimal("14.0000"),
    ]
    assert all(lot.inventory_role == StockLotRole.DRAGONZAP_FINISHED for lot in output_lots)


@pytest.mark.asyncio
async def test_shortage_blocks_planning_and_cancel_releases_order_row(
    async_client: AsyncClient,
    test_session: AsyncSession,
):
    warehouse, _, _, stock_item, _ = await _wave_scenario(
        test_session,
        requested_quantity=5,
        available_quantities=(2,),
    )
    groups = await async_client.get("/inventory/production-groups")
    assert groups.status_code == 200, groups.text
    created = await async_client.post(
        "/inventory/production-waves",
        json={
            "stock_order_item_ids": [stock_item.id],
            "warehouse_id": warehouse.id,
        },
    )
    assert created.status_code == 201, created.text
    wave = created.json()
    assert wave["items"][0]["shortage_quantity"] == 3
    assert "Не хватает материала" in wave["error_message"]

    blocked = await async_client.post(f"/inventory/production-waves/{wave['id']}/plan")
    assert blocked.status_code == 400, blocked.text
    cancelled = await async_client.post(f"/inventory/production-waves/{wave['id']}/cancel")
    assert cancelled.status_code == 200, cancelled.text
    assert cancelled.json()["status"] == ProductionWaveStatus.CANCELLED.value

    eligible = await async_client.get("/inventory/production-waves/eligible")
    assert eligible.status_code == 200, eligible.text
    assert stock_item.id in {row["stock_order_item_id"] for row in eligible.json()["items"]}

    allocations = int(
        (await test_session.execute(select(func.count(ProductionWaveAllocation.id)))).scalar_one()
    )
    assert allocations == 1


@pytest.mark.asyncio
async def test_scheduled_wave_is_planned_and_creates_labels(
    async_client: AsyncClient,
    test_session: AsyncSession,
):
    warehouse, *_ = await _wave_scenario(test_session, requested_quantity=3)
    groups = await async_client.get("/inventory/production-groups")
    assert groups.status_code == 200, groups.text
    wave = await create_scheduled_production_wave(
        test_session,
        warehouse_id=warehouse.id,
    )
    assert wave is not None
    assert wave.source == ProductionWaveSource.SCHEDULED
    assert wave.status == ProductionWaveStatus.PLANNED, wave.error_message
    await test_session.commit()
    labels_count = int(
        (
            await test_session.execute(
                select(func.count(ProductionWaveLabel.id)).where(
                    ProductionWaveLabel.wave_id == wave.id
                )
            )
        ).scalar_one()
    )
    assert labels_count == 3
