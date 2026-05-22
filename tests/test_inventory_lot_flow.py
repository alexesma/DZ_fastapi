from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import noload

from dz_fastapi.models.autopart import AutoPart, StorageLocation
from dz_fastapi.models.inventory import (
    LotSourceType,
    ShipmentDocument,
    ShipmentDocumentItem,
    ShipmentDocumentItemLotAllocation,
    ShipmentDocumentStatus,
    StockByLocation,
    StockDocument,
    StockDocumentItem,
    StockDocumentStatus,
    StockDocumentType,
    StockLot,
)
from dz_fastapi.models.partner import Provider, SupplierReceipt, SupplierReceiptItem
from dz_fastapi.services.inventory_stock import (
    backfill_opening_balance_lots,
    post_shipment_document,
    post_stock_document,
    receive_stock,
    reconcile_stock_absolute,
    transfer_stock_with_lot_trace,
)


async def _lot_sum(
    session: AsyncSession,
    *,
    autopart_id: int,
    storage_location_id: int,
) -> int:
    stmt = select(func.coalesce(func.sum(StockLot.remaining_quantity), 0)).where(
        StockLot.autopart_id == autopart_id,
        StockLot.storage_location_id == storage_location_id,
    )
    return int((await session.execute(stmt)).scalar_one())


async def _sbl_qty(
    session: AsyncSession,
    *,
    autopart_id: int,
    storage_location_id: int,
) -> int:
    stmt = select(StockByLocation).where(
        StockByLocation.autopart_id == autopart_id,
        StockByLocation.storage_location_id == storage_location_id,
    )
    row = (await session.execute(stmt)).scalar_one_or_none()
    return int(row.quantity) if row else 0


@pytest.mark.asyncio
async def test_receive_stock_with_gtd_creates_receipt_lot(
    test_session: AsyncSession,
    created_autopart: AutoPart,
    created_providers: list[Provider],
):
    receipt = SupplierReceipt(
        provider_id=created_providers[0].id,
        document_number="R-1",
        document_date=date.today(),
    )
    receipt.items = [
        SupplierReceiptItem(
            autopart_id=created_autopart.id,
            received_quantity=5,
            price=Decimal("123.45"),
            gtd_code="GTD-001",
            country_code="CN",
            country_name="China",
        )
    ]
    test_session.add(receipt)
    await test_session.flush()

    await receive_stock(test_session, receipt=receipt, reverse=False)
    await test_session.commit()

    lots = (
        (
            await test_session.execute(
                select(StockLot).where(StockLot.source_receipt_id == receipt.id)
            )
        )
        .scalars()
        .all()
    )
    assert len(lots) == 1
    assert lots[0].source_type == LotSourceType.RECEIPT
    assert lots[0].gtd_number == "GTD-001"
    assert lots[0].remaining_quantity == 5
    assert lots[0].cost_price == Decimal("123.4500")


@pytest.mark.asyncio
async def test_manual_receipt_document_creates_manual_lot(
    test_session: AsyncSession,
    created_autopart: AutoPart,
    created_storage: StorageLocation,
):
    doc = StockDocument(
        doc_type=StockDocumentType.MANUAL_RECEIPT,
        status=StockDocumentStatus.DRAFT,
        reason="manual in",
    )
    test_session.add(doc)
    await test_session.flush()
    test_session.add(
        StockDocumentItem(
            document_id=doc.id,
            autopart_id=created_autopart.id,
            storage_location_id=created_storage.id,
            quantity=4,
            cost_price=Decimal("55.4321"),
            gtd_number="GTD-MANUAL",
        )
    )
    await test_session.flush()

    await post_stock_document(test_session, document_id=doc.id)
    await test_session.commit()

    manual_lots = (
        (
            await test_session.execute(
                select(StockLot)
                .options(noload("*"))
                .where(
                    StockLot.autopart_id == created_autopart.id,
                    StockLot.storage_location_id == created_storage.id,
                    StockLot.source_type == LotSourceType.MANUAL,
                )
            )
        )
        .scalars()
        .all()
    )
    assert len(manual_lots) == 1
    assert manual_lots[0].gtd_number == "GTD-MANUAL"
    assert manual_lots[0].remaining_quantity == 4
    assert manual_lots[0].cost_price == Decimal("55.4321")


@pytest.mark.asyncio
async def test_manual_writeoff_consumes_fifo_lots(
    test_session: AsyncSession,
    created_autopart: AutoPart,
    created_storage: StorageLocation,
):
    # two receipts with different GTD to verify FIFO by age
    in_doc_1 = StockDocument(
        doc_type=StockDocumentType.MANUAL_RECEIPT,
        status=StockDocumentStatus.DRAFT,
    )
    in_doc_2 = StockDocument(
        doc_type=StockDocumentType.MANUAL_RECEIPT,
        status=StockDocumentStatus.DRAFT,
    )
    test_session.add_all([in_doc_1, in_doc_2])
    await test_session.flush()
    test_session.add_all(
        [
            StockDocumentItem(
                document_id=in_doc_1.id,
                autopart_id=created_autopart.id,
                storage_location_id=created_storage.id,
                quantity=3,
                gtd_number="GTD-OLD",
            ),
            StockDocumentItem(
                document_id=in_doc_2.id,
                autopart_id=created_autopart.id,
                storage_location_id=created_storage.id,
                quantity=3,
                gtd_number="GTD-NEW",
            ),
        ]
    )
    await test_session.flush()
    await post_stock_document(test_session, document_id=in_doc_1.id)
    await post_stock_document(test_session, document_id=in_doc_2.id)

    out_doc = StockDocument(
        doc_type=StockDocumentType.MANUAL_WRITEOFF,
        status=StockDocumentStatus.DRAFT,
    )
    test_session.add(out_doc)
    await test_session.flush()
    test_session.add(
        StockDocumentItem(
            document_id=out_doc.id,
            autopart_id=created_autopart.id,
            storage_location_id=created_storage.id,
            quantity=4,
        )
    )
    await test_session.flush()
    await post_stock_document(test_session, document_id=out_doc.id)
    await test_session.commit()

    lots = (
        (
            await test_session.execute(
                select(StockLot)
                .where(
                    StockLot.autopart_id == created_autopart.id,
                    StockLot.storage_location_id == created_storage.id,
                    StockLot.gtd_number.in_(["GTD-OLD", "GTD-NEW"]),
                )
                .order_by(StockLot.received_at, StockLot.id)
            )
        )
        .scalars()
        .all()
    )
    by_gtd = {lot.gtd_number: lot for lot in lots}
    assert by_gtd["GTD-OLD"].remaining_quantity == 0
    assert by_gtd["GTD-NEW"].remaining_quantity == 2


@pytest.mark.asyncio
async def test_inventory_shortage_uses_fifo(
    test_session: AsyncSession,
    created_autopart: AutoPart,
    created_storage: StorageLocation,
):
    for qty, gtd in ((3, "INV-A"), (2, "INV-B")):
        doc = StockDocument(
            doc_type=StockDocumentType.MANUAL_RECEIPT,
            status=StockDocumentStatus.DRAFT,
        )
        test_session.add(doc)
        await test_session.flush()
        test_session.add(
            StockDocumentItem(
                document_id=doc.id,
                autopart_id=created_autopart.id,
                storage_location_id=created_storage.id,
                quantity=qty,
                gtd_number=gtd,
            )
        )
        await test_session.flush()
        await post_stock_document(test_session, document_id=doc.id)

    # current=5 -> target=1, should consume oldest first
    await reconcile_stock_absolute(
        test_session,
        autopart_id=created_autopart.id,
        storage_location_id=created_storage.id,
        target_quantity=1,
        inventory_session_id=100,
    )
    await test_session.commit()

    lots = (
        (
            await test_session.execute(
                select(StockLot).where(
                    StockLot.autopart_id == created_autopart.id,
                    StockLot.storage_location_id == created_storage.id,
                )
            )
        )
        .scalars()
        .all()
    )
    by_gtd = {lot.gtd_number: lot.remaining_quantity for lot in lots}
    assert by_gtd["INV-A"] == 0
    assert by_gtd["INV-B"] == 1


@pytest.mark.asyncio
async def test_inventory_surplus_creates_inventory_correction_lot(
    test_session: AsyncSession,
    created_autopart: AutoPart,
    created_storage: StorageLocation,
):
    await reconcile_stock_absolute(
        test_session,
        autopart_id=created_autopart.id,
        storage_location_id=created_storage.id,
        target_quantity=7,
        inventory_session_id=101,
    )
    await test_session.commit()

    correction_lot = (
        await test_session.execute(
            select(StockLot).where(
                StockLot.autopart_id == created_autopart.id,
                StockLot.storage_location_id == created_storage.id,
                StockLot.source_type == LotSourceType.INVENTORY_CORRECTION,
            )
        )
    ).scalar_one_or_none()
    assert correction_lot is not None
    assert correction_lot.remaining_quantity == 7


@pytest.mark.asyncio
async def test_inventory_surplus_infers_weighted_cost_from_existing_lots(
    test_session: AsyncSession,
    created_autopart: AutoPart,
    created_storage: StorageLocation,
):
    for qty, cost in ((3, Decimal("10.00")), (1, Decimal("20.00"))):
        doc = StockDocument(
            doc_type=StockDocumentType.MANUAL_RECEIPT,
            status=StockDocumentStatus.DRAFT,
        )
        test_session.add(doc)
        await test_session.flush()
        test_session.add(
            StockDocumentItem(
                document_id=doc.id,
                autopart_id=created_autopart.id,
                storage_location_id=created_storage.id,
                quantity=qty,
                cost_price=cost,
            )
        )
        await test_session.flush()
        await post_stock_document(test_session, document_id=doc.id)

    await reconcile_stock_absolute(
        test_session,
        autopart_id=created_autopart.id,
        storage_location_id=created_storage.id,
        target_quantity=6,
        inventory_session_id=104,
    )
    await test_session.commit()

    correction_lot = (
        await test_session.execute(
            select(StockLot)
            .where(
                StockLot.autopart_id == created_autopart.id,
                StockLot.storage_location_id == created_storage.id,
                StockLot.source_type == LotSourceType.INVENTORY_CORRECTION,
            )
            .order_by(StockLot.id.desc())
        )
    ).scalar_one()

    assert correction_lot.remaining_quantity == 2
    assert correction_lot.cost_price == Decimal("12.5000")


@pytest.mark.asyncio
async def test_transfer_keeps_lot_trace_data(
    test_session: AsyncSession,
    created_autopart: AutoPart,
):
    src = StorageLocation(name="TRSRC")
    dst = StorageLocation(name="TRDST")
    test_session.add_all([src, dst])
    await test_session.flush()

    doc = StockDocument(
        doc_type=StockDocumentType.MANUAL_RECEIPT,
        status=StockDocumentStatus.DRAFT,
    )
    test_session.add(doc)
    await test_session.flush()
    test_session.add(
        StockDocumentItem(
            document_id=doc.id,
            autopart_id=created_autopart.id,
            storage_location_id=src.id,
            quantity=6,
            gtd_number="TR-GTD",
        )
    )
    await test_session.flush()
    await post_stock_document(test_session, document_id=doc.id)

    await transfer_stock_with_lot_trace(
        test_session,
        autopart_id=created_autopart.id,
        from_location_id=src.id,
        to_location_id=dst.id,
        quantity=4,
        notes="move",
    )
    await test_session.commit()

    dst_lots = (
        (
            await test_session.execute(
                select(StockLot).where(
                    StockLot.autopart_id == created_autopart.id,
                    StockLot.storage_location_id == dst.id,
                )
            )
        )
        .scalars()
        .all()
    )
    assert len(dst_lots) == 1
    assert dst_lots[0].gtd_number == "TR-GTD"
    assert dst_lots[0].remaining_quantity == 4
    assert dst_lots[0].cost_price is None


@pytest.mark.asyncio
async def test_transfer_keeps_receipt_lot_cost_price(
    test_session: AsyncSession,
    created_autopart: AutoPart,
    created_providers: list[Provider],
):
    dst = StorageLocation(name="DSTCOST")
    test_session.add(dst)
    await test_session.flush()

    receipt = SupplierReceipt(
        provider_id=created_providers[0].id,
        document_number="R-COST-TRANSFER",
        document_date=date.today(),
    )
    receipt.items = [
        SupplierReceiptItem(
            autopart_id=created_autopart.id,
            received_quantity=4,
            price=Decimal("87.65"),
            warehouse_id=None,
        )
    ]
    test_session.add(receipt)
    await test_session.flush()
    await receive_stock(test_session, receipt=receipt, reverse=False)

    src_lot = (
        await test_session.execute(
            select(StockLot).where(StockLot.source_receipt_id == receipt.id)
        )
    ).scalar_one()

    await transfer_stock_with_lot_trace(
        test_session,
        autopart_id=created_autopart.id,
        from_location_id=src_lot.storage_location_id,
        to_location_id=dst.id,
        quantity=3,
        notes="cost move",
    )
    await test_session.commit()

    dst_lot = (
        await test_session.execute(
            select(StockLot).where(
                StockLot.autopart_id == created_autopart.id,
                StockLot.storage_location_id == dst.id,
            )
        )
    ).scalar_one()
    assert dst_lot.cost_price == Decimal("87.6500")


@pytest.mark.asyncio
async def test_post_shipment_snapshots_fifo_costs_and_allocations(
    test_session: AsyncSession,
    created_autopart: AutoPart,
    created_providers: list[Provider],
):
    receipt_old = SupplierReceipt(
        provider_id=created_providers[0].id,
        document_number="R-OLD",
        document_date=date.today(),
    )
    receipt_old.items = [
        SupplierReceiptItem(
            autopart_id=created_autopart.id,
            received_quantity=2,
            price=Decimal("100.00"),
        )
    ]
    receipt_new = SupplierReceipt(
        provider_id=created_providers[1].id,
        document_number="R-NEW",
        document_date=date.today(),
    )
    receipt_new.items = [
        SupplierReceiptItem(
            autopart_id=created_autopart.id,
            received_quantity=3,
            price=Decimal("120.00"),
        )
    ]
    test_session.add_all([receipt_old, receipt_new])
    await test_session.flush()
    await receive_stock(test_session, receipt=receipt_old, reverse=False)
    await receive_stock(test_session, receipt=receipt_new, reverse=False)

    lots = (
        (
            await test_session.execute(
                select(StockLot)
                .where(StockLot.autopart_id == created_autopart.id)
                .order_by(StockLot.received_at, StockLot.id)
            )
        )
        .scalars()
        .all()
    )

    shipment = ShipmentDocument(
        status=ShipmentDocumentStatus.DRAFT,
        doc_number="SHIP-1",
    )
    test_session.add(shipment)
    await test_session.flush()
    test_session.add(
        ShipmentDocumentItem(
            document_id=shipment.id,
            autopart_id=created_autopart.id,
            storage_location_id=lots[0].storage_location_id,
            quantity=4,
            price=Decimal("150.00"),
        )
    )
    await test_session.flush()

    result = await post_shipment_document(test_session, shipment.id)
    await test_session.commit()

    shipment_item = (
        await test_session.execute(
            select(ShipmentDocumentItem).where(
                ShipmentDocumentItem.document_id == shipment.id
            )
        )
    ).scalar_one()
    allocations = (
        (
            await test_session.execute(
                select(ShipmentDocumentItemLotAllocation)
                .where(
                    ShipmentDocumentItemLotAllocation.shipment_document_item_id
                    == shipment_item.id
                )
                .order_by(ShipmentDocumentItemLotAllocation.id)
            )
        )
        .scalars()
        .all()
    )

    assert result["movements_created"] == 2
    assert shipment_item.lot_id == lots[0].id
    assert shipment_item.cost_total == Decimal("440.00")
    assert shipment_item.cost_price == Decimal("110.0000")
    assert [(a.provider_id, a.quantity, a.total_cost_price) for a in allocations] == [
        (created_providers[0].id, 2, Decimal("200.00")),
        (created_providers[1].id, 2, Decimal("240.00")),
    ]


@pytest.mark.asyncio
async def test_profit_report_groups_by_provider_and_month(
    test_session: AsyncSession,
    async_client,
    created_autopart: AutoPart,
    created_providers: list[Provider],
):
    receipt_old = SupplierReceipt(
        provider_id=created_providers[0].id,
        document_number="R-REPORT-OLD",
        document_date=date.today(),
    )
    receipt_old.items = [
        SupplierReceiptItem(
            autopart_id=created_autopart.id,
            received_quantity=1,
            price=Decimal("90.00"),
        )
    ]
    receipt_new = SupplierReceipt(
        provider_id=created_providers[1].id,
        document_number="R-REPORT-NEW",
        document_date=date.today(),
    )
    receipt_new.items = [
        SupplierReceiptItem(
            autopart_id=created_autopart.id,
            received_quantity=2,
            price=Decimal("110.00"),
        )
    ]
    test_session.add_all([receipt_old, receipt_new])
    await test_session.flush()
    await receive_stock(test_session, receipt=receipt_old, reverse=False)
    await receive_stock(test_session, receipt=receipt_new, reverse=False)

    lots = (
        (
            await test_session.execute(
                select(StockLot)
                .where(StockLot.autopart_id == created_autopart.id)
                .order_by(StockLot.received_at, StockLot.id)
            )
        )
        .scalars()
        .all()
    )

    shipment = ShipmentDocument(status=ShipmentDocumentStatus.DRAFT)
    test_session.add(shipment)
    await test_session.flush()
    test_session.add(
        ShipmentDocumentItem(
            document_id=shipment.id,
            autopart_id=created_autopart.id,
            storage_location_id=lots[0].storage_location_id,
            quantity=3,
            price=Decimal("150.00"),
        )
    )
    await test_session.flush()
    await post_shipment_document(test_session, shipment.id)
    await test_session.commit()

    response = await async_client.get(
        "/inventory/shipments/profit-report/",
        params={"period": "month"},
    )
    assert response.status_code == 200
    rows = response.json()
    assert len(rows) == 2
    by_provider = {row["provider_id"]: row for row in rows}
    assert by_provider[created_providers[0].id]["quantity"] == 1
    assert by_provider[created_providers[0].id]["revenue_total"] == "150.00"
    assert by_provider[created_providers[0].id]["cost_total"] == "90.00"
    assert by_provider[created_providers[0].id]["gross_profit"] == "60.00"
    assert by_provider[created_providers[1].id]["quantity"] == 2
    assert by_provider[created_providers[1].id]["revenue_total"] == "300.00"
    assert by_provider[created_providers[1].id]["cost_total"] == "220.00"
    assert by_provider[created_providers[1].id]["gross_profit"] == "80.00"


@pytest.mark.asyncio
async def test_profit_report_can_roll_up_by_customer_and_brand(
    test_session: AsyncSession,
    async_client,
    created_autopart: AutoPart,
    created_customers,
    created_providers: list[Provider],
):
    receipt_old = SupplierReceipt(
        provider_id=created_providers[0].id,
        document_number="R-ROLLUP-OLD",
        document_date=date.today(),
    )
    receipt_old.items = [
        SupplierReceiptItem(
            autopart_id=created_autopart.id,
            received_quantity=1,
            price=Decimal("90.00"),
        )
    ]
    receipt_new = SupplierReceipt(
        provider_id=created_providers[1].id,
        document_number="R-ROLLUP-NEW",
        document_date=date.today(),
    )
    receipt_new.items = [
        SupplierReceiptItem(
            autopart_id=created_autopart.id,
            received_quantity=2,
            price=Decimal("110.00"),
        )
    ]
    test_session.add_all([receipt_old, receipt_new])
    await test_session.flush()
    await receive_stock(test_session, receipt=receipt_old, reverse=False)
    await receive_stock(test_session, receipt=receipt_new, reverse=False)

    lot = (
        await test_session.execute(
            select(StockLot)
            .where(StockLot.autopart_id == created_autopart.id)
            .order_by(StockLot.received_at, StockLot.id)
        )
    ).scalars().first()

    shipment = ShipmentDocument(
        status=ShipmentDocumentStatus.DRAFT,
        customer_id=created_customers[0].id,
    )
    test_session.add(shipment)
    await test_session.flush()
    test_session.add(
        ShipmentDocumentItem(
            document_id=shipment.id,
            autopart_id=created_autopart.id,
            storage_location_id=lot.storage_location_id,
            quantity=3,
            price=Decimal("150.00"),
        )
    )
    await test_session.flush()
    await post_shipment_document(test_session, shipment.id)
    await test_session.commit()

    response = await async_client.get(
        "/inventory/shipments/profit-report/",
        params={
            "period": "month",
            "group_by_customer": True,
            "group_by_provider": False,
            "group_by_brand": True,
            "group_by_autopart": False,
        },
    )

    assert response.status_code == 200
    rows = response.json()
    assert len(rows) == 1
    row = rows[0]
    assert row["customer_id"] == created_customers[0].id
    assert row["brand_id"] == created_autopart.brand_id
    assert row["quantity"] == 3
    assert row["revenue_total"] == "450.00"
    assert row["cost_total"] == "310.00"
    assert row["gross_profit"] == "140.00"


@pytest.mark.asyncio
async def test_profit_report_export_returns_excel_workbook(
    test_session: AsyncSession,
    async_client,
    created_autopart: AutoPart,
    created_customers,
    created_providers: list[Provider],
):
    receipt = SupplierReceipt(
        provider_id=created_providers[0].id,
        document_number="R-EXPORT",
        document_date=date.today(),
    )
    receipt.items = [
        SupplierReceiptItem(
            autopart_id=created_autopart.id,
            received_quantity=1,
            price=Decimal("95.00"),
        )
    ]
    test_session.add(receipt)
    await test_session.flush()
    await receive_stock(test_session, receipt=receipt, reverse=False)

    lot = (
        await test_session.execute(
            select(StockLot).where(StockLot.source_receipt_id == receipt.id)
        )
    ).scalar_one()

    shipment = ShipmentDocument(
        status=ShipmentDocumentStatus.DRAFT,
        customer_id=created_customers[0].id,
    )
    test_session.add(shipment)
    await test_session.flush()
    test_session.add(
        ShipmentDocumentItem(
            document_id=shipment.id,
            autopart_id=created_autopart.id,
            storage_location_id=lot.storage_location_id,
            quantity=1,
            price=Decimal("150.00"),
        )
    )
    await test_session.flush()
    await post_shipment_document(test_session, shipment.id)
    await test_session.commit()

    response = await async_client.get(
        "/inventory/shipments/profit-report/export/",
        params={
            "period": "month",
            "group_by_customer": True,
            "group_by_provider": True,
            "group_by_brand": True,
            "group_by_autopart": True,
        },
    )

    assert response.status_code == 200
    assert (
        response.headers["content-type"]
        == "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
    assert "shipment_profit_report_month_" in response.headers[
        "content-disposition"
    ]
    assert response.content[:2] == b"PK"
    assert len(response.content) > 1000


@pytest.mark.asyncio
async def test_list_shipments_supports_profit_drilldown_filters(
    test_session: AsyncSession,
    async_client,
    created_autopart: AutoPart,
    created_customers,
    created_providers: list[Provider],
):
    receipt = SupplierReceipt(
        provider_id=created_providers[0].id,
        document_number="R-DRILLDOWN",
        document_date=date.today(),
    )
    receipt.items = [
        SupplierReceiptItem(
            autopart_id=created_autopart.id,
            received_quantity=2,
            price=Decimal("95.00"),
        )
    ]
    test_session.add(receipt)
    await test_session.flush()
    await receive_stock(test_session, receipt=receipt, reverse=False)

    lot = (
        await test_session.execute(
            select(StockLot).where(StockLot.source_receipt_id == receipt.id)
        )
    ).scalar_one()

    shipment = ShipmentDocument(
        status=ShipmentDocumentStatus.DRAFT,
        customer_id=created_customers[0].id,
        doc_number="SHIP-DRILLDOWN",
    )
    test_session.add(shipment)
    await test_session.flush()
    test_session.add(
        ShipmentDocumentItem(
            document_id=shipment.id,
            autopart_id=created_autopart.id,
            storage_location_id=lot.storage_location_id,
            quantity=1,
            price=Decimal("150.00"),
        )
    )
    await test_session.flush()
    await post_shipment_document(test_session, shipment.id)
    await test_session.commit()

    posted_from = (
        datetime.now(timezone.utc) - timedelta(days=1)
    ).isoformat()
    posted_to = (
        datetime.now(timezone.utc) + timedelta(days=1)
    ).isoformat()
    response = await async_client.get(
        "/inventory/shipments/",
        params={
            "status": "posted",
            "customer_id": created_customers[0].id,
            "autopart_id": created_autopart.id,
            "provider_id": created_providers[0].id,
            "posted_from": posted_from,
            "posted_to": posted_to,
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert len(payload) == 1
    assert payload[0]["id"] == shipment.id


@pytest.mark.asyncio
async def test_unpost_receipt_with_partial_consumption_keeps_audit_trace(
    test_session: AsyncSession,
    created_autopart: AutoPart,
    created_providers: list[Provider],
):
    receipt = SupplierReceipt(
        provider_id=created_providers[0].id,
        document_number="R-PARTIAL",
        document_date=date.today(),
    )
    receipt.items = [
        SupplierReceiptItem(
            autopart_id=created_autopart.id,
            received_quantity=5,
            gtd_code="GTD-PARTIAL",
        )
    ]
    test_session.add(receipt)
    await test_session.flush()
    await receive_stock(test_session, receipt=receipt, reverse=False)

    lot = (
        await test_session.execute(select(StockLot).where(StockLot.source_receipt_id == receipt.id))
    ).scalar_one()

    await reconcile_stock_absolute(
        test_session,
        autopart_id=created_autopart.id,
        storage_location_id=lot.storage_location_id,
        target_quantity=2,
        inventory_session_id=102,
    )
    await receive_stock(test_session, receipt=receipt, reverse=True)
    await test_session.commit()

    refreshed = await test_session.get(StockLot, lot.id)
    assert refreshed is not None
    assert refreshed.remaining_quantity == 0


@pytest.mark.asyncio
async def test_lot_sum_equals_stock_by_location_after_operation_chain(
    test_session: AsyncSession,
    created_autopart: AutoPart,
):
    loc_a = StorageLocation(name="LOCA")
    loc_b = StorageLocation(name="LOCB")
    test_session.add_all([loc_a, loc_b])
    await test_session.flush()

    # manual receipt
    doc = StockDocument(
        doc_type=StockDocumentType.MANUAL_RECEIPT,
        status=StockDocumentStatus.DRAFT,
    )
    test_session.add(doc)
    await test_session.flush()
    test_session.add(
        StockDocumentItem(
            document_id=doc.id,
            autopart_id=created_autopart.id,
            storage_location_id=loc_a.id,
            quantity=10,
            gtd_number="CHAIN-1",
        )
    )
    await test_session.flush()
    await post_stock_document(test_session, document_id=doc.id)

    # writeoff part
    out_doc = StockDocument(
        doc_type=StockDocumentType.MANUAL_WRITEOFF,
        status=StockDocumentStatus.DRAFT,
    )
    test_session.add(out_doc)
    await test_session.flush()
    test_session.add(
        StockDocumentItem(
            document_id=out_doc.id,
            autopart_id=created_autopart.id,
            storage_location_id=loc_a.id,
            quantity=3,
        )
    )
    await test_session.flush()
    await post_stock_document(test_session, document_id=out_doc.id)

    # transfer
    await transfer_stock_with_lot_trace(
        test_session,
        autopart_id=created_autopart.id,
        from_location_id=loc_a.id,
        to_location_id=loc_b.id,
        quantity=4,
    )

    # inventory correction
    await reconcile_stock_absolute(
        test_session,
        autopart_id=created_autopart.id,
        storage_location_id=loc_b.id,
        target_quantity=6,
        inventory_session_id=103,
    )
    await test_session.commit()

    for loc_id in (loc_a.id, loc_b.id):
        assert await _lot_sum(
            test_session,
            autopart_id=created_autopart.id,
            storage_location_id=loc_id,
        ) == await _sbl_qty(
            test_session,
            autopart_id=created_autopart.id,
            storage_location_id=loc_id,
        )


@pytest.mark.asyncio
async def test_backfill_opening_balance_infers_cost_from_known_lots(
    test_session: AsyncSession,
    created_autopart: AutoPart,
):
    source_location = StorageLocation(name="OBSRC")
    target_location = StorageLocation(name="OBTGT")
    test_session.add_all([source_location, target_location])
    await test_session.flush()

    doc = StockDocument(
        doc_type=StockDocumentType.MANUAL_RECEIPT,
        status=StockDocumentStatus.DRAFT,
    )
    test_session.add(doc)
    await test_session.flush()
    test_session.add(
        StockDocumentItem(
            document_id=doc.id,
            autopart_id=created_autopart.id,
            storage_location_id=source_location.id,
            quantity=5,
            cost_price=Decimal("33.33"),
        )
    )
    await test_session.flush()
    await post_stock_document(test_session, document_id=doc.id)

    test_session.add(
        StockByLocation(
            autopart_id=created_autopart.id,
            storage_location_id=target_location.id,
            quantity=4,
        )
    )
    await test_session.flush()

    result = await backfill_opening_balance_lots(test_session)
    await test_session.commit()

    opening_balance_lot = (
        await test_session.execute(
            select(StockLot).where(
                StockLot.autopart_id == created_autopart.id,
                StockLot.storage_location_id == target_location.id,
                StockLot.source_type == LotSourceType.OPENING_BALANCE,
            )
        )
    ).scalar_one()

    assert result["lots_created"] == 1
    assert opening_balance_lot.remaining_quantity == 4
    assert opening_balance_lot.cost_price == Decimal("33.3300")
