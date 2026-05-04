from __future__ import annotations

from datetime import date

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.models.autopart import AutoPart, StorageLocation
from dz_fastapi.models.inventory import (LotSourceType, StockByLocation,
                                         StockDocument, StockDocumentItem,
                                         StockDocumentStatus,
                                         StockDocumentType, StockLot)
from dz_fastapi.models.partner import (Provider, SupplierReceipt,
                                       SupplierReceiptItem)
from dz_fastapi.services.inventory_stock import (post_stock_document,
                                                 receive_stock,
                                                 reconcile_stock_absolute,
                                                 transfer_stock_with_lot_trace)


async def _lot_sum(
    session: AsyncSession,
    *,
    autopart_id: int,
    storage_location_id: int,
) -> int:
    stmt = select(
        func.coalesce(func.sum(StockLot.remaining_quantity), 0)
    ).where(
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
        document_number='R-1',
        document_date=date.today(),
    )
    receipt.items = [
        SupplierReceiptItem(
            autopart_id=created_autopart.id,
            received_quantity=5,
            gtd_code='GTD-001',
            country_code='CN',
            country_name='China',
        )
    ]
    test_session.add(receipt)
    await test_session.flush()

    await receive_stock(test_session, receipt=receipt, reverse=False)
    await test_session.commit()

    lots = (await test_session.execute(
        select(StockLot).where(StockLot.source_receipt_id == receipt.id)
    )).scalars().all()
    assert len(lots) == 1
    assert lots[0].source_type == LotSourceType.RECEIPT
    assert lots[0].gtd_number == 'GTD-001'
    assert lots[0].remaining_quantity == 5


@pytest.mark.asyncio
async def test_manual_receipt_document_creates_manual_lot(
    test_session: AsyncSession,
    created_autopart: AutoPart,
    created_storage: StorageLocation,
):
    doc = StockDocument(
        doc_type=StockDocumentType.MANUAL_RECEIPT,
        status=StockDocumentStatus.DRAFT,
        reason='manual in',
    )
    test_session.add(doc)
    await test_session.flush()
    test_session.add(StockDocumentItem(
        document_id=doc.id,
        autopart_id=created_autopart.id,
        storage_location_id=created_storage.id,
        quantity=4,
        gtd_number='GTD-MANUAL',
    ))
    await test_session.flush()

    await post_stock_document(test_session, document_id=doc.id)
    await test_session.commit()

    manual_lots = (await test_session.execute(
        select(StockLot).where(
            StockLot.autopart_id == created_autopart.id,
            StockLot.storage_location_id == created_storage.id,
            StockLot.source_type == LotSourceType.MANUAL,
        )
    )).scalars().all()
    assert len(manual_lots) == 1
    assert manual_lots[0].gtd_number == 'GTD-MANUAL'
    assert manual_lots[0].remaining_quantity == 4


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
    test_session.add_all([
        StockDocumentItem(
            document_id=in_doc_1.id,
            autopart_id=created_autopart.id,
            storage_location_id=created_storage.id,
            quantity=3,
            gtd_number='GTD-OLD',
        ),
        StockDocumentItem(
            document_id=in_doc_2.id,
            autopart_id=created_autopart.id,
            storage_location_id=created_storage.id,
            quantity=3,
            gtd_number='GTD-NEW',
        ),
    ])
    await test_session.flush()
    await post_stock_document(test_session, document_id=in_doc_1.id)
    await post_stock_document(test_session, document_id=in_doc_2.id)

    out_doc = StockDocument(
        doc_type=StockDocumentType.MANUAL_WRITEOFF,
        status=StockDocumentStatus.DRAFT,
    )
    test_session.add(out_doc)
    await test_session.flush()
    test_session.add(StockDocumentItem(
        document_id=out_doc.id,
        autopart_id=created_autopart.id,
        storage_location_id=created_storage.id,
        quantity=4,
    ))
    await test_session.flush()
    await post_stock_document(test_session, document_id=out_doc.id)
    await test_session.commit()

    lots = (await test_session.execute(
        select(StockLot)
        .where(
            StockLot.autopart_id == created_autopart.id,
            StockLot.storage_location_id == created_storage.id,
            StockLot.gtd_number.in_(['GTD-OLD', 'GTD-NEW']),
        )
        .order_by(StockLot.received_at, StockLot.id)
    )).scalars().all()
    by_gtd = {lot.gtd_number: lot for lot in lots}
    assert by_gtd['GTD-OLD'].remaining_quantity == 0
    assert by_gtd['GTD-NEW'].remaining_quantity == 2


@pytest.mark.asyncio
async def test_inventory_shortage_uses_fifo(
    test_session: AsyncSession,
    created_autopart: AutoPart,
    created_storage: StorageLocation,
):
    for qty, gtd in ((3, 'INV-A'), (2, 'INV-B')):
        doc = StockDocument(
            doc_type=StockDocumentType.MANUAL_RECEIPT,
            status=StockDocumentStatus.DRAFT,
        )
        test_session.add(doc)
        await test_session.flush()
        test_session.add(StockDocumentItem(
            document_id=doc.id,
            autopart_id=created_autopart.id,
            storage_location_id=created_storage.id,
            quantity=qty,
            gtd_number=gtd,
        ))
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

    lots = (await test_session.execute(
        select(StockLot)
        .where(
            StockLot.autopart_id == created_autopart.id,
            StockLot.storage_location_id == created_storage.id,
        )
    )).scalars().all()
    by_gtd = {lot.gtd_number: lot.remaining_quantity for lot in lots}
    assert by_gtd['INV-A'] == 0
    assert by_gtd['INV-B'] == 1


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

    correction_lot = (await test_session.execute(
        select(StockLot).where(
            StockLot.autopart_id == created_autopart.id,
            StockLot.storage_location_id == created_storage.id,
            StockLot.source_type == LotSourceType.INVENTORY_CORRECTION,
        )
    )).scalar_one_or_none()
    assert correction_lot is not None
    assert correction_lot.remaining_quantity == 7


@pytest.mark.asyncio
async def test_transfer_keeps_lot_trace_data(
    test_session: AsyncSession,
    created_autopart: AutoPart,
):
    src = StorageLocation(name='TRSRC')
    dst = StorageLocation(name='TRDST')
    test_session.add_all([src, dst])
    await test_session.flush()

    doc = StockDocument(
        doc_type=StockDocumentType.MANUAL_RECEIPT,
        status=StockDocumentStatus.DRAFT,
    )
    test_session.add(doc)
    await test_session.flush()
    test_session.add(StockDocumentItem(
        document_id=doc.id,
        autopart_id=created_autopart.id,
        storage_location_id=src.id,
        quantity=6,
        gtd_number='TR-GTD',
    ))
    await test_session.flush()
    await post_stock_document(test_session, document_id=doc.id)

    await transfer_stock_with_lot_trace(
        test_session,
        autopart_id=created_autopart.id,
        from_location_id=src.id,
        to_location_id=dst.id,
        quantity=4,
        notes='move',
    )
    await test_session.commit()

    dst_lots = (await test_session.execute(
        select(StockLot).where(
            StockLot.autopart_id == created_autopart.id,
            StockLot.storage_location_id == dst.id,
        )
    )).scalars().all()
    assert len(dst_lots) == 1
    assert dst_lots[0].gtd_number == 'TR-GTD'
    assert dst_lots[0].remaining_quantity == 4


@pytest.mark.asyncio
async def test_unpost_receipt_with_partial_consumption_keeps_audit_trace(
    test_session: AsyncSession,
    created_autopart: AutoPart,
    created_providers: list[Provider],
):
    receipt = SupplierReceipt(
        provider_id=created_providers[0].id,
        document_number='R-PARTIAL',
        document_date=date.today(),
    )
    receipt.items = [
        SupplierReceiptItem(
            autopart_id=created_autopart.id,
            received_quantity=5,
            gtd_code='GTD-PARTIAL',
        )
    ]
    test_session.add(receipt)
    await test_session.flush()
    await receive_stock(test_session, receipt=receipt, reverse=False)

    lot = (await test_session.execute(
        select(StockLot).where(StockLot.source_receipt_id == receipt.id)
    )).scalar_one()

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
    loc_a = StorageLocation(name='LOCA')
    loc_b = StorageLocation(name='LOCB')
    test_session.add_all([loc_a, loc_b])
    await test_session.flush()

    # manual receipt
    doc = StockDocument(
        doc_type=StockDocumentType.MANUAL_RECEIPT,
        status=StockDocumentStatus.DRAFT,
    )
    test_session.add(doc)
    await test_session.flush()
    test_session.add(StockDocumentItem(
        document_id=doc.id,
        autopart_id=created_autopart.id,
        storage_location_id=loc_a.id,
        quantity=10,
        gtd_number='CHAIN-1',
    ))
    await test_session.flush()
    await post_stock_document(test_session, document_id=doc.id)

    # writeoff part
    out_doc = StockDocument(
        doc_type=StockDocumentType.MANUAL_WRITEOFF,
        status=StockDocumentStatus.DRAFT,
    )
    test_session.add(out_doc)
    await test_session.flush()
    test_session.add(StockDocumentItem(
        document_id=out_doc.id,
        autopart_id=created_autopart.id,
        storage_location_id=loc_a.id,
        quantity=3,
    ))
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
