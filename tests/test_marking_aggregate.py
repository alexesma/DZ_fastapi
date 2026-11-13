from datetime import date
from decimal import Decimal

import pytest
from sqlalchemy import select
from sqlalchemy.orm import selectinload

from dz_fastapi.models.inventory import (
    ReceiptScanCount,
    ShipmentDocument,
    ShipmentDocumentItem,
    ShipmentDocumentStatus,
    StockLot,
)
from dz_fastapi.models.partner import SupplierReceipt, SupplierReceiptItem
from dz_fastapi.services import marking_aggregate, receipt_marking_scan, shipment_marking_scan
from dz_fastapi.services.inventory_stock import post_shipment_document, receive_stock
from dz_fastapi.services.marking_aggregate import (
    AggregateError,
    expand_aggregate,
    parse_transport_code,
)
from dz_fastapi.services.receipt_marking_scan import marking_scan_state, record_aggregate_scan
from dz_fastapi.services.shipment_marking_scan import record_shipment_aggregate_scan

GTIN = "04600000000017"
CODE_A = f"01{GTIN}21AAAAAAAAAAAAA"
CODE_B = f"01{GTIN}21BBBBBBBBBBBBB"
CODE_C = f"01{GTIN}21CCCCCCCCCCCCC"
FOREIGN = f"01{GTIN}21ZZZZZZZZZZZZZ"
GROUP = f"01{GTIN}21GROUP00000001"
SSCC = "00146000000000000017"


class FakeGisMt:
    """Палета → групповая упаковка (A, B) и отдельная штука C."""

    tree = {SSCC: [GROUP, CODE_C], GROUP: [CODE_A, CODE_B]}

    async def get_cises_info(self, codes):
        return [
            {"cisInfo": {"requestedCis": code, "child": self.tree.get(code, [])}}
            for code in codes
        ]


@pytest.fixture
def gis_mt(monkeypatch):
    async def client(session):
        return FakeGisMt()

    for module in (marking_aggregate, receipt_marking_scan, shipment_marking_scan):
        monkeypatch.setattr(module, "gis_mt_client_or_none", client)


def test_transport_code_is_recognized():
    assert parse_transport_code(SSCC) == SSCC
    assert parse_transport_code("]C1" + SSCC) == SSCC
    assert parse_transport_code("(00)" + SSCC[2:]) == SSCC
    assert parse_transport_code(CODE_A) is None
    assert parse_transport_code("4600000000017") is None


@pytest.mark.asyncio
async def test_nested_packages_expand_to_units(test_session, gis_mt):
    assert await expand_aggregate(test_session, SSCC) == [CODE_C, CODE_A, CODE_B]
    assert await expand_aggregate(test_session, GROUP) == [CODE_A, CODE_B]
    # Отдельная штука — не упаковка.
    assert await expand_aggregate(test_session, CODE_A) == []


@pytest.mark.asyncio
async def test_expanding_needs_a_gis_mt_login(test_session):
    with pytest.raises(AggregateError):
        await expand_aggregate(test_session, SSCC)


async def _receipt(session, provider, autopart, codes):
    receipt = SupplierReceipt(
        provider_id=provider.id,
        document_number="R-AGG",
        document_date=date.today(),
    )
    receipt.items = [
        SupplierReceiptItem(
            autopart_id=autopart.id,
            received_quantity=len(codes),
            price=Decimal("100.00"),
            marking_codes=codes,
        )
    ]
    session.add(receipt)
    await session.commit()
    return (
        await session.execute(
            select(SupplierReceipt)
            .where(SupplierReceipt.id == receipt.id)
            .options(selectinload(SupplierReceipt.items))
        )
    ).scalar_one()


@pytest.mark.asyncio
async def test_pallet_scan_on_receiving_confirms_and_counts_units(
    test_session, created_autopart, created_providers, gis_mt
):
    receipt = await _receipt(
        test_session, created_providers[0], created_autopart, [CODE_A, CODE_B]
    )
    item_id = receipt.items[0].id
    result = await record_aggregate_scan(test_session, receipt, SSCC)
    # В документе две штуки из трёх, что лежат на палете.
    assert (result["units"], result["confirmed"], result["extra"]) == (3, 2, 1)
    state = await marking_scan_state(test_session, receipt)
    assert state["missing"] == 0
    assert [row["code"] for row in state["extra"]] == [CODE_C]
    accepted = await test_session.scalar(
        select(ReceiptScanCount.accepted_qty).where(
            ReceiptScanCount.receipt_item_id == item_id
        )
    )
    assert accepted == 2

    again = await record_aggregate_scan(test_session, receipt, SSCC)
    assert again["duplicates"] == 3
    # Код группы выглядит как обычный, но раскрывается так же.
    assert (await record_aggregate_scan(test_session, receipt, GROUP))[
        "units"
    ] == 2
    # Обычный код из документа упаковкой не считается.
    assert await record_aggregate_scan(test_session, receipt, CODE_A) is None


@pytest.mark.asyncio
async def test_group_scan_on_shipping_pins_every_unit(
    test_session, created_autopart, created_providers, gis_mt, monkeypatch
):
    receipt = await _receipt(
        test_session, created_providers[0], created_autopart, [CODE_A, CODE_B]
    )
    await receive_stock(test_session, receipt=receipt, reverse=False)
    await test_session.commit()
    lot = (
        await test_session.execute(
            select(StockLot).where(StockLot.autopart_id == created_autopart.id)
        )
    ).scalars().first()
    shipment = ShipmentDocument(
        status=ShipmentDocumentStatus.DRAFT, doc_number="SHIP-AGG"
    )
    test_session.add(shipment)
    await test_session.flush()
    test_session.add(
        ShipmentDocumentItem(
            document_id=shipment.id,
            autopart_id=created_autopart.id,
            storage_location_id=lot.storage_location_id,
            quantity=2,
            price=Decimal("150.00"),
        )
    )
    await test_session.commit()
    shipment_id = shipment.id

    async def load():
        test_session.expire_all()
        return (
            await test_session.execute(
                select(ShipmentDocument)
                .where(ShipmentDocument.id == shipment_id)
                .options(
                    selectinload(ShipmentDocument.items).selectinload(
                        ShipmentDocumentItem.allocations
                    )
                )
            )
        ).scalar_one()

    # На палете есть штука C, которой нет в нашем учёте: её пропускаем.
    result = await record_shipment_aggregate_scan(
        test_session, await load(), SSCC
    )
    await test_session.commit()
    assert (result["units"], result["accepted"]) == (3, 2)
    assert [row["code"] for row in result["errors"]] == [CODE_C]

    await post_shipment_document(test_session, shipment_id)
    await test_session.commit()
    document = await load()
    codes = [
        code
        for allocation in document.items[0].allocations
        for code in allocation.marking_codes
    ]
    assert sorted(codes) == [CODE_A, CODE_B]
