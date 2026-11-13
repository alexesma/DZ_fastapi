from datetime import date
from decimal import Decimal

import pytest
from sqlalchemy import select
from sqlalchemy.orm import selectinload

from dz_fastapi.models.inventory import (
    MarkingCodeStatus,
    ProductMarkingCode,
    ShipmentDocument,
    ShipmentDocumentItem,
    ShipmentDocumentStatus,
    StockLot,
)
from dz_fastapi.models.partner import SupplierReceipt, SupplierReceiptItem
from dz_fastapi.models.settings import DiadocIntegrationSettings, GisMtSettings
from dz_fastapi.models.user import User, UserRole, UserStatus
from dz_fastapi.services.diadoc_outgoing import build_shipment_formalized_readiness
from dz_fastapi.services.inventory_stock import (
    post_shipment_document,
    receive_stock,
    unpost_shipment_document,
)
from dz_fastapi.services.shipment_marking_scan import (
    ShipmentMarkingScanError,
    record_shipment_marking_scan,
    shipment_marking_state,
)

GTIN = "04600000000017"
CODE_A = f"01{GTIN}21AAAAAAAAAAAAA"
CODE_B = f"01{GTIN}21BBBBBBBBBBBBB"
CODE_C = f"01{GTIN}21CCCCCCCCCCCCC"
CODE_D = f"01{GTIN}21DDDDDDDDDDDDD"


async def _stock_with_codes(session, provider, autopart):
    receipt = SupplierReceipt(
        provider_id=provider.id,
        document_number="R-SHIP-MARK",
        document_date=date.today(),
    )
    receipt.items = [
        SupplierReceiptItem(
            autopart_id=autopart.id,
            received_quantity=3,
            price=Decimal("100.00"),
            marking_codes=[CODE_A, CODE_B, CODE_C],
        )
    ]
    session.add(receipt)
    await session.flush()
    await receive_stock(session, receipt=receipt, reverse=False)
    await session.commit()
    return (
        await session.execute(
            select(StockLot).where(StockLot.autopart_id == autopart.id)
        )
    ).scalars().first()


async def _draft(session, autopart, lot, quantity=1):
    shipment = ShipmentDocument(
        status=ShipmentDocumentStatus.DRAFT, doc_number="SHIP-MARK"
    )
    session.add(shipment)
    await session.flush()
    session.add(
        ShipmentDocumentItem(
            document_id=shipment.id,
            autopart_id=autopart.id,
            storage_location_id=lot.storage_location_id,
            quantity=quantity,
            price=Decimal("150.00"),
        )
    )
    await session.commit()
    return shipment.id


async def _load(session, shipment_id):
    session.expire_all()
    return (
        await session.execute(
            select(ShipmentDocument)
            .where(ShipmentDocument.id == shipment_id)
            .options(
                selectinload(ShipmentDocument.items).selectinload(
                    ShipmentDocumentItem.allocations
                )
            )
        )
    ).scalar_one()


async def _statuses(session):
    session.expire_all()
    rows = (await session.execute(select(ProductMarkingCode))).scalars().all()
    return {row.code: row.status for row in rows}


def _codes(document):
    return [
        code
        for item in document.items
        for allocation in item.allocations
        for code in (allocation.marking_codes or [])
    ]


@pytest.mark.asyncio
async def test_code_scanned_before_posting_goes_to_the_shipment(
    test_session, created_autopart, created_providers
):
    lot = await _stock_with_codes(
        test_session, created_providers[0], created_autopart
    )
    shipment_id = await _draft(test_session, created_autopart, lot)
    document = await _load(test_session, shipment_id)
    result = await record_shipment_marking_scan(test_session, document, CODE_C)
    assert result["duplicate"] is False
    await test_session.commit()

    await post_shipment_document(test_session, shipment_id)
    await test_session.commit()

    document = await _load(test_session, shipment_id)
    # По FIFO ушёл бы первый код партии, а уходит отсканированный.
    assert _codes(document) == [CODE_C]
    statuses = await _statuses(test_session)
    assert statuses[CODE_C] == MarkingCodeStatus.SHIPPED
    assert statuses[CODE_A] == MarkingCodeStatus.IN_STOCK


@pytest.mark.asyncio
async def test_scan_after_posting_replaces_the_auto_assigned_code(
    test_session, created_autopart, created_providers
):
    lot = await _stock_with_codes(
        test_session, created_providers[0], created_autopart
    )
    shipment_id = await _draft(test_session, created_autopart, lot)
    await post_shipment_document(test_session, shipment_id)
    await test_session.commit()
    document = await _load(test_session, shipment_id)
    assert _codes(document) == [CODE_A]

    result = await record_shipment_marking_scan(test_session, document, CODE_B)
    await test_session.commit()
    assert result["replaced_auto_code"] is True

    document = await _load(test_session, shipment_id)
    assert _codes(document) == [CODE_B]
    statuses = await _statuses(test_session)
    assert statuses[CODE_A] == MarkingCodeStatus.IN_STOCK
    assert statuses[CODE_B] == MarkingCodeStatus.SHIPPED
    document = await _load(test_session, shipment_id)
    state = await shipment_marking_state(test_session, document)
    assert (state["expected"], state["assigned"], state["scanned"]) == (1, 1, 1)

    # Все штуки строки уже отсканированы — второй код не нужен.
    with pytest.raises(ShipmentMarkingScanError):
        await record_shipment_marking_scan(test_session, document, CODE_C)


@pytest.mark.asyncio
async def test_without_auto_assignment_only_scanned_codes_are_shipped(
    test_session, created_autopart, created_providers
):
    test_session.add(GisMtSettings(shipment_codes_auto_fifo=False))
    lot = await _stock_with_codes(
        test_session, created_providers[0], created_autopart
    )
    shipment_id = await _draft(test_session, created_autopart, lot, quantity=2)
    await post_shipment_document(test_session, shipment_id)
    await test_session.commit()
    document = await _load(test_session, shipment_id)
    assert _codes(document) == []

    await record_shipment_marking_scan(test_session, document, CODE_B)
    await test_session.commit()
    document = await _load(test_session, shipment_id)
    assert _codes(document) == [CODE_B]
    state = await shipment_marking_state(test_session, document)
    assert state["auto_fifo"] is False
    assert (state["expected"], state["assigned"], state["scanned"]) == (2, 1, 1)


@pytest.mark.asyncio
async def test_unknown_and_repeated_codes(
    test_session, created_autopart, created_providers
):
    lot = await _stock_with_codes(
        test_session, created_providers[0], created_autopart
    )
    shipment_id = await _draft(test_session, created_autopart, lot, quantity=2)
    document = await _load(test_session, shipment_id)
    with pytest.raises(ShipmentMarkingScanError):
        await record_shipment_marking_scan(
            test_session, document, f"01{GTIN}21ZZZZZZZZZZZZZ"
        )
    await record_shipment_marking_scan(test_session, document, CODE_A)
    again = await record_shipment_marking_scan(test_session, document, CODE_A)
    assert again["duplicate"] is True

    # Код, отсканированный в одной непроведённой отгрузке, не берётся в другую.
    other_id = await _draft(test_session, created_autopart, lot)
    other = await _load(test_session, other_id)
    with pytest.raises(ShipmentMarkingScanError):
        await record_shipment_marking_scan(test_session, other, CODE_A)
    await post_shipment_document(test_session, other_id)
    await test_session.commit()
    other = await _load(test_session, other_id)
    assert _codes(other) == [CODE_B]


@pytest.mark.asyncio
async def test_endpoints(
    async_client, test_session, created_autopart, created_providers
):
    test_session.add(
        User(
            id=1,
            name="Test Admin",
            email="test-admin@example.com",
            password_hash="unused",
            role=UserRole.ADMIN,
            status=UserStatus.ACTIVE,
        )
    )
    lot = await _stock_with_codes(
        test_session, created_providers[0], created_autopart
    )
    response = await async_client.put(
        "/inventory/gis-mt/shipment-codes-auto-fifo", params={"enabled": False}
    )
    assert response.json()["shipment_codes_auto_fifo"] is False

    shipment_id = await _draft(test_session, created_autopart, lot, quantity=2)
    await post_shipment_document(test_session, shipment_id)
    await test_session.commit()

    url = f"/inventory/shipments/{shipment_id}/marking-scan"
    response = await async_client.post(url, json={"code": CODE_C})
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["state"]["lines"][0]["scanned"] == 1

    response = await async_client.post(url, json={"code": "garbage"})
    assert response.status_code == 409

    response = await async_client.post(f"{url}/auto-assign")
    assert response.status_code == 200, response.text
    assert response.json()["assigned"] == 1
    state = (await async_client.get(url)).json()
    line = state["lines"][0]
    assert (line["quantity"], line["assigned"], line["scanned"]) == (2, 2, 1)
    assert line["unscanned_codes"] == [CODE_A]


@pytest.mark.asyncio
async def test_missing_codes_block_the_formalized_upd(
    test_session, created_autopart, created_providers
):
    test_session.add(GisMtSettings(shipment_codes_auto_fifo=False))
    lot = await _stock_with_codes(
        test_session, created_providers[0], created_autopart
    )
    shipment_id = await _draft(test_session, created_autopart, lot, quantity=2)
    await post_shipment_document(test_session, shipment_id)
    await test_session.commit()

    async def blockers():
        test_session.expire_all()
        readiness = await build_shipment_formalized_readiness(
            test_session,
            shipment_id=shipment_id,
            integration=DiadocIntegrationSettings(),
        )
        return [
            item
            for item in readiness["missing_required_fields"]
            if item.startswith("КИЗ")
        ]

    assert len(await blockers()) == 1
    for code in (CODE_A, CODE_C):
        document = await _load(test_session, shipment_id)
        await record_shipment_marking_scan(test_session, document, code)
        await test_session.commit()
    assert await blockers() == []


async def _second_lot(session, provider, autopart):
    receipt = SupplierReceipt(
        provider_id=provider.id,
        document_number="R-SHIP-MARK-2",
        document_date=date.today(),
    )
    receipt.items = [
        SupplierReceiptItem(
            autopart_id=autopart.id,
            received_quantity=1,
            price=Decimal("250.00"),
            marking_codes=[CODE_D],
        )
    ]
    session.add(receipt)
    await session.flush()
    await receive_stock(session, receipt=receipt, reverse=False)
    await session.commit()


async def _lots(session, autopart_id):
    session.expire_all()
    rows = (
        await session.execute(
            select(StockLot.remaining_quantity, StockLot.id)
            .where(StockLot.autopart_id == autopart_id)
            .order_by(StockLot.id)
        )
    ).all()
    return [(int(left), lot_id) for left, lot_id in rows]


@pytest.mark.asyncio
async def test_scanned_code_moves_the_write_off_to_its_lot(
    test_session, created_autopart, created_providers
):
    autopart_id = created_autopart.id
    lot = await _stock_with_codes(
        test_session, created_providers[0], created_autopart
    )
    await _second_lot(test_session, created_providers[0], created_autopart)
    shipment_id = await _draft(test_session, created_autopart, lot)
    document = await _load(test_session, shipment_id)
    await record_shipment_marking_scan(test_session, document, CODE_D)
    await test_session.commit()

    await post_shipment_document(test_session, shipment_id)
    await test_session.commit()

    # По FIFO списалась бы первая партия, но в посылке штука из второй.
    (first_left, _), (second_left, second_id) = await _lots(
        test_session, autopart_id
    )
    assert (first_left, second_left) == (3, 0)
    document = await _load(test_session, shipment_id)
    (allocation,) = document.items[0].allocations
    assert allocation.stock_lot_id == second_id
    assert allocation.marking_codes == [CODE_D]
    assert allocation.unit_cost_price == Decimal("250.0000")
    assert document.items[0].cost_total == Decimal("250.00")

    await unpost_shipment_document(test_session, shipment_id)
    await test_session.commit()
    assert [left for left, _ in await _lots(test_session, autopart_id)] == [
        3,
        1,
    ]


@pytest.mark.asyncio
async def test_scan_after_posting_also_moves_the_lot(
    test_session, created_autopart, created_providers
):
    autopart_id = created_autopart.id
    lot = await _stock_with_codes(
        test_session, created_providers[0], created_autopart
    )
    await _second_lot(test_session, created_providers[0], created_autopart)
    shipment_id = await _draft(test_session, created_autopart, lot)
    await post_shipment_document(test_session, shipment_id)
    await test_session.commit()
    assert [left for left, _ in await _lots(test_session, autopart_id)] == [
        2,
        1,
    ]

    document = await _load(test_session, shipment_id)
    await record_shipment_marking_scan(test_session, document, CODE_D)
    await test_session.commit()

    assert [left for left, _ in await _lots(test_session, autopart_id)] == [
        3,
        0,
    ]
    document = await _load(test_session, shipment_id)
    assert _codes(document) == [CODE_D]
    statuses = await _statuses(test_session)
    assert statuses[CODE_A] == MarkingCodeStatus.IN_STOCK
