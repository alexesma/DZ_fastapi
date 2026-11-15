from datetime import date
from decimal import Decimal

import pytest
from sqlalchemy import select

from dz_fastapi.models.inventory import (
    ShipmentDocument,
    ShipmentDocumentItem,
    ShipmentDocumentStatus,
    StockLot,
)
from dz_fastapi.models.partner import SupplierReceipt, SupplierReceiptItem
from dz_fastapi.services.document_numbering import (
    KIND_SHIPMENT,
    get_numbering,
    next_document_number,
    update_numbering,
)
from dz_fastapi.services.inventory_stock import (
    post_shipment_document,
    receive_stock,
    unpost_shipment_document,
)
from dz_fastapi.services.one_c_enterprise_data_export import DATA_NS, event_body_objects
from dz_fastapi.services.one_c_outbox import build_shipment_snapshot

COMPANY = {"short_name": "Драгонзап", "inn": "1234567890", "kpp": "123456789"}


@pytest.mark.asyncio
async def test_numbers_are_sequential_and_off_by_default(test_session):
    assert await next_document_number(test_session, KIND_SHIPMENT) is None

    await update_numbering(
        test_session, KIND_SHIPMENT, enabled=True, prefix="ДЗ-", digits=6
    )
    assert await next_document_number(test_session, KIND_SHIPMENT) == "ДЗ-000001"
    assert await next_document_number(test_session, KIND_SHIPMENT) == "ДЗ-000002"

    # Продолжить серию бухгалтера с нужного номера.
    await update_numbering(
        test_session, KIND_SHIPMENT, enabled=True, prefix="ДЗ-", digits=6, last_value=499
    )
    assert await next_document_number(test_session, KIND_SHIPMENT) == "ДЗ-000500"
    shipment = next(
        row for row in await get_numbering(test_session) if row["kind"] == KIND_SHIPMENT
    )
    assert shipment["next_number"] == "ДЗ-000501"


@pytest.mark.asyncio
async def test_number_that_does_not_fit_unf_is_rejected(test_session):
    with pytest.raises(ValueError):
        await update_numbering(
            test_session, KIND_SHIPMENT, enabled=True, prefix="ДРАГОН-", digits=6
        )


async def _posted_shipment(session, provider, autopart, customer):
    receipt = SupplierReceipt(
        provider_id=provider.id, document_number="R-NUM", document_date=date.today()
    )
    receipt.items = [
        SupplierReceiptItem(
            autopart_id=autopart.id, received_quantity=5, price=Decimal("100.00")
        )
    ]
    session.add(receipt)
    await session.flush()
    await receive_stock(session, receipt=receipt, reverse=False)
    lot = (
        await session.execute(
            select(StockLot).where(StockLot.autopart_id == autopart.id)
        )
    ).scalars().first()
    shipment = ShipmentDocument(
        status=ShipmentDocumentStatus.DRAFT,
        doc_number="SHP-CO-000777",
        customer_id=customer.id,
    )
    session.add(shipment)
    await session.flush()
    session.add(
        ShipmentDocumentItem(
            document_id=shipment.id,
            autopart_id=autopart.id,
            storage_location_id=lot.storage_location_id,
            quantity=1,
            price=Decimal("150.00"),
        )
    )
    await session.commit()
    return shipment.id


@pytest.mark.asyncio
async def test_posting_assigns_the_number_once_and_sends_it_to_unf(
    test_session, created_autopart, created_providers, created_customers
):
    await update_numbering(
        test_session, KIND_SHIPMENT, enabled=True, prefix="ДЗ-", digits=6
    )
    shipment_id = await _posted_shipment(
        test_session, created_providers[0], created_autopart, created_customers[0]
    )

    await post_shipment_document(test_session, shipment_id)
    await test_session.commit()
    number = await test_session.scalar(
        select(ShipmentDocument.doc_number).where(ShipmentDocument.id == shipment_id)
    )
    assert number == "ДЗ-000001"

    # В УНФ уходит тот же номер, что будет в УПД.
    snapshot = await build_shipment_snapshot(test_session, shipment_id)
    document = event_body_objects("shipment", "posted", snapshot, COMPANY)[-1]
    assert (
        document.find(f"{{{DATA_NS}}}КлючевыеСвойства/{{{DATA_NS}}}Номер").text
        == "ДЗ-000001"
    )

    # Отмена проведения номер не освобождает: он уже мог уйти клиенту.
    await unpost_shipment_document(test_session, shipment_id)
    await test_session.commit()
    assert await test_session.scalar(
        select(ShipmentDocument.doc_number).where(ShipmentDocument.id == shipment_id)
    ) == "ДЗ-000001"
    assert await next_document_number(test_session, KIND_SHIPMENT) == "ДЗ-000002"


@pytest.mark.asyncio
async def test_without_own_numbering_the_document_keeps_its_number(
    test_session, created_autopart, created_providers, created_customers
):
    shipment_id = await _posted_shipment(
        test_session, created_providers[0], created_autopart, created_customers[0]
    )
    await post_shipment_document(test_session, shipment_id)
    await test_session.commit()
    assert await test_session.scalar(
        select(ShipmentDocument.doc_number).where(ShipmentDocument.id == shipment_id)
    ) == "SHP-CO-000777"
    snapshot = await build_shipment_snapshot(test_session, shipment_id)
    document = event_body_objects("shipment", "posted", snapshot, COMPANY)[-1]
    assert document.find(
        f"{{{DATA_NS}}}КлючевыеСвойства/{{{DATA_NS}}}Номер"
    ).text == f"DZ{shipment_id:09d}"
