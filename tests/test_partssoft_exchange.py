from decimal import Decimal

import pytest
from sqlalchemy import func, select

from dz_fastapi.models.finance import PaymentInvoice
from dz_fastapi.models.partner import (
    CustomerExternalReference,
    ProviderExternalReference,
    SupplierReceipt,
    SupplierReceiptItem,
)
from dz_fastapi.models.partssoft import PartsSoftDocumentSnapshot, PartsSoftProductOutbox
from dz_fastapi.services import partssoft_exchange as service
from dz_fastapi.services import partssoft_order_reconciliation as reconciliation
from dz_fastapi.services.partssoft_reconciliation import PARTS_SOFT_SOURCE


@pytest.mark.asyncio
async def test_product_outbox_creates_remote_product_and_marks_sent(
    test_session,
    created_autopart,
    monkeypatch,
):
    row = PartsSoftProductOutbox(
        autopart_id=created_autopart.id,
        operation="upsert",
        status="pending",
    )
    test_session.add(row)
    await test_session.commit()

    async def fake_upsert(autopart):
        assert autopart.id == created_autopart.id
        return 88001, {"product": {"id": 88001}}, []

    monkeypatch.setattr(service, "_send_product_upsert", fake_upsert)
    result = await service.process_product_outbox(test_session)

    assert result == {"processed": 1, "counts": {"upserted": 1}}
    await test_session.refresh(row)
    await test_session.refresh(created_autopart)
    assert row.status == "sent"
    assert row.external_product_id == 88001
    assert created_autopart.partssoft_product_id == 88001


@pytest.mark.asyncio
async def test_product_outbox_records_remote_error_after_rollback(
    test_session,
    created_autopart,
    monkeypatch,
):
    row = PartsSoftProductOutbox(
        autopart_id=created_autopart.id,
        operation="upsert",
        status="pending",
    )
    test_session.add(row)
    await test_session.commit()
    row_id = row.id

    async def fake_upsert(_autopart):
        raise RuntimeError("Parts-Soft rejected product")

    monkeypatch.setattr(service, "_send_product_upsert", fake_upsert)
    result = await service.process_product_outbox(test_session)

    assert result == {"processed": 1, "counts": {"errors": 1}}
    failed = await test_session.get(PartsSoftProductOutbox, row_id)
    assert failed.status == "error"
    assert failed.attempts == 1
    assert failed.locked_at is None
    assert failed.last_error == "Parts-Soft rejected product"


@pytest.mark.asyncio
async def test_document_sync_imports_matched_invoices_once(
    test_session,
    created_customers,
    created_providers,
    created_autopart,
    created_brand,
    monkeypatch,
):
    customer = created_customers[0]
    provider = created_providers[0]
    test_session.add_all(
        [
            CustomerExternalReference(
                customer_id=customer.id,
                source_system=PARTS_SOFT_SOURCE,
                external_customer_id=701,
                is_active=True,
                is_verified=True,
            ),
            ProviderExternalReference(
                provider_id=provider.id,
                source_system=PARTS_SOFT_SOURCE,
                external_supplier_id=702,
                is_active=True,
            ),
        ]
    )
    await test_session.commit()

    customer_invoice = {
        "id": 9001,
        "customer_id": 701,
        "no": "РН-1",
        "sum": 1200,
        "nds_percent": 20,
        "status": "created",
        "created_at": "2026-09-12T10:00:00+03:00",
        "invoice_items": [
            {
                "id": 1,
                "oem": created_autopart.oem_number,
                "make_name": created_brand.name,
                "detail_name": created_autopart.name,
                "qnt": 2,
                "price": 600,
                "effective_nds_percent": 20,
            }
        ],
    }
    supplier_invoice = {
        "id": 9002,
        "customer_id": 702,
        "no": "УПД-2",
        "sum": 800,
        "nds_percent": 20,
        "status": "created",
        "created_at": "2026-09-12T11:00:00+03:00",
        "doc_guid": "document-guid",
        "invoice_items": [
            {
                "id": 2,
                "oem": created_autopart.oem_number,
                "make_name": created_brand.name,
                "detail_name": created_autopart.name,
                "qnt": 1,
                "price": 800,
                "source_sum": 800,
                "marking": {"income_codes": ["010123456789012321ABC"]},
            }
        ],
    }

    async def fake_fetch(endpoint, collection_key, *, since):
        del endpoint, since
        return [customer_invoice] if collection_key == "invoices" else [supplier_invoice]

    monkeypatch.setattr(service, "_fetch_documents", fake_fetch)

    first = await service.sync_partssoft_documents(test_session, days=30)
    second = await service.sync_partssoft_documents(test_session, days=30)

    assert first["counts"]["imported"] == 2
    assert second["counts"]["already_imported"] == 2
    assert await test_session.scalar(select(func.count(PaymentInvoice.id))) == 1
    assert await test_session.scalar(select(func.count(SupplierReceipt.id))) == 1
    assert await test_session.scalar(select(func.count(PartsSoftDocumentSnapshot.id))) == 2
    invoice = await test_session.scalar(select(PaymentInvoice))
    assert invoice.total_amount == Decimal("1200")
    receipt_item = await test_session.scalar(select(SupplierReceiptItem))
    assert receipt_item.marking_codes == ["010123456789012321ABC"]


@pytest.mark.asyncio
async def test_unmatched_document_imports_after_customer_is_linked(
    test_session,
    created_customers,
    monkeypatch,
):
    payload = {
        "id": 9101,
        "customer_id": 801,
        "no": "РН-ожидание",
        "sum": 450,
        "nds_percent": 20,
        "created_at": "2026-09-12T12:00:00+03:00",
        "invoice_items": [],
    }

    async def fake_fetch(endpoint, collection_key, *, since):
        del endpoint, since
        return [payload] if collection_key == "invoices" else []

    monkeypatch.setattr(service, "_fetch_documents", fake_fetch)
    reference = CustomerExternalReference(
        customer_id=created_customers[0].id,
        source_system=PARTS_SOFT_SOURCE,
        external_customer_id=801,
        is_active=True,
        is_verified=False,
    )
    test_session.add(reference)
    await test_session.commit()

    first = await service.sync_partssoft_documents(test_session, days=30)
    snapshot = await test_session.scalar(
        select(PartsSoftDocumentSnapshot).where(
            PartsSoftDocumentSnapshot.external_document_id == 9101
        )
    )
    assert first["counts"]["unmatched"] == 1
    assert snapshot.import_status == "unmatched_counterparty"
    assert snapshot.local_payment_invoice_id is None

    reference.is_verified = True
    await test_session.commit()

    second = await service.sync_partssoft_documents(test_session, days=30)
    await test_session.refresh(snapshot)
    assert second["counts"]["imported"] == 1
    assert snapshot.import_status == "imported"
    assert snapshot.local_payment_invoice_id is not None
    assert await test_session.scalar(select(func.count(PaymentInvoice.id))) == 1


@pytest.mark.asyncio
async def test_full_product_sync_queues_remote_deletion_for_recreation(
    test_session,
    created_autopart,
    monkeypatch,
):
    created_autopart.partssoft_product_id = 99001
    await test_session.commit()

    async def fake_fetch_products(*, updated_since):
        assert updated_since is None
        return []

    monkeypatch.setattr(reconciliation, "_fetch_products", fake_fetch_products)
    result = await reconciliation.sync_partssoft_products(test_session, full=True)

    queued = await test_session.scalar(
        select(PartsSoftProductOutbox).where(
            PartsSoftProductOutbox.autopart_id == created_autopart.id
        )
    )
    assert result["counts"]["remote_missing_queued"] == 1
    assert queued.operation == "upsert"
    assert queued.status == "pending"
    assert queued.external_product_id == 99001
