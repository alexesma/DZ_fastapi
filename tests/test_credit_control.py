from datetime import date
from decimal import Decimal

import pytest

from dz_fastapi.models.finance import CustomerPayment, InvoiceStatus, PaymentInvoice
from dz_fastapi.models.inventory import (
    ShipmentDocument,
    ShipmentDocumentItem,
    ShipmentDocumentStatus,
)
from dz_fastapi.models.partner import Customer, Provider, SupplierReceipt, SupplierReceiptItem
from dz_fastapi.services.credit_control import (
    CreditLimitExceeded,
    build_customer_reconciliation_act,
    check_customer_credit_policy,
)
from dz_fastapi.services.inventory_stock import post_shipment_document, receive_stock


@pytest.mark.asyncio
async def test_customer_reconciliation_act_calculates_balances(
    test_session,
    created_customers: list[Customer],
):
    customer = created_customers[0]
    test_session.add_all(
        [
            PaymentInvoice(
                customer_id=customer.id,
                invoice_number="INV-OLD",
                invoice_date=date(2026, 1, 10),
                total_amount=Decimal("1000.00"),
                paid_amount=Decimal("0.00"),
                status=InvoiceStatus.SENT,
            ),
            CustomerPayment(
                customer_id=customer.id,
                amount=Decimal("200.00"),
                payment_date=date(2026, 1, 20),
            ),
            PaymentInvoice(
                customer_id=customer.id,
                invoice_number="INV-NEW",
                invoice_date=date(2026, 2, 5),
                total_amount=Decimal("500.00"),
                paid_amount=Decimal("0.00"),
                status=InvoiceStatus.SENT,
            ),
            CustomerPayment(
                customer_id=customer.id,
                amount=Decimal("300.00"),
                payment_date=date(2026, 2, 7),
                reference="PAY-1",
            ),
        ]
    )
    await test_session.flush()

    act = await build_customer_reconciliation_act(
        test_session,
        customer_id=customer.id,
        date_from=date(2026, 2, 1),
        date_to=date(2026, 2, 28),
    )

    assert act is not None
    assert act.opening_balance == Decimal("800.00")
    assert act.debit_turnover == Decimal("500.00")
    assert act.credit_turnover == Decimal("300.00")
    assert act.closing_balance == Decimal("1000.00")
    assert [line.document_number for line in act.lines] == ["INV-NEW", "PAY-1"]


@pytest.mark.asyncio
async def test_post_shipment_blocks_when_credit_limit_exceeded(
    test_session,
    created_autopart,
    created_customers: list[Customer],
    created_providers: list[Provider],
):
    customer = created_customers[0]
    customer.credit_limit = Decimal("100.00")
    customer.credit_control_mode = "block"
    test_session.add(
        PaymentInvoice(
            customer_id=customer.id,
            invoice_number="INV-DEBT",
            invoice_date=date(2026, 2, 1),
            total_amount=Decimal("90.00"),
            paid_amount=Decimal("0.00"),
            status=InvoiceStatus.SENT,
        )
    )
    receipt = SupplierReceipt(
        provider_id=created_providers[0].id,
        document_number="R-CREDIT",
        document_date=date(2026, 2, 1),
    )
    receipt.items = [
        SupplierReceiptItem(
            autopart_id=created_autopart.id,
            received_quantity=2,
            price=Decimal("10.00"),
        )
    ]
    test_session.add(receipt)
    await test_session.flush()
    await receive_stock(test_session, receipt=receipt, reverse=False)

    shipment = ShipmentDocument(
        status=ShipmentDocumentStatus.DRAFT,
        customer_id=customer.id,
        doc_number="SHIP-CREDIT",
    )
    test_session.add(shipment)
    await test_session.flush()
    test_session.add(
        ShipmentDocumentItem(
            document_id=shipment.id,
            autopart_id=created_autopart.id,
            quantity=1,
            price=Decimal("20.00"),
        )
    )
    await test_session.flush()

    with pytest.raises(CreditLimitExceeded):
        await post_shipment_document(test_session, shipment.id)


@pytest.mark.asyncio
async def test_credit_policy_warn_mode_returns_warning_without_blocking(
    test_session,
    created_customers: list[Customer],
):
    customer = created_customers[0]
    customer.credit_limit = Decimal("100.00")
    customer.credit_control_mode = "warn"
    test_session.add(
        PaymentInvoice(
            customer_id=customer.id,
            invoice_number="INV-WARN",
            invoice_date=date(2026, 2, 1),
            total_amount=Decimal("90.00"),
            paid_amount=Decimal("0.00"),
            status=InvoiceStatus.SENT,
        )
    )
    await test_session.flush()

    check = await check_customer_credit_policy(
        test_session,
        customer_id=customer.id,
        pending_amount=Decimal("20.00"),
    )

    assert check is not None
    assert check.should_warn is True
    assert check.should_block is False
    assert check.reasons == ["credit_limit"]
