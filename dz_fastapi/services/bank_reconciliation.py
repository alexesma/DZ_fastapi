"""
Bank reconciliation service.

Auto-matches BankTransaction rows to CustomerPayment / SupplierPayment by:
  1. INN of counterparty → Customer (incoming) or Provider (outgoing)
  2. Amount + date window → open PaymentInvoice
  3. Purpose text → invoice number / contract reference

Creates payment records automatically when match found.
"""

import logging
import re
from datetime import date, timedelta
from decimal import Decimal
from typing import Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.models.finance import (
    BankStatement,
    BankTransaction,
    BankTxnStatus,
    CustomerPayment,
    InvoiceStatus,
    PaymentInvoice,
    PaymentMethod,
    SupplierPayment,
)
from dz_fastapi.models.partner import Customer, Provider

logger = logging.getLogger(__name__)

# Maximum difference in days between transaction date and invoice date for a match
DATE_WINDOW_DAYS = 30
# Maximum relative amount difference for fuzzy match (e.g. 0.01 = ±1%)
AMOUNT_TOLERANCE = Decimal("0.01")


# ── Invoice number extractor ───────────────────────────────────────────────────

_INV_RE = re.compile(
    r"(?:счёт|счет|с/ф|invoice|inv|№)\s*[№#]?\s*([A-Za-zА-Яа-я0-9\-/]{2,20})",
    re.IGNORECASE,
)


def _extract_invoice_refs(purpose: str) -> list[str]:
    """Extract possible invoice numbers from payment purpose."""
    if not purpose:
        return []
    return [m.group(1).strip() for m in _INV_RE.finditer(purpose)]


async def _find_customer_by_inn(
    session: AsyncSession, inn: str
) -> Optional[Customer]:
    if not inn:
        return None
    result = await session.execute(select(Customer).where(Customer.inn == inn))
    return result.scalar_one_or_none()


async def _find_provider_by_inn(
    session: AsyncSession, inn: str
) -> Optional[Provider]:
    if not inn:
        return None
    result = await session.execute(select(Provider).where(Provider.inn == inn))
    return result.scalar_one_or_none()


async def _find_matching_invoice(
    session: AsyncSession,
    customer_id: int,
    amount: Decimal,
    value_date: date,
    invoice_refs: list[str],
) -> Optional[PaymentInvoice]:
    """Try to find an open invoice that matches amount and/or invoice number."""

    # 1. Exact match by invoice number from purpose
    for ref in invoice_refs:
        result = await session.execute(
            select(PaymentInvoice).where(
                PaymentInvoice.customer_id == customer_id,
                PaymentInvoice.invoice_number == ref,
                PaymentInvoice.status.in_(
                    [
                        InvoiceStatus.SENT,
                        InvoiceStatus.PARTIALLY_PAID,
                        InvoiceStatus.OVERDUE,
                    ]
                ),
            )
        )
        inv = result.scalar_one_or_none()
        if inv:
            return inv

    # 2. Amount match among open invoices within date window
    date_min = value_date - timedelta(days=DATE_WINDOW_DAYS)
    result = await session.execute(
        select(PaymentInvoice).where(
            PaymentInvoice.customer_id == customer_id,
            PaymentInvoice.status.in_(
                [
                    InvoiceStatus.SENT,
                    InvoiceStatus.PARTIALLY_PAID,
                    InvoiceStatus.OVERDUE,
                ]
            ),
            PaymentInvoice.invoice_date >= date_min,
        )
    )
    invoices = result.scalars().all()

    # Exact remaining amount match
    for inv in invoices:
        remaining = Decimal(str(inv.total_amount)) - Decimal(
            str(inv.paid_amount)
        )
        if abs(remaining - amount) <= Decimal("0.01"):
            return inv

    # Fuzzy: within tolerance
    for inv in invoices:
        remaining = Decimal(str(inv.total_amount)) - Decimal(
            str(inv.paid_amount)
        )
        if remaining > 0:
            ratio = abs(remaining - amount) / remaining
            if ratio <= AMOUNT_TOLERANCE:
                return inv

    return None


async def auto_match_transaction(
    session: AsyncSession,
    txn: BankTransaction,
) -> Optional[str]:
    """
    Attempt to auto-match a single transaction.
    Returns match note string on success, None if not matched.
    Creates CustomerPayment or SupplierPayment and links to transaction.
    """
    if txn.status != BankTxnStatus.UNMATCHED:
        return None

    if txn.direction == "incoming":
        # --- Try to match incoming payment to a Customer ---
        customer = await _find_customer_by_inn(
            session, txn.counterparty_inn or ""
        )
        if not customer:
            return None

        invoice_refs = _extract_invoice_refs(txn.purpose or "")
        invoice = await _find_matching_invoice(
            session, customer.id, txn.amount, txn.value_date, invoice_refs
        )

        # Create CustomerPayment
        payment = CustomerPayment(
            customer_id=customer.id,
            invoice_id=invoice.id if invoice else None,
            amount=txn.amount,
            payment_date=txn.value_date,
            payment_method=PaymentMethod.BANK_TRANSFER,
            reference=txn.doc_number,
            notes=txn.purpose[:500] if txn.purpose else None,
        )
        session.add(payment)
        await session.flush()

        txn.customer_payment_id = payment.id
        txn.status = BankTxnStatus.MATCHED

        note = f"Клиент: {customer.name}"
        if invoice:
            note += f", счёт № {invoice.invoice_number}"
            # Update invoice paid amount
            from dz_fastapi.crud.finance import recalculate_invoice_status

            await recalculate_invoice_status(session, invoice)
        else:
            note += " (аванс — счёт не найден)"

        txn.match_note = note
        return note

    elif txn.direction == "outgoing":
        # --- Try to match outgoing payment to a Provider ---
        provider = await _find_provider_by_inn(
            session, txn.counterparty_inn or ""
        )
        if not provider:
            return None

        payment = SupplierPayment(
            provider_id=provider.id,
            amount=txn.amount,
            payment_date=txn.value_date,
            payment_method=PaymentMethod.BANK_TRANSFER,
            reference=txn.doc_number,
            notes=txn.purpose[:500] if txn.purpose else None,
        )
        session.add(payment)
        await session.flush()

        txn.supplier_payment_id = payment.id
        txn.status = BankTxnStatus.MATCHED
        note = f"Поставщик: {provider.name}"
        txn.match_note = note
        return note

    return None


async def auto_match_statement(
    session: AsyncSession,
    statement: BankStatement,
) -> dict:
    """
    Run auto-matching on all UNMATCHED transactions of a statement.
    Returns summary dict: {matched, skipped, errors}.
    """
    result_stmt = await session.execute(
        select(BankTransaction).where(
            BankTransaction.statement_id == statement.id,
            BankTransaction.status == BankTxnStatus.UNMATCHED,
        )
    )
    transactions = result_stmt.scalars().all()

    matched = 0
    skipped = 0
    errors = 0

    for txn in transactions:
        try:
            note = await auto_match_transaction(session, txn)
            if note:
                matched += 1
            else:
                skipped += 1
        except Exception as exc:
            logger.exception("Error matching txn %s: %s", txn.id, exc)
            errors += 1

    # Update matched_count on statement
    statement.matched_count = (
        (
            await session.execute(
                select(BankTransaction).where(
                    BankTransaction.statement_id == statement.id,
                    BankTransaction.status == BankTxnStatus.MATCHED,
                )
            )
        )
        .scalars()
        .all()
        .__len__()
    )

    await session.commit()
    return {"matched": matched, "skipped": skipped, "errors": errors}
