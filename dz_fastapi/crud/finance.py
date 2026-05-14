"""CRUD operations for financial documents."""

import logging
from datetime import date
from decimal import Decimal
from typing import List, Optional

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from dz_fastapi.models.finance import (
    CustomerPayment,
    InvoiceStatus,
    PaymentInvoice,
    PaymentInvoiceItem,
    SupplierPayment,
)
from dz_fastapi.models.partner import Customer, Provider
from dz_fastapi.schemas.finance import (
    CustomerDebtOut,
    CustomerPaymentCreate,
    CustomerPaymentUpdate,
    PaymentInvoiceCreate,
    PaymentInvoiceItemCreate,
    PaymentInvoiceItemUpdate,
    PaymentInvoiceUpdate,
    ProviderDebtOut,
    SupplierPaymentCreate,
    SupplierPaymentUpdate,
)

logger = logging.getLogger(__name__)


# ─── PaymentInvoice ───────────────────────────────────────────────────────────


def _calc_item_total(
    quantity: Decimal, unit_price: Decimal, vat_rate: Decimal
) -> Decimal:
    """Итого с НДС: unit_price * quantity * (1 + vat_rate/100)."""
    return (unit_price * quantity * (1 + vat_rate / 100)).quantize(
        Decimal("0.01")
    )


async def create_invoice(
    session: AsyncSession,
    data: PaymentInvoiceCreate,
) -> PaymentInvoice:
    items_data = data.items if data.items else []
    invoice_dict = data.model_dump(exclude={"items"})

    # Если переданы позиции — total_amount рассчитывается автоматически
    if items_data:
        computed_total = sum(
            _calc_item_total(
                Decimal(str(it.quantity)),
                Decimal(str(it.unit_price)),
                Decimal(str(it.vat_rate)),
            )
            for it in items_data
        )
        invoice_dict["total_amount"] = computed_total

    invoice = PaymentInvoice(**invoice_dict)
    session.add(invoice)
    await session.flush()  # get id for FK

    for pos, it in enumerate(items_data, start=1):
        item_dict = it.model_dump()
        item_dict["invoice_id"] = invoice.id
        item_dict["position"] = it.position if it.position != 1 else pos
        item_dict["total"] = _calc_item_total(
            Decimal(str(it.quantity)),
            Decimal(str(it.unit_price)),
            Decimal(str(it.vat_rate)),
        )
        session.add(PaymentInvoiceItem(**item_dict))

    await session.commit()
    await session.refresh(invoice)
    return invoice


async def get_invoice(
    session: AsyncSession,
    invoice_id: int,
) -> Optional[PaymentInvoice]:
    result = await session.execute(
        select(PaymentInvoice)
        .where(PaymentInvoice.id == invoice_id)
        .options(
            selectinload(PaymentInvoice.payments),
            selectinload(PaymentInvoice.items),
        )
    )
    return result.scalar_one_or_none()


async def list_invoices(
    session: AsyncSession,
    customer_id: Optional[int] = None,
    status: Optional[InvoiceStatus] = None,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
    skip: int = 0,
    limit: int = 100,
) -> List[PaymentInvoice]:
    q = select(PaymentInvoice)
    if customer_id is not None:
        q = q.where(PaymentInvoice.customer_id == customer_id)
    if status is not None:
        q = q.where(PaymentInvoice.status == status)
    if date_from is not None:
        q = q.where(PaymentInvoice.invoice_date >= date_from)
    if date_to is not None:
        q = q.where(PaymentInvoice.invoice_date <= date_to)
    q = (
        q.order_by(PaymentInvoice.invoice_date.desc())
        .offset(skip)
        .limit(limit)
    )
    result = await session.execute(q)
    return result.scalars().all()


async def update_invoice(
    session: AsyncSession,
    invoice: PaymentInvoice,
    data: PaymentInvoiceUpdate,
) -> PaymentInvoice:
    for field, value in data.model_dump(exclude_unset=True).items():
        setattr(invoice, field, value)
    await session.commit()
    await session.refresh(invoice)
    return invoice


async def delete_invoice(
    session: AsyncSession,
    invoice: PaymentInvoice,
) -> None:
    await session.delete(invoice)
    await session.commit()


async def recalculate_invoice_status(
    session: AsyncSession,
    invoice: PaymentInvoice,
) -> PaymentInvoice:
    """Пересчитывает paid_amount и обновляет статус счёта."""
    result = await session.execute(
        select(func.coalesce(func.sum(CustomerPayment.amount), 0)).where(
            CustomerPayment.invoice_id == invoice.id
        )
    )
    paid = result.scalar() or Decimal("0.00")
    invoice.paid_amount = paid

    if invoice.status == InvoiceStatus.CANCELLED:
        pass  # Не меняем статус аннулированных счетов
    elif paid == 0:
        invoice.status = (
            InvoiceStatus.SENT
            if invoice.status != InvoiceStatus.DRAFT
            else InvoiceStatus.DRAFT
        )
    elif paid < invoice.total_amount:
        invoice.status = InvoiceStatus.PARTIALLY_PAID
    else:
        invoice.status = InvoiceStatus.PAID

    await session.commit()
    await session.refresh(invoice)
    return invoice


# ─── CustomerPayment ──────────────────────────────────────────────────────────


async def create_customer_payment(
    session: AsyncSession,
    data: CustomerPaymentCreate,
) -> CustomerPayment:
    payment = CustomerPayment(**data.model_dump())
    session.add(payment)
    await session.flush()  # get id before commit

    # Пересчитываем статус счёта, если платёж привязан
    if payment.invoice_id:
        invoice = await get_invoice(session, payment.invoice_id)
        if invoice:
            await recalculate_invoice_status(session, invoice)

    await session.commit()
    await session.refresh(payment)
    return payment


async def get_customer_payment(
    session: AsyncSession,
    payment_id: int,
) -> Optional[CustomerPayment]:
    result = await session.execute(
        select(CustomerPayment).where(CustomerPayment.id == payment_id)
    )
    return result.scalar_one_or_none()


async def list_customer_payments(
    session: AsyncSession,
    customer_id: Optional[int] = None,
    invoice_id: Optional[int] = None,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
    skip: int = 0,
    limit: int = 100,
) -> List[CustomerPayment]:
    q = select(CustomerPayment)
    if customer_id is not None:
        q = q.where(CustomerPayment.customer_id == customer_id)
    if invoice_id is not None:
        q = q.where(CustomerPayment.invoice_id == invoice_id)
    if date_from is not None:
        q = q.where(CustomerPayment.payment_date >= date_from)
    if date_to is not None:
        q = q.where(CustomerPayment.payment_date <= date_to)
    q = (
        q.order_by(CustomerPayment.payment_date.desc())
        .offset(skip)
        .limit(limit)
    )
    result = await session.execute(q)
    return result.scalars().all()


async def update_customer_payment(
    session: AsyncSession,
    payment: CustomerPayment,
    data: CustomerPaymentUpdate,
) -> CustomerPayment:
    old_invoice_id = payment.invoice_id
    for field, value in data.model_dump(exclude_unset=True).items():
        setattr(payment, field, value)
    await session.flush()

    # Пересчёт статуса затронутых счетов
    affected_ids = {i for i in [old_invoice_id, payment.invoice_id] if i}
    for inv_id in affected_ids:
        invoice = await get_invoice(session, inv_id)
        if invoice:
            await recalculate_invoice_status(session, invoice)

    await session.commit()
    await session.refresh(payment)
    return payment


async def delete_customer_payment(
    session: AsyncSession,
    payment: CustomerPayment,
) -> None:
    invoice_id = payment.invoice_id
    await session.delete(payment)
    await session.flush()
    if invoice_id:
        invoice = await get_invoice(session, invoice_id)
        if invoice:
            await recalculate_invoice_status(session, invoice)
    await session.commit()


# ─── SupplierPayment ──────────────────────────────────────────────────────────


async def create_supplier_payment(
    session: AsyncSession,
    data: SupplierPaymentCreate,
) -> SupplierPayment:
    payment = SupplierPayment(**data.model_dump())
    session.add(payment)
    await session.commit()
    await session.refresh(payment)
    return payment


async def get_supplier_payment(
    session: AsyncSession,
    payment_id: int,
) -> Optional[SupplierPayment]:
    result = await session.execute(
        select(SupplierPayment).where(SupplierPayment.id == payment_id)
    )
    return result.scalar_one_or_none()


async def list_supplier_payments(
    session: AsyncSession,
    provider_id: Optional[int] = None,
    supplier_order_id: Optional[int] = None,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
    skip: int = 0,
    limit: int = 100,
) -> List[SupplierPayment]:
    q = select(SupplierPayment)
    if provider_id is not None:
        q = q.where(SupplierPayment.provider_id == provider_id)
    if supplier_order_id is not None:
        q = q.where(SupplierPayment.supplier_order_id == supplier_order_id)
    if date_from is not None:
        q = q.where(SupplierPayment.payment_date >= date_from)
    if date_to is not None:
        q = q.where(SupplierPayment.payment_date <= date_to)
    q = (
        q.order_by(SupplierPayment.payment_date.desc())
        .offset(skip)
        .limit(limit)
    )
    result = await session.execute(q)
    return result.scalars().all()


async def update_supplier_payment(
    session: AsyncSession,
    payment: SupplierPayment,
    data: SupplierPaymentUpdate,
) -> SupplierPayment:
    for field, value in data.model_dump(exclude_unset=True).items():
        setattr(payment, field, value)
    await session.commit()
    await session.refresh(payment)
    return payment


async def delete_supplier_payment(
    session: AsyncSession,
    payment: SupplierPayment,
) -> None:
    await session.delete(payment)
    await session.commit()


# ─── Debt report ──────────────────────────────────────────────────────────────


async def get_customer_debt(
    session: AsyncSession,
    customer_id: int,
) -> Optional[CustomerDebtOut]:
    """Рассчитывает задолженность клиента по незакрытым счетам."""
    # Подтягиваем клиента
    customer_result = await session.execute(
        select(Customer).where(Customer.id == customer_id)
    )
    customer = customer_result.scalar_one_or_none()
    if not customer:
        return None

    today = date.today()

    # Суммы по счетам (кроме DRAFT и CANCELLED)
    active_statuses = [
        InvoiceStatus.SENT,
        InvoiceStatus.PARTIALLY_PAID,
        InvoiceStatus.OVERDUE,
    ]
    inv_result = await session.execute(
        select(
            func.coalesce(func.sum(PaymentInvoice.total_amount), 0),
            func.coalesce(func.sum(PaymentInvoice.paid_amount), 0),
        ).where(
            PaymentInvoice.customer_id == customer_id,
            PaymentInvoice.status.in_(active_statuses),
        )
    )
    total_invoiced, total_paid = inv_result.one()

    # Просроченная задолженность — счета с due_date < today
    overdue_result = await session.execute(
        select(
            func.coalesce(
                func.sum(
                    PaymentInvoice.total_amount - PaymentInvoice.paid_amount
                ),
                0,
            )
        ).where(
            PaymentInvoice.customer_id == customer_id,
            PaymentInvoice.status.in_(active_statuses),
            PaymentInvoice.due_date < today,
        )
    )
    overdue_amount = overdue_result.scalar() or Decimal("0.00")

    return CustomerDebtOut(
        customer_id=customer_id,
        customer_name=customer.name,
        total_invoiced=Decimal(str(total_invoiced)),
        total_paid=Decimal(str(total_paid)),
        debt=Decimal(str(total_invoiced)) - Decimal(str(total_paid)),
        overdue_amount=Decimal(str(overdue_amount)),
        credit_limit=customer.credit_limit,
        payment_terms_days=customer.payment_terms_days,
    )


async def get_debtors_report(
    session: AsyncSession,
    only_overdue: bool = False,
) -> List[CustomerDebtOut]:
    """Список всех клиентов с задолженностью."""
    today = date.today()
    active_statuses = [
        InvoiceStatus.SENT,
        InvoiceStatus.PARTIALLY_PAID,
        InvoiceStatus.OVERDUE,
    ]

    q = (
        select(
            Customer.id,
            Customer.name,
            Customer.credit_limit,
            Customer.payment_terms_days,
            func.coalesce(func.sum(PaymentInvoice.total_amount), 0).label(
                "total_invoiced"
            ),
            func.coalesce(func.sum(PaymentInvoice.paid_amount), 0).label(
                "total_paid"
            ),
        )
        .join(PaymentInvoice, PaymentInvoice.customer_id == Customer.id)
        .where(PaymentInvoice.status.in_(active_statuses))
        .group_by(
            Customer.id,
            Customer.name,
            Customer.credit_limit,
            Customer.payment_terms_days,
        )
        .having(
            func.sum(PaymentInvoice.total_amount)
            > func.sum(PaymentInvoice.paid_amount)
        )
    )

    rows = (await session.execute(q)).all()

    result = []
    for row in rows:
        # Отдельным запросом считаем просрочку для каждого
        overdue_result = await session.execute(
            select(
                func.coalesce(
                    func.sum(
                        PaymentInvoice.total_amount
                        - PaymentInvoice.paid_amount
                    ),
                    0,
                )
            ).where(
                PaymentInvoice.customer_id == row.id,
                PaymentInvoice.status.in_(active_statuses),
                PaymentInvoice.due_date < today,
            )
        )
        overdue_amount = Decimal(str(overdue_result.scalar() or 0))

        if only_overdue and overdue_amount == 0:
            continue

        result.append(
            CustomerDebtOut(
                customer_id=row.id,
                customer_name=row.name,
                total_invoiced=Decimal(str(row.total_invoiced)),
                total_paid=Decimal(str(row.total_paid)),
                debt=Decimal(str(row.total_invoiced))
                - Decimal(str(row.total_paid)),
                overdue_amount=overdue_amount,
                credit_limit=row.credit_limit,
                payment_terms_days=row.payment_terms_days,
            )
        )

    result.sort(key=lambda x: x.debt, reverse=True)
    return result


# ─── PaymentInvoiceItem ───────────────────────────────────────────────────────


async def add_invoice_item(
    session: AsyncSession,
    invoice: PaymentInvoice,
    data: PaymentInvoiceItemCreate,
) -> PaymentInvoiceItem:
    """Добавить позицию к счёту, пересчитать total_amount."""
    item = PaymentInvoiceItem(
        invoice_id=invoice.id,
        **data.model_dump(),
        total=_calc_item_total(
            Decimal(str(data.quantity)),
            Decimal(str(data.unit_price)),
            Decimal(str(data.vat_rate)),
        ),
    )
    session.add(item)
    await session.flush()
    await _recalc_invoice_total(session, invoice)
    await session.commit()
    await session.refresh(item)
    return item


async def update_invoice_item(
    session: AsyncSession,
    item: PaymentInvoiceItem,
    data: PaymentInvoiceItemUpdate,
) -> PaymentInvoiceItem:
    for field, value in data.model_dump(exclude_unset=True).items():
        setattr(item, field, value)
    item.total = _calc_item_total(
        Decimal(str(item.quantity)),
        Decimal(str(item.unit_price)),
        Decimal(str(item.vat_rate)),
    )
    await session.flush()
    invoice = await get_invoice(session, item.invoice_id)
    if invoice:
        await _recalc_invoice_total(session, invoice)
    await session.commit()
    await session.refresh(item)
    return item


async def delete_invoice_item(
    session: AsyncSession,
    item: PaymentInvoiceItem,
) -> None:
    invoice_id = item.invoice_id
    await session.delete(item)
    await session.flush()
    invoice = await get_invoice(session, invoice_id)
    if invoice:
        await _recalc_invoice_total(session, invoice)
    await session.commit()


async def get_invoice_item(
    session: AsyncSession,
    item_id: int,
) -> Optional[PaymentInvoiceItem]:
    result = await session.execute(
        select(PaymentInvoiceItem).where(PaymentInvoiceItem.id == item_id)
    )
    return result.scalar_one_or_none()


async def _recalc_invoice_total(
    session: AsyncSession,
    invoice: PaymentInvoice,
) -> None:
    """Пересчитывает total_amount счёта по сумме позиций."""
    result = await session.execute(
        select(func.coalesce(func.sum(PaymentInvoiceItem.total), 0)).where(
            PaymentInvoiceItem.invoice_id == invoice.id
        )
    )
    invoice.total_amount = result.scalar() or Decimal("0.00")


# ─── Creditors report ─────────────────────────────────────────────────────────


async def get_creditors_report(
    session: AsyncSession,
    only_owed: bool = False,
) -> List[ProviderDebtOut]:
    """Кредиторская задолженность: итого заказано vs оплачено по поставщикам."""
    from dz_fastapi.models.partner import SupplierOrder, SupplierOrderItem

    # Сумма оплат и последняя дата оплаты по поставщикам
    payments_q = select(
        SupplierPayment.provider_id,
        func.coalesce(func.sum(SupplierPayment.amount), 0).label("total_paid"),
        func.max(SupplierPayment.payment_date).label("last_payment_date"),
    ).group_by(SupplierPayment.provider_id)
    payments_rows = {
        row.provider_id: row
        for row in (await session.execute(payments_q)).all()
    }

    # Сумма заказов (price * quantity) по поставщикам — только там где есть цена
    ordered_q = (
        select(
            SupplierOrder.provider_id,
            func.coalesce(
                func.sum(SupplierOrderItem.price * SupplierOrderItem.quantity),
                0,
            ).label("total_ordered"),
        )
        .join(
            SupplierOrderItem,
            SupplierOrderItem.supplier_order_id == SupplierOrder.id,
        )
        .where(SupplierOrderItem.price.isnot(None))
        .group_by(SupplierOrder.provider_id)
    )
    ordered_rows = {
        row.provider_id: Decimal(str(row.total_ordered))
        for row in (await session.execute(ordered_q)).all()
    }

    # Все поставщики с хоть какой-то активностью
    provider_ids = set(payments_rows.keys()) | set(ordered_rows.keys())
    if not provider_ids:
        return []

    providers_result = await session.execute(
        select(Provider.id, Provider.name, Provider.payment_terms_days).where(
            Provider.id.in_(provider_ids)
        )
    )
    providers = {row.id: row for row in providers_result.all()}

    result = []
    for pid in provider_ids:
        if pid not in providers:
            continue
        prov = providers[pid]
        pay_row = payments_rows.get(pid)
        total_paid = (
            Decimal(str(pay_row.total_paid)) if pay_row else Decimal("0.00")
        )
        total_ordered = ordered_rows.get(pid, Decimal("0.00"))
        owed = max(Decimal("0.00"), total_ordered - total_paid)

        if only_owed and owed == 0:
            continue

        result.append(
            ProviderDebtOut(
                provider_id=pid,
                provider_name=prov.name,
                total_ordered=total_ordered,
                total_paid=total_paid,
                owed=owed,
                last_payment_date=(
                    pay_row.last_payment_date if pay_row else None
                ),
                payment_terms_days=prov.payment_terms_days,
            )
        )

    result.sort(key=lambda x: x.owed, reverse=True)
    return result
