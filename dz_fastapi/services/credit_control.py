"""Customer credit limit checks and reconciliation act reports."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from decimal import ROUND_HALF_UP, Decimal
from html import escape

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from dz_fastapi.models.finance import CustomerPayment, InvoiceStatus, PaymentInvoice
from dz_fastapi.models.inventory import ShipmentDocument
from dz_fastapi.models.partner import Customer

CREDIT_CONTROL_OFF = "off"
CREDIT_CONTROL_WARN = "warn"
CREDIT_CONTROL_BLOCK = "block"
MONEY_ZERO = Decimal("0.00")
MONEY_QUANT = Decimal("0.01")
ACTIVE_DEBT_STATUSES = (
    InvoiceStatus.SENT,
    InvoiceStatus.PARTIALLY_PAID,
    InvoiceStatus.OVERDUE,
)
RECONCILIATION_INVOICE_STATUSES = (
    InvoiceStatus.SENT,
    InvoiceStatus.PARTIALLY_PAID,
    InvoiceStatus.PAID,
    InvoiceStatus.OVERDUE,
)


class CreditLimitExceeded(ValueError):
    def __init__(
        self,
        *,
        customer_id: int,
        customer_name: str,
        mode: str,
        credit_limit: Decimal,
        current_debt: Decimal,
        pending_amount: Decimal,
        projected_debt: Decimal,
        overdue_amount: Decimal = MONEY_ZERO,
        reasons: list[str] | None = None,
    ) -> None:
        self.customer_id = customer_id
        self.customer_name = customer_name
        self.mode = mode
        self.credit_limit = credit_limit
        self.current_debt = current_debt
        self.pending_amount = pending_amount
        self.projected_debt = projected_debt
        self.overdue_amount = overdue_amount
        self.reasons = reasons or ["credit_limit"]
        self.exceeded_by = _money(
            max(projected_debt - credit_limit, MONEY_ZERO)
        )
        reason_text = "; ".join(_reason_label(reason) for reason in self.reasons)
        super().__init__(
            "Нарушена кредитная политика клиента "
            f"{customer_name}: {reason_text}. "
            f"Лимит {credit_limit:.2f} руб., "
            f"текущий долг {current_debt:.2f} руб., "
            f"новая операция {pending_amount:.2f} руб., "
            f"прогноз {projected_debt:.2f} руб., "
            f"просрочено {overdue_amount:.2f} руб."
        )

    def to_detail(self) -> dict:
        return {
            "code": "credit_limit_exceeded",
            "message": str(self),
            "customer_id": self.customer_id,
            "customer_name": self.customer_name,
            "mode": self.mode,
            "credit_limit": self.credit_limit,
            "current_debt": self.current_debt,
            "pending_amount": self.pending_amount,
            "projected_debt": self.projected_debt,
            "exceeded_by": self.exceeded_by,
            "overdue_amount": self.overdue_amount,
            "reasons": self.reasons,
        }


@dataclass(slots=True)
class CreditPolicyCheck:
    customer_id: int
    customer_name: str
    mode: str
    credit_limit: Decimal
    payment_terms_days: int
    current_debt: Decimal
    pending_amount: Decimal
    projected_debt: Decimal
    overdue_amount: Decimal
    reasons: list[str]

    @property
    def has_violation(self) -> bool:
        return bool(self.reasons)

    @property
    def should_warn(self) -> bool:
        return self.mode == CREDIT_CONTROL_WARN and self.has_violation

    @property
    def should_block(self) -> bool:
        return self.mode == CREDIT_CONTROL_BLOCK and self.has_violation

    def to_detail(self) -> dict:
        exceeded_by = _money(
            max(self.projected_debt - self.credit_limit, MONEY_ZERO)
        )
        return {
            "code": "credit_policy_warning",
            "message": _build_policy_message(
                customer_name=self.customer_name,
                reasons=self.reasons,
                credit_limit=self.credit_limit,
                current_debt=self.current_debt,
                pending_amount=self.pending_amount,
                projected_debt=self.projected_debt,
                overdue_amount=self.overdue_amount,
            ),
            "customer_id": self.customer_id,
            "customer_name": self.customer_name,
            "mode": self.mode,
            "credit_limit": self.credit_limit,
            "payment_terms_days": self.payment_terms_days,
            "current_debt": self.current_debt,
            "pending_amount": self.pending_amount,
            "projected_debt": self.projected_debt,
            "exceeded_by": exceeded_by,
            "overdue_amount": self.overdue_amount,
            "reasons": self.reasons,
        }


@dataclass(slots=True)
class ReconciliationActLine:
    operation_date: date
    operation_type: str
    document_number: str | None
    description: str
    debit: Decimal
    credit: Decimal
    balance: Decimal


@dataclass(slots=True)
class ReconciliationAct:
    customer_id: int
    customer_name: str
    date_from: date | None
    date_to: date | None
    opening_balance: Decimal
    debit_turnover: Decimal
    credit_turnover: Decimal
    closing_balance: Decimal
    lines: list[ReconciliationActLine]


def _money(value: object) -> Decimal:
    return Decimal(str(value or 0)).quantize(MONEY_QUANT, rounding=ROUND_HALF_UP)


def _reason_label(reason: str) -> str:
    return {
        "credit_limit": "превышен лимит суммы отсрочки",
        "payment_overdue": "есть просроченная задолженность",
    }.get(reason, reason)


def _build_policy_message(
    *,
    customer_name: str,
    reasons: list[str],
    credit_limit: Decimal,
    current_debt: Decimal,
    pending_amount: Decimal,
    projected_debt: Decimal,
    overdue_amount: Decimal,
) -> str:
    reason_text = "; ".join(_reason_label(reason) for reason in reasons)
    return (
        f"Клиент {customer_name}: {reason_text}. "
        f"Лимит {credit_limit:.2f} руб., долг {current_debt:.2f} руб., "
        f"операция {pending_amount:.2f} руб., прогноз {projected_debt:.2f} руб., "
        f"просрочено {overdue_amount:.2f} руб."
    )


async def calculate_customer_debt_amount(
    session: AsyncSession,
    *,
    customer_id: int,
) -> Decimal:
    result = await session.execute(
        select(
            func.coalesce(
                func.sum(PaymentInvoice.total_amount - PaymentInvoice.paid_amount),
                0,
            )
        ).where(
            PaymentInvoice.customer_id == customer_id,
            PaymentInvoice.status.in_(ACTIVE_DEBT_STATUSES),
        )
    )
    return _money(result.scalar() or 0)


async def calculate_customer_overdue_amount(
    session: AsyncSession,
    *,
    customer_id: int,
    payment_terms_days: int,
) -> Decimal:
    if payment_terms_days <= 0:
        return MONEY_ZERO
    today = date.today()
    fallback_due_before = today - timedelta(days=payment_terms_days)
    result = await session.execute(
        select(
            func.coalesce(
                func.sum(PaymentInvoice.total_amount - PaymentInvoice.paid_amount),
                0,
            )
        ).where(
            PaymentInvoice.customer_id == customer_id,
            PaymentInvoice.status.in_(ACTIVE_DEBT_STATUSES),
            (
                (PaymentInvoice.due_date.is_not(None))
                & (PaymentInvoice.due_date < today)
            )
            | (
                (PaymentInvoice.due_date.is_(None))
                & (PaymentInvoice.invoice_date < fallback_due_before)
            ),
        )
    )
    return _money(result.scalar() or 0)


def calculate_shipment_amount(document: ShipmentDocument) -> Decimal:
    total = MONEY_ZERO
    for item in list(getattr(document, "items", []) or []):
        if item.price is None:
            continue
        total += _money(item.price) * Decimal(int(item.quantity or 0))
    return _money(total)


async def _shipment_has_accounted_invoice(
    session: AsyncSession,
    *,
    shipment_id: int,
) -> bool:
    result = await session.execute(
        select(PaymentInvoice.id)
        .where(
            PaymentInvoice.shipment_id == shipment_id,
            PaymentInvoice.status.in_(
                (
                    InvoiceStatus.SENT,
                    InvoiceStatus.PARTIALLY_PAID,
                    InvoiceStatus.PAID,
                    InvoiceStatus.OVERDUE,
                )
            ),
        )
        .limit(1)
    )
    return result.scalar_one_or_none() is not None


async def assert_customer_credit_available(
    session: AsyncSession,
    *,
    customer_id: int | None,
    pending_amount: Decimal | int | float | str = MONEY_ZERO,
) -> None:
    check = await check_customer_credit_policy(
        session,
        customer_id=customer_id,
        pending_amount=pending_amount,
    )
    if check is None or not check.should_block:
        return
    raise _credit_limit_exception_from_check(check)


async def check_customer_credit_policy(
    session: AsyncSession,
    *,
    customer_id: int | None,
    pending_amount: Decimal | int | float | str = MONEY_ZERO,
) -> CreditPolicyCheck | None:
    if customer_id is None:
        return None
    customer = await session.get(Customer, customer_id)
    if customer is None:
        return None
    mode = str(getattr(customer, "credit_control_mode", None) or CREDIT_CONTROL_OFF)
    mode = mode.lower()
    if mode not in {CREDIT_CONTROL_OFF, CREDIT_CONTROL_WARN, CREDIT_CONTROL_BLOCK}:
        mode = CREDIT_CONTROL_OFF
    if mode == CREDIT_CONTROL_OFF:
        return None

    credit_limit = _money(customer.credit_limit)
    payment_terms_days = int(customer.payment_terms_days or 0)
    if credit_limit <= MONEY_ZERO and payment_terms_days <= 0:
        return None

    current_debt = await calculate_customer_debt_amount(
        session,
        customer_id=customer_id,
    )
    pending = _money(pending_amount)
    projected = _money(current_debt + pending)
    overdue_amount = await calculate_customer_overdue_amount(
        session,
        customer_id=customer_id,
        payment_terms_days=payment_terms_days,
    )
    reasons: list[str] = []
    if credit_limit > MONEY_ZERO and projected > credit_limit:
        reasons.append("credit_limit")
    if payment_terms_days > 0 and overdue_amount > MONEY_ZERO:
        reasons.append("payment_overdue")
    return CreditPolicyCheck(
        customer_id=customer_id,
        customer_name=str(customer.name or f"#{customer_id}"),
        mode=mode,
        credit_limit=credit_limit,
        payment_terms_days=payment_terms_days,
        current_debt=current_debt,
        pending_amount=pending,
        projected_debt=projected,
        overdue_amount=overdue_amount,
        reasons=reasons,
    )


def _credit_limit_exception_from_check(
    check: CreditPolicyCheck,
) -> CreditLimitExceeded:
    return CreditLimitExceeded(
        customer_id=check.customer_id,
        customer_name=check.customer_name,
        mode=check.mode,
        credit_limit=check.credit_limit,
        current_debt=check.current_debt,
        pending_amount=check.pending_amount,
        projected_debt=check.projected_debt,
        overdue_amount=check.overdue_amount,
        reasons=check.reasons,
    )


async def check_shipment_credit_policy(
    session: AsyncSession,
    *,
    document: ShipmentDocument,
) -> CreditPolicyCheck | None:
    if document.customer_id is None:
        return None
    amount = calculate_shipment_amount(document)
    if document.id and await _shipment_has_accounted_invoice(
        session,
        shipment_id=int(document.id),
    ):
        amount = MONEY_ZERO
    return await check_customer_credit_policy(
        session,
        customer_id=document.customer_id,
        pending_amount=amount,
    )


async def assert_shipment_credit_available(
    session: AsyncSession,
    *,
    document: ShipmentDocument,
) -> None:
    check = await check_shipment_credit_policy(session, document=document)
    if check is None or not check.should_block:
        return
    raise _credit_limit_exception_from_check(check)


async def _customer_or_none(
    session: AsyncSession,
    customer_id: int,
) -> Customer | None:
    return await session.get(Customer, customer_id)


async def _sum_invoices_before(
    session: AsyncSession,
    *,
    customer_id: int,
    before_date: date | None,
) -> Decimal:
    if before_date is None:
        return MONEY_ZERO
    result = await session.execute(
        select(func.coalesce(func.sum(PaymentInvoice.total_amount), 0)).where(
            PaymentInvoice.customer_id == customer_id,
            PaymentInvoice.status.in_(RECONCILIATION_INVOICE_STATUSES),
            PaymentInvoice.invoice_date < before_date,
        )
    )
    return _money(result.scalar() or 0)


async def _sum_payments_before(
    session: AsyncSession,
    *,
    customer_id: int,
    before_date: date | None,
) -> Decimal:
    if before_date is None:
        return MONEY_ZERO
    result = await session.execute(
        select(func.coalesce(func.sum(CustomerPayment.amount), 0)).where(
            CustomerPayment.customer_id == customer_id,
            CustomerPayment.payment_date < before_date,
        )
    )
    return _money(result.scalar() or 0)


def _date_range_filter(column, date_from: date | None, date_to: date | None):
    filters = []
    if date_from is not None:
        filters.append(column >= date_from)
    if date_to is not None:
        filters.append(column <= date_to)
    return filters


async def build_customer_reconciliation_act(
    session: AsyncSession,
    *,
    customer_id: int,
    date_from: date | None = None,
    date_to: date | None = None,
) -> ReconciliationAct | None:
    customer = await _customer_or_none(session, customer_id)
    if customer is None:
        return None

    opening_balance = _money(
        await _sum_invoices_before(
            session,
            customer_id=customer_id,
            before_date=date_from,
        )
        - await _sum_payments_before(
            session,
            customer_id=customer_id,
            before_date=date_from,
        )
    )

    invoice_stmt = (
        select(PaymentInvoice)
        .where(
            PaymentInvoice.customer_id == customer_id,
            PaymentInvoice.status.in_(RECONCILIATION_INVOICE_STATUSES),
            *_date_range_filter(PaymentInvoice.invoice_date, date_from, date_to),
        )
        .options(selectinload(PaymentInvoice.shipment))
        .order_by(PaymentInvoice.invoice_date.asc(), PaymentInvoice.id.asc())
    )
    payment_stmt = (
        select(CustomerPayment)
        .where(
            CustomerPayment.customer_id == customer_id,
            *_date_range_filter(CustomerPayment.payment_date, date_from, date_to),
        )
        .order_by(CustomerPayment.payment_date.asc(), CustomerPayment.id.asc())
    )
    invoices = list((await session.execute(invoice_stmt)).scalars().all())
    payments = list((await session.execute(payment_stmt)).scalars().all())

    raw_lines: list[tuple[date, int, ReconciliationActLine]] = []
    for invoice in invoices:
        raw_lines.append(
            (
                invoice.invoice_date,
                0,
                ReconciliationActLine(
                    operation_date=invoice.invoice_date,
                    operation_type="invoice",
                    document_number=invoice.invoice_number,
                    description="Счёт/реализация клиенту",
                    debit=_money(invoice.total_amount),
                    credit=MONEY_ZERO,
                    balance=MONEY_ZERO,
                ),
            )
        )
    for payment in payments:
        raw_lines.append(
            (
                payment.payment_date,
                1,
                ReconciliationActLine(
                    operation_date=payment.payment_date,
                    operation_type="payment",
                    document_number=payment.reference,
                    description="Оплата клиента",
                    debit=MONEY_ZERO,
                    credit=_money(payment.amount),
                    balance=MONEY_ZERO,
                ),
            )
        )

    balance = opening_balance
    lines: list[ReconciliationActLine] = []
    for _, _, line in sorted(raw_lines, key=lambda item: item[:2]):
        balance = _money(balance + line.debit - line.credit)
        lines.append(
            ReconciliationActLine(
                operation_date=line.operation_date,
                operation_type=line.operation_type,
                document_number=line.document_number,
                description=line.description,
                debit=line.debit,
                credit=line.credit,
                balance=balance,
            )
        )

    debit_turnover = _money(sum((line.debit for line in lines), MONEY_ZERO))
    credit_turnover = _money(sum((line.credit for line in lines), MONEY_ZERO))
    closing_balance = _money(opening_balance + debit_turnover - credit_turnover)
    return ReconciliationAct(
        customer_id=customer_id,
        customer_name=str(customer.name or ""),
        date_from=date_from,
        date_to=date_to,
        opening_balance=opening_balance,
        debit_turnover=debit_turnover,
        credit_turnover=credit_turnover,
        closing_balance=closing_balance,
        lines=lines,
    )


def _format_money(value: Decimal) -> str:
    return f"{_money(value):,.2f}".replace(",", " ")


def render_reconciliation_act_html(act: ReconciliationAct) -> str:
    period = "за весь период"
    if act.date_from and act.date_to:
        period = f"за период {act.date_from:%d.%m.%Y} - {act.date_to:%d.%m.%Y}"
    elif act.date_from:
        period = f"с {act.date_from:%d.%m.%Y}"
    elif act.date_to:
        period = f"по {act.date_to:%d.%m.%Y}"

    rows = "\n".join(_render_reconciliation_row(line) for line in act.lines)
    if not rows:
        rows = (
            "<tr><td colspan='6' class='empty'>Операций за период нет</td></tr>"
        )
    return f"""<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <title>Акт сверки</title>
  <style>
    body {{ font-family: Arial, sans-serif; color: #111; margin: 32px; }}
    h1 {{ font-size: 22px; margin: 0 0 8px; }}
    .muted {{ color: #666; margin-bottom: 24px; }}
    table {{ border-collapse: collapse; width: 100%; font-size: 13px; }}
    th, td {{ border: 1px solid #bbb; padding: 7px 8px; }}
    th {{ background: #f2f2f2; text-align: left; }}
    td.money, th.money {{ text-align: right; white-space: nowrap; }}
    .summary {{ margin: 18px 0 24px; max-width: 520px; }}
    .summary td:first-child {{ font-weight: bold; }}
    .empty {{ text-align: center; color: #777; padding: 18px; }}
    .signatures {{ display: flex; gap: 48px; margin-top: 48px; }}
    .signature {{ flex: 1; border-top: 1px solid #333; padding-top: 8px; }}
    @media print {{ body {{ margin: 18mm; }} }}
  </style>
</head>
<body>
  <h1>Акт сверки взаимных расчётов</h1>
  <div class="muted">{escape(period)} · Клиент: {escape(act.customer_name)}</div>

  <table class="summary">
    <tr><td>Сальдо начальное</td><td class="money">{_format_money(act.opening_balance)}</td></tr>
    <tr><td>Дебетовый оборот</td><td class="money">{_format_money(act.debit_turnover)}</td></tr>
    <tr><td>Кредитовый оборот</td><td class="money">{_format_money(act.credit_turnover)}</td></tr>
    <tr><td>Сальдо конечное</td><td class="money">{_format_money(act.closing_balance)}</td></tr>
  </table>

  <table>
    <thead>
      <tr>
        <th>Дата</th>
        <th>Документ</th>
        <th>Операция</th>
        <th class="money">Дебет</th>
        <th class="money">Кредит</th>
        <th class="money">Сальдо</th>
      </tr>
    </thead>
    <tbody>{rows}</tbody>
  </table>

  <div class="signatures">
    <div class="signature">Поставщик</div>
    <div class="signature">Покупатель</div>
  </div>
</body>
</html>"""


def _render_reconciliation_row(line: ReconciliationActLine) -> str:
    doc = escape(line.document_number or "—")
    return (
        "<tr>"
        f"<td>{line.operation_date:%d.%m.%Y}</td>"
        f"<td>{doc}</td>"
        f"<td>{escape(line.description)}</td>"
        f"<td class='money'>{_format_money(line.debit)}</td>"
        f"<td class='money'>{_format_money(line.credit)}</td>"
        f"<td class='money'>{_format_money(line.balance)}</td>"
        "</tr>"
    )
