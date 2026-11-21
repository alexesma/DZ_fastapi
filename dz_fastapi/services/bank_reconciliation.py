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
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
from typing import Optional

from sqlalchemy import func, select
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


# ── Нормализация названий и ИНН ────────────────────────────────────────────────

_LEGAL_FORMS_RE = re.compile(
    r"\b(общество с ограниченной ответственностью|индивидуальный предприниматель|"
    r"публичное акционерное общество|акционерное общество|ооо|оао|зао|пао|ао|ип)\b",
    re.IGNORECASE,
)
_NAME_NOISE_RE = re.compile(r"[^0-9a-zа-яё]+", re.IGNORECASE)
_CLIENT_SUFFIX_RE = re.compile(r"\((клиент|поставщик)\)", re.IGNORECASE)


def normalize_inn(value: Optional[str]) -> str:
    digits = re.sub(r"\D", "", value or "")
    # Нулевой ИНН банк подставляет, когда плательщик — физлицо/не указан.
    if not digits or set(digits) == {"0"}:
        return ""
    return digits


def normalize_name(value: Optional[str]) -> str:
    """Название без правовой формы, кавычек и регистра: для сравнения."""
    text = _CLIENT_SUFFIX_RE.sub(" ", (value or "").lower().replace("ё", "е"))
    text = _LEGAL_FORMS_RE.sub(" ", text)
    return _NAME_NOISE_RE.sub(" ", text).strip()


# ── Не торговые операции ───────────────────────────────────────────────────────

NON_TRADE_LABELS = {
    "deposit_open": "Депозит: открытие",
    "deposit_return": "Депозит: возврат",
    "deposit_interest": "Депозит: проценты",
    "tax": "Налоги и сборы",
    "salary": "Зарплата",
    "bank_fee": "Банковская комиссия",
    "bank_interest": "Проценты банка",
}
DEPOSIT_CATEGORIES = ("deposit_open", "deposit_return", "deposit_interest")

# Операции с этими признаками не привязываются к клиентам и поставщикам.
_NON_TRADE_RULES = (
    (
        "tax",
        ("единый налоговый", "налоговый платеж", "казначейство", "фнс россии"),
    ),
    ("salary", ("заработная плата", "заработной платы", "отпускные")),
    (
        "bank_fee",
        ("комиссия за", "обслуживание счета", "обслуживание счёта"),
    ),
    ("bank_interest", ("выплата процентов", "проценты на остаток")),
)
_OWN_BANK_INNS = {"9721194461"}  # Банк Точка: проценты, комиссии
_DEAL_REF_RE = re.compile(r"сделк\w*\s*№?\s*([A-Za-z]{2,5}-?\d{5,})", re.I)


@dataclass
class NonTradeClass:
    category: str
    deal_ref: Optional[str] = None

    @property
    def label(self) -> str:
        return NON_TRADE_LABELS[self.category]


def extract_deal_ref(purpose: Optional[str]) -> Optional[str]:
    match = _DEAL_REF_RE.search(purpose or "")
    return match.group(1).upper() if match else None


def classify_operation(txn: BankTransaction) -> Optional[NonTradeClass]:
    """Служебная ли операция и какая именно (депозит, налог, зарплата…)."""
    purpose = (txn.purpose or "").casefold()
    haystack = f"{purpose} {(txn.counterparty_name or '').casefold()}"
    # Торговые слова: «налог» в «без налога (НДС)» не делает платёж налогом.
    if any(word in haystack for word in ("автозапчаст", "запчаст", "товар")):
        return None
    if "депозит" in purpose:
        if "открытия депозита" in purpose:
            category = "deposit_open"
        elif "выплата процентов" in purpose:
            category = "deposit_interest"
        elif "возврат средств" in purpose:
            category = "deposit_return"
        else:
            category = (
                "deposit_open"
                if txn.direction == "outgoing"
                else "deposit_return"
            )
        return NonTradeClass(category, extract_deal_ref(txn.purpose))
    for category, markers in _NON_TRADE_RULES:
        if any(marker in haystack for marker in markers):
            return NonTradeClass(category)
    if normalize_inn(txn.counterparty_inn) in _OWN_BANK_INNS:
        return NonTradeClass("bank_interest")
    return None


def classify_non_trade(txn: BankTransaction) -> Optional[str]:
    found = classify_operation(txn)
    return found.label if found else None


# ── Поиск клиента / поставщика ─────────────────────────────────────────────────


async def _find_partner(
    session: AsyncSession,
    model,
    *,
    inn: str,
    name: str,
    account: str = "",
):
    """ИНН -> (для клиентов) расчётный счёт -> точное имя без правовой формы.

    Возвращает (партнёр, способ). Имя используется, только если подходит ровно
    один партнёр: иначе лучше оставить операцию на ручную разноску.
    """
    inn = normalize_inn(inn)
    if inn:
        found = (
            await session.execute(
                select(model).where(model.inn == inn).order_by(model.id)
            )
        ).scalars().first()
        if found:
            return found, "inn"
    if account and hasattr(model, "bank_account"):
        found = (
            await session.execute(
                select(model)
                .where(model.bank_account == account)
                .order_by(model.id)
            )
        ).scalars().first()
        if found:
            return found, "account"
    target = normalize_name(name)
    if len(target) >= 3:
        rows = (await session.execute(select(model))).scalars().all()
        exact = [
            row
            for row in rows
            if target
            in {
                normalize_name(getattr(row, "name", None)),
                normalize_name(getattr(row, "legal_name", None)),
            }
        ]
        if len(exact) == 1:
            return exact[0], "name"
    return None, None


async def _find_customer_by_inn(
    session: AsyncSession, inn: str
) -> Optional[Customer]:
    found, _ = await _find_partner(session, Customer, inn=inn, name="")
    return found


async def _find_provider_by_inn(
    session: AsyncSession, inn: str
) -> Optional[Provider]:
    found, _ = await _find_partner(session, Provider, inn=inn, name="")
    return found


async def suggest_partners(
    session: AsyncSession, txn: BankTransaction, *, limit: int = 6
) -> list[dict]:
    """Кандидаты для ручной разноски: похожие по названию клиенты/поставщики."""
    target = normalize_name(txn.counterparty_name)
    tokens = {t for t in target.split() if len(t) >= 3}
    out: list[dict] = []
    for kind, model in (("customer", Customer), ("provider", Provider)):
        rows = (await session.execute(select(model))).scalars().all()
        for row in rows:
            names = {
                normalize_name(getattr(row, "name", None)),
                normalize_name(getattr(row, "legal_name", None)),
            } - {""}
            score = 0.0
            for candidate in names:
                cand_tokens = {t for t in candidate.split() if len(t) >= 3}
                if not cand_tokens or not tokens:
                    continue
                common = len(tokens & cand_tokens)
                if candidate == target:
                    score = max(score, 1.0)
                elif common:
                    score = max(
                        score, common / max(len(tokens), len(cand_tokens))
                    )
                elif candidate in target or target in candidate:
                    score = max(score, 0.5)
            if score > 0:
                out.append(
                    {
                        "type": kind,
                        "id": row.id,
                        "name": row.name,
                        "inn": getattr(row, "inn", None),
                        "score": round(score, 2),
                    }
                )
    out.sort(key=lambda item: -item["score"])
    # Для прихода сначала клиенты, для расхода — поставщики.
    preferred = "customer" if txn.direction == "incoming" else "provider"
    out.sort(key=lambda item: (item["type"] != preferred, -item["score"]))
    return out[:limit]


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


def _remember_inn(partner, txn: BankTransaction, how: Optional[str]) -> None:
    """Нашли по названию/счёту — запоминаем ИНН, дальше узнаем по ИНН."""
    inn = normalize_inn(txn.counterparty_inn)
    if how in {"name", "account"} and inn and not getattr(partner, "inn", None):
        partner.inn = inn
        kpp = (txn.counterparty_kpp or "").strip()
        if kpp and hasattr(partner, "kpp") and not partner.kpp:
            partner.kpp = kpp


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

    non_trade = classify_operation(txn)
    if non_trade:
        txn.status = BankTxnStatus.IGNORED
        txn.category = non_trade.category
        txn.deal_ref = non_trade.deal_ref
        note = f"Не торговая операция: {non_trade.label}"
        if non_trade.deal_ref:
            note += f" № {non_trade.deal_ref}"
        txn.match_note = note
        return note

    if txn.direction == "incoming":
        # --- Try to match incoming payment to a Customer ---
        customer, how = await _find_partner(
            session,
            Customer,
            inn=txn.counterparty_inn or "",
            name=txn.counterparty_name or "",
            account=txn.counterparty_account or "",
        )
        if not customer:
            return None
        _remember_inn(customer, txn, how)

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
        provider, how = await _find_partner(
            session,
            Provider,
            inn=txn.counterparty_inn or "",
            name=txn.counterparty_name or "",
        )
        if not provider:
            return None
        _remember_inn(provider, txn, how)

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

    # Сессия работает без autoflush: без flush подсчёт не увидит только что
    # разнесённые операции и счётчик отстанет на один проход.
    await session.flush()
    await _refresh_matched_count(session, statement.id)

    await session.commit()
    return {"matched": matched, "skipped": skipped, "errors": errors}


async def assign_transaction_to_partner(
    session: AsyncSession,
    txn: BankTransaction,
    *,
    customer_id: Optional[int] = None,
    provider_id: Optional[int] = None,
    invoice_id: Optional[int] = None,
    remember: bool = True,
    note: Optional[str] = None,
) -> int:
    """Ручная разноска на выбранного клиента/поставщика.

    Создаёт платёж, при ``remember`` запоминает ИНН контрагента у клиента/
    поставщика (если там пусто) и сразу разносит остальные операции с этим
    ИНН. Возвращает, сколько ещё операций разнесено автоматически.
    """
    if txn.status == BankTxnStatus.MATCHED:
        raise ValueError("Операция уже разнесена")
    if bool(customer_id) == bool(provider_id):
        raise ValueError("Укажите клиента или поставщика")

    if customer_id:
        partner = await session.get(Customer, customer_id)
        if partner is None:
            raise ValueError("Клиент не найден")
        invoice = (
            await session.get(PaymentInvoice, invoice_id) if invoice_id else None
        )
        payment = CustomerPayment(
            customer_id=partner.id,
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
        txn.supplier_payment_id = None
        label = f"Клиент: {partner.name}"
        if invoice:
            from dz_fastapi.crud.finance import recalculate_invoice_status

            await recalculate_invoice_status(session, invoice)
            label += f", счёт № {invoice.invoice_number}"
    else:
        partner = await session.get(Provider, provider_id)
        if partner is None:
            raise ValueError("Поставщик не найден")
        payment = SupplierPayment(
            provider_id=partner.id,
            amount=txn.amount,
            payment_date=txn.value_date,
            payment_method=PaymentMethod.BANK_TRANSFER,
            reference=txn.doc_number,
            notes=txn.purpose[:500] if txn.purpose else None,
        )
        session.add(payment)
        await session.flush()
        txn.supplier_payment_id = payment.id
        txn.customer_payment_id = None
        label = f"Поставщик: {partner.name}"

    txn.status = BankTxnStatus.MATCHED
    txn.match_note = note or label

    also = 0
    inn = normalize_inn(txn.counterparty_inn)
    if remember and inn:
        if not getattr(partner, "inn", None):
            partner.inn = inn
        await session.flush()
        others = (
            await session.execute(
                select(BankTransaction).where(
                    BankTransaction.counterparty_inn == txn.counterparty_inn,
                    BankTransaction.status == BankTxnStatus.UNMATCHED,
                    BankTransaction.id != txn.id,
                )
            )
        ).scalars().all()
        touched: set[int] = set()
        for other in others:
            if await auto_match_transaction(session, other):
                also += 1
                touched.add(other.statement_id)
        for statement_id in touched | {txn.statement_id}:
            await _refresh_matched_count(session, statement_id)
    else:
        await _refresh_matched_count(session, txn.statement_id)
    await session.commit()
    return also


async def _refresh_matched_count(session: AsyncSession, statement_id: int):
    """«Разнесено» = разнесённые + служебные (обработанные) операции."""
    await session.flush()
    statement = await session.get(BankStatement, statement_id)
    if statement is None:
        return
    count = await session.scalar(
        select(func.count())
        .select_from(BankTransaction)
        .where(
            BankTransaction.statement_id == statement_id,
            BankTransaction.status.in_(
                [BankTxnStatus.MATCHED, BankTxnStatus.IGNORED]
            ),
        )
    )
    statement.matched_count = int(count or 0)


# ── Депозиты ───────────────────────────────────────────────────────────────────


async def build_deposit_summary(
    session: AsyncSession,
    *,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
) -> dict:
    """Сделки «деньги на вечер»: открытие → возврат → проценты.

    Операции связываются в сделку по номеру. В отчёт попадают сделки, у
    которых хотя бы одна операция лежит в периоде; проценты в итогах — только
    те, что выплачены в периоде. Ставка — годовая: проценты / сумма / дни * 365.
    """
    rows = (
        await session.execute(
            select(BankTransaction)
            .where(BankTransaction.category.in_(DEPOSIT_CATEGORIES))
            .order_by(BankTransaction.value_date, BankTransaction.id)
        )
    ).scalars().all()

    deals: dict[str, dict] = {}
    for txn in rows:
        key = txn.deal_ref or f"txn-{txn.id}"
        deal = deals.setdefault(
            key,
            {
                "deal_ref": txn.deal_ref,
                "opened_on": None,
                "returned_on": None,
                "principal": Decimal("0"),
                "returned_amount": Decimal("0"),
                "interest": Decimal("0"),
                "dates": [],
                "interest_in_period": Decimal("0"),
            },
        )
        deal["dates"].append(txn.value_date)
        amount = Decimal(str(txn.amount))
        if txn.category == "deposit_open":
            deal["opened_on"] = txn.value_date
            deal["principal"] += amount
        elif txn.category == "deposit_return":
            deal["returned_on"] = txn.value_date
            deal["returned_amount"] += amount
        else:
            deal["interest"] += amount
            in_period = (date_from is None or txn.value_date >= date_from) and (
                date_to is None or txn.value_date <= date_to
            )
            if in_period:
                deal["interest_in_period"] += amount

    def _in_period(deal: dict) -> bool:
        return any(
            (date_from is None or d >= date_from)
            and (date_to is None or d <= date_to)
            for d in deal["dates"]
        )

    out_deals = []
    interest_total = Decimal("0")
    weighted_days = Decimal("0")  # sum(principal * days) по закрытым сделкам
    weighted_interest = Decimal("0")
    open_principal = Decimal("0")
    turnover = Decimal("0")
    for deal in deals.values():
        if not _in_period(deal):
            continue
        closed = deal["returned_on"] is not None
        days = None
        rate = None
        if deal["opened_on"] and closed:
            days = max((deal["returned_on"] - deal["opened_on"]).days, 1)
            if deal["principal"] > 0:
                rate = (
                    deal["interest"] / deal["principal"] / days * 365 * 100
                ).quantize(Decimal("0.01"))
                weighted_days += deal["principal"] * days
                weighted_interest += deal["interest"]
        if not closed and deal["principal"] > 0:
            open_principal += deal["principal"]
        turnover += deal["principal"]
        interest_total += deal["interest_in_period"]
        out_deals.append(
            {
                "deal_ref": deal["deal_ref"],
                "status": "closed" if closed else "open",
                "opened_on": deal["opened_on"],
                "returned_on": deal["returned_on"],
                "principal": deal["principal"],
                "returned_amount": deal["returned_amount"],
                "interest": deal["interest"],
                "days": days,
                "rate_percent": rate,
            }
        )
    out_deals.sort(
        key=lambda d: d["opened_on"] or d["returned_on"] or date.min,
        reverse=True,
    )
    average = None
    if weighted_days > 0:
        average = (weighted_interest / weighted_days * 365 * 100).quantize(
            Decimal("0.01")
        )
    return {
        "totals": {
            "interest_total": interest_total,
            "deals_total": len(out_deals),
            "deals_open": sum(1 for d in out_deals if d["status"] == "open"),
            "open_principal": open_principal,
            "average_rate_percent": average,
            "principal_turnover": turnover,
        },
        "deals": out_deals,
    }
