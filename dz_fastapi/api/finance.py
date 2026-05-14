"""API routes for financial documents: invoices, payments, bank statements."""

import logging
from datetime import date
from typing import List, Optional

from fastapi import APIRouter, Body, Depends, File, HTTPException, Query, UploadFile, status
from fastapi.responses import HTMLResponse
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.core.db import get_session
from dz_fastapi.crud.finance import (
    add_invoice_item,
    create_customer_payment,
    create_invoice,
    create_supplier_payment,
    delete_customer_payment,
    delete_invoice,
    delete_invoice_item,
    delete_supplier_payment,
    get_creditors_report,
    get_customer_debt,
    get_customer_payment,
    get_debtors_report,
    get_invoice,
    get_invoice_item,
    get_supplier_payment,
    list_customer_payments,
    list_invoices,
    list_supplier_payments,
    update_customer_payment,
    update_invoice,
    update_invoice_item,
    update_supplier_payment,
)
from dz_fastapi.models.finance import InvoiceStatus
from dz_fastapi.models.partner import Customer
from dz_fastapi.schemas.finance import (
    AutoMatchResult,
    BankAccountCreate,
    BankAccountOut,
    BankStatementOut,
    BankTransactionMatchRequest,
    BankTransactionOut,
    CustomerDebtOut,
    CustomerPaymentCreate,
    CustomerPaymentOut,
    CustomerPaymentUpdate,
    PaymentInvoiceCreate,
    PaymentInvoiceItemCreate,
    PaymentInvoiceItemOut,
    PaymentInvoiceItemUpdate,
    PaymentInvoiceListOut,
    PaymentInvoiceOut,
    PaymentInvoiceUpdate,
    ProviderDebtOut,
    SupplierPaymentCreate,
    SupplierPaymentOut,
    SupplierPaymentUpdate,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/finance", tags=["Finance"])


# ──────────────────────────────────────────────────────────────────────────────
# PaymentInvoice (Счёт на оплату)
# ──────────────────────────────────────────────────────────────────────────────


@router.post(
    "/invoices",
    response_model=PaymentInvoiceOut,
    status_code=status.HTTP_201_CREATED,
    summary="Создать счёт на оплату",
)
async def create_invoice_endpoint(
    data: PaymentInvoiceCreate,
    session: AsyncSession = Depends(get_session),
):
    # ── Проверка кредитного лимита ──────────────────────────────────────────
    customer_result = await session.execute(
        select(Customer).where(Customer.id == data.customer_id)
    )
    customer = customer_result.scalar_one_or_none()
    if not customer:
        raise HTTPException(status_code=404, detail="Клиент не найден")

    if customer.credit_limit is not None and customer.credit_limit > 0:
        # Считаем текущий долг (открытые счета)
        from decimal import Decimal

        from sqlalchemy import func

        from dz_fastapi.models.finance import PaymentInvoice as PI

        debt_result = await session.execute(
            select(
                func.coalesce(func.sum(PI.total_amount - PI.paid_amount), 0)
            ).where(
                PI.customer_id == data.customer_id,
                PI.status.in_(
                    [
                        InvoiceStatus.SENT,
                        InvoiceStatus.PARTIALLY_PAID,
                        InvoiceStatus.OVERDUE,
                    ]
                ),
            )
        )
        current_debt = Decimal(str(debt_result.scalar() or 0))
        new_invoice_amount = Decimal(str(data.total_amount))
        if current_debt + new_invoice_amount > customer.credit_limit:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=(
                    f"Превышен кредитный лимит клиента. "
                    f"Лимит: {customer.credit_limit:.2f} руб., "
                    f"текущий долг: {current_debt:.2f} руб., "
                    f"новый счёт: {new_invoice_amount:.2f} руб."
                ),
            )
    return await create_invoice(session, data)


@router.get(
    "/invoices",
    response_model=List[PaymentInvoiceListOut],
    summary="Список счетов на оплату",
)
async def list_invoices_endpoint(
    customer_id: Optional[int] = Query(default=None),
    invoice_status: Optional[InvoiceStatus] = Query(
        default=None, alias="status"
    ),
    date_from: Optional[date] = Query(default=None),
    date_to: Optional[date] = Query(default=None),
    skip: int = Query(default=0, ge=0),
    limit: int = Query(default=100, ge=1, le=500),
    session: AsyncSession = Depends(get_session),
):
    invoices = await list_invoices(
        session,
        customer_id=customer_id,
        status=invoice_status,
        date_from=date_from,
        date_to=date_to,
        skip=skip,
        limit=limit,
    )
    result = []
    for inv in invoices:
        item = PaymentInvoiceListOut.model_validate(inv)
        if inv.customer:
            item.customer_name = inv.customer.name
        result.append(item)
    return result


@router.get(
    "/invoices/{invoice_id}",
    response_model=PaymentInvoiceOut,
    summary="Счёт на оплату по ID",
)
async def get_invoice_endpoint(
    invoice_id: int,
    session: AsyncSession = Depends(get_session),
):
    invoice = await get_invoice(session, invoice_id)
    if not invoice:
        raise HTTPException(status_code=404, detail="Счёт не найден")
    return invoice


@router.patch(
    "/invoices/{invoice_id}",
    response_model=PaymentInvoiceOut,
    summary="Обновить счёт на оплату",
)
async def update_invoice_endpoint(
    invoice_id: int,
    data: PaymentInvoiceUpdate,
    session: AsyncSession = Depends(get_session),
):
    invoice = await get_invoice(session, invoice_id)
    if not invoice:
        raise HTTPException(status_code=404, detail="Счёт не найден")
    return await update_invoice(session, invoice, data)


@router.delete(
    "/invoices/{invoice_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Удалить счёт на оплату",
)
async def delete_invoice_endpoint(
    invoice_id: int,
    session: AsyncSession = Depends(get_session),
):
    invoice = await get_invoice(session, invoice_id)
    if not invoice:
        raise HTTPException(status_code=404, detail="Счёт не найден")
    if invoice.status not in (InvoiceStatus.DRAFT, InvoiceStatus.CANCELLED):
        raise HTTPException(
            status_code=400,
            detail="Удалить можно только счета в статусе DRAFT или CANCELLED",
        )
    await delete_invoice(session, invoice)


# ──────────────────────────────────────────────────────────────────────────────
# PaymentInvoiceItem (Позиции счёта)
# ──────────────────────────────────────────────────────────────────────────────


@router.get(
    "/invoices/{invoice_id}/items",
    response_model=List[PaymentInvoiceItemOut],
    summary="Позиции счёта",
)
async def list_invoice_items_endpoint(
    invoice_id: int,
    session: AsyncSession = Depends(get_session),
):
    invoice = await get_invoice(session, invoice_id)
    if not invoice:
        raise HTTPException(status_code=404, detail="Счёт не найден")
    return invoice.items


@router.post(
    "/invoices/{invoice_id}/items",
    response_model=PaymentInvoiceItemOut,
    status_code=status.HTTP_201_CREATED,
    summary="Добавить позицию к счёту",
)
async def add_invoice_item_endpoint(
    invoice_id: int,
    data: PaymentInvoiceItemCreate,
    session: AsyncSession = Depends(get_session),
):
    invoice = await get_invoice(session, invoice_id)
    if not invoice:
        raise HTTPException(status_code=404, detail="Счёт не найден")
    return await add_invoice_item(session, invoice, data)


@router.patch(
    "/invoices/{invoice_id}/items/{item_id}",
    response_model=PaymentInvoiceItemOut,
    summary="Обновить позицию счёта",
)
async def update_invoice_item_endpoint(
    invoice_id: int,
    item_id: int,
    data: PaymentInvoiceItemUpdate,
    session: AsyncSession = Depends(get_session),
):
    item = await get_invoice_item(session, item_id)
    if not item or item.invoice_id != invoice_id:
        raise HTTPException(status_code=404, detail="Позиция не найдена")
    return await update_invoice_item(session, item, data)


@router.delete(
    "/invoices/{invoice_id}/items/{item_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Удалить позицию счёта",
)
async def delete_invoice_item_endpoint(
    invoice_id: int,
    item_id: int,
    session: AsyncSession = Depends(get_session),
):
    item = await get_invoice_item(session, item_id)
    if not item or item.invoice_id != invoice_id:
        raise HTTPException(status_code=404, detail="Позиция не найдена")
    await delete_invoice_item(session, item)


# ──────────────────────────────────────────────────────────────────────────────
# CustomerPayment (Оплата от клиента)
# ──────────────────────────────────────────────────────────────────────────────


@router.post(
    "/customer-payments",
    response_model=CustomerPaymentOut,
    status_code=status.HTTP_201_CREATED,
    summary="Зарегистрировать оплату от клиента",
)
async def create_customer_payment_endpoint(
    data: CustomerPaymentCreate,
    session: AsyncSession = Depends(get_session),
):
    return await create_customer_payment(session, data)


@router.get(
    "/customer-payments",
    response_model=List[CustomerPaymentOut],
    summary="Список оплат от клиентов",
)
async def list_customer_payments_endpoint(
    customer_id: Optional[int] = Query(default=None),
    invoice_id: Optional[int] = Query(default=None),
    date_from: Optional[date] = Query(default=None),
    date_to: Optional[date] = Query(default=None),
    skip: int = Query(default=0, ge=0),
    limit: int = Query(default=100, ge=1, le=500),
    session: AsyncSession = Depends(get_session),
):
    payments = await list_customer_payments(
        session,
        customer_id=customer_id,
        invoice_id=invoice_id,
        date_from=date_from,
        date_to=date_to,
        skip=skip,
        limit=limit,
    )
    result = []
    for p in payments:
        item = CustomerPaymentOut.model_validate(p)
        if p.customer:
            item.customer_name = p.customer.name
        result.append(item)
    return result


@router.get(
    "/customer-payments/{payment_id}",
    response_model=CustomerPaymentOut,
    summary="Оплата клиента по ID",
)
async def get_customer_payment_endpoint(
    payment_id: int,
    session: AsyncSession = Depends(get_session),
):
    payment = await get_customer_payment(session, payment_id)
    if not payment:
        raise HTTPException(status_code=404, detail="Оплата не найдена")
    return payment


@router.patch(
    "/customer-payments/{payment_id}",
    response_model=CustomerPaymentOut,
    summary="Обновить оплату клиента",
)
async def update_customer_payment_endpoint(
    payment_id: int,
    data: CustomerPaymentUpdate,
    session: AsyncSession = Depends(get_session),
):
    payment = await get_customer_payment(session, payment_id)
    if not payment:
        raise HTTPException(status_code=404, detail="Оплата не найдена")
    return await update_customer_payment(session, payment, data)


@router.delete(
    "/customer-payments/{payment_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Удалить оплату клиента",
)
async def delete_customer_payment_endpoint(
    payment_id: int,
    session: AsyncSession = Depends(get_session),
):
    payment = await get_customer_payment(session, payment_id)
    if not payment:
        raise HTTPException(status_code=404, detail="Оплата не найдена")
    await delete_customer_payment(session, payment)


# ──────────────────────────────────────────────────────────────────────────────
# SupplierPayment (Оплата поставщику)
# ──────────────────────────────────────────────────────────────────────────────


@router.post(
    "/supplier-payments",
    response_model=SupplierPaymentOut,
    status_code=status.HTTP_201_CREATED,
    summary="Зарегистрировать оплату поставщику",
)
async def create_supplier_payment_endpoint(
    data: SupplierPaymentCreate,
    session: AsyncSession = Depends(get_session),
):
    return await create_supplier_payment(session, data)


@router.get(
    "/supplier-payments",
    response_model=List[SupplierPaymentOut],
    summary="Список оплат поставщикам",
)
async def list_supplier_payments_endpoint(
    provider_id: Optional[int] = Query(default=None),
    supplier_order_id: Optional[int] = Query(default=None),
    date_from: Optional[date] = Query(default=None),
    date_to: Optional[date] = Query(default=None),
    skip: int = Query(default=0, ge=0),
    limit: int = Query(default=100, ge=1, le=500),
    session: AsyncSession = Depends(get_session),
):
    payments = await list_supplier_payments(
        session,
        provider_id=provider_id,
        supplier_order_id=supplier_order_id,
        date_from=date_from,
        date_to=date_to,
        skip=skip,
        limit=limit,
    )
    result = []
    for p in payments:
        item = SupplierPaymentOut.model_validate(p)
        if p.provider:
            item.provider_name = p.provider.name
        result.append(item)
    return result


@router.get(
    "/supplier-payments/{payment_id}",
    response_model=SupplierPaymentOut,
    summary="Оплата поставщику по ID",
)
async def get_supplier_payment_endpoint(
    payment_id: int,
    session: AsyncSession = Depends(get_session),
):
    payment = await get_supplier_payment(session, payment_id)
    if not payment:
        raise HTTPException(status_code=404, detail="Оплата не найдена")
    return payment


@router.patch(
    "/supplier-payments/{payment_id}",
    response_model=SupplierPaymentOut,
    summary="Обновить оплату поставщику",
)
async def update_supplier_payment_endpoint(
    payment_id: int,
    data: SupplierPaymentUpdate,
    session: AsyncSession = Depends(get_session),
):
    payment = await get_supplier_payment(session, payment_id)
    if not payment:
        raise HTTPException(status_code=404, detail="Оплата не найдена")
    return await update_supplier_payment(session, payment, data)


@router.delete(
    "/supplier-payments/{payment_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Удалить оплату поставщику",
)
async def delete_supplier_payment_endpoint(
    payment_id: int,
    session: AsyncSession = Depends(get_session),
):
    payment = await get_supplier_payment(session, payment_id)
    if not payment:
        raise HTTPException(status_code=404, detail="Оплата не найдена")
    await delete_supplier_payment(session, payment)


# ──────────────────────────────────────────────────────────────────────────────
# Invoice print & email
# ──────────────────────────────────────────────────────────────────────────────


@router.get(
    "/invoices/{invoice_id}/print",
    response_class=HTMLResponse,
    summary="HTML-версия счёта для печати / сохранения PDF",
)
async def print_invoice_endpoint(
    invoice_id: int,
    session: AsyncSession = Depends(get_session),
):
    from dz_fastapi.services.finance import render_invoice_html

    invoice = await get_invoice(session, invoice_id)
    if not invoice:
        raise HTTPException(status_code=404, detail="Счёт не найден")
    html = await render_invoice_html(session, invoice)
    return HTMLResponse(content=html)


@router.post(
    "/invoices/{invoice_id}/send-email",
    status_code=status.HTTP_200_OK,
    summary="Отправить счёт клиенту по email",
)
async def send_invoice_email_endpoint(
    invoice_id: int,
    to_email: Optional[str] = Body(default=None, embed=True),
    session: AsyncSession = Depends(get_session),
):
    from dz_fastapi.services.finance import send_invoice_email

    invoice = await get_invoice(session, invoice_id)
    if not invoice:
        raise HTTPException(status_code=404, detail="Счёт не найден")
    ok = await send_invoice_email(session, invoice, to_email=to_email)
    if not ok:
        raise HTTPException(
            status_code=500,
            detail="Не удалось отправить письмо. Проверьте настройки почты.",
        )
    return {"detail": "Счёт отправлен"}


# ──────────────────────────────────────────────────────────────────────────────
# Debt / Reports
# ──────────────────────────────────────────────────────────────────────────────


@router.get(
    "/debtors",
    response_model=List[CustomerDebtOut],
    summary="Отчёт по дебиторской задолженности",
)
async def debtors_report_endpoint(
    only_overdue: bool = Query(
        default=False, description="Только просроченная задолженность"
    ),
    session: AsyncSession = Depends(get_session),
):
    return await get_debtors_report(session, only_overdue=only_overdue)


@router.get(
    "/debtors/{customer_id}",
    response_model=CustomerDebtOut,
    summary="Задолженность конкретного клиента",
)
async def customer_debt_endpoint(
    customer_id: int,
    session: AsyncSession = Depends(get_session),
):
    result = await get_customer_debt(session, customer_id)
    if not result:
        raise HTTPException(status_code=404, detail="Клиент не найден")
    return result


@router.get(
    "/creditors",
    response_model=List[ProviderDebtOut],
    summary="Отчёт по кредиторской задолженности (к оплате поставщикам)",
)
async def creditors_report_endpoint(
    only_owed: bool = Query(
        default=False, description="Только поставщики с долгом"
    ),
    session: AsyncSession = Depends(get_session),
):
    return await get_creditors_report(session, only_owed=only_owed)


# ──────────────────────────────────────────────────────────────────────────────
# BankAccount — расчётные счета организации
# ──────────────────────────────────────────────────────────────────────────────


@router.get(
    "/bank-accounts",
    response_model=List[BankAccountOut],
    summary="Расчётные счета",
)
async def list_bank_accounts(session: AsyncSession = Depends(get_session)):
    from dz_fastapi.models.finance import BankAccount

    result = await session.execute(
        select(BankAccount).order_by(BankAccount.id)
    )
    return result.scalars().all()


@router.post(
    "/bank-accounts",
    response_model=BankAccountOut,
    status_code=status.HTTP_201_CREATED,
    summary="Добавить расчётный счёт",
)
async def create_bank_account(
    data: BankAccountCreate,
    session: AsyncSession = Depends(get_session),
):
    from dz_fastapi.models.finance import BankAccount

    acc = BankAccount(**data.model_dump())
    session.add(acc)
    await session.commit()
    await session.refresh(acc)
    return acc


@router.delete(
    "/bank-accounts/{account_id}", status_code=status.HTTP_204_NO_CONTENT
)
async def delete_bank_account(
    account_id: int,
    session: AsyncSession = Depends(get_session),
):
    from dz_fastapi.models.finance import BankAccount

    acc = await session.get(BankAccount, account_id)
    if not acc:
        raise HTTPException(status_code=404, detail="Счёт не найден")
    await session.delete(acc)
    await session.commit()


# ──────────────────────────────────────────────────────────────────────────────
# BankStatement — загрузка выписок
# ──────────────────────────────────────────────────────────────────────────────


@router.post(
    "/bank-statements/upload",
    response_model=BankStatementOut,
    status_code=status.HTTP_201_CREATED,
    summary="Загрузить выписку банка (CSV или 1CClientBankExchange .txt)",
)
async def upload_bank_statement(
    file: UploadFile = File(...),
    bank_account_id: Optional[int] = Query(default=None),
    auto_match: bool = Query(
        default=True, description="Запустить авторазноску сразу после загрузки"
    ),
    session: AsyncSession = Depends(get_session),
):
    from dz_fastapi.models.finance import BankAccount, BankStatement, BankTransaction
    from dz_fastapi.services.bank_reconciliation import auto_match_statement
    from dz_fastapi.services.bank_statement_parser import detect_and_parse

    content = await file.read()
    if not content:
        raise HTTPException(status_code=400, detail="Файл пустой")

    parsed = detect_and_parse(content, filename=file.filename or "")
    if parsed.format == "unknown" and not parsed.transactions:
        detail = "Неизвестный формат файла. " + "; ".join(parsed.errors[:3])
        raise HTTPException(status_code=422, detail=detail)

    # Resolve / auto-create BankAccount
    if bank_account_id:
        bank_account = await session.get(BankAccount, bank_account_id)
        if not bank_account:
            raise HTTPException(
                status_code=404, detail="Расчётный счёт не найден"
            )
    elif parsed.account_number:
        result_acc = await session.execute(
            select(BankAccount).where(
                BankAccount.account_number == parsed.account_number
            )
        )
        bank_account = result_acc.scalar_one_or_none()
        if bank_account is None:
            bank_account = BankAccount(
                account_number=parsed.account_number,
                bank_name=parsed.bank_name or "Неизвестный банк",
                bik=parsed.bik,
            )
            session.add(bank_account)
            await session.flush()
    else:
        bank_account = None

    stmt = BankStatement(
        bank_account_id=bank_account.id if bank_account else None,
        period_from=parsed.period_from or date.today(),
        period_to=parsed.period_to or date.today(),
        opening_balance=parsed.opening_balance,
        closing_balance=parsed.closing_balance,
        total_incoming=parsed.total_incoming,
        total_outgoing=parsed.total_outgoing,
        format=parsed.format,
        filename=file.filename,
        txn_count=len(parsed.transactions),
        matched_count=0,
    )
    session.add(stmt)
    await session.flush()

    for pt in parsed.transactions:
        txn = BankTransaction(
            statement_id=stmt.id,
            doc_number=pt.doc_number,
            doc_date=pt.doc_date,
            value_date=pt.value_date,
            direction=pt.direction,
            amount=pt.amount,
            vat_amount=pt.vat_amount,
            currency=pt.currency,
            purpose=pt.purpose,
            balance_after=pt.balance_after,
            counterparty_name=pt.counterparty_name,
            counterparty_inn=pt.counterparty_inn,
            counterparty_kpp=pt.counterparty_kpp,
            counterparty_account=pt.counterparty_account,
            counterparty_bank=pt.counterparty_bank,
            counterparty_bik=pt.counterparty_bik,
        )
        session.add(txn)

    await session.commit()
    await session.refresh(stmt)

    if auto_match and parsed.transactions:
        await auto_match_statement(session, stmt)
        await session.refresh(stmt)

    # Enrich output
    out = BankStatementOut.model_validate(stmt)
    if bank_account:
        out.bank_account_number = bank_account.account_number
        out.bank_account_bank = bank_account.bank_name
    return out


@router.get(
    "/bank-statements",
    response_model=List[BankStatementOut],
    summary="Список выписок",
)
async def list_bank_statements(
    bank_account_id: Optional[int] = Query(default=None),
    skip: int = Query(default=0, ge=0),
    limit: int = Query(default=50, ge=1, le=200),
    session: AsyncSession = Depends(get_session),
):
    from dz_fastapi.models.finance import BankStatement

    q = (
        select(BankStatement)
        .order_by(BankStatement.uploaded_at.desc())
        .offset(skip)
        .limit(limit)
    )
    if bank_account_id:
        q = q.where(BankStatement.bank_account_id == bank_account_id)
    result = await session.execute(q)
    stmts = result.scalars().all()
    out = []
    for s in stmts:
        item = BankStatementOut.model_validate(s)
        if s.bank_account:
            item.bank_account_number = s.bank_account.account_number
            item.bank_account_bank = s.bank_account.bank_name
        out.append(item)
    return out


@router.delete(
    "/bank-statements/{stmt_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Удалить выписку и все её транзакции",
)
async def delete_bank_statement(
    stmt_id: int,
    session: AsyncSession = Depends(get_session),
):
    from dz_fastapi.models.finance import BankStatement

    stmt = await session.get(BankStatement, stmt_id)
    if not stmt:
        raise HTTPException(status_code=404, detail="Выписка не найдена")
    await session.delete(stmt)
    await session.commit()


# ──────────────────────────────────────────────────────────────────────────────
# BankTransaction — просмотр и разноска
# ──────────────────────────────────────────────────────────────────────────────


@router.get(
    "/bank-statements/{stmt_id}/transactions",
    response_model=List[BankTransactionOut],
    summary="Транзакции выписки",
)
async def list_bank_transactions(
    stmt_id: int,
    direction: Optional[str] = Query(
        default=None, description="incoming | outgoing"
    ),
    txn_status: Optional[str] = Query(default=None, alias="status"),
    skip: int = Query(default=0, ge=0),
    limit: int = Query(default=200, ge=1, le=1000),
    session: AsyncSession = Depends(get_session),
):
    from dz_fastapi.models.finance import BankStatement, BankTransaction

    stmt = await session.get(BankStatement, stmt_id)
    if not stmt:
        raise HTTPException(status_code=404, detail="Выписка не найдена")
    q = (
        select(BankTransaction)
        .where(BankTransaction.statement_id == stmt_id)
        .order_by(BankTransaction.value_date)
        .offset(skip)
        .limit(limit)
    )
    if direction:
        q = q.where(BankTransaction.direction == direction)
    if txn_status:
        q = q.where(BankTransaction.status == txn_status)
    result = await session.execute(q)
    return result.scalars().all()


@router.patch(
    "/bank-statements/{stmt_id}/transactions/{txn_id}",
    response_model=BankTransactionOut,
    summary="Ручная разноска / пометить как пропущенную",
)
async def match_bank_transaction(
    stmt_id: int,
    txn_id: int,
    data: BankTransactionMatchRequest,
    session: AsyncSession = Depends(get_session),
):
    from dz_fastapi.models.finance import BankTransaction, BankTxnStatus

    txn = await session.get(BankTransaction, txn_id)
    if not txn or txn.statement_id != stmt_id:
        raise HTTPException(status_code=404, detail="Транзакция не найдена")

    if data.ignore:
        txn.status = BankTxnStatus.IGNORED
        txn.match_note = data.match_note or "Помечено вручную как пропущенное"
    else:
        if data.customer_payment_id:
            txn.customer_payment_id = data.customer_payment_id
            txn.supplier_payment_id = None
            txn.status = BankTxnStatus.MATCHED
        elif data.supplier_payment_id:
            txn.supplier_payment_id = data.supplier_payment_id
            txn.customer_payment_id = None
            txn.status = BankTxnStatus.MATCHED
        if data.match_note:
            txn.match_note = data.match_note

    await session.commit()
    await session.refresh(txn)
    return txn


@router.post(
    "/bank-statements/{stmt_id}/auto-match",
    response_model=AutoMatchResult,
    summary="Запустить авторазноску по выписке",
)
async def auto_match_bank_statement(
    stmt_id: int,
    session: AsyncSession = Depends(get_session),
):
    from dz_fastapi.models.finance import BankStatement
    from dz_fastapi.services.bank_reconciliation import auto_match_statement

    stmt = await session.get(BankStatement, stmt_id)
    if not stmt:
        raise HTTPException(status_code=404, detail="Выписка не найдена")
    result = await auto_match_statement(session, stmt)
    return result
