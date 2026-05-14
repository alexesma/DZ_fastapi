"""Pydantic schemas for Financial Documents."""

from datetime import date, datetime
from decimal import Decimal
from typing import List, Optional

from pydantic import BaseModel, ConfigDict, Field, condecimal

from dz_fastapi.models.finance import InvoiceStatus, PaymentMethod
from dz_fastapi.models.inventory import SyncStatus

MoneyDecimal = condecimal(max_digits=12, decimal_places=2, ge=0)


# ─── PaymentInvoiceItem ──────────────────────────────────────────────────────


class PaymentInvoiceItemBase(BaseModel):
    position: int = 1
    autopart_id: Optional[int] = None
    name: str = Field(..., min_length=1, max_length=500)
    oem_number: Optional[str] = Field(default=None, max_length=100)
    quantity: Decimal = Decimal("1.000")
    unit_price: MoneyDecimal = Decimal("0.00")
    vat_rate: Decimal = Decimal("20.00")


class PaymentInvoiceItemCreate(PaymentInvoiceItemBase):
    pass


class PaymentInvoiceItemUpdate(BaseModel):
    position: Optional[int] = None
    name: Optional[str] = Field(default=None, min_length=1, max_length=500)
    oem_number: Optional[str] = Field(default=None, max_length=100)
    quantity: Optional[Decimal] = None
    unit_price: Optional[MoneyDecimal] = None
    vat_rate: Optional[Decimal] = None


class PaymentInvoiceItemOut(PaymentInvoiceItemBase):
    id: int
    invoice_id: int
    total: Decimal

    model_config = ConfigDict(from_attributes=True)


# ─── PaymentInvoice ──────────────────────────────────────────────────────────


class PaymentInvoiceBase(BaseModel):
    customer_id: int
    shipment_id: Optional[int] = None
    customer_order_id: Optional[int] = None
    invoice_number: str = Field(..., min_length=1, max_length=50)
    invoice_date: date
    due_date: Optional[date] = None
    total_amount: MoneyDecimal = Decimal("0.00")
    notes: Optional[str] = None


class PaymentInvoiceCreate(PaymentInvoiceBase):
    items: List["PaymentInvoiceItemCreate"] = []


class PaymentInvoiceUpdate(BaseModel):
    shipment_id: Optional[int] = None
    customer_order_id: Optional[int] = None
    invoice_number: Optional[str] = Field(
        default=None, min_length=1, max_length=50
    )
    invoice_date: Optional[date] = None
    due_date: Optional[date] = None
    total_amount: Optional[MoneyDecimal] = None
    status: Optional[InvoiceStatus] = None
    notes: Optional[str] = None
    external_id: Optional[str] = Field(default=None, max_length=100)
    sync_status: Optional[SyncStatus] = None


class CustomerPaymentShort(BaseModel):
    """Краткое представление оплаты — для вложения в счёт."""

    id: int
    amount: Decimal
    payment_date: date
    payment_method: PaymentMethod
    reference: Optional[str] = None
    model_config = ConfigDict(from_attributes=True)


class PaymentInvoiceOut(PaymentInvoiceBase):
    id: int
    paid_amount: Decimal
    status: InvoiceStatus
    external_id: Optional[str] = None
    sync_status: SyncStatus
    created_at: datetime
    updated_at: Optional[datetime] = None
    payments: List[CustomerPaymentShort] = []
    items: List[PaymentInvoiceItemOut] = []

    model_config = ConfigDict(from_attributes=True)


class PaymentInvoiceListOut(BaseModel):
    """Строка в списке счетов — без вложенных оплат."""

    id: int
    customer_id: int
    customer_name: Optional[str] = None
    invoice_number: str
    invoice_date: date
    due_date: Optional[date] = None
    total_amount: Decimal
    paid_amount: Decimal
    status: InvoiceStatus
    created_at: datetime

    model_config = ConfigDict(from_attributes=True)


# ─── CustomerPayment ─────────────────────────────────────────────────────────


class CustomerPaymentBase(BaseModel):
    customer_id: int
    invoice_id: Optional[int] = None
    amount: MoneyDecimal
    payment_date: date
    payment_method: PaymentMethod = PaymentMethod.BANK_TRANSFER
    reference: Optional[str] = Field(default=None, max_length=255)
    notes: Optional[str] = None


class CustomerPaymentCreate(CustomerPaymentBase):
    pass


class CustomerPaymentUpdate(BaseModel):
    invoice_id: Optional[int] = None
    amount: Optional[MoneyDecimal] = None
    payment_date: Optional[date] = None
    payment_method: Optional[PaymentMethod] = None
    reference: Optional[str] = Field(default=None, max_length=255)
    notes: Optional[str] = None
    external_id: Optional[str] = Field(default=None, max_length=100)
    sync_status: Optional[SyncStatus] = None


class CustomerPaymentOut(CustomerPaymentBase):
    id: int
    external_id: Optional[str] = None
    sync_status: SyncStatus
    created_at: datetime
    updated_at: Optional[datetime] = None
    # Краткая инфо по клиенту
    customer_name: Optional[str] = None

    model_config = ConfigDict(from_attributes=True)


# ─── SupplierPayment ─────────────────────────────────────────────────────────


class SupplierPaymentBase(BaseModel):
    provider_id: int
    supplier_order_id: Optional[int] = None
    amount: MoneyDecimal
    payment_date: date
    payment_method: PaymentMethod = PaymentMethod.BANK_TRANSFER
    reference: Optional[str] = Field(default=None, max_length=255)
    notes: Optional[str] = None


class SupplierPaymentCreate(SupplierPaymentBase):
    pass


class SupplierPaymentUpdate(BaseModel):
    supplier_order_id: Optional[int] = None
    amount: Optional[MoneyDecimal] = None
    payment_date: Optional[date] = None
    payment_method: Optional[PaymentMethod] = None
    reference: Optional[str] = Field(default=None, max_length=255)
    notes: Optional[str] = None
    external_id: Optional[str] = Field(default=None, max_length=100)
    sync_status: Optional[SyncStatus] = None


class SupplierPaymentOut(SupplierPaymentBase):
    id: int
    external_id: Optional[str] = None
    sync_status: SyncStatus
    created_at: datetime
    updated_at: Optional[datetime] = None
    provider_name: Optional[str] = None

    model_config = ConfigDict(from_attributes=True)


# ─── Customer finance fields ──────────────────────────────────────────────────


class CustomerFinanceUpdate(BaseModel):
    """Обновление финансовых параметров клиента."""

    credit_limit: Optional[Decimal] = Field(default=None, ge=0)
    payment_terms_days: Optional[int] = Field(default=None, ge=0)


class CustomerDebtOut(BaseModel):
    """Задолженность клиента — суммарный отчёт."""

    customer_id: int
    customer_name: str
    total_invoiced: Decimal
    total_paid: Decimal
    debt: Decimal
    overdue_amount: Decimal
    credit_limit: Optional[Decimal] = None
    payment_terms_days: int

    model_config = ConfigDict(from_attributes=True)


# ─── Provider finance fields ──────────────────────────────────────────────────


class ProviderFinanceUpdate(BaseModel):
    """Обновление финансовых параметров поставщика."""

    payment_terms_days: Optional[int] = Field(default=None, ge=0)


class ProviderDebtOut(BaseModel):
    """Кредиторская задолженность перед поставщиком — суммарный отчёт."""

    provider_id: int
    provider_name: str
    total_ordered: Decimal  # сумма заказов (price * qty из SupplierOrderItem)
    total_paid: Decimal  # уже оплачено (SupplierPayment)
    owed: Decimal  # к оплате (total_ordered - total_paid, ≥ 0)
    last_payment_date: Optional[date] = None
    payment_terms_days: int

    model_config = ConfigDict(from_attributes=True)


# ─── BankAccount ──────────────────────────────────────────────────────────────


class BankAccountCreate(BaseModel):
    account_number: str = Field(..., min_length=20, max_length=20)
    bank_name: str = Field(..., min_length=1, max_length=255)
    bik: Optional[str] = Field(default=None, max_length=9)
    corr_account: Optional[str] = Field(default=None, max_length=20)
    currency: str = Field(default="RUB", max_length=3)
    description: Optional[str] = Field(default=None, max_length=255)


class BankAccountOut(BankAccountCreate):
    id: int
    is_active: bool
    created_at: datetime
    model_config = ConfigDict(from_attributes=True)


# ─── BankStatement ─────────────────────────────────────────────────────────────


class BankStatementOut(BaseModel):
    id: int
    bank_account_id: Optional[int] = None
    bank_account_number: Optional[str] = None
    bank_account_bank: Optional[str] = None
    period_from: date
    period_to: date
    opening_balance: Optional[Decimal] = None
    closing_balance: Optional[Decimal] = None
    total_incoming: Optional[Decimal] = None
    total_outgoing: Optional[Decimal] = None
    format: str
    filename: Optional[str] = None
    uploaded_at: datetime
    txn_count: int
    matched_count: int
    model_config = ConfigDict(from_attributes=True)


# ─── BankTransaction ──────────────────────────────────────────────────────────


class BankTransactionOut(BaseModel):
    id: int
    statement_id: int
    doc_number: Optional[str] = None
    doc_date: Optional[date] = None
    value_date: date
    direction: str
    amount: Decimal
    vat_amount: Optional[Decimal] = None
    currency: str
    purpose: Optional[str] = None
    balance_after: Optional[Decimal] = None
    counterparty_name: Optional[str] = None
    counterparty_inn: Optional[str] = None
    counterparty_kpp: Optional[str] = None
    counterparty_account: Optional[str] = None
    counterparty_bank: Optional[str] = None
    status: str
    match_note: Optional[str] = None
    customer_payment_id: Optional[int] = None
    supplier_payment_id: Optional[int] = None
    model_config = ConfigDict(from_attributes=True)


class BankTransactionMatchRequest(BaseModel):
    """Manual match: link transaction to existing payment or mark as ignored."""

    customer_payment_id: Optional[int] = None
    supplier_payment_id: Optional[int] = None
    ignore: bool = False
    match_note: Optional[str] = Field(default=None, max_length=500)


class AutoMatchResult(BaseModel):
    matched: int
    skipped: int
    errors: int
