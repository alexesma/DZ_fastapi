"""
Finance models:
  - InvoiceStatus       — статус счёта на оплату
  - PaymentMethod       — способ оплаты
  - PaymentInvoice      — счёт на оплату (клиенту)
  - CustomerPayment     — поступление оплаты от клиента
  - SupplierPayment     — оплата поставщику
  - BankAccount         — расчётный счёт организации
  - BankStatement       — загруженная выписка банка
  - BankTransaction     — строка выписки (платёжное поручение)
"""

from enum import StrEnum, unique

from sqlalchemy import DECIMAL, Boolean, Column, Date, DateTime
from sqlalchemy import Enum as SAEnum
from sqlalchemy import ForeignKey, Integer, String, Text
from sqlalchemy.orm import relationship

from dz_fastapi.core.db import Base
from dz_fastapi.core.time import now_moscow
from dz_fastapi.models.inventory import SyncStatus


@unique
class InvoiceStatus(StrEnum):
    DRAFT = "draft"  # черновик
    SENT = "sent"  # выставлен клиенту
    PARTIALLY_PAID = "partially_paid"  # частично оплачен
    PAID = "paid"  # полностью оплачен
    CANCELLED = "cancelled"  # аннулирован
    OVERDUE = "overdue"  # просрочен


@unique
class PaymentMethod(StrEnum):
    BANK_TRANSFER = "bank_transfer"  # безналичный расчёт
    CASH = "cash"  # наличные
    CARD = "card"  # карта
    OFFSET = "offset"  # взаимозачёт


class PaymentInvoice(Base):
    """Счёт на оплату клиенту."""

    __tablename__ = "paymentinvoice"

    customer_id = Column(
        Integer,
        ForeignKey("customer.id", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )
    # Привязка к отгрузке — опционально
    shipment_id = Column(
        Integer,
        ForeignKey("shipmentdocument.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    # Привязка к клиентскому заказу — опционально
    customer_order_id = Column(
        Integer,
        ForeignKey("customerorder.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    invoice_number = Column(
        String(50), nullable=False, unique=True, index=True
    )
    invoice_date = Column(Date, nullable=False)
    due_date = Column(Date, nullable=True)  # срок оплаты
    total_amount = Column(DECIMAL(12, 2), nullable=False, default=0)
    paid_amount = Column(DECIMAL(12, 2), nullable=False, default=0)
    status = Column(
        SAEnum(
            InvoiceStatus,
            name="invoicestatus",
            values_callable=lambda enum: [e.value for e in enum],
        ),
        nullable=False,
        default=InvoiceStatus.DRAFT,
    )
    notes = Column(Text, nullable=True)
    # Для синхронизации с 1С
    external_id = Column(String(100), nullable=True, index=True)
    sync_status = Column(
        SAEnum(
            SyncStatus,
            name="syncstatus_finance_invoice",
            values_callable=lambda enum: [e.value for e in enum],
        ),
        nullable=False,
        default=SyncStatus.PENDING,
    )
    created_at = Column(
        DateTime(timezone=True), default=now_moscow, nullable=False
    )
    updated_at = Column(
        DateTime(timezone=True),
        default=now_moscow,
        onupdate=now_moscow,
    )

    # Relationships
    customer = relationship(
        "Customer", back_populates="payment_invoices", lazy="joined"
    )
    shipment = relationship(
        "ShipmentDocument", lazy="joined", foreign_keys=[shipment_id]
    )
    customer_order = relationship(
        "CustomerOrder", lazy="joined", foreign_keys=[customer_order_id]
    )
    payments = relationship(
        "CustomerPayment",
        back_populates="invoice",
        cascade="all, delete-orphan",
        lazy="selectin",
    )
    items = relationship(
        "PaymentInvoiceItem",
        back_populates="invoice",
        cascade="all, delete-orphan",
        order_by="PaymentInvoiceItem.position",
        lazy="selectin",
    )


class PaymentInvoiceItem(Base):
    """Строка (позиция) счёта на оплату."""

    __tablename__ = "paymentinvoiceitem"

    invoice_id = Column(
        Integer,
        ForeignKey("paymentinvoice.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    position = Column(
        Integer, nullable=False, default=1
    )  # порядковый номер строки
    # Ссылка на запчасть (опционально — можно указать свободным текстом)
    autopart_id = Column(
        Integer,
        ForeignKey("autopart.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    name = Column(
        String(500), nullable=False
    )  # наименование (заполняется из autopart или вручную)
    oem_number = Column(String(100), nullable=True)  # OEM номер
    quantity = Column(DECIMAL(10, 3), nullable=False, default=1)
    unit_price = Column(
        DECIMAL(12, 2), nullable=False, default=0
    )  # цена за единицу без НДС
    vat_rate = Column(
        DECIMAL(5, 2), nullable=False, default=20
    )  # ставка НДС, %
    total = Column(DECIMAL(12, 2), nullable=False, default=0)  # итого с НДС

    invoice = relationship(
        "PaymentInvoice", back_populates="items", lazy="joined"
    )
    autopart = relationship("AutoPart", lazy="joined")


class CustomerPayment(Base):
    """Поступление оплаты от клиента."""

    __tablename__ = "customerpayment"

    customer_id = Column(
        Integer,
        ForeignKey("customer.id", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )
    # Может быть авансом — без привязки к счёту
    invoice_id = Column(
        Integer,
        ForeignKey("paymentinvoice.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    amount = Column(DECIMAL(12, 2), nullable=False)
    payment_date = Column(Date, nullable=False)
    payment_method = Column(
        SAEnum(
            PaymentMethod,
            name="paymentmethod_customer",
            values_callable=lambda enum: [e.value for e in enum],
        ),
        nullable=False,
        default=PaymentMethod.BANK_TRANSFER,
    )
    # Номер платёжного поручения / ссылка
    reference = Column(String(255), nullable=True)
    notes = Column(Text, nullable=True)
    external_id = Column(String(100), nullable=True, index=True)
    sync_status = Column(
        SAEnum(
            SyncStatus,
            name="syncstatus_finance_cpayment",
            values_callable=lambda enum: [e.value for e in enum],
        ),
        nullable=False,
        default=SyncStatus.PENDING,
    )
    created_at = Column(
        DateTime(timezone=True), default=now_moscow, nullable=False
    )
    updated_at = Column(
        DateTime(timezone=True),
        default=now_moscow,
        onupdate=now_moscow,
    )

    customer = relationship(
        "Customer", back_populates="customer_payments", lazy="joined"
    )
    invoice = relationship(
        "PaymentInvoice", back_populates="payments", lazy="joined"
    )


class SupplierPayment(Base):
    """Оплата поставщику."""

    __tablename__ = "supplierpayment"

    provider_id = Column(
        Integer,
        ForeignKey("provider.id", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )
    # Привязка к заказу поставщику — опционально
    supplier_order_id = Column(
        Integer,
        ForeignKey("supplierorder.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    amount = Column(DECIMAL(12, 2), nullable=False)
    payment_date = Column(Date, nullable=False)
    payment_method = Column(
        SAEnum(
            PaymentMethod,
            name="paymentmethod_supplier",
            values_callable=lambda enum: [e.value for e in enum],
        ),
        nullable=False,
        default=PaymentMethod.BANK_TRANSFER,
    )
    reference = Column(String(255), nullable=True)
    notes = Column(Text, nullable=True)
    external_id = Column(String(100), nullable=True, index=True)
    sync_status = Column(
        SAEnum(
            SyncStatus,
            name="syncstatus_finance_spayment",
            values_callable=lambda enum: [e.value for e in enum],
        ),
        nullable=False,
        default=SyncStatus.PENDING,
    )
    created_at = Column(
        DateTime(timezone=True), default=now_moscow, nullable=False
    )
    updated_at = Column(
        DateTime(timezone=True),
        default=now_moscow,
        onupdate=now_moscow,
    )

    provider = relationship(
        "Provider", back_populates="supplier_payments", lazy="joined"
    )
    supplier_order = relationship(
        "SupplierOrder", lazy="joined", foreign_keys=[supplier_order_id]
    )


# ═══════════════════════════════════════════════════════════════════════════════
# Банковские выписки
# ═══════════════════════════════════════════════════════════════════════════════


@unique
class BankStatementFormat(StrEnum):
    TOCHKA_CSV = "tochka_csv"  # Точка Банк, CSV UTF-8
    C1_EXCHANGE = "1c_exchange"  # 1CClientBankExchange .txt
    ALFABANK_CSV = "alfabank_csv"  # Альфа-Банк, CSV cp1251
    SBERBANK_CSV = "sberbank_csv"  # Сбербанк CSV
    UNKNOWN = "unknown"


@unique
class BankTxnDirection(StrEnum):
    INCOMING = "incoming"  # Зачисление (Входящий)
    OUTGOING = "outgoing"  # Списание (Исходящий)


@unique
class BankTxnStatus(StrEnum):
    UNMATCHED = "unmatched"  # Не разнесена
    MATCHED = "matched"  # Разнесена (привязана к оплате)
    IGNORED = "ignored"  # Пропущена вручную (техническая, внутренняя)


class BankAccount(Base):
    """Расчётный счёт организации."""

    __tablename__ = "bankaccount"

    account_number = Column(
        String(20), nullable=False, unique=True, index=True
    )
    bank_name = Column(String(255), nullable=False)
    bik = Column(String(9), nullable=True)
    corr_account = Column(String(20), nullable=True)
    currency = Column(String(3), nullable=False, default="RUB")
    description = Column(String(255), nullable=True)
    is_active = Column(Boolean, nullable=False, default=True)
    created_at = Column(DateTime(timezone=True), default=now_moscow)

    statements = relationship(
        "BankStatement",
        back_populates="bank_account",
        cascade="all, delete-orphan",
        lazy="noload",
    )


class BankStatement(Base):
    """Загруженная выписка банка."""

    __tablename__ = "bankstatement"

    bank_account_id = Column(
        Integer, ForeignKey("bankaccount.id"), nullable=True, index=True
    )
    period_from = Column(Date, nullable=False)
    period_to = Column(Date, nullable=False)
    opening_balance = Column(DECIMAL(15, 2), nullable=True)
    closing_balance = Column(DECIMAL(15, 2), nullable=True)
    total_incoming = Column(DECIMAL(15, 2), nullable=True)
    total_outgoing = Column(DECIMAL(15, 2), nullable=True)
    format = Column(
        SAEnum(
            BankStatementFormat,
            name="bankstatementformat",
            values_callable=lambda enum: [e.value for e in enum],
        ),
        nullable=False,
        default=BankStatementFormat.UNKNOWN,
    )
    filename = Column(String(255), nullable=True)
    uploaded_at = Column(
        DateTime(timezone=True), default=now_moscow, nullable=False
    )
    # Counts after parsing
    txn_count = Column(Integer, nullable=False, default=0)
    matched_count = Column(Integer, nullable=False, default=0)

    bank_account = relationship(
        "BankAccount", back_populates="statements", lazy="joined"
    )
    transactions = relationship(
        "BankTransaction",
        back_populates="statement",
        cascade="all, delete-orphan",
        lazy="noload",
    )


class BankTransaction(Base):
    """Строка банковской выписки — одно платёжное поручение."""

    __tablename__ = "banktransaction"

    statement_id = Column(
        Integer,
        ForeignKey("bankstatement.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    doc_number = Column(String(50), nullable=True)
    doc_date = Column(Date, nullable=True)  # дата документа
    value_date = Column(Date, nullable=False)  # дата проводки / зачисления

    direction = Column(
        SAEnum(
            BankTxnDirection,
            name="banktxndirection",
            values_callable=lambda enum: [e.value for e in enum],
        ),
        nullable=False,
    )
    amount = Column(DECIMAL(15, 2), nullable=False)
    vat_amount = Column(
        DECIMAL(15, 2), nullable=True
    )  # сумма НДС из назначения
    currency = Column(String(3), nullable=False, default="RUB")

    # Counterparty (payer or receiver depending on direction)
    counterparty_name = Column(String(500), nullable=True)
    counterparty_inn = Column(String(12), nullable=True, index=True)
    counterparty_kpp = Column(String(9), nullable=True)
    counterparty_account = Column(String(20), nullable=True)
    counterparty_bank = Column(String(255), nullable=True)
    counterparty_bik = Column(String(9), nullable=True)

    purpose = Column(Text, nullable=True)  # назначение платежа
    balance_after = Column(DECIMAL(15, 2), nullable=True)

    status = Column(
        SAEnum(
            BankTxnStatus,
            name="banktxnstatus",
            values_callable=lambda enum: [e.value for e in enum],
        ),
        nullable=False,
        default=BankTxnStatus.UNMATCHED,
        index=True,
    )

    # Привязка к оплате после разноски
    customer_payment_id = Column(
        Integer,
        ForeignKey("customerpayment.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    supplier_payment_id = Column(
        Integer,
        ForeignKey("supplierpayment.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    match_note = Column(String(500), nullable=True)  # комментарий при разноске

    statement = relationship(
        "BankStatement", back_populates="transactions", lazy="noload"
    )
    customer_payment = relationship(
        "CustomerPayment", lazy="joined", foreign_keys=[customer_payment_id]
    )
    supplier_payment = relationship(
        "SupplierPayment", lazy="joined", foreign_keys=[supplier_payment_id]
    )
