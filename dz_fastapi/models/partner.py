# dragonzap_fastapi/models/partner.py

from datetime import date
from enum import StrEnum, unique
from typing import Optional
from uuid import uuid4

from email_validator import EmailNotValidError, validate_email
from sqlalchemy import DECIMAL, JSON, Boolean, Column, Date, DateTime
from sqlalchemy import Enum as SAEnum
from sqlalchemy import (Float, ForeignKey, Index, Integer, String, Text,
                        UniqueConstraint, event)
from sqlalchemy.orm import relationship, validates

from dz_fastapi.core.db import Base
from dz_fastapi.core.time import now_moscow

DEFAULT_IS_ACTIVE = True
MAX_NAME_PARTNER = 256


@unique
class TYPE_PRICES(StrEnum):
    '''
    Типы цен
    '''

    WHOLESALE = 'Wholesale'
    RETAIL = 'Retail'
    CASH = 'Cash'


@unique
class PROVIDER_DELIVERY_METHOD(StrEnum):
    DELIVERED = 'Delivered'
    SELF_PICKUP = 'Self pickup'
    COURIER_FOOT = 'Courier foot'
    COURIER_CAR = 'Courier car'


@unique
class TYPE_STATUS_ORDER(StrEnum):
    '''
    Типы статусов для заказов
    '''

    NEW_OREDER = 'New order'
    ORDERED = 'Ordered'
    CONFIRMED = 'Confirmed'
    ARRIVED = 'Arrived'
    SHIPPED = 'Shipped'
    REFUSAL = 'Refusal'
    ERROR = 'Error'
    REMOVED = 'Removed'
    PROCESSING = 'Processing'
    TRANSIT = 'In transit'
    ACCEPTED = 'Accepted'
    RETURNED = 'Returned'

    @property
    def label(self) -> str:
        return {
            TYPE_STATUS_ORDER.NEW_OREDER: 'Новый заказ',
            TYPE_STATUS_ORDER.ORDERED: 'В заказе',
            TYPE_STATUS_ORDER.CONFIRMED: 'Подтверждён',
            TYPE_STATUS_ORDER.ARRIVED: 'Прибыл на склад',
            TYPE_STATUS_ORDER.SHIPPED: 'Выдан клиенту',
            TYPE_STATUS_ORDER.REFUSAL: 'Отказ поставщика',
            TYPE_STATUS_ORDER.REMOVED: 'Удалён',
            TYPE_STATUS_ORDER.ERROR: 'Ошибка',
            TYPE_STATUS_ORDER.PROCESSING: 'Обрабатывается',
            TYPE_STATUS_ORDER.TRANSIT: 'В пути',
            TYPE_STATUS_ORDER.ACCEPTED: 'Ожидает приёмки',
            TYPE_STATUS_ORDER.RETURNED: 'Возврат',
        }[self]


@unique
class TYPE_ORDER_ITEM_STATUS(StrEnum):
    NEW = 'NEW'
    SENT = 'SENT'
    CONFIRMED = 'CONFIRMED'
    IN_PROGRESS = 'IN_PROGRESS'
    DELIVERED = 'DELIVERED'
    CANCELLED = 'CANCELLED'
    FAILED = 'FAILED'
    ERROR = 'ERROR'

    @property
    def label(self) -> str:
        return {
            TYPE_ORDER_ITEM_STATUS.NEW: 'Новый',
            TYPE_ORDER_ITEM_STATUS.SENT: 'Отправлен',
            TYPE_ORDER_ITEM_STATUS.CONFIRMED: 'Подтвержден',
            TYPE_ORDER_ITEM_STATUS.IN_PROGRESS: 'В работе',
            TYPE_ORDER_ITEM_STATUS.DELIVERED: 'Получено',
            TYPE_ORDER_ITEM_STATUS.CANCELLED: 'Отменен',
            TYPE_ORDER_ITEM_STATUS.FAILED: 'Ошибка',
            TYPE_ORDER_ITEM_STATUS.ERROR: 'Ошибка',
        }[self]


@unique
class TYPE_PAYMENT_STATUS(StrEnum):
    '''
    Типы статусов оплаты для клиентских заказов
    '''

    PAID = 'Paid'
    PARTIALLY = 'Partially'
    NOT_PAID = 'Not paid'


@unique
class CUSTOMER_ORDER_STATUS(StrEnum):
    NEW = 'NEW'
    PROCESSED = 'PROCESSED'
    SENT = 'SENT'
    ERROR = 'ERROR'


@unique
class CUSTOMER_ORDER_ITEM_STATUS(StrEnum):
    NEW = 'NEW'
    OWN_STOCK = 'OWN_STOCK'
    SUPPLIER = 'SUPPLIER'
    REJECTED = 'REJECTED'


@unique
class SUPPLIER_ORDER_STATUS(StrEnum):
    NEW = 'NEW'
    SCHEDULED = 'SCHEDULED'
    SENT = 'SENT'
    ERROR = 'ERROR'


@unique
class ORDER_TRACKING_SOURCE(StrEnum):
    DRAGONZAP_SEARCH = 'DRAGONZAP_SEARCH'
    SEARCH_OFFERS = 'SEARCH_OFFERS'
    CUSTOMER_ORDER = 'CUSTOMER_ORDER'


@unique
class STOCK_ORDER_STATUS(StrEnum):
    NEW = 'NEW'
    COMPLETED = 'COMPLETED'
    ERROR = 'ERROR'


@unique
class CUSTOMER_ORDER_SHIP_MODE(StrEnum):
    REPLACE_QTY = 'REPLACE_QTY'
    WRITE_SHIP_QTY = 'WRITE_SHIP_QTY'
    WRITE_REJECT_QTY = 'WRITE_REJECT_QTY'


def set_date(mapper, connection, target):
    if not getattr(target, 'date', None):
        target.date = now_moscow().date()


class Client(Base):
    name = Column(String(MAX_NAME_PARTNER), nullable=False, unique=True)
    type_prices = Column(
        SAEnum(
            TYPE_PRICES,
            values_callable=lambda enum: [e.name for e in enum],
            native_enum=True,
        ),
        default=TYPE_PRICES.WHOLESALE,
    )
    email_contact = Column(String(255), unique=True, index=True, nullable=True)
    description = Column(Text, nullable=True)
    comment = Column(Text, default='')

    @staticmethod
    def is_valid_email(email):
        try:
            validate_email(email)
            return True
        except EmailNotValidError:
            return False

    @validates('email_contact')
    def validate_email_contact(self, key, email):
        if email and not self.is_valid_email(email):
            raise ValueError('Invalid email address')
        return email


class Provider(Client):
    id = Column(
        Integer, ForeignKey('client.id'), primary_key=True, unique=True
    )
    email_incoming_price = Column(
        String(255), index=True, nullable=True, unique=True
    )
    price_lists = relationship('PriceList', back_populates='provider')
    pricelist_configs = relationship(
        'ProviderPriceListConfig',
        back_populates='provider',
        cascade='all, delete-orphan',
        lazy='selectin',
        single_parent=True,
    )
    supplier_response_configs = relationship(
        'SupplierResponseConfig',
        back_populates='provider',
        cascade='all, delete-orphan',
        lazy='selectin',
    )
    provider_last_uid = relationship(
        'ProviderLastEmailUID', back_populates='provider', uselist=False
    )
    is_virtual = Column(Boolean, default=False)
    is_own_price = Column(Boolean, default=False)
    order_schedule_days = Column(JSON, default=[])
    order_schedule_times = Column(JSON, default=[])
    order_schedule_enabled = Column(Boolean, default=False)
    supplier_response_allow_shipping_docs = Column(
        Boolean,
        default=True,
        nullable=False,
    )
    supplier_response_allow_response_files = Column(
        Boolean,
        default=True,
        nullable=False,
    )
    supplier_response_allow_text_status = Column(
        Boolean,
        default=True,
        nullable=False,
    )
    supplier_response_filename_pattern = Column(String(255), nullable=True)
    supplier_shipping_doc_filename_pattern = Column(
        String(255),
        nullable=True,
    )
    supplier_response_start_row = Column(Integer, default=1, nullable=False)
    supplier_response_oem_col = Column(Integer, nullable=True)
    supplier_response_brand_col = Column(Integer, nullable=True)
    supplier_response_qty_col = Column(Integer, nullable=True)
    supplier_response_price_col = Column(Integer, nullable=True)
    supplier_response_comment_col = Column(Integer, nullable=True)
    supplier_response_status_col = Column(Integer, nullable=True)
    default_delivery_method = Column(
        SAEnum(
            PROVIDER_DELIVERY_METHOD,
            values_callable=lambda enum: [e.name for e in enum],
            native_enum=True,
        ),
        default=PROVIDER_DELIVERY_METHOD.DELIVERED,
    )

    @validates('email_incoming_price')
    def validate_email_incoming_price(self, key, email):
        if email and not self.is_valid_email(email):
            raise ValueError('Invalid email address for incoming price')
        return email


class Customer(Client):
    id = Column(
        Integer, ForeignKey('client.id'), primary_key=True, unique=True
    )
    email_outgoing_price = Column(
        String(255), index=True, nullable=True, unique=True
    )
    customer_price_lists = relationship(
        'CustomerPriceList', back_populates='customer'
    )
    pricelist_configs = relationship(
        'CustomerPriceListConfig',
        back_populates='customer',
        cascade='all, delete-orphan',
    )
    order_configs = relationship(
        'CustomerOrderConfig',
        back_populates='customer',
        cascade='all, delete-orphan',
    )
    customer_orders = relationship(
        'CustomerOrder',
        back_populates='customer',
        cascade='all, delete-orphan',
    )

    @validates('email_outgoing_price')
    def validate_email_outgoing_price(self, key, email):
        if email and not self.is_valid_email(email):
            raise ValueError("Invalid email address for outgoing price")
        return email


class PriceListAutoPartAssociation(Base):
    id = None

    pricelist_id = Column(
        Integer, ForeignKey('pricelist.id'), primary_key=True
    )
    autopart_id = Column(Integer, ForeignKey('autopart.id'), primary_key=True)
    quantity = Column(Integer, nullable=False)
    price = Column(DECIMAL(10, 2), nullable=False)
    multiplicity = Column(Integer, nullable=False, default=1)

    pricelist = relationship(
        'PriceList', back_populates='autopart_associations'
    )
    autopart = relationship(
        'AutoPart', back_populates='price_list_associations'
    )

    __table_args__ = (
        Index('ix_price_list_autopart_id', 'autopart_id', unique=False),
        Index('ix_price_list_pricelist_id', 'pricelist_id', unique=False),
    )


class PriceList(Base):
    '''
    Модель Прайс-листа.
    '''

    date = Column(Date)
    provider_id = Column(Integer, ForeignKey('provider.id'))
    provider = relationship('Provider', back_populates='price_lists')
    provider_config_id = Column(
        Integer, ForeignKey('providerpricelistconfig.id'), nullable=True
    )
    config = relationship('ProviderPriceListConfig', lazy='selectin')
    is_active = Column(Boolean, default=DEFAULT_IS_ACTIVE)
    autopart_associations = relationship(
        'PriceListAutoPartAssociation',
        back_populates='pricelist',
        cascade='all, delete-orphan',
    )
    missing_brand_stats = relationship(
        'PriceListMissingBrand',
        back_populates='pricelist',
        cascade='all, delete-orphan',
    )


class PriceListMissingBrand(Base):
    pricelist_id = Column(
        Integer,
        ForeignKey('pricelist.id', ondelete='CASCADE'),
        nullable=False,
    )
    provider_config_id = Column(
        Integer,
        ForeignKey('providerpricelistconfig.id', ondelete='CASCADE'),
        nullable=False,
    )
    brand_name = Column(String(255), nullable=False)
    positions_count = Column(Integer, nullable=False, default=0)
    created_at = Column(DateTime(timezone=True), default=now_moscow)

    pricelist = relationship('PriceList', back_populates='missing_brand_stats')
    provider_config = relationship(
        'ProviderPriceListConfig', back_populates='missing_brand_stats'
    )

    __table_args__ = (
        Index(
            'ix_pricelistmissingbrand_provider_config_id',
            'provider_config_id',
        ),
        Index(
            'ix_pricelistmissingbrand_pricelist_id',
            'pricelist_id',
        ),
        Index(
            'ix_pricelistmissingbrand_brand_name',
            'brand_name',
        ),
    )


class CustomerPriceListAutoPartAssociation(Base):
    id = None
    customerpricelist_id = Column(
        Integer, ForeignKey('customerpricelist.id'), primary_key=True
    )
    autopart_id = Column(Integer, ForeignKey('autopart.id'), primary_key=True)
    quantity = Column(Integer, nullable=False)
    price = Column(DECIMAL(10, 2))

    customerpricelist = relationship(
        "CustomerPriceList", back_populates="autopart_associations"
    )
    autopart = relationship(
        "AutoPart", back_populates="customer_price_list_associations"
    )

    __table_args__ = (
        Index(
            'ix_customer_price_list_autopart_id', 'autopart_id', unique=False
        ),
        Index(
            'ix_customer_price_list_customerpricelist_id',
            'customerpricelist_id',
            unique=False,
        ),
    )


class CustomerPriceList(Base):
    '''
    Модель Прайс-листа для клиента.
    '''

    date = Column(Date, default=date.today)
    customer_id = Column(Integer, ForeignKey('customer.id'))
    customer = relationship('Customer', back_populates='customer_price_lists')
    autopart_associations = relationship(
        'CustomerPriceListAutoPartAssociation',
        back_populates='customerpricelist',
        cascade='all, delete-orphan',
        lazy='selectin',
    )
    is_active = Column(Boolean, default=DEFAULT_IS_ACTIVE)


event.listen(PriceList, 'before_insert', set_date)
event.listen(CustomerPriceList, 'before_insert', set_date)


class ProviderPriceListConfig(Base):
    provider_id = Column(Integer, ForeignKey('provider.id'))
    incoming_email_account_id = Column(
        Integer, ForeignKey('emailaccount.id'), nullable=True
    )
    start_row = Column(Integer, nullable=False)
    oem_col = Column(Integer, nullable=False)
    name_col = Column(Integer, nullable=True)
    brand_col = Column(Integer, nullable=True)
    multiplicity_col = Column(Integer, nullable=True)
    qty_col = Column(Integer, nullable=False)
    price_col = Column(Integer, nullable=False)
    name_price = Column(String, nullable=True)
    name_mail = Column(String, nullable=True)
    file_url = Column(String, nullable=True)
    min_price = Column(Float, nullable=True)
    max_price = Column(Float, nullable=True)
    min_quantity = Column(Integer, nullable=True)
    max_quantity = Column(Integer, nullable=True)
    exclude_positions = Column(JSON, default=[])
    max_days_without_update = Column(Integer, nullable=True, default=3)
    last_stale_alert_at = Column(DateTime(timezone=True), nullable=True)
    min_delivery_day = Column(Integer, nullable=True, default=1)
    max_delivery_day = Column(Integer, nullable=True, default=2)
    is_active = Column(Boolean, default=True, nullable=False)
    provider = relationship('Provider', back_populates='pricelist_configs')
    incoming_email_account = relationship('EmailAccount', lazy='selectin')
    price_lists = relationship(
        'PriceList', back_populates='config', lazy='selectin'
    )
    missing_brand_stats = relationship(
        'PriceListMissingBrand',
        back_populates='provider_config',
        cascade='all, delete-orphan',
    )
    last_email_uid = relationship(
        'ProviderConfigLastEmailUID',
        back_populates='provider_config',
        uselist=False,
        cascade='all, delete-orphan',
    )


class SupplierResponseConfig(Base):
    provider_id = Column(
        Integer,
        ForeignKey('provider.id', ondelete='CASCADE'),
        nullable=False,
    )
    name = Column(String(255), nullable=False)
    is_active = Column(Boolean, default=True, nullable=False)
    inbox_email_account_id = Column(
        Integer,
        ForeignKey('emailaccount.id'),
        nullable=True,
    )
    sender_emails = Column(JSON, default=[])
    response_type = Column(String(16), default='file', nullable=False)
    process_shipping_docs = Column(Boolean, default=True, nullable=False)

    file_format = Column(String(16), nullable=True)
    file_payload_type = Column(
        String(16), default='response', nullable=False
    )
    filename_pattern = Column(String(255), nullable=True)
    shipping_doc_filename_pattern = Column(String(255), nullable=True)
    start_row = Column(Integer, default=1, nullable=False)
    oem_col = Column(Integer, nullable=True)
    brand_col = Column(Integer, nullable=True)
    qty_col = Column(Integer, nullable=True)
    status_col = Column(Integer, nullable=True)
    comment_col = Column(Integer, nullable=True)
    price_col = Column(Integer, nullable=True)
    document_number_col = Column(Integer, nullable=True)
    document_date_col = Column(Integer, nullable=True)
    gtd_col = Column(Integer, nullable=True)
    country_code_col = Column(Integer, nullable=True)
    country_name_col = Column(Integer, nullable=True)
    total_price_with_vat_col = Column(Integer, nullable=True)

    confirm_keywords = Column(JSON, default=[])
    reject_keywords = Column(JSON, default=[])
    value_after_article_type = Column(
        String(16), default='both', nullable=False
    )

    provider = relationship(
        'Provider', back_populates='supplier_response_configs'
    )
    inbox_email_account = relationship('EmailAccount', lazy='selectin')

    @property
    def inbox_email_account_name(self) -> Optional[str]:
        if self.inbox_email_account is None:
            return None
        return self.inbox_email_account.name

    @property
    def inbox_email_account_email(self) -> Optional[str]:
        if self.inbox_email_account is None:
            return None
        return self.inbox_email_account.email


class CustomerPriceListConfig(Base):
    id = Column(Integer, primary_key=True)

    customer_id = Column(Integer, ForeignKey('customer.id'), nullable=False)

    name = Column(String(255), nullable=False, unique=True)
    general_markup = Column(Float, default=0.0)  # Общая наценка
    own_price_list_markup = Column(
        Float, default=0.0
    )  # Наценка на наш прайс-лист
    third_party_markup = Column(
        Float, default=0.0
    )  # Наценка на стороние прайс-листы общая
    individual_markups = Column(
        JSON, default={}
    )  # Индивидуальная наценка (provider_id: markup)
    brand_filters = Column(
        JSON, default=[]
    )  # Список брендов для фильтра(include/exclude)
    category_filter = Column(
        JSON, default=[]
    )  # Список категорий для фильтра(include/exclude)
    price_intervals = Column(
        JSON, default=[]
    )  # Price intervals with coefficients
    position_filters = Column(
        JSON, default=[]
    )  # List of position IDs to include/exclude
    supplier_quantity_filters = Column(
        JSON, default=[]
    )  # Supplier-specific quantity filters
    additional_filters = Column(JSON, default={})  # Other custom filters
    default_filters = Column(
        JSON, default={}
    )  # Общие фильтры по умолчанию
    own_filters = Column(
        JSON, default={}
    )  # Фильтры для нашего прайса
    other_filters = Column(
        JSON, default={}
    )  # Фильтры для остальных поставщиков
    supplier_filters = Column(
        JSON, default={}
    )  # Индивидуальные фильтры для поставщиков
    schedule_days = Column(JSON, default=[])
    schedule_times = Column(JSON, default=[])
    emails = Column(JSON, default=[])
    export_file_name = Column(String(255), nullable=True)
    export_file_format = Column(
        String(16), nullable=False, default='xlsx', server_default='xlsx'
    )
    export_file_extension = Column(String(16), nullable=True)
    outgoing_email_account_id = Column(
        Integer, ForeignKey('emailaccount.id'), nullable=True
    )
    is_active = Column(Boolean, default=True)
    last_sent_at = Column(DateTime(timezone=True), nullable=True)

    customer = relationship('Customer', back_populates='pricelist_configs')
    outgoing_email_account = relationship('EmailAccount', lazy='selectin')
    sources = relationship(
        'CustomerPriceListSource',
        back_populates='config',
        cascade='all, delete-orphan',
        lazy='selectin',
    )


class CustomerOrderConfig(Base):
    customer_id = Column(Integer, ForeignKey('customer.id'), nullable=False)

    order_email = Column(String(255), nullable=True, index=True)
    order_emails = Column(JSON, default=[])
    order_subject_pattern = Column(String(255), nullable=True)
    order_filename_pattern = Column(String(255), nullable=True)
    order_reply_emails = Column(JSON, default=[])
    email_account_id = Column(
        Integer, ForeignKey('emailaccount.id'), nullable=True
    )

    pricelist_config_id = Column(
        Integer, ForeignKey('customerpricelistconfig.id'), nullable=True
    )

    order_start_row = Column(Integer, default=1)
    order_number_column = Column(Integer, nullable=True)
    order_number_row = Column(Integer, nullable=True)
    order_date_column = Column(Integer, nullable=True)
    order_date_row = Column(Integer, nullable=True)
    order_number_regex_subject = Column(String(255), nullable=True)
    order_number_regex_filename = Column(String(255), nullable=True)
    order_number_regex_body = Column(String(255), nullable=True)
    order_number_prefix = Column(String(255), nullable=True)
    order_number_suffix = Column(String(255), nullable=True)
    order_number_source = Column(String(32), nullable=True)

    oem_col = Column(Integer, nullable=False)
    brand_col = Column(Integer, nullable=False)
    name_col = Column(Integer, nullable=True)
    qty_col = Column(Integer, nullable=False)
    price_col = Column(Integer, nullable=True)
    ship_qty_col = Column(Integer, nullable=True)
    ship_price_col = Column(Integer, nullable=True)
    reject_qty_col = Column(Integer, nullable=True)
    ship_mode = Column(
        SAEnum(
            CUSTOMER_ORDER_SHIP_MODE,
            values_callable=lambda enum: [e.name for e in enum],
            native_enum=True,
            name='customerordershipmode',
        ),
        default=CUSTOMER_ORDER_SHIP_MODE.REPLACE_QTY,
    )

    price_tolerance_pct = Column(Float, default=2.0)
    price_warning_pct = Column(Float, default=5.0)

    is_active = Column(Boolean, default=True)
    last_uid = Column(Integer, default=0)
    folder_last_uids = Column(JSON, default=dict)

    customer = relationship('Customer', back_populates='order_configs')
    email_account = relationship('EmailAccount')


class CustomerOrder(Base):
    customer_id = Column(Integer, ForeignKey('customer.id'), nullable=False)
    order_config_id = Column(
        Integer, ForeignKey('customerorderconfig.id'), nullable=True
    )
    status = Column(
        SAEnum(
            CUSTOMER_ORDER_STATUS,
            values_callable=lambda enum: [e.name for e in enum],
            native_enum=True,
            name='customerorderstatus',
        ),
        default=CUSTOMER_ORDER_STATUS.NEW,
    )
    received_at = Column(DateTime(timezone=True), default=now_moscow)
    processed_at = Column(DateTime(timezone=True), nullable=True)

    source_email = Column(String(255), nullable=True)
    source_uid = Column(Integer, nullable=True)
    source_subject = Column(String(255), nullable=True)
    source_filename = Column(String(255), nullable=True)
    file_hash = Column(String(64), nullable=True, index=True)

    order_number = Column(String(255), nullable=True)
    order_date = Column(Date, nullable=True)

    response_file_path = Column(String(255), nullable=True)
    response_file_name = Column(String(255), nullable=True)
    error_details = Column(String(500), nullable=True)

    customer = relationship('Customer', back_populates='customer_orders')
    order_config = relationship('CustomerOrderConfig')
    items = relationship(
        'CustomerOrderItem',
        back_populates='order',
        cascade='all, delete-orphan',
    )


class CustomerOrderItem(Base):
    order_id = Column(Integer, ForeignKey('customerorder.id'), nullable=False)
    row_index = Column(Integer, nullable=True)
    oem = Column(String(255), nullable=False)
    brand = Column(String(255), nullable=False)
    name = Column(String(255), nullable=True)
    requested_qty = Column(Integer, nullable=False)
    requested_price = Column(DECIMAL(10, 2), nullable=True)
    ship_qty = Column(Integer, nullable=True)
    reject_qty = Column(Integer, nullable=True)
    status = Column(
        SAEnum(
            CUSTOMER_ORDER_ITEM_STATUS,
            values_callable=lambda enum: [e.name for e in enum],
            native_enum=True,
            name='customerorderitemstatus',
        ),
        default=CUSTOMER_ORDER_ITEM_STATUS.NEW,
    )
    supplier_id = Column(Integer, ForeignKey('provider.id'), nullable=True)
    autopart_id = Column(Integer, ForeignKey('autopart.id'), nullable=True)
    matched_price = Column(DECIMAL(10, 2), nullable=True)
    price_diff_pct = Column(Float, nullable=True)
    reject_reason_code = Column(String(64), nullable=True)
    reject_reason_text = Column(String(500), nullable=True)

    order = relationship('CustomerOrder', back_populates='items')
    supplier = relationship('Provider')


class SupplierOrder(Base):
    provider_id = Column(Integer, ForeignKey('provider.id'), nullable=False)
    source_type = Column(
        String(32),
        nullable=False,
        default=ORDER_TRACKING_SOURCE.CUSTOMER_ORDER.value,
    )
    created_by_user_id = Column(
        Integer, ForeignKey('app_user.id'), nullable=True
    )
    status = Column(
        SAEnum(
            SUPPLIER_ORDER_STATUS,
            values_callable=lambda enum: [e.name for e in enum],
            native_enum=True,
            name='supplierorderstatus',
        ),
        default=SUPPLIER_ORDER_STATUS.NEW,
    )
    created_at = Column(DateTime(timezone=True), default=now_moscow)
    scheduled_at = Column(DateTime(timezone=True), nullable=True)
    sent_at = Column(DateTime(timezone=True), nullable=True)
    response_status_raw = Column(String(255), nullable=True)
    response_status_normalized = Column(String(255), nullable=True, index=True)
    response_status_synced_at = Column(DateTime(timezone=True), nullable=True)

    provider = relationship('Provider')
    created_by_user = relationship('User', foreign_keys=[created_by_user_id])
    items = relationship(
        'SupplierOrderItem',
        back_populates='supplier_order',
        cascade='all, delete-orphan',
    )
    messages = relationship(
        'SupplierOrderMessage',
        back_populates='supplier_order',
        cascade='all, delete-orphan',
    )
    receipts = relationship(
        'SupplierReceipt',
        back_populates='supplier_order',
    )


class SupplierOrderItem(Base):
    supplier_order_id = Column(
        Integer, ForeignKey('supplierorder.id'), nullable=False
    )
    customer_order_item_id = Column(
        Integer, ForeignKey('customerorderitem.id'), nullable=True
    )
    autopart_id = Column(Integer, ForeignKey('autopart.id'), nullable=True)
    oem_number = Column(String(120), nullable=True, index=True)
    brand_name = Column(String(120), nullable=True)
    autopart_name = Column(String(512), nullable=True)
    quantity = Column(Integer, nullable=False)
    price = Column(DECIMAL(10, 2), nullable=True)
    min_delivery_day = Column(Integer, nullable=True)
    max_delivery_day = Column(Integer, nullable=True)
    confirmed_quantity = Column(Integer, nullable=True)
    response_price = Column(DECIMAL(10, 2), nullable=True)
    response_comment = Column(String(500), nullable=True)
    response_status_raw = Column(String(255), nullable=True)
    response_status_normalized = Column(String(255), nullable=True, index=True)
    response_status_synced_at = Column(DateTime(timezone=True), nullable=True)
    received_quantity = Column(Integer, nullable=True)
    received_at = Column(DateTime(timezone=True), nullable=True)

    supplier_order = relationship('SupplierOrder', back_populates='items')
    customer_order_item = relationship('CustomerOrderItem')
    autopart = relationship('AutoPart')
    receipt_items = relationship(
        'SupplierReceiptItem',
        back_populates='supplier_order_item',
    )


class SupplierOrderMessage(Base):
    supplier_order_id = Column(
        Integer, ForeignKey('supplierorder.id'), nullable=True, index=True
    )
    provider_id = Column(Integer, ForeignKey('provider.id'), nullable=False)
    message_type = Column(String(32), nullable=False, default='UNKNOWN')
    subject = Column(String(500), nullable=True)
    sender_email = Column(String(255), nullable=True)
    received_at = Column(DateTime(timezone=True), nullable=False)
    body_preview = Column(Text, nullable=True)
    raw_status = Column(String(255), nullable=True)
    normalized_status = Column(String(255), nullable=True, index=True)
    parse_confidence = Column(Float, nullable=True)
    source_uid = Column(String(128), nullable=True, index=True)
    source_message_id = Column(String(255), nullable=True, index=True)
    response_config_id = Column(
        Integer,
        ForeignKey('supplierresponseconfig.id'),
        nullable=True,
        index=True,
    )
    import_error_details = Column(String(500), nullable=True)
    mapping_id = Column(
        Integer,
        ForeignKey('external_status_mapping.id'),
        nullable=True,
        index=True,
    )

    supplier_order = relationship(
        'SupplierOrder',
        back_populates='messages',
    )
    provider = relationship('Provider')
    response_config = relationship('SupplierResponseConfig')
    mapping = relationship('ExternalStatusMapping')
    attachments = relationship(
        'SupplierOrderAttachment',
        back_populates='message',
        cascade='all, delete-orphan',
    )
    receipts = relationship('SupplierReceipt', back_populates='source_message')


class SupplierOrderAttachment(Base):
    message_id = Column(
        Integer,
        ForeignKey('supplierordermessage.id', ondelete='CASCADE'),
        nullable=False,
        index=True,
    )
    filename = Column(String(255), nullable=False)
    mime_type = Column(String(255), nullable=True)
    file_path = Column(String(1024), nullable=False)
    sha256 = Column(String(64), nullable=True, index=True)
    parsed_kind = Column(String(64), nullable=True)

    message = relationship(
        'SupplierOrderMessage',
        back_populates='attachments',
    )


class SupplierReceipt(Base):
    provider_id = Column(Integer, ForeignKey('provider.id'), nullable=False)
    supplier_order_id = Column(
        Integer, ForeignKey('supplierorder.id'), nullable=True, index=True
    )
    source_message_id = Column(
        Integer,
        ForeignKey('supplierordermessage.id'),
        nullable=True,
        index=True,
    )
    document_number = Column(String(120), nullable=True, index=True)
    document_date = Column(Date, nullable=True, index=True)
    created_by_user_id = Column(
        Integer, ForeignKey('app_user.id'), nullable=True
    )
    created_at = Column(DateTime(timezone=True), default=now_moscow)
    posted_at = Column(DateTime(timezone=True), nullable=True)
    comment = Column(Text, nullable=True)

    provider = relationship('Provider')
    supplier_order = relationship('SupplierOrder', back_populates='receipts')
    source_message = relationship(
        'SupplierOrderMessage',
        back_populates='receipts',
    )
    created_by_user = relationship('User', foreign_keys=[created_by_user_id])
    items = relationship(
        'SupplierReceiptItem',
        back_populates='receipt',
        cascade='all, delete-orphan',
    )


class SupplierReceiptItem(Base):
    receipt_id = Column(
        Integer,
        ForeignKey('supplierreceipt.id', ondelete='CASCADE'),
        nullable=False,
        index=True,
    )
    supplier_order_id = Column(
        Integer, ForeignKey('supplierorder.id'), nullable=True, index=True
    )
    supplier_order_item_id = Column(
        Integer,
        ForeignKey('supplierorderitem.id'),
        nullable=True,
        index=True,
    )
    customer_order_item_id = Column(
        Integer,
        ForeignKey('customerorderitem.id'),
        nullable=True,
        index=True,
    )
    autopart_id = Column(Integer, ForeignKey('autopart.id'), nullable=True)
    oem_number = Column(String(120), nullable=True, index=True)
    brand_name = Column(String(120), nullable=True)
    autopart_name = Column(String(512), nullable=True)
    ordered_quantity = Column(Integer, nullable=True)
    confirmed_quantity = Column(Integer, nullable=True)
    received_quantity = Column(Integer, nullable=False, default=0)
    price = Column(DECIMAL(10, 2), nullable=True)
    total_price_with_vat = Column(DECIMAL(12, 2), nullable=True)
    gtd_code = Column(String(64), nullable=True)
    country_code = Column(String(16), nullable=True)
    country_name = Column(String(120), nullable=True)
    comment = Column(String(500), nullable=True)

    receipt = relationship('SupplierReceipt', back_populates='items')
    supplier_order = relationship('SupplierOrder')
    supplier_order_item = relationship(
        'SupplierOrderItem',
        back_populates='receipt_items',
    )
    customer_order_item = relationship('CustomerOrderItem')
    autopart = relationship('AutoPart')


class StockOrder(Base):
    customer_id = Column(Integer, ForeignKey('customer.id'), nullable=True)
    status = Column(
        SAEnum(
            STOCK_ORDER_STATUS,
            values_callable=lambda enum: [e.name for e in enum],
            native_enum=True,
            name='stockorderstatus',
        ),
        default=STOCK_ORDER_STATUS.NEW,
    )
    created_at = Column(DateTime(timezone=True), default=now_moscow)

    customer = relationship('Customer')
    items = relationship(
        'StockOrderItem',
        back_populates='stock_order',
        cascade='all, delete-orphan',
    )


class StockOrderItem(Base):
    stock_order_id = Column(Integer, ForeignKey('stockorder.id'))
    customer_order_item_id = Column(
        Integer, ForeignKey('customerorderitem.id'), nullable=True
    )
    autopart_id = Column(Integer, ForeignKey('autopart.id'), nullable=True)
    quantity = Column(Integer, nullable=False)
    picked_quantity = Column(Integer, nullable=False, default=0)
    picked_at = Column(DateTime(timezone=True), nullable=True)
    picked_by_user_id = Column(
        Integer, ForeignKey('app_user.id'), nullable=True
    )
    pick_comment = Column(String(500), nullable=True)
    pick_last_scan_code = Column(String(255), nullable=True)

    stock_order = relationship('StockOrder', back_populates='items')
    customer_order_item = relationship('CustomerOrderItem')
    autopart = relationship('AutoPart')
    picked_by_user = relationship('User', foreign_keys=[picked_by_user_id])


class CustomerPriceListSource(Base):
    customer_config_id = Column(
        Integer,
        ForeignKey('customerpricelistconfig.id', ondelete='CASCADE'),
        nullable=False,
        index=True,
    )
    provider_config_id = Column(
        Integer, ForeignKey('providerpricelistconfig.id'), nullable=False
    )
    enabled = Column(Boolean, default=True)
    markup = Column(Float, default=1.0)
    brand_filters = Column(JSON, default={})
    position_filters = Column(JSON, default={})
    min_price = Column(DECIMAL(10, 2), nullable=True)
    max_price = Column(DECIMAL(10, 2), nullable=True)
    min_quantity = Column(Integer, nullable=True)
    max_quantity = Column(Integer, nullable=True)
    additional_filters = Column(JSON, default={})

    config = relationship('CustomerPriceListConfig', back_populates='sources')
    provider_config = relationship(
        'ProviderPriceListConfig',
        lazy='selectin',
    )

    __table_args__ = (
        UniqueConstraint(
            'customer_config_id',
            'provider_config_id',
            name='uq_customer_pricelist_source_config_provider',
        ),
        Index(
            'ix_customer_pricelist_source_config',
            'customer_config_id',
            unique=False,
        ),
        Index(
            'ix_customer_pricelist_source_provider_config',
            'provider_config_id',
            unique=False,
        ),
    )


class ProviderLastEmailUID(Base):
    id = Column(Integer, primary_key=True, autoincrement=True)
    provider_id = Column(
        Integer, ForeignKey('provider.id'), primary_key=True, unique=True
    )
    last_uid = Column(Integer, nullable=False, default=0)
    folder_last_uids = Column(JSON, default=dict)
    updated_at = Column(
        DateTime(timezone=True), default=now_moscow, onupdate=now_moscow
    )
    provider = relationship('Provider', back_populates='provider_last_uid')


class ProviderConfigLastEmailUID(Base):
    id = Column(Integer, primary_key=True, autoincrement=True)
    provider_config_id = Column(
        Integer,
        ForeignKey('providerpricelistconfig.id', ondelete='CASCADE'),
        nullable=False,
        unique=True,
    )
    last_uid = Column(Integer, nullable=False, default=0)
    folder_last_uids = Column(JSON, default=dict)
    updated_at = Column(
        DateTime(timezone=True), default=now_moscow, onupdate=now_moscow
    )
    provider_config = relationship(
        'ProviderPriceListConfig', back_populates='last_email_uid'
    )


class ProviderAbbreviation(Base):
    abbreviation = Column(String(20), unique=True, nullable=False)
    provider_id = Column(Integer, ForeignKey('provider.id'), nullable=False)
    provider = relationship('Provider', backref='abbreviations')


class Order(Base):
    order_number = Column(
        String(36), default=lambda: str(uuid4()), unique=True
    )
    provider_id = Column(Integer, ForeignKey('provider.id'), nullable=False)
    customer_id = Column(Integer, ForeignKey('customer.id'), nullable=False)
    source_type = Column(
        String(32),
        nullable=False,
        default=ORDER_TRACKING_SOURCE.DRAGONZAP_SEARCH.value,
    )
    created_by_user_id = Column(
        Integer, ForeignKey('app_user.id'), nullable=True
    )
    created_at = Column(DateTime(timezone=True), default=now_moscow)
    updated_at = Column(
        DateTime(timezone=True), default=now_moscow, onupdate=now_moscow
    )
    status = Column(
        SAEnum(
            TYPE_STATUS_ORDER,
            values_callable=lambda enum: [e.name for e in enum],
            native_enum=True,
        ),
        default=TYPE_STATUS_ORDER.NEW_OREDER,
    )
    comment = Column(Text, nullable=True)
    provider = relationship('Provider', backref='orders')
    customer = relationship('Customer', backref='orders')
    created_by_user = relationship('User', foreign_keys=[created_by_user_id])
    order_items = relationship('OrderItem', back_populates='order')


class OrderItem(Base):
    order_id = Column(Integer, ForeignKey('order.id'))
    autopart_id = Column(Integer, ForeignKey('autopart.id'))
    oem_number = Column(String(120), nullable=True, index=True)
    brand_name = Column(String(120), nullable=True)
    autopart_name = Column(String(512), nullable=True)
    quantity = Column(Integer, nullable=False)
    price = Column(DECIMAL(10, 2))
    min_delivery_day = Column(Integer, nullable=True)
    max_delivery_day = Column(Integer, nullable=True)
    received_quantity = Column(Integer, nullable=True)
    received_at = Column(DateTime(timezone=True), nullable=True)
    created_at = Column(DateTime(timezone=True), default=now_moscow)
    updated_at = Column(
        DateTime(timezone=True), default=now_moscow, onupdate=now_moscow
    )
    tracking_uuid = Column(
        String(36), default=lambda: str(uuid4()), unique=True, index=True
    )
    status = Column(
        SAEnum(
            TYPE_ORDER_ITEM_STATUS,
            values_callable=lambda enum: [e.name for e in enum],
            native_enum=True,
        ),
        default=TYPE_ORDER_ITEM_STATUS.NEW,
    )
    comments = Column(Text, nullable=True)
    order = relationship('Order', back_populates='order_items')
    hash_key = Column(String(255), nullable=True, index=True)
    system_hash = Column(String(255), nullable=True, index=True)
    external_status_source = Column(String(64), nullable=True, index=True)
    external_status_raw = Column(Text, nullable=True)
    external_status_normalized = Column(String(255), nullable=True, index=True)
    external_status_synced_at = Column(DateTime(timezone=True), nullable=True)
    external_status_mapping_id = Column(
        Integer,
        ForeignKey('external_status_mapping.id'),
        nullable=True,
        index=True,
    )
    autopart = relationship('AutoPart')
    restock_supplier_id = Column(
        Integer,
        ForeignKey('autopartrestockdecisionsupplier.id'),
        nullable=True,
    )
    restock_supplier = relationship(
        'AutoPartRestockDecisionSupplier', back_populates='order_items'
    )
    external_status_mapping = relationship(
        'ExternalStatusMapping', back_populates='order_items'
    )
