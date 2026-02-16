# dragonzap_fastapi/models/partner.py

from datetime import date, datetime, timezone
from enum import StrEnum, unique
from uuid import uuid4

from email_validator import EmailNotValidError, validate_email
from sqlalchemy import DECIMAL, JSON, Boolean, Column, Date, DateTime
from sqlalchemy import Enum as SAEnum
from sqlalchemy import Float, ForeignKey, Index, Integer, String, Text, event
from sqlalchemy.orm import relationship, validates

from dz_fastapi.core.db import Base

DEFAULT_IS_ACTIVE = True
MAX_NAME_PARTNER = 256


@unique
class TYPE_PRICES(StrEnum):
    '''
    Типы цен
    '''

    WHOLESALE = 'Wholesale'
    RETAIL = 'Retail'


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


@unique
class TYPE_PAYMENT_STATUS(StrEnum):
    '''
    Типы статусов оплаты для клиентских заказов
    '''

    PAID = 'Paid'
    PARTIALLY = 'Partially'
    NOT_PAID = 'Not paid'


def set_date(mapper, connection, target):
    target.date = datetime.now(timezone.utc).date()


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
    provider_last_uid = relationship(
        'ProviderLastEmailUID', back_populates='provider', uselist=False
    )
    is_virtual = Column(Boolean, default=False)
    is_own_price = Column(Boolean, default=False)

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
    start_row = Column(Integer, nullable=False)
    oem_col = Column(Integer, nullable=False)
    name_col = Column(Integer, nullable=True)
    brand_col = Column(Integer, nullable=True)
    qty_col = Column(Integer, nullable=False)
    price_col = Column(Integer, nullable=False)
    name_price = Column(String, nullable=True)
    name_mail = Column(String, nullable=True)
    file_url = Column(String, nullable=True)
    min_delivery_day = Column(Integer, nullable=True, default=1)
    max_delivery_day = Column(Integer, nullable=True, default=2)
    provider = relationship('Provider', back_populates='pricelist_configs')
    price_lists = relationship(
        'PriceList', back_populates='config', lazy='selectin'
    )


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
    schedule_days = Column(JSON, default=[])
    schedule_times = Column(JSON, default=[])
    emails = Column(JSON, default=[])
    is_active = Column(Boolean, default=True)
    last_sent_at = Column(DateTime(timezone=True), nullable=True)

    customer = relationship('Customer', back_populates='pricelist_configs')
    sources = relationship(
        'CustomerPriceListSource',
        back_populates='config',
        cascade='all, delete-orphan',
        lazy='selectin',
    )


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
    updated_at = Column(
        DateTime(timezone=True), default=datetime.now, onupdate=datetime.now
    )
    provider = relationship('Provider', back_populates='provider_last_uid')


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
    created_at = Column(DateTime(timezone=True), default=datetime.now)
    updated_at = Column(
        DateTime(timezone=True), default=datetime.now, onupdate=datetime.now
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
    order_items = relationship('OrderItem', back_populates='order')


class OrderItem(Base):
    order_id = Column(Integer, ForeignKey('order.id'))
    autopart_id = Column(Integer, ForeignKey('autopart.id'))
    quantity = Column(Integer, nullable=False)
    price = Column(DECIMAL(10, 2))
    created_at = Column(DateTime(timezone=True), default=datetime.now)
    updated_at = Column(
        DateTime(timezone=True), default=datetime.now, onupdate=datetime.now
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
    autopart = relationship('AutoPart')
    restock_supplier_id = Column(
        Integer,
        ForeignKey('autopartrestockdecisionsupplier.id'),
        nullable=True,
    )
    restock_supplier = relationship(
        'AutoPartRestockDecisionSupplier', back_populates='order_items'
    )
