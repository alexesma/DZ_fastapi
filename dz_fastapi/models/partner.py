# dragonzap_fastapi/models/partner.py

from sqlalchemy import (
    Column,
    String,
    Text,
    Enum,
    Date,
    Integer,
    Boolean,
    ForeignKey,
    DECIMAL,
    Index,
    event,
    Float,
    JSON,
    DateTime
)
from datetime import date
from email_validator import validate_email, EmailNotValidError
from sqlalchemy.orm import relationship, validates
from dz_fastapi.core.constants import MAX_NAME_PARTNER
from datetime import datetime, timedelta, timezone

from enum import unique, StrEnum

from dz_fastapi.core.db import Base

DEFAULT_IS_ACTIVE = True


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
    type_prices = Column(Enum(TYPE_PRICES), default=TYPE_PRICES.WHOLESALE)
    email_contact = Column(
        String(255),
        unique=True,
        index=True,
        nullable=True
    )
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
        Integer,
        ForeignKey('client.id'),
        primary_key=True,
        unique=True
    )
    email_incoming_price = Column(
        String(255),
        index=True,
        nullable=True,
        unique=True
    )
    price_lists = relationship(
        'PriceList',
        back_populates='provider'
    )
    pricelist_config = relationship(
        'ProviderPriceListConfig',
        uselist=False, back_populates='provider'
    )
    provider_last_uid = relationship(
        'ProviderLastEmailUID',
        back_populates='provider',
        uselist=False
    )


    @validates('email_incoming_price')
    def validate_email_incoming_price(self, key, email):
        if email and not self.is_valid_email(email):
            raise ValueError('Invalid email address for incoming price')
        return email


class Customer(Client):
    id = Column(
        Integer,
        ForeignKey('client.id'),
        primary_key=True,
        unique=True
    )
    email_outgoing_price = Column(
        String(255),
        index=True,
        nullable=True,
        unique=True
    )
    customer_price_lists = relationship(
        'CustomerPriceList',
        back_populates='customer'
    )
    pricelist_configs = relationship(
        'CustomerPriceListConfig',
        back_populates='customer',
        cascade='all, delete-orphan'
    )

    @validates('email_outgoing_price')
    def validate_email_outgoing_price(self, key, email):
        if email and not self.is_valid_email(email):
            raise ValueError("Invalid email address for outgoing price")
        return email


class PriceListAutoPartAssociation(Base):
    id = None

    pricelist_id = Column(
        Integer,
        ForeignKey('pricelist.id'),
        primary_key=True
    )
    autopart_id = Column(
        Integer,
        ForeignKey('autopart.id'),
        primary_key=True
    )
    quantity = Column(Integer, nullable=False)
    price = Column(DECIMAL(10, 2), nullable=False)

    pricelist = relationship(
        'PriceList',
        back_populates='autopart_associations'
    )
    autopart = relationship(
        'AutoPart',
        back_populates='price_list_associations'
    )

    __table_args__ = (
        Index(
            'ix_price_list_autopart_id',
            'autopart_id',
            unique=False
        ),
        Index(
            'ix_price_list_pricelist_id',
            'pricelist_id',
            unique=False
        ),
    )


class PriceList(Base):
    '''
    Модель Прайс-листа.
    '''
    date = Column(Date)
    provider_id = Column(
        Integer,
        ForeignKey('provider.id')
    )
    provider = relationship('Provider', back_populates='price_lists')
    is_active = Column(Boolean, default=DEFAULT_IS_ACTIVE)
    autopart_associations = relationship(
        'PriceListAutoPartAssociation',
        back_populates='pricelist',
        cascade='all, delete-orphan'
    )


class CustomerPriceListAutoPartAssociation(Base):
    id = None
    customerpricelist_id = Column(
        Integer,
        ForeignKey('customerpricelist.id'),
        primary_key=True
    )
    autopart_id = Column(
        Integer,
        ForeignKey('autopart.id'),
        primary_key=True
    )
    quantity = Column(Integer, nullable=False)
    price = Column(DECIMAL(10, 2))

    customerpricelist = relationship(
        "CustomerPriceList",
        back_populates="autopart_associations"
    )
    autopart = relationship(
        "AutoPart",
        back_populates="customer_price_list_associations"
    )

    __table_args__ = (
        Index('ix_customer_price_list_autopart_id',
              'autopart_id',
              unique=False
              ),
        Index(
            'ix_customer_price_list_customerpricelist_id',
            'customerpricelist_id',
            unique=False
        ),
    )


class CustomerPriceList(Base):
    '''
    Модель Прайс-листа для клиента.
    '''
    date = Column(Date, default=date.today)
    customer_id = Column(
        Integer,
        ForeignKey('customer.id')
    )
    customer = relationship(
        'Customer',
        back_populates='customer_price_lists'
    )
    autopart_associations = relationship(
        'CustomerPriceListAutoPartAssociation',
        back_populates='customerpricelist',
        cascade='all, delete-orphan',
        lazy='selectin'
    )
    is_active = Column(Boolean, default=DEFAULT_IS_ACTIVE)


event.listen(PriceList, 'before_insert', set_date)
event.listen(CustomerPriceList, 'before_insert', set_date)


class ProviderPriceListConfig(Base):
    provider_id = Column(Integer, ForeignKey('provider.id'), unique=True)
    start_row = Column(Integer, nullable=False)
    oem_col = Column(Integer, nullable=False)
    name_col = Column(Integer, nullable=True)
    brand_col = Column(Integer, nullable=True)
    qty_col = Column(Integer, nullable=False)
    price_col = Column(Integer, nullable=False)
    name_price = Column(String, nullable=True)
    name_mail = Column(String, nullable=True)

    provider = relationship('Provider', back_populates='pricelist_config')


class CustomerPriceListConfig(Base):
    id = Column(Integer, primary_key=True)

    customer_id = Column(
        Integer,
        ForeignKey('customer.id'),
        nullable=False
    )

    name = Column(String(255), nullable=False, unique=True)
    general_markup = Column(Float, default=0.0)  # Общая наценка
    own_price_list_markup = Column(Float, default=0.0)  # Наценка на наш прайс-лист
    third_party_markup = Column(Float, default=0.0)  # Наценка на стороние прайс-листы общая
    individual_markups = Column(JSON, default={})  # Индивидуальная наценка (provider_id: markup)
    brand_filters = Column(JSON, default=[])  # Список брендов для фильтра(include/exclude)
    category_filter = Column(JSON, default=[])  # Список категорий для фильтра(include/exclude)
    price_intervals = Column(JSON, default=[])  # Price intervals with coefficients
    position_filters = Column(JSON, default=[])  # List of position IDs to include/exclude
    supplier_quantity_filters = Column(JSON, default=[])  # Supplier-specific quantity filters
    additional_filters = Column(JSON, default={})  # Other custom filters

    customer = relationship(
        'Customer',
        back_populates='pricelist_configs'
    )


class ProviderLastEmailUID(Base):
    id = Column(Integer, primary_key=True, autoincrement=True)
    provider_id = Column(Integer, ForeignKey('provider.id'), primary_key=True, unique=True)
    last_uid = Column(Integer, nullable=False, default=0)
    updated_at = Column(
        DateTime(timezone=True),
        default=datetime.now,
        onupdate=datetime.now
    )
    provider = relationship('Provider', back_populates='provider_last_uid')
