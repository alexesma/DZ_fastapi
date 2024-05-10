# # dragonzap_fastapi/models/partner.py
#
# from sqlalchemy import (
#     Column,
#     String,
#     Text,
#     Enum,
#     Date,
#     Integer,
#     Boolean,
#     ForeignKey,
#     Table,
#     DECIMAL,
#     TIMESTAMP,
#     Index,
#     event
# )
# from sqlalchemy.orm import relationship, validates
# from dz_fastapi.core.constants import MAX_NAME_PARTNER
# import re
# from datetime import datetime, timedelta, timezone
#
# from enum import unique, StrEnum
#
# from dz_fastapi.core.db import Base
#
# DEFAULT_IS_ACTIVE = True
#
#
# @unique
# class TYPE_PRICES(StrEnum):
#     '''
#     Типы цен
#     '''
#     WHOLESALE = 'Wholesale'
#     RETAIL = 'Retail'
#
#
# @unique
# class TYPE_STATUS_ORDER(StrEnum):
#     '''
#     Типы статусов для заказов
#     '''
#     NEW_OREDER = 'New order'
#     ORDERED = 'Ordered'
#     CONFIRMED = 'Confirmed'
#     ARRIVED = 'Arrived'
#     SHIPPED = 'Shipped'
#     REFUSAL = 'Refusal'
#     ERROR = 'Error'
#     REMOVED = 'Removed'
#
#
# @unique
# class TYPE_PAYMENT_STATUS(StrEnum):
#     '''
#     Типы статусов оплаты для клиентских заказов
#     '''
#     PAID = 'Paid'
#     PARTIALLY = 'Partially'
#     NOT_PAID = 'Not paid'
#
#
# def set_date(mapper, connection, target):
#     target.date = datetime.now(timezone.utc).date()
#
#
# class Client(Base):
#     name = Column(String(MAX_NAME_PARTNER), nullable=False, unique=True)
#     type_prices = Column(Enum(TYPE_PRICES), default=TYPE_PRICES.WHOLESALE)
#     email_contact = Column(String(255), unique=True, index=True, nullable=True)
#     description = Column(Text)
#     comment = Column(Text, default='')
#
#     @staticmethod
#     def is_valid_email(email):
#         """Проверка корректности email-адреса."""
#         pattern = r'^[\w\.-]+@[\w\.-]+\.\w+$'
#         return re.match(pattern, email) is not None
#
#     @validates('email_contact')
#     def validate_email_contact(self, key, email):
#         if email and not self.is_valid_email(email):
#             raise ValueError("Invalid email address")
#         return email
#
#
# class Provider(Client):
#     id = Column(
#         Integer,
#         ForeignKey('client.id'),
#         primary_key=True,
#         unique=True
#     )
#     email_incoming_price = Column(
#         String(255),
#         index=True,
#         nullable=True,
#         unique=True
#     )
#     price_lists = relationship(
#         'PriceList',
#         back_populates='provider'
#     )
#
#     @validates('email_incoming_price')
#     def validate_email_incoming_price(self, key, email):
#         if email and not self.is_valid_email(email):
#             raise ValueError("Invalid email address for incoming price")
#         return email
#
#
# class Customer(Client):
#     id = Column(
#         Integer,
#         ForeignKey('client.id'),
#         primary_key=True,
#         unique=True
#     )
#     email_outgoing_price = Column(
#         String(255),
#         index=True,
#         nullable=True,
#         unique=True
#     )
#     customer_price_list = relationship(
#         'CustomerPriceList',
#         back_populates='customer'
#     )
#
#     @validates('email_outgoing_price')
#     def validate_email_outgoing_price(self, key, email):
#         if email and not self.is_valid_email(email):
#             raise ValueError("Invalid email address for outgoing price")
#         return email
#
#
# # Ассоциативная таблица для связи прайс-листа с автозапчастями
# price_list_autopart_association = Table(
#     'price_list_autopart_association',
#     Base.metadata,
#     Column('pricelist_id',ForeignKey('pricelist.id')),
#     Column('autopart_id',ForeignKey('autopart.id')),
#     Column('quantity', Integer, nullable=False),
#     Column('price', DECIMAL(10, 2)),
#     Index('ix_price_list_autopart_id', 'autopart_id', unique=False),
#     Index('ix_price_list_pricelist_id', 'pricelist_id', unique=False)
# )
#
#
# class PriceList(Base):
#     '''
#     Модель Прайс-листа.
#     '''
#     date = Column(Date)
#     provider_id = Column(
#         Integer,
#         ForeignKey('provider.id')
#     )
#     provider = relationship('Provider', back_populates='price_lists')
#     is_active = Column(Boolean, default=DEFAULT_IS_ACTIVE)
#     autoparts = relationship(
#         'AutoPart',
#         secondary='price_list_autopart_association',
#         back_populates='price_lists'
#     )
#
#
# class CustomerPriceList(Base):
#     '''
#     Модель Прайс-листа для клиента.
#     '''
#     date = Column(Date)
#     customer_id = Column(
#         Integer,
#         ForeignKey('customer.id')
#     )
#     customer = relationship('Customer', back_populates='customer_price_lists')
#     autoparts = relationship(
#         'AutoPart',
#         secondary='customer_price_list_autopart_association',
#         back_populates='customer_price_lists'
#     )
#     is_active = Column(Boolean, default=DEFAULT_IS_ACTIVE)
#
#
# event.listen(PriceList, 'before_insert', set_date)
# event.listen(CustomerPriceList, 'before_insert', set_date)
#
# customer_price_list_autopart_association = Table(
#     'customer_price_list_autopart_association',
#     Base.metadata,
#     Column('customerpricelist_id',ForeignKey('customerpricelist.id')),
#     Column('autopart_id',ForeignKey('autopart.id')),
#     Column('quantity', Integer, nullable=False),
#     Column('price', DECIMAL(10, 2)),
#     Index('ix_customer_price_list_autopart_id', 'autopart_id', unique=False),
#     Index('ix_customer_price_list_customerpricelist_id', 'customerpricelist_id', unique=False)
# )
