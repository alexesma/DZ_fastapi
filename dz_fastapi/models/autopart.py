import logging
import re
from datetime import datetime, timezone
from enum import StrEnum, unique
from uuid import uuid4

from sqlalchemy import DECIMAL, Boolean, CheckConstraint, Column, DateTime
from sqlalchemy import Enum as SAEnum
from sqlalchemy import (Float, ForeignKey, Index, Integer,
                        PrimaryKeyConstraint, String, Table, Text,
                        UniqueConstraint, event, inspect, select)
from sqlalchemy.orm import relationship

from dz_fastapi.core.base import Base
from dz_fastapi.core.constants import (MAX_LEN_WEBSITE, MAX_LIGHT_BARCODE,
                                       MAX_LIGHT_NAME_LOCATION, MAX_LIGHT_OEM,
                                       MAX_NAME_CATEGORY)

logger = logging.getLogger('dz_fastapi')


@unique
class TYPE_RESTOCK_DECISION_STATUS(StrEnum):
    '''
    Типы статусов для решения о необходимости пополнения конкретной детали
    '''

    NEW = 'New'
    IN_PROGRESS = 'In Progress'
    FULFILLED = 'Fulfilled'
    CANCELLED = 'Cancelled'


@unique
class TYPE_SUPPLIER_DECISION_STATUS(StrEnum):
    '''
    Типы статусов для решения для конкретного поставщика
    '''

    NEW = 'New'
    SEND = 'Send'
    CONFIRMED = 'Confirmed'
    REJECTED = 'Rejected'
    FULFILLED = 'Fulfilled'
    ERROR = 'Error'


@unique
class TYPE_SEND_METHOD(StrEnum):
    '''
    Типы способов отправки заказов поставщику
    '''

    API = 'API'
    MAIL = 'E-mail'


def change_string(old_string: str) -> str:
    '''
    Функция для изменения строки преобразования
    "АВТОЗАПЧАСТЬ ДЛЯ Haval f7" в "Автозапчасть для HAVAL F7"
    '''
    old_string = old_string.capitalize()
    new_string = ''
    for char in old_string:
        if ('A' <= char <= 'Z') or ('a' <= char <= 'z'):
            char = char.upper()
        new_string += char
    return new_string


def preprocess_oem_number(oem_number: str) -> str:
    '''
    Функция для предварительного обработки отправляемого номера запчасти.
    Удаляет все символы, кроме латинских букв и цифр.
    Переводит все символы в верхний регистр.
    Обрабатывает отправляемые номера запчасти таким образом,
    чтобы они были уникальными.
    :param oem_number:
    :return:
    '''
    return re.sub(r'[^a-zA-Z0-9]', '', oem_number).upper()


class AutoPart(Base):
    '''
    Модель Автозапчасть
    '''

    brand_id = Column(Integer, ForeignKey('brand.id'), nullable=False)
    brand = relationship('Brand', back_populates='autoparts')
    oem_number = Column(String(MAX_LIGHT_OEM), nullable=False, index=True)
    name = Column(String(MAX_LIGHT_OEM), nullable=False)
    description = Column(Text, nullable=True)
    width = Column(Float, nullable=True)
    height = Column(Float, nullable=True)
    length = Column(Float, nullable=True)
    weight = Column(Float, nullable=True)
    photos = relationship('Photo', back_populates='autopart')
    purchase_price = Column(DECIMAL(10, 2), default=0)
    retail_price = Column(DECIMAL(10, 2), default=0)
    wholesale_price = Column(DECIMAL(10, 2), default=0)
    multiplicity = Column(Integer, default=1, nullable=True)
    minimum_balance = Column(Integer, default=0)
    min_balance_auto = Column(Boolean, default=False)
    min_balance_user = Column(Boolean, default=False)
    comment = Column(Text, nullable=True, default='')
    barcode = Column(String(MAX_LIGHT_BARCODE), nullable=False, unique=True)
    __table_args__ = (
        UniqueConstraint('brand_id', 'oem_number', name='uq_brand_oem_number'),
        CheckConstraint('width > 0', name='check_width_positive'),
        CheckConstraint('height > 0', name='check_height_positive'),
        CheckConstraint('length > 0', name='check_length_positive'),
        CheckConstraint('weight > 0', name='check_weight_positive'),
        CheckConstraint(
            'purchase_price >= 0', name='check_purchase_price_non_negative'
        ),
        CheckConstraint(
            'retail_price >= 0', name='check_retail_price_non_negative'
        ),
        CheckConstraint(
            'wholesale_price >= 0', name='check_wholesale_price_non_negative'
        ),
    )
    categories = relationship(
        'Category',
        secondary='autopart_category_association',
        back_populates='autoparts',
        lazy='selectin',
    )
    storage_locations = relationship(
        'StorageLocation',
        secondary='autopart_storage_association',
        back_populates='autoparts',
        lazy='selectin',
    )
    price_list_associations = relationship(
        'PriceListAutoPartAssociation',
        back_populates='autopart',
        cascade='all, delete-orphan',
    )
    customer_price_list_associations = relationship(
        'CustomerPriceListAutoPartAssociation',
        back_populates='autopart',
        cascade='all, delete-orphan',
    )
    __mapper_args__ = {'polymorphic_identity': 'autopart'}


@event.listens_for(AutoPart, 'before_insert')
def preprocess_auto_part(mapper, connection, target):
    try:
        from dz_fastapi.models.brand import Brand

        # Преобразовать oem_number в верхний регистр
        # и удалить специальные символы
        target.oem_number = preprocess_oem_number(target.oem_number)

        # Обработка имени
        target.name = change_string(target.name)

        # Обработка описания
        if target.description:
            target.description = change_string(target.description)

        if target.brand_id:
            brand_name_result = connection.execute(
                select(Brand.name).where(Brand.id == target.brand_id)
            ).fetchone()
            if brand_name_result:
                brand_name = brand_name_result[0]
                target.barcode = f'{brand_name}{target.oem_number}'
            else:
                raise ValueError('Brand not found')
        else:
            raise ValueError('Cannot create AutoPart without a brand')
    except Exception as e:
        logger.exception(f"Error in preprocess_auto_part: {e}")
        raise


@event.listens_for(AutoPart, 'before_update')
def preprocess_auto_part_update(mapper, connection, target):
    state = inspect(target)

    if state.attrs.oem_number.history.has_changes():
        target.oem_number = preprocess_oem_number(target.oem_number)

    if state.attrs.name.history.has_changes():
        target.name = change_string(target.name)

    if state.attrs.description.history.has_changes() and target.description:
        target.description = change_string(target.description)

    if (
        state.attrs.brand_id.history.has_changes()
        or state.attrs.oem_number.history.has_changes()
    ):
        if not target.brand:
            raise ValueError(
                'Нельзя изменить автозапчасть без указания бренда.'
            )
        target.barcode = f'{target.brand.name}{target.oem_number}'


class Category(Base):
    '''
    Модель Категория запчасти или детали автомобиля.
    '''

    name = Column(String(MAX_NAME_CATEGORY), nullable=False, unique=True)
    parent_id = Column(Integer, ForeignKey('category.id'), nullable=True)
    children = relationship(
        'Category', back_populates='parent', lazy='selectin'
    )
    parent = relationship(
        'Category',
        remote_side=lambda: [Category.id],
        back_populates='children',
        lazy='selectin',
    )
    comment = Column(Text, nullable=True, default='')
    autoparts = relationship(
        'AutoPart',
        secondary='autopart_category_association',
        lazy='selectin',
        back_populates='categories',
    )


@event.listens_for(Category, 'before_insert')
def preprocess_category(mapper, connection, target):
    '''
    Преобразовать имя категории и удалить специальные символы
    :param mapper:
    :param connection:
    :param target:
    :return:
    '''
    target.name = re.sub(r'[^\w\s\-\*\(\)""]', '', target.name)


class StorageLocation(Base):
    '''
    Модель Складское месторасположение запчасти.
    '''

    name = Column(String(MAX_LIGHT_NAME_LOCATION), nullable=False, unique=True)
    autoparts = relationship(
        'AutoPart',
        secondary='autopart_storage_association',
        back_populates='storage_locations',
        lazy='selectin',
        cascade='all, delete',
    )
    __table_args__ = (
        CheckConstraint(
            "name ~ '^[A-Z0-9 /]+$'", name='latin_characters_only'
        ),
    )


class Photo(Base):
    '''
    Модель Фотография запчасти.
    '''

    url = Column(String(MAX_LEN_WEBSITE), nullable=False, unique=True)
    autopart_id = Column(Integer, ForeignKey('autopart.id'))
    autopart = relationship('AutoPart', back_populates='photos')
    __table_args__ = (
        UniqueConstraint('url', 'autopart_id', name='unique_photo'),
        {"extend_existing": True},
    )


autopart_storage_association = Table(
    'autopart_storage_association',
    Base.metadata,
    Column(
        'autopart_id',
        ForeignKey('autopart.id'),
        nullable=False,
    ),
    Column(
        'storage_location_id',
        ForeignKey('storagelocation.id'),
        nullable=False,
    ),
    UniqueConstraint(
        'autopart_id',
        'storage_location_id',
        name='unique_autopart_storage_location',
    ),
)

autopart_category_association = Table(
    'autopart_category_association',
    Base.metadata,
    Column(
        'autopart_id',
        ForeignKey('autopart.id'),
        nullable=False,
    ),
    Column(
        'category_id',
        ForeignKey('category.id'),
        nullable=False,
    ),
    PrimaryKeyConstraint('autopart_id', 'category_id'),
    UniqueConstraint(
        'autopart_id', 'category_id', name='unique_autopart_category'
    ),
)


class AutoPartPriceHistory(Base):
    '''
    Модель для хранения истории по запчасти.
    '''

    autopart_id = Column(Integer, ForeignKey('autopart.id'), nullable=False)
    provider_id = Column(Integer, ForeignKey('provider.id'), nullable=False)
    pricelist_id = Column(Integer, nullable=False, index=True)

    created_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )
    price = Column(DECIMAL(10, 2), nullable=False)
    quantity = Column(Integer, nullable=False)

    autopart = relationship('AutoPart')
    provider = relationship('Provider')

    __table_args__ = (
        Index(
            'idx_autopart_price_history_autopart_provider_created_at',
            'autopart_id',
            'provider_id',
            'created_at',
        ),
    )


class AutoPartRestockDecision(Base):
    autopart_id = Column(Integer, ForeignKey('autopart.id'), index=True)
    required_quantity = Column(Integer, nullable=False)
    decision_date = Column(DateTime, default=datetime.now)
    status = Column(
        SAEnum(
            TYPE_RESTOCK_DECISION_STATUS,
            values_callable=lambda enum: [e.name for e in enum],
            native_enum=True,
        ),
        default=TYPE_RESTOCK_DECISION_STATUS.NEW,
    )

    autopart = relationship('AutoPart')
    suppliers = relationship(
        'AutoPartRestockDecisionSupplier',
        cascade='all,delete-orphan',
        lazy='selectin',
        back_populates='restock_decision',
    )


class AutoPartRestockDecisionSupplier(Base):
    restock_decision_id = Column(
        Integer, ForeignKey('autopartrestockdecision.id')
    )
    supplier_id = Column(Integer, ForeignKey('provider.id'))
    status = Column(
        SAEnum(
            TYPE_SUPPLIER_DECISION_STATUS,
            values_callable=lambda enum: [e.name for e in enum],
            native_enum=True,
        ),
        default=TYPE_SUPPLIER_DECISION_STATUS.NEW,
    )

    send_method = Column(
        SAEnum(
            TYPE_SEND_METHOD,
            values_callable=lambda enum: [e.name for e in enum],
            native_enum=True,
        ),
        default=TYPE_SEND_METHOD.MAIL,
    )
    send_date = Column(DateTime, nullable=True)
    price = Column(DECIMAL(10, 2))
    quantity = Column(Integer, nullable=True)
    hash_key = Column(String(255), nullable=True, index=True)
    system_hash = Column(String(255), nullable=True, index=True)
    restock_decision = relationship(
        'AutoPartRestockDecision', back_populates='suppliers'
    )
    brand_name = Column(String)
    min_delivery_day = Column(Integer, default=1)
    max_delivery_day = Column(Integer, default=3)
    supplier = relationship('Provider')
    order_items = relationship('OrderItem', back_populates='restock_supplier')
    tracking_uuid = Column(
        String(36), default=lambda: str(uuid4()), unique=True, index=True
    )
