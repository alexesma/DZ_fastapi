import re
import unicodedata
from typing import Optional, Union, Type
from decimal import Decimal

from sqlalchemy import (
    Column,
    String,
    Text,
    Integer,
    Boolean,
    Float,
    ForeignKey,
    Table,
    DECIMAL,
    event,
    UniqueConstraint,
    CheckConstraint,
    PrimaryKeyConstraint,
)
from sqlalchemy.orm import relationship

from dz_fastapi.core.constants import (
    MAX_LIGHT_OEM,
    MAX_NAME_CATEGORY,
    MAX_LIGHT_BARCODE,
    MAX_LIGHT_NAME_LOCATION,
    MAX_LEN_WEBSITE
)
from dz_fastapi.core.db import Base



def change_string(old_string: str) -> str:
    '''
    Функция для изменения строки преобразования
    "АВТОЗАПЧАСТЬ ДЛЯ Haval f7" в "Автозапчасть для HAVAL F7"
    '''
    old_string = old_string.capitalize()
    new_string = ''
    for char in old_string:
        if unicodedata.category(char).startswith('L'):
            char = char.upper()
        new_string += char
    return new_string


def preprocess_oem_number(oem_number):
    '''
    Функция для предварительного обработки отправляемого номера запчасти.
    Удаляет все символы, кроме латинских букв и цифр. Переводит все символы в верхний регистр.
    Обрабатывает отправляемые номера запчасти таким образом, чтобы они были уникальными.
    :param oem_number:
    :return:
    '''
    return re.sub(r'[^a-zA-Z0-9]', '', oem_number).upper()


class AutoPart(Base):
    '''
    Модель Автозапчасть
    '''
    brand_id = Column(
        Integer,
        ForeignKey('brand.id'),
        nullable=False
    )
    brand = relationship('Brand')
    oem_number = Column(
        String(MAX_LIGHT_OEM),
        nullable=False,
        index=True
    )
    name = Column(String(MAX_LIGHT_OEM), nullable=False)
    description = Column(Text, nullable=True)
    width = Column(Float, nullable=True)
    height = Column(Float, nullable=True)
    length = Column(Float, nullable=True)
    weight = Column(Float, nullable=True)
    photos = relationship('Photo', back_populates='autopart')
    purchase_price = Column(DECIMAL(10, 2), default=0)
    retail_price = Column(DECIMAL(10, 2),  default=0)
    wholesale_price = Column(DECIMAL(10, 2),  default=0)
    multiplicity = Column(Integer, default=1,  nullable=True)
    minimum_balance = Column(Integer, default=0)
    min_balance_auto = Column(Boolean, default=False)
    min_balance_user = Column(Boolean, default=False)
    comment = Column(Text, nullable=True, default='')
    barcode = Column(String(MAX_LIGHT_BARCODE), nullable=False, unique=True)
    __table_args__ = (
        UniqueConstraint(
            'brand_id',
            'oem_number',
            name='uq_brand_oem_number'
        ),
        CheckConstraint('width > 0', name='check_width_positive'),
        CheckConstraint('height > 0', name='check_height_positive'),
        CheckConstraint('length > 0', name='check_length_positive'),
        CheckConstraint('weight > 0', name='check_weight_positive'),
        CheckConstraint('purchase_price >= 0', name='check_purchase_price_non_negative'),
        CheckConstraint('retail_price >= 0', name='check_retail_price_non_negative'),
        CheckConstraint('wholesale_price >= 0', name='check_wholesale_price_non_negative'),
    )
    __mapper_args__ = {'polymorphic_identity': 'autopart'}


@event.listens_for(AutoPart, 'before_insert')
def preprocess_auto_part(mapper, connection, target):
    # Преобразовать oem_number в верхний регистр и удалить специальные символы
    target.oem_number = preprocess_oem_number(target.oem_number)

    # Обработка имени
    target.name = change_string(target.name)

    # Обработка описания
    if target.description:
        target.description = change_string(target.description)

    if target.brand:
        target.barcode = f"{target.brand.name}{target.oem_number}"
    else:
        raise ValueError("Cannot create AutoPart without a brand")


@event.listens_for(AutoPart, 'before_update')
def preprocess_auto_part_update(mapper, connection, target):
    target.oem_number = preprocess_oem_number(target.oem_number)
    target.name = change_string(target.name)
    if target.description:
        target.description = change_string(target.description)
    if 'brand_id' in target.dirty or 'oem_number' in target.dirty:
        if not target.brand:
            raise Exception("Нельзя изменить автозапчасть без указания бренда.")
        target.barcode = f"{target.brand.name}{target.oem_number}"


# class Category(Base):
#     '''
#     Модель Категория запчасти или детали автомобиля.
#     '''
#     name = Column(
#         String(MAX_NAME_CATEGORY),
#         nullable=False,
#         unique=True
#     )
#     parent_id = Column(Integer, ForeignKey('category.id'), nullable=True)
#     children = relationship(
#         'Category',
#         backref='parent',
#         remote_side=[id]
#     )
#     comment = Column(Text, nullable=True, default='')
#     autoparts = relationship(
#         'AutoPart',
#         secondary='autopart_category_association',
#         back_populates='categories'
#     )
#     __table_args__ = (
#         UniqueConstraint(
#             'parent_id',
#             name='unique_parent_id'
#         ),
#     )
#
#
# @event.listens_for(Category, 'before_insert')
# def preprocess_category(mapper, connection, target):
#     # Преобразовать имя категории и удалить специальные символы
#     target.name = re.sub(r'[^\w-]', '', target.name).capitalize()
#
#
# class StorageLocation(Base):
#     '''
#     Модель Складское месторасположение запчасти.
#     '''
#     name = Column(
#         String(MAX_LIGHT_NAME_LOCATION),
#         nullable=False,
#         unique=True
#     )
#     autoparts = relationship(
#         'AutoPart',
#         secondary='autopart_storage_association',
#         back_populates='storage_locations',
#         cascade='all, delete'
#     )
#     __table_args__ = (
#         CheckConstraint(
#             "name ~ '^[A-Z0-9]+$'",
#             name='latin_characters_only'
#         ),
#     )


class Photo(Base):
    '''
    Модель Фотография запчасти.
    '''
    url = Column(String(MAX_LEN_WEBSITE), nullable=False,  unique=True)
    autopart_id = Column(
        Integer,
        ForeignKey('autopart.id')
    )
    autopart = relationship('AutoPart', back_populates='photos')
    __table_args__ = (
        UniqueConstraint(
            'url',
            'autopart_id',
            name='unique_photo'
        ),
        {"extend_existing": True},
    )


# autopart_storage_association = Table(
#     'autopart_storage_association',
#     Base.metadata,
#     Column(
#         'autopart_id',
#         ForeignKey('autopart.id'),
#         nullable=True,
#     ),
#     Column(
#         'storage_location_id',
#         ForeignKey('storagelocation.id'),
#         nullable=True,
#     ),
#     UniqueConstraint(
#         'autopart_id',
#         'storage_location_id',
#         name='unique_autopart_storage_location'
#     )
# )
#
# autopart_category_association = Table(
#     'autopart_category_association',
#     Base.metadata,
#     Column(
#         'autopart_id',
#         ForeignKey('autopart.id'),
#         nullable=True,
#     ),
#     Column(
#         'category_id',
#         ForeignKey('category.id'),
#         nullable=True,
#     ),
#     PrimaryKeyConstraint('autopart_id', 'category_id'),
#     UniqueConstraint(
#         'autopart_id',
#         'category_id',
#         name='unique_autopart_category'
#     )
# )
