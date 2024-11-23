from http import HTTPStatus
import re

from fastapi import HTTPException
import string
import unicodedata
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.crud.brand import brand_crud
from dz_fastapi.models.brand import Brand


async def get_brand_by_id(brand_id: int, session: AsyncSession) -> Brand:
    brand = await brand_crud.get_brand_by_id(brand_id, session)
    if not brand:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail="Brand not found")
    return brand


async def duplicate_brand_name(
        brand_name: str, session: AsyncSession
) -> None:
    brand = await brand_crud.get_brand_by_name(brand_name, session)
    if brand is not None:
        raise HTTPException(
            status_code=HTTPStatus.CONFLICT,
            detail=f"Brand with name '{brand_name}' already exists"
        )


async def brand_exists(
        brand_id: int, session: AsyncSession
) -> Brand:
    brand = await brand_crud.get(session, brand_id)
    if brand is None:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail='Brand not found'
        )
    return brand


async def change_string(old_string: str) -> str:
    '''
    Функция для изменения строки преобразования
    "АВТОЗАПЧАСТЬ ДЛЯ Haval f7" в "Автозапчасть для HAVAL F7"
    '''
    old_string = old_string.capitalize()
    new_string = ''
    for char in old_string:
        if char in string.ascii_letters:
            char = char.upper()
        new_string += char
    return new_string


async def change_brand_name(brand_name: str) -> str:
    '''
    Функция для изменения имени бренда
    "АВТОЗАПЧАСТ�� ДЛЯ Haval f7" в "Автозапчасть для HAVAL F7"
    '''
    # Приведение к верхнему регистру для ASCII символов
    brand_name = ''.join([char.upper() if char in string.ascii_letters else char for char in brand_name])
    # Удаление всех символов, кроме a-z, A-Z, 0-9, пробелов и дефисов
    brand_name = re.sub(r'[^a-zA-Z0-9 -]', '', brand_name)
    # Замена нескольких пробелов или дефисов на один
    brand_name = re.sub(r'[ -]{2,}', '-', brand_name)
    # Удаление начальных и конечных пробелов или дефисов
    brand_name = brand_name.strip(' -')
    return brand_name

async def change_storage_name(storage_name: str) -> str:
    storage_name = ''.join([char.upper() if char in string.ascii_letters else char for char in storage_name])
    storage_name = re.sub(r'[^A-Z0-9 -]', '', storage_name)
    storage_name = re.sub(r'[ -]{2,}', '-', storage_name)
    storage_name = storage_name.strip(' -')
    return storage_name


async def apply_filters(autopart_associations, config):
    # Filter by brands
    if config.brand_filters:
        autopart_associations = [
            assoc for assoc in autopart_associations
            if assoc.autopart.brand.name in config.brand_filters
        ]

    # Filter by price range
    min_price = config.price_filters.get('min_price')
    max_price = config.price_filters.get('max_price')
    if min_price is not None:
        autopart_associations = [
            assoc for assoc in autopart_associations if float(assoc.price) >= min_price
        ]
    if max_price is not None:
        autopart_associations = [
            assoc for assoc in autopart_associations if float(assoc.price) <= max_price
        ]

    # Apply additional filters if any
    # Implement your logic based on config.additional_filters

    return autopart_associations

async def calculate_price(assoc, config):
    # Check if there's an individual markup for this supplier price list
    supplier_pricelist_id = assoc.pricelist_id
    individual_markup = config.individual_markups.get(str(supplier_pricelist_id))

    if individual_markup is not None:
        markup = individual_markup
    else:
        markup = config.general_markup

    new_price = float(assoc.price) * (1 + markup / 100)
    return round(new_price, 2)
