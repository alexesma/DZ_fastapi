from http import HTTPStatus

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
            detail="Name's brand already exists"
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
