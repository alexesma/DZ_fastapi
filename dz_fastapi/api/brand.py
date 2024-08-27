import traceback
from typing import Optional, List

from sqlalchemy import delete
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
from sqlalchemy.future import select

from fastapi import APIRouter, HTTPException, UploadFile, File, Depends, Body
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from dz_fastapi.core.db import get_async_session
from dz_fastapi.crud.brand import brand_crud
from dz_fastapi.core.constants import UPLOAD_DIR
from dz_fastapi.api.validators import duplicate_brand_name, brand_exists, change_string
from dz_fastapi.models.brand import Brand, brand_synonyms
from dz_fastapi.schemas.brand import BrandCreate, BrandResponse, BrandCreateInDB, BrandUpdate, SynonymCreate
import os
from pathlib import Path

import logging

logger = logging.getLogger('dz_fastapi')

router = APIRouter()
UPLOAD_DIR = Path(UPLOAD_DIR)


@router.get(
    '/brand',
    response_model=list[BrandCreateInDB],
    tags=['brand'],
    summary='Список брендов',
    response_model_exclude_none=True
)
async def get_brand(session: AsyncSession = Depends(get_async_session)):
    brands = await brand_crud.get_multi_with_synonyms(session)
    for brand in brands:
        brand.synonyms = await brand_crud.get_all_synonyms_bi_directional(brand, session)
    return brands


@router.post(
    '/upload-logo',
    tags=['brand'],
    summary='Upload brand logo'
)
async def upload_logo(
    logo: UploadFile = File(...)
):
    logo_filename = f"{logo.filename}"
    logo_path = UPLOAD_DIR / logo_filename
    os.makedirs(UPLOAD_DIR, exist_ok=True)
    with open(logo_path, "wb") as f:
        f.write(logo.file.read())
    return {"logo_path": os.path.abspath(logo_path)}


@router.post(
    '/brand',
    response_model=BrandCreateInDB,
    tags=['brand'],
    summary='Создание бренда',
    response_model_exclude_none=True
)
async def create_brand(
        brand: BrandCreate,
        session: AsyncSession = Depends(get_async_session)
):
    try:
        logger.debug('Начало создания бренда api')
        brand.name = await change_string(brand.name)
        logger.debug(f'Изменённое имя бренда: {brand.name}')
        await duplicate_brand_name(brand_name=brand.name, session=session)
        logger.debug('Проверка дубликата имени бренда завершена')
        new_brand = await brand_crud.create(brand, session, commit=True)
        logger.debug(f'Бренд создан и добавлен в сессию: {new_brand}')
        stmt = select(Brand).options(selectinload(Brand.synonyms)).filter_by(id=new_brand.id)
        result = await session.execute(stmt)
        new_brand = result.scalar_one()
        logger.debug(f'Создан новый бренд: {new_brand}')
        return new_brand

    except IntegrityError as e:
        logger.error(f"Integrity error occurred: {e}")
        await session.rollback()
        raise HTTPException(status_code=400, detail="Integrity error occurred")
    except SQLAlchemyError as e:
        logger.error(f"Database error occurred: {e}")
        await session.rollback()
        raise HTTPException(status_code=500, detail="Internal Server Error")
    except Exception as e:
        logger.error(f"Unexpected error occurred: {e}")
        await session.rollback()
        raise HTTPException(status_code=500, detail="Unexpected error occurred")


@router.delete(
    '/brand/{brand_id}',
    response_model=BrandCreateInDB,
)
async def remove_brand(
        brand_id: int,
        session: AsyncSession = Depends(get_async_session)
):
    brand = await brand_exists(brand_id, session)

    await session.execute(
        delete(brand_synonyms)
        .where(
            (brand_synonyms.c.brand_id == brand.id) |
            (brand_synonyms.c.synonym_id == brand.id)
        )
    )
    return await brand_crud.remove(brand, session, commit=True)


@router.patch(
    '/brand/{brand_id}',
    response_model=BrandCreateInDB,
)
async def update_brand(
        brand_id: int,
        brand: BrandUpdate = Body(...),
        session: AsyncSession = Depends(get_async_session)
):
    async with session.begin():
        brand_db = await brand_exists(brand_id, session)
        logger.debug(f"Existing brand: {brand_db}")
        brand.name = await change_string(brand.name)
        if brand_db.name.lower() != brand.name.lower():
            await duplicate_brand_name(brand_name=brand.name, session=session)
        updated_brand = await brand_crud.update(brand_db, brand, session, commit=False)
        logger.debug(f"Updated brand: {updated_brand}")

        if brand.synonym_name:
            synonym_name = await change_string(brand.synonym_name)
            synonym_brand = await brand_crud.get_brand_by_name(synonym_name, session)
            logger.debug(f"Synonym brand: {synonym_brand}")
            if synonym_brand:
                await brand_crud.add_synonym(updated_brand, synonym_brand, session)
        await session.commit()

    async with session.begin():
        await session.refresh(updated_brand, ['synonyms'])
        logger.debug(f"Final updated brand with synonyms: {updated_brand}")
    return updated_brand


@router.post(
    '/brand/{brand_id}/synonyms',
    response_model=BrandResponse,
    tags=['brand'],
    summary='Добавление синонимов к бренду',
    response_model_exclude_none=True
)
async def add_synonyms(
        brand_id: int,
        synonyms: SynonymCreate,
        session: AsyncSession = Depends(get_async_session)
):
    try:
        async with session.begin():
            change_synonyms = [await change_string(synonym) for synonym in synonyms.names]
            updated_brand = await brand_crud.add_synonyms(session=session, brand_id=brand_id, synonym_names=change_synonyms)
            await session.refresh(updated_brand, attribute_names=['id', 'name', 'synonyms'])
            response_data = {
                "id": updated_brand.id,
                "name": updated_brand.name,
                "synonyms": [{"id": s.id, "name": s.name} for s in updated_brand.synonyms]
            }

        return response_data
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        await session.rollback()
        logger.error(f"Ошибка добаление синонимов: {str(e)}")
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")
