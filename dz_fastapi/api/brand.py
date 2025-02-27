import io
import logging
from pathlib import Path

import aiofiles
from fastapi import APIRouter, Body, Depends, File, HTTPException, UploadFile
from PIL import Image
from sqlalchemy import delete
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession
from starlette import status

from dz_fastapi.api.validators import change_string
from dz_fastapi.core.constants import (UPLOAD_DIR, get_max_file_size,
                                       get_upload_dir)
from dz_fastapi.core.db import get_session
from dz_fastapi.crud.brand import (brand_crud, brand_exists,
                                   duplicate_brand_name)
from dz_fastapi.models.brand import Brand, brand_synonyms
from dz_fastapi.schemas.brand import (BrandCreate, BrandCreateInDB,
                                      BrandResponse, BrandUpdate,
                                      SynonymCreate)

logger = logging.getLogger('dz_fastapi')

router = APIRouter()
UPLOAD_DIR = Path(UPLOAD_DIR)


@router.get(
    '/brand',
    response_model=list[BrandCreateInDB],
    tags=['brand'],
    summary='Список брендов',
    response_model_exclude_none=True,
)
async def get_brands(session: AsyncSession = Depends(get_session)):
    brands = await brand_crud.get_multi_with_synonyms(session)
    for brand in brands:
        brand.synonyms = await brand_crud.get_all_synonyms_bi_directional(
            brand, session
        )
    return brands


@router.get(
    '/brand/{brand_id}',
    response_model=BrandCreateInDB,
    tags=['brand'],
    summary='Получение данных по бренду',
    status_code=status.HTTP_200_OK,
    response_model_exclude_none=True,
)
async def get_brand(
    brand_id: int, session: AsyncSession = Depends(get_session)
):
    brand = await brand_crud.get_brand_by_id(
        brand_id=brand_id, session=session
    )

    if not brand:
        raise HTTPException(status_code=404, detail='Brand not found')

    brand.synonyms = await brand_crud.get_all_synonyms_bi_directional(
        brand=brand, session=session
    )
    return brand


@router.patch(
    '/brand/{brand_id}/upload-logo',
    tags=['brand'],
    summary='Загрузка логотипа бренда',
    response_model_exclude_none=True,
    response_model=BrandCreateInDB,
)
async def upload_logo(
    brand_id: int,
    file: UploadFile = File(...),
    session: AsyncSession = Depends(get_session),
    max_file_size: int = Depends(get_max_file_size),
    upload_dir: str = Depends(get_upload_dir),
):
    try:
        logger.info(f'Try to change brand id: {brand_id}')
        brand_db = await brand_exists(brand_id, session)
        if file.content_type not in ['image/jpeg', 'image/png']:
            logger.debug(f'File for brand id {brand_id} is not jpeg or png')
            raise HTTPException(
                status_code=400,
                detail='Invalid file type. Only JPEG and PNG are allowed.',
            )
        contents = await file.read()
        if len(contents) > max_file_size:
            logger.debug(
                f'File size exceeds limit: {len(contents)} '
                f'> {max_file_size} for brand id {brand_id}'
            )
            raise HTTPException(
                status_code=400,
                detail='File size exceeds the maximum allowed size.',
            )

        logger.info(
            f'File size = {len(contents)} and max size = {max_file_size}'
        )
        file_ext = Path(file.filename).suffix
        logo_filename = f"brand_{brand_id}_logo{file_ext}"
        file_path = Path(upload_dir) / logo_filename
        logger.debug(f'Path = {file_path}')

        async with aiofiles.open(file_path, 'wb') as buffer:
            await buffer.write(contents)

        try:
            with Image.open(io.BytesIO(contents)) as img:
                img.verify()
        except (IOError, SyntaxError) as e:
            file_path.unlink(missing_ok=True)
            logger.debug(f'FIle for brand id {brand_id} is invalid')
            raise HTTPException(
                status_code=400, detail=f'Invalid image file. Error: {str(e)}'
            )
        brand_db.logo = str(file_path)

        all_synonyms = await brand_crud.get_all_synonyms_bi_directional(
            brand_db, session
        )
        brand_data = {
            'id': brand_db.id,
            'name': brand_db.name,
            'country_of_origin': brand_db.country_of_origin,
            'website': brand_db.website,
            'description': brand_db.description,
            'logo': brand_db.logo,
            'synonyms': [
                {'id': syn.id, 'name': syn.name}
                for syn in all_synonyms
                if syn.id != brand_db.id
            ],
        }
        await session.commit()
        await session.refresh(brand_db)
        return BrandCreateInDB(**brand_data)

    except SQLAlchemyError as e:
        await session.rollback()
        logger.exception('Database error occurred while uploading logo')
        raise HTTPException(
            status_code=500,
            detail=f'Database error occurred while uploading logo. Error: {e}',
        )
    except HTTPException:
        raise
    except Exception as e:
        await session.rollback()
        logger.exception('Unexpected error occurred while uploading logo')
        raise HTTPException(
            status_code=500,
            detail=(
                f'Unexpected error occurred while uploading logo. Error: {e}'
            ),
        )


@router.post(
    '/brand/',
    response_model=BrandCreateInDB,
    tags=['brand'],
    summary='Создание бренда',
    response_model_exclude_none=True,
    status_code=status.HTTP_201_CREATED,
)
async def create_brand(
    brand: BrandCreate, session: AsyncSession = Depends(get_session)
):
    return await brand_crud.create(brand=brand, session=session)


@router.delete(
    '/brand/{brand_id}',
    tags=['brand'],
    summary='Удаление бренда',
    status_code=status.HTTP_200_OK,
    response_model=BrandCreateInDB,
)
async def remove_brand(
    brand_id: int, session: AsyncSession = Depends(get_session)
):
    brand = await brand_exists(brand_id, session)

    await session.execute(
        delete(brand_synonyms).where(
            (brand_synonyms.c.brand_id == brand.id)
            | (brand_synonyms.c.synonym_id == brand.id)
        )
    )
    return await brand_crud.remove(brand, session, commit=True)


@router.patch(
    '/brand/{brand_id}',
    tags=['brand'],
    summary='Обновление бренда',
    response_model_exclude_none=True,
    response_model=BrandCreateInDB,
)
async def update_brand(
    brand_id: int,
    brand: BrandUpdate = Body(...),
    session: AsyncSession = Depends(get_session),
):
    try:
        async with session.begin():
            brand_db = await brand_exists(brand_id, session)
            logger.debug(f"Existing brand: {brand_db}")
            if brand.name:
                brand.name = await change_string(brand.name)
                logger.debug(f'Updated brand name: {brand.name}')
                if brand_db.name != brand.name:
                    await duplicate_brand_name(
                        brand_name=brand.name, session=session
                    )
            updated_brand = await brand_crud.update(
                brand_db, brand, session, commit=False
            )
            if not updated_brand:
                raise HTTPException(
                    status_code=500, detail='Failed to update brand'
                )
            logger.debug(f'Updated brand: {updated_brand}')

            await brand_crud.get_all_synonyms_bi_directional(
                updated_brand, session
            )
            # brand_data = {
            #     'id': updated_brand.id,
            #     'name': updated_brand.name,
            #     'country_of_origin':  updated_brand.country_of_origin,
            #     'logo': updated_brand.logo,
            #     'website': updated_brand.website,
            #     'description': updated_brand.description,
            #     'main_brand': updated_brand.main_brand,
            #     'synonyms': [{
            #     'id': syn.id,
            #     'name': syn.name
            #     } for syn in all_synonyms if syn.id != updated_brand.id]
            # }
            #
            # return BrandCreateInDB(**brand_data)
            brand_in_db = BrandCreateInDB.from_orm(updated_brand)
            return brand_in_db

    except Exception as e:
        logger.error(f'Error updating brand: {str(e)}')
        await session.rollback()
        raise HTTPException(
            status_code=500, detail=f'An error occurred: {str(e)}'
        )


@router.post(
    '/brand/{brand_id}/synonyms',
    response_model=BrandResponse,
    tags=['brand'],
    summary='Добавление синонимов к бренду',
    status_code=status.HTTP_200_OK,
    response_model_exclude_none=True,
)
async def add_synonyms(
    brand_id: int,
    synonyms: SynonymCreate,
    session: AsyncSession = Depends(get_session),
):
    try:
        async with session.begin():
            change_synonyms = [
                await change_string(synonym) for synonym in synonyms.names
            ]
            await brand_crud.add_synonyms(
                session=session,
                brand_id=brand_id,
                synonym_names=change_synonyms,
            )
            updated_brand = await session.get(Brand, brand_id)
            await session.refresh(
                updated_brand, attribute_names=['id', 'name', 'synonyms']
            )
            response_data = {
                'id': updated_brand.id,
                'name': updated_brand.name,
                'synonyms': [
                    {'id': s.id, 'name': s.name}
                    for s in updated_brand.synonyms
                ],
            }

        return response_data
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        await session.rollback()
        logger.error(f'Ошибка добаление синонимов: {str(e)}')
        raise HTTPException(
            status_code=500, detail=f'An error occurred: {str(e)}'
        )


@router.delete(
    '/brand/{brand_id}/synonyms',
    response_model=BrandResponse,
    tags=['brand'],
    summary='Удаление синонимов к бренду',
    status_code=status.HTTP_200_OK,
    response_model_exclude_none=True,
)
async def delete_synonyms(
    brand_id: int,
    synonyms: SynonymCreate,
    session: AsyncSession = Depends(get_session),
):
    try:
        async with session.begin():
            change_synonyms = [
                await change_string(synonym) for synonym in synonyms.names
            ]
            updated_brand = await brand_crud.remove_synonyms(
                session=session,
                brand_id=brand_id,
                synonym_names=change_synonyms,
            )
            response_data = {
                'id': updated_brand.id,
                'name': updated_brand.name,
                'synonyms': [
                    {'id': s.id, 'name': s.name}
                    for s in updated_brand.synonyms
                ],
            }
            return response_data
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        await session.rollback()
        logger.error(f'Ошибка удаления синонимов: {str(e)}')
        raise HTTPException(
            status_code=500, detail=f'An error occurred: {str(e)}'
        )
