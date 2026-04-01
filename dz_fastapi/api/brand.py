import io
import logging
from pathlib import Path

import aiofiles
from fastapi import APIRouter, Body, Depends, File, HTTPException, UploadFile
from PIL import Image
from sqlalchemy import delete, func, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession
from starlette import status

from dz_fastapi.api.validators import change_brand_name, change_string
from dz_fastapi.core.constants import (UPLOAD_DIR, get_max_file_size,
                                       get_upload_dir)
from dz_fastapi.core.db import get_session
from dz_fastapi.crud.brand import (brand_crud, brand_exists,
                                   duplicate_brand_name)
from dz_fastapi.models.brand import Brand, brand_synonyms
from dz_fastapi.models.partner import (PriceList, PriceListMissingBrand,
                                       Provider, ProviderPriceListConfig)
from dz_fastapi.schemas.brand import (BrandCreate, BrandCreateInDB,
                                      BrandLookupItem, BrandResponse,
                                      BrandUpdate, MissingBrandByPricelist,
                                      MissingBrandResolveRequest,
                                      SynonymCreate)

logger = logging.getLogger('dz_fastapi')

router = APIRouter(prefix='/brand')
UPLOAD_DIR = Path(UPLOAD_DIR)


async def _serialize_brand_for_response(
    brand: Brand, session: AsyncSession
) -> BrandCreateInDB:
    all_synonyms = await brand_crud.get_all_synonyms_bi_directional(
        brand, session
    )
    return BrandCreateInDB(
        id=brand.id,
        name=brand.name,
        description=brand.description,
        main_brand=bool(brand.main_brand),
        website=brand.website,
        country_of_origin=brand.country_of_origin,
        logo=brand.logo,
        synonyms=[
            {'id': syn.id, 'name': syn.name}
            for syn in all_synonyms
            if syn.id != brand.id
        ],
    )


@router.get(
    '/',
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
    '/lookup/',
    response_model=list[BrandLookupItem],
    tags=['brand'],
    summary='Поиск брендов по имени',
)
async def lookup_brands(
    q: str = '',
    limit: int = 50,
    ids: str | None = None,
    session: AsyncSession = Depends(get_session),
):
    normalized = await change_brand_name(q) if q else ''
    stmt = select(Brand.id, Brand.name).order_by(Brand.name.asc())
    brand_ids: list[int] = []
    if ids:
        brand_ids = [
            int(part)
            for part in ids.split(',')
            if part.strip().isdigit()
        ]
    if brand_ids:
        stmt = stmt.where(Brand.id.in_(brand_ids))
        stmt = stmt.limit(max(1, len(brand_ids)))
    elif normalized:
        stmt = stmt.where(Brand.name.ilike(f'%{normalized}%'))
        stmt = stmt.limit(max(1, min(limit, 200)))
    else:
        stmt = stmt.limit(max(1, min(limit, 200)))
    rows = (await session.execute(stmt)).all()
    return [BrandLookupItem(id=row.id, name=row.name) for row in rows]


@router.get(
    '/missing-from-pricelists',
    response_model=list[MissingBrandByPricelist],
    tags=['brand'],
    summary='Отсутствующие бренды из последних прайсов поставщиков',
    response_model_exclude_none=True,
)
async def get_missing_brands_from_pricelists(
    session: AsyncSession = Depends(get_session),
):
    latest_pricelist_subquery = (
        select(
            PriceList.provider_config_id.label('provider_config_id'),
            PriceList.id.label('pricelist_id'),
            PriceList.date.label('pricelist_date'),
            func.row_number()
            .over(
                partition_by=PriceList.provider_config_id,
                order_by=(PriceList.date.desc(), PriceList.id.desc()),
            )
            .label('row_number'),
        )
        .where(PriceList.provider_config_id.is_not(None))
        .subquery()
    )
    stmt = (
        select(
            Provider.id.label('provider_id'),
            Provider.name.label('provider_name'),
            ProviderPriceListConfig.id.label('provider_config_id'),
            ProviderPriceListConfig.name_price.label(
                'provider_config_name'
            ),
            PriceListMissingBrand.pricelist_id.label('pricelist_id'),
            latest_pricelist_subquery.c.pricelist_date.label(
                'pricelist_date'
            ),
            PriceListMissingBrand.brand_name.label('brand_name'),
            PriceListMissingBrand.positions_count.label(
                'positions_count'
            ),
        )
        .join(
            latest_pricelist_subquery,
            (
                latest_pricelist_subquery.c.provider_config_id
                == PriceListMissingBrand.provider_config_id
            )
            & (
                latest_pricelist_subquery.c.pricelist_id
                == PriceListMissingBrand.pricelist_id
            )
            & (latest_pricelist_subquery.c.row_number == 1),
        )
        .join(
            ProviderPriceListConfig,
            ProviderPriceListConfig.id
            == PriceListMissingBrand.provider_config_id,
        )
        .join(Provider, Provider.id == ProviderPriceListConfig.provider_id)
        .outerjoin(Brand, Brand.name == PriceListMissingBrand.brand_name)
        .where(PriceListMissingBrand.positions_count > 0, Brand.id.is_(None))
        .order_by(
            PriceListMissingBrand.positions_count.desc(),
            Provider.name.asc(),
            ProviderPriceListConfig.name_price.asc().nullslast(),
            PriceListMissingBrand.brand_name.asc(),
        )
    )
    rows = (await session.execute(stmt)).mappings().all()
    return [MissingBrandByPricelist(**row) for row in rows]


@router.post(
    '/missing-from-pricelists/resolve',
    tags=['brand'],
    summary='Разрешить отсутствующий бренд',
    response_model=BrandCreateInDB,
    response_model_exclude_none=True,
)
async def resolve_missing_brand(
    payload: MissingBrandResolveRequest,
    session: AsyncSession = Depends(get_session),
):
    missing_brand_name = await change_brand_name(payload.missing_brand_name)
    if not missing_brand_name:
        raise HTTPException(
            status_code=400, detail='Пустое имя отсутствующего бренда'
        )

    existing_missing_brand = await brand_crud.get_brand_by_name_or_none(
        brand_name=missing_brand_name, session=session
    )

    if payload.action == 'create_brand':
        if existing_missing_brand:
            return await _serialize_brand_for_response(
                existing_missing_brand, session
            )
        brand_in = BrandCreate(
            name=missing_brand_name,
            country_of_origin=payload.country_of_origin,
            website=payload.website,
            description=payload.description,
            main_brand=payload.main_brand,
            synonyms=[],
        )
        created = await brand_crud.create(brand=brand_in, session=session)
        return await _serialize_brand_for_response(created, session)

    if payload.target_brand_id is None:
        raise HTTPException(
            status_code=400,
            detail='Не передан target_brand_id для действия set_synonym',
        )

    target_brand = await brand_exists(payload.target_brand_id, session)

    if not existing_missing_brand:
        missing_brand_in = BrandCreate(
            name=missing_brand_name,
            country_of_origin=payload.country_of_origin,
            website=payload.website,
            description=payload.description,
            main_brand=False,
            synonyms=[],
        )
        existing_missing_brand = await brand_crud.create(
            brand=missing_brand_in, session=session
        )
    elif existing_missing_brand.id == target_brand.id:
        return await _serialize_brand_for_response(target_brand, session)

    await brand_crud.add_synonym(
        brand=target_brand, synonym=existing_missing_brand, session=session
    )
    await session.commit()
    await session.refresh(target_brand)
    return await _serialize_brand_for_response(target_brand, session)


@router.get(
    '/{brand_id}',
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
    '/{brand_id}/upload-logo',
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
    '/',
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
    '/{brand_id}',
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
    '/{brand_id}',
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

            return await _serialize_brand_for_response(updated_brand, session)

    except Exception as e:
        logger.error(f'Error updating brand: {str(e)}')
        await session.rollback()
        raise HTTPException(
            status_code=500, detail=f'An error occurred: {str(e)}'
        )


@router.post(
    '/{brand_id}/synonyms/',
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
        logger.error(f'Ошибка добавление синонимов: {str(e)}')
        raise HTTPException(
            status_code=500, detail=f'An error occurred: {str(e)}'
        )


@router.delete(
    '/{brand_id}/synonyms',
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
