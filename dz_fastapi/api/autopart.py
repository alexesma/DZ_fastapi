import io
import logging
import zipfile
from io import StringIO
from typing import List, Optional

import pandas as pd
import plotly.express as px
import rarfile
from fastapi import (APIRouter, BackgroundTasks, Body, Depends, File, Form,
                     HTTPException, Query, UploadFile, status)
from pydantic import conint
from sqlalchemy import func
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.orm import selectinload
from starlette.responses import HTMLResponse

from dz_fastapi.analytics.price_history import analyze_autopart_allprices
from dz_fastapi.analytics.restock_logic import (
    get_autoparts_below_min_balance, process_restock_pipeline)
from dz_fastapi.api.validators import change_storage_name
from dz_fastapi.core.db import get_session
from dz_fastapi.crud.autopart import crud_autopart, crud_category, crud_storage
from dz_fastapi.crud.brand import brand_crud, brand_exists
from dz_fastapi.models.autopart import (AutoPart, Category, StorageLocation,
                                        preprocess_oem_number)
from dz_fastapi.models.brand import Brand
from dz_fastapi.models.partner import (PriceList, PriceListAutoPartAssociation,
                                       Provider, ProviderPriceListConfig)
from dz_fastapi.schemas.autopart import (AutoPartCreate, AutopartOfferRow,
                                         AutopartOffersResponse,
                                         AutopartOrderRequest,
                                         AutoPartResponse, AutoPartUpdate,
                                         BulkUpdateResponse, CategoryCreate,
                                         CategoryResponse, CategoryUpdate,
                                         StorageLocationCreate,
                                         StorageLocationResponse,
                                         StorageLocationUpdate)
from dz_fastapi.services.process import (assign_brand,
                                         check_start_and_finish_date,
                                         write_error_for_bulk)

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

router = APIRouter()


@router.post(
    '/autoparts/',
    tags=['autopart'],
    status_code=status.HTTP_201_CREATED,
    summary='Создание автозапчасти',
    response_model=AutoPartResponse,
)
async def create_autopart_endpoint(
    autopart: AutoPartCreate, session: AsyncSession = Depends(get_session)
):
    brand_db = await brand_exists(autopart.brand_id, session)
    autopart = await crud_autopart.create_autopart(autopart, brand_db, session)
    return await crud_autopart.get_autopart_by_id(
        session=session, autopart_id=autopart.id
    )


@router.get(
    '/autoparts/offers/',
    tags=['autopart', 'offer'],
    summary='Предложения из прайс-листов по OEM',
    response_model=AutopartOffersResponse,
)
async def get_autopart_offers(
    oem: str = Query(..., description='OEM номер запчасти'),
    session: AsyncSession = Depends(get_session),
):
    normalized_oem = preprocess_oem_number(oem)
    partition_key = func.coalesce(
        PriceList.provider_config_id, PriceList.provider_id
    )
    row_number = func.row_number().over(
        partition_by=(
            PriceListAutoPartAssociation.autopart_id,
            partition_key,
        ),
        order_by=(PriceList.date.desc(), PriceList.id.desc()),
    ).label('rn')

    stmt = (
        select(
            AutoPart.id.label('autopart_id'),
            AutoPart.oem_number.label('oem_number'),
            AutoPart.name.label('autopart_name'),
            Brand.name.label('brand_name'),
            Provider.id.label('provider_id'),
            Provider.name.label('provider_name'),
            Provider.is_own_price.label('is_own_price'),
            ProviderPriceListConfig.id.label('provider_config_id'),
            ProviderPriceListConfig.name_price.label(
                'provider_config_name'
            ),
            PriceListAutoPartAssociation.price.label('price'),
            PriceListAutoPartAssociation.quantity.label('quantity'),
            ProviderPriceListConfig.min_delivery_day.label(
                'min_delivery_day'
            ),
            ProviderPriceListConfig.max_delivery_day.label(
                'max_delivery_day'
            ),
            PriceList.id.label('pricelist_id'),
            PriceList.date.label('pricelist_date'),
            row_number,
        )
        .select_from(PriceListAutoPartAssociation)
        .join(
            PriceList,
            PriceList.id == PriceListAutoPartAssociation.pricelist_id,
        )
        .join(
            AutoPart,
            AutoPart.id == PriceListAutoPartAssociation.autopart_id,
        )
        .join(Brand, Brand.id == AutoPart.brand_id)
        .join(Provider, Provider.id == PriceList.provider_id)
        .outerjoin(
            ProviderPriceListConfig,
            ProviderPriceListConfig.id == PriceList.provider_config_id,
        )
        .where(AutoPart.oem_number == normalized_oem)
    )

    subq = stmt.subquery()
    final_stmt = (
        select(subq)
        .where(subq.c.rn == 1)
        .order_by(
            subq.c.provider_name.asc(),
            subq.c.provider_config_name.asc().nullslast(),
            subq.c.price.asc(),
        )
    )
    result = await session.execute(final_stmt)
    rows = result.mappings().all()

    offers = []
    for row in rows:
        price_value = row.get('price')
        offers.append(
            AutopartOfferRow(
                autopart_id=row['autopart_id'],
                oem_number=row['oem_number'],
                brand_name=row.get('brand_name'),
                name=row.get('autopart_name'),
                provider_id=row['provider_id'],
                provider_name=row['provider_name'],
                provider_config_id=row.get('provider_config_id'),
                provider_config_name=row.get('provider_config_name'),
                price=float(price_value) if price_value is not None else 0.0,
                quantity=row.get('quantity') or 0,
                min_delivery_day=row.get('min_delivery_day'),
                max_delivery_day=row.get('max_delivery_day'),
                pricelist_id=row['pricelist_id'],
                pricelist_date=row.get('pricelist_date'),
                is_own_price=bool(row.get('is_own_price')),
            )
        )

    return AutopartOffersResponse(
        oem_number=normalized_oem, offers=offers
    )


@router.get(
    '/autoparts/{autopart_id}/',
    tags=['autopart'],
    summary='Получение автозапчасти по ID',
    response_model=AutoPartResponse,
)
async def get_autopart_endpoint(
    autopart_id: int, session: AsyncSession = Depends(get_session)
):
    autopart = await crud_autopart.get_autopart_by_id(
        autopart_id=autopart_id, session=session
    )
    if not autopart:
        raise HTTPException(status_code=404, detail='Autopart not found')
    return autopart


@router.get(
    '/autoparts/',
    tags=['autopart'],
    summary='Получение всех автозапчастей',
    response_model=List[AutoPartResponse],
)
async def get_all_autoparts(
    skip: int = 0,
    limit: int = 100,
    oem: Optional[str] = Query(None, description='OEM код запчасти'),
    brand: Optional[str] = Query(None, description='Имя бренда'),
    session: AsyncSession = Depends(get_session),
):
    return await crud_autopart.get_filtered(
        session=session, oem=oem, brand=brand, skip=skip, limit=limit
    )


@router.patch(
    '/autoparts/bulk/',
    tags=['autoparts'],
    summary='Массовая обновления автозапчастей',
    response_model=BulkUpdateResponse,
    status_code=status.HTTP_200_OK,
)
async def bulk_update_autoparts(
    oem_number_col: int = Form(...),
    start_row: int = Form(...),
    file: UploadFile = File(...),
    brand_col: Optional[int] = Form(None),
    multiplicity_col: Optional[int] = Form(None),
    barcode_col: Optional[int] = Form(None),
    storage_locations_col: Optional[int] = Form(None),
    categories_col: Optional[int] = Form(None),
    min_balance_user_col: Optional[int] = Form(None),
    session: AsyncSession = Depends(get_session),
):
    """
    Обновляет автозапчасти в пакетном режиме по данным из Excel-файла.
    Ожидается, что Excel-файл содержит как минимум следующие столбцы:
      - oem_number
      - brand
      - new_storage_location
    """
    try:
        logger.debug(
            f'brand_col = {brand_col}|'
            f' storage_locations_col = {storage_locations_col}|'
            f' oem_number_col = {oem_number_col}'
        )
        file_content = await file.read()
        file_extension = file.filename.split('.')[-1].lower()
        columns = {
            'brand': brand_col,
            'oem_number': oem_number_col,
            'multiplicity': multiplicity_col,
            'barcode': barcode_col,
            'storage_locations': storage_locations_col,
            'categories': categories_col,
            'min_balance_user': min_balance_user_col,
        }
        temp_columns = []
        for name, user_col in columns.items():
            if user_col is None:
                continue
            if user_col < 1:
                raise HTTPException(
                    status_code=400,
                    detail=f'Invalid number column for {name}, got {user_col}',
                )
            zero_based_col = user_col - 1
            temp_columns.append((name, zero_based_col))

        temp_columns.sort(key=lambda x: x[1])

        usecols_list = [col_index for _, col_index in temp_columns]
        col_names = [col_name for col_name, _ in temp_columns]
        try:
            if file_extension == 'zip':
                with zipfile.ZipFile(io.BytesIO(file_content)) as zip:
                    zip_list = zip.namelist()
                    if not zip_list:
                        raise HTTPException(
                            status_code=400, detail="Zip archive is empty"
                        )

                    file_in_zip = zip_list[0]
                    with zip.open(file_in_zip) as inner_file:
                        file_content = inner_file.read()
                        file_extension = file_in_zip.split('.')[-1].lower()

            if file_extension == 'rar':
                with rarfile.RarFile(io.BytesIO(file_content)) as rar:
                    rar_list = rar.namelist()
                    if not rar_list:
                        raise HTTPException(
                            status_code=400, detail="Rar archive is empty"
                        )
                    file_in_rar = rar_list[0]
                    with zip.open(file_in_rar) as inner_file:
                        file_content = inner_file.read()
                        file_extension = file_in_rar.split('.')[-1].lower()

            if file_extension in ['xls', 'xlsx']:
                df = pd.read_excel(
                    io.BytesIO(file_content),
                    header=start_row - 1,
                    usecols=usecols_list,
                )
            elif file_extension == 'csv':
                df = pd.read_csv(
                    io.StringIO(file_content.decode('utf-8')),
                    header=start_row - 1,
                    sep=None,
                    engine='python',
                    usecols=usecols_list,
                )
            else:
                raise HTTPException(
                    status_code=400, detail='Unsupported file type'
                )
            df.columns = col_names
            logger.debug(f'DataFrame:\n{df.head()}')
        except Exception as e:
            raise HTTPException(
                status_code=400, detail=f'Invalid format file:{e}'
            )

        updated_ids = []
        not_found = []
        records = df.to_dict(orient='records')
        for record in records:
            try:
                oem_number = preprocess_oem_number(str(record['oem_number']))
                if brand_col is None:
                    brand_name = assign_brand(oem_original=oem_number)[0]
                else:
                    brand_name = record['brand']
                    logger.debug(f'Extracted brand from file: {brand_name}')
            except Exception as e:
                write_error_for_bulk(
                    problem_items=record,
                    not_found=not_found,
                    error=str(e),
                    error_message='Cannot extract oem/brand',
                )
                continue
            brand_obj = await brand_crud.get_brand_by_name_or_none(
                brand_name=brand_name, session=session
            )
            logger.debug(
                f'Name brand = {brand_obj.name if brand_obj else None}'
            )
            if not oem_number or not brand_obj:
                write_error_for_bulk(
                    problem_items=record,
                    not_found=not_found,
                    error_message='Missing OEM number or brand not found',
                )
                continue

            autopart = await crud_autopart.get_autopart_by_oem_brand_or_none(
                oem_number=oem_number, brand_id=brand_obj.id, session=session
            )
            if not autopart:
                logger.debug(
                    f'Autoparts not found {oem_number}, {brand_obj.name}'
                )
                write_error_for_bulk(
                    problem_items=record,
                    not_found=not_found,
                    error_message='AutoPart not found',
                )
                continue
            logger.debug(
                f'Autoparts found = {autopart.name} {autopart.oem_number}'
            )
            update_fields = {}
            relationship_updated = False
            if multiplicity_col is not None:
                multiplicity = record.get('multiplicity')
                if multiplicity not in (None, '', 'null'):
                    update_fields['multiplicity'] = multiplicity
            if barcode_col is not None:
                barcode = record.get('barcode')
                if barcode not in (None, '', 'null'):
                    update_fields['barcode'] = barcode
            if storage_locations_col is not None:
                storage_locations = record.get('storage_locations')
                if storage_locations not in (None, '', 'null'):
                    storage_obj = (
                        await crud_storage.get_storage_location_id_by_name(
                            storage_location_name=storage_locations,
                            session=session,
                        )
                    )
                    logger.debug(
                        f'Storage location = '
                        f'{storage_obj.name if storage_obj else None}'
                    )
                    if storage_obj is None:
                        write_error_for_bulk(
                            problem_items=record,
                            not_found=not_found,
                            error_message='Storage location not found',
                            error=str(storage_locations),
                        )
                    else:
                        if storage_obj not in autopart.storage_locations:
                            autopart.storage_locations.append(storage_obj)
                            relationship_updated = True
            if categories_col is not None:
                categories = record.get('categories')
                if categories not in (None, '', 'null'):
                    category_obj = await crud_category.get_category_id_by_name(
                        category_name=categories, session=session
                    )
                    if category_obj is None:
                        write_error_for_bulk(
                            problem_items=record,
                            not_found=not_found,
                            error_message='Categories not found',
                            error=str(categories),
                        )
                    else:
                        if category_obj not in autopart.categories:
                            autopart.categories.append(category_obj)
                            relationship_updated = True

            if update_fields or relationship_updated:
                logger.debug(f'update_fields = {update_fields}')
                for field, value in update_fields.items():
                    setattr(autopart, field, value)
                updated_ids.append(autopart.id)
            else:
                logger.debug('update_fields not found')
                not_found.append(
                    {
                        'record': {
                            'oem_number': oem_number,
                            'brand': brand_name,
                        },
                        'error': 'No update fields provided',
                    }
                )
        try:
            logger.debug('Try commit')
            await session.commit()
            logger.debug('Try successfull')
        except Exception as e:
            logger.exception('Error during bulk update')
            await session.rollback()
            raise HTTPException(
                status_code=500, detail=f'Error during bulk update: {e}'
            )
        return BulkUpdateResponse(
            updated_count=len(updated_ids), not_found_parts=not_found
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.exception('Unexpected error in bulk update')
        raise HTTPException(status_code=500, detail=f'Unexpected error: {e}')


@router.patch(
    '/autoparts/{autopart_id}/',
    tags=['autopart'],
    summary='Обновление автозапчасти',
    response_model=AutoPartResponse,
)
async def update_autopart(
    autopart_id: int,
    autopart: AutoPartUpdate = Body(...),
    session: AsyncSession = Depends(get_session),
):
    autopart_db = await crud_autopart.get_autopart_by_id(
        autopart_id=autopart_id, session=session
    )
    update_data = autopart.model_dump(exclude_unset=True)
    if autopart_db is None:
        raise HTTPException(status_code=404, detail='AutoPart not found')
    if 'brand_id' not in update_data or update_data['brand_id'] is None:
        update_data['brand_id'] = autopart_db.brand_id
    else:
        await brand_exists(update_data['brand_id'], session)
    if 'name' not in update_data or update_data['name'] is None:
        update_data['name'] = autopart_db.name

    if 'oem_number' not in update_data or update_data['oem_number'] is None:
        update_data['oem_number'] = autopart_db.oem_number
    updated_autopart = await crud_autopart.update(
        db_obj=autopart_db, obj_in=autopart, session=session
    )
    return updated_autopart


@router.post(
    '/categories/',
    tags=['category'],
    summary='Создание категории',
    response_model=CategoryResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_category(
    category_in: CategoryCreate, session: AsyncSession = Depends(get_session)
):
    try:
        result = await session.execute(
            select(Category).where(Category.name == category_in.name)
        )
        existing_category = result.scalar_one_or_none()

        if existing_category:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=(
                    f'Category with name {category_in.name} already exists.'
                ),
            )
        new_category = Category(**category_in.model_dump())
        session.add(new_category)
        await session.commit()
        await session.refresh(new_category)
        result = await session.execute(
            select(Category)
            .options(selectinload(Category.children))
            .where(Category.id == new_category.id)
        )
        category = result.scalar_one()
        return category
    except SQLAlchemyError as error:
        await session.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail='An error occurred while creating the category.',
        ) from error


@router.get(
    '/categories/',
    tags=['category'],
    summary='Получение всех категорий',
    response_model=list[CategoryResponse],
)
async def get_categories(
    skip: conint(ge=0) = 0,
    limit: conint(ge=1) = 100,
    session: AsyncSession = Depends(get_session),
):
    categories = await crud_category.get_multi(session, skip=skip, limit=limit)
    return categories


@router.get(
    '/categories/{category_id}/',
    tags=['category'],
    summary='Получение категории по ID',
    response_model=CategoryResponse,
)
async def get_category(
    category_id: int, session: AsyncSession = Depends(get_session)
):
    category = await crud_category.get_category_by_id(
        category_id=category_id, session=session
    )
    if not category:
        raise HTTPException(status_code=404, detail='Category not found')
    return category


@router.patch(
    '/categories/{category_id}/',
    tags=['category'],
    summary='Обновление категории',
    response_model=CategoryResponse,
)
async def update_category(
    category_id: int,
    category_in: CategoryUpdate,
    session: AsyncSession = Depends(get_session),
):
    category_old = await crud_category.get_category_by_id(
        category_id=category_id, session=session
    )
    if not category_old:
        raise HTTPException(status_code=404, detail='Category not found')
    try:
        updated_category = await crud_category.update(
            db_obj=category_old, obj_in=category_in, session=session
        )
    except IntegrityError as e:
        await session.rollback()
        raise HTTPException(
            status_code=400,
            detail=f'Category with name {category_in.name} already exists.',
        ) from e
    return updated_category


@router.post(
    '/categories/bulk/',
    tags=['category'],
    summary='Массовая вставка категорий автозапчастей',
    status_code=status.HTTP_200_OK,
    response_model=List[CategoryResponse],
)
async def create_categories_bulk(
    categories_data: List[CategoryCreate],
    session: AsyncSession = Depends(get_session),
):
    created_cats = await crud_category.create_many(
        category_data=categories_data, session=session
    )
    return [CategoryResponse.from_orm(cat) for cat in created_cats]


@router.post(
    '/storage/',
    status_code=status.HTTP_201_CREATED,
    summary='Создание местохранения',
    tags=['storage'],
    response_model=StorageLocationUpdate,
)
async def create_storage_location(
    storage_in: StorageLocationCreate,
    session: AsyncSession = Depends(get_session),
):
    storage_in.name = await change_storage_name(storage_in.name)
    logger.debug(f'Processed storage name: {storage_in.name}')
    if len(storage_in.name) <= 2:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f'Storage\'s name ({storage_in.name}) is short',
        )
    try:
        result = await session.execute(
            select(StorageLocation).where(
                StorageLocation.name == storage_in.name
            )
        )
        existing_storage = result.scalar_one_or_none()

        if existing_storage:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=(
                    f'Storage with name {storage_in.name} already exists.'
                ),
            )
        storage = await crud_storage.create(storage_in, session)
        return storage
    except IntegrityError as error:
        await session.rollback()
        if 'unique constraint' in str(error):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=(
                    f'Storage with name {storage_in.name} already exists.'
                ),
            ) from error
        elif 'check constraint' in str(error):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail='Storage name violates database constraints.',
            ) from error
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail='An error occurred while creating the storage.',
            ) from error


@router.get(
    '/storage/',
    summary='Получение всех местохранений',
    tags=['storage'],
    status_code=status.HTTP_200_OK,
    response_model=list[StorageLocationResponse],
)
async def get_storage_locations(
    session: AsyncSession = Depends(get_session),
    skip: int = 0,
    limit: int = 100,
):
    storages = await crud_storage.get_multi(session, skip=skip, limit=limit)
    return storages


@router.get(
    '/storage/{storage_id}/',
    summary='Получение местохранения по ID',
    status_code=status.HTTP_200_OK,
    tags=['storage'],
    response_model=StorageLocationResponse,
)
async def get_storage_location(
    storage_id: int, session: AsyncSession = Depends(get_session)
):
    storage = await crud_storage.get_storage_location_by_id(
        storage_location_id=storage_id, session=session
    )
    if not storage:
        raise HTTPException(
            status_code=404, detail='Storage location not found'
        )
    return storage


@router.patch(
    '/storage/{storage_id}/',
    summary='Обновление местохранения',
    tags=['storage'],
    status_code=status.HTTP_200_OK,
    response_model=StorageLocationResponse,
)
async def update_storage_location(
    storage_id: int,
    storage_in: StorageLocationUpdate,
    session: AsyncSession = Depends(get_session),
):
    storage_old = await crud_storage.get_storage_location_by_id(
        storage_location_id=storage_id, session=session
    )
    if not storage_old:
        raise HTTPException(
            status_code=404, detail='Storage location not found'
        )
    try:
        if len(storage_in.name) <= 2:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f'Storage\'s name ({storage_in.name}) is short',
            )
        updated_storage = await crud_storage.update(
            db_obj=storage_old, obj_in=storage_in, session=session
        )
    except IntegrityError as e:
        await session.rollback()
        raise HTTPException(
            status_code=400,
            detail=f'Storage with name {storage_in.name} already exists.',
        ) from e
    return updated_storage


@router.post(
    '/storage/bulk/',
    summary='Массовое создание мест хранения',
    status_code=status.HTTP_200_OK,
    tags=['storage'],
    response_model=List[StorageLocationResponse],
)
async def create_storages_bulk(
    storages_data: List[StorageLocationCreate],
    session: AsyncSession = Depends(get_session),
):
    created_locations = await crud_storage.create_locations(
        locations_data=storages_data, session=session
    )
    return [
        StorageLocationResponse.from_orm(location)
        for location in created_locations
    ]


@router.get(
    '/autoparts/{oem_number}/price-history/plot/',
    summary='Анализ изменения цены/временя по поставщикам',
    status_code=status.HTTP_200_OK,
    tags=['autopart', 'analytic'],
)
async def get_price_history_plot(
    oem_number: str,
    date_start: Optional[str] = Query(
        default=None, description='Start date in format YYYY-MM-DD'
    ),
    date_finish: Optional[str] = Query(
        default=None, description='End date in format YYYY-MM-DD'
    ),
    session: AsyncSession = Depends(get_session),
) -> HTMLResponse:
    # 1. Получаем запчасть по oem_number
    normalized_oem = preprocess_oem_number(oem_number)

    autoparts_for_analytic = await crud_autopart.get_autoparts_by_oem_or_none(
        oem_number=normalized_oem, session=session
    )
    if not autoparts_for_analytic:
        logger.debug(f'No autoparts found for OEM {normalized_oem}')
    else:
        names = ', '.join(
            [autopart.name for autopart in autoparts_for_analytic]
        )
        logger.debug(f'Autoparts found for OEM {oem_number}: {names}')

    start_dt, finish_dt = check_start_and_finish_date(date_start, date_finish)
    df = await analyze_autopart_allprices(
        session=session,
        autoparts=autoparts_for_analytic,
        date_start=start_dt,
        date_finish=finish_dt,
    )
    fig = px.line(
        df,
        x='created_at',
        y='price',
        color='provider',
        title=f'Динамика цены по OEM {normalized_oem}',
        labels={
            'created_at': 'Дата',
            'price': 'Цена',
            'provider': 'Поставщик',
        },
        markers=True,
    )
    fig.update_layout(hovermode='x unified')

    html_io = StringIO()
    fig.write_html(html_io, include_plotlyjs='include')
    html_io.seek(0)
    return HTMLResponse(content=html_io.getvalue())


@router.post(
    '/autoparts/restock/',
    summary='Анализ и формирование заказа',
    status_code=status.HTTP_200_OK,
    tags=['autopart', 'restock'],
)
async def restock_autoparts(
    background_tasks: BackgroundTasks,
    request: AutopartOrderRequest = Body(default_factory=AutopartOrderRequest),
    session: AsyncSession = Depends(get_session),
):
    autoparts = request.autoparts
    if autoparts is None:
        autoparts = await get_autoparts_below_min_balance(
            threshold_percent=request.threshold_percent, session=session
        )
    background_tasks.add_task(
        process_restock_pipeline,
        session=session,
        budget_limit=request.budget_limit,
        months_back=request.months_back,
        email_to=request.email_to,
        telegram_chat_id=request.telegram_chat_id,
        autoparts=autoparts,
        threshold_percent=request.threshold_percent,
    )
    return {
        'status': 'success',
        'message': 'Отчет формируется и '
                   'будет отправлен на указанные контакты.',
    }
