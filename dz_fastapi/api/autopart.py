import io
import logging
import zipfile
from io import StringIO
from typing import List, Optional

import pandas as pd
import plotly.graph_objects as go
import rarfile
from fastapi import (APIRouter, BackgroundTasks, Body, Depends, File, Form,
                     HTTPException, Query, UploadFile, status)
from plotly.colors import qualitative
from plotly.subplots import make_subplots
from pydantic import conint
from sqlalchemy import case, func
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.orm import selectinload
from starlette.responses import HTMLResponse

from dz_fastapi.analytics.price_history import (
    analyze_autopart_allprices, prepare_price_history_plot_data)
from dz_fastapi.analytics.restock_logic import (
    get_autoparts_below_min_balance, process_restock_pipeline)
from dz_fastapi.api.validators import change_storage_name
from dz_fastapi.core.db import get_session
from dz_fastapi.crud.autopart import (crud_autopart, crud_category,
                                      crud_storage, crud_warehouse)
from dz_fastapi.crud.brand import brand_crud, brand_exists
from dz_fastapi.models.autopart import (AutoPart, Category, StorageLocation,
                                        preprocess_oem_number)
from dz_fastapi.models.brand import Brand
from dz_fastapi.models.cross import AutoPartCross
from dz_fastapi.models.inventory import Warehouse
from dz_fastapi.models.nomenclature import (ApplicabilityNode,
                                            HonestSignCategory)
from dz_fastapi.models.partner import (PriceList, PriceListAutoPartAssociation,
                                       Provider, ProviderPriceListConfig)
from dz_fastapi.schemas.autopart import (ApplicabilityNodeCreate,
                                         ApplicabilityNodeFlatOut,
                                         ApplicabilityNodeOut,
                                         AutoPartCatalogItem,
                                         AutoPartCatalogResponse,
                                         AutoPartCreate,
                                         AutoPartDetailResponse,
                                         AutoPartLookupItem, AutopartOfferRow,
                                         AutopartOffersResponse,
                                         AutopartOrderRequest,
                                         AutoPartResponse, AutoPartUpdate,
                                         BulkUpdateResponse, CategoryCreate,
                                         CategoryResponse, CategoryUpdate,
                                         CrossCreate, CrossOut,
                                         HonestSignCategoryCreate,
                                         HonestSignCategoryOut,
                                         StorageLocationCreate,
                                         StorageLocationOut,
                                         StorageLocationResponse,
                                         StorageLocationUpdate)
from dz_fastapi.schemas.inventory import (WarehouseCreate, WarehouseOut,
                                          WarehouseUpdate)
from dz_fastapi.services.inventory_stock import ensure_default_warehouse
from dz_fastapi.services.process import (assign_brand,
                                         check_start_and_finish_date,
                                         write_error_for_bulk)

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

router = APIRouter()


def _warehouse_to_out(warehouse: Warehouse) -> WarehouseOut:
    locations = list(getattr(warehouse, 'locations', None) or [])
    locations_count = sum(1 for loc in locations if loc.system_code is None)
    return WarehouseOut(
        id=warehouse.id,
        name=warehouse.name,
        comment=warehouse.comment,
        is_active=bool(warehouse.is_active),
        locations_count=locations_count,
    )


def _storage_to_response(storage: StorageLocation) -> StorageLocationResponse:
    warehouse = getattr(storage, 'warehouse', None)
    return StorageLocationResponse(
        id=storage.id,
        name=storage.name,
        location_type=storage.location_type,
        capacity=storage.capacity,
        warehouse_id=storage.warehouse_id,
        warehouse_name=warehouse.name if warehouse is not None else None,
        system_code=storage.system_code,
        is_system=bool(storage.system_code),
        autoparts=list(getattr(storage, 'autoparts', None) or []),
    )


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
    partial: bool = Query(
        False, description='Искать по части OEM номера'
    ),
    session: AsyncSession = Depends(get_session),
):
    normalized_oem = preprocess_oem_number(oem)
    if not normalized_oem:
        return AutopartOffersResponse(
            oem_number=normalized_oem,
            offers=[],
            historical_offers=[],
        )
    startswith_pattern = f'{normalized_oem}%'
    contains_pattern = f'%{normalized_oem}%'
    oem_filter = (
        AutoPart.oem_number.ilike(contains_pattern)
        if partial
        else AutoPart.oem_number == normalized_oem
    )
    oem_rank = case(
        (AutoPart.oem_number == normalized_oem, 0),
        (AutoPart.oem_number.ilike(startswith_pattern), 1),
        else_=2,
    )

    partition_key = func.coalesce(
        PriceList.provider_config_id, PriceList.provider_id
    ).label('partition_key')
    latest_pricelist_rank = func.row_number().over(
        partition_by=partition_key,
        order_by=(PriceList.date.desc(), PriceList.id.desc()),
    ).label('latest_rn')

    latest_pricelists = (
        select(
            PriceList.id.label('pricelist_id'),
            partition_key,
            PriceList.date.label('pricelist_date'),
            latest_pricelist_rank,
        )
        .select_from(PriceList)
        .where(PriceList.is_active.is_(True))
        .subquery()
    )

    current_stmt = (
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
            latest_pricelists.c.partition_key.label('partition_key'),
        )
        .select_from(latest_pricelists)
        .join(PriceList, PriceList.id == latest_pricelists.c.pricelist_id)
        .join(
            PriceListAutoPartAssociation,
            PriceListAutoPartAssociation.pricelist_id == PriceList.id,
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
        .where(latest_pricelists.c.latest_rn == 1)
        .where(oem_filter)
        .order_by(
            oem_rank,
            AutoPart.oem_number.asc(),
            Provider.name.asc(),
            ProviderPriceListConfig.name_price.asc().nullslast(),
            PriceListAutoPartAssociation.price.asc(),
        )
    )
    current_rows = (await session.execute(current_stmt)).mappings().all()
    current_offer_keys = {
        (row['partition_key'], row['oem_number'])
        for row in current_rows
        if row.get('partition_key') is not None
    }

    history_rank = func.row_number().over(
        partition_by=(
            PriceListAutoPartAssociation.autopart_id,
            func.coalesce(
                PriceList.provider_config_id, PriceList.provider_id
            ),
        ),
        order_by=(PriceList.date.desc(), PriceList.id.desc()),
    ).label('history_rn')

    historical_subq = (
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
            func.coalesce(
                PriceList.provider_config_id, PriceList.provider_id
            ).label('partition_key'),
            history_rank,
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
        .where(PriceList.is_active.is_(True))
        .where(oem_filter)
        .subquery()
    )

    history_stmt = (
        select(historical_subq)
        .where(historical_subq.c.history_rn == 1)
        .order_by(
            case(
                (historical_subq.c.oem_number == normalized_oem, 0),
                (
                    historical_subq.c.oem_number.ilike(startswith_pattern),
                    1,
                ),
                else_=2,
            ),
            historical_subq.c.oem_number.asc(),
            historical_subq.c.provider_name.asc(),
            historical_subq.c.provider_config_name.asc().nullslast(),
            historical_subq.c.pricelist_date.desc(),
            historical_subq.c.price.asc(),
        )
    )
    historical_rows = (await session.execute(history_stmt)).mappings().all()

    offers = []
    for row in current_rows:
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

    historical_offers = []
    for row in historical_rows:
        if (
            row.get('partition_key'),
            row.get('oem_number'),
        ) in current_offer_keys:
            continue
        price_value = row.get('price')
        historical_offers.append(
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

    # ── Check nomenclature ──────────────────────────────────────────────────
    in_nomenclature = False
    nomenclature_autopart_id = None
    nomenclature_brand_name = None
    nomenclature_name = None

    if not partial:
        nom_stmt = (
            select(AutoPart, Brand.name.label('brand_name'))
            .join(Brand, Brand.id == AutoPart.brand_id)
            .where(AutoPart.oem_number == normalized_oem)
            .limit(1)
        )
        nom_result = (await session.execute(nom_stmt)).mappings().first()
        if nom_result:
            in_nomenclature = True
            nomenclature_autopart_id = nom_result['AutoPart'].id
            nomenclature_brand_name = nom_result['brand_name']
            nomenclature_name = nom_result['AutoPart'].name

    return AutopartOffersResponse(
        oem_number=normalized_oem,
        offers=offers,
        historical_offers=historical_offers,
        in_nomenclature=in_nomenclature,
        nomenclature_autopart_id=nomenclature_autopart_id,
        nomenclature_brand_name=nomenclature_brand_name,
        nomenclature_name=nomenclature_name,
    )


@router.get(
    '/autoparts/lookup/',
    tags=['autopart'],
    summary='Поиск автозапчастей по OEM',
    response_model=list[AutoPartLookupItem],
)
async def lookup_autoparts_by_oem(
    oem: str = Query(..., description='OEM номер запчасти'),
    limit: conint(ge=1, le=100) = 50,
    session: AsyncSession = Depends(get_session),
):
    normalized_oem = preprocess_oem_number(oem)
    stmt = (
        select(
            AutoPart.id.label('id'),
            AutoPart.oem_number.label('oem_number'),
            Brand.id.label('brand_id'),
            Brand.name.label('brand'),
            AutoPart.name.label('name'),
        )
        .select_from(AutoPart)
        .join(Brand, Brand.id == AutoPart.brand_id)
        .where(AutoPart.oem_number == normalized_oem)
        .order_by(Brand.name.asc(), AutoPart.name.asc().nullslast())
        .limit(limit)
    )
    result = await session.execute(stmt)
    rows = result.mappings().all()
    return [AutoPartLookupItem(**row) for row in rows]


@router.get(
    '/autoparts/search/',
    tags=['autopart'],
    summary='Поиск автозапчастей по артикулу',
    response_model=list[AutoPartLookupItem],
)
async def search_autoparts_by_oem(
    q: str = Query(..., min_length=1, description='Часть артикула OEM'),
    limit: conint(ge=1, le=100) = 50,
    session: AsyncSession = Depends(get_session),
):
    normalized_oem = preprocess_oem_number(q)
    if not normalized_oem:
        return []

    startswith_pattern = f'{normalized_oem}%'
    contains_pattern = f'%{normalized_oem}%'
    stmt = (
        select(
            AutoPart.id.label('id'),
            AutoPart.oem_number.label('oem_number'),
            Brand.id.label('brand_id'),
            Brand.name.label('brand'),
            AutoPart.name.label('name'),
        )
        .select_from(AutoPart)
        .join(Brand, Brand.id == AutoPart.brand_id)
        .where(AutoPart.oem_number.ilike(contains_pattern))
        .order_by(
            case(
                (AutoPart.oem_number == normalized_oem, 0),
                (AutoPart.oem_number.ilike(startswith_pattern), 1),
                else_=2,
            ),
            AutoPart.oem_number.asc(),
            Brand.name.asc(),
            AutoPart.id.asc(),
        )
        .limit(limit)
    )
    result = await session.execute(stmt)
    rows = result.mappings().all()
    return [AutoPartLookupItem(**row) for row in rows]


@router.get(
    '/autoparts/{autopart_id:int}/',
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
                    with rar.open(file_in_rar) as inner_file:
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
    '/autoparts/{autopart_id:int}/',
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
    '/warehouses/',
    status_code=status.HTTP_201_CREATED,
    summary='Создание склада',
    tags=['warehouse'],
    response_model=WarehouseOut,
)
async def create_warehouse(
    warehouse_in: WarehouseCreate,
    session: AsyncSession = Depends(get_session),
):
    payload = warehouse_in.model_dump(exclude_unset=True)
    payload['name'] = str(payload.get('name') or '').strip()
    if not payload['name']:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail='Название склада не может быть пустым',
        )
    existing = (
        await session.execute(
            select(Warehouse).where(Warehouse.name == payload['name'])
        )
    ).scalar_one_or_none()
    if existing is not None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f'Склад с названием {payload["name"]} уже существует.',
        )
    warehouse = Warehouse(**payload)
    session.add(warehouse)
    await session.commit()
    await session.refresh(warehouse)
    return _warehouse_to_out(warehouse)


@router.get(
    '/warehouses/',
    summary='Получение всех складов',
    tags=['warehouse'],
    status_code=status.HTTP_200_OK,
    response_model=List[WarehouseOut],
)
async def list_warehouses(
    include_inactive: bool = Query(False),
    session: AsyncSession = Depends(get_session),
):
    warehouses = await crud_warehouse.get_multi(
        session,
        include_inactive=include_inactive,
    )
    return [_warehouse_to_out(warehouse) for warehouse in warehouses]


@router.get(
    '/warehouses/{warehouse_id}/',
    summary='Получение склада по ID',
    tags=['warehouse'],
    status_code=status.HTTP_200_OK,
    response_model=WarehouseOut,
)
async def get_warehouse(
    warehouse_id: int,
    session: AsyncSession = Depends(get_session),
):
    warehouse = await crud_warehouse.get_by_id(warehouse_id, session)
    if warehouse is None:
        raise HTTPException(status_code=404, detail='Склад не найден')
    return _warehouse_to_out(warehouse)


@router.patch(
    '/warehouses/{warehouse_id}/',
    summary='Обновление склада',
    tags=['warehouse'],
    status_code=status.HTTP_200_OK,
    response_model=WarehouseOut,
)
async def update_warehouse(
    warehouse_id: int,
    warehouse_in: WarehouseUpdate,
    session: AsyncSession = Depends(get_session),
):
    warehouse = await crud_warehouse.get_by_id(warehouse_id, session)
    if warehouse is None:
        raise HTTPException(status_code=404, detail='Склад не найден')
    if warehouse_in.name is not None:
        new_name = str(warehouse_in.name).strip()
        if not new_name:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail='Название склада не может быть пустым',
            )
        exists = (
            await session.execute(
                select(Warehouse).where(
                    Warehouse.name == new_name,
                    Warehouse.id != warehouse_id,
                )
            )
        ).scalar_one_or_none()
        if exists is not None:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f'Склад с названием {new_name} уже существует.',
            )
        warehouse.name = new_name
    if warehouse_in.comment is not None:
        warehouse.comment = warehouse_in.comment
    if warehouse_in.is_active is not None:
        warehouse.is_active = warehouse_in.is_active
    await session.commit()
    await session.refresh(warehouse)
    return _warehouse_to_out(warehouse)


@router.post(
    '/storage/',
    status_code=status.HTTP_201_CREATED,
    summary='Создание местохранения',
    tags=['storage'],
    response_model=StorageLocationResponse,
)
async def create_storage_location(
    storage_in: StorageLocationCreate,
    session: AsyncSession = Depends(get_session),
):
    storage_in.name = await change_storage_name(storage_in.name)
    if storage_in.warehouse_id is None:
        default_warehouse = await ensure_default_warehouse(session)
        storage_in.warehouse_id = default_warehouse.id
    else:
        warehouse = await crud_warehouse.get_by_id(
            storage_in.warehouse_id,
            session
        )
        if warehouse is None:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail='Склад для места хранения не найден',
            )
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
        storage = await crud_storage.get_storage_location_by_id(
            storage.id,
            session,
        )
        return _storage_to_response(storage)
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
    warehouse_id: Optional[int] = Query(default=None),
    include_system: bool = Query(default=False),
):
    storages = await crud_storage.get_multi(
        session,
        skip=skip,
        limit=limit,
        warehouse_id=warehouse_id,
        include_system=include_system,
    )
    return [_storage_to_response(storage) for storage in storages]


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
    return _storage_to_response(storage)


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
    if storage_old.system_code:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail='Системное место хранения нельзя редактировать вручную.',
        )
    try:
        if storage_in.warehouse_id is not None:
            warehouse = await crud_warehouse.get_by_id(
                storage_in.warehouse_id,
                session,
            )
            if warehouse is None:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail='Склад для места хранения не найден',
                )
        if storage_in.name is not None and len(storage_in.name) <= 2:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f'Storage\'s name ({storage_in.name}) is short',
            )
        updated_storage = await crud_storage.update(
            db_obj=storage_old, obj_in=storage_in, session=session
        )
        updated_storage = await crud_storage.get_storage_location_by_id(
            storage_id,
            session,
        )
    except IntegrityError as e:
        await session.rollback()
        raise HTTPException(
            status_code=400,
            detail=f'Storage with name {storage_in.name} already exists.',
        ) from e
    return _storage_to_response(updated_storage)


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
    if storages_data:
        default_warehouse = await ensure_default_warehouse(session)
        for storage in storages_data:
            if storage.warehouse_id is None:
                storage.warehouse_id = default_warehouse.id
    created_locations = await crud_storage.create_locations(
        locations_data=storages_data, session=session
    )
    created_ids = [location.id for location in created_locations]
    reloaded_locations = []
    for location_id in created_ids:
        location = await crud_storage.get_storage_location_by_id(
            location_id,
            session,
        )
        if location is not None:
            reloaded_locations.append(location)
    return [
        _storage_to_response(location)
        for location in reloaded_locations
    ]


@router.delete(
    '/storage/{storage_id}/',
    summary='Удалить место хранения (только если нет запчастей)',
    tags=['storage'],
    status_code=status.HTTP_204_NO_CONTENT,
)
async def delete_storage_location(
    storage_id: int,
    session: AsyncSession = Depends(get_session),
):
    result = await session.execute(
        select(StorageLocation)
        .where(StorageLocation.id == storage_id)
        .options(selectinload(StorageLocation.autoparts))
    )
    storage = result.scalar_one_or_none()
    if not storage:
        raise HTTPException(
            status_code=404,
            detail='Место хранения не найдено'
        )
    if storage.system_code:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail='Системное место хранения нельзя удалить.',
        )
    if storage.autoparts:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=(
                f'Нельзя удалить: в месте хранения «{storage.name}» '
                f'находится {len(storage.autoparts)} запчасть(-ей). '
                'Сначала переместите товары в другое место.'
            ),
        )
    await session.delete(storage)
    await session.commit()


@router.get(
    '/storage/{storage_id}/autoparts/',
    summary='Список запчастей в месте хранения (с количеством по ячейке)',
    tags=['storage'],
    status_code=status.HTTP_200_OK,
)
async def get_storage_autoparts(
    storage_id: int,
    session: AsyncSession = Depends(get_session),
):
    """Return StockByLocation records for the given storage location."""
    from dz_fastapi.models.inventory import StockByLocation

    # avoid circular import

    storage = await session.get(StorageLocation, storage_id)
    if not storage:
        raise HTTPException(
            status_code=404,
            detail='Место хранения не найдено'
        )

    rows = (await session.execute(
        select(StockByLocation)
        .where(StockByLocation.storage_location_id == storage_id)
        .options(
            selectinload(StockByLocation.autopart).selectinload(
                AutoPart.brand
            ),
            selectinload(StockByLocation.storage_location),
        )
        .order_by(StockByLocation.autopart_id)
    )).scalars().all()

    return [
        {
            'sbl_id': r.id,
            'autopart_id': r.autopart_id,
            'oem_number': r.autopart.oem_number if r.autopart else None,
            'name': r.autopart.name if r.autopart else None,
            'brand_name': r.autopart.brand.name if (
                    r.autopart and r.autopart.brand
            ) else '',
            'stock_quantity': r.quantity,
            'updated_at': r.updated_at.isoformat() if r.updated_at else None,
        }
        for r in rows
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
    actual_df, step_df, stockout_df = prepare_price_history_plot_data(
        df, finish_dt
    )

    fig = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.08,
        row_heights=[0.66, 0.34],
        subplot_titles=(
            'Цена по поставщикам',
            'Остаток по поставщикам',
        ),
    )

    palette = qualitative.Plotly
    provider_names = sorted(step_df['provider'].dropna().unique())
    stockout_legend_added = False

    for index, provider_name in enumerate(provider_names):
        color = palette[index % len(palette)]
        provider_step_df = step_df[step_df['provider'] == provider_name]
        provider_actual_df = actual_df[actual_df['provider'] == provider_name]
        provider_stockout_df = stockout_df[
            stockout_df['provider'] == provider_name
        ]

        fig.add_trace(
            go.Scatter(
                x=provider_step_df['created_at'],
                y=provider_step_df['price'],
                mode='lines',
                name=provider_name,
                legendgroup=provider_name,
                line={
                    'color': color,
                    'width': 2.5,
                    'shape': 'hv',
                },
                customdata=provider_step_df[['quantity']].to_numpy(),
                hovertemplate=(
                    '<b>%{fullData.name}</b><br>'
                    'Дата: %{x|%d.%m.%Y %H:%M}<br>'
                    'Цена: %{y:.2f}<br>'
                    'Остаток: %{customdata[0]:.0f}<extra></extra>'
                ),
            ),
            row=1,
            col=1,
        )
        fig.add_trace(
            go.Scatter(
                x=provider_actual_df['created_at'],
                y=provider_actual_df['price'],
                mode='markers',
                name=provider_name,
                showlegend=False,
                legendgroup=provider_name,
                marker={
                    'color': color,
                    'size': 8,
                    'line': {'color': '#ffffff', 'width': 1},
                },
                customdata=provider_actual_df[['quantity']].to_numpy(),
                hovertemplate=(
                    '<b>%{fullData.name}</b><br>'
                    'Дата обновления: %{x|%d.%m.%Y %H:%M}<br>'
                    'Цена: %{y:.2f}<br>'
                    'Остаток: %{customdata[0]:.0f}<extra></extra>'
                ),
            ),
            row=1,
            col=1,
        )
        fig.add_trace(
            go.Scatter(
                x=provider_step_df['created_at'],
                y=provider_step_df['quantity'],
                mode='lines',
                showlegend=False,
                legendgroup=provider_name,
                line={
                    'color': color,
                    'width': 2,
                    'shape': 'hv',
                },
                customdata=provider_step_df[['price']].to_numpy(),
                hovertemplate=(
                    '<b>%{fullData.legendgroup}</b><br>'
                    'Дата: %{x|%d.%m.%Y %H:%M}<br>'
                    'Остаток: %{y:.0f}<br>'
                    'Цена: %{customdata[0]:.2f}<extra></extra>'
                ),
            ),
            row=2,
            col=1,
        )
        fig.add_trace(
            go.Scatter(
                x=provider_actual_df['created_at'],
                y=provider_actual_df['quantity'],
                mode='markers',
                name=provider_name,
                showlegend=False,
                legendgroup=provider_name,
                marker={
                    'color': color,
                    'size': 7,
                    'line': {'color': '#ffffff', 'width': 1},
                },
                customdata=provider_actual_df[['price']].to_numpy(),
                hovertemplate=(
                    '<b>%{fullData.name}</b><br>'
                    'Дата обновления: %{x|%d.%m.%Y %H:%M}<br>'
                    'Остаток: %{y:.0f}<br>'
                    'Цена: %{customdata[0]:.2f}<extra></extra>'
                ),
            ),
            row=2,
            col=1,
        )
        if not provider_stockout_df.empty:
            fig.add_trace(
                go.Scatter(
                    x=provider_stockout_df['created_at'],
                    y=provider_stockout_df['price'],
                    mode='markers',
                    name=provider_name,
                    showlegend=False,
                    legendgroup=provider_name,
                    marker={
                        'color': '#dc2626',
                        'size': 10,
                        'symbol': 'x',
                        'line': {'width': 2},
                    },
                    hovertemplate=(
                        '<b>%{fullData.name}</b><br>'
                        'Дата: %{x|%d.%m.%Y %H:%M}<br>'
                        'Цена: %{y:.2f}<br>'
                        'Деталь закончилась<extra></extra>'
                    ),
                ),
                row=1,
                col=1,
            )
            fig.add_trace(
                go.Scatter(
                    x=provider_stockout_df['created_at'],
                    y=provider_stockout_df['quantity'],
                    mode='markers',
                    name='Нет в наличии',
                    showlegend=not stockout_legend_added,
                    marker={
                        'color': '#dc2626',
                        'size': 10,
                        'symbol': 'x',
                        'line': {'width': 2},
                    },
                    hovertemplate=(
                        '<b>%{fullData.name}</b><br>'
                        'Дата: %{x|%d.%m.%Y %H:%M}<br>'
                        'Деталь закончилась<extra></extra>'
                    ),
                ),
                row=2,
                col=1,
            )
            stockout_legend_added = True

    fig.add_hline(
        y=0,
        line_width=1,
        line_dash='dot',
        line_color='#94a3b8',
        row=2,
        col=1,
    )
    fig.add_annotation(
        text=(
            'Горизонтальные линии показывают, что цена и остаток'
            ' не менялись до следующего обновления.'
        ),
        xref='paper',
        yref='paper',
        x=0,
        y=1.11,
        showarrow=False,
        font={'size': 12, 'color': '#6b7280'},
        align='left',
    )
    fig.update_xaxes(
        showspikes=True,
        spikecolor='#94a3b8',
        spikethickness=1,
        spikesnap='cursor',
        spikemode='across',
    )
    fig.update_yaxes(title_text='Цена', row=1, col=1)
    fig.update_yaxes(title_text='Остаток', row=2, col=1)
    fig.update_layout(
        title=f'История цены и наличия по OEM {normalized_oem}',
        hovermode='x unified',
        template='plotly_white',
        height=760,
        legend_title_text='Поставщик',
        margin={'t': 110, 'r': 24, 'b': 40, 'l': 56},
    )

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


# ═══════════════════════════════════════════════════════════════════════════════
# NOMENCLATURE CATALOG
# ═══════════════════════════════════════════════════════════════════════════════

@router.get(
    '/autoparts/catalog/',
    tags=['autopart', 'catalog'],
    summary='Каталог номенклатуры (постраничный)',
    response_model=AutoPartCatalogResponse,
)
async def get_autoparts_catalog(
    q_oem: Optional[str] = Query(
        None, description='Поиск по OEM-номеру (от 3 символов)'
    ),
    q_name: Optional[str] = Query(
        None, description='Поиск по наименованию (от 3 символов)'
    ),
    q_brand: Optional[str] = Query(
        None, description='Поиск по бренду (от 3 символов)'
    ),
    offset: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=200),
    session: AsyncSession = Depends(get_session),
):
    items, total = await crud_autopart.list_for_catalog(
        session,
        q_oem=q_oem,
        q_name=q_name,
        q_brand=q_brand,
        offset=offset,
        limit=limit,
    )
    # Get own pricelist IDs for stock lookup
    own_pl_stmt = (
        select(PriceList.id)
        .join(Provider, Provider.id == PriceList.provider_id)
        .where(Provider.is_own_price.is_(True), PriceList.is_active.is_(True))
    )
    own_pl_ids = (await session.execute(own_pl_stmt)).scalars().all()

    # Fetch stock quantities for fetched autoparts
    stock_map: dict[int, int] = {}
    if own_pl_ids and items:
        ap_ids = [ap.id for ap in items]
        stock_stmt = (
            select(
                PriceListAutoPartAssociation.autopart_id,
                func.sum(PriceListAutoPartAssociation.quantity).label('qty'),
            )
            .where(
                PriceListAutoPartAssociation.pricelist_id.in_(own_pl_ids),
                PriceListAutoPartAssociation.autopart_id.in_(ap_ids),
            )
            .group_by(PriceListAutoPartAssociation.autopart_id)
        )
        for row in (await session.execute(stock_stmt)).mappings().all():
            stock_map[row['autopart_id']] = row['qty'] or 0

    catalog_items = []
    for ap in items:
        catalog_items.append(
            AutoPartCatalogItem(
                id=ap.id,
                brand_id=ap.brand_id,
                brand_name=ap.brand.name if ap.brand else None,
                oem_number=ap.oem_number,
                name=ap.name,
                purchase_price=float(
                    ap.purchase_price
                ) if ap.purchase_price else None,
                retail_price=float(
                    ap.retail_price
                ) if ap.retail_price else None,
                wholesale_price=float(
                    ap.wholesale_price
                ) if ap.wholesale_price else None,
                minimum_balance=ap.minimum_balance,
                min_balance_auto=ap.min_balance_auto,
                barcode=ap.barcode,
                honest_sign_category=ap.honest_sign_category,
                applicability=ap.applicability,
                categories=ap.categories,
                storage_locations=ap.storage_locations,
                stock_quantity=stock_map.get(ap.id, 0),
            )
        )
    return AutoPartCatalogResponse(
        items=catalog_items,
        total=total,
        offset=offset,
        limit=limit
    )


@router.get(
    '/autoparts/{autopart_id:int}/detail/',
    tags=['autopart', 'catalog'],
    summary='Полная карточка запчасти с кросс-номерами',
    response_model=AutoPartDetailResponse,
)
async def get_autopart_detail(
    autopart_id: int,
    session: AsyncSession = Depends(get_session),
):
    ap = await crud_autopart.get_detail_with_crosses(session, autopart_id)
    if not ap:
        raise HTTPException(status_code=404, detail='Запчасть не найдена')

    crosses_stmt = (
        select(AutoPartCross)
        .where(AutoPartCross.source_autopart_id == autopart_id)
        .options(selectinload(AutoPartCross.cross_brand))
        .order_by(AutoPartCross.priority.asc())
    )
    crosses_result = await session.execute(crosses_stmt)
    crosses_raw = crosses_result.scalars().all()
    crosses = [
        CrossOut(
            id=c.id,
            cross_brand_id=c.cross_brand_id,
            cross_brand_name=c.cross_brand.name if c.cross_brand else None,
            cross_oem_number=c.cross_oem_number,
            cross_autopart_id=c.cross_autopart_id,
            priority=c.priority,
            comment=c.comment,
        )
        for c in crosses_raw
    ]

    return AutoPartDetailResponse(
        id=ap.id,
        brand_id=ap.brand_id,
        brand_name=ap.brand.name if ap.brand else None,
        oem_number=ap.oem_number,
        name=ap.name,
        description=ap.description,
        width=ap.width,
        height=ap.height,
        length=ap.length,
        weight=ap.weight,
        purchase_price=float(
            ap.purchase_price
        ) if ap.purchase_price else None,
        retail_price=float(ap.retail_price) if ap.retail_price else None,
        wholesale_price=float(
            ap.wholesale_price
        ) if ap.wholesale_price else None,
        multiplicity=ap.multiplicity,
        minimum_balance=ap.minimum_balance,
        min_balance_auto=ap.min_balance_auto,
        min_balance_user=ap.min_balance_user,
        comment=ap.comment,
        barcode=ap.barcode,
        honest_sign_category=ap.honest_sign_category,
        applicability=ap.applicability,
        categories=ap.categories,
        storage_locations=ap.storage_locations,
        crosses=crosses,
        honest_sign_categories=[
            HonestSignCategoryOut.model_validate(h)
            for h in (ap.honest_sign_categories or [])
        ],
        applicability_nodes=[
            ApplicabilityNodeFlatOut.model_validate(n)
            for n in (ap.applicability_nodes or [])
        ],
    )


@router.patch(
    '/autoparts/{autopart_id:int}/update/',
    tags=['autopart', 'catalog'],
    summary='Обновление карточки запчасти',
    response_model=AutoPartDetailResponse,
)
async def update_autopart_catalog(
    autopart_id: int,
    payload: AutoPartUpdate,
    session: AsyncSession = Depends(get_session),
):
    ap = await crud_autopart.get_autopart_by_id(
        session=session,
        autopart_id=autopart_id
    )
    if not ap:
        raise HTTPException(status_code=404, detail='Запчасть не найдена')
    data = payload.model_dump(exclude_unset=True)
    ap = await crud_autopart.update_full(session, ap, data)
    # Re-fetch full detail
    return await get_autopart_detail(autopart_id=ap.id, session=session)


@router.post(
    '/autoparts/',
    tags=['autopart', 'catalog'],
    summary='Создание запчасти через каталог',
    response_model=AutoPartDetailResponse,
    status_code=status.HTTP_201_CREATED,
    include_in_schema=False,
    # already exists as create_autopart_endpoint above
)
async def create_autopart_catalog(
    payload: AutoPartCreate,
    session: AsyncSession = Depends(get_session),
):
    """Duplicate of POST /autoparts/ that returns detail + crosses."""
    brand_db = await brand_exists(payload.brand_id, session)
    ap = await crud_autopart.create_autopart(payload, brand_db, session)
    return await get_autopart_detail(autopart_id=ap.id, session=session)


# ─── Cross-numbers endpoints ────────────────────────────────────────────────

@router.get(
    '/autoparts/{autopart_id:int}/crosses/',
    tags=['autopart', 'catalog'],
    summary='Список кросс-номеров запчасти',
    response_model=list[CrossOut],
)
async def list_autopart_crosses(
    autopart_id: int,
    session: AsyncSession = Depends(get_session),
):
    stmt = (
        select(AutoPartCross)
        .where(AutoPartCross.source_autopart_id == autopart_id)
        .options(selectinload(AutoPartCross.cross_brand))
        .order_by(AutoPartCross.priority.asc())
    )
    result = await session.execute(stmt)
    crosses = result.scalars().all()
    return [
        CrossOut(
            id=c.id,
            cross_brand_id=c.cross_brand_id,
            cross_brand_name=c.cross_brand.name if c.cross_brand else None,
            cross_oem_number=c.cross_oem_number,
            cross_autopart_id=c.cross_autopart_id,
            priority=c.priority,
            comment=c.comment,
        )
        for c in crosses
    ]


@router.post(
    '/autoparts/{autopart_id:int}/crosses/',
    tags=['autopart', 'catalog'],
    summary='Добавить кросс-номер',
    response_model=CrossOut,
    status_code=status.HTTP_201_CREATED,
)
async def add_autopart_cross(
    autopart_id: int,
    payload: CrossCreate,
    session: AsyncSession = Depends(get_session),
):
    # Check autopart exists
    ap = await session.get(AutoPart, autopart_id)
    if not ap:
        raise HTTPException(status_code=404, detail='Запчасть не найдена')
    cross_oem = preprocess_oem_number(payload.cross_oem_number)
    cross = AutoPartCross(
        source_autopart_id=autopart_id,
        cross_brand_id=payload.cross_brand_id,
        cross_oem_number=cross_oem,
        priority=payload.priority,
        comment=payload.comment,
    )
    # Try to find matching autopart for cross_autopart_id
    match_stmt = select(AutoPart).where(
        AutoPart.brand_id == payload.cross_brand_id,
        AutoPart.oem_number == cross_oem,
    )
    match = (await session.execute(match_stmt)).scalar_one_or_none()
    if match:
        cross.cross_autopart_id = match.id
    session.add(cross)
    try:
        await session.commit()
        await session.refresh(cross)
    except IntegrityError:
        await session.rollback()
        raise HTTPException(
            status_code=409,
            detail='Такой кросс-номер уже существует'
        )
    brand_result = await session.get(Brand, cross.cross_brand_id)
    return CrossOut(
        id=cross.id,
        cross_brand_id=cross.cross_brand_id,
        cross_brand_name=brand_result.name if brand_result else None,
        cross_oem_number=cross.cross_oem_number,
        cross_autopart_id=cross.cross_autopart_id,
        priority=cross.priority,
        comment=cross.comment,
    )


@router.delete(
    '/autoparts/crosses/{cross_id:int}',
    tags=['autopart', 'catalog'],
    summary='Удалить кросс-номер',
    status_code=status.HTTP_204_NO_CONTENT,
)
async def delete_autopart_cross(
    cross_id: int,
    session: AsyncSession = Depends(get_session),
):
    cross = await session.get(AutoPartCross, cross_id)
    if not cross:
        raise HTTPException(status_code=404, detail='Кросс-номер не найден')
    await session.delete(cross)
    await session.commit()


@router.get(
    '/autoparts/storage-locations/',
    tags=['autopart', 'catalog'],
    summary='Список мест хранения',
    response_model=list[StorageLocationOut],
)
async def list_storage_locations(
    session: AsyncSession = Depends(get_session),
):
    result = await session.execute(
        select(StorageLocation).order_by(StorageLocation.name)
    )
    return [
        StorageLocationOut(
            id=s.id,
            name=s.name
        ) for s in result.scalars().all()
    ]


# ═══════════════════════════════════════════════════════════════════════════════
# ЧЕСТНЫЙ ЗНАК (HonestSignCategory)
# ═══════════════════════════════════════════════════════════════════════════════

@router.get(
    '/honest-sign-categories/',
    tags=['nomenclature'],
    summary='Список категорий Честного знака',
    response_model=list[HonestSignCategoryOut],
)
async def list_honest_sign_categories(
    session: AsyncSession = Depends(get_session),
):
    result = await session.execute(
        select(HonestSignCategory).order_by(HonestSignCategory.name)
    )
    return result.scalars().all()


@router.post(
    '/honest-sign-categories/',
    tags=['nomenclature'],
    summary='Создать категорию Честного знака',
    response_model=HonestSignCategoryOut,
    status_code=status.HTTP_201_CREATED,
)
async def create_honest_sign_category(
    payload: HonestSignCategoryCreate,
    session: AsyncSession = Depends(get_session),
):
    existing = (await session.execute(
        select(HonestSignCategory).where(
            HonestSignCategory.name == payload.name
        )
    )).scalar_one_or_none()
    if existing:
        raise HTTPException(
            status_code=409,
            detail=f'Категория «{payload.name}» уже существует'
        )
    obj = HonestSignCategory(**payload.model_dump())
    session.add(obj)
    await session.commit()
    await session.refresh(obj)
    return obj


@router.post(
    '/autoparts/{autopart_id:int}/honest-sign-categories/',
    tags=['nomenclature'],
    summary='Назначить категории ЧЗ для запчасти',
    response_model=list[HonestSignCategoryOut],
)
async def assign_honest_sign_categories(
    autopart_id: int,
    category_ids: List[int] = Body(..., description='Список ID категорий ЧЗ'),
    session: AsyncSession = Depends(get_session),
):
    # Load ap with the honest_sign_categories relationship already populated
    ap_result = await session.execute(
        select(AutoPart)
        .where(AutoPart.id == autopart_id)
        .options(selectinload(AutoPart.honest_sign_categories))
    )
    ap = ap_result.scalar_one_or_none()
    if not ap:
        raise HTTPException(status_code=404, detail='Запчасть не найдена')
    cats = list((await session.execute(
        select(HonestSignCategory).where(
            HonestSignCategory.id.in_(category_ids)
        )
    )).scalars().all())
    ap.honest_sign_categories = cats
    await session.commit()
    # Return cats we already have — avoids post-commit lazy-load
    return [HonestSignCategoryOut.model_validate(c) for c in cats]


# ═══════════════════════════════════════════════════════════════════════════════
# ПРИМЕНИМОСТЬ (ApplicabilityNode)
# ═══════════════════════════════════════════════════════════════════════════════

@router.get(
    '/applicability-nodes/',
    tags=['nomenclature'],
    summary='Дерево применимости (только корневые узлы с детьми)',
    response_model=list[ApplicabilityNodeOut],
)
async def list_applicability_nodes(
    parent_id: Optional[int] = Query(
        None,
        description='ID родительского узла (None = корень)'
    ),
    session: AsyncSession = Depends(get_session),
):
    stmt = (
        select(ApplicabilityNode)
        .where(
            ApplicabilityNode.parent_id == parent_id
            if parent_id is not None
            else ApplicabilityNode.parent_id.is_(None)
        )
        .options(selectinload(ApplicabilityNode.children))
        .order_by(ApplicabilityNode.name)
    )
    result = await session.execute(stmt)
    return result.scalars().all()


@router.get(
    '/applicability-nodes/all/',
    tags=['nomenclature'],
    summary='Все узлы применимости плоским списком '
            '(с parent_id для построения дерева на фронте)',
    response_model=list[ApplicabilityNodeFlatOut],
)
async def list_all_applicability_nodes(
    session: AsyncSession = Depends(get_session),
):
    result = await session.execute(
        select(ApplicabilityNode)
        .order_by(
            ApplicabilityNode.parent_id.asc().nullsfirst(),
            ApplicabilityNode.name,
        )
    )
    return [
        ApplicabilityNodeFlatOut.model_validate(
            n
        ) for n in result.scalars().all()
    ]


@router.post(
    '/applicability-nodes/',
    tags=['nomenclature'],
    summary='Создать узел применимости',
    response_model=ApplicabilityNodeFlatOut,
    status_code=status.HTTP_201_CREATED,
)
async def create_applicability_node(
    payload: ApplicabilityNodeCreate,
    session: AsyncSession = Depends(get_session),
):
    if payload.parent_id is not None:
        parent = await session.get(ApplicabilityNode, payload.parent_id)
        if not parent:
            raise HTTPException(
                status_code=404,
                detail=f'Родительский узел #{payload.parent_id} не найден'
            )
    obj = ApplicabilityNode(**payload.model_dump())
    session.add(obj)
    await session.commit()
    await session.refresh(obj)
    return ApplicabilityNodeFlatOut.model_validate(obj)


@router.post(
    '/autoparts/{autopart_id:int}/applicability-nodes/',
    tags=['nomenclature'],
    summary='Назначить узлы применимости для запчасти',
    response_model=list[ApplicabilityNodeFlatOut],
)
async def assign_applicability_nodes(
    autopart_id: int,
    node_ids: List[int] = Body(
        ...,
        description='Список ID узлов применимости'
    ),
    session: AsyncSession = Depends(get_session),
):
    # Load ap with the applicability_nodes relationship already populated
    ap_result = await session.execute(
        select(AutoPart)
        .where(AutoPart.id == autopart_id)
        .options(selectinload(AutoPart.applicability_nodes))
    )
    ap = ap_result.scalar_one_or_none()
    if not ap:
        raise HTTPException(status_code=404, detail='Запчасть не найдена')
    nodes = list((await session.execute(
        select(ApplicabilityNode).where(ApplicabilityNode.id.in_(node_ids))
    )).scalars().all())
    ap.applicability_nodes = nodes
    await session.commit()
    # Return nodes we already have — avoids post-commit lazy-load
    return [ApplicabilityNodeFlatOut.model_validate(n) for n in nodes]
