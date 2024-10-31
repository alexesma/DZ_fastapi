from typing import List

from fastapi import APIRouter, Depends, Body, HTTPException, status
from pydantic import conint
from sqlalchemy.orm import selectinload

from dz_fastapi.api.validators import brand_exists, change_string, change_storage_name
from dz_fastapi.crud.autopart import crud_autopart, crud_category, crud_storage
from dz_fastapi.schemas.autopart import (
    AutoPartCreate,
    AutoPartUpdate,
    AutoPartResponse,
    CategoryResponse,
    CategoryCreate,
    CategoryUpdate,
    StorageLocationCreate,
    StorageLocationUpdate,
    StorageLocationResponse
)
from dz_fastapi.models.autopart import Category, StorageLocation
from dz_fastapi.core.db import get_session
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

router = APIRouter()


@router.post(
    '/autoparts/',
    tags=['autopart'],
    status_code=status.HTTP_201_CREATED,
    summary='Создание автозапчасти',
    response_model=AutoPartResponse
)
async def create_autopart_endpoint(
        autopart: AutoPartCreate,
        session: AsyncSession = Depends(get_session)
):
    brand_db = await brand_exists(autopart.brand_id, session)
    autopart = await crud_autopart.create_autopart(autopart, brand_db, session)
    return await crud_autopart.get_autopart_by_id(session=session, autopart_id=autopart.id)


@router.get(
    '/autoparts/{autopart_id}/',
    tags=['autopart'],
    summary='Получение автозапчасти по ID',
    response_model=AutoPartResponse
)
async def get_autopart_endpoint(
        autopart_id: int,
        session: AsyncSession = Depends(get_session)
):
    autopart = await crud_autopart.get_autopart_by_id(autopart_id=autopart_id, session=session)
    if not autopart:
        raise HTTPException(status_code=404, detail='Autopart not found')
    return autopart


@router.get(
    '/autoparts/',
    tags=['autopart'],
    summary='Получение всех автозапчастей',
    response_model=List[AutoPartResponse]
)
async def get_all_autoparts(
        skip: int = 0,
        limit: int = 100,
        session: AsyncSession = Depends(get_session)
):
    return await crud_autopart.get_multi(session=session, skip=skip, limit=limit)


@router.patch(
    '/autoparts/{autopart_id}/',
    tags=['autopart'],
    summary='Обновление автозапчасти',
    response_model=AutoPartResponse
)
async def update_autopart(
        autopart_id: int,
        autopart: AutoPartUpdate = Body(...),
        session: AsyncSession = Depends(get_session)
):
    autopart_db = await crud_autopart.get_autopart_by_id(
        autopart_id=autopart_id,
        session=session
    )
    update_data = autopart.model_dump(exclude_unset=True)
    if autopart_db is None:
        raise HTTPException(status_code=404, detail="AutoPart not found")
    if 'brand_id' not in update_data or update_data['brand_id'] is None:
        update_data['brand_id'] = autopart_db.brand_id
    else:
        await brand_exists(update_data['brand_id'], session)
    if 'name' not in update_data or update_data['name'] is None:
        update_data['name'] = autopart_db.name

    if 'oem_number' not in update_data or update_data['oem_number'] is None:
        update_data['oem_number'] = autopart_db.oem_number
    updated_autopart = await crud_autopart.update(
        db_obj=autopart_db,
        obj_in=autopart,
        session=session
    )
    return updated_autopart


@router.post(
    '/categories/',
    tags=['category'],
    summary='Создание категории',
    response_model=CategoryResponse,
    status_code=status.HTTP_201_CREATED
)
async def create_category(
    category_in: CategoryCreate,
    session: AsyncSession = Depends(get_session)
):
    try:
        result = await session.execute(
            select(Category).where(Category.name == category_in.name)
        )
        existing_category = result.scalar_one_or_none()

        if existing_category:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Category with name '{category_in.name}' already exists.",
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
            detail="An error occurred while creating the category.",
        ) from error


@router.get(
    '/categories/',
    tags=['category'],
    summary='Получение всех категорий',
    response_model=list[CategoryResponse]
)
async def get_categories(
        skip: conint(ge=0) = 0,
        limit: conint(ge=1) = 100,
        session: AsyncSession = Depends(get_session)
):
    categories = await crud_category.get_multi(
        session,
        skip=skip,
        limit=limit
    )
    return categories


@router.get(
    '/categories/{category_id}/',
    tags=['category'],
    summary='Получение категории по ID',
    response_model=CategoryResponse
)
async def get_category(
    category_id: int,
    session: AsyncSession = Depends(get_session)
):
    category = await crud_category.get_category_by_id(
        category_id=category_id,
        session=session
    )
    if not category:
        raise HTTPException(status_code=404, detail='Category not found')
    return category


@router.patch(
    '/categories/{category_id}/',
    tags=['category'],
    summary='Обновление категории',
    response_model=CategoryResponse
)
async def update_category(
        category_id: int,
        category_in: CategoryUpdate,
        session: AsyncSession = Depends(get_session)
):
    category_old = await crud_category.get_category_by_id(
        category_id=category_id,
        session=session
    )
    if not category_old:
        raise HTTPException(status_code=404, detail="Category not found")
    try:
        updated_category = await crud_category.update(
            db_obj=category_old,
            obj_in=category_in,
            session=session
        )
    except IntegrityError as e:
        await session.rollback()
        raise HTTPException(
            status_code=400,
            detail=f"Category with name '{category_in.name}' already exists."
        ) from e
    return updated_category


@router.post(
    '/storage/',
    status_code=status.HTTP_201_CREATED,
    summary='Создание местохранения',
    tags=['storage'],
    response_model=StorageLocationUpdate
)
async def create_storage_location(
    storage_in: StorageLocationCreate,
    session: AsyncSession = Depends(get_session)
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
            select(StorageLocation).where(StorageLocation.name == storage_in.name)
        )
        existing_storage = result.scalar_one_or_none()

        if existing_storage:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f'Storage with name {storage_in.name} already exists.',
            )
        storage = await crud_storage.create(storage_in, session)
        return storage
    except IntegrityError as error:
        await session.rollback()
        if 'unique constraint' in str(error):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f'Storage with name {storage_in.name} already exists.',
            ) from error
        elif 'check constraint' in str(error):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail='Storage name violates database constraints.'
            ) from error
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="An error occurred while creating the storage.",
            ) from error


@router.get(
    '/storage/',
    summary='Получение всех местохранений',
    tags=['storage'],
    response_model=list[StorageLocationResponse]
)
async def get_storage_locations(
        session: AsyncSession = Depends(get_session),
        skip: int = 0,
        limit: int = 100
):
    storages = await crud_storage.get_multi(session, skip=skip, limit=limit)
    return storages


@router.get(
    '/storage/{storage_id}/',
    summary='Получение местохранения по ID',
    tags=['storage'],
    response_model=StorageLocationResponse
)
async def get_storage_location(
    storage_id: int,
    session: AsyncSession = Depends(get_session)
):
    storage = await crud_storage.get_storage_location_by_id(
        storage_location_id=storage_id,
        session=session
    )
    if not storage:
        raise HTTPException(
            status_code=404,
            detail='Storage location not found'
        )
    return storage


@router.patch(
    '/storage/{storage_id}/',
    summary='Обновление местохранения',
    tags=['storage'],
    response_model=StorageLocationResponse
)
async def update_storage_location(
        storage_id: int,
        storage_in: StorageLocationUpdate,
        session: AsyncSession = Depends(get_session)
):
    storage_old = await crud_storage.get_storage_location_by_id(
        storage_location_id=storage_id,
        session=session
    )
    if not storage_old:
        raise HTTPException(
            status_code=404,
            detail='Storage location not found'
        )
    try:
        if len(storage_in.name) <= 2:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f'Storage\'s name ({storage_in.name}) is short',
            )
        updated_storage = await crud_storage.update(
            db_obj=storage_old,
            obj_in=storage_in,
            session=session
        )
    except IntegrityError as e:
        await session.rollback()
        raise HTTPException(
            status_code=400,
            detail=f"Storage with name '{storage_in.name}' already exists."
        ) from e
    return updated_storage
