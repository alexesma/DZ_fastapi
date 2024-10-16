from typing import List

from fastapi import APIRouter, Depends, Body, HTTPException, status
from sqlalchemy.orm import selectinload

from dz_fastapi.api.validators import brand_exists, change_string
from dz_fastapi.crud.autopart import crud_autopart, crud_category, crud_storage
from dz_fastapi.schemas.autopart import (
    AutoPartCreate,
    AutoPartResponse,
    CategoryResponse,
    CategoryCreate,
    CategoryUpdate,
    StorageLocationCreate,
    StorageLocationUpdate,
    StorageLocationResponse
)
from dz_fastapi.models.autopart import Category
from dz_fastapi.core.db import get_async_session
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.exc import SQLAlchemyError

router = APIRouter()


@router.post(
    '/autoparts/',
    tags=['autopart'],
    response_model=AutoPartResponse
)
async def create_autopart_endpoint(
        autopart: AutoPartCreate,
        session: AsyncSession = Depends(get_async_session)
):
    brand_db = await brand_exists(autopart.brand_id, session)
    autopart = await crud_autopart.create_autopart(autopart, brand_db, session)
    return await crud_autopart.get_autopart_by_id(session=session, autopart_id=autopart.id)


@router.get(
    '/autoparts/{autopart_id}/',
    tags=['autopart'],
    response_model=AutoPartResponse
)
async def get_autopart_endpoint(
        autopart_id: int,
        session: AsyncSession = Depends(get_async_session)
):
    return await crud_autopart.get(obj_id=autopart_id, session=session)


@router.get(
    '/autoparts/',
    tags=['autopart'],
    response_model=List[AutoPartResponse]
)
async def get_all_autoparts(
        skip: int = 0, limit: int = 100,
        session: AsyncSession = Depends(get_async_session)
):
    return await crud_autopart.get_multi(session=session, skip=skip, limit=limit)


@router.patch(
    '/autoparts/{autopart_id}',
    tags=['autopart'],
    response_model=AutoPartResponse
)
async def update_autopart(
        autopart_id: int,
        autopart: AutoPartCreate = Body(...),
        session: AsyncSession = Depends(get_async_session)
):
    autopart_old = await crud_autopart.get(obj_id=autopart_id, session=session)
    if autopart_old is None:
        raise HTTPException(status_code=404, detail="AutoPart not found")
    if autopart.brand_id is not None:
        await brand_exists(autopart.brand_id, session)
    updated_autopart = await crud_autopart.update(db_obj=autopart_old, obj_in=autopart, session=session)
    return updated_autopart


@router.post(
    '/categories/',
    response_model=CategoryResponse,
    status_code=status.HTTP_201_CREATED
)
async def create_category(
    category_in: CategoryCreate,
    session: AsyncSession = Depends(get_async_session)
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
        new_category = Category(**category_in.dict())
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


@router.get('/categories/', response_model=list[CategoryResponse])
async def get_categories(
        skip: int = 0,
        limit: int = 100,
        session: AsyncSession = Depends(get_async_session)
):
    categories = await crud_category.get_multi(session, skip=skip, limit=limit)
    return categories


@router.get('/categories/{category_id}/', response_model=CategoryResponse)
async def get_category(
    category_id: int,
    session: AsyncSession = Depends(get_async_session)
):
    category = await crud_category.get_category_by_id(category_id=category_id, session=session)
    if not category:
        raise HTTPException(status_code=404, detail="Category not found")
    return category


@router.patch('/categories/{category_id}/', response_model=CategoryResponse)
async def update_category(
        category_id: int,
        category_in: CategoryUpdate,
        session: AsyncSession = Depends(get_async_session)
):
    category_old = await crud_category.get_category_by_id(category_id=category_id, session=session)
    if not category_old:
        raise HTTPException(status_code=404, detail="Category not found")
    updated_category = await crud_category.update(db_obj=category_old, obj_in=category_in, session=session)
    return updated_category


@router.post('/storage/', response_model=StorageLocationUpdate)
async def create_storage_location(
    storage_in: StorageLocationCreate,
    session: AsyncSession = Depends(get_async_session)
):
    storage = await crud_storage.create(storage_in, session)
    return storage


@router.get('/storage/', response_model=list[StorageLocationResponse])
async def get_storage_locations(
        session: AsyncSession = Depends(get_async_session),
        skip: int = 0,
        limit: int = 100
):
    storages = await crud_storage.get_multi(session, skip=skip, limit=limit)
    return storages


@router.get('/storage/{storage_id}/', response_model=StorageLocationResponse)
async def get_storage_location(
    storage_id: int,
    session: AsyncSession = Depends(get_async_session)
):
    storage = await crud_storage.get_storage_location_by_id(storage_location_id=storage_id, session=session)
    if not storage:
        raise HTTPException(status_code=404, detail="Storage location not found")
    return storage

@router.patch('/storage/{storage_id}/', response_model=StorageLocationResponse)
async def update_storage_location(
        storage_id: int,
        storage_in: StorageLocationUpdate,
        session: AsyncSession = Depends(get_async_session)
):
    storage_old = await crud_storage.get_storage_location_by_id(storage_location_id=storage_id, session=session)
    if not storage_old:
        raise HTTPException(status_code=404, detail="Storage location not found")
    updated_storage = await crud_storage.update(db_obj=storage_old, obj_in=storage_in, session=session)
    return updated_storage
