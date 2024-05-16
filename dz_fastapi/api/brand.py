from fastapi import APIRouter, HTTPException, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from dz_fastapi.core.db import get_async_session

from dz_fastapi.crud.brand import brand_crud
from dz_fastapi.api.validators import duplicate_brand_name, brand_exists, change_string
from dz_fastapi.schemas.brand import BrandCreate, BrandBase, BrandCreateInDB

router = APIRouter()


@router.get(
    '/brand',
    response_model=list[BrandCreateInDB],
    tags=['brand'],
    summary='Список брендов',
    response_model_exclude_none=True
)
async def get_brand(session: AsyncSession = Depends(get_async_session)):
    return await brand_crud.get_multi(session)


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
    brand.name = await change_string(brand.name)
    await duplicate_brand_name(brand_name=brand.name, session=session)
    new_brand = await brand_crud.create(brand, session, commit=False)
    session.add_all(
        [new_brand]
    )
    await session.commit()
    await session.refresh(new_brand)
    return new_brand


@router.delete(
    '/brand/{brand_id}',
    response_model=BrandCreateInDB,
)
async def remove_brand(
        brand_id: int,
        session: AsyncSession = Depends(get_async_session)
):
    brand = await brand_exists(brand_id, session)
    return await brand_crud.remove(brand, session, commit=True)


@router.patch(
    '/brand/{brand_id}',
    response_model=BrandCreateInDB,
)
async def update_brand(
        brand_id: int,
        brand: BrandBase,
        session: AsyncSession = Depends(get_async_session)
):
    brand_db = await brand_exists(brand_id, session)
    brand.name = await change_string(brand.name)
    if brand_db.name != brand.name:
        await duplicate_brand_name(brand_name=brand.name, session=session)
    brand_db = await brand_crud.update(brand_db, brand, session, commit=True)
    return brand_db
