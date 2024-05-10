from fastapi import APIRouter, HTTPException, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from dz_fastapi.core.db import get_async_session

from dz_fastapi.crud.brand import brand_crud
from dz_fastapi.schemas.brand import BrandCreate, BrandBase

router = APIRouter()


@router.get(
    '/brand',
    response_model=list[BrandBase],
    tags=['brand'],
    summary='Список брендов',
    response_model_exclude_none=True
)
async def get_brand(session: AsyncSession = Depends(get_async_session)):
    return await brand_crud.get_multi(session)


@router.post(
    '/brand',
    response_model=BrandBase,
    tags=['brand'],
    summary='Создание бренда',
    response_model_exclude_none=True
)
async def create_brand(
        brand: BrandCreate,
        session: AsyncSession = Depends(get_async_session)
):
    new_brand = await brand_crud.create(brand, session, commit=False)
    session.add_all(
        [new_brand]
    )
    await session.commit()
    await session.refresh(new_brand)
    return new_brand

# @router.post('/brand', response_model=BrandBase, tags=['brand'], summary='Добавление нового бренда')
# async def create_brand_endpoint(brand: BrandCreate):
#     try:
#         created_brand = await create_brand(brand)
#         return created_brand
#     except Exception as error:
#         error_message = f"Failed to create brand: {str(error)}"
#         raise HTTPException(status_code=500, detail=error_message)
