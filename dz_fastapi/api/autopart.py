from fastapi import APIRouter, Depends, Body, HTTPException

from dz_fastapi.api.validators import brand_exists, change_string
from dz_fastapi.crud.autopart import crud_autopart
from dz_fastapi.schemas.autopart import AutoPartCreate, AutoPartResponse
from dz_fastapi.core.db import get_async_session
from sqlalchemy.ext.asyncio import AsyncSession

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
    # autopart.name = await change_string(autopart.name)
    return await crud_autopart.create_autopart(autopart, brand_db, session)


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
    response_model=list[AutoPartResponse]
)
async def get_all_autoparts(
        session: AsyncSession = Depends(get_async_session)
):
    return await crud_autopart.get_multi(session)


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
