from fastapi import APIRouter, Depends

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
    autopart.name = await change_string(autopart.name)
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
