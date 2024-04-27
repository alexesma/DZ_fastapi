from fastapi import APIRouter

from dz_fastapi.crud.autopart import create_autopart
from dz_fastapi.schemas.autopart import AutoPartCreate

router = APIRouter()


@router.post('/autoparts/')
async def create_autopart_endpoint(autopart: AutoPartCreate):
    return await create_autopart(autopart)
