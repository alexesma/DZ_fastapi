from fastapi import APIRouter, HTTPException, UploadFile, File, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from dz_fastapi.core.db import get_async_session
from dz_fastapi.crud.brand import brand_crud
from dz_fastapi.core.constants import UPLOAD_DIR
from dz_fastapi.api.validators import duplicate_brand_name, brand_exists, change_string
from dz_fastapi.schemas.brand import BrandCreate, BrandBase, BrandCreateInDB
import os
from pathlib import Path

router = APIRouter()
UPLOAD_DIR = Path(UPLOAD_DIR)


@router.get(
    '/brand',
    response_model=list[BrandCreateInDB],
    tags=['brand'],
    summary='Список брендов',
    response_model_exclude_none=True
)
async def get_brand(session: AsyncSession = Depends(get_async_session)):
    brands = await brand_crud.get_multi_with_synonyms(session)
    for brand in brands:
        brand.synonyms = await brand_crud.get_all_synonyms(brand, session)
    return brands


@router.post(
    '/brand',
    response_model=BrandCreateInDB,
    tags=['brand'],
    summary='Создание бренда',
    response_model_exclude_none=True
)
async def create_brand(
        brand: BrandCreate,
        logo: UploadFile = File(None),
        synonym_name: str = None,
        session: AsyncSession = Depends(get_async_session)
):
    if logo:
        logo_filename = f"{brand.name}_{logo.filename}"
        logo_path = UPLOAD_DIR / logo_filename
        os.makedirs(UPLOAD_DIR, exist_ok=True)
        with open(logo_path, "wb") as f:
            f.write(logo.file.read())
        brand.logo = str(logo_path)

    brand.name = await change_string(brand.name)
    await duplicate_brand_name(brand_name=brand.name, session=session)

    new_brand = await brand_crud.create(brand, session, commit=False)
    session.add_all([new_brand])
    await session.commit()
    await session.refresh(new_brand)

    if synonym_name:
        synonym_name = await change_string(synonym_name)
        synonym_brand = await brand_crud.get_brand_by_name(synonym_name, session)
        if synonym_brand:
            all_synonyms = await brand_crud.get_all_synonyms(new_brand, session)
            if synonym_brand not in all_synonyms:
                new_brand.synonyms.append(synonym_brand)
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
        synonym_name: str = None,
        logo: UploadFile = File(None),
        session: AsyncSession = Depends(get_async_session)
):
    brand_db = await brand_exists(brand_id, session)
    if logo:
        logo_filename = f"{brand.name}_{logo.filename}"
        logo_path = UPLOAD_DIR / logo_filename
        os.makedirs(UPLOAD_DIR, exist_ok=True)
        with open(logo_path, "wb") as f:
            f.write(logo.file.read())
        brand.logo = str(logo_path)

    brand.name = await change_string(brand.name)
    if brand_db.name != brand.name:
        await duplicate_brand_name(brand_name=brand.name, session=session)

    updated_brand = await brand_crud.update(brand_db, brand, session, commit=True)
    session.add_all([updated_brand])
    await session.commit()
    await session.refresh(updated_brand)

    if synonym_name:
        synonym_name = await change_string(synonym_name)
        synonym_brand = await brand_crud.get_brand_by_name(synonym_name, session)
        if synonym_brand:
            all_synonyms = await brand_crud.get_all_synonyms(updated_brand, session)
            if synonym_brand not in all_synonyms:
                updated_brand.synonyms.append(synonym_brand)
                await session.commit()
                await session.refresh(updated_brand)
    return updated_brand
