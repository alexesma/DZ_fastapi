import asyncio

from dz_fastapi.core.constants import BRANDS, create_brands
from dz_fastapi.core.db import get_async_session
from dz_fastapi.crud.brand import brand_crud
from dz_fastapi.schemas.brand import BrandCreate


async def seed_data():
    async_session = get_async_session()
    async with async_session() as session:

        for brand_data in create_brands(BRANDS):
            existing = await brand_crud.get_brand_by_name(
                brand_data['name'], session
            )
            if not existing:
                brand_obj = BrandCreate(**brand_data)
                await brand_crud.create(
                    obj_in=brand_obj, session=session, commit=True
                )

        # Добавить синонимы, используем метод или crud,
        # который их присоединяет например:
        # brand = await brand_crud.get_brand_by_name(
        # "BrandOne", session
        # )
        # synonym_brand = await brand_crud.get_brand_by_name(
        # "BrandTwo", session
        # )
        # await brand_crud.add_synonym(brand, synonym_brand, session=session)


if __name__ == "__main__":
    asyncio.run(seed_data())
