from typing import Optional, List
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload, selectinload

from dz_fastapi.crud.base import CRUDBase
from dz_fastapi.models.brand import Brand
from dz_fastapi.schemas.brand import BrandCreate, BrandUpdate

class CRUDBrand(CRUDBase[Brand, BrandCreate, BrandUpdate]):
    async def get_brand_by_name(
            self,
            brand_name: str,
            session: AsyncSession
    ) -> Optional[Brand]:
        db_brand = await session.execute(
            select(Brand).where(Brand.name == brand_name)
        )
        return db_brand.scalars().first()

    async def add_synonyms(self, brand_id: int, synonyms: list[str], session: AsyncSession):
        brand = await self.get(session, brand_id)
        for synonym_name in synonyms:
            synonym = await self.get_brand_by_name(synonym_name, session)
            if synonym:
                brand.synonyms.append(synonym)
        await session.commit()
        await session.refresh(brand)
        return brand

    async def get_multi_with_synonyms(self, session: AsyncSession) -> List[Brand]:
        result = await session.execute(
            select(Brand).options(selectinload(Brand.synonyms))
        )
        return result.scalars().all()

    async def get_all_synonyms(self, brand: Brand, session: AsyncSession) -> List[Brand]:
        checked = set()
        to_check = [brand]
        all_synonyms = set()

        while to_check:
            current = to_check.pop()
            if current.id in checked:
                continue
            checked.add(current.id)
            all_synonyms.add(current)
            await session.refresh(current)
            for synonym in current.synonyms:
                if synonym.id not in checked:
                    to_check.append(synonym)

        return list(all_synonyms)

brand_crud = CRUDBrand(Brand)
