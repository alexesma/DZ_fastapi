from typing import Optional
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from dz_fastapi.crud.base import CRUDBase
from dz_fastapi.models.brand import Brand
from dz_fastapi.schemas.brand import BrandCreate, BrandUpdate
import logging

logger = logging.getLogger(__name__)


class CRUDBrand(CRUDBase[Brand, BrandCreate, BrandUpdate]):
    async def get_brand_by_name(
            self,
            brand_name: str,
            sessions: AsyncSession
    ) -> Optional[int]:
        db_brand_id = await sessions.execute(
            select(Brand.id).where(Brand.name == brand_name)
        )
        return db_brand_id.scalars().first()


brand_crud = CRUDBrand(Brand)

# async def create_brand(new_brand: BrandCreate, db: AsyncSessionLocal = AsyncSessionLocal()) -> Brand:
#     try:
#         logger.info(f"Creating brand with data: {new_brand}")
#         brand = Brand(**new_brand.dict())
#         async with db as session:
#             session.add(brand)
#             await session.commit()
#             await session.refresh(brand)
#         return brand
#     except Exception as error:
#         logger.error(f"Failed to create brand: {error}")
#         raise Exception("Failed to create brand") from error
