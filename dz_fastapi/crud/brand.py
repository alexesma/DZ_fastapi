from typing import Optional, List
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload, selectinload

from fastapi import HTTPException

from dz_fastapi.crud.base import CRUDBase
from dz_fastapi.models.brand import Brand
from dz_fastapi.schemas.brand import BrandCreate, BrandUpdate
import logging

logger = logging.getLogger('dz_fastapi')

class CRUDBrand(CRUDBase[Brand, BrandCreate, BrandUpdate]):
    async def get_brand_by_id(
            self,
            brand_id: int,
            session: AsyncSession
    ) -> Optional[Brand]:
        try:
            logger.debug(f"Получение бренда по ID: {brand_id}")
            logger.debug(f"Тип сессии: {type(session)}")
            result = await session.execute(
                select(Brand).options(selectinload(Brand.synonyms)).where(Brand.id == brand_id)
            )
            brand = result.scalars().first()
            logger.debug(f"Получен бренд: {brand}")

            return brand
        except Exception as e:
            logger.error(f'Ошибка в get_brand_by_id: {e}')
            logger.exception("Полный стек ошибки:")
            raise

    async def get_brand_by_name(
            self,
            brand_name: str,
            session: AsyncSession
    ) -> Optional[Brand]:
        try:
            logger.debug('Зашли в get_brand_by_name')
            db_brand = await session.execute(
                select(Brand).options(selectinload(Brand.synonyms)).where(Brand.name == brand_name)
            )
            logger.debug(f'Результат запроса: {db_brand}')
            brand = db_brand.scalars().first()
            logger.debug(f'Первый результат запроса: {brand}')
            # if brand:
            #     await session.refresh(brand)  # Без использования options
            return brand
        except Exception as e:
            logger.error(f'Ошибка в get_brand_by_name: {e}')
            raise

    async def get_multi_with_synonyms(
            self,
            session: AsyncSession
    ) -> List[Brand]:
        result = await session.execute(
            select(Brand).options(selectinload(Brand.synonyms)).order_by(Brand.id)
        )
        return result.scalars().all()

    async def get_with_synonyms(self, brand_id: int, session: AsyncSession) -> Optional[Brand]:
        result = await session.execute(
            select(Brand).options(selectinload(Brand.synonyms)).where(Brand.id == brand_id)
        )
        return result.scalars().first()

    async def get_all_synonyms(
            self,
            brand: Brand,
            session: AsyncSession
    ) -> List[Brand]:
        checked = set()
        to_check = [brand]
        all_synonyms = set()

        while to_check:
            current = to_check.pop()
            if current.id in checked:
                continue
            checked.add(current.id)
            all_synonyms.add(current)
            brand_with_synonyms = await self.get_with_synonyms(current.id, session)
            if not brand_with_synonyms:
                logger.warning(f"Brand with id {current.name} not found")
                continue
            for synonym in brand_with_synonyms.synonyms:
                if synonym.id not in checked:
                    to_check.append(synonym)

        return list(all_synonyms)

    async def get_all_synonyms_bi_directional(self, brand: Brand, session: AsyncSession) -> List[Brand]:
        checked = set()
        to_check = [brand]
        all_synonyms = set()

        while to_check:
            current = to_check.pop()
            if current.id in checked:
                continue
            checked.add(current.id)
            all_synonyms.add(current)
            brand_with_synonyms = await self.get_with_synonyms(current.id, session)
            for synonym in brand_with_synonyms.synonyms:
                if synonym.id not in checked:
                    to_check.append(synonym)
                    all_synonyms.add(synonym)

        return list(all_synonyms)

    async def add_synonym(self, brand: Brand, synonym: Brand, session: AsyncSession) -> Brand:
        logger.debug(f'Добавление синонима: бренд={brand.name}, синоним={synonym.name}')
        logger.debug(f'Атрибуты и методы бренда: {dir(brand)}')
        logger.debug(f'Атрибуты бренда: {vars(brand)}')
        if synonym not in brand.synonyms:
            brand.synonyms.append(synonym)
            logger.debug(f'Добавили синоним')
        if brand not in synonym.synonyms:
            synonym.synonyms.append(brand)
        logger.debug(f'Создали синонимы')

        logger.debug(f'Синонимы бренда после добавления: {brand.synonyms}')
        logger.debug(f'Синонимы синонима после добавления: {synonym.synonyms}')
        session.add(brand)
        session.add(synonym)
        try:
            await session.flush()
            logger.debug("Успешно выполнен flush")
        except Exception as e:
            logger.error(f"Ошибка при выполнении flush: {str(e)}")
            raise
        logger.debug(f'Синонимы бренда после flush: {brand.synonyms}')
        logger.debug(f'Синонимы синонима после flush: {synonym.synonyms}')
        return brand

    async def add_synonyms(self, session: AsyncSession, brand_id: int, synonym_names: List[str]) -> Brand:
        try:
            brand = await self.get_brand_by_id(brand_id=brand_id, session=session)
            if brand is None:
                raise Exception("Failed to add synonym, returned None")

            logger.debug(f'Исходные синонимы бренда: {brand.synonyms}')

            for synonym_name in synonym_names:
                synonym = await self.get_brand_by_name(brand_name=synonym_name, session=session)
                if not synonym:
                    raise ValueError(f"Synonym brand '{synonym_name}' not found")

                logger.debug(f'Добавление синонима {synonym_name}')
                brand = await self.add_synonym(brand=brand, synonym=synonym, session=session)
                if brand is None:
                    raise Exception(f"Failed to add synonym {synonym_name}")
                logger.debug(f'Результат add_synonym: {brand}')

            logger.debug("Перед выполнением flush")
            await session.flush()
            logger.debug("После выполнения flush")

            logger.debug("Перед выполнением refresh")
            await session.refresh(brand, attribute_names=['synonyms'])
            logger.debug("После выполнения refresh")

            logger.debug(f'Финальные синонимы бренда: {brand.synonyms}')

            return brand
        except Exception as e:
            logger.exception(f"Ошибка в add_synonyms: {str(e)}")
            raise

    async def remove_synonyms(self,
                              session: AsyncSession,
                              brand_id: int,
                              synonym_names: List[str]
                              ) -> Brand:
        try:
            brand = await self.get_brand_by_id(
                brand_id=brand_id,
                session=session
            )
            if brand is None:
                raise Exception("Failed to add synonym, returned None")
            logger.debug(f'Исходные синонимы бренда: {[s.name for s in brand.synonyms]}')

            for synonym_name in synonym_names:
                synonym = await self.get_brand_by_name(
                    brand_name=synonym_name,
                    session=session
                )
                if not synonym:
                    raise ValueError(f"Synonym brand '{synonym_name}' not found")
                logger.debug(f'Удаление синонима {synonym_name}')
                if synonym in brand.synonyms:
                    brand.synonyms.remove(synonym)
                    logger.debug(f'Synonym {synonym_name} removed from brand {brand.name}')
                else:
                    logger.debug(f'Synonym {synonym_name} not found in brand {brand.name}')

                if brand in synonym.synonyms:
                    synonym.synonyms.remove(brand)
                    logger.debug(f'Brand {brand.name} removed from synonym {synonym.name}')
                else:
                    logger.debug(f'Brand {brand.name} not found in synonym {synonym.name}')

            await session.flush()
            logger.debug("Successfully flushed session after removing synonyms")

            await session.refresh(brand, attribute_names=['synonyms'])
            logger.debug("Brand refreshed after removing synonyms")
            logger.debug(f'Final synonyms of the brand: {[s.name for s in brand.synonyms]}')

            return brand
        except ValueError as ve:
            logger.error(f"ValueError in remove_synonyms: {str(ve)}")
            raise
        except Exception as e:
            logger.exception(f"Error in remove_synonyms: {str(e)}")
            raise


brand_crud = CRUDBrand(Brand)
