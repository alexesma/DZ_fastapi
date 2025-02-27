import logging
from http import HTTPStatus
from typing import List, Optional

from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from dz_fastapi.api.validators import change_brand_name
from dz_fastapi.core.base import Brand
from dz_fastapi.crud.base import CRUDBase
from dz_fastapi.schemas.brand import BrandCreate, BrandUpdate

logger = logging.getLogger('dz_fastapi')


async def duplicate_brand_name(brand_name: str, session: AsyncSession) -> None:
    brand = await brand_crud.get_brand_by_name(brand_name, session)
    if brand is not None:
        raise HTTPException(
            status_code=HTTPStatus.CONFLICT,
            detail=f'Brand with name {brand_name} already exists',
        )


async def brand_exists(brand_id: int, session: AsyncSession) -> Brand:
    brand = await brand_crud.get(session, brand_id)
    if brand is None:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND, detail='Brand not found'
        )
    return brand


class CRUDBrand(CRUDBase[Brand, BrandCreate, BrandUpdate]):
    async def create(self, brand: Brand, session: AsyncSession, **kwargs):
        try:
            logger.debug('Начало создания бренда api')
            brand.name = await change_brand_name(brand.name)
            logger.debug(f'Изменённое имя бренда: {brand.name}')
            await duplicate_brand_name(brand_name=brand.name, session=session)
            logger.debug('Проверка дубликата имени бренда завершена')
            new_brand = await super().create(brand, session, commit=True)
            logger.debug(f'Бренд создан и добавлен в сессию: {new_brand}')
            stmt = (
                select(Brand)
                .options(selectinload(Brand.synonyms))
                .filter_by(id=new_brand.id)
            )
            result = await session.execute(stmt)
            new_brand = result.scalar_one()
            logger.debug(f'Создан новый бренд: {new_brand}')
            return new_brand

        except IntegrityError as e:
            logger.error(f'Integrity error occurred: {e}')
            await session.rollback()
            raise HTTPException(
                status_code=409,
                detail=f'Brand with name {brand.name} already exists',
            )
        except SQLAlchemyError as e:
            logger.error(f'Database error occurred: {e}')
            await session.rollback()
            raise HTTPException(
                status_code=500, detail=f'Database error occurred: {str(e)}'
            )
        except Exception as e:
            logger.error(f'Unexpected error occurred: {e}')
            await session.rollback()
            raise HTTPException(
                status_code=500, detail=f'Unexpected error occurred: {str(e)}'
            )

    async def get_brand_by_id(
        self, brand_id: int, session: AsyncSession
    ) -> Optional[Brand]:
        try:
            logger.debug(f'Получение бренда по ID: {brand_id}')
            logger.debug(f'Тип сессии: {type(session)}')
            result = await session.execute(
                select(Brand)
                .options(selectinload(Brand.synonyms))
                .where(Brand.id == brand_id)
            )
            brand = result.scalars().first()
            logger.debug(f'Получен бренд: {brand}')

            return brand
        except Exception as e:
            logger.error(f'Ошибка в get_brand_by_id: {e}')
            logger.exception('Полный стек ошибки:')
            raise

    async def get_brand_by_name(
        self, brand_name: str, session: AsyncSession
    ) -> Optional[Brand]:
        try:
            logger.debug('Зашли в get_brand_by_name')
            normal_name = await change_brand_name(brand_name)
            db_brand = await session.execute(
                select(Brand)
                .options(selectinload(Brand.synonyms))
                .where(Brand.name == normal_name)
            )
            logger.debug(f'Результат запроса: {db_brand}')
            brand = db_brand.scalars().first()
            logger.debug(f'Первый результат запроса: {brand}')
            return brand
        except Exception as e:
            logger.error(f'Ошибка в get_brand_by_name: {e}')
            raise

    async def get_brand_by_name_or_none(
        self, brand_name: str, session: AsyncSession
    ) -> Optional[Brand]:
        brand_name = await change_brand_name(brand_name=brand_name)
        result = await session.execute(
            select(Brand)
            .options(selectinload(Brand.synonyms))
            .where(Brand.name == brand_name)
        )
        brand = result.scalar_one_or_none()

        if not brand:
            return None

        if brand.main_brand or not brand.synonyms:
            return brand

        for synonym in brand.synonyms:
            if synonym.main_brand:
                return synonym

        return brand

    async def get_multi_with_synonyms(
        self, session: AsyncSession
    ) -> List[Brand]:
        result = await session.execute(
            select(Brand)
            .options(selectinload(Brand.synonyms))
            .order_by(Brand.id)
        )
        return result.scalars().all()

    async def get_with_synonyms(
        self, brand_id: int, session: AsyncSession
    ) -> Optional[Brand]:
        result = await session.execute(
            select(Brand)
            .options(selectinload(Brand.synonyms))
            .where(Brand.id == brand_id)
        )
        return result.scalars().first()

    async def get_all_synonyms(
        self, brand: Brand, session: AsyncSession
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
            brand_with_synonyms = await self.get_with_synonyms(
                current.id, session
            )
            if not brand_with_synonyms:
                logger.warning(f'Brand with id {current.name} not found')
                continue
            for synonym in brand_with_synonyms.synonyms:
                if synonym.id not in checked:
                    to_check.append(synonym)

        return list(all_synonyms)

    async def get_all_synonyms_bi_directional(
        self, brand: Brand, session: AsyncSession
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
            brand_with_synonyms = await self.get_with_synonyms(
                current.id, session
            )
            for synonym in brand_with_synonyms.synonyms:
                if synonym.id not in checked:
                    to_check.append(synonym)
                    all_synonyms.add(synonym)

        return list(all_synonyms)

    async def get_brands_by_names(
        self, brand_names: List[str], session: AsyncSession
    ) -> List[Brand]:
        try:
            logger.debug('Получение брендов по названиям')
            db_brands = await session.execute(
                select(Brand)
                .options(selectinload(Brand.synonyms))
                .where(Brand.name.in_(brand_names))
            )
            brands = db_brands.scalars().all()
            logger.debug(f'Найденные бренды: {brands}')
            return brands
        except Exception as e:
            logger.error(f'Ошибка в get_brands_by_names: {e}')
            raise

    async def add_synonym(
        self, brand: Brand, synonym: Brand, session: AsyncSession
    ) -> Brand:
        logger.debug(
            f'Добавление синонима: бренд={brand.name}, '
            f'синоним={synonym.name}'
        )
        logger.debug(f'Атрибуты и методы бренда: {dir(brand)}')
        logger.debug(f'Атрибуты бренда: {vars(brand)}')
        if synonym not in brand.synonyms:
            brand.synonyms.append(synonym)
            logger.debug('Добавили синоним')
        if brand not in synonym.synonyms:
            synonym.synonyms.append(brand)
        logger.debug('Создали синонимы')

        logger.debug(f'Синонимы бренда после добавления: {brand.synonyms}')
        logger.debug(f'Синонимы синонима после добавления: {synonym.synonyms}')
        session.add(brand)
        session.add(synonym)
        try:
            await session.flush()
        except Exception as e:
            logger.error(f'Ошибка при выполнении flush: {str(e)}')
            raise
        logger.debug(f'Синонимы бренда после flush: {brand.synonyms}')
        logger.debug(f'Синонимы синонима после flush: {synonym.synonyms}')
        return brand

    async def add_synonyms(
        self, session: AsyncSession, brand_id: int, synonym_names: List[str]
    ) -> Brand:
        try:
            brand = await self.get_brand_by_id(
                brand_id=brand_id, session=session
            )
            if brand is None:
                raise Exception('Failed to add synonym, returned None')

            logger.debug(f'Исходные синонимы бренда: {brand.synonyms}')

            for synonym_name in synonym_names:
                synonym = await self.get_brand_by_name(
                    brand_name=synonym_name, session=session
                )
                if not synonym:
                    raise ValueError(f'Synonym brand {synonym_name} not found')

                logger.debug(f'Добавление синонима {synonym_name}')
                brand = await self.add_synonym(
                    brand=brand, synonym=synonym, session=session
                )
                if brand is None:
                    raise Exception(f'Failed to add synonym {synonym_name}')
                logger.debug(f'Результат add_synonym: {brand}')

            await session.flush()
            await session.refresh(brand, attribute_names=['synonyms'])
            logger.debug(f'Финальные синонимы бренда: {brand.synonyms}')

            return brand
        except Exception as e:
            logger.exception(f'Ошибка в add_synonyms: {str(e)}')
            raise

    async def remove_synonyms(
        self, session: AsyncSession, brand_id: int, synonym_names: List[str]
    ) -> Brand:
        try:
            brand = await self.get_brand_by_id(
                brand_id=brand_id, session=session
            )
            if brand is None:
                raise Exception('Failed to add synonym, returned None')
            logger.debug(
                f'Исходные синонимы бренда: '
                f'{[s.name for s in brand.synonyms]}'
            )

            for synonym_name in synonym_names:
                synonym = await self.get_brand_by_name(
                    brand_name=synonym_name, session=session
                )
                if not synonym:
                    raise ValueError(f'Synonym brand {synonym_name} not found')
                logger.debug(f'Удаление синонима {synonym_name}')
                if synonym in brand.synonyms:
                    brand.synonyms.remove(synonym)
                    logger.debug(
                        f'Synonym {synonym_name} '
                        f'removed from brand {brand.name}'
                    )
                else:
                    logger.debug(
                        f'Synonym {synonym_name} '
                        f'not found in brand {brand.name}'
                    )

                if brand in synonym.synonyms:
                    synonym.synonyms.remove(brand)
                    logger.debug(
                        f'Brand {brand.name} removed '
                        f'from synonym {synonym.name}'
                    )
                else:
                    logger.debug(
                        f'Brand {brand.name} not '
                        f'found in synonym {synonym.name}'
                    )

            await session.flush()
            logger.debug(
                'Successfully flushed session after removing synonyms'
            )

            await session.refresh(brand, attribute_names=['synonyms'])
            logger.debug('Brand refreshed after removing synonyms')
            logger.debug(
                f'Final synonyms of the '
                f'brand: {[s.name for s in brand.synonyms]}'
            )

            return brand
        except ValueError as ve:
            logger.error(f'ValueError in remove_synonyms: {str(ve)}')
            raise
        except Exception as e:
            logger.exception(f'Error in remove_synonyms: {str(e)}')
            raise


brand_crud = CRUDBrand(Brand)
