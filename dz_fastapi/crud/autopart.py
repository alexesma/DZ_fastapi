import logging
import re
from typing import List, Optional

from fastapi import HTTPException
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.future import select
from sqlalchemy.orm import selectinload

from dz_fastapi.api.validators import change_brand_name, change_string
from dz_fastapi.core.db import AsyncSession
from dz_fastapi.crud.base import CRUDBase
from dz_fastapi.crud.brand import brand_crud
from dz_fastapi.models.autopart import (AutoPart, Category, StorageLocation,
                                        preprocess_oem_number)
from dz_fastapi.models.brand import Brand
from dz_fastapi.schemas.autopart import (AutoPartCreate,
                                         AutoPartCreatePriceList,
                                         AutoPartUpdate, CategoryCreate,
                                         CategoryUpdate, StorageLocationCreate,
                                         StorageLocationUpdate)

logger = logging.getLogger('dz_fastapi')


def get_recursive_selectinloads(depth: int):
    def recursive_load(level):
        if level == 0:
            return selectinload(Category.children)
        else:
            return selectinload(Category.children).options(
                recursive_load(level - 1)
            )

    return recursive_load(depth - 1)


class CRUDAutopart(CRUDBase[AutoPart, AutoPartCreate, AutoPartUpdate]):
    async def create_autopart(
        self, new_autopart: AutoPartCreate, brand: Brand, session: AsyncSession
    ) -> AutoPart:
        """
        Создает новую автозапчасть в базе данных.

        Args:
            new_autopart (AutoPartCreate):
            Данные для создания новой автозапчасти.
            brand (Brand): Бренд, к которому принадлежит автозапчасть.
            session (AsyncSessionLocal): Объект сессии базы данных.

        Returns:
            AutoPart: Созданная автозапчасть.

        Raises:
            Exception:
            Возникает при ошибке создания или сохранения автозапчасти.
        """
        try:
            autopart_data = new_autopart.model_dump(exclude_unset=True)
            category_name = autopart_data.pop('category_name', None)
            storage_location_name = autopart_data.pop(
                'storage_location_name', None
            )
            autopart_data['name'] = await change_string(autopart_data['name'])
            autopart = AutoPart(**autopart_data)
            autopart.brand = brand
            autopart.categories = []
            if category_name:
                category = await crud_category.get_category_id_by_name(
                    category_name, session
                )
                if not category:
                    raise HTTPException(
                        status_code=400,
                        detail=(f'Category {category_name} does not exist.'),
                    )
                autopart.categories.append(category)
            autopart.storage_locations = []
            if storage_location_name:
                storage_location = (
                    await (
                        crud_storage.get_storage_location_id_by_name(
                            storage_location_name, session
                        )
                    )
                )
                if not storage_location:
                    raise HTTPException(
                        status_code=400,
                        detail=(
                            f'Storage location '
                            f'{storage_location_name} does not exist.'
                        ),
                    )
                autopart.storage_locations.append(storage_location)
            session.add(autopart)
            await session.commit()
            await session.refresh(autopart)
            logger.debug(f"Created new AutoPart: ID={autopart.id}")
            return autopart
        except SQLAlchemyError as error:
            await session.rollback()
            raise SQLAlchemyError("Failed to create autopart") from error

    async def get_multi(
        self, session: AsyncSession, *, skip: int = 0, limit: int = 100
    ) -> List[AutoPart]:
        stmt = (
            select(AutoPart)
            .options(
                selectinload(AutoPart.categories),
                selectinload(AutoPart.storage_locations),
            )
            .offset(skip)
            .limit(limit)
        )
        result = await session.execute(stmt)
        autoparts = result.scalars().unique().all()
        return autoparts

    async def get_autopart_by_oem_brand_or_none(
        self,
        oem_number: str,
        brand_id: int,
        session: AsyncSession,
    ) -> Optional[AutoPart]:
        stmt = select(AutoPart).where(
            AutoPart.brand_id == brand_id, AutoPart.oem_number == oem_number
        )
        result = await session.execute(stmt)
        return result.scalar_one_or_none()

    async def get_autoparts_by_oem_or_none(
        self,
        oem_number: str,
        session: AsyncSession,
    ) -> Optional[List[AutoPart]]:
        stmt = select(AutoPart).where(
            AutoPart.oem_number == oem_number
        )
        result = await session.execute(stmt)
        autoparts = result.scalars().all()
        return autoparts if autoparts else None

    async def get_autopart_by_id(
        self, session: AsyncSession, autopart_id: int
    ) -> Optional[AutoPart]:
        try:
            stmt = (
                select(AutoPart)
                .where(AutoPart.id == autopart_id)
                .options(
                    selectinload(AutoPart.categories),
                    selectinload(AutoPart.storage_locations),
                )
            )
            result = await session.execute(stmt)
            autopart = result.scalars().unique().one_or_none()
            return autopart
        except SQLAlchemyError as e:
            logger.error(
                f'Database error when fetching autopart {autopart_id}: {e}'
            )
            raise

    async def create_autopart_from_price(
        self,
        new_autopart: AutoPartCreatePriceList,
        session: AsyncSession,
        default_brand: Optional[Brand] = None,
    ) -> Optional[AutoPart]:
        try:
            logger.debug(
                f'Starting create_autopart_from_price '
                f'with data: {new_autopart}'
            )
            autopart_data = new_autopart.model_dump(exclude_unset=True)
            logger.debug(f'Extracted autopart_data: {autopart_data}')
            brand_name = autopart_data.pop('brand', None)
            logger.debug(f'Extracted brand_name: {brand_name}')
            if brand_name:
                brand_name = await change_brand_name(brand_name=brand_name)
                logger.debug(f'Changed brand_name: {brand_name}')
                brand = await brand_crud.get_brand_by_name_or_none(
                    brand_name=brand_name, session=session
                )
                logger.debug(f'Retrieved brand: {brand}')
                if not brand:
                    logger.warning(
                        f'Brand {brand_name} not found. '
                        f'Skipping autopart creation.'
                    )
                    return None
            elif default_brand:
                brand = default_brand
                logger.debug(f'Using default_brand: {brand}')
            else:
                logger.warning(
                    'No brand specified and no default brand provided. '
                    'Skipping autopart creation.'
                )
                return None

            if 'oem_number' in autopart_data and autopart_data['oem_number']:
                normalized_oem = preprocess_oem_number(
                    autopart_data['oem_number']
                )
                autopart_data['oem_number'] = normalized_oem
            else:
                logger.error('oem_number is missing in autopart_data')
                return None

            existing_autopart = await self.get_autopart_by_oem_brand_or_none(
                oem_number=autopart_data['oem_number'],
                brand_id=brand.id,
                session=session,
            )
            logger.debug(f'Existing autopart: {existing_autopart}')

            if existing_autopart:
                logger.debug(
                    f'Autopart already exists: ID {existing_autopart.id}'
                )
                return existing_autopart

            autopart_create_data = AutoPartCreate(
                **autopart_data, brand_id=brand.id
            )
            logger.debug(f'AutopartCreate data: {autopart_create_data}')

            autopart = await self.create_autopart(
                new_autopart=autopart_create_data, brand=brand, session=session
            )
            logger.debug(f'Created autopart: {autopart}')

            return autopart
        except Exception as e:
            logger.exception(f'Error in create_autopart_from_price: {e}')
            return None

    async def get_filtered(
        self,
        session: AsyncSession,
        oem: Optional[str] = None,
        brand: Optional[str] = None,
        skip: int = 0,
        limit: int = 100,
    ) -> List[AutoPart]:
        stmt = (
            select(AutoPart)
            .options(
                selectinload(AutoPart.categories),
                selectinload(AutoPart.storage_locations),
            )
            .offset(skip)
            .limit(limit)
        )
        if oem is not None:
            stmt = stmt.where(AutoPart.oem_number == oem)
        if brand is not None:
            brand_obj = await brand_crud.get_brand_by_name(
                brand_name=brand, session=session
            )
            if not brand_obj:
                raise HTTPException(status_code=404, detail='Brand not found')
            stmt = stmt.where(AutoPart.brand_id == brand_obj.id)
        result = await session.execute(stmt)
        autoparts = result.scalars().unique().all()
        return autoparts


crud_autopart = CRUDAutopart(AutoPart)


class CRUDCategory(CRUDBase[Category, CategoryCreate, CategoryUpdate]):
    async def get_multi(
        self, session: AsyncSession, *, skip: int = 0, limit: int = 100
    ) -> List[Category]:
        try:
            stmt = (
                select(Category)
                .filter(Category.parent_id == None)  # noqa: E711
                .options(get_recursive_selectinloads(5))
                .offset(skip)
                .limit(limit)
            )
            result = await session.execute(stmt)
            categories = result.scalars().unique().all()
            return categories
        except SQLAlchemyError as error:
            raise error

    async def get_categories(session: AsyncSession):
        result = await session.execute(
            select(Category).options(selectinload(Category.children))
        )
        categories = result.scalars().all()
        return categories

    async def get_category_by_id(
        self, category_id: int, session: AsyncSession
    ) -> Category:
        try:
            stmt = (
                select(Category)
                .where(Category.id == category_id)
                .options(get_recursive_selectinloads(5))
            )
            result = await session.execute(stmt)
            return result.scalars().unique().one_or_none()
        except SQLAlchemyError as error:
            raise error

    async def get_category_id_by_name(
        self, category_name: str, session: AsyncSession
    ) -> Category:
        try:
            stmt = select(Category).where(Category.name == category_name)
            result = await session.execute(stmt)
            return result.scalars().first()
        except SQLAlchemyError as error:
            raise error

    async def create_many(
        self, category_data: List[CategoryCreate], session: AsyncSession
    ):
        """
        Массово создать категории из списка CategoryCreate
        """
        try:
            category_objs = [
                Category(**category.dict(exclude_unset=True))
                for category in category_data
            ]
            session.add_all(category_objs)
            await session.commit()
            for cat_obj in category_objs:
                await session.refresh(cat_obj)

            return category_objs
        except IntegrityError as e:
            await session.rollback()
            detail = None
            if hasattr(e.orig, 'diag') and getattr(
                e.orig.diag, 'message_detail', None
            ):
                detail = e.orig.diag.message_detail
            detail = detail or str(e)
            match = re.search(r'Key \(name\)=\((.+)\) already exists.', detail)
            if match:
                duplicate_name = match.group(1)
                detail = f'Category {duplicate_name} already exists'

            raise HTTPException(
                status_code=400, detail=f'Integrity error: {detail}'
            ) from e
        except SQLAlchemyError as error:
            await session.rollback()
            raise HTTPException(
                status_code=400, detail='Error creating categories in bulk'
            ) from error


class CRUDStorageLocation(
    CRUDBase[StorageLocation, StorageLocationCreate, StorageLocationUpdate]
):
    async def get_multi(
        self, session: AsyncSession, *, skip: int = 0, limit: int = 100
    ) -> List[StorageLocation]:
        try:
            stmt = (
                select(StorageLocation)
                .options(
                    selectinload(StorageLocation.autoparts).options(
                        selectinload(AutoPart.categories),
                        selectinload(AutoPart.storage_locations),
                    )
                )
                .offset(skip)
                .limit(limit)
            )
            result = await session.execute(stmt)
            storage_locations = result.scalars().unique().all()
            return storage_locations
        except SQLAlchemyError as error:
            raise error

    async def get_storage_location_by_id(
        self, storage_location_id: int, session: AsyncSession
    ) -> StorageLocation:
        try:
            stmt = (
                select(StorageLocation)
                .where(StorageLocation.id == storage_location_id)
                .options(selectinload(StorageLocation.autoparts))
            )
            result = await session.execute(stmt)
            return result.scalars().unique().one_or_none()
        except SQLAlchemyError as error:
            raise error

    async def get_storage_location_id_by_name(
        self, storage_location_name: str, session: AsyncSession
    ) -> StorageLocation:
        try:
            stmt = select(StorageLocation).where(
                StorageLocation.name == storage_location_name
            )
            result = await session.execute(stmt)
            return result.scalars().first()
        except SQLAlchemyError as error:
            raise error

    async def create_locations(
        self,
        locations_data: List[StorageLocationCreate],
        session: AsyncSession,
    ):
        try:
            location_objs = [
                StorageLocation(**loc.dict(exclude_unset=True))
                for loc in locations_data
            ]
            session.add_all(location_objs)
            await session.commit()
            for loc_obj in location_objs:
                await session.refresh(loc_obj)
            return location_objs
        except IntegrityError as e:
            await session.rollback()
            detail = None
            if hasattr(e.orig, 'diag') and getattr(
                e.orig.diag, 'message_detail', None
            ):
                detail = e.orig.diag.message_detail
            detail = detail or str(e)
            match = re.search(r'Key \(name\)=\((.+)\) already exists.', detail)
            if match:
                duplicate_name = match.group(1)
                detail = f'Storage location {duplicate_name} already exists'

            raise HTTPException(
                status_code=400, detail=f'Integrity error: {detail}'
            ) from e

        except SQLAlchemyError as e:
            await session.rollback()
            raise HTTPException(
                status_code=400,
                detail='Database error when creating storage locations',
            ) from e


crud_category = CRUDCategory(Category)
crud_storage = CRUDStorageLocation(StorageLocation)
