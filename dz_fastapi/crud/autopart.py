from typing import List, Optional

from fastapi import HTTPException
from sqlalchemy.future import select
from sqlalchemy.orm import selectinload

from dz_fastapi.core.db import AsyncSession
from dz_fastapi.models.autopart import AutoPart, Category, StorageLocation
from dz_fastapi.crud.base import CRUDBase
from dz_fastapi.models.brand import Brand
from dz_fastapi.schemas.autopart import (
    AutoPartCreate,
    AutoPartUpdate,
    CategoryCreate,
    CategoryUpdate,
    StorageLocationCreate,
    StorageLocationUpdate
)
from sqlalchemy.exc import SQLAlchemyError

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
            self,
            new_autopart: AutoPartCreate,
            brand:  Brand,
            session: AsyncSession
    ) -> AutoPart:
        """
        Создает новую автозапчасть в базе данных.

        Args:
            new_autopart (AutoPartCreate): Данные для создания новой автозапчасти.
            brand (Brand): Бренд, к которому принадлежит автозапчасть.
            session (AsyncSessionLocal): Объект сессии базы данных.

        Returns:
            AutoPart: Созданная автозапчасть.

        Raises:
            Exception: Возникает при ошибке создания или сохранения автозапчасти.
        """
        try:
            autopart_data = new_autopart.dict(exclude_unset=True)
            category_name = autopart_data.pop('category_name', None)
            storage_location_name = autopart_data.pop('storage_location_name', None)
            autopart = AutoPart(**autopart_data)
            autopart.brand = brand
            autopart.categories = []
            if category_name:
                category = await crud_category.get_category_id_by_name(category_name, session)
                if not category:
                    raise HTTPException(status_code=400, detail="Category '{category_name}' does not exist.")
                autopart.categories.append(category)
            autopart.storage_locations = []
            if storage_location_name:
                storage_location = await crud_storage.get_storage_location_id_by_name(storage_location_name, session)
                if not storage_location:
                    raise HTTPException(status_code=400, detail="Storage location '{storage_location_name}' does not exist.")
                autopart.storage_locations.append(storage_location)
            session.add(autopart)
            await session.commit()
            await session.refresh(autopart)
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
                selectinload(AutoPart.storage_locations)
            )
            .offset(skip)
            .limit(limit)
        )
        result = await session.execute(stmt)
        autoparts = result.scalars().unique().all()
        return autoparts

    async def get_autopart_by_id(
        self,
        session: AsyncSession,
        autopart_id: int
    ) -> Optional[AutoPart]:
        stmt = (
            select(AutoPart)
            .where(AutoPart.id == autopart_id)
            .options(
                selectinload(AutoPart.categories),
                selectinload(AutoPart.storage_locations)
            )
        )
        result = await session.execute(stmt)
        autopart = result.scalars().unique().one_or_none()
        return autopart


crud_autopart = CRUDAutopart(AutoPart)

class CRUDCategory(CRUDBase[Category, CategoryCreate, CategoryUpdate]):
    async def get_multi(
        self, session: AsyncSession, *, skip: int = 0, limit: int = 100
    ) -> List[Category]:
        try:
            stmt = (
                select(Category)
                .filter(Category.parent_id == None)
                .options(
                    get_recursive_selectinloads(5)
                )
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
            select(Category)
            .options(
                selectinload(Category.children)
            )
        )
        categories = result.scalars().all()
        return categories

    async def get_category_by_id(
            self,
            category_id: int,
            session: AsyncSession
    ) -> Category:
        try:
            stmt = (
                select(Category)
                .where(Category.id == category_id)
                .options(
                    get_recursive_selectinloads(5)
                )
            )
            result = await session.execute(stmt)
            return result.scalars().unique().one_or_none()
        except SQLAlchemyError as error:
            raise error

    async def get_category_id_by_name(
            self,
            category_name: str,
            session: AsyncSession
    ) -> Category:
        try:
            stmt = (
                select(Category)
                .where(Category.name == category_name)
            )
            result = await session.execute(stmt)
            return result.scalars().first()
        except SQLAlchemyError as error:
            raise error


class CRUDStorageLocation(CRUDBase[StorageLocation, StorageLocationCreate, StorageLocationUpdate]):
    async def get_multi(
            self, session: AsyncSession, *, skip: int = 0, limit: int = 100
    ) -> List[StorageLocation]:
        try:
            stmt = (
                select(StorageLocation)
                .options(
                    selectinload(StorageLocation.autoparts).options(
                        selectinload(AutoPart.categories),
                         selectinload(AutoPart.storage_locations)
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
            self,
            storage_location_id: int,
            session: AsyncSession
    ) -> StorageLocation:
        try:
            stmt = (
                select(StorageLocation)
                .where(StorageLocation.id == storage_location_id)
                .options(
                    selectinload(StorageLocation.autoparts)
                )
            )
            result = await session.execute(stmt)
            return result.scalars().unique().one_or_none()
        except SQLAlchemyError as error:
            raise error

    async def get_storage_location_id_by_name(
            self,
            storage_location_name: str,
            session: AsyncSession
    ) -> StorageLocation:
        try:
            stmt = (
                select(StorageLocation)
                .where(StorageLocation.name == storage_location_name)
            )
            result = await session.execute(stmt)
            return result.scalars().first()
        except SQLAlchemyError as error:
            raise error

crud_category = CRUDCategory(Category)
crud_storage = CRUDStorageLocation(StorageLocation)
