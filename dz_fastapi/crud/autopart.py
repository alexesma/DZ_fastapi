from typing import List
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
            autopart = AutoPart(**new_autopart.dict())
            autopart.brand = brand
            session.add(autopart)
            await session.commit()
            await session.refresh(autopart)
            return autopart
        except SQLAlchemyError as error:
            await session.rollback()
            raise SQLAlchemyError("Failed to create autopart") from error

    # async def get_autopart_by_id(
    #         self,
    #         autopart_id: int,
    #         session: AsyncSession
    # ) -> AutoPart:
    #     """
    #     Получает автозапчасть по ее идентификатору.
    #
    #     Args:
    #         autopart_id (int): Идентификатор автозапчасти.
    #         session (AsyncSessionLocal): Объект сессии базы данных.
    #
    #     Returns:
    #         AutoPart: Автозапчасть с указанным идентификатором.
    #
    #     Raises:
    #         Exception: Возникает при ошибке получения автозапчасти.
    #     """
    #     try:
    #         autopart = await session.get(AutoPart, autopart_id)
    #         return autopart
    #     except SQLAlchemyError as error:
    #         raise SQLAlchemyError('Failed to get autopart') from error


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


class CRUDStorageLocation(CRUDBase[StorageLocation, StorageLocationCreate, StorageLocationUpdate]):

    pass

crud_category = CRUDCategory(Category)
crud_storage_location = CRUDStorageLocation(StorageLocation)
