from dz_fastapi.core.db import AsyncSessionLocal
from dz_fastapi.models.autopart import AutoPart
from dz_fastapi.crud.base import CRUDBase
from dz_fastapi.models.brand import Brand
from dz_fastapi.schemas.autopart import AutoPartCreate, AutoPartUpdate
from sqlalchemy.exc import SQLAlchemyError


class CRUDAutopart(CRUDBase[AutoPart, AutoPartCreate, AutoPartUpdate]):
    async def create_autopart(
            new_autopart: AutoPartCreate,
            brand:  Brand,
            session: AsyncSessionLocal
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
            async with session as session_1:
                session_1.add(autopart)
                await session_1.commit()
                await session_1.refresh(autopart)
            return autopart
        except SQLAlchemyError as error:
            raise SQLAlchemyError("Failed to create autopart") from error

crud_autopart = CRUDAutopart(AutoPart)
