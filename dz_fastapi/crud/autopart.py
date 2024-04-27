from dz_fastapi.core.db import AsyncSessionLocal
from dz_fastapi.models.autopart import AutoPart
from dz_fastapi.schemas.autopart import AutoPartCreate, AutoPartUpdate
from sqlalchemy.exc import SQLAlchemyError


async def create_autopart(
    new_autopart: AutoPartCreate, db: AsyncSessionLocal
    ) -> AutoPart:
    """
    Создает новую автозапчасть в базе данных.

    Args:
        new_autopart (AutoPartCreate): Данные для создания новой автозапчасти.
        db (AsyncSessionLocal): Объект сессии базы данных.

    Returns:
        AutoPart: Созданная автозапчасть.

    Raises:
        Exception: Возникает при ошибке создания или сохранения автозапчасти.
    """
    try:
        autopart = AutoPart(**new_autopart.dict())
        async with db as session:
            session.add(autopart)
            await session.commit()
            await session.refresh(autopart)
        return autopart
    except SQLAlchemyError as error:
        raise SQLAlchemyError("Failed to create autopart") from error
