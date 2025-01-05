import logging
from typing import Any, Dict, Generic, List, Optional, Type, TypeVar, Union

from fastapi import HTTPException
from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.core.db import Base

logger = logging.getLogger('dz_fastapi')

ModelType = TypeVar("ModelType", bound=Base)
CreateSchemaType = TypeVar("CreateSchemaType", bound=BaseModel)
UpdateSchemaType = TypeVar("UpdateSchemaType", bound=BaseModel)


class CRUDBase(Generic[ModelType, CreateSchemaType, UpdateSchemaType]):
    def __init__(self, model: Type[ModelType]):
        """
        CRUD object with default methods to
        Create, Read, Update, Delete (CRUD).

        **Parameters**

        * `model`: A SQLAlchemy model class
        * `schema`: A Pydantic model (schema) class
        """
        self.model = model

    async def get(
            self,
            session: AsyncSession,
            obj_id: int
    ) -> Optional[ModelType]:
        db_obj = await session.execute(
            select(self.model).where(self.model.id == obj_id)
        )
        result = db_obj.scalars().first()
        if not result:
            raise HTTPException(
                status_code=404,
                detail=f'{self.model.__name__} not found'
            )
        return result

    async def get_multi(
            self,
            session: AsyncSession,
    ) -> List[ModelType]:
        db_objs = await session.execute(select(self.model))
        return db_objs.scalars().all()

    async def create(
            self,
            obj_in,
            session: AsyncSession,
            commit: bool = True,
    ):
        try:
            logger.debug(f'Создание объекта: {obj_in}')
            obj_in_data = obj_in.model_dump()

            db_obj = self.model(**obj_in_data)
            session.add(db_obj)
            await session.flush()
            await session.refresh(db_obj)
            logger.debug(f'Бренд создан и добавлен в сессию: {db_obj}')

            if commit:
                await session.commit()
                logger.debug('Сессия зафиксирована после создания бренда')
                await session.refresh(db_obj)
            return db_obj
        except SQLAlchemyError as e:
            logger.error(f'Database error occurred: {e}')
            await session.rollback()
            raise HTTPException(
                status_code=500,
                detail='Internal Server Error during create brand'
            )
        except Exception as e:
            logger.error(f'Неожиданная ошибка при создании объекта: {e}')
            await session.rollback()
            raise HTTPException(
                status_code=500,
                detail='Unexpected error occurred during create object'
            )

    async def update(
            self,
            db_obj: ModelType,
            obj_in: Union[UpdateSchemaType, Dict[str, Any]],
            session: AsyncSession,
            commit: bool = True,
    ) -> ModelType:
        obj_data = jsonable_encoder(db_obj)
        if isinstance(obj_in, dict):
            update_data = obj_in
        else:
            update_data = obj_in.model_dump(exclude_unset=True)
        for field in obj_data:
            if field in update_data:
                setattr(db_obj, field, update_data[field])
        session.add(db_obj)
        if commit:
            await session.commit()
            await session.refresh(db_obj)
        return db_obj

    async def remove(
            self,
            db_obj,
            session: AsyncSession,
            commit: bool = True,
    ) -> ModelType:
        await session.delete(db_obj)
        if commit:
            await session.commit()
        return db_obj
