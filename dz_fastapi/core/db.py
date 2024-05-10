import asyncio

from sqlalchemy import Column, Integer, text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine,async_sessionmaker
from sqlalchemy.orm import declarative_base, declared_attr

from dz_fastapi.core.config import settings


class PreBase:
    @declared_attr
    def __tablename__(cls):
        return cls.__name__.lower()
    id = Column(Integer, primary_key=True)


Base = declarative_base(cls=PreBase)

engine = create_async_engine(
    url=settings.database_url,
    echo=True,  # в prod удалить echo
    future=True,   # для асинхронных запросов
    pool_pre_ping=True,   # проверка соединения при запросе к БД
    pool_size=10,  # максимальное количество соединений в пуле
    max_overflow=20,   # максимальное количество соединений при их исчерпании
)

AsyncSessionLocal = async_sessionmaker(engine, class_=AsyncSession)

async def get_async_session():
    async with AsyncSessionLocal() as session:
        yield session
