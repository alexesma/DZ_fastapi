import os
from typing import AsyncGenerator

from sqlalchemy import Column, Integer
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import declarative_base, declared_attr

from dz_fastapi.core.config import settings


class PreBase:
    @declared_attr
    def __tablename__(cls):
        return cls.__name__.lower()

    id = Column(Integer, primary_key=True)


Base = declarative_base(cls=PreBase)


_ENGINE_CACHE: dict[bool, AsyncEngine] = {}
_SESSION_FACTORY_CACHE: dict[bool, async_sessionmaker[AsyncSession]] = {}
DB_POOL_SIZE = max(1, int(os.getenv("DATABASE_POOL_SIZE", "10")))
DB_MAX_OVERFLOW = max(0, int(os.getenv("DATABASE_MAX_OVERFLOW", "10")))
DB_POOL_TIMEOUT = max(5, int(os.getenv("DATABASE_POOL_TIMEOUT", "30")))
# Таймаут установки одного соединения с Postgres.
# Увеличен с 10 до 30 с: при кратковременной загрузке event loop (например,
# во время пакетных site API вызовов автозаказа) asyncpg получал CancelledError
# при DNS-резолве → TimeoutError в scheduler-задачах.
DB_CONNECT_TIMEOUT = max(5, int(os.getenv("DATABASE_CONNECT_TIMEOUT", "30")))


def get_engine(test=False):
    """
    Lazily initializes and returns a shared database engine.
    """
    test_key = bool(test)
    engine = _ENGINE_CACHE.get(test_key)
    if engine is not None:
        return engine

    database_url = settings.get_database_url(test_key)
    engine = create_async_engine(
        database_url,
        echo=settings.database_echo,
        pool_size=DB_POOL_SIZE,
        max_overflow=DB_MAX_OVERFLOW,
        pool_pre_ping=True,
        pool_timeout=DB_POOL_TIMEOUT,
        pool_recycle=1800,  # переоткрывать соединения каждые 30 минут
        connect_args={
            "timeout": DB_CONNECT_TIMEOUT
        },  # таймаут установки соединения с Postgres
        future=True,
    )
    _ENGINE_CACHE[test_key] = engine
    return engine


def get_async_session(test=False):
    test_key = bool(test)
    session_factory = _SESSION_FACTORY_CACHE.get(test_key)
    if session_factory is not None:
        return session_factory

    session_factory = async_sessionmaker(
        get_engine(test_key),
        class_=AsyncSession,
        expire_on_commit=False,
        autoflush=False,
        autocommit=False,
    )
    _SESSION_FACTORY_CACHE[test_key] = session_factory
    return session_factory


async def dispose_engines() -> None:
    engines = list(_ENGINE_CACHE.values())
    _SESSION_FACTORY_CACHE.clear()
    _ENGINE_CACHE.clear()
    for engine in engines:
        await engine.dispose()


async def get_session() -> AsyncGenerator[AsyncSession, None]:
    """
    Dependency-injected session generator for FastAPI routes.
    """
    async_session = get_async_session()
    async with async_session() as session:
        yield session
