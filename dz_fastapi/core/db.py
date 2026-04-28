from typing import AsyncGenerator

from sqlalchemy import Column, Integer
from sqlalchemy.ext.asyncio import (AsyncEngine, AsyncSession,
                                    async_sessionmaker, create_async_engine)
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
        pool_size=5,
        max_overflow=10,
        pool_pre_ping=True,
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
