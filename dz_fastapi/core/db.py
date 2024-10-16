import asyncio

from sqlalchemy import Column, Integer, text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine,async_sessionmaker
from sqlalchemy.pool import StaticPool
from sqlalchemy.orm import declarative_base, declared_attr

from dz_fastapi.core.config import settings


class PreBase:
    @declared_attr
    def __tablename__(cls):
        return cls.__name__.lower()
    id = Column(Integer, primary_key=True)


Base = declarative_base(cls=PreBase)

# engine = None
# AsyncSessionLocal = None

def get_engine(test=False):
    """
    Lazily initializes and returns the database engine.
    """
    # global engine
    # if engine is not None:
    #     return engine

    database_url = settings.get_database_url(test)
    engine = create_async_engine(
        database_url,
        echo=settings.database_echo,
        pool_size=5,
        max_overflow=10,
        future=True,
    )
    return engine

    # if settings.use_test_db:
    #     engine = create_async_engine(
    #         settings.test_database_url,
    #         echo=settings.database_echo,
    #         connect_args={"check_same_thread": False},
    #         poolclass=StaticPool
    #     )
    # else:
    #     engine = create_async_engine(
    #         settings.database_url,
    #         echo=settings.database_echo,
    #         pool_size=10,
    #         max_overflow=20,
    #     )
    # return engine


# def get_async_session(test=False):
#     """
#     Lazily initializes and returns the async sessionmaker.
#     """
#     global AsyncSessionLocal
#     if AsyncSessionLocal is None:
#         engine = get_engine(test)
#         AsyncSessionLocal = async_sessionmaker(
#             engine,
#             class_=AsyncSession,
#             expire_on_commit=False,
#             autoflush=False,
#             autocommit=False,
#             # future=True
#         )
#     return AsyncSessionLocal

def get_async_session(test=False):
    engine = get_engine(test)
    return async_sessionmaker(
        engine,
        class_=AsyncSession,
        expire_on_commit=False,
        autoflush=False,
        autocommit=False,
    )

async def get_session():
    """
    Dependency-injected session generator for FastAPI routes.
    """
    async_session = get_async_session()
    async with async_session() as session:
        yield session
