import tempfile
import pytest
import logging

logger = logging.getLogger('dz_fastapi')
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from dz_fastapi.core.db import Base, get_async_session, get_session
from dz_fastapi.core.config import settings
from dz_fastapi.models.brand import Brand
from dz_fastapi.models.autopart import AutoPart
from dz_fastapi.main import app
from pathlib import Path
from dz_fastapi.core.constants import get_max_file_size, get_upload_dir
from logging.handlers import RotatingFileHandler


@pytest.fixture(scope="function")
async def test_engine():
    engine = create_async_engine(settings.get_database_url(test=True), echo=True, future=True)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    yield engine

    # Удаление таблиц после тестов
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)

    # Закрытие движка
    await engine.dispose()


@pytest.fixture(scope="function")
async def test_db(test_engine):
    async with test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
    yield
    async with test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)


@pytest.fixture(scope="function")
async def test_session(test_db, test_engine) -> AsyncSession:
    async_session = sessionmaker(
        test_engine,
        class_=AsyncSession,
        expire_on_commit=False
    )
    async with async_session() as session:
        yield session
        await session.rollback()

@pytest.fixture
async def created_brand(test_session: AsyncSession) -> Brand:
    brand = Brand(
        name='TEST BRAND',
        country_of_origin='USA',
        website='https://example.com',
        description='A test brand',
    )
    test_session.add(brand)
    await test_session.commit()
    await test_session.refresh(brand)
    return brand

@pytest.fixture
async def created_autopart(test_session: AsyncSession, created_brand: Brand) -> AutoPart:
    autopart = AutoPart(
        name='TEST AUTOPART',
        brand_id=created_brand.id,
        oem_number='E4G163611091',
        description='A test autopart'
    )
    test_session.add(autopart)
    await test_session.commit()
    await test_session.refresh(autopart)
    return autopart


@pytest.fixture(scope='function', autouse=True)
async def override_dependencies(test_engine):
    """
    Fixture that automatically overrides dependencies for all tests.
    """

    # Logger setup
    logger = logging.getLogger("dz_fastapi")
    if not logger.handlers:
        logger.setLevel(logging.DEBUG)
        handler = RotatingFileHandler("test_dz_fastapi.log", maxBytes=2000, backupCount=100)
        handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    # Use a temporary directory
    temp_upload_dir = tempfile.TemporaryDirectory()
    logger.debug(f"Temporary upload directory: {temp_upload_dir.name}")

    # Override UPLOAD_DIR
    async def override_get_upload_dir():
        return Path(temp_upload_dir.name)

    # Override MAX_FILE_SIZE
    async def override_get_max_file_size():
        return 1 * 50 * 1024  # 1 MB

    # Create sessionmaker using test_engine
    async_sessionmaker = sessionmaker(
        test_engine,
        class_=AsyncSession,
        expire_on_commit=False
    )

    # Override get_session
    async def override_get_session():
        async with async_sessionmaker() as session:
            yield session

    # Apply dependency overrides
    app.dependency_overrides[get_upload_dir] = override_get_upload_dir
    app.dependency_overrides[get_max_file_size] = override_get_max_file_size
    app.dependency_overrides[get_session] = override_get_session
    app.dependency_overrides[get_async_session] = override_get_session

    logger.debug("Dependencies overridden for the test")

    yield  # Run the test

    # Clean up
    temp_upload_dir.cleanup()

    # Clear overrides after test
    app.dependency_overrides.clear()
    logger.debug("Dependencies overrides cleared after the test")