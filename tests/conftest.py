import logging
import tempfile
from logging.handlers import RotatingFileHandler
from pathlib import Path

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

from dz_fastapi.core.base import (AutoPart, Brand, Category, Customer,
                                  Provider, StorageLocation)
from dz_fastapi.core.config import settings
from dz_fastapi.core.constants import get_max_file_size, get_upload_dir
from dz_fastapi.core.db import Base, get_async_session, get_session
from dz_fastapi.main import app

logger = logging.getLogger('dz_fastapi')


@pytest.fixture(scope="function")
async def test_engine():
    engine = create_async_engine(
        settings.get_database_url(test=True),
        echo=True,
        future=True
    )
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
async def created_autopart(
        test_session: AsyncSession,
        created_brand: Brand
) -> AutoPart:
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


@pytest.fixture
async def created_category(test_session: AsyncSession) -> Category:
    category = Category(
        name='Test Category'
    )
    test_session.add(category)
    await test_session.commit()
    await test_session.refresh(category)
    return category


@pytest.fixture
async def created_providers(test_session: AsyncSession) -> list[Provider]:
    providers_data = [
        {
            'name': 'Test Provider 1',
            'email_contact': 'test1@test.com',
            'email_incoming_price': 'prices1@test.com',
            'description': 'First test provider',
            'comment': 'No comment',
            'type_prices': 'Wholesale'
        },
        {
            'name': 'Test Provider 2',
            'email_contact': 'test2@test.com',
            'email_incoming_price': 'prices2@test.com',
            'description': 'Second test provider',
            'comment': 'No comment',
            'type_prices': 'Retail'
        }
    ]

    providers = []
    for data in providers_data:
        provider = Provider(**data)
        test_session.add(provider)
        providers.append(provider)
    await test_session.commit()
    for provider in providers:
        await test_session.refresh(provider)
    return providers


@pytest.fixture
async def created_customers(test_session: AsyncSession) -> list[Customer]:
    customers_data = [
        {
            'name': 'Test Customer 1',
            'email_contact': 'test1@customer.com',
            'email_outgoing_price': 'prices1@costomer.com',
            'description': 'First test customer',
            'comment': 'No comment',
            'type_prices': 'Wholesale'
        },
        {
            'name': 'Test Customer 2',
            'email_contact': 'test2@customer.com',
            'email_outgoing_price': 'prices2@customer.com',
            'description': 'Second test customer',
            'comment': 'No comment',
            'type_prices': 'Retail'
        }
    ]

    customers = []
    for data in customers_data:
        customer = Customer(**data)
        test_session.add(customer)
        customers.append(customer)
    await test_session.commit()
    for customer in customers:
        await test_session.refresh(customer)
    return customers


@pytest.fixture
async def created_storage(test_session: AsyncSession) -> StorageLocation:
    storage = StorageLocation(
        name='AA 8'
    )
    test_session.add(storage)
    await test_session.commit()
    await test_session.refresh(storage)
    return storage


@pytest.fixture(scope='function')
async def async_client(test_session: AsyncSession):
    transport = ASGITransport(app=app)
    async with AsyncClient(
            transport=transport,
            base_url='http://test'
    ) as client:
        yield client


@pytest.fixture(scope='function', autouse=True)
async def override_dependencies(test_engine):
    """
    Fixture that automatically overrides dependencies for all tests.
    """

    # Logger setup
    logger = logging.getLogger("dz_fastapi")
    if not logger.handlers:
        logger.setLevel(logging.DEBUG)
        handler = RotatingFileHandler(
            "test_dz_fastapi.log",
            maxBytes=2000,
            backupCount=100
        )
        handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    # Use a temporary directory
    temp_upload_dir = tempfile.TemporaryDirectory()
    logger.debug(
        f'Temporary upload directory: {temp_upload_dir.name}'
    )

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
