import logging
import tempfile
from logging.handlers import RotatingFileHandler
from pathlib import Path

import pytest
import pytest_asyncio
from email_validator import validate_email as real_validate_email
from httpx import ASGITransport, AsyncClient
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool

from dz_fastapi.api.deps import get_current_user
from dz_fastapi.core.base import (
    AutoPart,
    Brand,
    Category,
    Customer,
    Provider,
    ProviderPriceListConfig,
    StorageLocation,
)
from dz_fastapi.core.config import settings
from dz_fastapi.core.constants import get_max_file_size, get_upload_dir
from dz_fastapi.core.db import Base, get_async_session, get_session
from dz_fastapi.main import app
from dz_fastapi.models.user import User, UserRole, UserStatus

logger = logging.getLogger("dz_fastapi")


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "no_auth_override: keep real auth dependencies for auth/security tests",
    )


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


@pytest.fixture(scope="session")
def test_schema_name(request) -> str:
    """Give every xdist worker an isolated PostgreSQL schema."""
    worker_input = getattr(request.config, "workerinput", None) or {}
    worker_id = worker_input.get("workerid", "master")
    return f"pytest_{worker_id}"


async def _truncate_database(engine, schema_name: str) -> None:
    """Remove test data without rebuilding the complete database schema."""
    async with engine.begin() as conn:
        if conn.dialect.name == "postgresql":
            preparer = conn.dialect.identifier_preparer
            quoted_schema = preparer.quote_schema(schema_name)
            checks = " UNION ALL ".join(
                (
                    f"SELECT '{table.name}' AS table_name "
                    f"WHERE EXISTS (SELECT 1 FROM {quoted_schema}."
                    f"{preparer.quote(table.name)} LIMIT 1)"
                )
                for table in Base.metadata.tables.values()
            )
            populated_tables = (
                (await conn.execute(text(checks))).scalars().all()
                if checks
                else []
            )
            if populated_tables:
                tables = ", ".join(
                    f"{quoted_schema}.{preparer.quote(table_name)}"
                    for table_name in populated_tables
                )
                await conn.execute(
                    text(f"TRUNCATE TABLE {tables} RESTART IDENTITY CASCADE")
                )
            return

        for table in reversed(Base.metadata.sorted_tables):
            await conn.execute(table.delete())


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def test_engine(test_schema_name):
    """Create tables once per worker instead of twice per individual test."""
    database_url = settings.get_database_url(test=True)
    dialect_name = make_url(database_url).get_backend_name()

    if dialect_name == "postgresql":
        admin_engine = create_async_engine(database_url, echo=False, future=True)
        quoted_schema = _quote_identifier(test_schema_name)
        async with admin_engine.begin() as conn:
            await conn.execute(text(f"DROP SCHEMA IF EXISTS {quoted_schema} CASCADE"))
            await conn.execute(text(f"CREATE SCHEMA {quoted_schema}"))
        await admin_engine.dispose()

        engine = create_async_engine(
            database_url,
            echo=False,
            future=True,
            poolclass=NullPool,
            connect_args={
                "server_settings": {
                    "search_path": test_schema_name,
                }
            },
        )
    else:
        engine = create_async_engine(
            database_url,
            echo=False,
            future=True,
            poolclass=NullPool,
        )

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    yield engine

    await engine.dispose()

    cleanup_engine = create_async_engine(database_url, echo=False, future=True)
    async with cleanup_engine.begin() as conn:
        if dialect_name == "postgresql":
            await conn.execute(
                text(
                    f"DROP SCHEMA IF EXISTS "
                    f"{_quote_identifier(test_schema_name)} CASCADE"
                )
            )
        else:
            await conn.run_sync(Base.metadata.drop_all)
    await cleanup_engine.dispose()


# @pytest_asyncio.fixture(scope="function")
# async def test_db(test_engine):
#     await _reset_database_schema(test_engine)
#     yield
#     await _reset_database_schema(test_engine)


@pytest_asyncio.fixture(scope="function")
async def test_session(test_engine, test_schema_name) -> AsyncSession:
    async_session = sessionmaker(test_engine, class_=AsyncSession, expire_on_commit=False)
    async with async_session() as session:
        yield session
        await session.rollback()
    await _truncate_database(test_engine, test_schema_name)


@pytest.fixture
def anyio_backend():
    """The application and asyncpg are asyncio-based; Trio is unsupported."""
    return "asyncio"


@pytest_asyncio.fixture
async def created_brand(test_session: AsyncSession) -> Brand:
    brand = Brand(
        name="TEST BRAND",
        country_of_origin="USA",
        website="https://example.com",
        description="A test brand",
    )
    test_session.add(brand)
    await test_session.commit()
    await test_session.refresh(brand)
    return brand


@pytest_asyncio.fixture
async def created_autopart(test_session: AsyncSession, created_brand: Brand) -> AutoPart:
    autopart = AutoPart(
        name="TEST AUTOPART",
        brand_id=created_brand.id,
        oem_number="E4G163611091",
        description="A test autopart",
    )
    test_session.add(autopart)
    await test_session.commit()
    await test_session.refresh(autopart)
    return autopart


@pytest_asyncio.fixture
async def created_category(test_session: AsyncSession) -> Category:
    category = Category(name="Test Category")
    test_session.add(category)
    await test_session.commit()
    await test_session.refresh(category)
    return category


@pytest.fixture(autouse=True)
def _patch_email_validator(monkeypatch):
    """
    В тестах отключаем проверку deliverability (DNS/MX),
    оставляя синтаксическую проверку.
    """
    import dz_fastapi.models.partner as partner_mod

    def patched_validate(email, *args, **kwargs):
        kwargs.setdefault("check_deliverability", False)
        return real_validate_email(email, *args, **kwargs)

    monkeypatch.setattr(partner_mod, "validate_email", patched_validate)


@pytest_asyncio.fixture
async def created_providers(test_session: AsyncSession) -> list[Provider]:
    providers_data = [
        {
            "name": "Test Provider 1",
            "email_contact": "test1@example.com",
            "email_incoming_price": "prices1@example.com",
            "description": "First test provider",
            "comment": "No comment",
            "type_prices": "Wholesale",
        },
        {
            "name": "Test Provider 2",
            "email_contact": "test2@axample.com",
            "email_incoming_price": "prices2@example.com",
            "description": "Second test provider",
            "comment": "No comment",
            "type_prices": "Retail",
        },
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


@pytest_asyncio.fixture
async def created_pricelist_config(
    created_providers: list[Provider], test_session: AsyncSession
) -> ProviderPriceListConfig:
    provider_pricelist_config_data = {
        "provider_id": created_providers[0].id,
        "start_row": 1,
        "oem_col": 0,
        "name_col": 2,
        "brand_col": 1,
        "qty_col": 3,
        "price_col": 4,
        "name_price": "PRICE_CONFIG",
        "name_mail": "MAIL_CONFIG",
    }
    config = ProviderPriceListConfig(**provider_pricelist_config_data)
    test_session.add(config)
    await test_session.commit()
    await test_session.refresh(config)
    return config


@pytest_asyncio.fixture
async def created_customers(test_session: AsyncSession) -> list[Customer]:
    customers_data = [
        {
            "name": "Test Customer 1",
            "email_contact": "test1@customer.com",
            "email_outgoing_price": "prices1@costomer.com",
            "description": "First test customer",
            "comment": "No comment",
            "type_prices": "Wholesale",
        },
        {
            "name": "Test Customer 2",
            "email_contact": "test2@customer.com",
            "email_outgoing_price": "prices2@customer.com",
            "description": "Second test customer",
            "comment": "No comment",
            "type_prices": "Retail",
        },
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


@pytest_asyncio.fixture
async def created_storage(test_session: AsyncSession) -> StorageLocation:
    storage = StorageLocation(name="AA 8")
    test_session.add(storage)
    await test_session.commit()
    await test_session.refresh(storage)
    return storage


@pytest_asyncio.fixture(scope="function")
async def async_client(test_session: AsyncSession):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


@pytest_asyncio.fixture(scope="function", autouse=True)
async def override_dependencies(
    test_engine, test_session, test_schema_name, request
):
    """
    Fixture that automatically overrides dependencies for all tests.
    """

    # Logger setup
    logger = logging.getLogger("dz_fastapi")
    if not logger.handlers:
        logger.setLevel(logging.DEBUG)
        handler = RotatingFileHandler(
            f"test_dz_fastapi_{test_schema_name}.log",
            maxBytes=2000,
            backupCount=100,
        )
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
    async_sessionmaker = sessionmaker(test_engine, class_=AsyncSession, expire_on_commit=False)

    # Override get_session
    async def override_get_session():
        async with async_sessionmaker() as session:
            yield session

    async def override_get_current_user():
        return User(
            id=1,
            name="Test Admin",
            email="test-admin@example.com",
            role=UserRole.ADMIN,
            status=UserStatus.ACTIVE,
        )

    # Create and store session factory for scheduler tests
    app.state.session_factory = async_sessionmaker

    # Apply dependency overrides
    app.dependency_overrides[get_upload_dir] = override_get_upload_dir
    app.dependency_overrides[get_max_file_size] = override_get_max_file_size
    app.dependency_overrides[get_session] = override_get_session
    app.dependency_overrides[get_async_session] = override_get_session
    if request.node.get_closest_marker("no_auth_override") is None:
        app.dependency_overrides[get_current_user] = override_get_current_user

    logger.debug("Dependencies overridden for the test")

    yield  # Run the test

    # Clean up
    temp_upload_dir.cleanup()

    # Clear overrides after test
    app.dependency_overrides.clear()
    if hasattr(app.state, "session_factory"):
        delattr(app.state, "session_factory")
    logger.debug("Dependencies overrides cleared after the test")
