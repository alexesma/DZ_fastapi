"""Кнопки в таблице «Конфигурации прайс-листов» клиента: скачать и отправить."""

import pytest
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.crud.partner import crud_pricelist
from dz_fastapi.models.brand import Brand
from dz_fastapi.models.email_account import EmailAccount
from dz_fastapi.models.partner import (
    Customer,
    CustomerPriceListConfig,
    CustomerPriceListSource,
    EmailOutbox,
    Provider,
    ProviderPriceListConfig,
)
from dz_fastapi.schemas.autopart import AutoPartPricelist
from dz_fastapi.schemas.partner import PriceListAutoPartAssociationCreate, PriceListCreate
from dz_fastapi.services import process as process_service


async def _prepare_config(
    session: AsyncSession,
    customer: Customer,
    brand: Brand,
    *,
    outgoing_account_id: int | None = None,
) -> CustomerPriceListConfig:
    provider = Provider(
        name="Action Provider",
        email_contact="action@example.com",
        email_incoming_price="action_prices@example.com",
        description="Action provider",
        comment="",
        type_prices="Wholesale",
    )
    session.add(provider)
    await session.commit()
    await session.refresh(provider)
    provider_config = ProviderPriceListConfig(
        provider_id=provider.id,
        start_row=1,
        oem_col=0,
        qty_col=1,
        price_col=2,
        name_price="ACTION_CFG",
        name_mail="ACTION_MAIL",
    )
    session.add(provider_config)
    await session.commit()
    await session.refresh(provider_config)
    await crud_pricelist.create(
        obj_in=PriceListCreate(
            provider_id=provider.id,
            provider_config_id=provider_config.id,
            autoparts=[
                PriceListAutoPartAssociationCreate(
                    autopart=AutoPartPricelist(
                        oem_number="OEM-ACTION-1", brand=brand.name, name="Action Part"
                    ),
                    quantity=5,
                    price=100.0,
                )
            ],
        ),
        session=session,
    )
    config = CustomerPriceListConfig(
        customer_id=customer.id,
        name="ACTION CONFIG",
        general_markup=1.0,
        own_price_list_markup=1.0,
        third_party_markup=1.0,
        emails=["client@example.com"],
        additional_filters={},
        outgoing_email_account_id=outgoing_account_id,
    )
    session.add(config)
    await session.commit()
    await session.refresh(config)
    session.add(
        CustomerPriceListSource(
            customer_config_id=config.id,
            provider_config_id=provider_config.id,
            enabled=True,
            markup=1.0,
            brand_filters={},
            position_filters={},
            additional_filters={},
        )
    )
    await session.commit()
    return config


async def _account(session: AsyncSession, **kwargs) -> EmailAccount:
    account = EmailAccount(
        name=kwargs.pop("name"),
        email=kwargs.pop("email"),
        password="x",
        purposes=["prices_out"],
        is_active=True,
        **kwargs,
    )
    session.add(account)
    await session.commit()
    await session.refresh(account)
    return account


@pytest.mark.asyncio
async def test_download_returns_xlsx_and_sends_nothing(
    test_session: AsyncSession,
    async_client: AsyncClient,
    created_customers: list[Customer],
    created_brand: Brand,
    monkeypatch: pytest.MonkeyPatch,
):
    def must_not_send(**kwargs):
        raise AssertionError("скачивание не должно отправлять письма")

    monkeypatch.setattr(process_service, "send_email_with_attachment", must_not_send)
    customer = created_customers[0]
    config = await _prepare_config(test_session, customer, created_brand)

    response = await async_client.post(
        f"/customers/{customer.id}/pricelist-configs/{config.id}/build-file"
    )

    assert response.status_code == 200, response.text
    assert response.content[:2] == b"PK"  # xlsx — это zip
    assert "attachment" in response.headers.get("content-disposition", "")
    await test_session.refresh(config)
    assert config.last_sent_at is None


@pytest.mark.asyncio
async def test_send_now_via_resend_account_sends_directly(
    test_session: AsyncSession,
    async_client: AsyncClient,
    created_customers: list[Customer],
    created_brand: Brand,
    monkeypatch: pytest.MonkeyPatch,
):
    sent = []
    monkeypatch.setattr(
        process_service,
        "send_email_with_attachment",
        lambda **kwargs: sent.append(kwargs) or True,
    )
    customer = created_customers[0]
    account = await _account(
        test_session,
        name="resend-price",
        email="price@example.online",
        transport="resend_api",
        resend_api_key="re_test",
    )
    config = await _prepare_config(
        test_session, customer, created_brand, outgoing_account_id=account.id
    )

    response = await async_client.post(
        f"/customers/{customer.id}/pricelist-configs/{config.id}/send-now"
    )

    assert response.status_code == 200, response.text
    assert [item["to_email"] for item in sent] == ["client@example.com"]
    assert sent[0]["from_email"] == "price@example.online"
    await test_session.refresh(config)
    assert config.last_sent_at is not None


@pytest.mark.asyncio
async def test_send_now_via_smtp_account_queues_for_relay(
    test_session: AsyncSession,
    async_client: AsyncClient,
    created_customers: list[Customer],
    created_brand: Brand,
    monkeypatch: pytest.MonkeyPatch,
):
    def must_not_send(**kwargs):
        raise AssertionError("с SMTP-ящика письмо идёт через очередь релея")

    monkeypatch.setattr(process_service, "send_email_with_attachment", must_not_send)
    customer = created_customers[0]
    account = await _account(
        test_session,
        name="smtp-price",
        email="price@example.ru",
        transport="smtp",
        smtp_host="smtp.example.ru",
    )
    config = await _prepare_config(
        test_session, customer, created_brand, outgoing_account_id=account.id
    )

    response = await async_client.post(
        f"/customers/{customer.id}/pricelist-configs/{config.id}/send-now"
    )

    assert response.status_code == 200, response.text
    assert response.json()["generation_status"] == "queued"
    queued = (await test_session.execute(select(EmailOutbox))).scalars().all()
    assert [row.to_email for row in queued] == ["client@example.com"]
    assert queued[0].from_email == "price@example.ru"


@pytest.mark.asyncio
async def test_send_now_without_recipients_explains_why(
    test_session: AsyncSession,
    async_client: AsyncClient,
    created_customers: list[Customer],
    created_brand: Brand,
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setattr(process_service, "send_email_with_attachment", lambda **kwargs: True)
    customer = created_customers[0]
    config = await _prepare_config(test_session, customer, created_brand)
    config.emails = []
    customer.email_outgoing_price = None
    await test_session.commit()

    response = await async_client.post(
        f"/customers/{customer.id}/pricelist-configs/{config.id}/send-now"
    )

    assert response.status_code == 400
    assert "не указаны получатели" in response.json()["detail"]
    # Прайс при этом собран и опубликован: он нужен для подбора по заказам
    from dz_fastapi.models.partner import CustomerPriceList

    built = (
        (
            await test_session.execute(
                select(CustomerPriceList).where(CustomerPriceList.customer_config_id == config.id)
            )
        )
        .scalars()
        .all()
    )
    assert len(built) == 1
    assert built[0].published_at is not None
    assert built[0].sent_at is None


@pytest.mark.asyncio
@pytest.mark.parametrize("action", ["build-file", "send-now", "drafts"])
async def test_actions_do_not_eager_load_price_history(
    test_session,
    async_client,
    created_customers,
    created_brand,
    monkeypatch,
    action,
):
    """A client with historical price rows must not load them to build/send a file."""
    from sqlalchemy import event
    from sqlalchemy.orm import Session

    from dz_fastapi.models.partner import (
        CustomerPriceList,
        CustomerPriceListAutoPartAssociation,
        PriceListAutoPartAssociation,
    )

    monkeypatch.setattr(process_service, "send_email_with_attachment", lambda **kw: True)
    customer = created_customers[0]
    account = await _account(
        test_session,
        name="history-test",
        email="history@example.online",
        transport="resend_api",
        resend_api_key="re_test",
    )
    config = await _prepare_config(
        test_session, customer, created_brand, outgoing_account_id=account.id
    )
    customer_id, config_id = customer.id, config.id
    old_price = CustomerPriceList(
        customer_id=customer_id,
        customer_config_id=config_id,
    )
    test_session.add(old_price)
    await test_session.flush()
    part_id = await test_session.scalar(select(PriceListAutoPartAssociation.autopart_id))
    test_session.add(
        CustomerPriceListAutoPartAssociation(
            customerpricelist_id=old_price.id,
            autopart_id=part_id,
            quantity=5,
            price=100,
        )
    )
    await test_session.commit()
    # Fresh identity map, as in a real HTTP request; setup must not mask eager reads.
    test_session.expunge_all()
    unwanted_loads = []

    def record_relationship_load(state):
        if state.is_relationship_load:
            path = str(state.loader_strategy_path)
            if any(
                name in path
                for name in (
                    "Customer.customer_price_lists",
                    "CustomerPriceList.autopart_associations",
                    "CustomerPriceList.published_aliases",
                    "ProviderPriceListConfig.price_lists",
                )
            ):
                unwanted_loads.append(path)

    event.listen(Session, "do_orm_execute", record_relationship_load)
    try:
        response = await async_client.post(
            f"/customers/{customer_id}/pricelist-configs/{config_id}/{action}"
        )
        if action == "drafts" and response.status_code == 200:
            draft_id = response.json()["id"]
            listing = await async_client.get(
                f"/customers/{customer_id}/pricelist-configs/{config_id}/drafts"
            )
            assert listing.status_code == 200, listing.text
            assert len(listing.json()) == 2
            download = await async_client.get(
                f"/customers/{customer_id}/pricelist-configs/{config_id}"
                f"/drafts/{draft_id}/download"
            )
            assert download.status_code == 200, download.text
            assert download.content[:2] == b"PK"
    finally:
        event.remove(Session, "do_orm_execute", record_relationship_load)
    assert response.status_code == 200, response.text
    assert not unwanted_loads, unwanted_loads


async def _job_admin(session):
    from dz_fastapi.models.user import User, UserRole, UserStatus

    session.add(
        User(
            id=1,
            name="Test Admin",
            email="jobs-admin@example.com",
            password_hash="x",
            role=UserRole.ADMIN,
            status=UserStatus.ACTIVE,
        )
    )
    await session.commit()


@pytest.mark.asyncio
async def test_background_job_returns_immediately_then_builds_and_downloads(
    test_session,
    test_engine,
    async_client,
    created_customers,
    created_brand,
    monkeypatch,
):
    from sqlalchemy.ext.asyncio import async_sessionmaker

    from dz_fastapi.services.customer_pricelist_jobs import run_pricelist_jobs

    await _job_admin(test_session)
    customer = created_customers[0]
    config = await _prepare_config(test_session, customer, created_brand)
    base = f"/customers/{customer.id}/pricelist-configs/{config.id}"
    monkeypatch.setattr(
        process_service,
        "send_email_with_attachment",
        lambda **kwargs: pytest.fail("Downloading must not send mail"),
    )
    first = await async_client.post(f"{base}/jobs", json={"action": "build"})
    assert first.status_code == 202, first.text
    assert first.json()["status"] == "queued"
    duplicate = await async_client.post(f"{base}/jobs", json={"action": "send"})
    assert duplicate.json()["id"] == first.json()["id"]
    assert duplicate.json()["action"] == "build"  # Do not silently add a send action.
    before = (await async_client.get(f"{base}/jobs")).json()
    assert before["latest"] is None
    observed_progress = []
    original_report = process_service._pricelist_progress

    async def observe_progress(callback, stage, percent):
        await original_report(callback, stage, percent)
        snapshot = (await async_client.get(f"{base}/jobs")).json()["job"]
        assert snapshot["status"] == "running"
        assert snapshot["progress"] == percent
        assert snapshot["stage"] == stage
        observed_progress.append(percent)

    monkeypatch.setattr(process_service, "_pricelist_progress", observe_progress)
    await run_pricelist_jobs(async_sessionmaker(test_engine, expire_on_commit=False))
    assert len(observed_progress) >= 5
    assert observed_progress == sorted(observed_progress)
    after = (await async_client.get(f"{base}/jobs")).json()
    assert after["job"]["status"] == "succeeded", after
    assert after["job"]["progress"] == 100
    assert after["latest"]["id"] == after["job"]["pricelist_id"]
    download = await async_client.get(f"{base}/drafts/{after['latest']['id']}/download")
    assert download.status_code == 200, download.text
    assert download.content[:2] == b"PK"
    # No generation work is needed to download the same artifact again.
    download_again = await async_client.get(f"{base}/drafts/{after['latest']['id']}/download")
    assert download_again.content == download.content


@pytest.mark.asyncio
async def test_background_job_send_saved_file_does_not_regenerate(
    test_session,
    test_engine,
    async_client,
    created_customers,
    created_brand,
    monkeypatch,
):
    from sqlalchemy.ext.asyncio import async_sessionmaker

    from dz_fastapi.models.partner import CustomerPriceList
    from dz_fastapi.services import customer_pricelist_jobs as jobs

    await _job_admin(test_session)
    account = await _account(
        test_session,
        name="saved-job",
        email="price@example.ru",
        transport="smtp",
        smtp_host="smtp.example.ru",
    )
    customer = created_customers[0]
    config = await _prepare_config(
        test_session, customer, created_brand, outgoing_account_id=account.id
    )
    base = f"/customers/{customer.id}/pricelist-configs/{config.id}"
    factory = async_sessionmaker(test_engine, expire_on_commit=False)
    await async_client.post(f"{base}/jobs", json={"action": "build"})
    await jobs.run_pricelist_jobs(factory)
    state = (await async_client.get(f"{base}/jobs")).json()
    assert state["job"]["status"] == "succeeded", state
    price_id = state["latest"]["id"]

    async def must_not_generate(**kwargs):
        pytest.fail("Sending a saved file must not regenerate it")

    monkeypatch.setattr(jobs, "process_customer_pricelist", must_not_generate)
    response = await async_client.post(
        f"{base}/jobs",
        json={
            "action": "send",
            "pricelist_id": price_id,
        },
    )
    assert response.status_code == 202, response.text
    await jobs.run_pricelist_jobs(factory)
    state = (await async_client.get(f"{base}/jobs")).json()
    assert state["job"]["status"] == "succeeded", state
    assert state["job"]["delivery_status"] == "queued"
    rows = (
        await test_session.scalars(
            select(EmailOutbox).where(
                EmailOutbox.source_id == price_id,
            )
        )
    ).all()
    assert len(rows) == 1
    assert rows[0].status.value == "pending"
    assert rows[0].to_email == "client@example.com"
    assert list(await test_session.scalars(select(CustomerPriceList.id))) == [price_id]


@pytest.mark.asyncio
async def test_interrupted_job_is_not_replayed_and_only_own_lock_is_cleared(
    test_session,
    test_engine,
    created_customers,
    created_brand,
    monkeypatch,
):
    from sqlalchemy.ext.asyncio import async_sessionmaker

    from dz_fastapi.models.pricelist_job import CustomerPricelistJob
    from dz_fastapi.services import customer_pricelist_jobs as jobs

    config = await _prepare_config(test_session, created_customers[0], created_brand)
    job = CustomerPricelistJob(
        customer_id=config.customer_id,
        config_id=config.id,
        action="send",
        status="running",
        progress=95,
        stage="Отправка",
    )
    test_session.add(job)
    await test_session.flush()
    config.generation_lock_token = f"pricelist-job-{job.id}"
    await test_session.commit()

    async def must_not_replay(**kwargs):
        pytest.fail("Interrupted sending must never be replayed automatically")

    monkeypatch.setattr(jobs, "process_customer_pricelist", must_not_replay)
    await jobs.run_pricelist_jobs(async_sessionmaker(test_engine, expire_on_commit=False))
    await test_session.refresh(job)
    await test_session.refresh(config)
    assert job.status == "failed"
    assert "проверьте" in job.error
    assert config.generation_lock_token is None


@pytest.mark.asyncio
async def test_job_worker_lock_excludes_second_worker(
    test_session,
    test_engine,
    created_customers,
    created_brand,
):
    from sqlalchemy import text
    from sqlalchemy.ext.asyncio import async_sessionmaker

    from dz_fastapi.models.pricelist_job import CustomerPricelistJob
    from dz_fastapi.services.customer_pricelist_jobs import WORKER_LOCK, run_pricelist_jobs

    config = await _prepare_config(test_session, created_customers[0], created_brand)
    job = CustomerPricelistJob(customer_id=config.customer_id, config_id=config.id, action="build")
    test_session.add(job)
    await test_session.commit()
    async with test_engine.connect() as connection:
        await connection.execute(text("SELECT pg_advisory_lock(:key)"), {"key": WORKER_LOCK})
        await connection.commit()
        try:
            await run_pricelist_jobs(async_sessionmaker(test_engine, expire_on_commit=False))
            await test_session.refresh(job)
            assert job.status == "queued"
        finally:
            await connection.execute(text("SELECT pg_advisory_unlock(:key)"), {"key": WORKER_LOCK})
            await connection.commit()


@pytest.mark.asyncio
async def test_job_rejects_cross_customer_file(
    test_session,
    async_client,
    created_customers,
    created_brand,
):
    from dz_fastapi.models.partner import CustomerPriceList

    await _job_admin(test_session)
    config = await _prepare_config(test_session, created_customers[0], created_brand)
    other = CustomerPriceList(customer_id=created_customers[1].id)
    test_session.add(other)
    await test_session.commit()
    response = await async_client.post(
        f"/customers/{config.customer_id}/pricelist-configs/{config.id}/jobs",
        json={"action": "send", "pricelist_id": other.id},
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_concurrent_enqueues_share_one_job(
    test_session, test_engine, created_customers, created_brand
):
    import asyncio

    from sqlalchemy.ext.asyncio import async_sessionmaker

    from dz_fastapi.models.pricelist_job import CustomerPricelistJob
    from dz_fastapi.services.customer_pricelist_jobs import enqueue_job

    config = await _prepare_config(test_session, created_customers[0], created_brand)
    customer_id, config_id = config.customer_id, config.id
    factory = async_sessionmaker(test_engine, expire_on_commit=False)

    async def enqueue():
        async with factory() as session:
            job = await enqueue_job(
                session, customer_id=customer_id, config_id=config_id, action="build", user_id=None
            )
            return job.id

    ids = await asyncio.gather(enqueue(), enqueue())
    assert ids[0] == ids[1]
    assert len(list(await test_session.scalars(select(CustomerPricelistJob.id)))) == 1


@pytest.mark.asyncio
async def test_missing_saved_file_must_not_trigger_new_build(
    test_session,
    test_engine,
    created_customers,
    created_brand,
    monkeypatch,
):
    from sqlalchemy.ext.asyncio import async_sessionmaker

    from dz_fastapi.models.pricelist_job import CustomerPricelistJob
    from dz_fastapi.services import customer_pricelist_jobs as jobs

    config = await _prepare_config(test_session, created_customers[0], created_brand)
    # ON DELETE SET NULL may remove the selected artifact while a job is waiting.
    job = CustomerPricelistJob(
        customer_id=config.customer_id,
        config_id=config.id,
        action="send",
        use_saved_file=True,
        pricelist_id=None,
    )
    test_session.add(job)
    await test_session.commit()

    async def must_not_generate(**kwargs):
        pytest.fail("A different version must never be generated instead of a missing saved file")

    monkeypatch.setattr(jobs, "process_customer_pricelist", must_not_generate)
    await jobs.run_pricelist_jobs(async_sessionmaker(test_engine, expire_on_commit=False))
    await test_session.refresh(job)
    assert job.status == "failed"
    assert "Файл больше недоступен" in job.error
    await test_session.refresh(config)
    assert config.generation_lock_token is None
