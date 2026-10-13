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
        await test_session.execute(
            select(CustomerPriceList).where(CustomerPriceList.customer_config_id == config.id)
        )
    ).scalars().all()
    assert len(built) == 1
    assert built[0].published_at is not None
    assert built[0].sent_at is None
