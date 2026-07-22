import pytest

from dz_fastapi.models.notification import AppNotificationLevel
from dz_fastapi.models.partner import Provider, ProviderPriceListConfig
from dz_fastapi.models.watchlist import PriceWatchItem
from dz_fastapi.services.watchlist import handle_provider_pricelist_watch


@pytest.mark.asyncio
async def test_watchlist_provider_creates_admin_notification(
    async_client, test_session, monkeypatch
):
    monkeypatch.setenv("WATCHLIST_NOTIFY_MODE", "immediate")
    payload = {
        "brand": "TESTBRAND",
        "oem": "OEM123",
        "max_price": 100.0,
    }
    response = await async_client.post("/watchlist", json=payload)
    assert response.status_code == 201

    provider = Provider(name="Test Provider", type_prices="WHOLESALE")
    test_session.add(provider)
    await test_session.commit()
    await test_session.refresh(provider)

    config = ProviderPriceListConfig(
        provider_id=provider.id,
        start_row=1,
        oem_col=0,
        brand_col=1,
        name_col=2,
        qty_col=3,
        price_col=4,
    )
    test_session.add(config)
    await test_session.commit()
    await test_session.refresh(config)

    sent = {}

    async def fake_notify_admin_all(**kwargs):
        sent.update(kwargs)
        return []

    monkeypatch.setattr(
        "dz_fastapi.services.watchlist.notify_admin_all",
        fake_notify_admin_all,
    )

    items = [{"brand": "TESTBRAND", "oem_number": "OEM123", "price": 99.0, "quantity": 1}]
    await handle_provider_pricelist_watch(
        session=test_session,
        provider=provider,
        provider_config=config,
        pricelist_id=1,
        items=items,
    )

    assert sent["session"] is test_session
    assert sent["title"] == "Подходящая цена: позиция найдена в прайсе"
    assert sent["level"] == AppNotificationLevel.WARNING
    assert sent["link"] == "/watchlist"
    assert sent["commit"] is False
    assert "TESTBRAND OEM123" in sent["message"]


@pytest.mark.asyncio
async def test_watchlist_provider_saves_offer_above_limit_without_notification(
    async_client,
    test_session,
    monkeypatch,
):
    monkeypatch.setenv("WATCHLIST_NOTIFY_MODE", "immediate")
    response = await async_client.post(
        "/watchlist",
        json={
            "brand": "TESTBRAND",
            "oem": "EXPENSIVE123",
            "max_price": 100.0,
        },
    )
    assert response.status_code == 201
    watch_id = response.json()["id"]

    provider = Provider(name="Expensive Provider", type_prices="WHOLESALE")
    test_session.add(provider)
    await test_session.flush()
    config = ProviderPriceListConfig(
        provider_id=provider.id,
        start_row=1,
        oem_col=0,
        brand_col=1,
        name_col=2,
        qty_col=3,
        price_col=4,
    )
    test_session.add(config)
    await test_session.commit()

    notified = False

    async def fake_notify_admin_all(**_kwargs):
        nonlocal notified
        notified = True
        return []

    monkeypatch.setattr(
        "dz_fastapi.services.watchlist.notify_admin_all",
        fake_notify_admin_all,
    )

    await handle_provider_pricelist_watch(
        session=test_session,
        provider=provider,
        provider_config=config,
        pricelist_id=123,
        items=[
            {
                "brand": "TESTBRAND",
                "oem_number": "EXPENSIVE123",
                "price": 150.0,
                "quantity": 2,
            }
        ],
    )

    item = await test_session.get(PriceWatchItem, watch_id)
    assert item.last_seen_provider_price == 150.0
    assert item.last_seen_provider_id == provider.id
    assert item.last_seen_provider_config_id == config.id
    assert item.last_seen_provider_pricelist_id == 123
    assert notified is False
