import pytest

from dz_fastapi.models.notification import AppNotificationLevel
from dz_fastapi.models.partner import Provider, ProviderPriceListConfig
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

    async def fake_create_admin_notifications(**kwargs):
        sent.update(kwargs)
        return []

    monkeypatch.setattr(
        "dz_fastapi.services.watchlist.create_admin_notifications",
        fake_create_admin_notifications,
    )

    items = [
        {
            "brand": "TESTBRAND",
            "oem_number": "OEM123",
            "price": 99.0, "quantity": 1
        }
    ]
    await handle_provider_pricelist_watch(
        session=test_session,
        provider=provider,
        provider_config=config,
        pricelist_id=1,
        items=items,
    )

    assert sent["session"] is test_session
    assert sent["title"] == "Watchlist: позиция найдена в прайсе"
    assert sent["level"] == AppNotificationLevel.INFO
    assert sent["link"] == "/watchlist"
    assert sent["commit"] is False
    assert "TESTBRAND OEM123" in sent["message"]
