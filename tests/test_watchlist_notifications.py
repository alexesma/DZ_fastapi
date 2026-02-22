import pytest

from dz_fastapi.models.partner import Provider, ProviderPriceListConfig
from dz_fastapi.services.watchlist import handle_provider_pricelist_watch


@pytest.mark.asyncio
async def test_watchlist_provider_telegram(
        async_client, test_session, monkeypatch
):
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

    calls = {"count": 0}

    async def fake_send_message(text, chat_id=None):
        calls["count"] += 1
        assert "TESTBRAND" in text

    monkeypatch.setattr(
        "dz_fastapi.services.watchlist.send_message_to_telegram",
        fake_send_message,
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

    assert calls["count"] == 1
