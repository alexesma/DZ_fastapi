import pytest

from dz_fastapi.services.watchlist_site import check_watchlist_site


@pytest.mark.asyncio
async def test_watchlist_site_telegram(
        async_client, test_session, monkeypatch
):
    payload = {
        "brand": "SITEBRAND",
        "oem": "SITE123",
        "max_price": 200.0,
    }
    response = await async_client.post("/watchlist", json=payload)
    assert response.status_code == 201

    class FakeClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def get_offers(self, oem, brand, without_cross=True):
            return [{"cost": "150", "qnt": "2"}]

    calls = {"count": 0}

    async def fake_send_message(text, chat_id=None):
        calls["count"] += 1
        assert "SITEBRAND" in text

    monkeypatch.setattr(
        "dz_fastapi.services.watchlist_site.DZSiteClient",
        lambda *args, **kwargs: FakeClient(),
    )
    monkeypatch.setattr(
        "dz_fastapi.services.watchlist_site.send_message_to_telegram",
        fake_send_message,
    )

    await check_watchlist_site(test_session)
    assert calls["count"] == 1
