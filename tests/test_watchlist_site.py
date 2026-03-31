import pytest

from dz_fastapi.models.notification import AppNotificationLevel
from dz_fastapi.services.watchlist_site import check_watchlist_site


@pytest.mark.asyncio
async def test_watchlist_site_creates_admin_notification(
        async_client, test_session, monkeypatch
):
    monkeypatch.setenv("WATCHLIST_NOTIFY_MODE", "immediate")
    monkeypatch.setenv("KEY_FOR_WEBSITE", "test")
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
            return [
                {"cost": "150", "qnt": "2", "price_name": "S1"},
                {"cost": "151", "qnt": "3", "price_name": "S2"},
                {"cost": "152", "qnt": "4", "price_name": "S3"},
                {"cost": "153", "qnt": "5", "price_name": "S4"},
            ]

    sent = {}

    async def fake_create_admin_notifications(**kwargs):
        sent.update(kwargs)
        return []

    monkeypatch.setattr(
        "dz_fastapi.services.watchlist_site.DZSiteClient",
        lambda *args, **kwargs: FakeClient(),
    )
    monkeypatch.setattr(
        "dz_fastapi.services.watchlist_site.create_admin_notifications",
        fake_create_admin_notifications,
    )

    await check_watchlist_site(test_session)
    assert sent["session"] is test_session
    assert sent["title"] == "Watchlist: позиция найдена на сайте"
    assert sent["level"] == AppNotificationLevel.INFO
    assert sent["link"] == "/watchlist"
    assert sent["commit"] is False
    assert "SITEBRAND SITE123" in sent["message"]
    assert "Топ 3 предложения:" in sent["message"]
    assert "4. " not in sent["message"]
