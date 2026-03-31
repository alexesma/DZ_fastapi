import pytest

from dz_fastapi.core.time import now_moscow
from dz_fastapi.crud.watchlist import crud_price_watch_item
from dz_fastapi.models.notification import AppNotificationLevel
from dz_fastapi.services.watchlist import (SITE_ITEM_SEPARATOR,
                                           send_watchlist_daily_notifications)


@pytest.mark.asyncio
async def test_watchlist_daily_notification_site_separator_and_top3(
    test_session,
    monkeypatch,
):
    monkeypatch.setenv("WATCHLIST_NOTIFY_MODE", "daily")
    monkeypatch.setenv("KEY_FOR_WEBSITE", "test")

    item_one = await crud_price_watch_item.create(
        test_session,
        brand="AAA",
        oem="111",
        max_price=500.0,
    )
    item_two = await crud_price_watch_item.create(
        test_session,
        brand="BBB",
        oem="222",
        max_price=500.0,
    )

    now = now_moscow()
    item_one.last_seen_provider_at = now
    item_one.last_seen_provider_price = 95.0
    item_one.last_seen_provider_id = 77
    item_one.last_seen_site_at = now
    item_one.last_seen_site_price = 100.0
    item_one.last_seen_site_qty = 1
    item_two.last_seen_site_at = now
    item_two.last_seen_site_price = 200.0
    item_two.last_seen_site_qty = 2
    test_session.add(item_one)
    test_session.add(item_two)
    await test_session.commit()

    class FakeClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def get_offers(self, oem, brand, without_cross=True):
            return [
                {"cost": "100", "qnt": "1", "price_name": "S1"},
                {"cost": "101", "qnt": "2", "price_name": "S2"},
                {"cost": "102", "qnt": "3", "price_name": "S3"},
                {"cost": "103", "qnt": "4", "price_name": "S4"},
            ]

    sent = {}

    async def fake_create_admin_notifications(**kwargs):
        sent.update(kwargs)
        return []

    monkeypatch.setattr(
        "dz_fastapi.services.watchlist.DZSiteClient",
        lambda *args, **kwargs: FakeClient(),
    )
    monkeypatch.setattr(
        "dz_fastapi.services.watchlist.create_admin_notifications",
        fake_create_admin_notifications,
    )

    await send_watchlist_daily_notifications(test_session)

    assert sent["session"] is test_session
    assert sent["title"] == "Watchlist: сводка по найденным позициям"
    assert sent["level"] == AppNotificationLevel.INFO
    assert sent["link"] == "/watchlist"
    assert sent["commit"] is False
    assert "Отслеживаемые позиции:" in sent["message"]
    assert SITE_ITEM_SEPARATOR in sent["message"]
    assert "AAA 111" in sent["message"]
    assert "Прайс: цена 95.0 | Поставщик 77" in sent["message"]
    assert "Сайт:" in sent["message"]
    assert "BBB 222" in sent["message"]
    assert "4. " not in sent["message"]


@pytest.mark.asyncio
async def test_watchlist_daily_notification_marks_items_after_notification(
    test_session,
    monkeypatch,
):
    monkeypatch.setenv("WATCHLIST_NOTIFY_MODE", "daily")

    item = await crud_price_watch_item.create(
        test_session,
        brand="CCC",
        oem="333",
        max_price=500.0,
    )
    now = now_moscow()
    item.last_seen_site_at = now
    item.last_seen_site_price = 123.0
    item.last_seen_site_qty = 4
    test_session.add(item)
    await test_session.commit()

    sent = {}

    async def fake_create_admin_notifications(**kwargs):
        sent.update(kwargs)
        return []

    monkeypatch.setattr(
        "dz_fastapi.services.watchlist.create_admin_notifications",
        fake_create_admin_notifications,
    )

    await send_watchlist_daily_notifications(test_session)
    await test_session.refresh(item)

    assert sent["title"] == "Watchlist: сводка по найденным позициям"
    assert "CCC 333" in sent["message"]
    assert item.last_notified_site_at is not None
