import pytest

from dz_fastapi.core.time import now_moscow
from dz_fastapi.crud.watchlist import crud_price_watch_item
from dz_fastapi.models.notification import AppNotificationLevel
from dz_fastapi.services.watchlist import SITE_ITEM_SEPARATOR, send_watchlist_daily_notifications


@pytest.mark.asyncio
async def test_watchlist_daily_notification_uses_saved_site_snapshot(
    test_session,
    monkeypatch,
):
    monkeypatch.setenv("WATCHLIST_NOTIFY_MODE", "daily")

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
    item_one.last_seen_site_offers = [
        {
            "price": 100.0,
            "qty": 1,
            "supplier_name": "S1",
            "min_delivery_day": 1,
            "max_delivery_day": 2,
        },
        {
            "price": 101.0,
            "qty": 2,
            "supplier_name": "S2",
            "min_delivery_day": 2,
            "max_delivery_day": 3,
        },
    ]
    item_two.last_seen_site_at = now
    item_two.last_seen_site_price = 200.0
    item_two.last_seen_site_qty = 2
    test_session.add(item_one)
    test_session.add(item_two)
    await test_session.commit()

    sent = {}

    async def fake_notify_admin_all(**kwargs):
        sent.update(kwargs)
        return []

    monkeypatch.setattr(
        "dz_fastapi.services.watchlist.notify_admin_all",
        fake_notify_admin_all,
    )

    await send_watchlist_daily_notifications(test_session)

    assert sent["session"] is test_session
    assert sent["title"] == "Подходящая цена: сводка по найденным позициям"
    assert sent["level"] == AppNotificationLevel.WARNING
    assert sent["link"] == "/watchlist"
    assert sent["commit"] is False
    assert "Отслеживаемые позиции:" in sent["message"]
    assert SITE_ITEM_SEPARATOR in sent["message"]
    assert "AAA 111" in sent["message"]
    assert "Прайс: цена 95.0 | Поставщик 77" in sent["message"]
    assert "Сайт:" in sent["message"]
    assert "BBB 222" in sent["message"]
    assert "1. S1 | Цена 100.00 | Кол-во 1 | Срок 1 - 2" in sent["message"]
    assert "2. S2 | Цена 101.00 | Кол-во 2 | Срок 2 - 3" in sent["message"]


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

    async def fake_notify_admin_all(**kwargs):
        sent.update(kwargs)
        return []

    monkeypatch.setattr(
        "dz_fastapi.services.watchlist.notify_admin_all",
        fake_notify_admin_all,
    )

    await send_watchlist_daily_notifications(test_session)
    await test_session.refresh(item)

    assert sent["title"] == "Подходящая цена: сводка по найденным позициям"
    assert "CCC 333" in sent["message"]
    assert item.last_notified_site_at is not None
