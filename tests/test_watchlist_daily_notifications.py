import pytest

from dz_fastapi.core.time import now_moscow
from dz_fastapi.crud.watchlist import crud_price_watch_item
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

    sent_messages = []

    async def fake_send_message(text, chat_id=None, parse_mode=None):
        sent_messages.append(text)
        assert parse_mode == "HTML"

    monkeypatch.setattr(
        "dz_fastapi.services.watchlist.DZSiteClient",
        lambda *args, **kwargs: FakeClient(),
    )
    monkeypatch.setattr(
        "dz_fastapi.services.watchlist.send_message_to_telegram",
        fake_send_message,
    )

    await send_watchlist_daily_notifications(test_session)

    assert len(sent_messages) == 1
    assert "<b>Отслеживаемые позиции:</b>" in sent_messages[0]
    assert SITE_ITEM_SEPARATOR in sent_messages[0]
    assert "<b>AAA 111</b>" in sent_messages[0]
    assert "<b>Прайс:</b> Цена 95.0 | Поставщик 77" in sent_messages[0]
    assert "<b>Сайт:</b>" in sent_messages[0]
    assert "<b>BBB 222</b>" in sent_messages[0]
    assert "4. " not in sent_messages[0]


@pytest.mark.asyncio
async def test_watchlist_daily_notification_falls_back_to_email(
    test_session,
    monkeypatch,
):
    monkeypatch.setenv("WATCHLIST_NOTIFY_MODE", "daily")
    monkeypatch.setenv("EMAIL_NAME_ANALYTIC", "analytic@example.com")

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

    async def fake_send_message(*args, **kwargs):
        raise TimeoutError("telegram timeout")

    email_calls = []

    def fake_send_email_message(**kwargs):
        email_calls.append(kwargs)
        return True

    monkeypatch.setattr(
        "dz_fastapi.services.watchlist.send_message_to_telegram",
        fake_send_message,
    )
    monkeypatch.setattr(
        "dz_fastapi.services.watchlist.send_email_message",
        fake_send_email_message,
    )

    await send_watchlist_daily_notifications(test_session)
    await test_session.refresh(item)

    assert len(email_calls) == 1
    assert email_calls[0]["to_email"] == "analytic@example.com"
    assert email_calls[0]["is_html"] is True
    assert item.last_notified_site_at is not None
