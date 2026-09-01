from datetime import datetime, timedelta
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest
from sqlalchemy import select

from dz_fastapi.core.time import now_moscow
from dz_fastapi.models.partner import SupplierOrderMessage, SupplierReceipt
from dz_fastapi.models.settings import CustomerOrderInboxSettings
from dz_fastapi.services.scheduler import (
    _close_stale_supplier_response_messages,
    _cron_minute_for_interval,
    _customer_pricelist_delivery_attempt_handled,
    _latest_due_customer_pricelist_schedule,
    _notify_scheduler_issue,
    _schedule_was_handled,
    _should_run_scheduled_job,
    cleanup_misc_logs_task,
    download_price_provider_task,
    process_new_provider_emails,
)


def test_cron_interval_59_runs_once_per_hour():
    assert _cron_minute_for_interval(59) == "0"
    assert _cron_minute_for_interval(30) == "*/30"


def test_customer_pricelist_schedule_catches_up_after_configured_time():
    now = datetime(2026, 8, 31, 8, 17, tzinfo=ZoneInfo("Europe/Moscow"))
    config = SimpleNamespace(
        id=15,
        schedule_days=["mon"],
        schedule_times=["06:00", "12:00"],
    )

    scheduled_at = _latest_due_customer_pricelist_schedule(config, now)

    assert scheduled_at == datetime(
        2026,
        8,
        31,
        6,
        0,
        tzinfo=ZoneInfo("Europe/Moscow"),
    )
    assert not _schedule_was_handled(
        datetime(2026, 8, 30, 12, 0, tzinfo=ZoneInfo("Europe/Moscow")),
        scheduled_at,
    )
    assert _schedule_was_handled(
        datetime(2026, 8, 31, 6, 4, tzinfo=ZoneInfo("Europe/Moscow")),
        scheduled_at,
    )


def test_customer_pricelist_schedule_uses_latest_due_slot():
    now = datetime(2026, 8, 31, 15, 5, tzinfo=ZoneInfo("Europe/Moscow"))
    config = SimpleNamespace(
        id=16,
        schedule_days=["mon"],
        schedule_times=["06:00", "12:00", "18:00"],
    )

    assert _latest_due_customer_pricelist_schedule(config, now) == datetime(
        2026,
        8,
        31,
        12,
        0,
        tzinfo=ZoneInfo("Europe/Moscow"),
    )


def test_customer_pricelist_failed_delivery_is_not_rebuilt_for_same_slot():
    scheduled_at = datetime(2026, 9, 1, 6, 0, tzinfo=ZoneInfo("Europe/Moscow"))

    assert _customer_pricelist_delivery_attempt_handled(
        datetime(2026, 9, 1, 6, 3, tzinfo=ZoneInfo("Europe/Moscow")),
        scheduled_at,
    )
    assert not _customer_pricelist_delivery_attempt_handled(
        datetime(2026, 9, 1, 5, 59, tzinfo=ZoneInfo("Europe/Moscow")),
        scheduled_at,
    )


@pytest.mark.asyncio
async def test_scheduler_logs_skip(async_client, test_session, monkeypatch):
    async def fake_get_emails(session):
        return []

    monkeypatch.setattr(
        "dz_fastapi.services.scheduler.get_emails",
        fake_get_emails,
    )

    from dz_fastapi.main import app

    await download_price_provider_task(app)

    response = await async_client.get("/alerts/price-check-logs", params={"limit": 5})
    assert response.status_code == 200
    data = response.json()
    assert len(data) >= 1


@pytest.mark.asyncio
async def test_provider_price_summary_keeps_provider_error_details(
    test_session,
    monkeypatch,
):
    provider = SimpleNamespace(id=937, name="COSMOPART")
    provider_config = SimpleNamespace(id=41, name_price="Cosmo.xlsx")

    async def fake_get_emails(*, session):
        return [(provider, "/tmp/Cosmo.xlsx", provider_config)]

    async def fake_process_one(item, app, sem):
        raise RuntimeError()

    monkeypatch.setattr(
        "dz_fastapi.services.scheduler.get_emails",
        fake_get_emails,
    )
    monkeypatch.setattr(
        "dz_fastapi.services.scheduler._process_one",
        fake_process_one,
    )

    summary = await process_new_provider_emails(
        test_session,
        SimpleNamespace(),
    )

    assert summary["errors"] == 1
    assert summary["error_details"] == [
        {
            "provider_id": 937,
            "provider_name": "COSMOPART",
            "provider_config_id": 41,
            "provider_config_name": "Cosmo.xlsx",
            "source_filename": "Cosmo.xlsx",
            "error_type": "RuntimeError",
            "error": "RuntimeError()",
        }
    ]


@pytest.mark.asyncio
async def test_provider_price_summary_separates_review_from_errors(
    test_session,
    monkeypatch,
):
    provider = SimpleNamespace(id=937, name="COSMOPART")
    provider_config = SimpleNamespace(id=53, name_price="Cosmo CS")

    async def fake_get_emails(*, session):
        return [(provider, "/tmp/Cosmo CS.xlsx", provider_config)]

    async def fake_process_one(item, app, sem):
        return {
            "status": "needs_review",
            "provider_id": 937,
            "provider_name": "COSMOPART",
            "provider_config_id": 53,
            "provider_config_name": "Cosmo CS",
            "source_filename": "Cosmo CS.xlsx",
            "message": "Требуется проверка администратора",
        }

    monkeypatch.setattr(
        "dz_fastapi.services.scheduler.get_emails",
        fake_get_emails,
    )
    monkeypatch.setattr(
        "dz_fastapi.services.scheduler._process_one",
        fake_process_one,
    )

    summary = await process_new_provider_emails(
        test_session,
        SimpleNamespace(),
    )

    assert summary["successful"] == 0
    assert summary["errors"] == 0
    assert summary["review_required"] == 1
    assert summary["review_details"][0]["provider_config_id"] == 53


@pytest.mark.asyncio
async def test_notify_scheduler_issue_creates_admin_notification(
    monkeypatch,
):
    sent = {}
    rolled_back = {"value": False}

    class FakeSession:
        async def rollback(self):
            rolled_back["value"] = True

    async def fake_create_admin_notifications(**kwargs):
        sent.update(kwargs)
        return [SimpleNamespace(id=1)]

    monkeypatch.setattr(
        "dz_fastapi.services.scheduler.create_admin_notifications",
        fake_create_admin_notifications,
    )

    session = FakeSession()
    await _notify_scheduler_issue(
        session=session,
        subject="Broken task",
        text="Something failed",
    )

    assert rolled_back["value"] is True
    assert sent["session"] is session
    assert sent["title"] == "Broken task"
    assert sent["message"] == "Something failed"
    assert sent["level"] == "error"
    assert sent["link"] == "/admin/settings"


@pytest.mark.asyncio
async def test_close_stale_supplier_response_messages(
    test_session,
    created_providers,
):
    provider_id = created_providers[0].id
    settings = CustomerOrderInboxSettings(
        lookback_days=1,
        mark_seen=False,
        error_file_retention_days=5,
        supplier_response_lookback_days=14,
        supplier_response_auto_close_stale_enabled=True,
        supplier_response_stale_days=7,
    )
    old_error = SupplierOrderMessage(
        provider_id=provider_id,
        message_type="IMPORT_ERROR",
        sender_email="zakaz@cosmopart.ru",
        subject="Re: Заказ",
        received_at=now_moscow() - timedelta(days=10),
        import_error_details="Ответ распознан, но не сопоставлен",
    )
    old_retry = SupplierOrderMessage(
        provider_id=provider_id,
        message_type="RETRY_PENDING",
        sender_email="zakaz@cosmopart.ru",
        subject="Re: Заказ",
        received_at=now_moscow() - timedelta(days=8),
    )
    fresh_error = SupplierOrderMessage(
        provider_id=provider_id,
        message_type="IMPORT_ERROR",
        sender_email="zakaz@cosmopart.ru",
        subject="Re: Заказ",
        received_at=now_moscow() - timedelta(days=2),
    )
    test_session.add_all([settings, old_error, old_retry, fresh_error])
    await test_session.commit()

    closed_count, stale_days = await _close_stale_supplier_response_messages(test_session)
    assert stale_days == 7
    assert closed_count == 2

    rows = (
        (
            await test_session.execute(
                select(SupplierOrderMessage)
                .where(SupplierOrderMessage.sender_email == "zakaz@cosmopart.ru")
                .order_by(SupplierOrderMessage.id)
            )
        )
        .scalars()
        .all()
    )
    types = [row.message_type for row in rows]
    assert types == ["IGNORED", "IGNORED", "IMPORT_ERROR"]
    assert "Автозакрыто как устаревшее: старше 7 дн." in (rows[0].import_error_details or "")


@pytest.mark.asyncio
async def test_cleanup_keeps_supplier_message_linked_to_receipt(
    test_session,
    created_providers,
):
    provider_id = created_providers[0].id
    linked_message = SupplierOrderMessage(
        provider_id=provider_id,
        message_type="IGNORED",
        received_at=now_moscow() - timedelta(days=10),
    )
    removable_message = SupplierOrderMessage(
        provider_id=provider_id,
        message_type="IGNORED",
        received_at=now_moscow() - timedelta(days=10),
    )
    test_session.add_all([linked_message, removable_message])
    await test_session.flush()
    test_session.add(
        SupplierReceipt(
            provider_id=provider_id,
            source_message_id=linked_message.id,
        )
    )
    await test_session.commit()

    from dz_fastapi.main import app

    await cleanup_misc_logs_task(app)

    remaining_ids = set(
        (
            await test_session.execute(
                select(SupplierOrderMessage.id).where(
                    SupplierOrderMessage.id.in_(
                        [linked_message.id, removable_message.id]
                    )
                )
            )
        ).scalars()
    )
    assert linked_message.id in remaining_ids
    assert removable_message.id not in remaining_ids


@pytest.mark.asyncio
async def test_should_run_scheduled_job_allows_cleanup_catch_up(
    monkeypatch,
):
    now = now_moscow().replace(hour=3, minute=10, second=0, microsecond=0)
    setting = SimpleNamespace(
        enabled=True,
        days=[],
        times=["02:30"],
        last_run_at=now - timedelta(days=1),
    )

    async def fake_get_or_create(session, key, defaults=None):
        assert key == "cleanup_old_pricelists"
        return setting

    monkeypatch.setattr(
        "dz_fastapi.services.scheduler.crud_scheduler_setting.get_or_create",
        fake_get_or_create,
    )
    monkeypatch.setattr("dz_fastapi.services.scheduler.now_moscow", lambda: now)

    should_run, resolved_setting = await _should_run_scheduled_job(
        session=SimpleNamespace(),
        key="cleanup_old_pricelists",
    )

    assert should_run is True
    assert resolved_setting is setting


@pytest.mark.asyncio
async def test_should_run_scheduled_job_skips_cleanup_outside_catch_up_window(
    monkeypatch,
):
    now = now_moscow().replace(hour=10, minute=0, second=0, microsecond=0)
    setting = SimpleNamespace(
        enabled=True,
        days=[],
        times=["02:30"],
        last_run_at=now - timedelta(days=1),
    )

    async def fake_get_or_create(session, key, defaults=None):
        assert key == "cleanup_old_pricelists"
        return setting

    monkeypatch.setattr(
        "dz_fastapi.services.scheduler.crud_scheduler_setting.get_or_create",
        fake_get_or_create,
    )
    monkeypatch.setattr("dz_fastapi.services.scheduler.now_moscow", lambda: now)

    should_run, resolved_setting = await _should_run_scheduled_job(
        session=SimpleNamespace(),
        key="cleanup_old_pricelists",
    )

    assert should_run is False
    assert resolved_setting is setting


@pytest.mark.asyncio
async def test_should_run_scheduled_job_allows_watchlist_notify_catch_up(
    monkeypatch,
):
    now = now_moscow().replace(hour=9, minute=20, second=0, microsecond=0)
    setting = SimpleNamespace(
        enabled=True,
        days=[],
        times=["09:00"],
        last_run_at=now - timedelta(days=1),
    )

    async def fake_get_or_create(session, key, defaults=None):
        assert key == "watchlist_notify"
        return setting

    monkeypatch.setattr(
        "dz_fastapi.services.scheduler.crud_scheduler_setting.get_or_create",
        fake_get_or_create,
    )
    monkeypatch.setattr("dz_fastapi.services.scheduler.now_moscow", lambda: now)

    should_run, resolved_setting = await _should_run_scheduled_job(
        session=SimpleNamespace(),
        key="watchlist_notify",
    )

    assert should_run is True
    assert resolved_setting is setting
