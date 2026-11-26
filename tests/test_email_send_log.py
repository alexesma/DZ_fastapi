"""Outgoing mail journal and short-interval retries."""

from datetime import timedelta

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.pool import NullPool

from dz_fastapi.core.config import settings
from dz_fastapi.core.time import now_moscow
from dz_fastapi.models.email_send_log import EmailSendLog
from dz_fastapi.services import email as email_service
from dz_fastapi.services import email_journal, email_retry


@pytest.fixture()
def journal_on(monkeypatch, test_schema_name):
    monkeypatch.setenv("EMAIL_LOG_IN_TESTS", "1")
    url = settings.get_database_url(test=True).replace("+asyncpg", "+psycopg2")
    engine = create_engine(
        url,
        poolclass=NullPool,
        connect_args={"options": f"-csearch_path={test_schema_name}"},
    )
    monkeypatch.setattr(email_journal, "_engine", engine)
    yield
    engine.dispose()


async def _rows(session):
    session.expire_all()
    return (await session.execute(select(EmailSendLog).order_by(EmailSendLog.id))).scalars().all()


@pytest.mark.asyncio
async def test_failed_send_is_journaled_with_error_and_queued_for_retry(
    monkeypatch, test_session, journal_on, tmp_path,
):
    monkeypatch.setattr(email_journal, "RETRY_DIR", str(tmp_path))
    monkeypatch.setattr(
        email_service, "_send_email_with_attachment_impl", lambda *a, **k: False
    )

    ok = email_service.send_email_with_attachment(
        "client@example.com", "Прайс", "<p>hi</p>", b"data", "price.xlsx",
        True, log_source="pricelist", log_source_id=7,
    )

    assert ok is False
    (row,) = await _rows(test_session)
    assert (row.status, row.attempts, row.source, row.source_id) == ("retry", 1, "pricelist", 7)
    assert row.to_email == "client@example.com"
    assert row.next_retry_at is not None
    assert row.body == "<p>hi</p>" and row.attachment_path
    assert row.events[0]["ok"] is False


@pytest.mark.asyncio
async def test_successful_send_is_journaled_without_body(
    monkeypatch, test_session, journal_on,
):
    monkeypatch.setattr(
        email_service, "_send_email_with_attachment_impl", lambda *a, **k: True
    )

    assert email_service.send_email_with_attachment(
        "client@example.com", "Прайс", "body"
    ) is True

    (row,) = await _rows(test_session)
    assert (row.status, row.attempts, row.body) == ("sent", 1, None)
    assert row.sent_at is not None


@pytest.mark.asyncio
async def test_retry_job_resends_with_growing_pause_then_gives_up(
    monkeypatch, test_session, journal_on, tmp_path,
):
    monkeypatch.setattr(email_journal, "RETRY_DIR", str(tmp_path))
    monkeypatch.setattr(
        email_service, "_send_email_with_attachment_impl", lambda *a, **k: False
    )
    email_service.send_email_with_attachment(
        "client@example.com", "Прайс", "body", b"x", "p.xlsx", False,
    )
    sends = []

    def failing(*args, **kwargs):
        sends.append(args[0])
        return False

    monkeypatch.setattr(email_retry, "_send_email_with_attachment_impl", failing)
    alerts = []

    async def fake_alert(session, row):
        alerts.append(row.id)

    monkeypatch.setattr(email_retry, "_alert_failed", fake_alert)

    pauses = []
    for _ in range(4):
        (row,) = await _rows(test_session)
        assert row.status == "retry"
        row.next_retry_at = now_moscow() - timedelta(seconds=1)
        await test_session.commit()
        await email_retry.retry_due_emails(test_session)
        (row,) = await _rows(test_session)
        if row.status == "retry":
            pauses.append(round((row.next_retry_at - now_moscow()).total_seconds() / 60))

    assert pauses == [2, 3, 5]
    (row,) = await _rows(test_session)
    assert (row.status, row.attempts) == ("failed", 5)
    assert len(sends) == 4 and alerts == [row.id]

    # Not retried any more by the job.
    await email_retry.retry_due_emails(test_session)
    assert len(sends) == 4


@pytest.mark.asyncio
async def test_retry_job_marks_sent_and_drops_body(
    monkeypatch, test_session, journal_on, tmp_path,
):
    monkeypatch.setattr(email_journal, "RETRY_DIR", str(tmp_path))
    monkeypatch.setattr(
        email_service, "_send_email_with_attachment_impl", lambda *a, **k: False
    )
    email_service.send_email_with_attachment(
        "client@example.com", "Прайс", "body", b"x", "p.xlsx", False,
    )
    monkeypatch.setattr(email_retry, "_send_email_with_attachment_impl", lambda *a, **k: True)
    (row,) = await _rows(test_session)
    row.next_retry_at = now_moscow() - timedelta(seconds=1)
    await test_session.commit()

    result = await email_retry.retry_due_emails(test_session)

    assert result["sent"] == 1
    (row,) = await _rows(test_session)
    assert (row.status, row.attempts, row.body, row.attachment_path) == ("sent", 2, None, None)
