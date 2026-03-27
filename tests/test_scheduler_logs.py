from types import SimpleNamespace

import pytest

from dz_fastapi.services.scheduler import (_notify_scheduler_issue,
                                           download_price_provider_task)


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

    response = await async_client.get(
        "/alerts/price-check-logs", params={"limit": 5}
    )
    assert response.status_code == 200
    data = response.json()
    assert len(data) >= 1


@pytest.mark.asyncio
async def test_notify_scheduler_issue_falls_back_to_email(monkeypatch):
    sent = {}

    async def fake_telegram(text):
        raise RuntimeError('telegram down')

    async def fake_accounts(session, purpose):
        assert purpose == 'reports_out'
        return [
            SimpleNamespace(
                email='reports@example.com',
                transport='smtp',
                smtp_host='smtp.example.com',
                smtp_port=465,
                smtp_use_ssl=True,
                password='smtp-pass',
            )
        ]

    def fake_send_email_message(**kwargs):
        sent.update(kwargs)
        return True

    monkeypatch.setenv('EMAIL_NAME_ANALYTIC', 'analytic@example.com')
    monkeypatch.setattr(
        'dz_fastapi.services.scheduler.send_message_to_telegram',
        fake_telegram,
    )
    monkeypatch.setattr(
        (
            'dz_fastapi.services.scheduler.'
            'crud_email_account.get_active_by_purpose'
        ),
        fake_accounts,
    )
    monkeypatch.setattr(
        'dz_fastapi.services.scheduler.send_email_message',
        fake_send_email_message,
    )

    await _notify_scheduler_issue(
        session=None,
        subject='Broken task',
        text='Something failed',
    )

    assert sent['to_email'] == 'analytic@example.com'
    assert sent['subject'] == 'Broken task'
    assert sent['body'] == 'Something failed'
    assert sent['smtp_host'] == 'smtp.example.com'
