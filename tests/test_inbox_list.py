from datetime import timedelta

import pytest

from dz_fastapi.core.time import now_moscow
from dz_fastapi.crud.inbox_email import list_inbox_emails
from dz_fastapi.models.email_account import EmailAccount
from dz_fastapi.models.inbox_email import InboxEmail


async def _seed(session):
    account = EmailAccount(
        name="Info", email="info@example.com", password="x", is_active=True
    )
    session.add(account)
    await session.flush()
    now = now_moscow()

    def mail(subject, **kwargs):
        defaults = dict(
            email_account_id=account.id,
            from_email="a@example.com",
            subject=subject,
            received_at=now - timedelta(hours=1),
            fetched_at=now,
        )
        defaults.update(kwargs)
        return InboxEmail(**defaults)

    session.add_all(
        [
            mail("no rule"),
            mail("pending", rule_type="price_list"),
            mail("done", rule_type="price_list", processed=True),
            mail(
                "broken",
                rule_type="price_list",
                processing_error="не разобран файл",
            ),
            mail("skip", rule_type="ignore"),
            # Загружено сегодня, но пришло 20 дней назад.
            mail("old", received_at=now - timedelta(days=20)),
        ]
    )
    await session.commit()


@pytest.mark.asyncio
async def test_inbox_list_counts_and_status_filter(test_session):
    await _seed(test_session)

    items, total, counts = await list_inbox_emails(test_session, days=3)
    assert total == 5
    assert counts == {
        "no_rule": 1,
        "pending": 1,
        "error": 1,
        "processed": 1,
        "ignored": 1,
    }

    items, total, counts = await list_inbox_emails(
        test_session, days=3, status="error"
    )
    assert [i.subject for i in items] == ["broken"]
    assert total == 1
    # Счётчики не зависят от выбранного статуса.
    assert counts["pending"] == 1


@pytest.mark.asyncio
async def test_inbox_period_uses_received_date_and_supports_ranges(
    test_session,
):
    await _seed(test_session)

    _, total, _ = await list_inbox_emails(test_session, days=3)
    assert total == 5  # «old» загружено сегодня, но получено 20 дней назад

    _, total, _ = await list_inbox_emails(test_session, days=30)
    assert total == 6

    today = now_moscow().date()
    items, total, _ = await list_inbox_emails(
        test_session,
        date_from=today - timedelta(days=25),
        date_to=today - timedelta(days=15),
    )
    assert [i.subject for i in items] == ["old"]
