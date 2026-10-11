import pytest

from dz_fastapi.services.email_outbox import (
    claim_pending_outbox,
    enqueue_email,
    mark_outbox_error,
    mark_outbox_sent,
    renew_outbox_claim,
)


@pytest.mark.asyncio
async def test_relay_can_only_renew_and_finish_its_own_claim(test_session):
    row = await enqueue_email(
        test_session,
        to_email="recipient@example.com",
        subject="Lease test",
    )
    claimed = await claim_pending_outbox(
        test_session,
        worker="relay-a",
        limit=1,
    )
    assert [item.id for item in claimed] == [row.id]
    row_id = row.id

    with pytest.raises(ValueError, match="другому relay"):
        await renew_outbox_claim(
            test_session,
            outbox_id=row_id,
            worker="relay-b",
        )
    await test_session.rollback()

    renewed = await renew_outbox_claim(
        test_session,
        outbox_id=row_id,
        worker="relay-a",
    )
    assert renewed.claimed_by == "relay-a"

    with pytest.raises(ValueError, match="другому relay"):
        await mark_outbox_sent(
            test_session,
            outbox_id=row_id,
            worker="relay-b",
        )
    await test_session.rollback()

    sent = await mark_outbox_sent(
        test_session,
        outbox_id=row_id,
        worker="relay-a",
    )
    assert str(getattr(sent.status, "value", sent.status)) == "sent"
    assert sent.claimed_by is None

    repeated = await mark_outbox_sent(
        test_session,
        outbox_id=row_id,
        worker="relay-a",
    )
    assert repeated.id == row_id
    assert repeated.attempts == 1


@pytest.mark.asyncio
async def test_network_failures_do_not_burn_send_attempts(test_session):
    row = await enqueue_email(
        test_session,
        to_email="recipient@example.com",
        subject="Network test",
    )
    row_id = row.id
    for _ in range(8):
        await claim_pending_outbox(test_session, worker="relay-a", limit=1)
        updated = await mark_outbox_error(
            test_session,
            outbox_id=row_id,
            error="timed out",
            retry=True,
            worker="relay-a",
        )
    assert updated.attempts == 0
    assert str(getattr(updated.status, "value", updated.status)) == "pending"
    assert updated.last_error == "timed out"


@pytest.mark.asyncio
async def test_real_errors_still_exhaust_attempts(test_session):
    row = await enqueue_email(
        test_session,
        to_email="recipient@example.com",
        subject="Auth test",
    )
    row_id = row.id
    for _ in range(5):
        await claim_pending_outbox(test_session, worker="relay-a", limit=1)
        updated = await mark_outbox_error(
            test_session,
            outbox_id=row_id,
            error="535 authentication failed",
            retry=True,
            worker="relay-a",
        )
    assert updated.attempts == 5
    assert str(getattr(updated.status, "value", updated.status)) == "error"


@pytest.mark.asyncio
async def test_mark_error_after_lost_claim_is_ignored_by_api(
    async_client, test_session, monkeypatch
):
    monkeypatch.setenv("EMAIL_RELAY_API_TOKEN", "relay-secret")
    row = await enqueue_email(
        test_session,
        to_email="recipient@example.com",
        subject="Lost claim",
    )
    row_id = row.id
    await claim_pending_outbox(test_session, worker="relay-b", limit=1)

    response = await async_client.post(
        f"/email-outbox/{row_id}/mark-error",
        params={"worker": "relay-a"},
        headers={"X-Email-Relay-Token": "relay-secret"},
        json={"error": "timed out", "retry": True},
    )

    assert response.status_code == 200
    assert response.json()["attempts"] == 0
