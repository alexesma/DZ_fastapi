import pytest

from dz_fastapi.services.email_outbox import (
    claim_pending_outbox,
    enqueue_email,
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
