import pytest
from sqlalchemy import select

from dz_fastapi.models.diadoc import DiadocBoxEvent
from dz_fastapi.services.diadoc_events import fetch_event_page, stage_event_cursor_boundary


class FakeDiadocClient:
    def __init__(self):
        self.last_event_calls = 0

    async def get_last_event(self, *, box_id_guid):
        self.last_event_calls += 1
        return {"EventId": "boundary", "IndexKey": "index-10"}

    async def get_new_events(self, **kwargs):
        return {
            "Events": [
                {
                    "EventId": "event-11",
                    "IndexKey": "index-11",
                    "Patch": {"MessageId": "message-1"},
                }
            ]
        }


@pytest.mark.asyncio
async def test_event_cursor_boundary_is_stable_and_events_are_durable(test_session):
    client = FakeDiadocClient()
    cursor = await stage_event_cursor_boundary(
        test_session,
        client=client,
        environment="staging",
        box_id_guid="box-1",
    )
    same_cursor = await stage_event_cursor_boundary(
        test_session,
        client=client,
        environment="staging",
        box_id_guid="box-1",
    )

    assert same_cursor.id == cursor.id
    assert same_cursor.index_key == "index-10"
    assert client.last_event_calls == 1

    first = await fetch_event_page(test_session, client=client, cursor=cursor)
    second = await fetch_event_page(test_session, client=client, cursor=cursor)

    assert first["created"] == 1
    assert second["created"] == 0
    assert cursor.index_key == "index-11"
    stored = await test_session.scalar(
        select(DiadocBoxEvent).where(DiadocBoxEvent.event_id == "event-11")
    )
    assert stored.event_id == "event-11"
    assert stored.status == "pending"


@pytest.mark.asyncio
async def test_event_stops_retrying_after_limit_and_can_be_requeued(test_session):
    from dz_fastapi.services import diadoc_events
    from dz_fastapi.services.diadoc_events import (
        MAX_EVENT_ATTEMPTS,
        event_queue_summary,
        process_pending_events,
        retry_failed_events,
    )

    event = DiadocBoxEvent(
        environment="staging",
        box_id_guid="box-9",
        event_id="broken",
        index_key="index-1",
        event_kind="patch",
        message_id="message-9",
        payload={"EventId": "broken", "Patch": {"MessageId": "message-9"}},
        attempt_count=MAX_EVENT_ATTEMPTS - 2,
        status="error",
    )
    test_session.add(event)
    await test_session.commit()

    async def boom(*args, **kwargs):
        raise RuntimeError("Diadoc 404")

    original = diadoc_events._refresh_incoming_docflows
    diadoc_events._refresh_incoming_docflows = boom
    try:
        kwargs = dict(
            client=None,
            environment="staging",
            box_id_guid="box-9",
            download_content=False,
            process_supplier_message=False,
        )
        first = await process_pending_events(test_session, **kwargs)
        assert first["failed"] == 1 and first["gave_up"] == 0
        assert event.status == "error"
        second = await process_pending_events(test_session, **kwargs)
        assert second["gave_up"] == 1
        assert event.status == "failed"
        assert "повторы остановлены" in second["errors"][0]
        third = await process_pending_events(test_session, **kwargs)
        assert third["failed"] == 0 and third["processed"] == 0
    finally:
        diadoc_events._refresh_incoming_docflows = original

    summary = await event_queue_summary(
        test_session, environment="staging", box_id_guid="box-9"
    )
    assert summary["failed"] == 1 and summary["initialized"] is False
    assert summary["failed_examples"][0]["error"] == "Diadoc 404"

    assert await retry_failed_events(
        test_session, environment="staging", box_id_guid="box-9"
    ) == 1
    assert event.status == "pending" and event.attempt_count == 0


def _outgoing(message_id, **extra):
    from dz_fastapi.models.diadoc import DiadocOutgoingDocument

    return DiadocOutgoingDocument(
        environment="staging",
        from_box_id_guid="box-out",
        to_box_id_guid="box-peer",
        file_name="upd.xml",
        local_file_path="uploads/upd.xml",
        message_id=message_id,
        entity_id=f"entity-{message_id}",
        is_draft=False,
        status="sent",
        **extra,
    )


class DocflowClient:
    def __init__(self):
        self.requested = []

    async def get_docflows(self, *, box_id_guid, document_ids):
        self.requested.extend(document_ids)
        return {
            "Documents": [
                {
                    "DocumentId": {"MessageId": m, "EntityId": e},
                    "LastEvent": {
                        "Docflow": {
                            "IsFinished": True,
                            "RecipientResponse": {
                                "ResponseStatus": "WithRecipientSignature"
                            },
                        }
                    },
                }
                for m, e in document_ids
            ]
        }


@pytest.mark.asyncio
async def test_event_for_outgoing_message_updates_its_status(test_session):
    from dz_fastapi.services.diadoc_events import process_pending_events

    signed = _outgoing("message-out-1")
    other = _outgoing("message-out-2")
    test_session.add_all(
        [
            signed,
            other,
            DiadocBoxEvent(
                environment="staging",
                box_id_guid="box-out",
                event_id="event-out-1",
                index_key="index-1",
                event_kind="patch",
                message_id="message-out-1",
                payload={"EventId": "event-out-1", "Patch": {"MessageId": "message-out-1"}},
            ),
        ]
    )
    await test_session.commit()

    client = DocflowClient()
    result = await process_pending_events(
        test_session,
        client=client,
        environment="staging",
        box_id_guid="box-out",
        download_content=False,
        process_supplier_message=False,
    )

    assert result["processed"] == 1 and result["outgoing"] == 1
    assert client.requested == [("message-out-1", "entity-message-out-1")]
    assert signed.status == "completed"
    assert signed.status_checked_at is not None
    assert other.status == "sent"


@pytest.mark.asyncio
async def test_polling_only_rechecks_stale_documents_when_feed_is_active(test_session):
    from datetime import timedelta

    from dz_fastapi.core.time import now_moscow
    from dz_fastapi.services.diadoc_status import refresh_diadoc_outgoing_statuses

    fresh = _outgoing("poll-fresh", status_checked_at=now_moscow())
    stale = _outgoing("poll-stale", status_checked_at=now_moscow() - timedelta(hours=7))
    never = _outgoing("poll-never")
    test_session.add_all([fresh, stale, never])
    await test_session.commit()

    client = DocflowClient()
    result = await refresh_diadoc_outgoing_statuses(
        test_session,
        client=client,
        environment="staging",
        recheck_after=timedelta(hours=6),
    )

    assert result["checked"] == 2
    assert {m for m, _ in client.requested} == {"poll-stale", "poll-never"}
    assert fresh.status == "sent"
