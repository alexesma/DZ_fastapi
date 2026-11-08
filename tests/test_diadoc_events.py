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
