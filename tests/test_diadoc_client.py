import httpx
import pytest

from dz_fastapi.http.diadoc_client import DiadocClient


@pytest.mark.asyncio
async def test_json_request_retries_accepted_async_operation(monkeypatch):
    client = DiadocClient("token", base_url="https://diadoc.example")
    responses = [
        httpx.Response(204, headers={"Retry-After": "0"}),
        httpx.Response(200, json={"MessageId": "message"}),
    ]
    calls = []

    async def fake_request(*args, **kwargs):
        calls.append((args, kwargs))
        return responses.pop(0)

    async def no_sleep(delay):
        assert delay == 0.1

    monkeypatch.setattr(client, "_request", fake_request)
    monkeypatch.setattr("dz_fastapi.http.diadoc_client.asyncio.sleep", no_sleep)

    result = await client._request_json(
        "POST",
        "/V3/PostMessage",
        params={"operationId": "same-operation"},
        json_body={"FromBoxId": "box"},
    )

    assert result == {"MessageId": "message"}
    assert len(calls) == 2
    assert calls[0][1] == calls[1][1]


@pytest.mark.asyncio
async def test_event_feed_and_docflow_use_current_api_versions(monkeypatch):
    client = DiadocClient("token", base_url="https://diadoc.example")
    calls = []

    async def fake_json(method, path, **kwargs):
        calls.append((method, path, kwargs))
        return {"Events": []} if "GetNewEvents" in path else {"Documents": []}

    monkeypatch.setattr(client, "_request_json", fake_json)

    await client.get_new_events(
        box_id_guid="box",
        after_index_key="cursor",
        document_direction="Inbound",
    )
    await client.get_docflows(
        box_id_guid="box",
        document_ids=[("message", "entity")],
    )

    assert calls[0][1] == "/V8/GetNewEvents"
    assert calls[0][2]["params"]["afterIndexKey"] == "cursor"
    assert calls[1][1] == "/V5/GetDocflows"
    assert calls[1][2]["json_body"]["Requests"][0]["DocumentId"] == {
        "MessageId": "message",
        "EntityId": "entity",
    }
