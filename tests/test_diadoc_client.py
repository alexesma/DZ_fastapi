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
