import base64
import hashlib
import hmac
import json
import time

import pytest

from dz_fastapi.services.resend_webhooks import InvalidResendWebhook, verify_resend_webhook


def _signed_headers(payload: bytes, secret: str, *, timestamp: int | None = None):
    message_id = "msg_test_123"
    timestamp = timestamp or int(time.time())
    encoded_secret = secret.removeprefix("whsec_")
    key = base64.urlsafe_b64decode(encoded_secret + "=" * (-len(encoded_secret) % 4))
    signed = f"{message_id}.{timestamp}.".encode() + payload
    signature = base64.b64encode(
        hmac.new(key, signed, hashlib.sha256).digest()
    ).decode()
    return {
        "svix-id": message_id,
        "svix-timestamp": str(timestamp),
        "svix-signature": f"v1,{signature}",
        "content-type": "application/json",
    }


def test_verify_resend_webhook_accepts_valid_raw_payload():
    secret = "whsec_" + base64.urlsafe_b64encode(b"test-secret").decode().rstrip("=")
    payload = b'{"type":"email.received"}'
    headers = _signed_headers(payload, secret)

    verify_resend_webhook(payload=payload, headers=headers, secret=secret)


def test_verify_resend_webhook_rejects_changed_payload():
    secret = "whsec_" + base64.urlsafe_b64encode(b"test-secret").decode().rstrip("=")
    payload = b'{"type":"email.received"}'
    headers = _signed_headers(payload, secret)

    with pytest.raises(InvalidResendWebhook, match="подпись"):
        verify_resend_webhook(
            payload=b'{"type":"email.sent"}',
            headers=headers,
            secret=secret,
        )


@pytest.mark.asyncio
async def test_resend_webhook_ingests_received_email(
    async_client,
    monkeypatch,
):
    secret = "whsec_" + base64.urlsafe_b64encode(b"test-secret").decode().rstrip("=")
    monkeypatch.setenv("RESEND_WEBHOOK_SECRET", secret)
    captured = {}

    async def fake_ingest(_session, *, event_data):
        captured.update(event_data)
        return {"status": "stored", "email_id": event_data["email_id"]}

    monkeypatch.setattr(
        "dz_fastapi.api.resend_webhook.ingest_resend_received_email",
        fake_ingest,
    )
    payload = json.dumps(
        {
            "type": "email.received",
            "data": {
                "email_id": "email_123",
                "to": ["order@dragonzap.online"],
                "from": "supplier@example.com",
                "subject": "RE: order 866",
            },
        },
        separators=(",", ":"),
    ).encode()

    response = await async_client.post(
        "/webhooks/resend",
        content=payload,
        headers=_signed_headers(payload, secret),
    )

    assert response.status_code == 200
    assert response.json() == {"status": "stored", "email_id": "email_123"}
    assert captured["to"] == ["order@dragonzap.online"]


@pytest.mark.asyncio
async def test_resend_webhook_rejects_missing_event_data(
    async_client,
    monkeypatch,
):
    secret = "whsec_" + base64.urlsafe_b64encode(b"test-secret").decode().rstrip("=")
    monkeypatch.setenv("RESEND_WEBHOOK_SECRET", secret)
    payload = b'{"type":"email.received"}'

    response = await async_client.post(
        "/webhooks/resend",
        content=payload,
        headers=_signed_headers(payload, secret),
    )

    assert response.status_code == 400
    assert "data" in response.json()["detail"]
