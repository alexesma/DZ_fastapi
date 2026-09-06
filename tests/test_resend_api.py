import httpx

from dz_fastapi.services import resend_api


def test_resend_retries_write_timeout_with_idempotency_key(monkeypatch):
    calls = []

    def fake_post(url, *, headers, json, timeout):
        calls.append(
            {
                "url": url,
                "headers": headers,
                "json": json,
                "timeout": timeout,
            }
        )
        if len(calls) == 1:
            raise httpx.WriteTimeout("upload timed out")
        return httpx.Response(
            200,
            json={"id": "email-123"},
            request=httpx.Request("POST", url),
        )

    monkeypatch.setattr(resend_api.httpx, "post", fake_post)
    monkeypatch.setattr(resend_api.time, "sleep", lambda _seconds: None)

    assert resend_api.send_email_via_resend(
        api_key="re_test",
        from_email="price@dragonzap.online",
        to_email=["client@example.com"],
        subject="Price",
        body="Attached",
        is_html=False,
        attachment_bytes=b"xlsx",
        attachment_filename="price.xlsx",
        timeout=20,
        idempotency_key="customer-pricelist/123",
    )

    assert len(calls) == 2
    assert calls[0]["headers"]["Idempotency-Key"] == "customer-pricelist/123"
    assert calls[0]["timeout"].write == 120
    assert calls[1]["headers"] == calls[0]["headers"]


def test_resend_rejects_request_above_provider_limit(monkeypatch):
    monkeypatch.setattr(resend_api, "MAX_RESEND_EMAIL_BYTES", 10)
    monkeypatch.setattr(
        resend_api.httpx,
        "post",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("oversized request must not be sent")
        ),
    )

    assert not resend_api.send_email_via_resend(
        api_key="re_test",
        from_email="price@dragonzap.online",
        to_email=["client@example.com"],
        subject="Price",
        body="Attached",
        is_html=False,
        attachment_bytes=b"large-xlsx",
        attachment_filename="price.xlsx",
    )
