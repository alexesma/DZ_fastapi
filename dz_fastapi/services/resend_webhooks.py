"""Verification helpers for Resend/Svix webhook requests."""

import base64
import hashlib
import hmac
import time
from collections.abc import Mapping


class InvalidResendWebhook(ValueError):
    """Raised when a webhook is missing headers, stale, or has a bad signature."""


def _decode_signing_secret(secret: str) -> bytes:
    normalized = str(secret or "").strip()
    if normalized.startswith("whsec_"):
        normalized = normalized[6:]
    if not normalized:
        raise InvalidResendWebhook("Resend webhook secret не настроен")
    padding = "=" * (-len(normalized) % 4)
    try:
        return base64.urlsafe_b64decode(normalized + padding)
    except (ValueError, TypeError) as exc:
        raise InvalidResendWebhook("Некорректный Resend webhook secret") from exc


def verify_resend_webhook(
    *,
    payload: bytes,
    headers: Mapping[str, str],
    secret: str,
    tolerance_seconds: int = 300,
    now_timestamp: int | None = None,
) -> None:
    """Verify the raw payload using Resend's Svix signature headers."""
    normalized_headers = {str(key).lower(): str(value) for key, value in headers.items()}
    message_id = normalized_headers.get("svix-id", "").strip()
    timestamp_raw = normalized_headers.get("svix-timestamp", "").strip()
    signature_header = normalized_headers.get("svix-signature", "").strip()
    if not message_id or not timestamp_raw or not signature_header:
        raise InvalidResendWebhook("Отсутствуют заголовки подписи Resend")

    try:
        timestamp = int(timestamp_raw)
    except ValueError as exc:
        raise InvalidResendWebhook("Некорректное время подписи Resend") from exc
    current_timestamp = int(now_timestamp if now_timestamp is not None else time.time())
    if abs(current_timestamp - timestamp) > max(1, int(tolerance_seconds)):
        raise InvalidResendWebhook("Подпись Resend просрочена")

    signed_payload = (
        message_id.encode("utf-8")
        + b"."
        + timestamp_raw.encode("ascii")
        + b"."
        + payload
    )
    expected = base64.b64encode(
        hmac.new(
            _decode_signing_secret(secret),
            signed_payload,
            hashlib.sha256,
        ).digest()
    ).decode("ascii")
    candidates = []
    for token in signature_header.split():
        version, separator, signature = token.partition(",")
        if separator and version == "v1" and signature:
            candidates.append(signature)
    if not candidates or not any(
        hmac.compare_digest(expected, candidate) for candidate in candidates
    ):
        raise InvalidResendWebhook("Неверная подпись Resend")
