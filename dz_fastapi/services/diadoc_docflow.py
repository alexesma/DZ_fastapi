"""Каноническое состояние документов по ответу GetDocflows V5."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from dz_fastapi.core.time import now_moscow

_DOTNET_TICKS_EPOCH = datetime(1, 1, 1, tzinfo=timezone.utc)


def _find_first(payload: Any, names: tuple[str, ...], depth: int = 10) -> Any:
    if depth < 0:
        return None
    if isinstance(payload, dict):
        for name in names:
            if name in payload and payload[name] not in (None, "", [], {}):
                return payload[name]
        for value in payload.values():
            found = _find_first(value, names, depth - 1)
            if found not in (None, "", [], {}):
                return found
    elif isinstance(payload, list):
        for value in payload:
            found = _find_first(value, names, depth - 1)
            if found not in (None, "", [], {}):
                return found
    return None


def _text(value: Any) -> str | None:
    if isinstance(value, dict):
        value = _find_first(value, ("Status", "StatusName", "Type", "Value"), 3)
    result = str(value or "").strip()
    return result or None


def _ticks_datetime(value: Any) -> datetime | None:
    if isinstance(value, dict):
        value = value.get("Ticks") or value.get("TimestampTicks")
    try:
        ticks = int(value)
    except (TypeError, ValueError):
        return None
    if ticks <= 0:
        return None
    try:
        return (_DOTNET_TICKS_EPOCH + timedelta(microseconds=ticks / 10)).astimezone()
    except OverflowError:
        return None


def derive_docflow_state(document: dict[str, Any], *, current_status: str) -> dict[str, Any]:
    """Разбирает перечисления Docflow V5 без поиска русских слов в статусе."""
    event = document.get("LastEvent") or document
    docflow = event.get("Docflow") or document.get("Docflow") or {}
    status_block = docflow.get("DocflowStatus") or {}
    primary = status_block.get("PrimaryStatus") or {}
    severity = _text(primary.get("Severity"))
    status_text = _text(primary.get("StatusText"))
    is_finished_raw = docflow.get("IsFinished")
    is_finished = is_finished_raw if isinstance(is_finished_raw, bool) else None

    recipient = docflow.get("RecipientResponse") or {}
    recipient_status = _text(
        recipient.get("ResponseStatus")
        or _find_first(docflow.get("ParticipantResponses") or [], ("ResponseStatus",))
    )
    revocation = docflow.get("Revocation") or {}
    revocation_status = _text(revocation.get("RevocationStatus"))
    sender = docflow.get("SenderTitle") or {}
    delivered_at = _ticks_datetime(sender.get("DeliveredAt"))

    recipient_lower = (recipient_status or "").lower()
    revocation_lower = (revocation_status or "").lower()
    severity_lower = (severity or "").lower()
    sender_signature = (_text(sender.get("SenderSignatureStatus")) or "").lower()

    if revocation_lower == "revocationaccepted":
        status = "revoked"
    elif any(
        marker in recipient_lower
        for marker in ("signaturerequestrejected", "rejected", "rejection")
    ):
        status = "rejected"
    elif severity_lower == "error" or "invalid" in sender_signature or "invalid" in recipient_lower:
        status = "technical_error"
    elif is_finished is True:
        status = "completed"
    elif delivered_at is not None:
        status = "delivered"
    else:
        status = current_status or "sent"

    return {
        "status": status,
        "is_finished": is_finished,
        "docflow_status_severity": severity,
        "docflow_status_text": status_text,
        "recipient_response_status": recipient_status,
        "revocation_status": revocation_status,
        "delivered_at": delivered_at,
        "checked_at": now_moscow(),
        "payload": document,
    }
