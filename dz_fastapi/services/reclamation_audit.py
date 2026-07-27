"""Append-only audit history for reclamation workflow actions."""

from datetime import date, datetime
from enum import Enum
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.models.partner import ReclamationEvent


def _json_safe(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, dict):
        return {
            str(key): _json_safe(item)
            for key, item in value.items()
        }
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(item) for item in value]
    return str(value)


async def record_reclamation_event(
    session: AsyncSession,
    *,
    reclamation_id: int,
    event_type: str,
    actor_user_id: int | None = None,
    details: dict[str, Any] | None = None,
) -> ReclamationEvent:
    event = ReclamationEvent(
        reclamation_id=int(reclamation_id),
        event_type=str(event_type)[:64],
        actor_user_id=actor_user_id,
        details=_json_safe(details or {}),
    )
    session.add(event)
    await session.flush()
    return event
