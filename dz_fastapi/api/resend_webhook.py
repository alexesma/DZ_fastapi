import json
import logging
import os

from fastapi import APIRouter, Depends, HTTPException, Request, status
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.core.db import get_session
from dz_fastapi.services.inbox_email import ingest_resend_received_email
from dz_fastapi.services.resend_webhooks import InvalidResendWebhook, verify_resend_webhook

logger = logging.getLogger("dz_fastapi")

router = APIRouter(prefix="/webhooks/resend", tags=["webhooks"])


@router.post("", status_code=status.HTTP_200_OK)
async def receive_resend_webhook(
    request: Request,
    session: AsyncSession = Depends(get_session),
):
    """Receive signed Resend events; only inbound mail changes application state."""
    payload = await request.body()
    secret = os.getenv("RESEND_WEBHOOK_SECRET", "").strip()
    if not secret:
        logger.error("RESEND_WEBHOOK_SECRET is not configured")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Resend webhook is not configured",
        )
    try:
        verify_resend_webhook(
            payload=payload,
            headers=request.headers,
            secret=secret,
            tolerance_seconds=int(
                os.getenv("RESEND_WEBHOOK_TOLERANCE_SECONDS", "300")
            ),
        )
    except InvalidResendWebhook as exc:
        logger.warning("Rejected Resend webhook: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid webhook signature",
        ) from exc

    try:
        event = json.loads(payload)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid webhook payload",
        ) from exc
    if not isinstance(event, dict):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid webhook payload",
        )

    event_type = str(event.get("type") or "").strip().lower()
    if event_type != "email.received":
        return {"status": "ignored", "event_type": event_type}

    try:
        event_data = event.get("data")
        if not isinstance(event_data, dict):
            raise ValueError("Resend webhook не содержит объект data")
        result = await ingest_resend_received_email(
            session,
            event_data=event_data,
        )
    except ValueError as exc:
        await session.rollback()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        ) from exc
    except Exception as exc:
        await session.rollback()
        logger.exception("Failed to ingest Resend inbound email: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Failed to ingest inbound email",
        ) from exc
    return result
