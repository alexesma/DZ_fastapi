from datetime import timedelta

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.api.deps import get_current_user, require_admin
from dz_fastapi.core.db import get_session
from dz_fastapi.core.time import now_moscow
from dz_fastapi.models.notification import AppNotification
from dz_fastapi.models.partner import ProviderPriceListConfig
from dz_fastapi.models.user import User
from dz_fastapi.schemas.notification import (
    AppNotificationListResponse,
    AppNotificationReadResponse,
    AppNotificationResponse,
    PricelistStaleActionRequest,
    PricelistStaleActionResponse,
)
from dz_fastapi.services.notifications import create_notification

router = APIRouter(prefix="/notifications", tags=["notifications"])


@router.get(
    "",
    response_model=AppNotificationListResponse,
    status_code=status.HTTP_200_OK,
)
async def list_notifications(
    unread_only: bool = Query(False),
    limit: int = Query(50, ge=1, le=200),
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    unread_count_stmt = select(func.count(AppNotification.id)).where(
        AppNotification.user_id == current_user.id,
        AppNotification.read_at.is_(None),
        or_(
            AppNotification.available_at.is_(None),
            AppNotification.available_at <= now_moscow(),
        ),
    )
    unread_count = int(
        (await session.execute(unread_count_stmt)).scalar() or 0
    )

    stmt = (
        select(AppNotification)
        .where(
            AppNotification.user_id == current_user.id,
            or_(
                AppNotification.available_at.is_(None),
                AppNotification.available_at <= now_moscow(),
            ),
        )
        .order_by(
            AppNotification.created_at.desc(),
            AppNotification.id.desc(),
        )
        .limit(limit)
    )
    if unread_only:
        stmt = stmt.where(AppNotification.read_at.is_(None))
    items = (await session.execute(stmt)).scalars().all()
    return AppNotificationListResponse(
        items=[AppNotificationResponse.model_validate(item) for item in items],
        unread_count=unread_count,
    )


@router.post(
    "/{notification_id}/read",
    response_model=AppNotificationReadResponse,
    status_code=status.HTTP_200_OK,
)
async def mark_notification_read(
    notification_id: int,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    notification = await session.get(AppNotification, notification_id)
    if not notification or notification.user_id != current_user.id:
        raise HTTPException(status_code=404, detail="Notification not found")
    payload = notification.payload if isinstance(notification.payload, dict) else {}
    if payload.get("notification_type") == "pricelist_stale_action":
        raise HTTPException(
            status_code=409,
            detail="Choose an action for the stale supplier price",
        )
    if notification.read_at is None:
        notification.read_at = now_moscow()
        session.add(notification)
        await session.commit()
        await session.refresh(notification)
    return AppNotificationReadResponse(
        id=notification.id,
        read_at=notification.read_at,
    )


@router.post(
    "/read-all",
    status_code=status.HTTP_200_OK,
)
async def mark_all_notifications_read(
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    result = await session.execute(
        select(AppNotification).where(
            AppNotification.user_id == current_user.id,
            AppNotification.read_at.is_(None),
            or_(
                AppNotification.available_at.is_(None),
                AppNotification.available_at <= now_moscow(),
            ),
        )
    )
    items = result.scalars().all()
    now = now_moscow()
    for item in items:
        payload = item.payload if isinstance(item.payload, dict) else {}
        if payload.get("notification_type") == "pricelist_stale_action":
            continue
        item.read_at = now
        session.add(item)
    await session.commit()
    return {"updated": sum(1 for item in items if item.read_at == now)}


@router.post(
    "/{notification_id}/pricelist-stale-action",
    response_model=PricelistStaleActionResponse,
    status_code=status.HTTP_200_OK,
)
async def handle_pricelist_stale_action(
    notification_id: int,
    request: PricelistStaleActionRequest,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(require_admin),
):
    notification = await session.get(AppNotification, notification_id)
    if not notification or notification.user_id != current_user.id:
        raise HTTPException(status_code=404, detail="Notification not found")

    payload = notification.payload if isinstance(notification.payload, dict) else {}
    if payload.get("notification_type") != "pricelist_stale_action":
        raise HTTPException(status_code=409, detail="Notification does not require this action")
    try:
        provider_config_id = int(payload.get("provider_config_id"))
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=409, detail="Supplier price source is missing") from exc

    config = await session.get(ProviderPriceListConfig, provider_config_id)
    if not config:
        raise HTTPException(status_code=404, detail="Supplier price source not found")

    now = now_moscow()
    if request.action == "snooze_30_minutes":
        available_at = now + timedelta(minutes=30)
        notification.read_at = now
        session.add(notification)
        replacement = await create_notification(
            session,
            user_id=current_user.id,
            title=notification.title,
            message=notification.message,
            level=notification.level,
            link=notification.link,
            payload=payload,
            available_at=available_at,
            commit=False,
        )
        await session.commit()
        return PricelistStaleActionResponse(
            notification_id=replacement.id,
            action=request.action,
            available_at=available_at,
        )

    override_until = now + timedelta(days=1)
    config.stale_override_until = override_until
    session.add(config)

    # Решение о продлении относится к источнику целиком. Закрываем такое же
    # требование у остальных администраторов, чтобы они не принимали его снова.
    open_notifications = (
        await session.execute(
            select(AppNotification).where(AppNotification.read_at.is_(None))
        )
    ).scalars().all()
    for item in open_notifications:
        item_payload = item.payload if isinstance(item.payload, dict) else {}
        if (
            item_payload.get("notification_type") == "pricelist_stale_action"
            and str(item_payload.get("provider_config_id")) == str(provider_config_id)
        ):
            item.read_at = now
            session.add(item)
    await session.commit()
    return PricelistStaleActionResponse(
        notification_id=notification.id,
        action=request.action,
        override_until=override_until,
    )
