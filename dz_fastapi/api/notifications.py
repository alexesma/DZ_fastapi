from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.api.deps import get_current_user
from dz_fastapi.core.db import get_session
from dz_fastapi.core.time import now_moscow
from dz_fastapi.models.notification import AppNotification
from dz_fastapi.models.user import User
from dz_fastapi.schemas.notification import (AppNotificationListResponse,
                                             AppNotificationReadResponse,
                                             AppNotificationResponse)

router = APIRouter(prefix='/notifications', tags=['notifications'])


@router.get(
    '',
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
    )
    unread_count = int(
        (await session.execute(unread_count_stmt)).scalar() or 0
    )

    stmt = (
        select(AppNotification)
        .where(AppNotification.user_id == current_user.id)
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
        items=[
            AppNotificationResponse.model_validate(item)
            for item in items
        ],
        unread_count=unread_count,
    )


@router.post(
    '/{notification_id}/read',
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
        raise HTTPException(status_code=404, detail='Notification not found')
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
    '/read-all',
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
        )
    )
    items = result.scalars().all()
    now = now_moscow()
    for item in items:
        item.read_at = now
        session.add(item)
    await session.commit()
    return {'updated': len(items)}
