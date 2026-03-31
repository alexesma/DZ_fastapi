from collections.abc import Iterable

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.models.notification import (AppNotification,
                                            AppNotificationLevel)
from dz_fastapi.models.user import User, UserRole, UserStatus


async def create_notification(
    session: AsyncSession,
    *,
    user_id: int,
    title: str,
    message: str,
    level: str = AppNotificationLevel.INFO,
    link: str | None = None,
    commit: bool = True,
) -> AppNotification:
    notification = AppNotification(
        user_id=user_id,
        title=title,
        message=message,
        level=level,
        link=link,
    )
    session.add(notification)
    if commit:
        await session.commit()
        await session.refresh(notification)
    else:
        await session.flush()
    return notification


async def create_notifications_for_users(
    session: AsyncSession,
    *,
    user_ids: Iterable[int],
    title: str,
    message: str,
    level: str = AppNotificationLevel.INFO,
    link: str | None = None,
    commit: bool = True,
) -> list[AppNotification]:
    notifications: list[AppNotification] = []
    for user_id in user_ids:
        notifications.append(
            AppNotification(
                user_id=user_id,
                title=title,
                message=message,
                level=level,
                link=link,
            )
        )
    if not notifications:
        return []
    session.add_all(notifications)
    if commit:
        await session.commit()
        for item in notifications:
            await session.refresh(item)
    else:
        await session.flush()
    return notifications


async def create_notifications_for_role(
    session: AsyncSession,
    *,
    role: UserRole,
    title: str,
    message: str,
    level: str = AppNotificationLevel.INFO,
    link: str | None = None,
    commit: bool = True,
) -> list[AppNotification]:
    result = await session.execute(
        select(User.id).where(
            User.role == role,
            User.status == UserStatus.ACTIVE,
        )
    )
    user_ids = list(result.scalars().all())
    return await create_notifications_for_users(
        session,
        user_ids=user_ids,
        title=title,
        message=message,
        level=level,
        link=link,
        commit=commit,
    )


async def create_admin_notifications(
    session: AsyncSession,
    *,
    title: str,
    message: str,
    level: str = AppNotificationLevel.INFO,
    link: str | None = None,
    commit: bool = True,
) -> list[AppNotification]:
    return await create_notifications_for_role(
        session=session,
        role=UserRole.ADMIN,
        title=title,
        message=message,
        level=level,
        link=link,
        commit=commit,
    )
