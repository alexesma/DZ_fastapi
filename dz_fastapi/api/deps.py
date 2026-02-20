from fastapi import Cookie, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.core.config import settings
from dz_fastapi.core.db import get_session
from dz_fastapi.crud.user import crud_user
from dz_fastapi.models.user import User, UserRole, UserStatus
from dz_fastapi.services.auth import decode_access_token


async def get_current_user(
    session: AsyncSession = Depends(get_session),
    token: str | None = Cookie(None, alias=settings.auth_cookie_name),
) -> User:
    if not token:
        raise HTTPException(status_code=401, detail="Not authenticated")
    payload = decode_access_token(token)
    if not payload or "sub" not in payload:
        raise HTTPException(status_code=401, detail="Invalid token")
    user_id = int(payload["sub"])
    user = await crud_user.get(session, user_id)
    if not user:
        raise HTTPException(status_code=401, detail="User not found")
    if user.status != UserStatus.ACTIVE:
        raise HTTPException(status_code=403, detail="User not active")
    return user


async def require_admin(
        current_user: User = Depends(get_current_user)
) -> User:
    if current_user.role != UserRole.ADMIN:
        raise HTTPException(status_code=403, detail="Admin access required")
    return current_user
