from datetime import timedelta

from fastapi import APIRouter, Depends, HTTPException, Response
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.api.deps import get_current_user, require_admin
from dz_fastapi.core.config import settings
from dz_fastapi.core.db import get_session
from dz_fastapi.crud.user import crud_user
from dz_fastapi.models.user import User, UserStatus
from dz_fastapi.schemas.auth import (UserLogin, UserRegister, UserResponse,
                                     UserRoleUpdate)
from dz_fastapi.services.auth import create_access_token, verify_password

router = APIRouter(tags=["auth"])


def _set_auth_cookie(response: Response, token: str) -> None:
    max_age = settings.jwt_access_token_expire_minutes * 60
    response.set_cookie(
        key=settings.auth_cookie_name,
        value=token,
        httponly=True,
        max_age=max_age,
        samesite="lax",
        secure=settings.auth_cookie_secure,
    )


def _clear_auth_cookie(response: Response) -> None:
    response.delete_cookie(
        key=settings.auth_cookie_name,
        samesite="lax",
    )


@router.post("/auth/register", response_model=UserResponse)
async def register(
    user_in: UserRegister, session: AsyncSession = Depends(get_session)
):
    user = await crud_user.create_user(session, user_in)
    return user


@router.post("/auth/login", response_model=UserResponse)
async def login(
    response: Response,
    user_in: UserLogin,
    session: AsyncSession = Depends(get_session),
):
    email = user_in.email.lower().strip()
    user = await crud_user.get_by_email(session, email)
    if not user or not verify_password(user_in.password, user.password_hash):
        raise HTTPException(status_code=401, detail="Invalid credentials")
    if user.status == UserStatus.PENDING:
        raise HTTPException(status_code=403, detail="User not approved yet")
    if user.status == UserStatus.DISABLED:
        raise HTTPException(status_code=403, detail="User is disabled")
    token = create_access_token(
        subject=str(user.id),
        expires_delta=timedelta(
            minutes=settings.jwt_access_token_expire_minutes
        ),
    )
    _set_auth_cookie(response, token)
    return user


@router.post("/auth/logout")
async def logout(response: Response):
    _clear_auth_cookie(response)
    return {"result": "ok"}


@router.get("/auth/me", response_model=UserResponse)
async def me(current_user: User = Depends(get_current_user)):
    return current_user


@router.get("/admin/users", response_model=list[UserResponse])
async def list_users(
    status: UserStatus | None = None,
    session: AsyncSession = Depends(get_session),
    _: User = Depends(require_admin),
):
    if status:
        return await crud_user.list_by_status(session, status)
    return await crud_user.get_multi(session)


@router.post("/admin/users/{user_id}/approve", response_model=UserResponse)
async def approve_user(
    user_id: int,
    session: AsyncSession = Depends(get_session),
    admin: User = Depends(require_admin),
):
    user = await crud_user.get(session, user_id)
    if user.status == UserStatus.ACTIVE:
        return user
    user.approve(admin.id)
    session.add(user)
    await session.commit()
    await session.refresh(user)
    return user


@router.post("/admin/users/{user_id}/disable", response_model=UserResponse)
async def disable_user(
    user_id: int,
    session: AsyncSession = Depends(get_session),
    _: User = Depends(require_admin),
):
    user = await crud_user.get(session, user_id)
    user.status = UserStatus.DISABLED
    session.add(user)
    await session.commit()
    await session.refresh(user)
    return user


@router.post("/admin/users/{user_id}/role", response_model=UserResponse)
async def update_user_role(
    user_id: int,
    role_in: UserRoleUpdate,
    session: AsyncSession = Depends(get_session),
    _: User = Depends(require_admin),
):
    user = await crud_user.get(session, user_id)
    user.role = role_in.role
    session.add(user)
    await session.commit()
    await session.refresh(user)
    return user
