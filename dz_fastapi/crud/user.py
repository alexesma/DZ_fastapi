from typing import Optional

from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.crud.base import CRUDBase
from dz_fastapi.models.user import User, UserRole, UserStatus
from dz_fastapi.schemas.auth import UserRegister
from dz_fastapi.services.auth import get_password_hash


class CRUDUser(CRUDBase[User, UserRegister, UserRegister]):
    async def get_by_email(
        self, session: AsyncSession, email: str
    ) -> Optional[User]:
        result = await session.execute(
            select(User).where(User.email == email)
        )
        return result.scalars().first()

    async def create_user(
        self,
        session: AsyncSession,
        user_in: UserRegister,
        role: UserRole = UserRole.MANAGER,
        status: UserStatus = UserStatus.PENDING,
    ) -> User:
        email = user_in.email.lower().strip()
        existing = await self.get_by_email(session, email)
        if existing:
            raise HTTPException(
                status_code=400, detail="User with this email already exists"
            )
        user = User(
            email=email,
            password_hash=get_password_hash(user_in.password),
            role=role,
            status=status,
        )
        session.add(user)
        await session.commit()
        await session.refresh(user)
        return user

    async def list_by_status(
        self, session: AsyncSession, status: UserStatus
    ) -> list[User]:
        result = await session.execute(
            select(User).where(User.status == status).order_by(User.id.desc())
        )
        return result.scalars().all()


crud_user = CRUDUser(User)
