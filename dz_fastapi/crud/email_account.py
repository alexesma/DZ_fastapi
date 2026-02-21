import logging
from typing import List, Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.crud.base import CRUDBase
from dz_fastapi.models.email_account import EmailAccount
from dz_fastapi.schemas.email_account import (EmailAccountCreate,
                                              EmailAccountUpdate)

logger = logging.getLogger('dz_fastapi')


class CRUDEmailAccount(
    CRUDBase[EmailAccount, EmailAccountCreate, EmailAccountUpdate]
):
    async def get_by_email(
        self, session: AsyncSession, email: str
    ) -> Optional[EmailAccount]:
        result = await session.execute(
            select(EmailAccount).where(EmailAccount.email == email)
        )
        return result.scalars().first()

    async def get_active_by_purpose(
        self, session: AsyncSession, purpose: str
    ) -> List[EmailAccount]:
        result = await session.execute(
            select(EmailAccount).where(EmailAccount.is_active.is_(True))
        )
        accounts = result.scalars().all()
        purpose = purpose.lower()
        return [
            acc
            for acc in accounts
            if any(p.lower() == purpose for p in (acc.purposes or []))
        ]

    async def get_multi(
        self, session: AsyncSession, *, skip: int = 0, limit: int = 100
    ) -> List[EmailAccount]:
        result = await session.execute(
            select(EmailAccount).offset(skip).limit(limit)
        )
        return result.scalars().all()


crud_email_account = CRUDEmailAccount(EmailAccount)
