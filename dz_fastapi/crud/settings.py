from typing import Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.models.settings import (PriceCheckLog, PriceCheckSchedule,
                                        PriceListStaleAlert)


class CRUDPriceCheckSchedule:
    async def get_or_create(
        self, session: AsyncSession
    ) -> PriceCheckSchedule:
        result = await session.execute(select(PriceCheckSchedule).limit(1))
        schedule = result.scalar_one_or_none()
        if schedule:
            return schedule
        schedule = PriceCheckSchedule(enabled=True, days=[], times=[])
        session.add(schedule)
        await session.commit()
        await session.refresh(schedule)
        return schedule

    async def update(
        self, session: AsyncSession, data: dict
    ) -> PriceCheckSchedule:
        schedule = await self.get_or_create(session)
        for key, value in data.items():
            setattr(schedule, key, value)
        session.add(schedule)
        await session.commit()
        await session.refresh(schedule)
        return schedule


class CRUDPriceListStaleAlert:
    async def create(
        self,
        session: AsyncSession,
        provider_id: int,
        provider_config_id: int,
        days_diff: int,
        last_price_date,
    ) -> PriceListStaleAlert:
        alert = PriceListStaleAlert(
            provider_id=provider_id,
            provider_config_id=provider_config_id,
            days_diff=days_diff,
            last_price_date=last_price_date,
        )
        session.add(alert)
        await session.commit()
        await session.refresh(alert)
        return alert

    async def list(
        self,
        session: AsyncSession,
        provider_id: Optional[int] = None,
        limit: int = 100,
        offset: int = 0,
    ):
        stmt = select(PriceListStaleAlert).order_by(
            PriceListStaleAlert.created_at.desc()
        )
        if provider_id is not None:
            stmt = stmt.where(PriceListStaleAlert.provider_id == provider_id)
        stmt = stmt.offset(offset).limit(limit)
        result = await session.execute(stmt)
        return result.scalars().all()


class CRUDPriceCheckLog:
    async def create(
        self,
        session: AsyncSession,
        status: str,
        message: str | None = None,
    ) -> PriceCheckLog:
        log = PriceCheckLog(status=status, message=message)
        session.add(log)
        await session.commit()
        await session.refresh(log)
        return log

    async def list(self, session: AsyncSession, limit: int = 100):
        stmt = select(PriceCheckLog).order_by(
            PriceCheckLog.checked_at.desc()
        ).limit(limit)
        result = await session.execute(stmt)
        return result.scalars().all()


crud_price_check_schedule = CRUDPriceCheckSchedule()
crud_price_stale_alert = CRUDPriceListStaleAlert()
crud_price_check_log = CRUDPriceCheckLog()
