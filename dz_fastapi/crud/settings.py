from typing import Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.models.settings import (CustomerOrderInboxSettings,
                                        PriceCheckLog, PriceCheckSchedule,
                                        PriceListStaleAlert, SchedulerSetting,
                                        SystemMetricSnapshot)


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


class CRUDSchedulerSetting:
    async def get_or_create(
        self,
        session: AsyncSession,
        key: str,
        defaults: dict | None = None,
    ) -> SchedulerSetting:
        result = await session.execute(
            select(SchedulerSetting).where(SchedulerSetting.key == key)
        )
        setting = result.scalar_one_or_none()
        if setting:
            return setting
        payload = defaults or {}
        setting = SchedulerSetting(key=key, **payload)
        session.add(setting)
        await session.commit()
        await session.refresh(setting)
        return setting

    async def list(self, session: AsyncSession):
        result = await session.execute(
            select(SchedulerSetting).order_by(SchedulerSetting.key.asc())
        )
        return result.scalars().all()

    async def update(
        self,
        session: AsyncSession,
        key: str,
        data: dict,
        defaults: dict | None = None,
    ) -> SchedulerSetting:
        setting = await self.get_or_create(
            session=session, key=key, defaults=defaults
        )
        for field, value in data.items():
            setattr(setting, field, value)
        session.add(setting)
        await session.commit()
        await session.refresh(setting)
        return setting


class CRUDCustomerOrderInboxSettings:
    async def get_or_create(
        self, session: AsyncSession
    ) -> CustomerOrderInboxSettings:
        result = await session.execute(
            select(CustomerOrderInboxSettings).limit(1)
        )
        setting = result.scalar_one_or_none()
        if setting:
            return setting
        setting = CustomerOrderInboxSettings(
            lookback_days=1,
            mark_seen=False,
            error_file_retention_days=5,
            supplier_response_lookback_days=14,
            supplier_order_stub_enabled=True,
            supplier_order_stub_email='info@dragonzap.ru',
        )
        session.add(setting)
        await session.commit()
        await session.refresh(setting)
        return setting

    async def update(
        self, session: AsyncSession, data: dict
    ) -> CustomerOrderInboxSettings:
        setting = await self.get_or_create(session)
        for key, value in data.items():
            setattr(setting, key, value)
        session.add(setting)
        await session.commit()
        await session.refresh(setting)
        return setting


class CRUDSystemMetricSnapshot:
    async def create(
        self,
        session: AsyncSession,
        payload: dict,
    ) -> SystemMetricSnapshot:
        snapshot = SystemMetricSnapshot(**payload)
        session.add(snapshot)
        await session.commit()
        await session.refresh(snapshot)
        return snapshot

    async def list(
        self,
        session: AsyncSession,
        limit: int = 200,
        offset: int = 0,
    ):
        stmt = (
            select(SystemMetricSnapshot)
            .order_by(SystemMetricSnapshot.created_at.desc())
            .offset(offset)
            .limit(limit)
        )
        result = await session.execute(stmt)
        return result.scalars().all()


crud_price_check_schedule = CRUDPriceCheckSchedule()
crud_price_stale_alert = CRUDPriceListStaleAlert()
crud_price_check_log = CRUDPriceCheckLog()
crud_scheduler_setting = CRUDSchedulerSetting()
crud_customer_order_inbox_settings = CRUDCustomerOrderInboxSettings()
crud_system_metric_snapshot = CRUDSystemMetricSnapshot()
