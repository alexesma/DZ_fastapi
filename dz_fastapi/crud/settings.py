from typing import Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.core.config import settings
from dz_fastapi.models.settings import (
    CustomerOrderInboxSettings,
    DiadocIntegrationSettings,
    PriceCheckLog,
    PriceCheckSchedule,
    PriceListStaleAlert,
    SchedulerSetting,
    SystemMetricSnapshot,
)


class CRUDPriceCheckSchedule:
    async def get_or_create(self, session: AsyncSession) -> PriceCheckSchedule:
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
        stmt = (
            select(PriceCheckLog)
            .order_by(PriceCheckLog.checked_at.desc())
            .limit(limit)
        )
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
            supplier_response_auto_close_stale_enabled=True,
            supplier_response_stale_days=7,
            supplier_order_stub_enabled=True,
            supplier_order_stub_email="info@dragonzap.ru",
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


class CRUDDiadocIntegrationSettings:
    async def get_or_create(
        self, session: AsyncSession
    ) -> DiadocIntegrationSettings:
        result = await session.execute(
            select(DiadocIntegrationSettings).limit(1)
        )
        item = result.scalar_one_or_none()
        if item:
            return item
        default_environment = (
            str(settings.diadoc_default_environment or "staging")
            .strip()
            .lower()
        )
        if default_environment not in {"staging", "prod"}:
            default_environment = "staging"
        item = DiadocIntegrationSettings(environment=default_environment)
        session.add(item)
        await session.commit()
        await session.refresh(item)
        return item

    async def update(
        self, session: AsyncSession, data: dict
    ) -> DiadocIntegrationSettings:
        item = await self.get_or_create(session)
        for key, value in data.items():
            setattr(item, key, value)
        session.add(item)
        await session.commit()
        await session.refresh(item)
        return item

    async def clear_connection(
        self, session: AsyncSession
    ) -> DiadocIntegrationSettings:
        item = await self.get_or_create(session)
        item.organization_id = None
        item.organization_name = None
        item.organization_inn = None
        item.organization_kpp = None
        item.box_id = None
        item.box_id_guid = None
        item.refresh_token = None
        item.access_token = None
        item.token_type = None
        item.token_scope = None
        item.access_token_expires_at = None
        item.connected_user_id = None
        item.connected_user_name = None
        item.connected_at = None
        item.last_sync_at = None
        item.last_error = None
        session.add(item)
        await session.commit()
        await session.refresh(item)
        return item


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
crud_diadoc_integration_settings = CRUDDiadocIntegrationSettings()
crud_system_metric_snapshot = CRUDSystemMetricSnapshot()
