from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.models.price_control import (CustomerPriceListOverride,
                                             PriceControlConfig,
                                             PriceControlManualItem,
                                             PriceControlRecommendation,
                                             PriceControlRun,
                                             PriceControlSource,
                                             PriceControlSourceRecommendation,
                                             PriceControlStateProfile)


class CRUDPriceControlConfig:
    async def get(self, session: AsyncSession, config_id: int):
        return await session.get(PriceControlConfig, config_id)

    async def get_by_customer_pricelist(
        self, session: AsyncSession, customer_id: int, pricelist_config_id: int
    ):
        stmt = select(PriceControlConfig).where(
            PriceControlConfig.customer_id == customer_id,
            PriceControlConfig.pricelist_config_id == pricelist_config_id,
        )
        return (await session.execute(stmt)).scalars().first()

    async def list_by_customer(self, session: AsyncSession, customer_id: int):
        stmt = select(PriceControlConfig).where(
            PriceControlConfig.customer_id == customer_id
        )
        return (await session.execute(stmt)).scalars().all()

    async def create(self, session: AsyncSession, data: dict):
        config = PriceControlConfig(**data)
        session.add(config)
        await session.commit()
        await session.refresh(config)
        return config

    async def update(
            self,
            session: AsyncSession,
            config: PriceControlConfig,
            data: dict
    ):
        for key, value in data.items():
            setattr(config, key, value)
        session.add(config)
        await session.commit()
        await session.refresh(config)
        return config


def _normalize_profile_value(value) -> str:
    return str(value or '').strip()


class CRUDPriceControlStateProfile:
    async def list_by_config(self, session: AsyncSession, config_id: int):
        stmt = (
            select(PriceControlStateProfile)
            .where(PriceControlStateProfile.config_id == config_id)
            .order_by(PriceControlStateProfile.updated_at.desc())
        )
        return (await session.execute(stmt)).scalars().all()

    async def get_by_identity(
        self,
        session: AsyncSession,
        config_id: int,
        site_api_key_env: str | None,
        our_offer_field: str | None,
        our_offer_match: str | None,
    ):
        stmt = select(PriceControlStateProfile).where(
            PriceControlStateProfile.config_id == config_id,
            PriceControlStateProfile.site_api_key_env
            == _normalize_profile_value(site_api_key_env),
            PriceControlStateProfile.our_offer_field
            == _normalize_profile_value(our_offer_field),
            PriceControlStateProfile.our_offer_match
            == _normalize_profile_value(our_offer_match),
        )
        return (await session.execute(stmt)).scalars().first()

    async def get_or_create_active(
        self,
        session: AsyncSession,
        config: PriceControlConfig,
    ):
        profile = await self.get_by_identity(
            session=session,
            config_id=config.id,
            site_api_key_env=getattr(config, 'site_api_key_env', None),
            our_offer_field=getattr(config, 'our_offer_field', None),
            our_offer_match=getattr(config, 'our_offer_match', None),
        )
        if profile:
            return profile
        profile = PriceControlStateProfile(
            config_id=config.id,
            site_api_key_env=_normalize_profile_value(
                getattr(config, 'site_api_key_env', None)
            ),
            our_offer_field=_normalize_profile_value(
                getattr(config, 'our_offer_field', None)
            ),
            our_offer_match=_normalize_profile_value(
                getattr(config, 'our_offer_match', None)
            ),
            client_markup_coef=float(
                getattr(config, 'client_markup_coef', None) or 1.0
            ),
            client_markup_sample_size=int(
                getattr(config, 'client_markup_sample_size', None) or 0
            ),
            client_markup_recent_coef=(
                getattr(config, 'client_markup_recent_coef', None) or []
            ),
            cooldown_hours=int(getattr(config, 'cooldown_hours', None) or 0),
            cooldown_reset_at=getattr(config, 'cooldown_reset_at', None),
        )
        session.add(profile)
        await session.flush()
        return profile


class CRUDPriceControlSource:
    async def list_by_config(self, session: AsyncSession, config_id: int):
        stmt = select(PriceControlSource).where(
            PriceControlSource.config_id == config_id
        )
        return (await session.execute(stmt)).scalars().all()

    async def replace_for_config(
        self, session: AsyncSession, config_id: int, sources: list[dict]
    ):
        await session.execute(
            delete(PriceControlSource).where(
                PriceControlSource.config_id == config_id
            )
        )
        for source in sources:
            session.add(PriceControlSource(config_id=config_id, **source))
        await session.commit()


class CRUDPriceControlManualItem:
    async def list_by_config(self, session: AsyncSession, config_id: int):
        stmt = select(PriceControlManualItem).where(
            PriceControlManualItem.config_id == config_id
        )
        return (await session.execute(stmt)).scalars().all()

    async def replace_for_config(
        self, session: AsyncSession, config_id: int, items: list[dict]
    ):
        await session.execute(
            delete(PriceControlManualItem).where(
                PriceControlManualItem.config_id == config_id
            )
        )
        for item in items:
            session.add(PriceControlManualItem(config_id=config_id, **item))
        await session.commit()


class CRUDPriceControlRun:
    async def create(
            self,
            session: AsyncSession,
            config_id: int,
            total_items: int
    ):
        run = PriceControlRun(config_id=config_id, total_items=total_items)
        session.add(run)
        await session.commit()
        await session.refresh(run)
        return run

    async def list_by_config(
        self, session: AsyncSession, config_id: int, limit: int = 20
    ):
        stmt = (
            select(PriceControlRun)
            .where(PriceControlRun.config_id == config_id)
            .order_by(PriceControlRun.run_at.desc())
            .limit(limit)
        )
        return (await session.execute(stmt)).scalars().all()


class CRUDPriceControlRecommendation:
    async def list_by_run(
        self, session: AsyncSession, run_id: int
    ):
        stmt = select(PriceControlRecommendation).where(
            PriceControlRecommendation.run_id == run_id
        )
        return (await session.execute(stmt)).scalars().all()

    async def create_many(
        self, session: AsyncSession, run_id: int, rows: list[dict]
    ):
        for row in rows:
            session.add(PriceControlRecommendation(run_id=run_id, **row))
        await session.commit()

    async def get_by_ids(self, session: AsyncSession, ids: list[int]):
        stmt = select(PriceControlRecommendation).where(
            PriceControlRecommendation.id.in_(ids)
        )
        return (await session.execute(stmt)).scalars().all()

    async def list_recent_keys_by_config(
        self,
        session: AsyncSession,
        config_id: int,
        since_dt,
    ) -> list[tuple[str, str]]:
        stmt = (
            select(
                PriceControlRecommendation.oem,
                PriceControlRecommendation.brand,
            )
            .join(
                PriceControlRun,
                PriceControlRun.id == PriceControlRecommendation.run_id,
            )
            .where(
                PriceControlRun.config_id == config_id,
                PriceControlRun.run_at >= since_dt,
            )
            .distinct()
        )
        rows = (await session.execute(stmt)).all()
        return [(str(oem or ''), str(brand or '')) for oem, brand in rows]


class CRUDPriceControlSourceRecommendation:
    async def list_by_run(self, session: AsyncSession, run_id: int):
        stmt = select(PriceControlSourceRecommendation).where(
            PriceControlSourceRecommendation.run_id == run_id
        )
        return (await session.execute(stmt)).scalars().all()

    async def create_many(
        self, session: AsyncSession, run_id: int, rows: list[dict]
    ):
        for row in rows:
            session.add(PriceControlSourceRecommendation(run_id=run_id, **row))
        await session.commit()

    async def get_by_ids(self, session: AsyncSession, ids: list[int]):
        stmt = select(PriceControlSourceRecommendation).where(
            PriceControlSourceRecommendation.id.in_(ids)
        )
        return (await session.execute(stmt)).scalars().all()


class CRUDCustomerPriceListOverride:
    async def upsert(
            self,
            session: AsyncSession,
            config_id: int,
            autopart_id: int,
            price: float
    ):
        stmt = select(CustomerPriceListOverride).where(
            CustomerPriceListOverride.config_id == config_id,
            CustomerPriceListOverride.autopart_id == autopart_id,
        )
        existing = (await session.execute(stmt)).scalars().first()
        if existing:
            existing.price = price
            existing.is_active = True
            session.add(existing)
            await session.commit()
            await session.refresh(existing)
            return existing
        item = CustomerPriceListOverride(
            config_id=config_id, autopart_id=autopart_id, price=price
        )
        session.add(item)
        await session.commit()
        await session.refresh(item)
        return item

    async def get_for_config(self, session: AsyncSession, config_id: int):
        stmt = select(CustomerPriceListOverride).where(
            CustomerPriceListOverride.config_id == config_id,
            CustomerPriceListOverride.is_active.is_(True),
        )
        rows = (await session.execute(stmt)).scalars().all()
        result: dict[int, float] = {}
        for row in rows:
            if not row.autopart_id:
                continue
            result[int(row.autopart_id)] = float(row.price)
        return result


crud_price_control_config = CRUDPriceControlConfig()
crud_price_control_state_profile = CRUDPriceControlStateProfile()
crud_price_control_source = CRUDPriceControlSource()
crud_price_control_manual = CRUDPriceControlManualItem()
crud_price_control_run = CRUDPriceControlRun()
crud_price_control_reco = CRUDPriceControlRecommendation()
crud_price_control_source_reco = CRUDPriceControlSourceRecommendation()
crud_customer_pricelist_override = CRUDCustomerPriceListOverride()
