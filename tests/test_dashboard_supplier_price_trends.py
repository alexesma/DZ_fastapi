from datetime import date

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.api.dashboard import _load_pair_stats_batch, get_supplier_pricelist_health
from dz_fastapi.models.autopart import AutoPart
from dz_fastapi.models.partner import (
    PriceList,
    PriceListAutoPartAssociation,
    Provider,
    ProviderPriceListConfig,
)


@pytest.mark.asyncio
async def test_pair_stats_batch_calculates_price_change_without_parallel_shm(
    test_session: AsyncSession,
    created_providers: list[Provider],
    created_pricelist_config: ProviderPriceListConfig,
    created_autopart: AutoPart,
):
    provider = created_providers[0]
    previous = PriceList(
        provider_id=provider.id,
        provider_config_id=created_pricelist_config.id,
        date=date.today(),
        is_active=True,
    )
    current = PriceList(
        provider_id=provider.id,
        provider_config_id=created_pricelist_config.id,
        date=date.today(),
        is_active=True,
    )
    test_session.add_all([previous, current])
    await test_session.flush()
    test_session.add_all(
        [
            PriceListAutoPartAssociation(
                pricelist_id=previous.id,
                autopart_id=created_autopart.id,
                quantity=5,
                price=100,
                multiplicity=1,
            ),
            PriceListAutoPartAssociation(
                pricelist_id=current.id,
                autopart_id=created_autopart.id,
                quantity=5,
                price=110,
                multiplicity=1,
            ),
        ]
    )
    await test_session.commit()

    result = await _load_pair_stats_batch(
        test_session,
        {(previous.id, current.id)},
    )

    overlap, median_pct, changed_share_pct = result[(previous.id, current.id)]
    assert overlap == 1
    assert median_pct == pytest.approx(10.0)
    assert changed_share_pct == 100.0


@pytest.mark.asyncio
async def test_supplier_pricelist_health_reports_stale_and_extended_sources(
    test_session: AsyncSession,
    created_providers: list[Provider],
    created_pricelist_config: ProviderPriceListConfig,
):
    from datetime import timedelta

    from dz_fastapi.core.time import now_moscow

    config = created_pricelist_config
    config.max_days_without_update = 1
    pricelist = PriceList(
        provider_id=created_providers[0].id,
        provider_config_id=config.id,
        date=now_moscow().date() - timedelta(days=5),
        is_active=True,
    )
    test_session.add_all([config, pricelist])
    await test_session.commit()

    stale_result = await get_supplier_pricelist_health(session=test_session)
    stale_item = next(
        item for item in stale_result.items if item.provider_config_id == config.id
    )
    assert stale_item.status == "stale"
    assert stale_result.summary.stale >= 1

    config.stale_override_until = now_moscow() + timedelta(days=1)
    test_session.add(config)
    await test_session.commit()
    extended_result = await get_supplier_pricelist_health(session=test_session)
    extended_item = next(
        item for item in extended_result.items if item.provider_config_id == config.id
    )
    assert extended_item.status == "extended"
