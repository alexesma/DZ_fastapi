from datetime import date

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.api.dashboard import _load_pair_stats_batch
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
