from datetime import date
from types import SimpleNamespace

import pytest

from dz_fastapi.analytics.price_history import _get_previous_pricelist
from dz_fastapi.crud.partner import crud_pricelist
from dz_fastapi.models.partner import PriceList, ProviderPriceListConfig


@pytest.mark.asyncio
async def test_get_last_pricelists_by_provider_scopes_to_config_and_newest_ids(
    test_session, created_providers
):
    provider = created_providers[0]
    cfg_a = ProviderPriceListConfig(
        provider_id=provider.id,
        start_row=1,
        oem_col=0,
        brand_col=1,
        name_col=2,
        qty_col=3,
        price_col=4,
        name_price='CFG_A',
        name_mail='MAIL_A',
    )
    cfg_b = ProviderPriceListConfig(
        provider_id=provider.id,
        start_row=1,
        oem_col=0,
        brand_col=1,
        name_col=2,
        qty_col=3,
        price_col=4,
        name_price='CFG_B',
        name_mail='MAIL_B',
    )
    test_session.add_all([cfg_a, cfg_b])
    await test_session.commit()
    await test_session.refresh(cfg_a)
    await test_session.refresh(cfg_b)

    older = PriceList(
        date=date(2026, 3, 27),
        provider_id=provider.id,
        provider_config_id=cfg_a.id,
    )
    newest_same_day = PriceList(
        date=date(2026, 3, 27),
        provider_id=provider.id,
        provider_config_id=cfg_a.id,
    )
    other_config = PriceList(
        date=date(2026, 3, 28),
        provider_id=provider.id,
        provider_config_id=cfg_b.id,
    )
    test_session.add_all([older, newest_same_day, other_config])
    await test_session.commit()
    await test_session.refresh(older)
    await test_session.refresh(newest_same_day)

    pricelists = await crud_pricelist.get_last_pricelists_by_provider(
        session=test_session,
        provider_id=provider.id,
        provider_config_id=cfg_a.id,
        limit_last_n=2,
    )

    assert [pl.id for pl in pricelists] == [newest_same_day.id, older.id]
    assert all(pl.provider_config_id == cfg_a.id for pl in pricelists)


def test_get_previous_pricelist_skips_current_pricelist():
    current = SimpleNamespace(id=10)
    previous = SimpleNamespace(id=9)

    resolved = _get_previous_pricelist(current, [current, previous])

    assert resolved is previous
