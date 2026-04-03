from datetime import date, datetime
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

from dz_fastapi.analytics.price_history import (
    _get_previous_pricelist, build_pricelist_change_summary,
    prepare_price_history_plot_data)
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


@pytest.mark.asyncio
async def test_build_pricelist_change_summary(monkeypatch):
    async def fake_get_autopart_details(session, autopart_ids):
        return {
            1: {
                'oem_number': 'OEM-1',
                'name': 'Part 1',
                'brand': 'BRAND-1',
            },
            2: {
                'oem_number': 'OEM-2',
                'name': 'Part 2',
                'brand': 'BRAND-2',
            },
            3: {
                'oem_number': 'OEM-3',
                'name': 'Part 3',
                'brand': 'BRAND-3',
            },
        }

    monkeypatch.setattr(
        'dz_fastapi.analytics.price_history._get_autopart_details',
        fake_get_autopart_details,
    )

    old_pl = SimpleNamespace(
        id=100,
        date=date(2026, 3, 26),
        autopart_associations=[
            SimpleNamespace(autopart_id=1, price=100, quantity=10),
            SimpleNamespace(autopart_id=2, price=200, quantity=20),
        ],
    )
    new_pl = SimpleNamespace(
        id=101,
        date=date(2026, 3, 27),
        autopart_associations=[
            SimpleNamespace(autopart_id=1, price=130, quantity=4),
            SimpleNamespace(autopart_id=2, price=180, quantity=18),
            SimpleNamespace(autopart_id=3, price=50, quantity=7),
        ],
    )

    summary = await build_pricelist_change_summary(
        new_pl=new_pl,
        old_pl=old_pl,
        session=None,
        top_n=20,
    )

    assert summary['latest_positions_count'] == 3
    assert summary['previous_positions_count'] == 2
    assert summary['new_positions_count'] == 1
    assert summary['removed_positions_count'] == 0
    assert summary['changed_price_count'] == 2
    assert summary['changed_quantity_count'] == 2

    assert summary['top_turnover_positions'][0]['autopart_id'] == 1
    assert summary['top_turnover_positions'][0]['quantity_drop'] == 6

    assert summary['sharpest_price_changes'][0]['autopart_id'] == 1
    assert (
        round(summary['sharpest_price_changes'][0]['price_diff_pct'], 2)
        == 30.0
    )


def test_prepare_price_history_plot_data_extends_flat_period_until_finish():
    tz = ZoneInfo('Europe/Moscow')
    df = pd.DataFrame(
        [
            {
                'created_at': datetime(2026, 4, 1, 10, 0, tzinfo=tz),
                'price': 250.0,
                'quantity': 6,
                'provider': 'AUTO-GA',
            },
            {
                'created_at': datetime(2026, 4, 2, 10, 0, tzinfo=tz),
                'price': 250.0,
                'quantity': 6,
                'provider': 'AUTO-GA',
            },
        ]
    )

    actual_df, step_df, stockout_df = prepare_price_history_plot_data(
        df,
        datetime(2026, 4, 3, 18, 0, tzinfo=tz),
    )

    assert len(actual_df) == 2
    assert stockout_df.empty
    assert len(step_df) == 3
    assert bool(step_df.iloc[-1]['is_projection']) is True
    assert step_df.iloc[-1]['created_at'] == pd.Timestamp(
        datetime(2026, 4, 3, 18, 0, tzinfo=tz)
    )
    assert step_df.iloc[-1]['price'] == 250.0
    assert step_df.iloc[-1]['quantity'] == 6


def test_prepare_price_history_plot_data_marks_stockout_points():
    tz = ZoneInfo('Europe/Moscow')
    df = pd.DataFrame(
        [
            {
                'created_at': datetime(2026, 4, 1, 10, 0, tzinfo=tz),
                'price': 250.0,
                'quantity': 6,
                'provider': 'AUTO-GA',
            },
            {
                'created_at': datetime(2026, 4, 2, 12, 30, tzinfo=tz),
                'price': 250.0,
                'quantity': 0,
                'provider': 'AUTO-GA',
            },
        ]
    )

    _, step_df, stockout_df = prepare_price_history_plot_data(
        df,
        datetime(2026, 4, 3, 18, 0, tzinfo=tz),
    )

    assert len(stockout_df) == 1
    assert stockout_df.iloc[0]['created_at'] == pd.Timestamp(
        datetime(2026, 4, 2, 12, 30, tzinfo=tz)
    )
    assert stockout_df.iloc[0]['quantity'] == 0
    assert step_df.iloc[-1]['quantity'] == 0
