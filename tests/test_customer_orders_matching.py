from types import SimpleNamespace

import pandas as pd
import pytest

from dz_fastapi.api.validators import normalize_brand_name
from dz_fastapi.crud.partner import crud_customer_pricelist
from dz_fastapi.services.customer_orders import (
    _apply_matched_email_state_for_configs, _build_current_offers,
    _canonicalize_brand_key, _normalize_key, _normalize_oem_key,
    _repair_cp1251_mojibake)
from dz_fastapi.services.process import _apply_source_filters


def test_normalize_oem_key_matches_autopart_storage_rules():
    assert _normalize_oem_key('90119-08419') == '9011908419'
    assert _normalize_oem_key(' 90 119/08419 ') == '9011908419'


def test_normalize_brand_name_matches_existing_rules():
    assert normalize_brand_name('Toyota') == 'TOYOTA'
    assert normalize_brand_name('  lexus  ') == 'LEXUS'
    assert normalize_brand_name('Лифан') == 'ЛИФАН'


def test_normalize_key_uses_brand_aliases_for_synonyms():
    brand_aliases = {
        'TOYOTA': 'TOYOTA',
        'LEXUS': 'TOYOTA',
    }

    assert _canonicalize_brand_key('Lexus', brand_aliases) == 'TOYOTA'
    assert _normalize_key('90119-08419', 'Toyota', brand_aliases) == (
        '9011908419',
        'TOYOTA',
    )
    assert _normalize_key('9011908419', 'Lexus', brand_aliases) == (
        '9011908419',
        'TOYOTA',
    )


def test_source_filters_can_ignore_price_and_quantity_thresholds():
    source = SimpleNamespace(
        brand_filters={},
        position_filters={},
        min_price=100,
        max_price=None,
        min_quantity=10,
        max_quantity=None,
    )
    df = pd.DataFrame(
        [
            {
                'oem_number': 'SH0113TM3',
                'brand': 'DRAGONZAP',
                'price': 70,
                'quantity': 1,
            }
        ]
    )

    filtered = _apply_source_filters(df, source)
    ignored = _apply_source_filters(
        df, source, ignore_price_quantity_filters=True
    )

    assert filtered.empty
    assert len(ignored) == 1
    assert ignored.iloc[0]['oem_number'] == 'SH0113TM3'


def test_apply_coefficient_can_ignore_price_and_quantity_thresholds():
    config = SimpleNamespace(
        individual_markups={},
        default_filters={},
        brand_filters=[],
        category_filter=[],
        price_intervals=[{'from': 100, 'to': 1000}],
        position_filters=[],
        supplier_quantity_filters=[{'min_quantity': 5}],
        additional_filters={},
        own_filters={},
        other_filters={'min_price': 100, 'min_quantity': 5},
        supplier_filters={},
        general_markup=1,
    )
    df = pd.DataFrame(
        [
            {
                'price': 70,
                'quantity': 1,
                'provider_id': 915,
                'is_own_price': False,
            }
        ]
    )

    filtered = crud_customer_pricelist.apply_coefficient(
        df.copy(),
        config,
        apply_general_markup=False,
        provider_id=915,
        is_own_price=False,
    )
    ignored = crud_customer_pricelist.apply_coefficient(
        df.copy(),
        config,
        apply_general_markup=False,
        provider_id=915,
        is_own_price=False,
        ignore_price_quantity_filters=True,
    )

    assert filtered.empty
    assert len(ignored) == 1
    assert float(ignored.iloc[0]['price']) == 70.0


def test_repair_cp1251_mojibake_fixes_garbled_russian_name():
    assert (
        _repair_cp1251_mojibake('ÏÎÄÊÐÛËÎÊ ÊÎË¨ÑÍÎÉ ÀÐÊÈ T19C')
        == 'ПОДКРЫЛОК КОЛЁСНОЙ АРКИ T19C'
    )


def test_repair_cp1251_mojibake_keeps_normal_text():
    assert _repair_cp1251_mojibake('Подкрылок колесной арки T19C') == (
        'Подкрылок колесной арки T19C'
    )


def test_apply_matched_email_state_updates_all_candidate_configs():
    class _Session:
        def __init__(self):
            self.added = []

        def add(self, obj):
            self.added.append(obj)

    session = _Session()
    config_old = SimpleNamespace(id=3, last_uid=100, folder_last_uids={})
    config_new = SimpleNamespace(id=4, last_uid=95, folder_last_uids={})
    msg = SimpleNamespace(uid='105', folder_name='INBOX', received_at=None)

    _apply_matched_email_state_for_configs(
        session,
        [config_old, config_new],
        msg,
        inbox_account=None,
    )

    assert config_old.last_uid == 105
    assert config_new.last_uid == 105
    assert config_old.folder_last_uids['INBOX'] == 105
    assert config_new.folder_last_uids['INBOX'] == 105


@pytest.mark.asyncio
async def test_build_current_offers_keeps_supplier_price_before_markups(
    monkeypatch,
):
    source = SimpleNamespace(
        enabled=True,
        provider_config_id=101,
        markup=2,
        brand_markups={},
        brand_filters={},
        position_filters={},
        min_price=None,
        max_price=None,
        min_quantity=None,
        max_quantity=None,
    )
    config = SimpleNamespace(
        id=77,
        individual_markups={},
        default_filters={},
        brand_filters=[],
        category_filter=[],
        price_intervals=[],
        position_filters=[],
        supplier_quantity_filters=[],
        additional_filters={},
        own_filters={},
        other_filters={},
        supplier_filters={},
        general_markup=1.5,
        own_price_list_markup=1,
        third_party_markup=1,
    )

    async def _fake_sources(*args, **kwargs):
        return [source]

    async def _fake_latest_pricelist(*args, **kwargs):
        return SimpleNamespace(id=501)

    async def _fake_fetch_data(*args, **kwargs):
        return [SimpleNamespace()]

    async def _fake_transform(*args, **kwargs):
        return pd.DataFrame(
            [
                {
                    'autopart_id': 10,
                    'provider_id': 937,
                    'provider_config_id': 101,
                    'oem_number': 'SMD359158',
                    'brand': 'CHERY',
                    'quantity': 5,
                    'price': 100.0,
                    'is_own_price': False,
                }
            ]
        )

    monkeypatch.setattr(
        'dz_fastapi.services.customer_orders.'
        'crud_customer_pricelist_source.get_by_config_id',
        _fake_sources,
    )
    monkeypatch.setattr(
        'dz_fastapi.services.customer_orders.'
        'crud_pricelist.get_latest_pricelist_by_config',
        _fake_latest_pricelist,
    )
    monkeypatch.setattr(
        'dz_fastapi.services.customer_orders.'
        'crud_pricelist.fetch_pricelist_data',
        _fake_fetch_data,
    )
    monkeypatch.setattr(
        'dz_fastapi.services.customer_orders.'
        'crud_pricelist.transform_to_dataframe',
        _fake_transform,
    )

    offers = await _build_current_offers(
        session=None,
        config=config,
        brand_aliases=None,
    )

    assert len(offers) == 1
    offer = next(iter(offers.values()))
    assert offer.supplier_price == pytest.approx(100.0)
    assert offer.price == pytest.approx(300.0)
