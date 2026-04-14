from types import SimpleNamespace

import pandas as pd

from dz_fastapi.api.validators import normalize_brand_name
from dz_fastapi.crud.partner import crud_customer_pricelist
from dz_fastapi.services.customer_orders import (_canonicalize_brand_key,
                                                 _normalize_key,
                                                 _normalize_oem_key,
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
