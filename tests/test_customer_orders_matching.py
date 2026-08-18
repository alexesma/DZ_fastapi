from types import SimpleNamespace

import pandas as pd
import pytest

from dz_fastapi.api.validators import normalize_brand_name
from dz_fastapi.crud.partner import crud_customer_pricelist
from dz_fastapi.services.customer_orders import (
    OfferRow,
    ParsedOrderRow,
    _apply_matched_email_state_for_configs,
    _build_current_offers,
    _canonicalize_brand_key,
    _merge_published_dragonzap_alias_offers,
    _normalize_key,
    _normalize_oem_key,
    _prepare_customer_order_context,
    _repair_cp1251_mojibake,
)
from dz_fastapi.services.process import _apply_source_filters


def test_normalize_oem_key_matches_autopart_storage_rules():
    assert _normalize_oem_key("90119-08419") == "9011908419"
    assert _normalize_oem_key(" 90 119/08419 ") == "9011908419"


def test_normalize_brand_name_matches_existing_rules():
    assert normalize_brand_name("Toyota") == "TOYOTA"
    assert normalize_brand_name("  lexus  ") == "LEXUS"
    assert normalize_brand_name("Лифан") == "ЛИФАН"


def test_normalize_key_uses_brand_aliases_for_synonyms():
    brand_aliases = {
        "TOYOTA": "TOYOTA",
        "LEXUS": "TOYOTA",
    }

    assert _canonicalize_brand_key("Lexus", brand_aliases) == "TOYOTA"
    assert _normalize_key("90119-08419", "Toyota", brand_aliases) == (
        "9011908419",
        "TOYOTA",
    )
    assert _normalize_key("9011908419", "Lexus", brand_aliases) == (
        "9011908419",
        "TOYOTA",
    )


def test_published_dragonzap_alias_maps_to_physical_stock_offer():
    source = SimpleNamespace(
        oem_number="DZT113001111BA",
        brand=SimpleNamespace(name="DRAGONZAP"),
    )
    alias = SimpleNamespace(
        advertised_oem="1014003218",
        advertised_brand="DRAGONZAP",
        source_autopart=source,
    )
    pricelist = SimpleNamespace(published_aliases=[alias])
    source_key = _normalize_key(
        source.oem_number,
        source.brand.name,
        None,
    )
    source_offer = OfferRow(
        autopart_id=501,
        provider_id=1,
        provider_config_id=2,
        quantity=46,
        price=125.0,
        supplier_price=100.0,
        is_own_price=True,
        actual_oem=source.oem_number,
        actual_brand=source.brand.name,
        actual_name="Actual stock part",
    )

    offers = _merge_published_dragonzap_alias_offers(
        pricelist,
        {source_key: source_offer},
    )
    matched = offers[_normalize_key("1014003218", "DRAGONZAP", None)]

    assert matched.autopart_id == 501
    assert matched.quantity == 46
    assert matched.price == 125.0
    assert matched.match_type == "dragonzap_cross"
    assert matched.actual_oem == "DZT113001111BA"


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
                "oem_number": "SH0113TM3",
                "brand": "DRAGONZAP",
                "price": 70,
                "quantity": 1,
            }
        ]
    )

    filtered = _apply_source_filters(df, source)
    ignored = _apply_source_filters(df, source, ignore_price_quantity_filters=True)

    assert filtered.empty
    assert len(ignored) == 1
    assert ignored.iloc[0]["oem_number"] == "SH0113TM3"


def test_apply_coefficient_can_ignore_price_and_quantity_thresholds():
    config = SimpleNamespace(
        individual_markups={},
        default_filters={},
        brand_filters=[],
        category_filter=[],
        price_intervals=[{"from": 100, "to": 1000}],
        position_filters=[],
        supplier_quantity_filters=[{"min_quantity": 5}],
        additional_filters={},
        own_filters={},
        other_filters={"min_price": 100, "min_quantity": 5},
        supplier_filters={},
        general_markup=1,
    )
    df = pd.DataFrame(
        [
            {
                "price": 70,
                "quantity": 1,
                "provider_id": 915,
                "is_own_price": False,
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
    assert float(ignored.iloc[0]["price"]) == 70.0


def test_repair_cp1251_mojibake_fixes_garbled_russian_name():
    assert _repair_cp1251_mojibake("ÏÎÄÊÐÛËÎÊ ÊÎË¨ÑÍÎÉ ÀÐÊÈ T19C") == "ПОДКРЫЛОК КОЛЁСНОЙ АРКИ T19C"


def test_repair_cp1251_mojibake_keeps_normal_text():
    assert _repair_cp1251_mojibake("Подкрылок колесной арки T19C") == (
        "Подкрылок колесной арки T19C"
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
    msg = SimpleNamespace(uid="105", folder_name="INBOX", received_at=None)

    _apply_matched_email_state_for_configs(
        session,
        [config_old, config_new],
        msg,
        inbox_account=None,
    )

    assert config_old.last_uid == 105
    assert config_new.last_uid == 105
    assert config_old.folder_last_uids["INBOX"] == 105
    assert config_new.folder_last_uids["INBOX"] == 105


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

    requested_oems = set()

    async def _fake_fetch_data(*args, **kwargs):
        requested_oems.update(kwargs.get("oem_numbers") or set())
        return [SimpleNamespace()]

    async def _fake_transform(*args, **kwargs):
        return pd.DataFrame(
            [
                {
                    "autopart_id": 10,
                    "provider_id": 937,
                    "provider_config_id": 101,
                    "oem_number": "SMD359158",
                    "brand": "CHERY",
                    "quantity": 5,
                    "price": 100.0,
                    "is_own_price": False,
                }
            ]
        )

    monkeypatch.setattr(
        "dz_fastapi.services.customer_orders." "crud_customer_pricelist_source.get_by_config_id",
        _fake_sources,
    )
    monkeypatch.setattr(
        "dz_fastapi.services.customer_orders." "crud_pricelist.get_latest_pricelist_by_config",
        _fake_latest_pricelist,
    )
    monkeypatch.setattr(
        "dz_fastapi.services.customer_orders." "crud_pricelist.fetch_pricelist_data",
        _fake_fetch_data,
    )
    monkeypatch.setattr(
        "dz_fastapi.services.customer_orders." "crud_pricelist.transform_to_dataframe",
        _fake_transform,
    )

    offers = await _build_current_offers(
        session=None,
        config=config,
        brand_aliases=None,
        required_oems={"SMD359158"},
    )

    assert len(offers) == 1
    assert requested_oems == {"SMD359158"}
    offer = next(iter(offers.values()))
    assert offer.supplier_price == pytest.approx(100.0)
    assert offer.price == pytest.approx(300.0)


@pytest.mark.asyncio
async def test_order_context_loads_only_requested_and_alias_source_oems(monkeypatch):
    requested = ParsedOrderRow(
        row_index=1,
        oem="2020-01932-AA",
        brand="DRAGONZAP",
        name="Стойка",
        requested_qty=2,
        requested_price=300.0,
    )
    source_autopart = SimpleNamespace(
        oem_number="DZ2906150XSZ08A",
        brand=SimpleNamespace(name="DRAGONZAP"),
    )
    alias = SimpleNamespace(
        advertised_oem="202001932AA",
        advertised_brand="DRAGONZAP",
        price=311.0,
        source_autopart=source_autopart,
    )
    last_pricelist = SimpleNamespace(
        autopart_associations=[],
        published_aliases=[alias],
    )
    loaded_oems = set()
    offered_oems = set()

    async def _fake_brand_aliases(*args, **kwargs):
        return {}

    async def _fake_latest(*args, **kwargs):
        loaded_oems.update(kwargs.get("normalized_oems") or set())
        return last_pricelist

    async def _fake_config(*args, **kwargs):
        return SimpleNamespace(id=11)

    async def _fake_offers(*args, **kwargs):
        offered_oems.update(kwargs.get("required_oems") or set())
        return {}

    monkeypatch.setattr(
        "dz_fastapi.services.customer_orders._load_brand_alias_map",
        _fake_brand_aliases,
    )
    monkeypatch.setattr(
        "dz_fastapi.services.customer_orders._load_latest_customer_pricelist",
        _fake_latest,
    )
    monkeypatch.setattr(
        "dz_fastapi.services.customer_orders._resolve_pricelist_config",
        _fake_config,
    )
    monkeypatch.setattr(
        "dz_fastapi.services.customer_orders._build_current_offers",
        _fake_offers,
    )

    config = SimpleNamespace(customer_id=946, pricelist_config_id=11)
    await _prepare_customer_order_context(None, config, [requested])

    assert loaded_oems == {"202001932AA"}
    assert offered_oems == {"202001932AA", "DZ2906150XSZ08A"}
