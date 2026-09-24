from types import SimpleNamespace

import pandas as pd
import pytest

from dz_fastapi.api.validators import normalize_brand_name
from dz_fastapi.crud.partner import crud_customer_pricelist
from dz_fastapi.models.partner import Provider, ProviderPriceListConfig
from dz_fastapi.services.customer_orders import (
    ConfirmedOwnCrossAlias,
    OfferRow,
    ParsedOrderRow,
    _apply_matched_email_state_for_configs,
    _build_current_offers,
    _canonicalize_brand_key,
    _get_or_create_open_supplier_order,
    _get_order_offer_sources,
    _merge_confirmed_own_cross_offers,
    _merge_published_dragonzap_alias_offers,
    _normalize_key,
    _normalize_oem_key,
    _prepare_customer_order_context,
    _repair_cp1251_mojibake,
    _supplier_order_source_config_id,
)
from dz_fastapi.services.process import _apply_source_filters


def test_normalize_oem_key_matches_autopart_storage_rules():
    assert _normalize_oem_key("90119-08419") == "9011908419"
    assert _normalize_oem_key(" 90 119/08419 ") == "9011908419"


def test_supplier_order_source_config_respects_provider_setting():
    assert (
        _supplier_order_source_config_id(
            split_orders_by_pricelist=False,
            provider_config_id=12,
        )
        is None
    )
    assert (
        _supplier_order_source_config_id(
            split_orders_by_pricelist=True,
            provider_config_id=12,
        )
        == 12
    )


@pytest.mark.asyncio
async def test_open_supplier_orders_are_isolated_by_pricelist_config(
    test_session,
    created_providers,
):
    provider = created_providers[0]
    configs = [
        ProviderPriceListConfig(
            provider_id=provider.id,
            start_row=1,
            oem_col=0,
            brand_col=1,
            qty_col=2,
            price_col=3,
            name_price=name,
        )
        for name in ("PRICE_A", "PRICE_B")
    ]
    test_session.add_all(configs)
    await test_session.flush()
    first = await _get_or_create_open_supplier_order(
        test_session,
        provider_id=provider.id,
        provider_config_id=configs[0].id,
    )
    same = await _get_or_create_open_supplier_order(
        test_session,
        provider_id=provider.id,
        provider_config_id=configs[0].id,
    )
    second = await _get_or_create_open_supplier_order(
        test_session,
        provider_id=provider.id,
        provider_config_id=configs[1].id,
    )
    general = await _get_or_create_open_supplier_order(
        test_session,
        provider_id=provider.id,
        provider_config_id=None,
    )

    assert same.id == first.id
    assert second.id != first.id
    assert general.id not in {first.id, second.id}


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


def test_confirmed_cross_maps_only_own_stock_without_overriding_direct_offer():
    alias = ConfirmedOwnCrossAlias(
        source_autopart_id=501,
        source_oem="DZ1086001128",
        source_brand="DRAGONZAP",
        advertised_oem="1086001128",
        advertised_brand="HOT-PARTS",
    )
    own_offer = OfferRow(
        autopart_id=501,
        provider_id=1,
        provider_config_id=2,
        quantity=1,
        price=536.0,
        supplier_price=536.0,
        is_own_price=True,
        actual_oem="DZ1086001128",
        actual_brand="DRAGONZAP",
    )
    source_key = _normalize_key("DZ1086001128", "DRAGONZAP", None)

    offers = _merge_confirmed_own_cross_offers([alias], {source_key: own_offer})
    matched = offers[_normalize_key("1086001128", "HOT-PARTS", None)]
    assert matched.autopart_id == 501
    assert matched.match_type == "confirmed_own_cross"

    direct_key = _normalize_key("1086001128", "HOT-PARTS", None)
    direct_offer = OfferRow(
        autopart_id=999,
        provider_id=8,
        provider_config_id=9,
        quantity=5,
        price=500.0,
        supplier_price=500.0,
        is_own_price=False,
    )
    offers = _merge_confirmed_own_cross_offers(
        [alias],
        {source_key: own_offer, direct_key: direct_offer},
    )
    assert offers[direct_key] is direct_offer

    supplier_offer = OfferRow(
        autopart_id=700,
        provider_id=9,
        provider_config_id=10,
        quantity=20,
        price=400.0,
        supplier_price=400.0,
        is_own_price=False,
    )
    assert _normalize_key("1086001128", "HOT-PARTS", None) not in (
        _merge_confirmed_own_cross_offers([alias], {source_key: supplier_offer})
    )


@pytest.mark.asyncio
async def test_order_sources_always_include_active_own_price(monkeypatch):
    configured_source = SimpleNamespace(provider_config_id=10, enabled=True)
    own_provider = Provider(id=1, is_own_price=True)
    own_config = ProviderPriceListConfig(id=20, provider_id=1, is_active=True)
    own_config.provider = own_provider

    async def _fake_sources(*args, **kwargs):
        return [configured_source]

    class _ScalarRows:
        def all(self):
            return [own_config]

    class _Session:
        async def scalars(self, _statement):
            return _ScalarRows()

    monkeypatch.setattr(
        "dz_fastapi.services.customer_orders." "crud_customer_pricelist_source.get_by_config_id",
        _fake_sources,
    )

    sources = await _get_order_offer_sources(_Session(), SimpleNamespace(id=77))

    assert [source.provider_config_id for source in sources] == [10, 20]
    assert sources[1].enabled is True
    assert sources[1].markup == pytest.approx(1.0)


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
    assert float(ignored.iloc[0]["price"]) == 70.0
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


def test_pricelist_rules_combine_applicability_and_honest_sign_filters():
    config = SimpleNamespace(
        individual_markups={},
        default_filters={
            "rules": [
                {"field": "applicability", "mode": "include", "values": [10]},
                {"field": "honest_sign", "mode": "exclude", "values": [100]},
            ]
        },
        brand_filters=[],
        category_filter=[],
        price_intervals=[],
        position_filters=[],
        supplier_quantity_filters=[],
        additional_filters={},
        own_filters={},
        other_filters={},
        supplier_filters={},
        general_markup=1,
    )
    df = pd.DataFrame(
        [
            {
                "autopart_id": 1,
                "brand_id": 1,
                "price": 100,
                "quantity": 1,
                "__applicability_node_ids": (10,),
                "__honest_sign_category_ids": (100,),
            },
            {
                "autopart_id": 2,
                "brand_id": 1,
                "price": 100,
                "quantity": 1,
                "__applicability_node_ids": (20,),
                "__honest_sign_category_ids": (200,),
            },
            {
                "autopart_id": 3,
                "brand_id": 1,
                "price": 100,
                "quantity": 1,
                "__applicability_node_ids": (10,),
                "__honest_sign_category_ids": (200,),
            },
        ]
    )

    result = crud_customer_pricelist.apply_coefficient(
        df,
        config,
        apply_general_markup=False,
        provider_id=1,
        is_own_price=False,
    )

    assert result["autopart_id"].tolist() == [3]


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

    async def _fake_fetch_dataframe(*args, **kwargs):
        requested_oems.update(kwargs.get("oem_numbers") or set())
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
        "dz_fastapi.services.customer_orders." "crud_pricelist.fetch_pricelist_dataframe",
        _fake_fetch_dataframe,
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

    async def _fake_confirmed_crosses(*args, **kwargs):
        return [
            ConfirmedOwnCrossAlias(
                source_autopart_id=502,
                source_oem="DZ-CATALOGUE-CROSS",
                source_brand="DRAGONZAP",
                advertised_oem=requested.oem,
                advertised_brand=requested.brand,
            )
        ]

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
    monkeypatch.setattr(
        "dz_fastapi.services.customer_orders._load_confirmed_own_cross_aliases",
        _fake_confirmed_crosses,
    )

    config = SimpleNamespace(customer_id=946, pricelist_config_id=11)
    await _prepare_customer_order_context(None, config, [requested])

    assert loaded_oems == {"202001932AA"}
    assert offered_oems == {
        "202001932AA",
        "DZ2906150XSZ08A",
        "DZCATALOGUECROSS",
    }
