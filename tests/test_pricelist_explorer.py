"""Просмотр загруженного прайса поставщика: сравнение, фильтры, интерес."""

from datetime import date
from decimal import Decimal

import pytest

from dz_fastapi.models.autopart import AutoPart, AutoPartTurnoverSummary
from dz_fastapi.models.brand import Brand
from dz_fastapi.models.partner import PriceList, PriceListAutoPartAssociation


async def _setup(test_session, created_brand, provider, config):
    other = Brand(name="OTHER BRAND", country_of_origin="China")
    test_session.add(other)
    await test_session.flush()
    parts = {
        "up": AutoPart(brand_id=created_brand.id, oem_number="UP1", name="Подорожала"),
        "down": AutoPart(brand_id=created_brand.id, oem_number="DOWN1", name="Подешевела"),
        "new": AutoPart(brand_id=other.id, oem_number="NEW1", name="Новая"),
        "gone": AutoPart(brand_id=other.id, oem_number="GONE1", name="Исчезла"),
    }
    test_session.add_all(parts.values())
    await test_session.flush()
    old = PriceList(provider_id=provider.id, provider_config_id=config.id, date=date(2026, 9, 1))
    new = PriceList(provider_id=provider.id, provider_config_id=config.id, date=date(2026, 9, 20))
    test_session.add_all([old, new])
    await test_session.flush()

    def row(pl, key, price, qty):
        return PriceListAutoPartAssociation(
            pricelist_id=pl.id, autopart_id=parts[key].id, price=Decimal(price), quantity=qty
        )

    test_session.add_all(
        [
            row(old, "up", "100", 5),
            row(old, "down", "200", 10),
            row(old, "gone", "50", 3),
            row(new, "up", "130", 5),
            row(new, "down", "150", 2),
            row(new, "new", "70", 8),
        ]
    )
    test_session.add(
        AutoPartTurnoverSummary(
            autopart_id=parts["up"].id,
            sold_qty_30d=7,
            sold_qty_90d=20,
            recommendation_score=80.0,
            is_top=True,
            min_purchase_price=Decimal("120"),
        )
    )
    await test_session.commit()
    return parts, old, new


@pytest.mark.asyncio
async def test_explorer_compares_with_previous_and_filters(
    async_client, test_session, created_brand, created_providers, created_pricelist_config
):
    provider = created_providers[0]
    parts, old, new = await _setup(test_session, created_brand, provider, created_pricelist_config)
    url = f"/providers/{provider.id}/pricelist-explorer/"

    data = (await async_client.get(url, params={"config_id": created_pricelist_config.id})).json()
    assert data["pricelist"]["id"] == new.id
    assert data["compared_with"]["id"] == old.id
    assert data["total"] == 3
    assert data["summary"]["removed_positions"] == 1
    assert data["summary"]["new_positions"] == 1
    by_oem = {r["oem_number"]: r for r in data["rows"]}
    assert round(by_oem["UP1"]["price_change_pct"], 1) == 30.0
    assert round(by_oem["DOWN1"]["price_change_pct"], 1) == -25.0
    assert by_oem["NEW1"]["is_new"] is True
    assert round(by_oem["UP1"]["vs_market_pct"], 1) == 8.3

    def oems(**params):
        return async_client.get(url, params={"config_id": created_pricelist_config.id, **params})

    res = (await oems(price_change="up", price_change_min_pct=20)).json()
    assert [r["oem_number"] for r in res["rows"]] == ["UP1"]
    res = (await oems(price_change="down")).json()
    assert [r["oem_number"] for r in res["rows"]] == ["DOWN1"]
    res = (await oems(price_change="new")).json()
    assert [r["oem_number"] for r in res["rows"]] == ["NEW1"]
    res = (await oems(top_only="true")).json()
    assert [r["oem_number"] for r in res["rows"]] == ["UP1"]
    res = (await oems(demand="without")).json()
    assert {r["oem_number"] for r in res["rows"]} == {"DOWN1", "NEW1"}
    res = (await oems(brand_ids=parts["new"].brand_id)).json()
    assert [r["oem_number"] for r in res["rows"]] == ["NEW1"]
    res = (await oems(qty_max=5, sort_by="price", sort_dir="asc")).json()
    assert [r["oem_number"] for r in res["rows"]] == ["UP1", "DOWN1"]
    res = (await oems(quantity_change="down")).json()
    assert [r["oem_number"] for r in res["rows"]] == ["DOWN1"]

    brands = (
        await async_client.get(
            f"/providers/{provider.id}/pricelist-explorer/brands/",
            params={"config_id": created_pricelist_config.id},
        )
    ).json()
    assert {b["name"]: b["positions"] for b in brands} == {"TEST BRAND": 2, "OTHER BRAND": 1}

    lists = (await async_client.get(f"{url}pricelists/")).json()
    assert [p["id"] for p in lists] == [new.id, old.id]

    export = await async_client.get(
        f"{url}export/", params={"config_id": created_pricelist_config.id}
    )
    assert export.status_code == 200
    assert export.content[:2] == b"PK"
