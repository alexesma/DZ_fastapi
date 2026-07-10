from datetime import timedelta

import pytest

from dz_fastapi.core.time import now_moscow
from dz_fastapi.models.inventory import StockLot
from dz_fastapi.models.partner import (
    PriceList,
    PriceListAutoPartAssociation,
    Provider,
    ProviderPriceListConfig,
)
from dz_fastapi.models.watchlist import PriceWatchItem


@pytest.mark.asyncio
async def test_watchlist_crud(async_client):
    payload = {
        "brand": "TEST",
        "oem": "ABC123",
        "max_price": 100.5,
    }
    response = await async_client.post("/watchlist", json=payload)
    assert response.status_code == 201
    item = response.json()
    assert item["brand"] == "TEST"
    assert item["oem"] == "ABC123"
    assert item["max_price"] == 100.5

    response = await async_client.get("/watchlist", params={"page": 1, "page_size": 10})
    assert response.status_code == 200
    data = response.json()
    assert data["total"] >= 1
    assert any(row["id"] == item["id"] for row in data["items"])

    response = await async_client.get("/watchlist", params={"search": "abc"})
    assert response.status_code == 200
    data = response.json()
    assert any(row["id"] == item["id"] for row in data["items"])

    response = await async_client.patch(
        f"/watchlist/{item['id']}",
        json={
            "brand": "TEST-UPDATED",
            "oem": "XYZ999",
            "max_price": 111.5,
        },
    )
    assert response.status_code == 200
    updated = response.json()
    assert updated["brand"] == "TEST-UPDATED"
    assert updated["oem"] == "XYZ999"
    assert updated["max_price"] == 111.5

    response = await async_client.delete(f"/watchlist/{item['id']}")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"

    response = await async_client.get("/watchlist", params={"search": "ABC123"})
    assert response.status_code == 200
    data = response.json()
    assert all(row["id"] != item["id"] for row in data["items"])


@pytest.mark.asyncio
async def test_watchlist_returns_saved_provider_offer(
    async_client,
    test_session,
    created_autopart,
    created_brand,
    created_providers,
    created_pricelist_config,
):
    provider = created_providers[0]
    response = await async_client.post(
        "/watchlist",
        json={
            "brand": created_brand.name,
            "oem": created_autopart.oem_number,
            "max_price": 200.0,
        },
    )
    assert response.status_code == 201
    watch_id = response.json()["id"]

    pricelist = PriceList(
        provider_id=provider.id,
        provider_config_id=created_pricelist_config.id,
    )
    test_session.add(pricelist)
    await test_session.flush()
    test_session.add(
        PriceListAutoPartAssociation(
            pricelist_id=pricelist.id,
            autopart_id=created_autopart.id,
            quantity=7,
            price=150.0,
            multiplicity=2,
        )
    )
    watch_item = await test_session.get(PriceWatchItem, watch_id)
    watch_item.last_seen_provider_id = provider.id
    watch_item.last_seen_provider_config_id = created_pricelist_config.id
    watch_item.last_seen_provider_pricelist_id = pricelist.id
    watch_item.last_seen_provider_price = 150.0
    test_session.add(watch_item)
    await test_session.commit()

    list_response = await async_client.get(
        "/watchlist", params={"page": 1, "page_size": 10}
    )
    assert list_response.status_code == 200
    row = next(
        item
        for item in list_response.json()["items"]
        if item["id"] == watch_id
    )
    offer = row["last_seen_provider_offer"]
    assert offer["source_type"] == "supplier"
    assert offer["supplier_id"] == provider.id
    assert offer["autopart_id"] == created_autopart.id
    assert offer["quantity"] == 7
    assert offer["price"] == 150.0
    assert offer["min_qnt"] == 2


@pytest.mark.asyncio
async def test_watchlist_returns_own_stock_info(
    async_client,
    test_session,
    created_autopart,
    created_brand,
):
    own_provider = Provider(
        name="Own Price Provider",
        email_contact="own@example.com",
        email_incoming_price="own-prices@example.com",
        description="Own price provider",
        comment="No comment",
        type_prices="Wholesale",
        is_own_price=True,
    )
    test_session.add(own_provider)
    await test_session.flush()
    own_config = ProviderPriceListConfig(
        provider_id=own_provider.id,
        start_row=1,
        oem_col=0,
        brand_col=1,
        name_col=2,
        qty_col=3,
        price_col=4,
        name_price="OWN_PRICE",
        use_for_order_insights=True,
    )
    test_session.add(own_config)
    await test_session.flush()
    own_pricelist = PriceList(
        provider_id=own_provider.id,
        provider_config_id=own_config.id,
        is_active=True,
    )
    test_session.add(own_pricelist)
    await test_session.flush()
    test_session.add(
        PriceListAutoPartAssociation(
            pricelist_id=own_pricelist.id,
            autopart_id=created_autopart.id,
            quantity=4,
            price=990.0,
        )
    )
    old_lot_at = now_moscow() - timedelta(days=30)
    fresh_lot_at = now_moscow() - timedelta(days=3)
    test_session.add_all(
        [
            StockLot(
                autopart_id=created_autopart.id,
                initial_quantity=10,
                remaining_quantity=0,
                cost_price=500.0,
                received_at=old_lot_at,
            ),
            StockLot(
                autopart_id=created_autopart.id,
                initial_quantity=5,
                remaining_quantity=5,
                cost_price=620.0,
                received_at=fresh_lot_at,
            ),
        ]
    )
    await test_session.commit()

    response = await async_client.post(
        "/watchlist",
        json={
            "brand": created_brand.name,
            "oem": created_autopart.oem_number,
            "max_price": 700.0,
        },
    )
    assert response.status_code == 201
    watch_id = response.json()["id"]

    list_response = await async_client.get(
        "/watchlist", params={"page": 1, "page_size": 10}
    )
    assert list_response.status_code == 200
    row = next(
        item
        for item in list_response.json()["items"]
        if item["id"] == watch_id
    )
    assert row["last_purchase_price"] == 620.0
    assert row["last_purchase_at"] is not None
    assert row["current_price"] == 990.0
    assert row["stock_quantity"] == 4


@pytest.mark.asyncio
async def test_watchlist_stock_info_without_own_price_config(
    async_client,
    test_session,
    created_autopart,
    created_brand,
):
    """Без «своего» прайса берём закупку из карточки, остаток — из партий."""
    created_autopart.purchase_price = 480.0
    test_session.add(created_autopart)
    test_session.add(
        StockLot(
            autopart_id=created_autopart.id,
            initial_quantity=6,
            remaining_quantity=2,
            cost_price=None,
            received_at=now_moscow() - timedelta(days=2),
        )
    )
    await test_session.commit()

    response = await async_client.post(
        "/watchlist",
        json={
            "brand": created_brand.name,
            "oem": created_autopart.oem_number,
        },
    )
    assert response.status_code == 201
    watch_id = response.json()["id"]

    list_response = await async_client.get(
        "/watchlist", params={"page": 1, "page_size": 10}
    )
    assert list_response.status_code == 200
    row = next(
        item
        for item in list_response.json()["items"]
        if item["id"] == watch_id
    )
    assert row["last_purchase_price"] == 480.0
    assert row["last_purchase_at"] is None
    assert row["current_price"] is None
    assert row["stock_quantity"] == 2
