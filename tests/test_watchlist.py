import pytest


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

    response = await async_client.get(
        "/watchlist", params={"page": 1, "page_size": 10}
    )
    assert response.status_code == 200
    data = response.json()
    assert data["total"] >= 1
    assert any(row["id"] == item["id"] for row in data["items"])

    response = await async_client.get("/watchlist", params={"search": "abc"})
    assert response.status_code == 200
    data = response.json()
    assert any(row["id"] == item["id"] for row in data["items"])

    response = await async_client.delete(f"/watchlist/{item['id']}")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"

    response = await async_client.get(
        "/watchlist", params={"search": "ABC123"}
    )
    assert response.status_code == 200
    data = response.json()
    assert all(row["id"] != item["id"] for row in data["items"])
