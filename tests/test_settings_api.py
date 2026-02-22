import pytest


@pytest.mark.asyncio
async def test_price_check_schedule(async_client):
    response = await async_client.get("/settings/price-check")
    assert response.status_code == 200
    data = response.json()
    assert "enabled" in data
    assert "days" in data
    assert "times" in data

    payload = {"enabled": True, "days": ["mon"], "times": ["09:00"]}
    response = await async_client.put("/settings/price-check", json=payload)
    assert response.status_code == 200
    updated = response.json()
    assert updated["days"] == ["mon"]
    assert updated["times"] == ["09:00"]


@pytest.mark.asyncio
async def test_price_check_logs_endpoint(async_client):
    response = await async_client.get(
        "/alerts/price-check-logs", params={"limit": 10}
    )
    assert response.status_code == 200
    assert isinstance(response.json(), list)
