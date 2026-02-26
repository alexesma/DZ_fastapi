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


@pytest.mark.asyncio
async def test_scheduler_settings(async_client):
    response = await async_client.get("/settings/scheduler")
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    assert data, "Expected scheduler settings list to be non-empty"
    key = data[0]["key"]

    payload = {"enabled": True, "days": ["mon"], "times": ["09:00"]}
    response = await async_client.put(
        f"/settings/scheduler/{key}", json=payload
    )
    assert response.status_code == 200
    updated = response.json()
    assert updated["key"] == key


@pytest.mark.asyncio
async def test_monitoring_endpoints(async_client):
    response = await async_client.get("/settings/monitor/summary")
    assert response.status_code == 200
    summary = response.json()
    assert "db" in summary
    assert "system" in summary
    assert "app" in summary

    response = await async_client.post("/settings/monitor/snapshot")
    assert response.status_code == 200
    snapshot = response.json()
    assert "created_at" in snapshot

    response = await async_client.get(
        "/settings/monitor/snapshots", params={"limit": 5}
    )
    assert response.status_code == 200
    assert isinstance(response.json(), list)
