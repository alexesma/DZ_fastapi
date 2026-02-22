import pytest

from dz_fastapi.services.scheduler import download_price_provider_task


@pytest.mark.asyncio
async def test_scheduler_logs_skip(async_client, test_session, monkeypatch):
    async def fake_get_emails(session):
        return []

    monkeypatch.setattr(
        "dz_fastapi.services.scheduler.get_emails",
        fake_get_emails,
    )

    from dz_fastapi.main import app

    await download_price_provider_task(app)

    response = await async_client.get(
        "/alerts/price-check-logs", params={"limit": 5}
    )
    assert response.status_code == 200
    data = response.json()
    assert len(data) >= 1
