import pytest
from httpx import AsyncClient, ASGITransport
from dz_fastapi.main import app
from dz_fastapi.core.db import get_session

@pytest.mark.asyncio
async def test_create_brand(test_session):
    async def override_get_session():
        yield test_session

    app.dependency_overrides[get_session] = override_get_session

    payload = {
        "name": "Test Brand",
        "country_of_origin": "USA",
    }
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        response = await ac.post("/brand", json=payload)

    assert response.status_code == 200, response.text
    data = response.json()
    assert data["name"] == "TEST BRAND"
    assert data["country_of_origin"] == "USA"

    # Clean up
    app.dependency_overrides.clear()
