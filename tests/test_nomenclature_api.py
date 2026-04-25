import pytest


@pytest.mark.asyncio
async def test_nomenclature_catalog_search_routes_are_not_shadowed(
    async_client,
    created_autopart,
):
    response = await async_client.get(
        "/autoparts/catalog/",
        params={"q_oem": created_autopart.oem_number[:5]},
    )
    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["total"] == 1
    assert payload["items"][0]["id"] == created_autopart.id

    response = await async_client.get(
        "/autoparts/catalog/",
        params={"q_name": "autopart"},
    )
    assert response.status_code == 200, response.text
    assert response.json()["total"] == 1

    response = await async_client.get(
        "/autoparts/catalog/",
        params={"q_brand": "brand"},
    )
    assert response.status_code == 200, response.text
    assert response.json()["total"] == 1


@pytest.mark.asyncio
async def test_nomenclature_static_autopart_routes_are_not_shadowed(
    async_client,
    created_storage,
):
    response = await async_client.get("/autoparts/storage-locations/")
    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload == [
        {"id": created_storage.id, "name": created_storage.name}
    ]
