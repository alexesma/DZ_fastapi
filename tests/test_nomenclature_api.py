import pytest

from dz_fastapi.api.deps import get_current_user
from dz_fastapi.main import app
from dz_fastapi.models.autopart import Photo
from dz_fastapi.models.user import User, UserRole, UserStatus


@pytest.fixture(autouse=True)
def override_current_user_for_nomenclature_api_tests():
    async def override_current_user():
        return User(
            id=1,
            name="Nomenclature Test Admin",
            email="nomenclature-admin@example.com",
            password_hash="not-a-real-hash",
            role=UserRole.ADMIN,
            status=UserStatus.ACTIVE,
        )

    app.dependency_overrides[get_current_user] = override_current_user
    yield
    app.dependency_overrides.pop(get_current_user, None)


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


@pytest.mark.asyncio
async def test_nomenclature_catalog_filters_partssoft_cards_and_content(
    async_client,
    test_session,
    created_autopart,
):
    created_autopart.partssoft_product_id = 741717
    test_session.add(Photo(autopart_id=created_autopart.id, url="https://example.com/photo.jpg"))
    await test_session.commit()

    response = await async_client.get(
        "/autoparts/catalog/",
        params={"partssoft": "true", "content": "complete"},
    )
    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["total"] == 1
    assert payload["items"][0]["partssoft_product_id"] == 741717
    assert payload["items"][0]["has_description"] is True
    assert payload["items"][0]["photo_count"] == 1
    assert payload["items"][0]["primary_photo_url"].endswith("photo.jpg")

    response = await async_client.get(
        "/autoparts/catalog/",
        params={"partssoft": "false"},
    )
    assert response.status_code == 200, response.text
    assert response.json()["total"] == 0

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
    assert payload == [{"id": created_storage.id, "name": created_storage.name}]
