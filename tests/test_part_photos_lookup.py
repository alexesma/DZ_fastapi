"""Ярлычок «фото детали»: пакетный поиск названий и фото по бренду и артикулу."""

import pytest

from dz_fastapi.models.autopart import AutoPart, Photo
from dz_fastapi.services.watchlist_site import extract_site_photo_url


@pytest.mark.asyncio
async def test_lookup_returns_name_and_photos(
    async_client, test_session, created_brand
):
    деталь = AutoPart(
        brand_id=created_brand.id, oem_number="PH123", name="Колодки"
    )
    test_session.add(деталь)
    await test_session.flush()
    test_session.add(Photo(autopart_id=деталь.id, url="/uploads/a.jpg"))
    пустая = AutoPart(
        brand_id=created_brand.id, oem_number="NOPH1", name="Без фото"
    )
    test_session.add(пустая)
    await test_session.commit()

    response = await async_client.post(
        "/autoparts/photos/lookup/",
        json={
            "items": [
                {"brand": created_brand.name, "oem": "ph-123"},
                {"brand": "Чужой", "oem": "NOPH1"},
                {"oem": "NOPH1"},
            ]
        },
    )
    assert response.status_code == 200
    rows = {r["oem"]: r for r in response.json()["rows"]}
    assert rows["PH123"]["photos"] == ["/uploads/a.jpg"]
    assert rows["PH123"]["name"] == "Колодки"
    assert rows["NOPH1"]["photos"] == []
    assert len(response.json()["rows"]) == 2


def test_extract_site_photo_url_skips_svg_labels():
    thumb = "https://x/thumbnails/images/5"
    assert extract_site_photo_url({"sys_info": {"goods_img_url": thumb}}) == thumb
    label = "https://x/labels/a.svg"
    assert extract_site_photo_url({"sys_info": {"goods_img_url": label}}) is None
    assert extract_site_photo_url({}) is None


@pytest.mark.asyncio
async def test_lookup_adds_site_photo_for_parts_without_catalog_photo(
    async_client, test_session, created_brand, monkeypatch
):
    async def fake_fetch(pairs):
        return {(brand.lower(), oem): "https://img/thumbnails/images/9" for brand, oem in pairs}

    monkeypatch.setattr("dz_fastapi.api.autopart.fetch_site_photos", fake_fetch)
    test_session.add(AutoPart(brand_id=created_brand.id, oem_number="SITE1", name="Без фото"))
    await test_session.commit()

    response = await async_client.post(
        "/autoparts/photos/lookup/",
        json={
            "items": [
                {"brand": created_brand.name, "oem": "SITE1"},
                {"brand": "CHERY", "oem": "A111601113"},
            ],
            "site": True,
        },
    )
    rows = {r["oem"]: r for r in response.json()["rows"]}
    assert rows["SITE1"]["photos"] == ["https://img/thumbnails/images/9"]
    assert rows["A111601113"]["photos"] == ["https://img/thumbnails/images/9"]
    assert rows["A111601113"]["autopart_id"] is None

    plain = await async_client.post(
        "/autoparts/photos/lookup/",
        json={"items": [{"brand": "CHERY", "oem": "A111601113"}]},
    )
    assert plain.json()["rows"] == []


def test_pick_thumbnail_ignores_labels():
    from dz_fastapi.services.site_photos import pick_thumbnail

    offers = [
        {"sys_info": {"goods_img_url": "https://x/labels/a.svg"}},
        {"sys_info": {"goods_img_url": "https://x/thumbnails/images/1"}},
    ]
    assert pick_thumbnail(offers) == "https://x/thumbnails/images/1"
    assert pick_thumbnail([]) is None
