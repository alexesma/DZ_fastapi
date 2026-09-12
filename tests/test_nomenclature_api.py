import pytest

from dz_fastapi.api.deps import get_current_user
from dz_fastapi.main import app
from dz_fastapi.models.autopart import AutoPart, Photo
from dz_fastapi.models.cross import AutoPartCross
from dz_fastapi.models.nomenclature import ApplicabilityNode, autopart_applicability_association
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


@pytest.mark.asyncio
async def test_catalog_reports_applicability_and_crosses(
    async_client,
    test_session,
    created_brand,
):
    """В списке номенклатуры видно применимость и число кроссов.

    Раньше наполненность карточки приходилось проверять, открывая каждый
    товар: в таблице не было ни применимости, ни кроссов.
    """
    деталь = AutoPart(
        brand_id=created_brand.id,
        oem_number='APPL0001',
        name='Деталь с применимостью',
    )
    пустая = AutoPart(
        brand_id=created_brand.id,
        oem_number='APPL0002',
        name='Деталь без связей',
    )
    test_session.add_all([деталь, пустая])
    await test_session.flush()

    узлы = [
        ApplicabilityNode(name='Toyota Camry XV70', node_type='vehicle'),
        ApplicabilityNode(name='Сальник 30x52x8', node_type='part'),
    ]
    test_session.add_all(узлы)
    await test_session.flush()
    for узел in узлы:
        await test_session.execute(
            autopart_applicability_association.insert().values(
                autopart_id=деталь.id, applicability_node_id=узел.id
            )
        )
    test_session.add_all(
        [
            AutoPartCross(
                source_autopart_id=деталь.id,
                cross_brand_id=created_brand.id,
                cross_oem_number='CROSS-A',
                is_bidirectional=True,
                priority=1,
            ),
            AutoPartCross(
                source_autopart_id=деталь.id,
                cross_brand_id=created_brand.id,
                cross_oem_number='CROSS-B',
                is_bidirectional=False,
                priority=2,
            ),
        ]
    )
    await test_session.commit()

    ответ = await async_client.get(
        '/autoparts/catalog/', params={'q_oem': 'APPL000', 'limit': 50}
    )

    assert ответ.status_code == 200
    по_артикулу = {
        строка['oem_number']: строка for строка in ответ.json()['items']
    }

    с_связями = по_артикулу['APPL0001']
    assert с_связями['applicability_count'] == 2
    assert с_связями['applicability_names'] == [
        'Toyota Camry XV70',
        'Сальник 30x52x8',
    ]
    assert с_связями['cross_count'] == 2

    без_связей = по_артикулу['APPL0002']
    assert без_связей['applicability_count'] == 0
    assert без_связей['applicability_names'] == []
    assert без_связей['cross_count'] == 0


@pytest.mark.asyncio
async def test_catalog_filters_by_applicability_and_crosses(
    async_client,
    test_session,
    created_brand,
):
    """Отбор незаполненных карточек: без применимости и без кроссов.

    Просматривать наполненность подряд по страницам бессмысленно —
    нужен отбор.
    """
    с_применимостью = AutoPart(
        brand_id=created_brand.id,
        oem_number='LINKS0001',
        name='Есть применимость',
    )
    с_кроссом = AutoPart(
        brand_id=created_brand.id,
        oem_number='LINKS0002',
        name='Есть кросс',
    )
    пустая = AutoPart(
        brand_id=created_brand.id,
        oem_number='LINKS0003',
        name='Ни того ни другого',
    )
    test_session.add_all([с_применимостью, с_кроссом, пустая])
    await test_session.flush()

    узел = ApplicabilityNode(name='Chery Tiggo 7 Pro', node_type='vehicle')
    test_session.add(узел)
    await test_session.flush()
    await test_session.execute(
        autopart_applicability_association.insert().values(
            autopart_id=с_применимостью.id, applicability_node_id=узел.id
        )
    )
    test_session.add(
        AutoPartCross(
            source_autopart_id=с_кроссом.id,
            cross_brand_id=created_brand.id,
            cross_oem_number='LINKS-CROSS',
            is_bidirectional=True,
            priority=1,
        )
    )
    await test_session.commit()

    async def артикулы(**фильтр):
        ответ = await async_client.get(
            '/autoparts/catalog/',
            params={'q_oem': 'LINKS000', 'limit': 50, **фильтр},
        )
        assert ответ.status_code == 200
        return {
            строка['oem_number'] for строка in ответ.json()['items']
        }

    все = await артикулы()
    assert {'LINKS0001', 'LINKS0002', 'LINKS0003'} <= все

    assert 'LINKS0001' in await артикулы(links='with_applicability')
    assert 'LINKS0002' not in await артикулы(links='with_applicability')

    без_применимости = await артикулы(links='without_applicability')
    assert 'LINKS0001' not in без_применимости
    assert {'LINKS0002', 'LINKS0003'} <= без_применимости

    assert 'LINKS0002' in await артикулы(links='with_crosses')
    assert 'LINKS0001' not in await артикулы(links='with_crosses')

    без_кроссов = await артикулы(links='without_crosses')
    assert 'LINKS0002' not in без_кроссов
    assert {'LINKS0001', 'LINKS0003'} <= без_кроссов


@pytest.mark.asyncio
async def test_catalog_rejects_unknown_links_filter(async_client):
    ответ = await async_client.get(
        '/autoparts/catalog/', params={'links': 'что-то своё'}
    )
    assert ответ.status_code == 422
