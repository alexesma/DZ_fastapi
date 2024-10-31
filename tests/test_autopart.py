import json
from decimal import Decimal

import pytest
import os
from io import BytesIO
from PIL import Image
from pathlib import Path
import tempfile
from httpx import AsyncClient, ASGITransport
import logging

from dz_fastapi.models.autopart import (
    AutoPart,
    change_string,
    preprocess_oem_number,
    Category,
    StorageLocation
)

logger = logging.getLogger('dz_fastapi')

from dz_fastapi.core.db import get_session
from dz_fastapi.main import app
from dz_fastapi.models.brand import Brand
from tests.test_constants import TEST_AUTOPART, TEST_BRAND


@pytest.mark.asyncio
async def test_create_autopart(test_session, created_brand: Brand):
    payload = TEST_AUTOPART
    payload['brand_id'] = created_brand.id
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url='http://test') as ac:
        response = await ac.post('/autoparts/', json=payload)

    assert response.status_code == 201, f"Expected status code 201, got {response.status_code}"
    data = response.json()
    assert data['name'] == 'TEST AUTOPART NAME тест'
    assert data['brand_id'] == payload['brand_id']
    assert data['oem_number'] == preprocess_oem_number(payload['oem_number'])
    assert data['description'] == change_string(payload['description'])
    assert data['width'] == payload['width']
    assert data['height'] == payload['height']
    assert data['length'] == payload['length']
    assert data['weight'] == payload['weight']
    assert data['purchase_price'] == payload['purchase_price']
    assert data['retail_price'] == payload['retail_price']
    assert data['wholesale_price'] == payload['wholesale_price']
    assert data['multiplicity'] == payload['multiplicity']
    assert data['minimum_balance'] == payload['minimum_balance']
    assert data['min_balance_auto'] == payload['min_balance_auto']
    assert data['min_balance_user'] == payload['min_balance_user']
    assert data['comment'] == payload['comment']
    expected_barcode = f"{created_brand.name}{payload['oem_number'].upper()}"
    assert data['barcode'] == expected_barcode

    # Verify that the autopart was correctly inserted into the database
    autopart_id = data['id']
    autopart_in_db = await test_session.get(AutoPart, autopart_id)
    assert autopart_in_db is not None
    # assert autopart_in_db.name == payload['name']
    assert autopart_in_db.brand_id == payload['brand_id']
    assert autopart_in_db.oem_number == payload['oem_number'].upper()
    assert autopart_in_db.width == payload['width']
    assert autopart_in_db.height == payload['height']
    assert autopart_in_db.length == payload['length']
    assert autopart_in_db.weight == payload['weight']
    assert autopart_in_db.purchase_price == Decimal(str(payload['purchase_price']))
    assert autopart_in_db.retail_price == Decimal(str(payload['retail_price']))
    assert autopart_in_db.wholesale_price == Decimal(str(payload['wholesale_price']))
    assert autopart_in_db.multiplicity == payload['multiplicity']
    assert autopart_in_db.minimum_balance == payload['minimum_balance']
    assert autopart_in_db.min_balance_auto == payload['min_balance_auto']
    assert autopart_in_db.min_balance_user == payload['min_balance_user']
    assert autopart_in_db.comment == payload['comment']
    assert autopart_in_db.barcode == expected_barcode


@pytest.mark.asyncio
async def test_get_autopart(test_session, created_brand: Brand, created_autopart: AutoPart):
    transport = ASGITransport(app=app)
    autopart_id = created_autopart.id
    async with AsyncClient(transport=transport, base_url='http://test') as ac:
        response = await ac.get(f'/autoparts/{autopart_id}/')
    assert response.status_code == 200, response.text
    data = response.json()
    assert data['id'] == autopart_id
    assert data['name'] == created_autopart.name
    assert data['brand_id'] == created_brand.id
    assert data['oem_number'] == created_autopart.oem_number
    assert data['description'] == created_autopart.description
    assert data['width'] == created_autopart.width
    assert data['height'] == created_autopart.height
    assert data['length'] == created_autopart.length
    assert data['weight'] == created_autopart.weight
    assert data['purchase_price'] == created_autopart.purchase_price
    assert data['retail_price'] == created_autopart.retail_price
    assert data['wholesale_price'] == created_autopart.wholesale_price
    assert data['multiplicity'] == created_autopart.multiplicity
    assert data['minimum_balance'] == created_autopart.minimum_balance
    assert data['min_balance_auto'] == created_autopart.min_balance_auto
    assert data['min_balance_user'] == created_autopart.min_balance_user
    assert data['comment'] == created_autopart.comment

    # Test retrieving a non-existent autopart (should return 404)
    invalid_id = autopart_id + 9999  # Assuming this ID does not exist
    async with AsyncClient(transport=transport, base_url='http://test') as ac:
        response = await ac.get(f'/autoparts/{invalid_id}/')
    assert response.status_code == 404, response.text
    data = response.json()
    assert data['detail'] == 'Autopart not found'


@pytest.mark.asyncio
async def test_get_all_autoparts(test_session, created_brand: Brand):
    transport = ASGITransport(app=app)
    autoparts_data = []
    for i in range(5):
        autopart_data = {
            "name": f"Test Autopart {i}",
            "brand_id": created_brand.id,
            "oem_number": f"E4G16361109{i}",
            "description": f"A test autopart {i}",
            "width": 10.0 + i,
            "height": 5.0 + i,
            "length": 15.0 + i,
            "weight": 2.5 + i,
            "purchase_price": 100.00 + i,
            "retail_price": 150.00 + i,
            "wholesale_price": 120.00 + i,
            "multiplicity": 1 + i,
            "minimum_balance": 5 + i,
            "min_balance_auto": i % 2 == 0,
            "min_balance_user": i % 2 != 0,
            "comment": f"Test autopart comment {i}"
        }
        async with AsyncClient(transport=transport, base_url='http://test') as ac:
            response = await ac.post('/autoparts/', json=autopart_data)
        assert response.status_code == 201
        created_autopart = response.json()
        autoparts_data.append(created_autopart)

    async with AsyncClient(transport=transport, base_url='http://test') as ac:
        response = await ac.get('/autoparts/')
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    assert len(data) >= 5
    retrieved_ids = [item['id'] for item in data]
    for autopart in autoparts_data:
        assert autopart['id'] in retrieved_ids

    skip = 2
    limit = 2
    async with AsyncClient(transport=transport, base_url='http://test') as ac:
        response = await ac.get('/autoparts/', params={'skip': skip, 'limit': limit})
    assert response.status_code == 200
    data = response.json()
    assert len(data) == limit
    expected_autoparts = autoparts_data[skip:skip + limit]
    for i, autopart in enumerate(data):
        assert autopart['id'] == expected_autoparts[i]['id']
        assert autopart['name'] == expected_autoparts[i]['name']

@pytest.mark.asyncio
async def test_update_autopart_success(test_session, created_autopart: AutoPart):
    payload = TEST_BRAND
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url='http://test') as ac:
        response = await ac.post('/brand', json=payload)

    assert response.status_code == 201, response.text
    data = response.json()
    autopart_id = created_autopart.id

    update_data = {
        'name': 'Updated Autopart Name',
        'description': 'Updated description',
        'brand_id': data['id'],
    }

    async with AsyncClient(transport=transport, base_url='http://test') as ac:
        response = await ac.patch(f'/autoparts/{autopart_id}/', json=update_data)
    assert response.status_code == 200, response.text
    updated_autopart = response.json()
    assert updated_autopart['id'] == autopart_id
    assert updated_autopart['name'] == update_data['name'].upper()
    assert updated_autopart['description'] == update_data['description'].upper()
    assert updated_autopart['brand_id'] == update_data['brand_id']


@pytest.mark.asyncio
async def test_update_autopart_not_found(test_session, created_brand: Brand):
    transport = ASGITransport(app=app)
    invalid_autopart_id = 9999

    update_data = {
        'name': 'Non-existent Autopart',
        'description': 'This autopart does not exist',
        'brand_id': created_brand.id
    }

    async with AsyncClient(transport=transport, base_url='http://test') as ac:
        response = await ac.patch(f'/autoparts/{invalid_autopart_id}/', json=update_data)
    assert response.status_code == 404, response.text
    error_response = response.json()
    assert error_response['detail'] == 'AutoPart not found'


@pytest.mark.asyncio
async def test_update_autopart_invalid_brand(test_session, created_autopart: Brand):
    transport = ASGITransport(app=app)
    autopart_id = created_autopart.id
    invalid_brand_id = 9999

    update_data = {
        'name': 'Autopart with Invalid Brand',
        'brand_id': invalid_brand_id,
    }

    async with AsyncClient(transport=transport, base_url='http://test') as ac:
        response = await ac.patch(f'/autoparts/{autopart_id}/', json=update_data)
    assert response.status_code == 400 or response.status_code == 404, response.text
    error_response = response.json()
    assert 'Brand not found' in error_response['detail']


@pytest.mark.asyncio
async def test_create_category_success(test_session):
    transport = ASGITransport(app=app)
    category_data = {'name': 'Unique Category Name'}

    async with AsyncClient(transport=transport, base_url='http://test') as ac:
        response = await ac.post('/categories/', json=category_data)

    assert response.status_code == 201, response.text
    created_category = response.json()
    assert created_category['name'] == category_data['name']
    assert 'id' in created_category


@pytest.mark.asyncio
async def test_create_category_duplicate_name(
        test_session,
        created_category: Category
):
    transport = ASGITransport(app=app)
    category_data = {'name': created_category.name}

    async with AsyncClient(transport=transport, base_url='http://test') as ac:
        response = await ac.post('/categories/', json=category_data)

    assert response.status_code == 400, response.text
    error_response = response.json()
    assert 'detail' in error_response
    assert f"Category with name '{category_data['name']}' already exists." in error_response['detail']


@pytest.mark.asyncio
async def test_create_category_invalid_data(test_session):
    transport = ASGITransport(app=app)
    category_data = {}

    async with AsyncClient(transport=transport, base_url='http://test') as ac:
        response = await ac.post('/categories/', json=category_data)

    assert response.status_code == 422, response.text
    error_response = response.json()
    assert 'detail' in error_response
    assert any(
        error['loc'] == ['body', 'name'] and error['msg'] == 'Field required'
        for error in error_response['detail']
    )


@pytest.mark.asyncio
async def test_get_categories_with_data(test_session, created_category: Category):
    transport = ASGITransport(app=app)
    category_data = {'name': 'Test Category 2'}
    async with AsyncClient(transport=transport, base_url='http://test') as ac:
        await ac.post('/categories/', json=category_data)

    async with AsyncClient(transport=transport, base_url='http://test') as ac:
        response = await ac.get('/categories/')

    assert response.status_code == 200, response.text
    categories = response.json()
    assert isinstance(categories, list)
    assert len(categories) == 2

    created_names = {category['name'] for category in [
        {'name': created_category.name}, category_data
    ]}
    response_names = {category['name'] for category in categories}
    assert created_names == response_names


@pytest.mark.asyncio
async def test_get_categories_with_pagination(
        test_session,
        created_category: Category
):
    limit = 2
    skip = 1
    transport = ASGITransport(app=app)
    categories_data = [
        {'name': 'Category 1'},
        {'name': 'Category 2'},
        {'name': 'Category 3'},
    ]
    async with AsyncClient(transport=transport, base_url='http://test') as ac:
        for category_data in categories_data:
            response = await ac.post('/categories/', json=category_data)
            assert response.status_code == 201, response.text
        response = await ac.get(f'/categories/?skip={skip}&limit={limit}')

    assert response.status_code == 200, response.text
    categories = response.json()
    assert isinstance(categories, list)
    assert len(categories) == limit

    expected_categories = categories_data[skip:skip + limit]
    for expected, actual in zip(expected_categories, categories[1:]):
        assert expected['name'] == actual['name']


@pytest.mark.asyncio
async def test_get_categories_invalid_pagination(test_session):
    transport = ASGITransport(app=app)
    categories_data = [
        {'name': 'Category 1'},
        {'name': 'Category 2'},
        {'name': 'Category 3'},
    ]
    invalid_params = [
        {'skip': -1, 'limit': 10},
        {'skip': 0, 'limit': -5},
        {'skip': 'abc', 'limit': 10},
        {'skip': 0, 'limit': 'xyz'},
    ]
    async with AsyncClient(
            transport=transport,
            base_url='http://test'
    ) as ac:
        for category_data in categories_data:
            response = await ac.post(
                '/categories/',
                json=category_data
            )
            assert response.status_code == 201, response.text
        for params in invalid_params:
            skip = params['skip']
            limit = params['limit']
            response = await ac.get(f'/categories/?skip={skip}&limit={limit}')
            assert response.status_code == 422, f"Failed with params skip={skip}, limit={limit}"
            error_response = response.json()
            assert 'detail' in error_response


@pytest.mark.asyncio
async def test_update_category_success(
        test_session,
        created_category: Category
):
    transport = ASGITransport(app=app)
    category_id = created_category.id
    update_data = {'name': 'Updated Category Name'}

    async with AsyncClient(
            transport=transport,
            base_url='http://test'
    ) as ac:
        response = await ac.patch(
            f'/categories/{category_id}/',
            json=update_data
        )

    assert response.status_code == 200, response.text
    updated_category = response.json()
    assert updated_category['id'] == category_id
    assert updated_category['name'] == update_data['name']
    assert updated_category['parent_id'] == None
    assert isinstance(updated_category['children'], list)


@pytest.mark.asyncio
async def test_update_category_not_found(test_session):
    transport = ASGITransport(app=app)
    invalid_category_id = 9999
    update_data = {'name': 'Should Not Exist'}

    async with AsyncClient(
            transport=transport,
            base_url='http://test'
    ) as ac:
        response = await ac.patch(
            f'/categories/{invalid_category_id}/',
            json=update_data
        )

    assert response.status_code == 404, response.text
    error_response = response.json()
    assert error_response['detail'] == 'Category not found'


@pytest.mark.asyncio
async def test_update_category_invalid_data(
        test_session,
        created_category: Category
):
    transport = ASGITransport(app=app)
    category_id = created_category.id
    update_data = {'name': ''}

    async with AsyncClient(
            transport=transport,
            base_url='http://test'
    ) as ac:
        response = await ac.patch(
            f'/categories/{category_id}/',
            json=update_data
        )

    assert response.status_code == 422, response.text
    error_response = response.json()
    assert 'detail' in error_response
    assert any(
        error['loc'] == ['body', 'name'] and 'String should have at least 1 character' in error['msg']
        for error in error_response['detail']
    )


@pytest.mark.asyncio
async def test_update_category_duplicate_name(
        test_session,
        created_category: Category
):
    transport = ASGITransport(app=app)
    category_data = {'name': 'Another Category'}
    async with AsyncClient(transport=transport, base_url='http://test') as ac:
        response = await ac.post('/categories/', json=category_data)
    assert response.status_code == 201, response.text
    another_category = response.json()

    category_id = created_category.id
    duplicate_name = another_category['name']
    update_data = {'name': duplicate_name}

    async with AsyncClient(transport=transport, base_url='http://test') as ac:
        response = await ac.patch(f'/categories/{category_id}/', json=update_data)

    assert response.status_code == 400, response.text
    error_response = response.json()
    assert 'detail' in error_response
    assert f"Category with name '{duplicate_name}' already exists." in error_response['detail']


@pytest.mark.asyncio
async def test_create_storage_locations_success(test_session):
    transport = ASGITransport(app=app)
    storage_data = {'name': 'AA 9'}

    async with AsyncClient(transport=transport, base_url='http://test') as ac:
        response = await ac.post('/storage/', json=storage_data)

    assert response.status_code == 201, response.text
    created_storage = response.json()
    assert created_storage['name'] == storage_data['name']


@pytest.mark.asyncio
async def test_create_storage_duplicate_name(
        test_session,
        created_storage: StorageLocation
):
    transport = ASGITransport(app=app)
    storage_data = {'name': created_storage.name}

    async with AsyncClient(transport=transport, base_url='http://test') as ac:
        response = await ac.post('/storage/', json=storage_data)

    assert response.status_code == 400, response.text
    error_response = response.json()
    assert 'detail' in error_response
    assert f'Storage with name {created_storage.name} already exists.' in error_response['detail']


@pytest.mark.asyncio
async def test_create_storage_invalid_data(test_session):
    transport = ASGITransport(app=app)
    storage_data = {}

    async with AsyncClient(transport=transport, base_url='http://test') as ac:
        response = await ac.post('/storage/', json=storage_data)

    assert response.status_code == 422, response.text
    error_response = response.json()
    assert 'detail' in error_response
    assert any(
        error['loc'] == ['body', 'name'] and error['msg'] == 'Field required'
        for error in error_response['detail']
    )


@pytest.mark.asyncio
async def test_get_storage_with_data(
        test_session,
        created_storage: StorageLocation
):
    transport = ASGITransport(app=app)
    storage_data = {'name': 'AA 2'}
    async with AsyncClient(transport=transport, base_url='http://test') as ac:
        await ac.post('/storage/', json=storage_data)

    async with AsyncClient(transport=transport, base_url='http://test') as ac:
        response = await ac.get('/storage/')

    assert response.status_code == 200, response.text
    storages = response.json()
    assert isinstance(storages, list)
    assert len(storages) == 2

    created_names = {storage['name'] for storage in [
        {'name': created_storage.name}, storage_data
    ]}
    response_names = {storages['name'] for storages in storages}
    assert created_names == response_names


@pytest.mark.asyncio
async def test_get_storages_with_pagination(
        test_session,
        created_storage: StorageLocation
):
    limit = 2
    skip = 1
    transport = ASGITransport(app=app)
    storages_data = [
        {'name': 'AA 1'},
        {'name': 'AA 2'},
        {'name': 'AA 3'},
    ]
    async with AsyncClient(transport=transport, base_url='http://test') as ac:
        for storage_data in storages_data:
            response = await ac.post('/storage/', json=storage_data)
            assert response.status_code == 201, response.text
        response = await ac.get(f'/storage/?skip={skip}&limit={limit}')

    assert response.status_code == 200, response.text
    storages = response.json()
    assert isinstance(storages, list)
    assert len(storages) == limit

    expected_storages = storages_data[skip:skip + limit]
    for expected, actual in zip(expected_storages, storages[1:]):
        assert expected['name'] == actual['name']


@pytest.mark.asyncio
async def test_update_storage_success(
        test_session,
        created_storage: StorageLocation
):
    transport = ASGITransport(app=app)
    storage_id = created_storage.id
    update_data = {'name': 'BB 1'}

    async with AsyncClient(
            transport=transport,
            base_url='http://test'
    ) as ac:
        response = await ac.patch(
            f'/storage/{storage_id}/',
            json=update_data
        )

    assert response.status_code == 200, response.text
    updated_storage = response.json()
    assert updated_storage['id'] == storage_id
    assert updated_storage['name'] == update_data['name']


@pytest.mark.asyncio
async def test_update_storage_not_found(test_session):
    transport = ASGITransport(app=app)
    invalid_storage_id = 9999
    update_data = {'name': 'BB 2'}

    async with AsyncClient(
            transport=transport,
            base_url='http://test'
    ) as ac:
        response = await ac.patch(
            f'/storage/{invalid_storage_id}/',
            json=update_data
        )

    assert response.status_code == 404, response.text
    error_response = response.json()
    assert error_response['detail'] == 'Storage location not found'


@pytest.mark.asyncio
async def test_update_storage_invalid_data(
        test_session,
        created_storage: StorageLocation
):
    transport = ASGITransport(app=app)
    storage_id = created_storage.id
    update_data = {'name': ''}

    async with AsyncClient(
            transport=transport,
            base_url='http://test'
    ) as ac:
        response = await ac.patch(
            f'/storage/{storage_id}/',
            json=update_data
        )

    assert response.status_code == 422, response.text
    error_response = response.json()
    assert 'detail' in error_response
    assert any(
        error['loc'] == ['body', 'name'] and 'String should match pattern' in error['msg']
        for error in error_response['detail']
    )


@pytest.mark.asyncio
async def test_update_storage_duplicate_name(
        test_session,
        created_storage: StorageLocation
):
    transport = ASGITransport(app=app)
    storage_data = {'name': 'BB 2'}
    async with AsyncClient(
            transport=transport,
            base_url='http://test'
    ) as ac:
        response = await ac.post('/storage/', json=storage_data)
    assert response.status_code == 201, response.text
    another_storage = response.json()

    storage_id = created_storage.id
    duplicate_name = another_storage['name']
    update_data = {'name': duplicate_name}

    async with AsyncClient(
            transport=transport,
            base_url='http://test'
    ) as ac:
        response = await ac.patch(
            f'/storage/{storage_id}/',
            json=update_data
        )

    assert response.status_code == 400, response.text
    error_response = response.json()
    assert 'detail' in error_response
    assert f"Storage with name '{duplicate_name}' already exists." in error_response['detail']
