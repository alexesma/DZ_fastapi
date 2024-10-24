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

from dz_fastapi.models.autopart import AutoPart, change_string, preprocess_oem_number

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
