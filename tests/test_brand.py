import pytest
import os
from io import BytesIO
from PIL import Image
from pathlib import Path
import tempfile
from httpx import AsyncClient, ASGITransport
import logging

logger = logging.getLogger('dz_fastapi')

from dz_fastapi.core.db import get_session
from dz_fastapi.main import app
from dz_fastapi.models.brand import Brand
from tests.test_constants import TEST_BRAND


@pytest.mark.asyncio
async def test_create_brand(test_session):

    payload = TEST_BRAND
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url='http://test') as ac:
        response = await ac.post('/brand', json=payload)

    assert response.status_code == 201, response.text
    data = response.json()
    assert data['name'] == 'TEST-BRAND'
    assert data['description'] == 'A test brand'
    assert data['website'] == 'https://example.com'
    assert data['main_brand'] is False
    assert data['country_of_origin'] == 'USA'
    assert 'id' in data


@pytest.mark.asyncio
async def test_upload_logo(test_session, created_brand: Brand):

    with tempfile.TemporaryDirectory() as temp_upload_dir:
        os.environ['UPLOAD_DIR'] = temp_upload_dir
        brand_id = created_brand.id
        # Создаём простое изображение
        image = Image.new("RGB", (100, 100), color="red")
        img_byte_arr = BytesIO()
        image.save(img_byte_arr, format='PNG')
        img_byte_arr.seek(0)

        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url='http://test') as ac:
            files = {'file': ('test_logo.png', img_byte_arr, 'image/png')}
            response = await ac.patch(f'/brand/{brand_id}/upload-logo', files=files)

        assert response.status_code == 200, response.text
        data = response.json()
        assert data['id'] == brand_id,  'Brand ID should match the uploaded one'
        assert data['name'] == created_brand.name, f'Expected"TEST BRAND", but got "{data["name"]}"'
        assert data['description'] == 'A test brand', 'Brand description should match the original'
        assert data['website'] == 'https://example.com', 'Brand website should match the original'
        assert data['main_brand'] is False, 'Brand should not be main'
        assert data['country_of_origin'] == 'USA', 'Brand country of origin should match the original'
        assert f'brand_{brand_id}_logo.png' in data['logo'], 'Logo URL should contain the brand ID and the original filename'
        logo_path = Path(data['logo'])
        assert logo_path.exists(), "Logo file does not exist on disk"

    # Clean up
    app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_upload_logo_invalid_file_type(test_session, created_brand: Brand):

    brand_id = created_brand.id
    # Подготавливаем не-изображение
    file_content = b"This is not an image."
    file_byte_arr = BytesIO(file_content)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url='http://test') as ac:
        files = {'file': ('test.txt', file_byte_arr, 'text/plain')}
        response = await ac.patch(f'/brand/{brand_id}/upload-logo', files=files)

    assert response.status_code == 400
    assert response.json()['detail'] == 'Invalid file type. Only JPEG and PNG are allowed.'


@pytest.mark.asyncio
async def test_upload_logo_file_size_exceeds(test_session, created_brand: Brand):
    """
    Тест проверяет, что загрузка файла, превышающего максимальный размер,
    возвращает ошибку 400.
    """
    brand_id = created_brand.id

    # Создаём изображение больше 1 МБ
    image = Image.new("RGB", (5000, 5000), color="blue")  # Большое изображение
    img_byte_arr = BytesIO()
    image.save(img_byte_arr, format='PNG', optimize=False)  # Отключаем оптимизацию для увеличения размера
    img_byte_arr.seek(0)

    # Проверяем реальный размер файла
    file_size = len(img_byte_arr.getvalue())
    assert file_size > 1 * 50 * 1024, f"File size is {file_size} bytes, which is not greater than 1 MB."

    # Импортируем приложение после настройки зависимостей
    # from dz_fastapi.main import app
    transport = ASGITransport(app=app)

    async with AsyncClient(transport=transport, base_url='http://test') as ac:
        files = {'file': ('large_logo.png', img_byte_arr, 'image/png')}
        response = await ac.patch(f'/brand/{brand_id}/upload-logo', files=files)

    # Проверки
    assert response.status_code == 400, f"Expected status code 400, but got {response.status_code}. Response: {response.text}"
    assert response.json()['detail'] == 'File size exceeds the maximum allowed size.'


@pytest.mark.asyncio
async def test_upload_logo_invalid_image(test_session, created_brand:Brand):
        brand_id = created_brand.id
        from dz_fastapi.main import app

        # Подготавливаем повреждённое изображение
        corrupted_image = BytesIO(b"this is not a valid image content")

        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url='http://test') as ac:
            files = {'file': ('corrupted_logo.png', corrupted_image, 'image/png')}
            response = await ac.patch(f'/brand/{brand_id}/upload-logo', files=files)

        assert response.status_code == 400
        assert 'Invalid image file' in response.json()['detail']


@pytest.mark.asyncio
async def test_get_brands(test_session, created_brand: Brand):

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url='http://test') as ac:
        response = await ac.get('/brand')

    assert response.status_code == 200, response.text
    data = response.json()[0]
    assert data['name'] == 'TEST BRAND'
    assert data['description'] == 'A test brand'
    assert data['website'] == 'https://example.com'
    assert data['main_brand'] is False
    assert data['country_of_origin'] == 'USA'
    assert isinstance(data['synonyms'], list)
    for synonym in data['synonyms']:
        assert 'id' in synonym
        assert 'name' in synonym
        assert isinstance(synonym['id'], int)
        assert isinstance(synonym['name'], str)
    assert 'id' in data


async def test_get_brand(test_session, created_brand: Brand):
    brand_id = created_brand.id

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url='http://test') as ac:
        response = await ac.get(f'/brand/{brand_id}')

    assert response.status_code == 200, response.text
    data = response.json()
    assert data['name'] == 'TEST BRAND'
    assert data['description'] == 'A test brand'
    assert data['website'] == 'https://example.com'
    assert data['main_brand'] is False
    assert data['country_of_origin'] == 'USA'
    assert isinstance(data['synonyms'], list)
    for synonym in data['synonyms']:
        assert 'id' in synonym
        assert 'name' in synonym
        assert isinstance(synonym['id'], int)
        assert isinstance(synonym['name'], str)
    assert 'id' in data


@pytest.mark.asyncio
async def test_update_brand(test_session, created_brand: Brand):
    brand_id = created_brand.id
    new_data = {
        'name': 'new brand',
        'country_of_origin': 'Germany',
        'description': 'A test brand new ',
        'website': 'https://test.com',
        'main_brand': True
    }

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url='http://test') as ac:
        response = await ac.patch(f'/brand/{brand_id}', json=new_data)

    assert response.status_code == 200, response.text
    data = response.json()
    assert data['name'] == 'NEW BRAND'
    assert data['description'] == new_data['description']
    assert data['website'] == new_data['website']
    assert data['main_brand'] is True
    assert data['country_of_origin'] == new_data['country_of_origin']
    for synonym in data['synonyms']:
        assert 'id' in synonym
        assert 'name' in synonym
        assert isinstance(synonym['id'], int)
        assert isinstance(synonym['name'], str)
    assert 'id' in data


@pytest.mark.asyncio
async def test_delete_brand(test_session, created_brand:Brand):
    brand_id = created_brand.id

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url='http://test') as ac:
        response = await ac.delete(f'/brand/{brand_id}')

    assert response.status_code == 200, response.text
    data = response.json()
    assert data['name'] == 'TEST BRAND'
    assert data['description'] == 'A test brand'
    assert data['website'] == 'https://example.com'
    assert data['main_brand'] is False
    assert data['country_of_origin'] == 'USA'
    assert 'id' in data


@pytest.mark.asyncio
async def test_add_synonyms(test_session, created_brand:Brand):
    brand_synonym = {
        'name': 'TEST BRAND 2',
        'country_of_origin': 'China'
    }
    brand_id = created_brand.id

    transport = ASGITransport(app=app)

    async with AsyncClient(transport=transport, base_url='http://test') as ac:
        response_create = await ac.post('/brand', json=brand_synonym)
        assert response_create.status_code == 201, response_create.text
        created_synonym = response_create.json()
        assert created_synonym['name'] == 'TEST BRAND 2'
        assert created_synonym['country_of_origin'] == 'China'
        synonym_id = created_synonym['id']

    synonym_payload = {
        'names': [brand_synonym['name']]
    }

    async with AsyncClient(transport=transport, base_url='http://test') as ac:
        response = await ac.post(f'/brand/{brand_id}/synonyms', json=synonym_payload)

    assert response.status_code == 200, response.text
    data = response.json()
    assert data['name'] == created_brand.name
    assert 'id' in data
    assert any(
        synonym['id'] == synonym_id and synonym['name'] == brand_synonym['name']
        for synonym in data['synonyms']
    )


@pytest.mark.asyncio
async def test_delete_synonyms(test_session, created_brand:Brand):
    brand_synonym = {
        'name': 'TEST BRAND 2',
        'country_of_origin': 'China'
    }
    brand_id = created_brand.id

    transport = ASGITransport(app=app)

    async with AsyncClient(transport=transport, base_url='http://test') as ac:
        response_create = await ac.post('/brand', json=brand_synonym)
        assert response_create.status_code == 201, response_create.text
        created_synonym = response_create.json()
        assert created_synonym['name'] == 'TEST BRAND 2'
        assert created_synonym['country_of_origin'] == 'China'

    synonym_payload = {
        'names': [brand_synonym['name']]
    }

    async with AsyncClient(transport=transport, base_url='http://test') as ac:
        response = await ac.post(f'/brand/{brand_id}/synonyms', json=synonym_payload)
    assert response.status_code == 200

    async with AsyncClient(transport=transport, base_url='http://test') as ac:
        response = await ac.request('DELETE', f'/brand/{brand_id}/synonyms', json=synonym_payload)

    assert response.status_code == 200, response.text
    data = response.json()
    assert data['name'] == created_brand.name
    assert 'id' in data
    assert data.get('synonyms') == [], "Synonyms list should be empty after deletion"
