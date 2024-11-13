import pytest
from httpx import AsyncClient, ASGITransport
import logging

from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger('dz_fastapi')

from dz_fastapi.main import app
from dz_fastapi.models.partner import Provider, Customer
from dz_fastapi.schemas.partner import ProviderResponse, CustomerResponse
from tests.test_constants import TEST_PROVIDER, TEST_CUSTOMER


@pytest.mark.asyncio
async def test_create_provider(test_session: AsyncSession):

    payload = TEST_PROVIDER
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url='http://test') as ac:
        response = await ac.post('/providers/', json=payload)

    assert response.status_code == 201, response.text
    data = response.json()
    assert data['name'] == 'TEST-PROVIDER'
    assert data['description'] == 'A test provider'
    assert data['email_contact'] == 'test2@test2.ru'
    assert data['comment'] == 'Test comment'
    assert data['email_incoming_price'] == 'test3@test2.ru'
    assert data['type_prices'] == 'Retail'
    assert 'id' in data


@pytest.mark.asyncio
async def test_get_providers(
        test_session: AsyncSession,
        created_providers: list[Provider]
):

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url='http://test') as ac:
        response = await ac.get('/providers/')

    data = response.json()
    assert isinstance(data, list)
    assert len(data) >= len(created_providers)

    response_providers = [ProviderResponse.model_validate(item) for item in data]

    for created_provider in created_providers:
        provider_in_response = next(
            (p for p in response_providers if p.id == created_provider.id),
            None
        )
        assert provider_in_response is not None, f'Provider with ID {created_provider.id} not found in response'

        assert provider_in_response.name == created_provider.name
        assert provider_in_response.description == created_provider.description
        assert provider_in_response.email_contact == created_provider.email_contact
        assert provider_in_response.comment == created_provider.comment
        assert provider_in_response.email_incoming_price == created_provider.email_incoming_price
        assert provider_in_response.type_prices == created_provider.type_prices


@pytest.mark.asyncio
async def test_get_provider_success(
        test_session: AsyncSession,
        created_providers: list[Provider]
):
    created_provider = created_providers[0]

    transport = ASGITransport(app=app)

    async with AsyncClient(transport=transport, base_url='http://test') as ac:
        response = await ac.get(f'/providers/{created_provider.id}/')

    assert response.status_code == 200, response.text
    data = response.json()

    provider_response = ProviderResponse.model_validate(data)

    assert provider_response.id == created_provider.id
    assert provider_response.name == created_provider.name
    assert provider_response.email_contact == created_provider.email_contact
    assert provider_response.email_incoming_price == created_provider.email_incoming_price
    assert provider_response.description == created_provider.description
    assert provider_response.comment == created_provider.comment
    assert provider_response.type_prices == created_provider.type_prices


@pytest.mark.asyncio
async def test_get_provider_not_found(test_session):
    transport = ASGITransport(app=app)
    invalid_provider_id = 99999

    async with AsyncClient(transport=transport, base_url='http://test') as ac:
        response = await ac.get(f'/providers/{invalid_provider_id}/')

    assert response.status_code == 404, response.text
    data = response.json()
    assert data['detail'] == 'Provider not found'


@pytest.mark.asyncio
async def test_delete_provider_success(
        test_session: AsyncSession,
        created_providers: list[Provider]
):
    transport = ASGITransport(app=app)
    provider_to_delete = created_providers[0]

    async with AsyncClient(transport=transport, base_url='http://test') as ac:
        response = await ac.delete(f'/providers/{provider_to_delete.id}/')

    assert response.status_code == 200, response.text
    data = response.json()
    deleted_provider = ProviderResponse.model_validate(data)
    assert deleted_provider.id == provider_to_delete.id
    assert deleted_provider.name == provider_to_delete.name

    async with AsyncClient(transport=transport, base_url='http://test') as ac:
            response = await ac.get(f'/providers/{provider_to_delete.id}/')
    assert response.status_code == 404, response.text
    data = response.json()
    assert data['detail'] == 'Provider not found'


@pytest.mark.asyncio
async def test_delete_provider_not_found(test_session: AsyncSession):

    transport = ASGITransport(app=app)
    invalid_provider_id = 99999

    async with AsyncClient(transport=transport, base_url='http://test') as ac:
        response = await ac.delete(f'/providers/{invalid_provider_id}/')

    assert response.status_code == 404, response.text
    data = response.json()
    assert data['detail'] == "Provider not found"


@pytest.mark.asyncio
async def test_update_provider_success(
        test_session: AsyncSession,
        created_providers: list[Provider]
):
    provider_to_update = created_providers[0]
    update_data = {
        'name': 'Updated Provider Name',
        'email_contact': 'updated_email@test.com',
        'description': 'Updated description'
    }

    transport = ASGITransport(app=app)

    async with AsyncClient(transport=transport, base_url='http://test') as ac:
        response = await ac.patch(
            f'/providers/{provider_to_update.id}/',
            json=update_data
        )

    assert response.status_code == 200, response.text
    data = response.json()
    updated_provider = ProviderResponse.model_validate(data)

    assert updated_provider.id == provider_to_update.id
    assert updated_provider.name == update_data['name']
    assert updated_provider.email_contact == update_data['email_contact']
    assert updated_provider.description == update_data['description']


@pytest.mark.asyncio
async def test_update_provider_no_data(
        test_session: AsyncSession,
        created_providers: list[Provider]
):
    transport = ASGITransport(app=app)
    provider_to_update = created_providers[0]
    update_data = {}

    async with AsyncClient(transport=transport, base_url='http://test') as ac:
        response = await ac.patch(
            f'/providers/{provider_to_update.id}/',
            json=update_data
        )

    assert response.status_code == 422, response.text
    data = response.json()
    assert isinstance(data, dict), f'Expected response to be a dict, got {type(data)}'
    assert data['detail'][0]['msg'] == 'Field required'


@pytest.mark.asyncio
async def test_update_provider_not_found(test_session: AsyncSession):
    transport = ASGITransport(app=app)
    invalid_provider_id = 99999
    update_data = {
        'name': 'Updated Provider Name'
    }

    async with AsyncClient(transport=transport, base_url='http://test') as ac:
        response = await ac.patch(f'/providers/{invalid_provider_id}/', json=update_data)

    assert response.status_code == 404, response.text
    data = response.json()
    assert data['detail'] == "Provider not found"


@pytest.mark.asyncio
async def test_create_customer(test_session: AsyncSession):

    payload = TEST_CUSTOMER
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url='http://test') as ac:
        response = await ac.post('/customers/', json=payload)

    assert response.status_code == 201, response.text
    data = response.json()
    assert data['name'] == 'TEST-CUSTOMER'
    assert data['description'] == 'A test customer'
    assert data['email_contact'] == 'testcustomer@customer.ru'
    assert data['comment'] == 'Test comment'
    assert data['email_outgoing_price'] == 'testcustomer@customer.ru'
    assert data['type_prices'] == 'Retail'
    assert 'id' in data


@pytest.mark.asyncio
async def test_get_customers(
        test_session: AsyncSession,
        created_customers: list[Customer]
):

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url='http://test') as ac:
        response = await ac.get('/customers/')

    data = response.json()
    assert isinstance(data, list)
    assert len(data) >= len(created_customers)

    response_providers = [CustomerResponse.model_validate(item) for item in data]

    for created_customer in created_customers:
        customer_in_response = next(
            (p for p in response_providers if p.id == created_customer.id),
            None
        )
        assert customer_in_response is not None, f'Customer with ID {created_customer.id} not found in response'

        assert customer_in_response.name == created_customer.name
        assert customer_in_response.description == created_customer.description
        assert customer_in_response.email_contact == created_customer.email_contact
        assert customer_in_response.comment == created_customer.comment
        assert customer_in_response.email_outgoing_price == created_customer.email_outgoing_price
        assert customer_in_response.type_prices == created_customer.type_prices


@pytest.mark.asyncio
async def test_get_customer_success(
        test_session: AsyncSession,
        created_customers: list[Customer]
):
    created_customer = created_customers[0]

    transport = ASGITransport(app=app)

    async with AsyncClient(transport=transport, base_url='http://test') as ac:
        response = await ac.get(f'/customers/{created_customer.id}/')

    assert response.status_code == 200, response.text
    data = response.json()

    customer_response = CustomerResponse.model_validate(data)

    assert customer_response.id == created_customer.id
    assert customer_response.name == created_customer.name
    assert customer_response.email_contact == created_customer.email_contact
    assert customer_response.email_outgoing_price == created_customer.email_outgoing_price
    assert customer_response.description == created_customer.description
    assert customer_response.comment == created_customer.comment
    assert customer_response.type_prices == created_customer.type_prices


@pytest.mark.asyncio
async def test_get_customer_not_found(test_session):
    transport = ASGITransport(app=app)
    invalid_customer_id = 99999

    async with AsyncClient(transport=transport, base_url='http://test') as ac:
        response = await ac.get(f'/customers/{invalid_customer_id}/')

    assert response.status_code == 404, response.text
    data = response.json()
    assert data['detail'] == 'Customer not found'


@pytest.mark.asyncio
async def test_delete_customer_success(
        test_session: AsyncSession,
        created_customers: list[Customer]
):
    transport = ASGITransport(app=app)
    customer_to_delete = created_customers[0]

    async with AsyncClient(transport=transport, base_url='http://test') as ac:
        response = await ac.delete(f'/customers/{customer_to_delete.id}/')

    assert response.status_code == 200, response.text
    data = response.json()
    deleted_customer = CustomerResponse.model_validate(data)
    assert deleted_customer.id == customer_to_delete.id
    assert deleted_customer.name == customer_to_delete.name

    async with AsyncClient(transport=transport, base_url='http://test') as ac:
            response = await ac.get(f'/customers/{customer_to_delete.id}/')
    assert response.status_code == 404, response.text
    data = response.json()
    assert data['detail'] == 'Customer not found'


@pytest.mark.asyncio
async def test_delete_customer_not_found(test_session: AsyncSession):

    transport = ASGITransport(app=app)
    invalid_customer_id = 99999

    async with AsyncClient(transport=transport, base_url='http://test') as ac:
        response = await ac.delete(f'/customers/{invalid_customer_id}/')

    assert response.status_code == 404, response.text
    data = response.json()
    assert data['detail'] == "Customer not found"


@pytest.mark.asyncio
async def test_update_customer_success(
        test_session: AsyncSession,
        created_customers: list[Customer]
):
    customer_to_update = created_customers[0]
    update_data = {
        'name': 'Updated Customer Name',
        'email_contact': 'updated_email@test.com',
        'description': 'Updated description'
    }

    transport = ASGITransport(app=app)

    async with AsyncClient(transport=transport, base_url='http://test') as ac:
        response = await ac.patch(
            f'/customers/{customer_to_update.id}/',
            json=update_data
        )

    assert response.status_code == 200, response.text
    data = response.json()
    updated_customer = CustomerResponse.model_validate(data)

    assert updated_customer.id == customer_to_update.id
    assert updated_customer.name == update_data['name']
    assert updated_customer.email_contact == update_data['email_contact']
    assert updated_customer.description == update_data['description']


@pytest.mark.asyncio
async def test_update_customer_no_data(
        test_session: AsyncSession,
        created_customers: list[Customer]
):
    transport = ASGITransport(app=app)
    customer_to_update = created_customers[0]
    update_data = {}

    async with AsyncClient(transport=transport, base_url='http://test') as ac:
        response = await ac.patch(
            f'/customers/{customer_to_update.id}/',
            json=update_data
        )

    assert response.status_code == 422, response.text
    data = response.json()
    assert isinstance(data, dict), f'Expected response to be a dict, got {type(data)}'
    assert data['detail'][0]['msg'] == 'Field required'


@pytest.mark.asyncio
async def test_update_customer_not_found(test_session: AsyncSession):
    transport = ASGITransport(app=app)
    invalid_customer_id = 99999
    update_data = {
        'name': 'Updated Provider Name'
    }

    async with AsyncClient(transport=transport, base_url='http://test') as ac:
        response = await ac.patch(
            f'/customers/{invalid_customer_id}/',
            json=update_data
        )

    assert response.status_code == 404, response.text
    data = response.json()
    assert data['detail'] == "Customer not found"
