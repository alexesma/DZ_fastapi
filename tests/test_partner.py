import io
import logging
from datetime import date
from decimal import Decimal

import pandas as pd
import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.crud.partner import crud_customer_pricelist, crud_pricelist
from dz_fastapi.main import app
from dz_fastapi.models.autopart import AutoPart, AutoPartPriceHistory
from dz_fastapi.models.brand import Brand
from dz_fastapi.models.partner import (Customer, CustomerPriceList,
                                       CustomerPriceListAutoPartAssociation,
                                       CustomerPriceListConfig,
                                       CustomerPriceListSource, PriceList,
                                       PriceListAutoPartAssociation, Provider,
                                       ProviderPriceListConfig)
from dz_fastapi.schemas.autopart import AutoPartPricelist
from dz_fastapi.schemas.partner import (CustomerResponse,
                                        PriceListAutoPartAssociationCreate,
                                        PriceListCreate, ProviderResponse)
from tests.test_constants import CONFIG_DATA, TEST_CUSTOMER, TEST_PROVIDER

logger = logging.getLogger('dz_fastapi')


@pytest.mark.asyncio
async def test_create_provider(
    test_session: AsyncSession, async_client: AsyncClient
):

    payload = TEST_PROVIDER

    response = await async_client.post('/providers/', json=payload)

    assert response.status_code == 201, response.text
    data = response.json()
    assert data['name'] == 'TEST-PROVIDER'
    assert data['description'] == 'A test provider'
    assert data['email_contact'] == 'test2@example.com'
    assert data['comment'] == 'Test comment'
    assert data['email_incoming_price'] == 'test3@example.com'
    assert data['type_prices'] == 'Retail'
    assert 'id' in data


@pytest.mark.asyncio
async def test_get_providers(
    test_session: AsyncSession,
    created_providers: list[Provider],
    async_client: AsyncClient,
):
    response = await async_client.get('/providers/')
    assert response.status_code == 200, response.text

    data = response.json()
    assert isinstance(data, dict)
    for key in ('items', 'page', 'page_size', 'total', 'pages'):
        assert key in data
    items = data['items']
    assert isinstance(items, list)
    by_id = {item['id']: item for item in items}

    for cp in created_providers:
        assert cp.id in by_id, f'Provider id {cp.id} not found in response'
        item = by_id[cp.id]

        assert item['name'] == cp.name
        assert item.get('email_contact') == getattr(cp, 'email_contact', None)
        assert item.get('email_incoming_price') == getattr(
            cp, 'email_incoming_price', None
        )

        # Дополнительно — типы вложенных структур
        assert 'pricelist_configs' in item and isinstance(
            item['pricelist_configs'], list
        )
        assert 'price_lists' in item and isinstance(item['price_lists'], list)
        # last_email_uid может быть None или словарём
        if item['last_email_uid'] is not None:
            assert isinstance(item['last_email_uid'], dict)
            assert 'uid' in item['last_email_uid']


@pytest.mark.asyncio
async def test_get_provider_success(
    test_session: AsyncSession,
    created_providers: list[Provider],
    async_client: AsyncClient,
):
    created_provider = created_providers[0]

    response = await async_client.get(f'/providers/{created_provider.id}/')

    assert response.status_code == 200, response.text
    data = response.json()

    provider_response = ProviderResponse.model_validate(data)

    assert provider_response.id == created_provider.id
    assert provider_response.name == created_provider.name
    assert provider_response.email_contact == created_provider.email_contact
    assert provider_response.email_incoming_price == (
        created_provider.email_incoming_price
    )
    assert provider_response.description == created_provider.description
    assert provider_response.comment == created_provider.comment
    assert provider_response.type_prices == created_provider.type_prices


@pytest.mark.asyncio
async def test_get_provider_not_found(
    test_session: AsyncSession, async_client: AsyncClient
):
    invalid_provider_id = 99999

    response = await async_client.get(f'/providers/{invalid_provider_id}/')

    assert response.status_code == 404, response.text
    data = response.json()

    assert data['detail'] == 'Provider not found'


@pytest.mark.asyncio
async def test_delete_provider_success(
    test_session: AsyncSession,
    async_client: AsyncClient,
    created_providers: list[Provider],
):

    provider_to_delete = created_providers[0]

    response = await async_client.delete(
        f'/providers/{provider_to_delete.id}/'
    )

    assert response.status_code == 200, response.text
    data = response.json()
    deleted_provider = ProviderResponse.model_validate(data)
    assert deleted_provider.id == provider_to_delete.id
    assert deleted_provider.name == provider_to_delete.name

    response = await async_client.get(f'/providers/{provider_to_delete.id}/')

    assert response.status_code == 404, response.text
    data = response.json()
    assert data['detail'] == 'Provider not found'


@pytest.mark.asyncio
async def test_delete_provider_not_found(
    test_session: AsyncSession, async_client: AsyncClient
):
    invalid_provider_id = 99999

    response = await async_client.delete(f'/providers/{invalid_provider_id}/')
    assert response.status_code == 404, response.text
    data = response.json()
    assert data['detail'] == 'Provider not found'


@pytest.mark.asyncio
async def test_update_provider_success(
    test_session: AsyncSession,
    async_client: AsyncClient,
    created_providers: list[Provider],
):
    provider_to_update = created_providers[0]
    update_data = {
        'name': 'Updated Provider Name',
        'email_contact': 'updated_email@example.com',
        'description': 'Updated description',
    }

    response = await async_client.patch(
        f'/providers/{provider_to_update.id}/', json=update_data
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
    async_client: AsyncClient,
    created_providers: list[Provider],
):
    provider_to_update = created_providers[0]
    update_data = {}
    resp_before = await async_client.get(
        f'/providers/{provider_to_update.id}/'
    )
    before = resp_before.json()

    response = await async_client.patch(
        f'/providers/{provider_to_update.id}/', json=update_data
    )
    assert response.status_code == 200
    after = response.json()
    assert after == before


@pytest.mark.asyncio
async def test_update_provider_not_found(
    test_session: AsyncSession, async_client: AsyncClient
):
    invalid_provider_id = 99999
    update_data = {'name': 'Updated Provider Name'}

    response = await async_client.patch(
        f'/providers/{invalid_provider_id}/', json=update_data
    )

    assert response.status_code == 404, response.text
    data = response.json()
    assert data['detail'] == 'Provider not found'


@pytest.mark.asyncio
async def test_create_customer(
    test_session: AsyncSession, async_client: AsyncClient
):
    payload = TEST_CUSTOMER

    response = await async_client.post('/customers/', json=payload)
    assert response.status_code == 201, response.text
    data = response.json()

    assert data['name'] == 'TEST-CUSTOMER'
    assert data['description'] == 'A test customer'
    assert data['email_contact'] == 'testcustomer@example.com'
    assert data['comment'] == 'Test comment'
    assert data['email_outgoing_price'] == 'testcustomer@example.com'
    assert data['type_prices'] == 'Retail'
    assert 'id' in data


@pytest.mark.asyncio
async def test_get_customers(
    test_session: AsyncSession,
    async_client: AsyncClient,
    created_customers: list[Customer],
):
    response = await async_client.get('/customers/')

    data = response.json()
    assert isinstance(data, list)
    assert len(data) >= len(created_customers)

    response_providers = [
        CustomerResponse.model_validate(item) for item in data
    ]

    for created_customer in created_customers:
        customer_in_response = next(
            (p for p in response_providers if p.id == created_customer.id),
            None,
        )
        assert (
            customer_in_response is not None
        ), f'Customer with ID {created_customer.id} not found in response'

        assert customer_in_response.name == created_customer.name
        assert customer_in_response.description == (
            created_customer.description
        )
        assert customer_in_response.email_contact == (
            created_customer.email_contact
        )
        assert customer_in_response.comment == created_customer.comment
        assert customer_in_response.email_outgoing_price == (
            created_customer.email_outgoing_price
        )
        assert customer_in_response.type_prices == (
            created_customer.type_prices
        )


@pytest.mark.asyncio
async def test_get_customer_success(
    test_session: AsyncSession,
    async_client: AsyncClient,
    created_customers: list[Customer],
):
    created_customer = created_customers[0]

    response = await async_client.get(f'/customers/{created_customer.id}/')

    assert response.status_code == 200, response.text
    data = response.json()
    customer_response = CustomerResponse.model_validate(data)

    assert customer_response.id == created_customer.id
    assert customer_response.name == created_customer.name
    assert customer_response.email_contact == created_customer.email_contact
    assert customer_response.email_outgoing_price == (
        created_customer.email_outgoing_price
    )
    assert customer_response.description == created_customer.description
    assert customer_response.comment == created_customer.comment
    assert customer_response.type_prices == created_customer.type_prices


@pytest.mark.asyncio
async def test_get_customer_not_found(
    test_session: AsyncSession, async_client: AsyncClient
):
    invalid_customer_id = 99999

    response = await async_client.get(f'/customers/{invalid_customer_id}/')
    assert response.status_code == 404, response.text
    data = response.json()
    assert data['detail'] == 'Customer not found'


@pytest.mark.asyncio
async def test_delete_customer_success(
    test_session: AsyncSession,
    async_client: AsyncClient,
    created_customers: list[Customer],
):
    customer_to_delete = created_customers[0]

    response = await async_client.delete(
        f'/customers/{customer_to_delete.id}/'
    )
    assert response.status_code == 200, response.text
    data = response.json()
    deleted_customer = CustomerResponse.model_validate(data)
    assert deleted_customer.id == customer_to_delete.id
    assert deleted_customer.name == customer_to_delete.name

    response = await async_client.get(f'/customers/{customer_to_delete.id}/')

    assert response.status_code == 404, response.text
    data = response.json()
    assert data['detail'] == 'Customer not found'


@pytest.mark.asyncio
async def test_delete_customer_not_found(
    test_session: AsyncSession, async_client: AsyncClient
):
    invalid_customer_id = 99999

    response = await async_client.delete(f'/customers/{invalid_customer_id}/')
    assert response.status_code == 404, response.text
    data = response.json()
    assert data['detail'] == 'Customer not found'


@pytest.mark.asyncio
async def test_update_customer_success(
    test_session: AsyncSession,
    async_client: AsyncClient,
    created_customers: list[Customer],
):
    customer_to_update = created_customers[0]
    update_data = {
        'name': 'Updated Customer Name',
        'email_contact': 'updated_email@example.com',
        'description': 'Updated description',
    }

    response = await async_client.patch(
        f'/customers/{customer_to_update.id}/', json=update_data
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
    async_client: AsyncClient,
    created_customers: list[Customer],
):
    customer_to_update = created_customers[0]
    update_data = {}

    response = await async_client.patch(
        f'/customers/{customer_to_update.id}/', json=update_data
    )
    assert response.status_code == 422, response.text
    data = response.json()
    assert isinstance(
        data, dict
    ), f'Expected response to be a dict, got {type(data)}'
    assert data['detail'][0]['msg'] == 'Field required'


@pytest.mark.asyncio
async def test_update_customer_not_found(
    test_session: AsyncSession,
    async_client: AsyncClient,
):
    invalid_customer_id = 99999
    update_data = {'name': 'Updated Provider Name'}

    response = await async_client.patch(
        f'/customers/{invalid_customer_id}/', json=update_data
    )
    assert response.status_code == 404, response.text
    data = response.json()
    assert data['detail'] == 'Customer not found'


@pytest.mark.asyncio
async def test_set_provider_pricelist_config_create(
    created_providers: list[Provider],
    async_client: AsyncClient,
    test_session: AsyncSession,
):
    provider = created_providers[0]

    response = await async_client.post(
        f'/providers/{provider.id}/pricelist-config/', json=CONFIG_DATA
    )
    assert response.status_code == 201
    data = response.json()
    assert data['start_row'] == CONFIG_DATA['start_row']
    assert data['oem_col'] == CONFIG_DATA['oem_col']
    assert data['qty_col'] == CONFIG_DATA['qty_col']
    assert data['price_col'] == CONFIG_DATA['price_col']


@pytest.mark.asyncio
async def test_set_provider_pricelist_config_provider_not_found(
    test_session: AsyncSession, async_client: AsyncClient
):
    invalid_customer_id = 99999

    response = await async_client.post(
        f'/providers/{invalid_customer_id}/pricelist-config/', json=CONFIG_DATA
    )
    assert response.status_code == 404
    data = response.json()
    assert data['detail'] == 'Provider not found'


@pytest.mark.asyncio
async def test_create_pricelist_config(
    created_providers: list[Provider],
    async_client: AsyncClient,
    test_session: AsyncSession,
):
    provider_id = created_providers[0].id
    config_in_data = {
        'start_row': 1,
        'oem_col': 0,
        'name_col': 2,
        'brand_col': 3,
        'qty_col': 4,
        'price_col': 5,
        'name_price': 'PRICE_CONFIG',
        'name_mail': 'MAIL_CONFIG',
    }
    response = await async_client.post(
        f'/providers/{provider_id}/pricelist-config/', json=config_in_data
    )
    assert response.status_code == 201
    data = response.json()
    assert data['id'] is not None
    assert data['start_row'] == 1
    assert data['name_price'] == 'PRICE_CONFIG'


@pytest.mark.asyncio
async def test_create_provider_pricelist_success(
    created_providers: list[Provider],
    created_pricelist_config: ProviderPriceListConfig,
    created_brand: Brand,
    async_client: AsyncClient,
    test_session: AsyncSession,
):
    provider = created_providers[0]
    provider_list_conf_id = created_pricelist_config.id
    # Создаем фиктивный файл. Здесь используем CSV или другой формат,
    # соответствующий ожидаемому в вашем endpoint-е.
    file_content = (
        'OEM,Brand,Name,Quantity,Price\nSE3841,Test Brand,'
        'Наконечник рулевой тяги,2,1200.00'
    ).encode('utf-8')
    file = io.BytesIO(file_content)
    file.name = 'test.csv'
    # Поскольку endpoint использует параметр file: UploadFile = File(...)
    # и другие параметры через Form, формируем multipart/form-data запрос:
    files = {'file': ('test.csv', file, 'text/csv')}
    response = await async_client.post(
        f'/providers/{provider.id}/pricelists/{provider_list_conf_id}/upload/',
        files=files,
    )

    assert response.status_code == 201
    data = response.json()
    assert data['provider']['id'] == provider.id
    assert len(data['autoparts']) == 1
    assert data['autoparts'][0]['quantity'] == 2


@pytest.mark.asyncio
async def test_create_provider_pricelist_validation_error(
    created_providers: list[Provider],
    created_pricelist_config: ProviderPriceListConfig,
    async_client: AsyncClient,
    test_session: AsyncSession,
):
    provider = created_providers[0]
    provider_list_conf_id = created_pricelist_config.id
    file_content_not_valid = 'Quanlity,Price\n2,1200.00'.encode('utf-8')
    file = io.BytesIO(file_content_not_valid)
    file.name = 'test_not_valid.csv'
    files = {'file': ('test_not_valid.csv', file, 'text/csv')}

    response = await async_client.post(
        f'/providers/{provider.id}/pricelists/{provider_list_conf_id}/upload/',
        files=files,
    )

    assert response.status_code == 422, response.text


@pytest.mark.asyncio
async def test_upload_provider_pricelist_no_config(
    created_providers: list[Provider],
    created_pricelist_config: ProviderPriceListConfig,
    created_brand: Brand,
    async_client: AsyncClient,
    test_session: AsyncSession,
):
    provider = created_providers[0]
    provider_list_conf_id = 999
    df = pd.DataFrame(
        {
            0: ['SE3841'],
            1: [f'{created_brand.name}'],
            2: ['Наконечник рулевой тяги'],
            3: [2],
            4: [1200.00],
        }
    )
    csv_bytes = df.to_csv(header=False, index=False).encode('utf-8')
    file = io.BytesIO(csv_bytes)
    file.name = 'test.csv'

    response = await async_client.post(
        f'/providers/{provider.id}/pricelists/{provider_list_conf_id}/upload/',
        files={'file': ('test.csv', file, 'text/csv')},
    )
    assert response.status_code == 404, response.text
    data = response.json()

    assert data['detail'] == 'Provider configuration not found'


@pytest.mark.asyncio
async def test_upload_provider_pricelist_invalid_file(
    created_providers: list[Provider],
    created_pricelist_config: ProviderPriceListConfig,
    created_brand: Brand,
    async_client: AsyncClient,
    test_session: AsyncSession,
):
    provider = created_providers[0]
    provider_list_conf_id = created_pricelist_config.id

    file = io.BytesIO(b'Invalid content')
    file.name = 'test.txt'

    response = await async_client.post(
        f'/providers/{provider.id}/pricelists/{provider_list_conf_id}/upload/',
        files={'file': ('test.csv', file, 'text/csv')},
    )
    assert response.status_code == 400, response.text
    data = response.json()

    assert data['detail'] == ("Invalid format file:400: Invalid CSV file.")


@pytest.mark.asyncio
async def test_crud_pricelist_create(
    created_providers: list[Provider],
    created_pricelist_config: ProviderPriceListConfig,
    created_brand: Brand,
    test_session: AsyncSession,
):
    provider = created_providers[0]

    autopart_data = AutoPartPricelist(
        oem_number='SE3841',
        brand=f'{created_brand.name}',
        name='Наконечник рулевой тяги',
    )
    autopart_assoc = PriceListAutoPartAssociationCreate(
        autopart=autopart_data, quantity=2, price=1200.00
    )

    pricelist_in = PriceListCreate(
        provider_id=provider.id,
        provider_config_id=created_pricelist_config.id,
        autoparts=[autopart_assoc],
    )
    pricelist = await crud_pricelist.create(
        obj_in=pricelist_in, session=test_session
    )
    assert pricelist.provider.id == provider.id
    assert len(pricelist.autoparts) == 1
    assert pricelist.autoparts[0].quantity == 2


@pytest.mark.asyncio
async def test_crud_pricelist_create_no_autoparts(
    created_providers: list[Provider],
    created_pricelist_config: ProviderPriceListConfig,
    created_brand: Brand,
    test_session: AsyncSession,
):
    provider = created_providers[0]
    pricelist_in = PriceListCreate(
        provider_id=provider.id,
        provider_config_id=created_pricelist_config.id,
        autoparts=[],
    )

    pricelist = await crud_pricelist.create(
        obj_in=pricelist_in, session=test_session
    )

    assert pricelist.provider.id == provider.id
    assert len(pricelist.autoparts) == 0


async def _count_history(
    session: AsyncSession,
    provider_id: int,
    provider_config_id: int,
    autopart_id: int,
) -> int:
    result = await session.execute(
        select(func.count(AutoPartPriceHistory.id))
        .where(AutoPartPriceHistory.provider_id == provider_id)
        .where(AutoPartPriceHistory.provider_config_id == provider_config_id)
        .where(AutoPartPriceHistory.autopart_id == autopart_id)
    )
    return int(result.scalar_one())


async def _create_pricelist_for_history(
    *,
    provider_id: int,
    provider_config_id: int,
    brand_name: str,
    price: float,
    quantity: int,
    session: AsyncSession,
):
    autopart_data = AutoPartPricelist(
        oem_number='SE3841',
        brand=brand_name,
        name='Наконечник рулевой тяги',
    )
    autopart_assoc = PriceListAutoPartAssociationCreate(
        autopart=autopart_data,
        quantity=quantity,
        price=price,
    )
    pricelist_in = PriceListCreate(
        provider_id=provider_id,
        provider_config_id=provider_config_id,
        autoparts=[autopart_assoc],
    )
    return await crud_pricelist.create(
        obj_in=pricelist_in,
        session=session,
    )


@pytest.mark.asyncio
async def test_price_history_records_on_price_change(
    created_providers: list[Provider],
    created_pricelist_config: ProviderPriceListConfig,
    created_brand: Brand,
    test_session: AsyncSession,
):
    provider = created_providers[0]

    pricelist_1 = await _create_pricelist_for_history(
        provider_id=provider.id,
        provider_config_id=created_pricelist_config.id,
        brand_name=created_brand.name,
        price=100.0,
        quantity=10,
        session=test_session,
    )
    autopart_id = pricelist_1.autoparts[0].autopart.id
    count_1 = await _count_history(
        test_session,
        provider.id,
        created_pricelist_config.id,
        autopart_id,
    )
    assert count_1 == 1

    await _create_pricelist_for_history(
        provider_id=provider.id,
        provider_config_id=created_pricelist_config.id,
        brand_name=created_brand.name,
        price=100.0,
        quantity=10,
        session=test_session,
    )
    count_2 = await _count_history(
        test_session,
        provider.id,
        created_pricelist_config.id,
        autopart_id,
    )
    assert count_2 == count_1

    await _create_pricelist_for_history(
        provider_id=provider.id,
        provider_config_id=created_pricelist_config.id,
        brand_name=created_brand.name,
        price=110.0,
        quantity=10,
        session=test_session,
    )
    count_3 = await _count_history(
        test_session,
        provider.id,
        created_pricelist_config.id,
        autopart_id,
    )
    assert count_3 == count_1 + 1


@pytest.mark.asyncio
async def test_price_history_records_on_quantity_change(
    created_providers: list[Provider],
    created_pricelist_config: ProviderPriceListConfig,
    created_brand: Brand,
    test_session: AsyncSession,
):
    provider = created_providers[0]

    pricelist_1 = await _create_pricelist_for_history(
        provider_id=provider.id,
        provider_config_id=created_pricelist_config.id,
        brand_name=created_brand.name,
        price=100.0,
        quantity=10,
        session=test_session,
    )
    autopart_id = pricelist_1.autoparts[0].autopart.id
    count_1 = await _count_history(
        test_session,
        provider.id,
        created_pricelist_config.id,
        autopart_id,
    )
    assert count_1 == 1

    await _create_pricelist_for_history(
        provider_id=provider.id,
        provider_config_id=created_pricelist_config.id,
        brand_name=created_brand.name,
        price=100.0,
        quantity=10,
        session=test_session,
    )
    count_2 = await _count_history(
        test_session,
        provider.id,
        created_pricelist_config.id,
        autopart_id,
    )
    assert count_2 == count_1

    await _create_pricelist_for_history(
        provider_id=provider.id,
        provider_config_id=created_pricelist_config.id,
        brand_name=created_brand.name,
        price=100.0,
        quantity=8,
        session=test_session,
    )
    count_3 = await _count_history(
        test_session,
        provider.id,
        created_pricelist_config.id,
        autopart_id,
    )
    assert count_3 == count_1 + 1


@pytest.mark.asyncio
async def test_create_customer_pricelist_config(
    created_customers: Customer,
    async_client: AsyncClient,
    test_session: AsyncSession,
):
    customer = created_customers[0]
    config_data = {'name': 'Test Config', 'own_price_list_markup': 10.0}

    response = await async_client.post(
        f'/customers/{customer.id}/pricelist-configs/', json=config_data
    )

    assert response.status_code == 201
    config = response.json()

    assert config['name'] == "Test Config"
    assert config['general_markup'] == 1.0
    assert config['own_price_list_markup'] == 10.0
    assert config['customer_id'] == customer.id


@pytest.mark.asyncio
async def test_update_customer_pricelist_config(
    created_customers: Customer,
    async_client: AsyncClient,
    test_session: AsyncSession,
):
    customer = created_customers[0]
    config_data = {'name': 'Test Config', 'general_markup': 10.0}
    response = await async_client.post(
        f'/customers/{customer.id}/pricelist-configs/', json=config_data
    )

    config = response.json()
    config_id = config['id']
    update_data = {'general_markup': 15.0}

    response = await async_client.patch(
        f'/customers/{customer.id}/pricelist-configs/{config_id}',
        json=update_data,
    )

    assert response.status_code == 200
    updated_config = response.json()
    assert updated_config['general_markup'] == 15.0


@pytest.mark.asyncio
async def test_get_customer_pricelist_configs(
    created_customers: Customer,
    async_client: AsyncClient,
    test_session: AsyncSession,
):
    customer = created_customers[0]
    ASGITransport(app=app)
    for i in range(3):
        config_data = {'name': f'Config {i}', 'general_markup': 10.0 + i}

        response = await async_client.post(
            f'/customers/{customer.id}/pricelist-configs/', json=config_data
        )

        assert response.status_code == 201

    response = await async_client.get(
        f'/customers/{customer.id}/pricelist-configs/'
    )

    assert response.status_code == 200
    configs = response.json()
    assert len(configs) == 3
    for i, config in enumerate(configs):
        assert config['name'] == f'Config {i}'
        assert config['general_markup'] == 10.0 + i


@pytest.mark.asyncio
async def test_create_customer_pricelist(
    test_session: AsyncSession,
    async_client: AsyncClient,
    created_brand: Brand,
    created_autopart: AutoPart,
    created_providers: list[Provider],
    created_customers: list[Customer],
):
    customer = created_customers[0]
    provider = created_providers[0]

    pricelist = PriceList(
        date=date.today(), provider_id=provider.id, is_active=True
    )
    test_session.add(pricelist)
    await test_session.flush()

    pricelist_assoc = PriceListAutoPartAssociation(
        pricelist_id=pricelist.id,
        autopart_id=created_autopart.id,
        quantity=10,
        price=100.0,
    )
    test_session.add(pricelist_assoc)
    await test_session.commit()
    await test_session.refresh(pricelist)

    config = CustomerPriceListConfig(
        customer_id=customer.id,
        name="Test Config",
        general_markup=10.0,  # 10% markup
        own_price_list_markup=5.0,
        third_party_markup=15.0,
        # 20% markup for this provider
        individual_markups={str(provider.id): 20.0},
        brand_filters={'include': [created_brand.id]},
        category_filter=[],
        price_intervals=[],
        position_filters=[],
        supplier_quantity_filters=[],
        additional_filters={},
    )
    test_session.add(config)
    await test_session.commit()
    await test_session.refresh(config)

    request_data = {
        "date": str(date.today()),
        "customer_id": customer.id,
        "config_id": config.id,
        "items": [pricelist.id],
        "excluded_own_positions": [],
        "excluded_supplier_positions": [],
    }

    response = await async_client.post(
        f'/customers/{customer.id}/pricelists/', json=request_data
    )
    assert response.status_code == 201, response.text

    response_data = response.json()
    # 10% markup + # 20% markup for this provider
    expected_price = 100.0 * 1.20 * 1.10

    assert response_data['customer_id'] == customer.id
    assert response_data['date'] == str(date.today())
    assert len(response_data['autoparts']) == 1
    autopart_data = response_data['autoparts'][0]
    assert autopart_data['autopart_id'] == created_autopart.id
    assert autopart_data['quantity'] == 10
    # Allowing for floating point errors
    assert abs(float(autopart_data['price']) - expected_price) < 0.01


@pytest.mark.asyncio
async def test_create_customer_pricelist_no_items(
    test_session: AsyncSession,
    async_client: AsyncClient,
    created_customers: list[Customer],
):
    customer = created_customers[0]

    config = CustomerPriceListConfig(
        customer_id=customer.id,
        name='Test Config',
        general_markup=10.0,
    )
    test_session.add(config)
    await test_session.commit()
    await test_session.refresh(config)

    request_data = {
        'date': str(date.today()),
        'customer_id': customer.id,
        'config_id': config.id,
        'items': [],
        'excluded_own_positions': [],
        'excluded_supplier_positions': [],
    }

    response = await async_client.post(
        f'/customers/{customer.id}/pricelists/', json=request_data
    )

    assert (
        response.status_code == 400
    ), f'Unexpected status code: {response.status_code}'

    response_data = response.json()
    assert response_data['detail'] == (
        'No autoparts to include in the pricelist'
    )


@pytest.mark.asyncio
async def test_create_customer_pricelist_invalid_customer(
    test_session: AsyncSession,
    async_client: AsyncClient,
    created_customers: list[Customer],
):
    invalid_customer_id = 99999
    request_data = {
        'date': str(date.today()),
        'customer_id': invalid_customer_id,
        'config_id': 1,
        'items': [],
        'excluded_own_positions': [],
        'excluded_supplier_positions': [],
    }

    response = await async_client.post(
        f'/customers/{invalid_customer_id}/pricelists/', json=request_data
    )

    assert (
        response.status_code == 404
    ), f'Unexpected status code: {response.status_code}'

    response_data = response.json()
    assert response_data['detail'] == 'Customer not found'


@pytest.mark.asyncio
async def test_customer_pricelist_prioritizes_own_price(
    test_session: AsyncSession,
    async_client: AsyncClient,
    created_customers: list[Customer],
    created_brand: Brand,
):
    customer = created_customers[0]

    provider_own = Provider(
        name='Own Provider',
        email_contact='own@example.com',
        email_incoming_price='own_prices@example.com',
        description='Own provider',
        comment='',
        type_prices='Wholesale',
        is_own_price=True,
    )
    provider_other = Provider(
        name='Other Provider',
        email_contact='other@example.com',
        email_incoming_price='other_prices@example.com',
        description='Other provider',
        comment='',
        type_prices='Wholesale',
        is_own_price=False,
    )
    test_session.add_all([provider_own, provider_other])
    await test_session.commit()
    await test_session.refresh(provider_own)
    await test_session.refresh(provider_other)

    own_cfg = ProviderPriceListConfig(
        provider_id=provider_own.id,
        start_row=1,
        oem_col=0,
        qty_col=1,
        price_col=2,
        name_price='OWN_CFG',
        name_mail='OWN_MAIL',
    )
    other_cfg = ProviderPriceListConfig(
        provider_id=provider_other.id,
        start_row=1,
        oem_col=0,
        qty_col=1,
        price_col=2,
        name_price='OTHER_CFG',
        name_mail='OTHER_MAIL',
    )
    test_session.add_all([own_cfg, other_cfg])
    await test_session.commit()
    await test_session.refresh(own_cfg)
    await test_session.refresh(other_cfg)

    autopart = AutoPartPricelist(
        oem_number='OEM-OWN-001',
        brand=created_brand.name,
        name='Test Part',
    )
    own_assoc = PriceListAutoPartAssociationCreate(
        autopart=autopart,
        quantity=5,
        price=120.0,
    )
    other_assoc = PriceListAutoPartAssociationCreate(
        autopart=autopart,
        quantity=7,
        price=100.0,
    )
    await crud_pricelist.create(
        obj_in=PriceListCreate(
            provider_id=provider_own.id,
            provider_config_id=own_cfg.id,
            autoparts=[own_assoc],
        ),
        session=test_session,
    )
    await crud_pricelist.create(
        obj_in=PriceListCreate(
            provider_id=provider_other.id,
            provider_config_id=other_cfg.id,
            autoparts=[other_assoc],
        ),
        session=test_session,
    )

    config = CustomerPriceListConfig(
        customer_id=customer.id,
        name='OWN PRIORITY CONFIG',
        general_markup=1.0,
        own_price_list_markup=1.0,
        third_party_markup=1.0,
        additional_filters={},
    )
    test_session.add(config)
    await test_session.commit()
    await test_session.refresh(config)

    test_session.add_all([
        CustomerPriceListSource(
            customer_config_id=config.id,
            provider_config_id=own_cfg.id,
            enabled=True,
            markup=1.0,
            brand_filters={},
            position_filters={},
            additional_filters={},
        ),
        CustomerPriceListSource(
            customer_config_id=config.id,
            provider_config_id=other_cfg.id,
            enabled=True,
            markup=1.0,
            brand_filters={},
            position_filters={},
            additional_filters={},
        ),
    ])
    await test_session.commit()

    request_data = {
        'date': str(date.today()),
        'customer_id': customer.id,
        'config_id': config.id,
        'items': [],
        'excluded_own_positions': [],
        'excluded_supplier_positions': [],
    }
    response = await async_client.post(
        f'/customers/{customer.id}/pricelists/', json=request_data
    )

    assert response.status_code == 201, response.text
    payload = response.json()
    assert len(payload['autoparts']) == 1
    assert abs(float(payload['autoparts'][0]['price']) - 120.0) < 0.01


@pytest.mark.asyncio
async def test_customer_pricelist_source_min_price_filter(
    test_session: AsyncSession,
    async_client: AsyncClient,
    created_customers: list[Customer],
    created_brand: Brand,
):
    customer = created_customers[0]

    provider = Provider(
        name='Filter Provider',
        email_contact='filter@example.com',
        email_incoming_price='filter_prices@example.com',
        description='Filter provider',
        comment='',
        type_prices='Wholesale',
    )
    test_session.add(provider)
    await test_session.commit()
    await test_session.refresh(provider)

    cfg = ProviderPriceListConfig(
        provider_id=provider.id,
        start_row=1,
        oem_col=0,
        qty_col=1,
        price_col=2,
        name_price='FILTER_CFG',
        name_mail='FILTER_MAIL',
    )
    test_session.add(cfg)
    await test_session.commit()
    await test_session.refresh(cfg)

    autopart_low = AutoPartPricelist(
        oem_number='OEM-FILTER-LOW',
        brand=created_brand.name,
        name='Low Part',
    )
    autopart_high = AutoPartPricelist(
        oem_number='OEM-FILTER-HIGH',
        brand=created_brand.name,
        name='High Part',
    )
    await crud_pricelist.create(
        obj_in=PriceListCreate(
            provider_id=provider.id,
            provider_config_id=cfg.id,
            autoparts=[
                PriceListAutoPartAssociationCreate(
                    autopart=autopart_low, quantity=5, price=50.0
                ),
                PriceListAutoPartAssociationCreate(
                    autopart=autopart_high, quantity=6, price=150.0
                ),
            ],
        ),
        session=test_session,
    )

    config = CustomerPriceListConfig(
        customer_id=customer.id,
        name='FILTER CONFIG',
        general_markup=1.0,
        own_price_list_markup=1.0,
        third_party_markup=1.0,
        additional_filters={},
    )
    test_session.add(config)
    await test_session.commit()
    await test_session.refresh(config)

    test_session.add(
        CustomerPriceListSource(
            customer_config_id=config.id,
            provider_config_id=cfg.id,
            enabled=True,
            markup=1.0,
            min_price=100.0,
            brand_filters={},
            position_filters={},
            additional_filters={},
        )
    )
    await test_session.commit()

    request_data = {
        'date': str(date.today()),
        'customer_id': customer.id,
        'config_id': config.id,
        'items': [],
        'excluded_own_positions': [],
        'excluded_supplier_positions': [],
    }
    response = await async_client.post(
        f'/customers/{customer.id}/pricelists/', json=request_data
    )

    assert response.status_code == 201, response.text
    payload = response.json()
    assert len(payload['autoparts']) == 1
    assert abs(float(payload['autoparts'][0]['price']) - 150.0) < 0.01


@pytest.mark.asyncio
async def test_customer_pricelist_send_now_updates_last_sent_at(
    test_session: AsyncSession,
    async_client: AsyncClient,
    created_customers: list[Customer],
    created_brand: Brand,
):
    customer = created_customers[0]

    provider = Provider(
        name='Send Provider',
        email_contact='send@example.com',
        email_incoming_price='send_prices@example.com',
        description='Send provider',
        comment='',
        type_prices='Wholesale',
    )
    test_session.add(provider)
    await test_session.commit()
    await test_session.refresh(provider)

    cfg = ProviderPriceListConfig(
        provider_id=provider.id,
        start_row=1,
        oem_col=0,
        qty_col=1,
        price_col=2,
        name_price='SEND_CFG',
        name_mail='SEND_MAIL',
    )
    test_session.add(cfg)
    await test_session.commit()
    await test_session.refresh(cfg)

    autopart = AutoPartPricelist(
        oem_number='OEM-SEND-001',
        brand=created_brand.name,
        name='Send Part',
    )
    await crud_pricelist.create(
        obj_in=PriceListCreate(
            provider_id=provider.id,
            provider_config_id=cfg.id,
            autoparts=[
                PriceListAutoPartAssociationCreate(
                    autopart=autopart, quantity=5, price=100.0
                ),
            ],
        ),
        session=test_session,
    )

    config = CustomerPriceListConfig(
        customer_id=customer.id,
        name='SEND CONFIG',
        general_markup=1.0,
        own_price_list_markup=1.0,
        third_party_markup=1.0,
        emails=['test@example.com'],
        additional_filters={},
    )
    test_session.add(config)
    await test_session.commit()
    await test_session.refresh(config)

    test_session.add(
        CustomerPriceListSource(
            customer_config_id=config.id,
            provider_config_id=cfg.id,
            enabled=True,
            markup=1.0,
            brand_filters={},
            position_filters={},
            additional_filters={},
        )
    )
    await test_session.commit()

    response = await async_client.post(
        f'/customers/{customer.id}/pricelist-configs/{config.id}/send-now'
    )
    assert response.status_code == 200, response.text

    await test_session.refresh(config)
    assert config.last_sent_at is not None


@pytest.mark.asyncio
async def test_get_customer_pricelists(
    test_session: AsyncSession,
    created_brand: Brand,
    created_autopart: AutoPart,
    created_providers: list[Provider],
    created_customers: list[Customer],
    async_client: AsyncClient,
):

    customer = created_customers[0]

    customer_pricelist = CustomerPriceList(
        customer_id=customer.id, date=date.today(), is_active=True
    )
    test_session.add(customer_pricelist)
    await test_session.flush()

    association = CustomerPriceListAutoPartAssociation(
        customerpricelist_id=customer_pricelist.id,
        autopart_id=created_autopart.id,
        quantity=5,
        price=Decimal('110.00'),
    )
    test_session.add(association)
    await test_session.commit()

    response = await async_client.get(f'/customers/{customer.id}/pricelists/')

    assert (
        response.status_code == 200
    ), f'Unexpected status code: {response.status_code}'

    response_data = response.json()
    assert isinstance(response_data, list)
    assert len(response_data) >= 1

    found = False
    for pricelist in response_data:
        if pricelist['id'] == customer_pricelist.id:
            found = True
            assert pricelist['customer_id'] == customer.id
            assert pricelist['date'] == str(customer_pricelist.date)
            assert 'items' in pricelist
            assert len(pricelist['items']) == 1
            item = pricelist['items'][0]
            assert item['quantity'] == 5
            assert item['price'] == 110.00
            assert 'autopart' in item
            autopart = item['autopart']
            assert autopart['id'] == created_autopart.id
            assert autopart['brand_id'] == created_autopart.brand_id
            break
    assert found, 'CustomerPricelist not found in response'


@pytest.mark.asyncio
async def test_delete_customer_pricelist(
    test_session: AsyncSession,
    async_client: AsyncClient,
    created_brand: Brand,
    created_autopart: AutoPart,
    created_providers: list[Provider],
    created_customers: list[Customer],
):
    customer = created_customers[0]

    customer_pricelist = CustomerPriceList(
        customer_id=customer.id, date=date.today(), is_active=True
    )
    test_session.add(customer_pricelist)
    await test_session.flush()

    association = CustomerPriceListAutoPartAssociation(
        customerpricelist_id=customer_pricelist.id,
        autopart_id=created_autopart.id,
        quantity=5,
        price=Decimal('110.00'),
    )
    test_session.add(association)
    await test_session.commit()

    response = await async_client.delete(
        f'/customers/{customer.id}/pricelists/{customer_pricelist.id}'
    )

    assert (
        response.status_code == 200
    ), f'Unexpected status code: {response.status_code}'

    response_data = response.json()
    expected_detail = (
        f'Deleted {customer_pricelist.id} '
        f'pricelist for customer {customer.id}'
    )
    assert response_data['detail'] == expected_detail

    deleted_pricelist = await crud_customer_pricelist.get_by_id(
        session=test_session,
        customer_id=customer.id,
        pricelist_id=customer_pricelist.id,
    )
    assert deleted_pricelist is None
