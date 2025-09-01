from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from aiohttp import ClientError

from dz_fastapi.http.http_client import HTTPClient


@pytest.mark.asyncio
async def test_get_success():
    client = HTTPClient(base_url='https://example.com', api_key='test')
    mock_response = AsyncMock()
    mock_response.raise_for_status = MagicMock()
    mock_response.text = AsyncMock(return_value='[{"id": 1}]')
    mock_response.status = 200

    async with client:
        with patch.object(
            client._session,
            'get',
            return_value=AsyncMock(
                __aenter__=AsyncMock(return_value=mock_response),
                __aexit__=AsyncMock(return_value=None),
            ),
        ):
            result = await client.get('/test')
            assert result == [{'id': 1}]


async def test_get_json_decode_error():
    client = HTTPClient(base_url='https://example.com', api_key='test')
    mock_response = AsyncMock()
    mock_response.raise_for_status = MagicMock()
    mock_response.text = AsyncMock(return_value='INVALID_JSON')
    mock_response.status = 200

    async with client:
        with patch.object(
            client._session,
            'get',
            return_value=AsyncMock(
                __aenter__=AsyncMock(return_value=mock_response),
                __aexit__=AsyncMock(return_value=None),
            ),
        ):
            result = await client.get('/test')
            assert result is None


@pytest.mark.asyncio
async def test_get_client_error():
    client = HTTPClient(base_url='https://example.com', api_key='test')

    async with client:
        with patch.object(
            client._session, 'get', side_effect=ClientError('Fail')
        ):
            result = await client.get('/test')
            assert result is None


@pytest.mark.asyncio
async def test_post_success():
    client = HTTPClient(base_url='https://example.com', api_key='test')
    mock_response = AsyncMock()
    mock_response.raise_for_status = MagicMock()
    mock_response.text = AsyncMock(return_value='{"result": "ok"}')
    mock_response.status = 200

    async with client:
        with patch.object(
            client._session,
            'post',
            return_value=AsyncMock(
                __aenter__=AsyncMock(return_value=mock_response),
                __aexit__=AsyncMock(return_value=None),
            ),
        ):
            result = await client.post('/post', json_data={'key': 'value'})
            assert result == {'result': 'ok'}


@pytest.mark.asyncio
async def test_post_api_error_result():
    client = HTTPClient(base_url='https://example.com', api_key='test')
    mock_response = AsyncMock()
    mock_response.raise_for_status = MagicMock()
    mock_response.text = AsyncMock(return_value='{"result": "error"}')
    mock_response.status = 200

    async with client:
        with patch.object(
            client._session,
            'post',
            return_value=AsyncMock(
                __aenter__=AsyncMock(return_value=mock_response),
                __aexit__=AsyncMock(return_value=None),
            ),
        ):
            result = await client.post('/post', json_data={'key': 'value'})
            assert result is None


@pytest.mark.asyncio
async def test_post_json_decode_error():
    client = HTTPClient(base_url='https://example.com', api_key='test')
    mock_response = AsyncMock()
    mock_response.raise_for_status = MagicMock()
    mock_response.text = AsyncMock(return_value='INVALID_JSON')
    mock_response.status = 200

    async with client:
        with patch.object(
            client._session,
            'post',
            return_value=AsyncMock(
                __aenter__=AsyncMock(return_value=mock_response),
                __aexit__=AsyncMock(return_value=None),
            ),
        ):
            result = await client.post('/post', json_data={'key': 'value'})
            assert result is None


@pytest.mark.asyncio
async def test_post_client_error():
    client = HTTPClient(base_url='https://example.com', api_key='test')

    async with client:
        with patch.object(
            client._session, 'post', side_effect=ClientError('Fail')
        ):
            result = await client.post('/post', json_data={'key': 'value'})
            assert result is None
