import asyncio
import json
import logging
import os
import ssl
from typing import Optional

import aiohttp
from aiohttp import ClientSession, ClientTimeout, TCPConnector

logger = logging.getLogger('dz_fastapi')


class HTTPClient:
    def __init__(self, base_url: str, api_key: str, verify_ssl: bool = True):
        self.base_url = base_url.rstrip('/')
        self.api_key = api_key
        self.verify_ssl = verify_ssl
        self._session: ClientSession | None = None

    def _make_connector(self):
        return TCPConnector(ssl=(False if not self.verify_ssl else None))

    def _ensure_session(self):
        if self._session is None or self._session.closed:
            timeout_total = float(os.getenv('HTTP_CLIENT_TIMEOUT', '20'))
            self._session = ClientSession(
                connector=self._make_connector(),
                timeout=ClientTimeout(total=timeout_total),
                headers={
                    # "Content-Type": "application/json",
                    "Accept": "application/json",
                },
            )

    async def __aenter__(self):
        self._ensure_session()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if self._session and not self._session.closed:
            await self._session.close()

    def _resolve_url(self, path: str) -> str:
        # Если path — абсолютный URL, используем его как есть
        if path.startswith('http://') or path.startswith('https://'):
            return path
        return f'{self.base_url}{path}'

    async def get(self, path: str, params: dict | None = None):
        self._ensure_session()
        url = self._resolve_url(path)
        params = dict(params or {})
        params.setdefault('api_key', self.api_key)
        logger.debug(f'GET {url} params={params}')
        last_error = None
        for attempt in range(3):
            try:
                async with self._session.get(url, params=params) as resp:
                    text = await resp.text()
                    if resp.status >= 400:
                        logger.warning(f'GET {url} -> {resp.status}: {text}')
                        resp.raise_for_status()
                    logger.debug(f'GET {url} <- {text}')
                    return json.loads(text)
            except (
                    aiohttp.ClientError,
                    asyncio.TimeoutError,
                    ssl.SSLError,
                    OSError
            ) as e:
                last_error = e
                logger.warning(
                    f'Ошибка GET {url} (попытка {attempt + 1}/3): {e}'
                )
                if attempt < 2:
                    await asyncio.sleep(0.5 * (attempt + 1))
                    continue
            except json.JSONDecodeError as e:
                last_error = e
                logger.warning(f'Ошибка парсинга JSON от {url}: {e}')
            break
        if last_error:
            logger.warning(f'GET {url} не выполнен: {last_error}')
        return None

    async def post(
        self,
        path: str,
        *,
        json_data: dict | None = None,
        data: dict | None = None,
        params: dict | None = None,
        headers: dict | None = None,
    ) -> Optional[dict]:
        self._ensure_session()
        url = self._resolve_url(path)
        params = dict(params or {})
        params.setdefault('api_key', self.api_key)

        if data is not None and json_data is not None:
            raise ValueError('Передай либо json_data, либо data, но не оба.')

        req_headers = dict(headers or {})
        if json_data is not None:
            req_headers.setdefault('Content-Type', 'application/json')

        payload_kwargs = {}
        if data is not None:
            payload_kwargs["data"] = data
        else:
            payload_kwargs["json"] = json_data or {}

        last_error = None
        for attempt in range(3):
            try:
                async with self._session.post(
                        url=url,
                        params=params,
                        headers=req_headers,
                        **payload_kwargs
                ) as resp:
                    text = await resp.text()
                    if resp.status >= 400:
                        logger.warning(f'POST {url} -> {resp.status}: {text}')
                        resp.raise_for_status()
                    try:
                        parsed = json.loads(text) if text else {}
                    except json.JSONDecodeError:
                        logger.warning(f'POST {url} вернул не-JSON: {text}')
                        return None
                    logger.debug(f'POST {url} parsed response: {parsed}')

                    if (
                        isinstance(parsed, dict)
                        and parsed.get('result') == 'error'
                    ):
                        logger.warning(f'API ответил ошибкой: {parsed}')
                        return None
                    return parsed
            except (
                    aiohttp.ClientError,
                    asyncio.TimeoutError,
                    ssl.SSLError,
                    OSError
            ) as e:
                last_error = e
                logger.warning(
                    f'Ошибка POST {url} (попытка {attempt + 1}/3): {e}'
                )
                if attempt < 2:
                    await asyncio.sleep(0.5 * (attempt + 1))
                    continue
            break
        if last_error:
            logger.warning(f'POST {url} не выполнен: {last_error}')
        return None
