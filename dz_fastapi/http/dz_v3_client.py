import logging
from typing import Optional

from aiohttp import BasicAuth, ClientSession, TCPConnector

logger = logging.getLogger('dz_fastapi')


class V3Client:
    def __init__(
        self,
        base_url: str,
        username: str,
        password: str,
        verify_ssl: bool = True,
    ):
        self.base_url = base_url.rstrip('/')
        self.auth = BasicAuth(login=username, password=password)
        self.verify_ssl = verify_ssl
        self._session = Optional[ClientSession] = None

    async def __aenter__(self):
        self._session = ClientSession(
            connector=TCPConnector(ssl=self.verify_ssl),
            auth=self.auth,
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._session:
            await self._session.close()

    async def post(self, path: str, data: dict):
        url = f'{self.base_url}{path}'
        try:
            async with self._session.post(url, json=data) as response:
                response.raise_for_status()
                return await response.json()
        except Exception as error:
            logger.error(f'Ошибка POST запроса на {url}: {error}')
            return None

    async def get(self, path: str, params: dict = None):
        url = f'{self.base_url}{path}'
        try:
            async with self._session.post(url, params=params) as response:
                response.raise_for_status()
                return await response.json()
        except Exception as error:
            logger.error(f'Ошибка GET запроса на {url}: {error}')
            return None
