import logging
from typing import Optional

from dz_fastapi.core.constants import CORE_BASE, URL_DZ_SEARCH
from dz_fastapi.http.http_client import HTTPClient

logger = logging.getLogger('dz_fastapi')


class DZSiteClient(HTTPClient):
    def __init__(
        self,
        api_key: str,
        verify_ssl: bool = True,
        base_url: str | None = None,
    ):
        # base_url оставлен для совместимости,
        # но игнорируется — поиск всегда через SEARCH_BASE
        super().__init__(
            base_url=URL_DZ_SEARCH, api_key=api_key, verify_ssl=verify_ssl
        )

    # ---------- Поиск ----------
    async def get_offers(
        self, oem: str, brand: str, without_cross: bool = False
    ) -> Optional[list[dict]]:
        path = '/get_offers_by_oem_and_make_name'
        params = {
            'oem': oem,
            'make_name': brand,
            'without_cross': 'true' if without_cross else 'false',
        }
        resp = await self.get(path=path, params=params)
        if not isinstance(resp, dict):
            logger.warning(
                f'Неожиданный ответ от {path}: {type(resp)} -> {resp}'
            )
            return []
        if resp.get('result') != 'ok':
            logger.warning(f'API вернул не ok: {resp}')
            return []
        data = resp.get('data') or []
        if not isinstance(data, list):
            logger.warning(f'Поле data не список: {type(data)} -> {data}')
            return []
        return data

    async def get_brands(self, oem: str) -> Optional[list[dict]]:
        try:
            return await self.get('/get_brands_by_oem', params={'oem': oem})
        except Exception as error:
            logger.warning(f'Ошибка при получении брендов: {error}')
            return None

    # ---------- Корзина/заказы ----------
    def _core(self, path: str) -> str:
        return f'{CORE_BASE}{path}'

    async def get_basket(self, api_key: str) -> Optional[list[dict]]:
        try:
            return await self.get(
                self._core('/baskets'), params={'api_key': api_key}
            )
        except Exception as error:
            logger.warning(f'Ошибка при получении корзины: {error}')
            return None

    async def clean_basket(self, api_key: str) -> bool:
        try:
            resp = await self.post(
                self._core('/baskets/clear'), params={'api_key': api_key}
            )
            return resp is not None
        except Exception as error:
            logger.warning(f'Ошибка очистки корзины: {error}')
            return False

    async def add_autopart_in_basket(
        self,
        oem: str,
        make_name: str,
        detail_name: str,
        qnt: int,
        comment: str,
        min_delivery_day: int,
        max_delivery_day: int,
        api_hash: str,
        api_key: str,
        use_form: bool = False,
    ) -> bool:
        body = {
            'oem': oem,
            'make_name': make_name,
            'detail_name': detail_name,
            'qnt': qnt,
            'comment': comment,
            'min_delivery_day': min_delivery_day,
            'max_delivery_day': max_delivery_day,
            'api_hash': api_hash,
        }
        try:
            if use_form:
                resp = await self.post(
                    self._core('/baskets'),
                    params={'api_key': api_key},
                    data=body,
                )
            else:
                resp = await self.post(
                    self._core('/baskets'),
                    params={'api_key': api_key},
                    json_data=body,
                )
            logger.debug(f'add_autopart_in_basket response: {resp}')
            return bool(resp and (resp.get('result') in (None, 'ok')))
        except Exception as e:
            logger.error(f'Ошибка при добавлении в корзину: {e}')
            return False

    async def get_order_items(
        self,
        api_key: str,
        page: int = 1,
        per_page: int = 10,
        search_id_eq: int | None = None,
        search_oem_eq: str | None = None,
        search_make_name_eq: str | None = None,
        search_comment_eq: str | None = None,
        search_status_code_eq: str | None = None,
    ) -> Optional[list[dict]]:
        params = {'api_key': api_key, 'page': page, 'per_page': per_page}
        if search_id_eq is not None:
            params['search[id_eq]'] = search_id_eq
        if search_oem_eq:
            params['search[oem_eq]'] = search_oem_eq
        if search_make_name_eq:
            params['search[make_name_eq]'] = search_make_name_eq
        if search_comment_eq:
            params['search[comment_eq]'] = search_comment_eq
        if search_status_code_eq:
            params['search[status_code_eq]'] = search_status_code_eq

        try:
            return await self.get(self._core('/order_items'), params=params)
        except Exception as error:
            logger.warning(f'Ошибка при получении позиций заказа: {error}')
            return None

    async def order_basket(
        self, api_key: str, comment: str | None = None
    ) -> bool:
        """
        Оформляет текущую корзину в заказ.
        POST /api/v1/baskets/order  ->  { "result": "ok" }
        """
        path = self._core('/baskets/order')
        resp = await self.post(
            path=path,
            params={'api_key': api_key},
            json_data={'comment': comment} if comment else {},
        )
        logger.debug(f'POST {path} parsed response: {resp}')
        return bool(resp and resp.get('result') == 'ok')
