import logging
import os
from typing import Any, Dict, List, Optional

from dz_fastapi.core.constants import URL_DZ_V3
from dz_fastapi.http.dz_v3_client import V3Client

logger = logging.getLogger('dz_fastapi')


async def creare_order_v3(
    order_items: List[Dict[str, Any]]
) -> Optional[Dict[str, Any]]:
    customer_id = os.getenv('WEBSITE_USER_ID')
    base_url = URL_DZ_V3
    username = os.getenv('V3_USERNAME')
    password = os.getenv('V3_PASSWORD')
    async with V3Client(
        base_url=base_url, username=username, password=password
    ) as client:
        data = {
            'order': {
                'customer_id': customer_id,
                'order_items_attributes': order_items,
            }
        }
        response = await client.post('/orders/manual.json', data=data)
        if response:
            logger.info(f'Заказ успешно создан: {response}')
        else:
            logger.error('Ошибка при создании заказа через API v3.')
        return response
