"""Фото детали с платформы Parts-Soft через поиск сайта.

Фото принадлежит платформе, у себя мы его не храним: сайт в ответе
поиска отдаёт ссылку на миниатюру ``sys_info.goods_img_url``. Настоящее
фото лежит на ``/thumbnails/``; ``/labels/*.svg`` — картинка-заглушка.
Ответы кэшируем в памяти, чтобы открытие таблицы не превращалось в залп
запросов к сайту.
"""

import asyncio
import logging
import os
import time

from dz_fastapi.core.constants import URL_DZ_SEARCH
from dz_fastapi.http.dz_site_client import DZSiteClient
from dz_fastapi.models.autopart import preprocess_oem_number

logger = logging.getLogger("dz_fastapi")

HIT_TTL_SECONDS = 12 * 3600
MISS_TTL_SECONDS = 2 * 3600
MAX_CONCURRENCY = 4

_cache: dict[tuple[str, str], tuple[float, str | None]] = {}


def pick_thumbnail(offers: list[dict] | None) -> str | None:
    for offer in offers or []:
        sys_info = offer.get("sys_info") if isinstance(offer, dict) else None
        url = sys_info.get("goods_img_url") if isinstance(sys_info, dict) else None
        if isinstance(url, str) and "/thumbnails/" in url:
            return url
    return None


def _key(brand: str, oem: str) -> tuple[str, str]:
    return brand.strip().lower(), preprocess_oem_number(oem)


async def fetch_site_photos(pairs: list[tuple[str, str]]) -> dict[tuple[str, str], str | None]:
    """Возвращает {(бренд в нижнем регистре, нормализованный артикул): url | None}."""
    api_key = (os.getenv("KEY_FOR_WEBSITE") or "").strip()
    result: dict[tuple[str, str], str | None] = {}
    todo: list[tuple[str, str]] = []
    now = time.monotonic()
    for brand, oem in pairs:
        key = _key(brand, oem)
        if not key[0] or not key[1]:
            continue
        cached = _cache.get(key)
        if cached and cached[0] > now:
            result[key] = cached[1]
        elif key not in result:
            result[key] = None
            todo.append((brand, oem))
    if not todo or not api_key:
        return result

    semaphore = asyncio.Semaphore(MAX_CONCURRENCY)

    async with DZSiteClient(base_url=URL_DZ_SEARCH, api_key=api_key, verify_ssl=False) as client:

        async def one(brand: str, oem: str) -> None:
            key = _key(brand, oem)
            async with semaphore:
                try:
                    offers = await asyncio.wait_for(
                        client.get_offers(oem, brand, without_cross=True), timeout=15
                    )
                except Exception as exc:  # сайт недоступен — просто без фото
                    logger.warning("Фото с сайта %s %s не получено: %s", brand, oem, exc)
                    return
            url = pick_thumbnail(offers)
            ttl = HIT_TTL_SECONDS if url else MISS_TTL_SECONDS
            _cache[key] = (time.monotonic() + ttl, url)
            result[key] = url

        await asyncio.gather(*(one(brand, oem) for brand, oem in todo))
    return result
