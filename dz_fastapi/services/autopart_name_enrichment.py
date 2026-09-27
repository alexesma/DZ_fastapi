"""Safe helpers for enriching code-only autopart names from Parts-Soft."""

from __future__ import annotations

import asyncio
import logging
import os
import re
from collections import Counter, defaultdict
from typing import Any

import aiohttp

from dz_fastapi.api.validators import normalize_brand_name
from dz_fastapi.core.constants import URL_DZ_SEARCH
from dz_fastapi.models.autopart import preprocess_oem_number

logger = logging.getLogger("dz_fastapi")

CYRILLIC_RE = re.compile(r"[А-Яа-яЁё]")
TOKEN_RE = re.compile(r"[A-Za-zА-Яа-яЁё0-9&+._-]+")
GENERIC_NAMES = {
    "деталь",
    "запчасть",
    "товар",
    "наименование",
    "артикул",
    "part",
    "product",
    "detail",
    "oem",
}
SAVED_NAME_KEYS = (
    "name",
    "des_text",
    "description",
    "detail_name",
    "title",
)


def clean_text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def classify_suspicious_name(value: Any, *, oem: Any = None) -> str | None:
    """Classify names safe to replace automatically.

    Descriptive Latin names are intentionally preserved. A name is selected
    only when it is empty/generic, equals the OEM, or consists entirely of
    number-bearing model/code tokens such as ``180108 (6008 2RS)``.
    """

    name = clean_text(value)
    normalized = name.casefold().strip(" .,:;-/")
    if not normalized:
        return "empty"
    if normalized in GENERIC_NAMES:
        return "generic"
    if CYRILLIC_RE.search(name):
        return None
    if oem and preprocess_oem_number(name) == preprocess_oem_number(clean_text(oem)):
        return "same_as_oem"

    tokens = TOKEN_RE.findall(name)
    if not tokens:
        return "numeric_or_symbols"
    if not any(char.isalpha() for char in name):
        return "numeric_or_symbols"
    if any(char.isdigit() for char in name) and all(
        any(char.isdigit() for char in token) or len(token) <= 2 for token in tokens
    ):
        return "code_only"
    return None


def is_useful_name(value: Any) -> bool:
    name = clean_text(value)
    normalized = name.casefold().strip(" .,:;-/")
    return (
        normalized not in GENERIC_NAMES and len(name) >= 3 and len(CYRILLIC_RE.findall(name)) >= 3
    )


def extract_saved_partssoft_name(payload: Any) -> str | None:
    """Use already stored Parts-Soft data before making a remote request."""

    if not isinstance(payload, dict):
        return None
    containers = [payload]
    for key in ("product", "card", "data"):
        nested = payload.get(key)
        if isinstance(nested, dict):
            containers.append(nested)
    for container in containers:
        for key in SAVED_NAME_KEYS:
            value = clean_text(container.get(key))
            if is_useful_name(value):
                return value[:256]
    return None


def choose_remote_name(products: list[dict[str, Any]]) -> tuple[str | None, str]:
    names = [clean_text(product.get("_resolved_name")).strip(" .,:;") for product in products]
    names = [name for name in names if is_useful_name(name)]
    if not names:
        return None, "partssoft_name_not_found"

    groups: dict[str, Counter[str]] = defaultdict(Counter)
    for name in names:
        key = re.sub(r"[^0-9a-zа-яё]+", " ", name.casefold()).strip()
        groups[key][name] += 1
    ranked = sorted(
        groups.items(),
        key=lambda item: (-sum(item[1].values()), -len(item[0]), item[0]),
    )
    if len(ranked) > 1 and sum(ranked[0][1].values()) == sum(ranked[1][1].values()):
        return None, "partssoft_name_ambiguous"
    variants = ranked[0][1]
    selected = sorted(
        variants,
        key=lambda name: (-variants[name], name.isupper(), -len(name), name),
    )[0]
    return selected[:256], "matched"


async def fetch_partssoft_names(
    keys: list[tuple[str, str]],
    *,
    concurrency: int = 5,
    request_timeout: float = 15.0,
    retries: int = 2,
) -> tuple[dict[tuple[str, str], list[dict[str, Any]]], set[tuple[str, str]]]:
    """Read name hints with bounded concurrency and retries."""

    api_key = clean_text(os.getenv("KEY_FOR_WEBSITE"))
    if not api_key:
        raise RuntimeError("KEY_FOR_WEBSITE is not configured")

    unique_keys = list(dict.fromkeys(keys))
    semaphore = asyncio.Semaphore(max(1, concurrency))
    result: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    errors: set[tuple[str, str]] = set()
    completed = 0
    lock = asyncio.Lock()
    connector = aiohttp.TCPConnector(ssl=False)
    timeout = aiohttp.ClientTimeout(total=max(3.0, request_timeout))

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as client:

        async def fetch_one(key: tuple[str, str]) -> None:
            nonlocal completed
            brand, oem = key
            payload: Any = None
            last_error: Exception | None = None
            for attempt in range(max(0, retries) + 1):
                try:
                    async with semaphore:
                        async with client.get(
                            f"{URL_DZ_SEARCH}/get_brands_by_oem",
                            params={"api_key": api_key, "oem": oem},
                        ) as response:
                            response.raise_for_status()
                            payload = await response.json(content_type=None)
                    last_error = None
                    break
                except (aiohttp.ClientError, asyncio.TimeoutError, ValueError) as exc:
                    last_error = exc
                    if attempt < retries:
                        await asyncio.sleep(0.5 * (2**attempt))
            if last_error is not None:
                logger.warning("Parts-Soft search failed for %s %s: %s", brand, oem, last_error)
                errors.add(key)
            elif isinstance(payload, dict) and payload.get("result") == "ok":
                data = payload.get("data")
                for offer in data if isinstance(data, list) else []:
                    if not isinstance(offer, dict):
                        continue
                    if normalize_brand_name(clean_text(offer.get("brand"))) != brand:
                        continue
                    name = clean_text(offer.get("des_text"))
                    if is_useful_name(name):
                        result[key].append({**offer, "_resolved_name": name})
            async with lock:
                completed += 1
                if completed % 25 == 0 or completed == len(unique_keys):
                    logger.info("Parts-Soft name search: %s/%s", completed, len(unique_keys))

        await asyncio.gather(*(fetch_one(key) for key in unique_keys))
    return result, errors
