from __future__ import annotations

import logging
from typing import Any, Optional

from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.crud.brand import brand_crud
from dz_fastapi.http.dz_site_client import DZSiteClient

logger = logging.getLogger("dz_fastapi")

_DRAGONZAP_BRAND = "DRAGONZAP"


def _dedupe_brand_names(candidates: list[str]) -> list[str]:
    unique: list[str] = []
    seen: set[str] = set()
    for candidate in candidates:
        normalized = str(candidate or "").strip().upper()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        unique.append(normalized)
    return unique


def _prioritize_site_query_brands(
    candidates: list[str],
    *,
    requested_brand: str,
) -> list[str]:
    unique = _dedupe_brand_names(candidates)
    if not unique:
        return []

    requested = str(requested_brand or "").strip().upper()
    if _DRAGONZAP_BRAND not in unique:
        if requested and requested in unique:
            return [requested, *[item for item in unique if item != requested]]
        return unique

    non_dragonzap = [item for item in unique if item != _DRAGONZAP_BRAND]
    ordered: list[str] = []
    if requested and requested != _DRAGONZAP_BRAND and requested in non_dragonzap:
        ordered.append(requested)
    ordered.extend(item for item in non_dragonzap if item not in ordered)
    # For Dragonzap-branded stock we prefer querying real supplier-brand
    # synonyms first and keep the umbrella Dragonzap brand as the last fallback.
    if _DRAGONZAP_BRAND in unique:
        ordered.append(_DRAGONZAP_BRAND)
    return ordered


async def expand_site_query_brands(
    session: AsyncSession,
    brand_name: Optional[str],
) -> list[str]:
    normalized_input = str(brand_name or "").strip().upper()
    if not normalized_input:
        return []

    expanded = [normalized_input]
    if normalized_input != _DRAGONZAP_BRAND:
        return expanded

    try:
        main_brand = await brand_crud.get_brand_by_name_or_none(
            brand_name=normalized_input,
            session=session,
        )
        if not main_brand:
            return expanded

        related = await brand_crud.get_all_synonyms_bi_directional(
            brand=main_brand,
            session=session,
        )
        candidates = [str(main_brand.name).strip().upper()]
        candidates.extend(
            str(item.name).strip().upper()
            for item in related
            if str(getattr(item, "name", "")).strip()
        )
        candidates.append(normalized_input)
        return _prioritize_site_query_brands(
            candidates,
            requested_brand=normalized_input,
        )
    except Exception as exc:
        logger.warning(
            "Failed to expand brand synonyms for %s: %s",
            normalized_input,
            exc,
        )
        return expanded


def _extract_site_brand_rows(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if not isinstance(payload, dict):
        return []
    raw_rows = payload.get("data")
    if not isinstance(raw_rows, list):
        return []
    return [item for item in raw_rows if isinstance(item, dict)]


def prepare_site_brand_candidates(
    payload: Any,
) -> list[dict[str, Any]]:
    deduped: dict[str, dict[str, Any]] = {}
    for row in _extract_site_brand_rows(payload):
        brand_name = str(row.get("brand") or "").strip().upper()
        if not brand_name:
            continue
        try:
            rate = int(row.get("rate") or 0)
        except (TypeError, ValueError):
            rate = 0
        current = deduped.get(brand_name)
        normalized_row = {
            "brand": brand_name,
            "number": row.get("number"),
            "des_text": row.get("des_text"),
            "rate": rate,
        }
        if current is None or rate > int(current.get("rate") or 0):
            deduped[brand_name] = normalized_row

    return sorted(
        deduped.values(),
        key=lambda item: (-int(item.get("rate") or 0), item["brand"]),
    )


def merge_site_offers(offers_by_brand: list[list[dict]]) -> list[dict]:
    merged: list[dict] = []
    seen = set()
    for offers in offers_by_brand:
        for raw in offers or []:
            key = (
                raw.get("system_hash")
                or raw.get("hash_key")
                or (
                    raw.get("oem"),
                    raw.get("make_name"),
                    raw.get("cost"),
                    raw.get("qnt"),
                    raw.get("price_name"),
                    raw.get("sup_logo"),
                    raw.get("min_delivery_day"),
                    raw.get("max_delivery_day"),
                )
            )
            if key in seen:
                continue
            seen.add(key)
            merged.append(raw)
    return merged


async def fetch_site_offers_for_brands(
    dz_site_client: DZSiteClient,
    *,
    oem: str,
    brands: list[str],
    without_cross: bool,
) -> list[list[dict]]:
    offers_by_brand: list[list[dict]] = []
    for brand_name in brands:
        offers = await dz_site_client.get_offers(
            oem=oem,
            brand=brand_name,
            without_cross=without_cross,
        )
        if not offers:
            continue
        for item in offers:
            if isinstance(item, dict):
                item.setdefault("query_brand", brand_name)
        offers_by_brand.append(offers)
    return offers_by_brand


async def resolve_fallback_site_brand(
    dz_site_client: DZSiteClient,
    *,
    oem: str,
    exclude_brands: list[str],
    without_cross: bool,
) -> tuple[list[list[dict]], list[dict[str, Any]], Optional[str]]:
    site_brand_candidates = prepare_site_brand_candidates(
        await dz_site_client.get_brands(oem)
    )
    tried = {str(item or "").strip().upper() for item in exclude_brands}
    for candidate in site_brand_candidates:
        brand_name = candidate["brand"]
        if brand_name in tried:
            continue
        offers_by_brand = await fetch_site_offers_for_brands(
            dz_site_client,
            oem=oem,
            brands=[brand_name],
            without_cross=without_cross,
        )
        if offers_by_brand:
            return offers_by_brand, site_brand_candidates, brand_name
    return [], site_brand_candidates, None
