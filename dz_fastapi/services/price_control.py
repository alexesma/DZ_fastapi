import logging
import os
import re
from collections import defaultdict
from dataclasses import dataclass
from datetime import timedelta

import pandas as pd
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.core.time import now_moscow
from dz_fastapi.crud.partner import (crud_customer_pricelist,
                                     crud_customer_pricelist_config,
                                     crud_customer_pricelist_source,
                                     crud_pricelist)
from dz_fastapi.crud.price_control import (crud_customer_pricelist_override,
                                           crud_price_control_manual,
                                           crud_price_control_reco,
                                           crud_price_control_run,
                                           crud_price_control_source,
                                           crud_price_control_source_reco,
                                           crud_price_control_state_profile)
from dz_fastapi.http.dz_site_client import DZSiteClient
from dz_fastapi.models.autopart import preprocess_oem_number
from dz_fastapi.models.partner import CustomerPriceListConfig
from dz_fastapi.models.price_control import (PriceControlConfig,
                                             PriceControlRun,
                                             PriceControlStateProfile)
from dz_fastapi.services.process import (_apply_source_filters,
                                         _apply_source_markups, assign_brand)
from dz_fastapi.services.utils import normalize_markup

logger = logging.getLogger('dz_fastapi')

CLIENT_COEF_DEFAULT = 1.0
CLIENT_COEF_MIN = 0.3
CLIENT_COEF_MAX = 5.0
CLIENT_COEF_MIN_SAMPLES = 3
CLIENT_COEF_TRIM_FRAC = 0.1
CLIENT_COEF_MAX_STEP_PCT = 0.1
CLIENT_COEF_HISTORY_SIZE = 10
DEFAULT_SITE_API_KEY_ENV = 'API_CONTROL_KEY_FOR_WEBSITE'
SITE_API_KEY_PREFIX = 'API_CONTROL_KEY_FOR_'
BRAND_ALIASES = {
    'HAVAL': ['GREAT WALL'],
    'GREAT WALL': ['HAVAL'],
}


@dataclass
class OfferRow:
    autopart_id: int
    provider_config_id: int
    provider_id: int | None
    price: float
    base_price: float
    quantity: int
    is_own_price: bool
    brand: str
    oem: str
    name: str


def _normalize_key(oem: str, brand: str) -> tuple[str, str]:
    return (str(oem).strip().upper(), str(brand).strip().upper())


def list_site_api_key_env_names() -> list[str]:
    names: list[str] = []
    for env_name, value in os.environ.items():
        if env_name.startswith(SITE_API_KEY_PREFIX) and value:
            names.append(env_name)
    if os.getenv(DEFAULT_SITE_API_KEY_ENV):
        names.append(DEFAULT_SITE_API_KEY_ENV)
    ordered = []
    seen = set()
    for name in sorted(names):
        if name in seen:
            continue
        seen.add(name)
        ordered.append(name)
    if (
        DEFAULT_SITE_API_KEY_ENV in ordered
        and ordered[0] != DEFAULT_SITE_API_KEY_ENV
    ):
        ordered.remove(DEFAULT_SITE_API_KEY_ENV)
        ordered.insert(0, DEFAULT_SITE_API_KEY_ENV)
    return ordered


def resolve_site_api_key(
        env_name: str | None
) -> tuple[str | None, str | None]:
    candidates: list[str] = []
    if env_name:
        raw = str(env_name).strip()
        if raw:
            candidates.append(raw)
            if not raw.startswith(SITE_API_KEY_PREFIX):
                candidates.append(f'{SITE_API_KEY_PREFIX}{raw.upper()}')
    candidates.extend(
        [DEFAULT_SITE_API_KEY_ENV, *list_site_api_key_env_names()]
    )
    seen = set()
    for candidate in candidates:
        if not candidate or candidate in seen:
            continue
        seen.add(candidate)
        value = os.getenv(candidate)
        if value:
            return candidate, value
    return None, None


def _skip_dragonzap_without_dz(oem: str, brand: str) -> bool:
    if str(brand or '').strip().upper() != 'DRAGONZAP':
        return False
    normalized_oem = preprocess_oem_number(str(oem or ''))
    return bool(normalized_oem) and not normalized_oem.startswith('DZ')


def _normalize_offer(offer: dict) -> dict | None:
    def _pick(keys):
        for key in keys:
            value = offer.get(key)
            if value not in (None, ''):
                return value
        return None

    def _to_float(value):
        try:
            if value is None:
                return None
            return float(value)
        except (TypeError, ValueError):
            return None

    def _to_int(value):
        try:
            if value is None:
                return None
            return int(float(value))
        except (TypeError, ValueError):
            return None

    price_raw = _pick(
        (
            'price',
            'price_rub',
            'price_total',
            'price_total_rub',
            'price_with_markup',
            'cost',
        )
    )
    qty_raw = _pick(('qnt', 'quantity', 'qty', 'balance', 'stock'))
    price = _to_float(price_raw)
    qty = _to_int(qty_raw)
    if price is None or qty is None:
        return None
    supplier_name = _pick(
        (
            'supplier_name',
            'supplier',
            'supplier_title',
            'supplier_company',
            'provider',
            'seller_name',
            'price_name',
            'sup_logo',
        )
    )
    min_delivery = _to_int(
        _pick(('min_delivery_day', 'min_delivery', 'min_delivery_days'))
    )
    max_delivery = _to_int(
        _pick(('max_delivery_day', 'max_delivery', 'max_delivery_days'))
    )
    return {
        'price': price,
        'qty': qty,
        'supplier_name': supplier_name,
        'min_delivery_day': min_delivery,
        'max_delivery_day': max_delivery,
    }


def _percent_from_multiplier(multiplier: float | None) -> float | None:
    if multiplier is None:
        return None
    try:
        numeric = float(multiplier)
    except (TypeError, ValueError):
        return None
    pct = max((numeric - 1) * 100, 0.0)
    return round(pct, 2)


def _percent_from_markup(value: float | None) -> float | None:
    if value is None:
        return None
    try:
        multiplier = normalize_markup(value)
    except (TypeError, ValueError):
        return None
    return _percent_from_multiplier(multiplier)


def _pick_percentile(values: list[float], pct: float) -> float | None:
    if not values:
        return None
    if pct < 0:
        pct = 0
    if pct > 100:
        pct = 100
    values = sorted(values)
    if len(values) == 1:
        return values[0]
    index = int(round((pct / 100) * (len(values) - 1)))
    index = max(0, min(index, len(values) - 1))
    return values[index]


def _matches_our_offer(
    offer: dict, field: str | None, match_value: str | None
) -> bool:
    if not field or not match_value:
        return False
    value = offer.get(field)
    if value is None:
        return False
    return str(value).strip().lower() == str(match_value).strip().lower()


def _expand_brand_candidates(brands: list[str]) -> list[str]:
    expanded: list[str] = []
    seen = set()
    for brand in brands:
        candidate = str(brand or '').strip().upper()
        if not candidate or candidate in seen:
            continue
        expanded.append(candidate)
        seen.add(candidate)
        for alias in BRAND_ALIASES.get(candidate, []):
            alias_norm = str(alias or '').strip().upper()
            if alias_norm and alias_norm not in seen:
                expanded.append(alias_norm)
                seen.add(alias_norm)
    return expanded


def _resolve_site_search_key(
    oem: str, brand: str
) -> tuple[str, list[str], bool]:
    query_oem = preprocess_oem_number(str(oem or ''))
    query_brand = str(brand or '').strip().upper()

    if query_brand != 'DRAGONZAP' or not query_oem.startswith('DZ'):
        return query_oem, [query_brand], False

    stripped = preprocess_oem_number(query_oem[2:])
    if not stripped:
        return query_oem, [query_brand], False

    assigned = assign_brand(stripped)
    assigned_brands = (
        [str(item).strip().upper() for item in assigned if str(item).strip()]
        if isinstance(assigned, list)
        else []
    )
    brand_candidates = _expand_brand_candidates(assigned_brands)
    if not brand_candidates:
        brand_candidates = ['HAVAL', 'GREAT WALL']

    logger.info(
        'Price control DZ search remap: %s/%s -> %s/%s',
        oem,
        brand,
        stripped,
        ', '.join(brand_candidates),
    )
    return stripped, brand_candidates, True


def _merge_offers(offers_by_brand: list[list[dict]]) -> list[dict]:
    merged = []
    seen = set()
    for offers in offers_by_brand:
        for raw in offers or []:
            key = (
                raw.get('system_hash')
                or raw.get('hash_key')
                or (
                    raw.get('oem'),
                    raw.get('make_name'),
                    raw.get('cost'),
                    raw.get('qnt'),
                    raw.get('price_name'),
                    raw.get('sup_logo'),
                    raw.get('min_delivery_day'),
                    raw.get('max_delivery_day'),
                )
            )
            if key in seen:
                continue
            seen.add(key)
            merged.append(raw)
    return merged


async def _fetch_site_offers_for_brands(
    client: DZSiteClient, oem: str, brands: list[str]
) -> list[dict]:
    offers_by_brand = []
    for brand in brands:
        offers = await _fetch_site_offers_with_brand_fallback(
            client=client, oem=oem, brand=brand
        )
        if offers:
            offers_by_brand.append(offers)
    return _merge_offers(offers_by_brand)


def _pick_own_offer(
    raw_offers: list[dict], field: str | None, match_value: str | None
) -> dict | None:
    best = None
    for raw in raw_offers or []:
        if not _matches_our_offer(raw, field, match_value):
            continue
        normalized = _normalize_offer(raw)
        if not normalized:
            continue
        if best is None or normalized['price'] < best['price']:
            best = normalized
    return best


def _median(values: list[float]) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    mid = len(ordered) // 2
    if len(ordered) % 2 == 1:
        return ordered[mid]
    return (ordered[mid - 1] + ordered[mid]) / 2


def _coerce_client_coef(value: float | None) -> float:
    try:
        coef = float(value)
    except (TypeError, ValueError):
        return CLIENT_COEF_DEFAULT
    if coef <= 0:
        return CLIENT_COEF_DEFAULT
    return min(max(coef, CLIENT_COEF_MIN), CLIENT_COEF_MAX)


def _normalize_recent_coef(values: object) -> list[float]:
    if not isinstance(values, list):
        return []
    normalized = []
    for value in values:
        try:
            coef = float(value)
        except (TypeError, ValueError):
            continue
        if CLIENT_COEF_MIN <= coef <= CLIENT_COEF_MAX:
            normalized.append(round(coef, 4))
    return normalized[-CLIENT_COEF_HISTORY_SIZE:]


def _update_client_coef(
    current_coef: float, observations: list[float]
) -> tuple[float, int, float | None]:
    valid = []
    for raw in observations:
        try:
            coef = float(raw)
        except (TypeError, ValueError):
            continue
        if coef <= 0:
            continue
        if coef < CLIENT_COEF_MIN or coef > CLIENT_COEF_MAX:
            continue
        valid.append(coef)

    sample_size = len(valid)
    if sample_size < CLIENT_COEF_MIN_SAMPLES:
        return current_coef, sample_size, None

    trimmed = sorted(valid)
    trim_n = int(len(trimmed) * CLIENT_COEF_TRIM_FRAC)
    if trim_n > 0 and len(trimmed) - 2 * trim_n >= 3:
        trimmed = trimmed[trim_n:-trim_n]
    observed_coef = _median(trimmed)
    if observed_coef is None:
        return current_coef, sample_size, None

    if sample_size < 30:
        alpha = 0.08
    elif sample_size < 100:
        alpha = 0.15
    else:
        alpha = 0.25

    proposed = current_coef + alpha * (observed_coef - current_coef)
    max_step = max(current_coef * CLIENT_COEF_MAX_STEP_PCT, 0.02)
    proposed = min(
        max(proposed, current_coef - max_step), current_coef + max_step
    )
    proposed = min(max(proposed, CLIENT_COEF_MIN), CLIENT_COEF_MAX)
    return round(proposed, 4), sample_size, round(observed_coef, 4)


def _extract_brand_values(raw: object) -> list[str]:
    if raw is None:
        return []
    payload = raw
    if isinstance(raw, dict) and isinstance(raw.get('data'), list):
        payload = raw.get('data') or []
    if isinstance(payload, dict):
        payload = [payload]
    if not isinstance(payload, list):
        return []

    values: list[str] = []
    for item in payload:
        if isinstance(item, str):
            candidate = item.strip().upper()
            if candidate:
                values.append(candidate)
            continue
        if not isinstance(item, dict):
            continue
        for key in ('make_name', 'brand', 'name', 'value', 'label'):
            value = item.get(key)
            if value:
                candidate = str(value).strip().upper()
                if candidate:
                    values.append(candidate)
                    break
    return list(dict.fromkeys(values))


def _is_related_brand(candidate: str, requested: str) -> bool:
    cand = candidate.strip().upper()
    req = requested.strip().upper()
    if not cand or not req:
        return False
    if cand == req:
        return True
    if req in cand or cand in req:
        return True
    cand_parts = set(filter(None, re.split(r'[^A-Z0-9]+', cand)))
    req_parts = set(filter(None, re.split(r'[^A-Z0-9]+', req)))
    return bool(cand_parts & req_parts)


async def _fetch_site_offers_with_brand_fallback(
    client: DZSiteClient, oem: str, brand: str
) -> list[dict]:
    direct_offers = await client.get_offers(
        oem=oem, brand=brand, without_cross=True
    )
    if direct_offers:
        return direct_offers

    brands_raw = await client.get_brands(oem=oem)
    site_brands = _extract_brand_values(brands_raw)
    related_brands = [
        b for b in site_brands if _is_related_brand(b, brand) and b != brand
    ]

    if related_brands:
        logger.info(
            'Price control brand fallback for %s: requested=%s, related=%s',
            oem,
            brand,
            ', '.join(related_brands[:5]),
        )
    else:
        logger.info(
            'Price control no related site '
            'brands for %s: requested=%s, available=%s',
            oem,
            brand,
            ', '.join(site_brands[:5]) if site_brands else '-',
        )

    for candidate_brand in related_brands:
        candidate_offers = await client.get_offers(
            oem=oem, brand=candidate_brand, without_cross=True
        )
        if candidate_offers:
            return candidate_offers

    # Last fallback: include crosses if strict search returned nothing.
    direct_with_cross = await client.get_offers(
        oem=oem, brand=brand, without_cross=False
    )
    if direct_with_cross:
        return direct_with_cross

    for candidate_brand in related_brands:
        candidate_with_cross = await client.get_offers(
            oem=oem, brand=candidate_brand, without_cross=False
        )
        if candidate_with_cross:
            return candidate_with_cross

    return []


def _offer_row_from_series(row: pd.Series) -> OfferRow:
    return OfferRow(
        autopart_id=int(row.get('autopart_id')),
        provider_config_id=int(row.get('provider_config_id')),
        provider_id=(
            int(row.get('provider_id'))
            if row.get('provider_id') is not None
            else None
        ),
        price=float(row.get('price') or 0),
        base_price=float(
            row.get('base_price') if row.get('base_price') is not None
            else row.get('price') or 0
        ),
        quantity=int(row.get('quantity') or 0),
        is_own_price=bool(row.get('is_own_price')),
        brand=str(row.get('brand') or ''),
        oem=str(row.get('oem_number') or ''),
        name=str(row.get('name') or ''),
    )


async def _build_current_offers(
    session: AsyncSession, config: CustomerPriceListConfig
) -> tuple[
    dict[tuple[str, str], OfferRow],
    dict[tuple[int, tuple[str, str]], OfferRow],
]:
    sources = await crud_customer_pricelist_source.get_by_config_id(
        config_id=config.id, session=session
    )
    combined = []
    source_offers: dict[tuple[int, tuple[str, str]], OfferRow] = {}
    for source in sources:
        if not source.enabled:
            logger.debug(
                'Price control source skipped (disabled): '
                'config=%s provider_config=%s',
                config.id,
                source.provider_config_id,
            )
            continue
        latest_pl = await crud_pricelist.get_latest_pricelist_by_config(
            session=session, provider_config_id=source.provider_config_id
        )
        if not latest_pl:
            logger.debug(
                'Price control source skipped (no latest pricelist): '
                'config=%s provider_config=%s',
                config.id,
                source.provider_config_id,
            )
            continue
        associations = await crud_pricelist.fetch_pricelist_data(
            latest_pl.id, session
        )
        if not associations:
            logger.debug(
                'Price control source skipped (empty pricelist rows): '
                'config=%s provider_config=%s pricelist_id=%s',
                config.id,
                source.provider_config_id,
                latest_pl.id,
            )
            continue
        df = await crud_pricelist.transform_to_dataframe(
            associations=associations, session=session
        )
        source_rows_before_filters = len(df)
        df = _apply_source_filters(df, source)
        if df.empty:
            logger.debug(
                'Price control source skipped (filtered to empty): '
                'config=%s provider_config=%s rows_before=%s',
                config.id,
                source.provider_config_id,
                source_rows_before_filters,
            )
            continue
        df['base_price'] = pd.to_numeric(df['price'], errors='coerce')
        df = crud_customer_pricelist.apply_coefficient(
            df, config, apply_general_markup=False
        )
        df = _apply_source_markups(df, config, source)
        source_df = (
            df.sort_values(by=['oem_number', 'brand', 'price'])
            .drop_duplicates(subset=['oem_number', 'brand'], keep='first')
        )
        source_unique = 0
        for _, row in source_df.iterrows():
            key = _normalize_key(row.get('oem_number'), row.get('brand'))
            source_offers[
                (source.provider_config_id, key)
            ] = _offer_row_from_series(row)
            source_unique += 1
        logger.info(
            'Price control source prepared: config=%s provider_config=%s '
            'rows_before=%s rows_after=%s unique_keys=%s',
            config.id,
            source.provider_config_id,
            source_rows_before_filters,
            len(df),
            source_unique,
        )
        combined.append(df)

    if not combined:
        return {}, {}

    final_df = pd.concat(combined, ignore_index=True)
    if 'is_own_price' in final_df.columns:
        final_df['__own_rank'] = final_df['is_own_price'].astype(int)
        final_df = (
            final_df.sort_values(
                by=['oem_number', 'brand', '__own_rank', 'price'],
                ascending=[True, True, False, True],
            )
            .drop_duplicates(subset=['oem_number', 'brand'], keep='first')
            .drop(columns=['__own_rank'])
        )
    else:
        final_df = final_df.sort_values(
            by=['oem_number', 'brand', 'price']
        ).drop_duplicates(subset=['oem_number', 'brand'], keep='first')

    offers: dict[tuple[str, str], OfferRow] = {}
    for _, row in final_df.iterrows():
        key = _normalize_key(row.get('oem_number'), row.get('brand'))
        offers[key] = _offer_row_from_series(row)
    overrides = await crud_customer_pricelist_override.get_for_config(
        session=session, config_id=config.id
    )
    if overrides:
        for offer in offers.values():
            if offer.autopart_id in overrides:
                offer.price = float(overrides[offer.autopart_id])
        for offer in source_offers.values():
            if offer.autopart_id in overrides:
                offer.price = float(overrides[offer.autopart_id])
    return offers, source_offers


def _allocate_counts(
    total: int, manual_count: int, sources: list[dict]
) -> dict[int, int]:
    remaining = max(total - manual_count, 0)
    if remaining <= 0 or not sources:
        return {s['provider_config_id']: 0 for s in sources}

    fixed = [s for s in sources if s.get('locked')]
    open_sources = [s for s in sources if not s.get('locked')]
    fixed_pct = sum(float(s.get('weight_pct') or 0) for s in fixed)
    fixed_pct = max(min(fixed_pct, 100.0), 0.0)
    remaining_pct = max(100.0 - fixed_pct, 0.0)

    for s in open_sources:
        s['weight_pct'] = remaining_pct / max(len(open_sources), 1)

    allocations: dict[int, int] = {}
    for source in sources:
        count = int(
            round(remaining * float(source.get('weight_pct') or 0) / 100)
        )
        allocations[source['provider_config_id']] = count

    # корректировка до точного remaining
    diff = remaining - sum(allocations.values())
    if diff != 0 and allocations:
        keys = list(allocations.keys())
        idx = 0
        while diff != 0:
            allocations[keys[idx % len(keys)]] += 1 if diff > 0 else -1
            diff += -1 if diff > 0 else 1
            idx += 1
    return allocations


async def _resolve_manual_items(
    session: AsyncSession, config_id: int
) -> list[tuple[str, str]]:
    items = await crud_price_control_manual.list_by_config(
        session=session, config_id=config_id
    )
    result = []
    for item in items:
        result.append((_normalize_key(item.oem, item.brand)))
    return result


async def _resolve_recently_checked_keys(
    session: AsyncSession,
    config: PriceControlConfig,
    state_profile: PriceControlStateProfile | None = None,
) -> set[tuple[str, str]]:
    source = state_profile or config
    cooldown_hours = int(getattr(source, 'cooldown_hours', 0) or 0)
    if cooldown_hours <= 0:
        return set()
    since_dt = now_moscow() - timedelta(hours=cooldown_hours)
    reset_at = getattr(source, 'cooldown_reset_at', None)
    if reset_at and reset_at > since_dt:
        since_dt = reset_at
    rows = await crud_price_control_reco.list_recent_keys_by_config(
        session=session,
        config_id=config.id,
        since_dt=since_dt,
    )
    return {_normalize_key(oem, brand) for oem, brand in rows}


def _pick_items_for_source(
    df: pd.DataFrame,
    count: int,
    exclude: set[tuple[str, str]] | None = None,
) -> list[tuple[str, str]]:
    if count <= 0 or df.empty:
        return []
    if exclude is None:
        exclude = set()
    if 'quantity' in df.columns:
        df = df.copy()
        df['__qty'] = pd.to_numeric(df['quantity'], errors='coerce').fillna(0)
        df = df.sort_values(
            by=['__qty', 'oem_number', 'brand'],
            ascending=[False, True, True],
        )
    else:
        df = df.sort_values(by=['oem_number', 'brand'])
    selected = []
    for _, row in df.iterrows():
        key = _normalize_key(row['oem_number'], row['brand'])
        if key in exclude:
            continue
        selected.append(key)
        exclude.add(key)
        if len(selected) >= count:
            break
    return selected


def _pick_items_any_source(
    df: pd.DataFrame,
    count: int,
    exclude: set[tuple[str, str]] | None = None,
) -> list[tuple[str, str, int]]:
    if count <= 0 or df.empty:
        return []
    if exclude is None:
        exclude = set()
    if 'quantity' in df.columns:
        df = df.copy()
        df['__qty'] = pd.to_numeric(df['quantity'], errors='coerce').fillna(0)
        df = df.sort_values(
            by=['__qty', 'provider_config_id', 'oem_number', 'brand'],
            ascending=[False, True, True, True],
        )
    else:
        df = df.sort_values(by=['provider_config_id', 'oem_number', 'brand'])
    selected: list[tuple[str, str, int]] = []
    for _, row in df.iterrows():
        key = _normalize_key(row['oem_number'], row['brand'])
        if key in exclude:
            continue
        selected.append(
            (key[0], key[1], int(row['provider_config_id']))
        )
        exclude.add(key)
        if len(selected) >= count:
            break
    return selected


def _calc_cost_price(
    offer: OfferRow,
    config: PriceControlConfig,
) -> float:
    if offer.is_own_price:
        brand_key = offer.brand.strip().upper()
        brand_markups = config.own_cost_markup_by_brand or {}
        markup = brand_markups.get(brand_key)
        if markup is None:
            markup = config.own_cost_markup_default
        multiplier = normalize_markup(markup)
        if multiplier <= 0:
            multiplier = 1.0
        return float(offer.price) / multiplier
    base_price = offer.base_price
    if base_price is None or pd.isna(base_price):
        base_price = offer.price
    return float(base_price)


def _min_allowed_price(cost_price: float, min_markup_pct: float) -> float:
    multiplier = normalize_markup(min_markup_pct)
    return float(cost_price) * multiplier


async def run_price_control(
    session: AsyncSession, config: PriceControlConfig
) -> int:
    pricelist_config = await crud_customer_pricelist_config.get_by_id(
        session=session,
        customer_id=config.customer_id,
        config_id=config.pricelist_config_id,
    )
    if not pricelist_config:
        raise ValueError('Pricelist config not found')

    sources = await crud_price_control_source.list_by_config(
        session=session, config_id=config.id
    )
    source_map = {s.provider_config_id: s for s in sources}
    customer_sources = await crud_customer_pricelist_source.get_by_config_id(
        config_id=pricelist_config.id, session=session
    )
    current_markup_map = {
        s.provider_config_id: s.markup for s in customer_sources
    }
    source_configs = [
        {
            'provider_config_id': s.provider_config_id,
            'weight_pct': s.weight_pct,
            'locked': s.locked,
        }
        for s in sources
    ]
    state_profile = await (
        crud_price_control_state_profile.get_or_create_active(
            session=session,
            config=config,
        )
    )

    manual_items = await _resolve_manual_items(session, config.id)
    paused_auto_keys = await _resolve_recently_checked_keys(
        session=session,
        config=config,
        state_profile=state_profile,
    )
    allocations = _allocate_counts(
        total=int(config.total_daily_count or 0),
        manual_count=len(manual_items),
        sources=source_configs,
    )

    offers, source_offers = await _build_current_offers(
        session, pricelist_config
    )
    if not offers and not source_offers:
        run = await crud_price_control_run.create(
            session=session, config_id=config.id, total_items=0
        )
        return run.id

    general_multiplier = normalize_markup(pricelist_config.general_markup)
    own_multiplier = normalize_markup(pricelist_config.own_price_list_markup)
    third_multiplier = normalize_markup(pricelist_config.third_party_markup)

    def _individual_multiplier(provider_id: int | None) -> float:
        if not provider_id:
            return 1.0
        markups = pricelist_config.individual_markups or {}
        value = markups.get(provider_id)
        if value is None:
            value = markups.get(str(provider_id))
        return normalize_markup(value)

    offer_df = pd.DataFrame([
        {
            'oem_number': key[1][0],
            'brand': key[1][1],
            'provider_config_id': key[0],
            'quantity': offer.quantity,
        }
        for key, offer in source_offers.items()
    ])

    selected_items: list[tuple[str, str, int | None]] = []
    selected_items.extend((oem, brand, None) for oem, brand in manual_items)
    selected_set = {(oem, brand) for oem, brand in manual_items}
    auto_exclude = set(selected_set)
    auto_exclude.update(paused_auto_keys - selected_set)

    source_pick_stats: list[tuple[int, int, int]] = []
    for provider_config_id, count in allocations.items():
        if count <= 0:
            continue
        df = offer_df[offer_df['provider_config_id'] == provider_config_id]
        picked = _pick_items_for_source(df, count, exclude=auto_exclude)
        selected_items.extend(
            (oem, brand, provider_config_id) for oem, brand in picked
        )
        auto_exclude.update(picked)
        source_pick_stats.append(
            (provider_config_id, int(count), len(picked))
        )

    auto_target = max(
        int(config.total_daily_count or 0) - len(manual_items), 0
    )
    selected_auto = max(len(selected_items) - len(manual_items), 0)
    if selected_auto < auto_target and not offer_df.empty:
        fill_needed = auto_target - selected_auto
        fallback_items = _pick_items_any_source(
            offer_df,
            fill_needed,
            exclude=auto_exclude,
        )
        selected_items.extend(fallback_items)
        auto_exclude.update((oem, brand) for oem, brand, _ in fallback_items)

    unique_items: list[tuple[str, str, int | None]] = []
    seen_keys: set[tuple[str, str]] = set()
    for oem, brand, provider_config_id in selected_items:
        key = _normalize_key(oem, brand)
        if key in seen_keys:
            continue
        seen_keys.add(key)
        unique_items.append((key[0], key[1], provider_config_id))
    selected_items = unique_items

    if bool(getattr(config, 'exclude_dragonzap_non_dz', False)):
        before = len(selected_items)
        selected_items = [
            item
            for item in selected_items
            if not _skip_dragonzap_without_dz(item[0], item[1])
        ]
        skipped = before - len(selected_items)
        if skipped:
            logger.info(
                'Price control skip DRAGONZAP without '
                'DZ: config=%s skipped=%s',
                config.id,
                skipped,
            )

    if paused_auto_keys:
        logger.info(
            'Price control cooldown: config=%s '
            'pause_hours=%s paused_auto=%s selected=%s manual=%s',
            config.id,
            int(getattr(state_profile, 'cooldown_hours', 0) or 0),
            len(paused_auto_keys),
            len(selected_items),
            len(manual_items),
        )
    if source_pick_stats:
        logger.info(
            'Price control source picks: config=%s %s',
            config.id,
            '; '.join(
                'provider_config=%s allocated=%s picked=%s'
                % (provider_id, allocated, picked)
                for provider_id, allocated, picked in source_pick_stats
            ),
        )

    run = await crud_price_control_run.create(
        session=session, config_id=config.id, total_items=len(selected_items)
    )

    api_key_env, key = resolve_site_api_key(
        getattr(config, 'site_api_key_env', None)
    )
    if not key:
        logger.warning(
            'Site API key not set for '
            'config=%s selected=%s; skip price control',
            config.id,
            getattr(config, 'site_api_key_env', None),
        )
        return run.id
    logger.info(
        'Price control using site API key env: config=%s env=%s',
        config.id,
        api_key_env,
    )

    recommendations = []
    source_reco_stats = defaultdict(list)
    client_coef_observations: list[float] = []
    recent_coef_history = _normalize_recent_coef(
        getattr(state_profile, 'client_markup_recent_coef', [])
    )
    client_markup_coef = _coerce_client_coef(
        getattr(state_profile, 'client_markup_coef', None)
    )
    running_client_coef = client_markup_coef
    if recent_coef_history:
        history_median = _median(recent_coef_history)
        if history_median is not None:
            running_client_coef = _coerce_client_coef(history_median)
    async with DZSiteClient(
        base_url=os.getenv('API_CONTROL_BASE_URL') or None,
        api_key=key,
        verify_ssl=False,
    ) as client:
        for oem, brand, selected_provider_config_id in selected_items:
            item_key = _normalize_key(oem, brand)
            offer = None
            if selected_provider_config_id is not None:
                offer = source_offers.get(
                    (selected_provider_config_id, item_key)
                )
            if not offer:
                offer = offers.get(item_key)
            if not offer:
                recommendations.append(
                    {
                        'oem': oem,
                        'brand': brand,
                        'missing_in_pricelist': True,
                        'missing_competitor': True,
                    }
                )
                continue
            try:
                (
                    query_oem,
                    query_brands,
                    is_dz_remap,
                ) = _resolve_site_search_key(
                    oem=oem,
                    brand=brand,
                )
                raw_offers = await _fetch_site_offers_for_brands(
                    client=client, oem=query_oem, brands=query_brands
                )
            except Exception as exc:
                logger.warning('Site offers failed for %s: %s', oem, exc)
                raw_offers = []
                is_dz_remap = False

            competitor_offers = []
            own_site_offer = None
            for raw in raw_offers or []:
                normalized = _normalize_offer(raw)
                if not normalized:
                    continue
                if _matches_our_offer(
                    raw, config.our_offer_field, config.our_offer_match
                ):
                    if (
                        own_site_offer is None
                        or normalized['price'] < own_site_offer['price']
                    ):
                        own_site_offer = normalized
                    continue
                if config.min_stock is not None and (
                    normalized['qty'] < int(config.min_stock)
                ):
                    continue
                if config.max_delivery_days is not None:
                    max_delivery = normalized.get('max_delivery_day')
                    if max_delivery is not None and (
                        max_delivery > int(config.max_delivery_days)
                    ):
                        continue
                competitor_offers.append(normalized)

            if is_dz_remap:
                try:
                    dz_direct_offers = await client.get_offers(
                        oem=preprocess_oem_number(str(oem or '')),
                        brand=str(brand or '').strip().upper(),
                        without_cross=True,
                    )
                    if not dz_direct_offers:
                        dz_direct_offers = await client.get_offers(
                            oem=preprocess_oem_number(str(oem or '')),
                            brand=str(brand or '').strip().upper(),
                            without_cross=False,
                        )
                    own_site_offer_direct = _pick_own_offer(
                        raw_offers=dz_direct_offers or [],
                        field=config.our_offer_field,
                        match_value=config.our_offer_match,
                    )
                    if own_site_offer_direct:
                        own_site_offer = own_site_offer_direct
                except Exception as exc:
                    logger.warning(
                        'Site offers failed for DZ direct coef %s: %s',
                        oem,
                        exc,
                    )

            competitor_offers.sort(key=lambda x: x['price'])
            best = competitor_offers[0] if competitor_offers else None
            own_site_price = (
                own_site_offer['price'] if own_site_offer else None
            )
            row_client_coef = None
            if (
                own_site_price is not None
                and own_site_price > 0
                and offer.price is not None
                and offer.price > 0
            ):
                raw_coef = offer.price / own_site_price
                if CLIENT_COEF_MIN <= raw_coef <= CLIENT_COEF_MAX:
                    row_client_coef = raw_coef
                    client_coef_observations.append(raw_coef)
                    observed_running = _median(
                        (
                            recent_coef_history
                            + client_coef_observations
                        )[-CLIENT_COEF_HISTORY_SIZE:]
                    )
                    if observed_running is not None:
                        running_client_coef = _coerce_client_coef(
                            observed_running
                        )

            source_cfg = source_map.get(offer.provider_config_id)
            min_markup = (
                float(source_cfg.min_markup_pct or 0.0)
                if source_cfg
                else 0.0
            )
            cost_price = _calc_cost_price(offer, config)
            min_allowed = _min_allowed_price(cost_price, min_markup)

            competitor_price = best['price'] if best else None
            target_price = None
            effective_coef = None
            if competitor_price is not None:
                delta_raw = float(config.delta_pct or 0.0)
                delta = min(max(delta_raw, 0.0), 100.0) / 100.0
                target_site_price = competitor_price * (1 - delta)
                effective_coef = _coerce_client_coef(
                    row_client_coef
                    if row_client_coef is not None
                    else running_client_coef
                )
                target_price = target_site_price * effective_coef
                logger.debug(
                    'Price control calc: oem=%s brand=%s query_oem=%s '
                    'query_brands=%s competitor=%s own_site=%s row_coef=%s '
                    'running_coef=%s target=%s',
                    oem,
                    brand,
                    query_oem,
                    ','.join(query_brands),
                    competitor_price,
                    own_site_price,
                    row_client_coef,
                    running_client_coef,
                    target_price,
                )

            is_cheapest = (
                competitor_price is not None
                and offer.price <= competitor_price
            )
            below_cost = (
                target_price is not None and target_price < cost_price
            )
            below_min_markup = (
                target_price is not None and target_price < min_allowed
            )
            missing_competitor = competitor_price is None
            suggested_action = 'keep'
            if (
                target_price is not None
                and not below_cost
                and not below_min_markup
            ):
                if offer.price > target_price:
                    suggested_action = 'lower'
                elif offer.price < target_price:
                    suggested_action = 'raise'

            recommendations.append(
                {
                    'provider_config_id': offer.provider_config_id,
                    'autopart_id': offer.autopart_id,
                    'oem': oem,
                    'brand': brand,
                    'name': offer.name,
                    'our_price': offer.price,
                    'competitor_price': competitor_price,
                    'competitor_qty': best['qty'] if best else None,
                    'competitor_supplier': (
                        best['supplier_name'] if best else None
                    ),
                    'competitor_min_delivery': (
                        best['min_delivery_day'] if best else None
                    ),
                    'competitor_max_delivery': (
                        best['max_delivery_day'] if best else None
                    ),
                    'target_price': target_price,
                    'effective_client_coef': effective_coef,
                    'effective_client_pct': _percent_from_multiplier(
                        effective_coef
                    ),
                    'cost_price': cost_price,
                    'min_allowed_price': min_allowed,
                    'is_cheapest': is_cheapest,
                    'below_cost': below_cost,
                    'below_min_markup': below_min_markup,
                    'missing_competitor': missing_competitor,
                    'missing_in_pricelist': False,
                    'suggested_action': suggested_action,
                }
            )
            if competitor_price is not None:
                individual_multiplier = _individual_multiplier(
                    offer.provider_id
                )
                base_multiplier = (
                    general_multiplier
                    * individual_multiplier
                    * (
                        own_multiplier
                        if offer.is_own_price
                        else third_multiplier
                    )
                )
                required_markup = None
                if (
                    offer.base_price
                    and offer.base_price > 0
                    and base_multiplier > 0
                ):
                    required_markup = target_price / (
                        offer.base_price * base_multiplier
                    )
                    if required_markup <= 0:
                        required_markup = None
                source_reco_stats[offer.provider_config_id].append(
                    {
                        'our_price': offer.price,
                        'competitor_price': competitor_price,
                        'required_markup': required_markup,
                    }
                )

    await crud_price_control_reco.create_many(
        session=session, run_id=run.id, rows=recommendations
    )

    source_recos = []
    for provider_config_id, pairs in source_reco_stats.items():
        if not pairs:
            continue
        cheaper_count = sum(
            1
            for item in pairs
            if item['our_price'] <= item['competitor_price']
        )
        coverage = cheaper_count / max(len(pairs), 1) * 100
        required_markups = [
            item['required_markup']
            for item in pairs
            if item.get('required_markup')
        ]
        target_pct = float(config.target_cheapest_pct or 0.0)
        raw_suggested_multiplier = _pick_percentile(
            required_markups, target_pct
        )
        suggested_multiplier = raw_suggested_multiplier
        source_cfg = source_map.get(provider_config_id)
        min_markup_pct = (
            float(source_cfg.min_markup_pct) if source_cfg else 0.0
        )
        min_multiplier = normalize_markup(min_markup_pct)
        note = None
        if raw_suggested_multiplier is None:
            note = 'Недостаточно данных'
        elif suggested_multiplier < min_multiplier:
            suggested_multiplier = min_multiplier
            if raw_suggested_multiplier < 1:
                raw_pct = round((raw_suggested_multiplier - 1) * 100, 2)
                note = (
                    f'Цель недостижима при мин. наценке '
                    f'(нужно {raw_pct}%)'
                )
            else:
                note = 'Минимальная наценка'
        current_markup = current_markup_map.get(provider_config_id)
        source_recos.append(
            {
                'provider_config_id': provider_config_id,
                'coverage_pct': round(coverage, 2),
                'sample_size': len(pairs),
                'current_markup_pct': _percent_from_markup(current_markup),
                'suggested_markup_pct': _percent_from_multiplier(
                    suggested_multiplier
                ),
                'note': note,
            }
        )

    if source_recos:
        await crud_price_control_source_reco.create_many(
            session=session, run_id=run.id, rows=source_recos
        )

    new_recent_coef = _normalize_recent_coef(
        [*recent_coef_history, *client_coef_observations]
    )
    state_profile.client_markup_recent_coef = new_recent_coef
    updated_coef, sample_size, observed_coef = _update_client_coef(
        client_markup_coef, new_recent_coef
    )
    state_profile.client_markup_coef = updated_coef
    state_profile.client_markup_sample_size = sample_size
    if updated_coef != client_markup_coef:
        logger.info(
            'Price control client coef updated: '
            'config=%s profile=%s old=%.4f observed=%s new=%.4f samples=%s',
            config.id,
            state_profile.id,
            client_markup_coef,
            observed_coef,
            updated_coef,
            sample_size,
        )
    else:
        logger.info(
            'Price control client coef unchanged: '
            'config=%s profile=%s current=%.4f observed=%s samples=%s',
            config.id,
            state_profile.id,
            client_markup_coef,
            observed_coef,
            sample_size,
        )

    state_profile.updated_at = now_moscow()
    session.add(state_profile)
    config.client_markup_recent_coef = state_profile.client_markup_recent_coef
    config.client_markup_coef = state_profile.client_markup_coef
    config.client_markup_sample_size = state_profile.client_markup_sample_size
    config.cooldown_hours = state_profile.cooldown_hours
    config.cooldown_reset_at = state_profile.cooldown_reset_at
    config.last_run_at = now_moscow()
    session.add(config)
    await session.commit()
    return run.id


async def apply_recommendations(
    session: AsyncSession,
    run_id: int,
    recommendation_ids: list[int],
):
    run = await session.get(PriceControlRun, run_id)
    if not run:
        return 0
    recos = await crud_price_control_reco.get_by_ids(
        session=session, ids=recommendation_ids
    )
    if not recos:
        return 0
    count = 0
    for reco in recos:
        if reco.autopart_id and reco.target_price and reco.target_price > 0:
            await crud_customer_pricelist_override.upsert(
                session=session,
                config_id=run.config_id,
                autopart_id=reco.autopart_id,
                price=float(reco.target_price),
            )
            count += 1
    return count


async def apply_source_recommendations(
    session: AsyncSession,
    run_id: int,
    source_recommendation_ids: list[int],
) -> int:
    run = await session.get(PriceControlRun, run_id)
    if not run:
        return 0
    config = await session.get(PriceControlConfig, run.config_id)
    if not config:
        return 0
    recos = await crud_price_control_source_reco.get_by_ids(
        session=session, ids=source_recommendation_ids
    )
    if not recos:
        return 0
    sources = await crud_customer_pricelist_source.get_by_config_id(
        config_id=config.pricelist_config_id, session=session
    )
    source_map = {s.provider_config_id: s for s in sources}
    updated = 0
    for reco in recos:
        if reco.suggested_markup_pct is None:
            continue
        source = source_map.get(reco.provider_config_id)
        if not source:
            continue
        source.markup = float(reco.suggested_markup_pct)
        session.add(source)
        updated += 1
    if updated:
        await session.commit()
    return updated
