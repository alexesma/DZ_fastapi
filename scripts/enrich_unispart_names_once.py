"""One-time name/applicability enrichment for rows present in one supplier file.

The script is intentionally not connected to the scheduler or regular price
imports.  It runs in preview mode unless ``--apply`` is passed.

Example inside the application container::

    python scripts/enrich_unispart_names_once.py \
        /app/uploads/pricelistprovider/unispart-4.xlsx \
        --report /tmp/unispart-name-preview.csv

    python scripts/enrich_unispart_names_once.py \
        /app/uploads/pricelistprovider/unispart-4.xlsx \
        --report /tmp/unispart-name-applied.csv \
        --apply
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import logging
import os
import re
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

import aiohttp
import pandas as pd
from sqlalchemy import select, tuple_
from sqlalchemy.orm import selectinload

from dz_fastapi.api.validators import normalize_brand_name
from dz_fastapi.core.base import Base  # noqa: F401
from dz_fastapi.core.constants import URL_DZ_SEARCH
from dz_fastapi.core.db import get_async_session
from dz_fastapi.models.autopart import AutoPart, preprocess_oem_number
from dz_fastapi.models.brand import Brand

logger = logging.getLogger("dz_fastapi")
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

CYRILLIC_RE = re.compile(r"[А-Яа-яЁё]")
ALLOWED_MODEL_CODE_RE = re.compile(r"^[A-Za-zА-Яа-яЁё0-9&+._() -]+$")
GENERIC_NAMES = {"деталь", "запчасть", "товар", "наименование", "part", "product"}
REPORT_COLUMNS = (
    "excel_row",
    "brand",
    "oem",
    "source_name",
    "partssoft_name",
    "applicability_to_add",
    "local_autopart_id",
    "status",
)


def _text(value: Any) -> str:
    if value is None:
        return ""
    if not isinstance(value, (dict, list, tuple, set)):
        try:
            if bool(pd.isna(value)):
                return ""
        except (TypeError, ValueError):
            pass
    return str(value).strip()


def is_replaceable_name(value: Any, *, oem: Any = None) -> bool:
    """Return true only for an empty, generic, code-only or non-Russian name."""
    name = _text(value)
    normalized = name.casefold().strip(" .,:;-/")
    if not normalized or normalized in GENERIC_NAMES:
        return True
    if CYRILLIC_RE.search(name):
        return False
    if oem and preprocess_oem_number(name) == preprocess_oem_number(_text(oem)):
        return True
    return True


def is_useful_partssoft_name(value: Any) -> bool:
    name = _text(value)
    normalized = name.casefold().strip(" .,:;-/")
    return normalized not in GENERIC_NAMES and len(CYRILLIC_RE.findall(name)) >= 3


def extract_applicability(value: Any, *, brand: Any = None) -> list[str]:
    """Extract slash-separated body/model codes such as FY-11 and KX-11."""
    name = _text(value)
    if name.count("/") < 2 or CYRILLIC_RE.search(name):
        return []
    tokens = [re.sub(r"\s+", " ", token).strip(" .,:;-") for token in name.split("/")]
    tokens = [token for token in tokens if token]
    if len(tokens) < 2:
        return []
    if any(len(token) > 48 or not ALLOWED_MODEL_CODE_RE.fullmatch(token) for token in tokens):
        return []
    if sum(any(char.isdigit() for char in token) for token in tokens) < max(1, len(tokens) // 2):
        return []

    brand_name = normalize_brand_name(_text(brand))
    result: list[str] = []
    seen: set[str] = set()
    for token in tokens:
        if brand_name and not token.casefold().startswith((brand_name.casefold(), "lynk&co")):
            label = f"{brand_name} {token}"
        else:
            label = token
        if label.casefold() not in seen:
            seen.add(label.casefold())
            result.append(label)
    return result


def merge_applicability(existing: Any, additions: list[str]) -> str | None:
    values = [part.strip() for part in re.split(r"[;\n]+", _text(existing)) if part.strip()]
    seen = {value.casefold() for value in values}
    for addition in additions:
        if addition.casefold() not in seen:
            seen.add(addition.casefold())
            values.append(addition)
    return "; ".join(values) or None


def read_price_candidates(
    path: Path,
    *,
    sheet: str,
    header_row: int,
    brand_column: str,
    name_column: str,
    oem_column: str,
) -> dict[tuple[str, str], dict[str, Any]]:
    dataframe = pd.read_excel(
        path,
        sheet_name=sheet,
        header=header_row - 1,
        dtype=object,
    )
    required = {brand_column, name_column, oem_column}
    missing = sorted(required.difference(dataframe.columns))
    if missing:
        raise ValueError(f"Columns not found: {', '.join(missing)}")

    candidates: dict[tuple[str, str], dict[str, Any]] = {}
    for index, row in dataframe.iterrows():
        brand = normalize_brand_name(_text(row[brand_column]))
        oem = preprocess_oem_number(_text(row[oem_column]))
        source_name = _text(row[name_column])
        if not brand or not oem or not is_replaceable_name(source_name, oem=oem):
            continue
        key = (brand, oem)
        candidates.setdefault(
            key,
            {
                "excel_row": int(index) + header_row + 1,
                "brand": brand,
                "oem": oem,
                "source_name": source_name,
                "applicability": extract_applicability(source_name, brand=brand),
            },
        )
    return candidates


async def fetch_site_names(
    keys: list[tuple[str, str]],
    *,
    concurrency: int,
    request_timeout: float,
) -> tuple[dict[tuple[str, str], list[dict[str, Any]]], set[tuple[str, str]]]:
    """Read Parts-Soft brand/name hints only for OEMs present in the file."""
    api_key = _text(os.getenv("KEY_FOR_WEBSITE"))
    if not api_key:
        raise RuntimeError("KEY_FOR_WEBSITE is not configured")

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
            offers: list[dict[str, Any]] = []
            try:
                async with semaphore:
                    async with client.get(
                        f"{URL_DZ_SEARCH}/get_brands_by_oem",
                        params={
                            "api_key": api_key,
                            "oem": oem,
                        },
                    ) as response:
                        response.raise_for_status()
                        payload = await response.json(content_type=None)
                if isinstance(payload, dict) and payload.get("result") == "ok":
                    data = payload.get("data")
                    if isinstance(data, list):
                        offers = [row for row in data if isinstance(row, dict)]
            except (aiohttp.ClientError, asyncio.TimeoutError, ValueError) as exc:
                logger.warning("Parts-Soft search failed for %s %s: %s", brand, oem, exc)
                errors.add(key)
            for offer in offers or []:
                if normalize_brand_name(_text(offer.get("brand"))) != brand:
                    continue
                name = _text(offer.get("des_text"))
                if is_useful_partssoft_name(name):
                    result[key].append({**offer, "_resolved_name": name})
            async with lock:
                completed += 1
                if completed % 25 == 0 or completed == len(keys):
                    logger.info("Parts-Soft search: %s/%s", completed, len(keys))

        await asyncio.gather(*(fetch_one(key) for key in keys))
    return result, errors


def choose_remote_name(products: list[dict[str, Any]]) -> tuple[str | None, str]:
    names = [_text(product.get("_resolved_name")).strip(" .,:;") for product in products]
    names = [name for name in names if name]
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
    return selected, "matched"


def write_report(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=REPORT_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


async def run(args: argparse.Namespace) -> Counter[str]:
    candidates = read_price_candidates(
        args.file,
        sheet=args.sheet,
        header_row=args.header_row,
        brand_column=args.brand_column,
        name_column=args.name_column,
        oem_column=args.oem_column,
    )
    logger.info("Code-only positions found in the file: %s", len(candidates))

    remote_index, search_errors = await fetch_site_names(
        list(candidates),
        concurrency=args.concurrency,
        request_timeout=args.request_timeout,
    )
    logger.info("Positions with a Russian Parts-Soft name: %s", len(remote_index))

    session_factory = get_async_session()
    report_rows: list[dict[str, Any]] = []
    counts: Counter[str] = Counter()
    async with session_factory() as session:
        brands = list((await session.scalars(select(Brand))).all())
        brand_ids = {
            normalize_brand_name(brand.name): int(brand.id)
            for brand in brands
            if normalize_brand_name(brand.name)
        }
        local_pairs = [(brand_ids[brand], oem) for brand, oem in candidates if brand in brand_ids]
        local_by_key: dict[tuple[str, str], AutoPart] = {}
        for start in range(0, len(local_pairs), 750):
            chunk = local_pairs[start:start + 750]
            rows = (
                await session.scalars(
                    select(AutoPart)
                    .where(tuple_(AutoPart.brand_id, AutoPart.oem_number).in_(chunk))
                    .options(selectinload(AutoPart.brand))
                )
            ).all()
            for autopart in rows:
                local_by_key[(normalize_brand_name(autopart.brand.name), autopart.oem_number)] = (
                    autopart
                )

        for key, candidate in candidates.items():
            remote_name, status = choose_remote_name(remote_index.get(key, []))
            autopart = local_by_key.get(key)
            additions = candidate["applicability"]
            report = {
                **candidate,
                "partssoft_name": remote_name or "",
                "applicability_to_add": "; ".join(additions),
                "local_autopart_id": int(autopart.id) if autopart else "",
                "status": status,
            }
            if key in search_errors and remote_name is None:
                report["status"] = "partssoft_search_error"
            if autopart is None:
                report["status"] = "local_card_not_found"
            else:
                changed = False
                merged = merge_applicability(autopart.applicability, additions)
                if merged != autopart.applicability:
                    autopart.applicability = merged
                    changed = True
                if (
                    remote_name
                    and remote_name != autopart.name
                    and is_replaceable_name(autopart.name, oem=autopart.oem_number)
                ):
                    payload = dict(autopart.partssoft_payload or {})
                    source_names = list(payload.get("_one_time_supplier_names") or [])
                    for source_name in (candidate["source_name"], autopart.name):
                        if source_name and source_name not in source_names:
                            source_names.append(source_name)
                    payload["_one_time_supplier_names"] = source_names
                    autopart.partssoft_payload = payload
                    autopart.name = remote_name[:256]
                    changed = True
                if changed:
                    report["status"] = "would_update" if not args.apply else "updated"
                elif not is_replaceable_name(autopart.name, oem=autopart.oem_number):
                    report["status"] = "local_name_already_descriptive"
            counts[report["status"]] += 1
            report_rows.append(report)

        if args.apply:
            await session.commit()
        else:
            await session.rollback()

    write_report(args.report, report_rows)
    logger.info("Mode: %s", "APPLY" if args.apply else "PREVIEW")
    logger.info("Result: %s", dict(sorted(counts.items())))
    logger.info("Report: %s", args.report)
    return counts


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "One-time enrichment of code-only names for positions present "
            "in the specified Unispart price file."
        )
    )
    parser.add_argument("file", type=Path, help="Path to the supplier XLSX file")
    parser.add_argument("--sheet", default="Прайс")
    parser.add_argument("--header-row", type=int, default=2)
    parser.add_argument("--brand-column", default="Производитель")
    parser.add_argument("--name-column", default="Наименование")
    parser.add_argument("--oem-column", default="Артикул")
    parser.add_argument(
        "--concurrency",
        type=int,
        default=10,
        help="Maximum simultaneous read-only Parts-Soft site searches.",
    )
    parser.add_argument(
        "--request-timeout",
        type=float,
        default=12.0,
        help="Timeout in seconds for one Parts-Soft site search (no retries).",
    )
    parser.add_argument(
        "--report",
        type=Path,
        default=Path("/tmp/unispart-name-enrichment.csv"),
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Persist changes. Without this flag the database is not changed.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    asyncio.run(run(parse_args()))
