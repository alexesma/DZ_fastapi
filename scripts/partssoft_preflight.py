"""Read-only pilot audit; prints aggregates, never credentials or customer records.

Run from repository root: poetry run python scripts/partssoft_preflight.py
Only the first 100 remote customers/products are inspected. No imports or writes.
"""

import asyncio
import json
import os
from collections import Counter

import aiohttp
from dotenv import load_dotenv
from sqlalchemy import func, select

from dz_fastapi.core.base import AutoPart, Brand, Customer
from dz_fastapi.core.db import get_async_session


def norm(value):
    return str(value or "").strip().casefold()


async def main():
    load_dotenv()
    base = (os.getenv("V3_BASE_URL") or "https://admin.dragonzap.ru/api/v3").rstrip("/")
    if not base.startswith("https://"):
        raise ValueError("V3_BASE_URL must use HTTPS")
    report = {
        "scope": "pilot: first page only, not a full reconciliation",
        "wholesale_candidate_rule": (
            "customer has an order whose embedded customer.region_id is Moscow (28)"
        ),
    }
    samples = {}
    async with aiohttp.ClientSession(
        auth=aiohttp.BasicAuth(os.environ["V3_USERNAME"], os.environ["V3_PASSWORD"]),
        timeout=aiohttp.ClientTimeout(total=30),
    ) as session:
        requests = (
            ("customers", {}),
            ("products", {}),
            ("customer_groups", {}),
            ("discount_types", {}),
            ("regions", {}),
            ("orders", {"search[customer_region_id_eq]": 28}),
        )
        for entity, filters in requests:
            async with session.get(
                f"{base}/{entity}.json",
                params={"page": 1, "per_page": 100, **filters},
                allow_redirects=False,
            ) as response:
                if response.status != 200:
                    report[entity] = {"http_status": response.status}
                    continue
                payload = await response.json()
                rows = payload.get(entity) if isinstance(payload, dict) else None
                if not isinstance(rows, list):
                    report[entity] = {"error": "unexpected response shape"}
                    continue
                samples[entity] = rows
                report[entity] = {"sample_size": len(rows)}
                if entity == "customers":
                    report[entity]["classification"] = {
                        field: dict(Counter(str(row.get(field)) for row in rows))
                        for field in (
                            "is_supplier",
                            "customer_group_id",
                            "client_type_id",
                            "discount_type_id",
                        )
                    }
                elif entity == "products":
                    report[entity]["populated"] = {
                        field: sum(bool(row.get(field)) for row in rows)
                        for field in (
                            "body",
                            "product_photo_url",
                            "images",
                            "product_properties",
                            "barcode",
                        )
                    }
                elif entity in {"customer_groups", "discount_types", "regions"}:
                    report[entity]["definitions"] = [
                        {"id": row.get("id"), "name": row.get("name")} for row in rows
                    ]
                elif entity == "orders":
                    customer_ids = {
                        row.get("customer_id") for row in rows if row.get("customer_id")
                    }
                    mismatched_regions = sum(
                        (row.get("customer") or {}).get("region_id") != 28 for row in rows
                    )
                    report[entity].update(
                        {
                            "unique_customer_ids": len(customer_ids),
                            "non_moscow_rows": mismatched_regions,
                        }
                    )

    try:
        async with get_async_session()() as session:
            report["local_counts"] = {
                "customers": await session.scalar(select(func.count(Customer.id))),
                "products": await session.scalar(select(func.count(AutoPart.id))),
            }
            product_candidates = Counter()
            for row in samples.get("products", []):
                if not norm(row.get("oem")) or not norm(row.get("make_name")):
                    product_candidates["missing_identity"] += 1
                    continue
                count = await session.scalar(
                    select(func.count(AutoPart.id))
                    .join(Brand, Brand.id == AutoPart.brand_id)
                    .where(
                        func.lower(func.trim(AutoPart.oem_number)) == norm(row.get("oem")),
                        func.lower(func.trim(Brand.name)) == norm(row.get("make_name")),
                    )
                )
                product_candidates[
                    (
                        "unique_candidate"
                        if count == 1
                        else "ambiguous" if count else "no_exact_candidate"
                    )
                ] += 1
            report["product_candidates"] = dict(product_candidates)
            report["matching_limits"] = (
                "Exact brand/article only; synonyms not applied. "
                "Customer matching is outside this first-page pilot; "
                "wholesale candidates come from Moscow orders."
            )
    except Exception as exc:
        # Exception messages may contain database URLs or personal data.
        report["local_error"] = type(exc).__name__
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
