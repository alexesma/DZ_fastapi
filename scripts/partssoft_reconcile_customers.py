"""Create a read-only reconciliation report for Parts-Soft Moscow customers."""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import os
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any

import aiohttp
from dotenv import load_dotenv
from sqlalchemy import select

from dz_fastapi.core.base import Customer, CustomerExternalReference
from dz_fastapi.core.db import get_async_session
from dz_fastapi.services.partssoft_reconciliation import (
    PARTS_SOFT_SOURCE,
    CustomerMatcher,
    compare_fields,
    flatten_remote_customer,
    normalize_digits,
)

MOSCOW_REGION_ID = 28
PAGE_SIZE = 100


async def fetch_json(session: aiohttp.ClientSession, url: str, **params: Any) -> dict[str, Any]:
    async with session.get(url, params=params, allow_redirects=False) as response:
        response.raise_for_status()
        payload = await response.json()
        if not isinstance(payload, dict):
            raise ValueError(f"Unexpected response type from {url}")
        return payload


async def fetch_moscow_customer_ids(session: aiohttp.ClientSession, base_url: str) -> list[int]:
    customer_ids: list[int] = []
    page = 1
    while True:
        payload = await fetch_json(
            session,
            f"{base_url}/customers.json",
            page=page,
            per_page=PAGE_SIZE,
            **{"search[region_id_eq]": MOSCOW_REGION_ID},
        )
        rows = payload.get("customers") or []
        if not isinstance(rows, list):
            raise ValueError("Unexpected customers response")
        for customer in rows:
            customer_id = customer.get("id")
            if customer_id:
                customer_ids.append(int(customer_id))
        print(
            f"Moscow customers: page {page}, total {len(customer_ids)}",
            file=sys.stderr,
            flush=True,
        )
        if len(rows) < PAGE_SIZE:
            break
        page += 1
    return customer_ids


async def check_moscow_order_sample(
    session: aiohttp.ClientSession, base_url: str
) -> dict[str, int]:
    payload = await fetch_json(
        session,
        f"{base_url}/orders.json",
        page=1,
        per_page=PAGE_SIZE,
        **{"search[customer_region_id_eq]": MOSCOW_REGION_ID},
    )
    rows = payload.get("orders") or []
    return {
        "sample_size": len(rows),
        "non_moscow_rows": sum(
            (row.get("customer") or {}).get("region_id") != MOSCOW_REGION_ID for row in rows
        ),
    }


async def fetch_customer_details(
    session: aiohttp.ClientSession, base_url: str, customer_ids: list[int]
) -> list[dict[str, Any]]:
    semaphore = asyncio.Semaphore(5)

    async def one(customer_id: int) -> dict[str, Any]:
        async with semaphore:
            payload = await fetch_json(session, f"{base_url}/customers/{customer_id}.json")
            customer = payload.get("customer")
            if not isinstance(customer, dict):
                raise ValueError(f"Customer {customer_id}: unexpected response")
            return customer

    result: list[dict[str, Any]] = []
    for start in range(0, len(customer_ids), 25):
        batch = customer_ids[start:start + 25]
        result.extend(await asyncio.gather(*(one(customer_id) for customer_id in batch)))
        print(
            f"Customers: {min(start + len(batch), len(customer_ids))}/{len(customer_ids)}",
            file=sys.stderr,
            flush=True,
        )
    return result


async def load_local_customers() -> list[dict[str, Any]]:
    async with get_async_session()() as session:
        customers = (await session.scalars(select(Customer).order_by(Customer.id))).all()
        references = (
            await session.scalars(
                select(CustomerExternalReference).where(
                    CustomerExternalReference.source_system == PARTS_SOFT_SOURCE,
                    CustomerExternalReference.is_active.is_(True),
                )
            )
        ).all()
    references_by_customer = {row.customer_id: row.external_customer_id for row in references}
    return [
        {
            "id": row.id,
            "external_id": references_by_customer.get(row.id),
            "name": row.name,
            "email": row.email_contact,
            "inn": row.inn,
            "kpp": row.kpp,
            "legal_address": row.legal_address,
            "postal_address": row.postal_address,
        }
        for row in customers
    ]


def write_reports(
    output_dir: Path,
    remote_customers: list[dict[str, Any]],
    order_control: dict[str, int],
    local_customers: list[dict[str, Any]],
) -> tuple[Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    matcher = CustomerMatcher(local_customers)
    rows = []
    summary: Counter[str] = Counter()
    for raw in remote_customers:
        remote = flatten_remote_customer(raw)
        if not normalize_digits(remote.get("inn")):
            summary["excluded_without_inn"] += 1
            continue
        match = matcher.match(remote)
        local = matcher.rows.get(match.local_id) if match.local_id else None
        missing, conflicts = compare_fields(remote, local)
        review_required = bool(
            match.classification in {"review_match", "ambiguous", "new_customer"} or conflicts
        )
        summary[match.classification] += 1
        summary["review_required" if review_required else "ready"] += 1
        summary["with_conflicts" if conflicts else "without_conflicts"] += 1
        rows.append(
            {
                "external_customer_id": int(remote["external_id"]),
                "qualification": "region_moscow",
                "classification": match.classification,
                "match_basis": match.basis,
                "local_customer_id": match.local_id or "",
                "candidate_local_ids": ",".join(map(str, match.candidate_ids)),
                "review_required": review_required,
                "missing_local_fields": ",".join(missing),
                "conflicting_fields": ",".join(conflicts),
                "partssoft_name": remote["name"],
                "local_name": local.get("name", "") if local else "",
                "partssoft_email": remote["email"],
                "local_email": local.get("email", "") if local else "",
                "partssoft_inn": remote["inn"],
                "local_inn": local.get("inn", "") if local else "",
                "partssoft_kpp": remote["kpp"],
                "local_kpp": local.get("kpp", "") if local else "",
                "partssoft_legal_address": remote["legal_address"],
                "local_legal_address": local.get("legal_address", "") if local else "",
                "partssoft_postal_address": remote["postal_address"],
                "local_postal_address": local.get("postal_address", "") if local else "",
                "discount_type_id": remote["discount_type_id"],
            }
        )
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path = output_dir / f"customers_{timestamp}.csv"
    json_path = output_dir / f"summary_{timestamp}.json"
    with csv_path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]) if rows else [])
        if rows:
            writer.writeheader()
            writer.writerows(rows)
    payload = {
        "generated_at": datetime.now().astimezone().isoformat(),
        "rule": "Customers assigned to region Moscow (28)",
        "read_only": True,
        "order_filter_control": order_control,
        "partssoft_moscow_customers": len(remote_customers),
        "partssoft_wholesale_candidates": len(rows),
        "local_customers": len(local_customers),
        "results": dict(sorted(summary.items())),
        "details_file": csv_path.name,
    }
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return csv_path, json_path


async def main(output_dir: Path) -> None:
    load_dotenv(".env")
    base_url = (os.getenv("V3_BASE_URL") or "https://admin.dragonzap.ru/api/v3").rstrip("/")
    if not base_url.startswith("https://"):
        raise ValueError("V3_BASE_URL must use HTTPS")
    auth = aiohttp.BasicAuth(os.environ["V3_USERNAME"], os.environ["V3_PASSWORD"])
    async with aiohttp.ClientSession(auth=auth, timeout=aiohttp.ClientTimeout(total=60)) as session:
        customer_ids = await fetch_moscow_customer_ids(session, base_url)
        order_control = await check_moscow_order_sample(session, base_url)
        remote = await fetch_customer_details(session, base_url, sorted(set(customer_ids)))
    local = await load_local_customers()
    csv_path, json_path = write_reports(output_dir, remote, order_control, local)
    print(json.dumps({"details": str(csv_path), "summary": str(json_path)}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/partssoft_reconciliation"),
    )
    args = parser.parse_args()
    asyncio.run(main(args.output_dir))
