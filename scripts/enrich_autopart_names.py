"""Enrich suspicious names across the whole nomenclature.

The command is safe by default: it only writes a CSV preview. Use ``--apply``
to persist reviewed changes. Work is bounded by ``--limit`` and can be resumed
with the reported ``next_after_id``.

Examples inside the application container::

    python -m scripts.enrich_autopart_names --saved-only \
        --limit 5000 --report /tmp/autopart-names-saved.csv

    python -m scripts.enrich_autopart_names --limit 500 \
        --report /tmp/autopart-names-preview.csv

    python -m scripts.enrich_autopart_names --limit 500 \
        --report /tmp/autopart-names-applied.csv --apply
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import logging
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import selectinload

from dz_fastapi.api.validators import normalize_brand_name
from dz_fastapi.core.base import Base  # noqa: F401
from dz_fastapi.core.db import get_async_session
from dz_fastapi.models.autopart import AutoPart
from dz_fastapi.models.brand import Brand
from dz_fastapi.services.autopart_name_enrichment import (
    choose_remote_name,
    classify_suspicious_name,
    extract_saved_partssoft_name,
    fetch_partssoft_names,
    is_useful_name,
)

logger = logging.getLogger("dz_fastapi")
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

REPORT_COLUMNS = (
    "autopart_id",
    "brand",
    "oem",
    "old_name",
    "new_name",
    "reason",
    "source",
    "status",
)


def _candidate_from_row(autopart: AutoPart) -> dict[str, Any] | None:
    reason = classify_suspicious_name(autopart.name, oem=autopart.oem_number)
    if reason is None or autopart.brand is None:
        return None
    brand = normalize_brand_name(autopart.brand.name)
    if not brand or not autopart.oem_number:
        return None
    saved_name = extract_saved_partssoft_name(autopart.partssoft_payload)
    saved_name_source = "saved_partssoft_payload" if saved_name else ""
    if not saved_name and is_useful_name(autopart.description) and len(autopart.description) <= 256:
        saved_name = autopart.description.strip()
        saved_name_source = "local_description"
    return {
        "autopart_id": int(autopart.id),
        "brand": brand,
        "oem": autopart.oem_number,
        "old_name": autopart.name or "",
        "reason": reason,
        "saved_name": saved_name,
        "saved_name_source": saved_name_source,
    }


async def _load_candidates(args: argparse.Namespace) -> tuple[list[dict[str, Any]], int]:
    session_factory = get_async_session()
    candidates: list[dict[str, Any]] = []
    cursor = max(int(args.after_id), 0)
    scan_size = max(int(args.scan_batch_size), 100)

    async with session_factory() as session:
        while len(candidates) < args.limit:
            stmt = (
                select(AutoPart)
                .where(AutoPart.id > cursor)
                .options(selectinload(AutoPart.brand))
                .order_by(AutoPart.id.asc())
                .limit(scan_size)
            )
            if args.brand:
                stmt = stmt.join(Brand, Brand.id == AutoPart.brand_id).where(
                    Brand.name.ilike(f"%{args.brand.strip()}%")
                )
            rows = list((await session.scalars(stmt)).all())
            if not rows:
                break
            for autopart in rows:
                cursor = int(autopart.id)
                candidate = _candidate_from_row(autopart)
                if candidate is not None:
                    candidates.append(candidate)
                    if len(candidates) >= args.limit:
                        break
            if len(rows) < scan_size:
                break
    return candidates, cursor


def _write_report(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=REPORT_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def _updated_payload(
    payload_value: Any,
    *,
    old_name: str,
    new_name: str,
    source: str,
) -> dict[str, Any]:
    payload = dict(payload_value) if isinstance(payload_value, dict) else {}
    history = list(payload.get("_name_enrichment_history") or [])
    history.append(
        {
            "old_name": old_name,
            "new_name": new_name,
            "source": source,
            "changed_at": datetime.now().astimezone().isoformat(),
        }
    )
    payload["_name_enrichment_history"] = history[-20:]
    return payload


async def _apply_changes(resolved: list[dict[str, Any]]) -> Counter[str]:
    counts: Counter[str] = Counter()
    if not resolved:
        return counts
    session_factory = get_async_session()
    ids = [row["autopart_id"] for row in resolved]
    by_id = {row["autopart_id"]: row for row in resolved}
    async with session_factory() as session:
        current_rows = list(
            (
                await session.scalars(
                    select(AutoPart).where(AutoPart.id.in_(ids)).with_for_update()
                )
            ).all()
        )
        for autopart in current_rows:
            report = by_id[int(autopart.id)]
            if classify_suspicious_name(autopart.name, oem=autopart.oem_number) is None:
                report["status"] = "skipped_name_changed"
                counts[report["status"]] += 1
                continue
            new_name = report["new_name"]
            old_name = autopart.name or ""
            if not new_name or new_name == old_name:
                report["status"] = "unchanged"
                counts[report["status"]] += 1
                continue
            autopart.name = new_name[:256]
            autopart.partssoft_payload = _updated_payload(
                autopart.partssoft_payload,
                old_name=old_name,
                new_name=autopart.name,
                source=report["source"],
            )
            report["status"] = "updated"
            counts[report["status"]] += 1
        await session.commit()
    return counts


async def run(args: argparse.Namespace) -> Counter[str]:
    candidates, cursor = await _load_candidates(args)
    logger.info("Suspicious names selected: %s", len(candidates))

    unresolved = [row for row in candidates if not row["saved_name"]]
    remote_index: dict[tuple[str, str], list[dict[str, Any]]] = {}
    remote_errors: set[tuple[str, str]] = set()
    if unresolved and not args.saved_only:
        remote_index, remote_errors = await fetch_partssoft_names(
            [(row["brand"], row["oem"]) for row in unresolved],
            concurrency=args.concurrency,
            request_timeout=args.request_timeout,
            retries=args.retries,
        )

    report_rows: list[dict[str, Any]] = []
    resolved: list[dict[str, Any]] = []
    counts: Counter[str] = Counter()
    for candidate in candidates:
        key = (candidate["brand"], candidate["oem"])
        new_name = candidate["saved_name"]
        source = candidate["saved_name_source"] if new_name else ""
        status = "matched_saved_payload" if new_name else ""
        if not new_name and not args.saved_only:
            new_name, remote_status = choose_remote_name(remote_index.get(key, []))
            source = "partssoft_search" if new_name else ""
            status = "matched_partssoft_search" if new_name else remote_status
            if key in remote_errors and not new_name:
                status = "partssoft_search_error"
        elif not new_name:
            status = "saved_name_not_found"

        report = {
            **candidate,
            "new_name": new_name or "",
            "source": source,
            "status": "would_update" if new_name and not args.apply else status,
        }
        if new_name:
            resolved.append(report)
        else:
            counts[report["status"]] += 1
        report_rows.append(report)

    if args.apply:
        counts.update(await _apply_changes(resolved))
    else:
        counts["would_update"] += len(resolved)

    _write_report(args.report, report_rows)
    logger.info("Mode: %s", "APPLY" if args.apply else "PREVIEW")
    logger.info("Result: %s", dict(sorted(counts.items())))
    logger.info("Report: %s", args.report)
    logger.info("next_after_id: %s", cursor)
    return counts


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Safely enrich suspicious names across all local nomenclature."
    )
    parser.add_argument("--after-id", type=int, default=0)
    parser.add_argument("--limit", type=int, default=500)
    parser.add_argument("--scan-batch-size", type=int, default=2000)
    parser.add_argument("--brand", help="Optional SQL ILIKE brand filter, for example GEELY")
    parser.add_argument("--saved-only", action="store_true")
    parser.add_argument("--concurrency", type=int, default=5)
    parser.add_argument("--request-timeout", type=float, default=15.0)
    parser.add_argument("--retries", type=int, default=2)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument(
        "--report",
        type=Path,
        default=Path("/tmp/autopart-name-enrichment.csv"),
    )
    args = parser.parse_args()
    if args.limit <= 0:
        parser.error("--limit must be greater than zero")
    return args


if __name__ == "__main__":
    asyncio.run(run(parse_args()))
