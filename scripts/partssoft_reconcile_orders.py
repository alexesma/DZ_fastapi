"""Write a read-only Parts-Soft order reconciliation report for the last 7 days."""

from __future__ import annotations

import asyncio
import csv
import json
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

from dz_fastapi.core.db import get_async_session
from dz_fastapi.services.partssoft_order_reconciliation import (
    MAX_RECONCILIATION_DAYS,
    reconcile_partssoft_orders,
)


async def main() -> None:
    load_dotenv(".env")
    async with get_async_session()() as session:
        report = await reconcile_partssoft_orders(
            session,
            days=MAX_RECONCILIATION_DAYS,
        )

    output_dir = Path("data/partssoft_reconciliation")
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path = output_dir / f"orders_7d_{timestamp}.csv"
    json_path = output_dir / f"orders_7d_summary_{timestamp}.json"
    orders = report.pop("orders")
    if orders:
        with csv_path.open("w", encoding="utf-8-sig", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(orders[0]))
            writer.writeheader()
            writer.writerows(orders)
    report["details_file"] = csv_path.name
    json_path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(
        json.dumps(
            {"details": str(csv_path), "summary": str(json_path)},
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    asyncio.run(main())
