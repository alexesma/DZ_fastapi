"""Read-only audit of duplicate email and Parts-Soft customer orders.

Run inside the backend container after deployment::

    poetry run python scripts/audit_customer_order_duplicates.py --days 7

The script never updates or deletes rows.  It reports matching order pairs and
all operational foreign-key references which must be reviewed before merging.
"""

from __future__ import annotations

import argparse
import asyncio
import json
from datetime import timedelta
from decimal import Decimal
from typing import Any

from dotenv import load_dotenv
from sqlalchemy import func, select
from sqlalchemy.orm import load_only, selectinload

from dz_fastapi.core.base import Base, CustomerOrder
from dz_fastapi.core.db import get_async_session
from dz_fastapi.core.time import now_moscow
from dz_fastapi.services.customer_order_identity import match_customer_order

PARTS_SOFT_SOURCE = "PARTS_SOFT"


def _order_total(order: CustomerOrder) -> Decimal:
    return sum(
        (
            Decimal(str(item.requested_price or 0)) * int(item.requested_qty or 0)
            for item in order.items or []
        ),
        Decimal("0"),
    ).quantize(Decimal("0.01"))


async def _dependency_counts(session, order: CustomerOrder) -> dict[str, int]:
    item_ids = [int(item.id) for item in order.items or [] if item.id is not None]
    counts: dict[str, int] = {}
    for table in Base.metadata.sorted_tables:
        for column in table.columns:
            for foreign_key in column.foreign_keys:
                target = f"{foreign_key.column.table.name}.{foreign_key.column.name}"
                values: list[int]
                if target == "customerorder.id":
                    # CustomerOrderItem is the order's own payload, not an
                    # operational dependency. ORM deletion handles it.
                    if table.name == "customerorderitem" and column.name == "order_id":
                        continue
                    values = [int(order.id)]
                elif target == "customerorderitem.id":
                    values = item_ids
                else:
                    continue
                if not values:
                    continue
                count = int(
                    await session.scalar(
                        select(func.count()).select_from(table).where(column.in_(values))
                    )
                    or 0
                )
                if count:
                    counts[f"{table.name}.{column.name}"] = count
    return dict(sorted(counts.items()))


async def build_report(days: int) -> dict[str, Any]:
    start = now_moscow() - timedelta(days=max(1, int(days)))
    async with get_async_session()() as session:
        orders = (
            (
                await session.scalars(
                    select(CustomerOrder)
                    .options(
                        load_only(
                            CustomerOrder.id,
                            CustomerOrder.customer_id,
                            CustomerOrder.external_source,
                            CustomerOrder.external_order_id,
                            CustomerOrder.received_at,
                            CustomerOrder.order_number,
                            CustomerOrder.order_date,
                            CustomerOrder.deleted_at,
                        ),
                        selectinload(CustomerOrder.items).load_only(
                            CustomerOrder.items.property.mapper.class_.id,
                            CustomerOrder.items.property.mapper.class_.oem,
                            CustomerOrder.items.property.mapper.class_.brand,
                            CustomerOrder.items.property.mapper.class_.requested_qty,
                            CustomerOrder.items.property.mapper.class_.requested_price,
                        ),
                    )
                    .where(
                        CustomerOrder.received_at >= start,
                        CustomerOrder.deleted_at.is_(None),
                    )
                    .order_by(CustomerOrder.received_at.asc(), CustomerOrder.id.asc())
                )
            )
            .unique()
            .all()
        )
        partssoft_orders = [order for order in orders if order.external_source == PARTS_SOFT_SOURCE]
        local_by_customer: dict[int, list[CustomerOrder]] = {}
        for order in orders:
            if order.external_source is None:
                local_by_customer.setdefault(int(order.customer_id), []).append(order)

        pairs: list[dict[str, Any]] = []
        used_local_ids: set[int] = set()
        for partssoft_order in partssoft_orders:
            candidates = [
                order
                for order in local_by_customer.get(int(partssoft_order.customer_id), [])
                if int(order.id) not in used_local_ids
            ]
            decision = match_customer_order(
                candidates,
                incoming_number=partssoft_order.order_number,
                incoming_date=partssoft_order.order_date,
                incoming_at=partssoft_order.received_at,
                incoming_items=partssoft_order.items or [],
            )
            if decision is None:
                continue
            local_order = decision.order
            used_local_ids.add(int(local_order.id))
            partssoft_dependencies = await _dependency_counts(session, partssoft_order)
            local_dependencies = await _dependency_counts(session, local_order)
            partssoft_dependency_total = sum(partssoft_dependencies.values())
            local_dependency_total = sum(local_dependencies.values())
            safely_mergeable = not partssoft_dependency_total or not local_dependency_total
            if partssoft_dependency_total and not local_dependency_total:
                recommended_keep = int(partssoft_order.id)
            elif local_dependency_total and not partssoft_dependency_total:
                recommended_keep = int(local_order.id)
            elif not partssoft_dependency_total and not local_dependency_total:
                recommended_keep = int(partssoft_order.id)
            else:
                recommended_keep = None
            pairs.append(
                {
                    "customer_id": int(partssoft_order.customer_id),
                    "partssoft_order_id": int(partssoft_order.id),
                    "partssoft_number": partssoft_order.order_number,
                    "partssoft_external_id": partssoft_order.external_order_id,
                    "email_order_id": int(local_order.id),
                    "email_number": local_order.order_number,
                    "match_basis": decision.basis,
                    "partssoft_total": str(_order_total(partssoft_order)),
                    "email_total": str(_order_total(local_order)),
                    "partssoft_dependencies": partssoft_dependencies,
                    "email_dependencies": local_dependencies,
                    "safe_to_merge_automatically": safely_mergeable,
                    "recommended_keep_order_id": recommended_keep,
                }
            )

    return {
        "read_only": True,
        "days": max(1, int(days)),
        "orders_checked": len(orders),
        "partssoft_orders_checked": len(partssoft_orders),
        "duplicate_pairs": len(pairs),
        "safe_pairs": sum(bool(row["safe_to_merge_automatically"]) for row in pairs),
        "review_required_pairs": sum(not bool(row["safe_to_merge_automatically"]) for row in pairs),
        "pairs": pairs,
    }


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=7)
    args = parser.parse_args()
    load_dotenv(".env")
    print(json.dumps(await build_report(args.days), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
