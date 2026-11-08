"""Повторное сопоставление строк поступления с заказами поставщику.

Нужно для документов, которые были проведены, когда подходящий заказ ещё
не находился. Связь ставится только с позициями складских заказов (без
привязки к заказу клиента): привязка к клиентскому заказу меняет сборку и
этикетки, такие позиции сотрудник обрабатывает вручную.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from dz_fastapi.models.partner import (
    SupplierOrder,
    SupplierOrderItem,
    SupplierReceipt,
    SupplierReceiptItem,
)
from dz_fastapi.services.supplier_order_responses import (
    ParsedSupplierResponseRow,
    _config_priority_brand_keys,
    _get_supplier_response_lookback_days,
    _load_brand_alias_map,
    _load_recent_provider_orders,
    _normalize_key,
    _normalize_oem_key,
    _select_row_item_candidate,
    supplier_response_cutoff,
)
from dz_fastapi.services.supplier_workflow import _recalculate_supplier_order_items_received


def _remaining(item: SupplierOrderItem) -> int:
    accepted = (
        int(item.confirmed_quantity)
        if item.confirmed_quantity is not None
        else int(item.quantity or 0)
    )
    return max(accepted - int(item.received_quantity or 0), 0)


async def relink_receipt_to_orders(
    session: AsyncSession,
    *,
    receipt_id: int,
    apply: bool = False,
    include_customer_linked: bool = False,
) -> dict[str, Any]:
    receipt = (
        await session.execute(
            select(SupplierReceipt)
            .options(selectinload(SupplierReceipt.items))
            .where(SupplierReceipt.id == receipt_id)
        )
    ).scalar_one_or_none()
    if receipt is None:
        raise LookupError("Документ поступления не найден")

    result: dict[str, Any] = {
        "receipt_id": int(receipt.id),
        "document_number": receipt.document_number,
        "matches": [],
        "unmatched": [],
        "skipped_customer_linked": 0,
        "applied": False,
    }
    unlinked = [
        item
        for item in (receipt.items or [])
        if item.supplier_order_item_id is None
        and int(item.received_quantity or 0) > 0
    ]
    if not unlinked:
        return result

    lookback = await _get_supplier_response_lookback_days(session)
    reference = receipt.document_date or (
        receipt.created_at.date() if receipt.created_at else None
    )
    cutoff = supplier_response_cutoff(days=lookback)
    date_from = min(reference, cutoff) if reference else cutoff
    orders = await _load_recent_provider_orders(
        session, provider_id=int(receipt.provider_id), date_from=date_from
    )
    brand_aliases = await _load_brand_alias_map(session)
    exact_map: dict[tuple[str, str], list[SupplierOrderItem]] = {}
    oem_map: dict[str, list[SupplierOrderItem]] = {}
    pool: dict[int, SupplierOrderItem] = {}
    for order in orders:
        for order_item in order.items or []:
            if _remaining(order_item) <= 0:
                continue
            if (
                order_item.customer_order_item_id is not None
                and not include_customer_linked
            ):
                result["skipped_customer_linked"] += 1
                continue
            pool[int(order_item.id)] = order_item
            exact_map.setdefault(
                _normalize_key(
                    order_item.oem_number, order_item.brand_name, brand_aliases
                ),
                [],
            ).append(order_item)
            oem_key = _normalize_oem_key(order_item.oem_number)
            if oem_key:
                oem_map.setdefault(oem_key, []).append(order_item)

    allocated: dict[int, int] = {}
    changed_order_item_ids: set[int] = set()
    new_rows: list[SupplierReceiptItem] = []
    for item in unlinked:
        qty = int(item.received_quantity or 0)
        row = ParsedSupplierResponseRow(
            oem_number=str(item.oem_number or ""),
            brand_name=item.brand_name,
            confirmed_quantity=qty,
            response_price=None,
            response_comment=None,
            response_status_raw=None,
        )
        matched = _select_row_item_candidate(
            row=row,
            exact_map=exact_map,
            oem_map=oem_map,
            brand_aliases=brand_aliases,
            priority_brand_keys=_config_priority_brand_keys(
                None, brand_aliases
            ),
        )
        label = f"{item.oem_number} ({item.brand_name or '—'})"
        if matched is None:
            result["unmatched"].append(label)
            continue
        room = _remaining(matched) - allocated.get(int(matched.id), 0)
        take = min(qty, room)
        if take <= 0:
            result["unmatched"].append(label)
            continue
        allocated[int(matched.id)] = allocated.get(int(matched.id), 0) + take
        result["matches"].append(
            {
                "oem_number": item.oem_number,
                "brand_name": item.brand_name,
                "received_quantity": take,
                "excess": qty - take,
                "supplier_order_id": int(matched.supplier_order_id),
                "supplier_order_item_id": int(matched.id),
                "customer_linked": matched.customer_order_item_id is not None,
            }
        )
        if matched.customer_order_item_id is not None:
            # Привязка к клиентскому заказу меняет сборку и этикетки: только
            # предпросмотр, применяется отдельным безопасным процессом.
            continue
        if not apply:
            continue
        if take < qty:
            data = {
                column.name: getattr(item, column.name)
                for column in SupplierReceiptItem.__table__.columns
                if column.name != "id"
            }
            total = item.total_price_with_vat
            data["received_quantity"] = qty - take
            if total is not None:
                data["total_price_with_vat"] = round(
                    float(total) * (qty - take) / qty, 2
                )
                item.total_price_with_vat = round(
                    float(total) * take / qty, 2
                )
            new_rows.append(SupplierReceiptItem(**data))
            item.received_quantity = take
        item.supplier_order_item_id = int(matched.id)
        item.supplier_order_id = int(matched.supplier_order_id)
        item.ordered_quantity = int(matched.quantity or 0)
        item.confirmed_quantity = matched.confirmed_quantity
        session.add(item)
        changed_order_item_ids.add(int(matched.id))

    if apply and changed_order_item_ids:
        for new_row in new_rows:
            session.add(new_row)
        await session.flush()
        if receipt.posted_at is not None:
            await _recalculate_supplier_order_items_received(
                session, order_item_ids=changed_order_item_ids
            )
        await session.commit()
        result["applied"] = True
    return result


async def find_receipts_to_relink(
    session: AsyncSession,
    *,
    provider_id: Optional[int] = None,
    limit: int = 200,
    since: Optional[datetime] = None,
) -> list[int]:
    """Поступления, в которых есть строки без привязки к заказу."""
    stmt = (
        select(SupplierReceipt.id)
        .join(
            SupplierReceiptItem,
            SupplierReceiptItem.receipt_id == SupplierReceipt.id,
        )
        .where(
            SupplierReceiptItem.supplier_order_item_id.is_(None),
            SupplierReceiptItem.received_quantity > 0,
        )
        .group_by(SupplierReceipt.id)
        .order_by(SupplierReceipt.id.desc())
        .limit(limit)
    )
    if provider_id is not None:
        stmt = stmt.where(SupplierReceipt.provider_id == provider_id)
    if since is not None:
        stmt = stmt.where(SupplierReceipt.created_at >= since)
        # Частый автозапуск: берём только поставщиков, у которых есть
        # складские позиции заказов с неполученным остатком.
        open_item = (
            select(SupplierOrderItem.id)
            .join(
                SupplierOrder,
                SupplierOrder.id == SupplierOrderItem.supplier_order_id,
            )
            .where(
                SupplierOrder.provider_id == SupplierReceipt.provider_id,
                SupplierOrderItem.customer_order_item_id.is_(None),
                func.coalesce(SupplierOrderItem.received_quantity, 0)
                < func.coalesce(
                    SupplierOrderItem.confirmed_quantity,
                    SupplierOrderItem.quantity,
                ),
            )
            .exists()
        )
        stmt = stmt.where(open_item)
    return [int(row) for row in (await session.execute(stmt)).scalars().all()]
