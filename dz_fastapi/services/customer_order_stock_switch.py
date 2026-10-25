"""Use received exact parts or confirmed Dragonzap crosses for open orders."""

from sqlalchemy import func, literal_column, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.api.validators import normalize_brand_name
from dz_fastapi.core.time import now_moscow
from dz_fastapi.models.autopart import AutoPart, preprocess_oem_number
from dz_fastapi.models.brand import Brand
from dz_fastapi.models.cross import AutoPartCross
from dz_fastapi.models.inventory import (
    CustomerOrderPickLabel,
    ReserveStatus,
    ShipmentDocument,
    ShipmentDocumentItem,
    ShipmentDocumentStatus,
    StockByLocation,
    StockDocument,
    StockDocumentStatus,
    StockLot,
    StockOrderPackageItem,
    StockReserve,
)
from dz_fastapi.models.partner import (
    CUSTOMER_ORDER_ITEM_STATUS,
    STOCK_ORDER_STATUS,
    CustomerOrder,
    CustomerOrderItem,
    CustomerPriceList,
    CustomerPriceListPublishedAlias,
    StockOrder,
    StockOrderItem,
    SupplierReceipt,
    SupplierReceiptItem,
)

SWITCH_KEY = "receipt_stock_source_switch"
SUGGESTION_KEY = "receipt_stock_source_suggestion"


async def _free_own_stock(session: AsyncSession, autopart_id: int,
                          *, lock: bool = True) -> int:
    """Match the own-stock label capacity; linked cross-docking lots are excluded."""
    balance_stmt = select(
        StockByLocation.storage_location_id, StockByLocation.quantity,
    ).where(StockByLocation.autopart_id == autopart_id,
            StockByLocation.quantity > 0)
    if lock:
        balance_stmt = balance_stmt.with_for_update()
    balances = (await session.execute(balance_stmt)).all()
    if not balances:
        return 0
    reserved_rows = (await session.execute(
        select(StockReserve.storage_location_id, func.sum(StockReserve.quantity))
        .where(StockReserve.autopart_id == autopart_id,
               StockReserve.status == ReserveStatus.ACTIVE)
        .group_by(StockReserve.storage_location_id)
    )).all()
    reserved = {location_id: int(qty) for location_id, qty in reserved_rows}
    cross_rows = (await session.execute(
        select(StockLot.storage_location_id, func.sum(StockLot.remaining_quantity))
        .join(SupplierReceiptItem,
              SupplierReceiptItem.id == StockLot.source_receipt_item_id)
        .where(StockLot.autopart_id == autopart_id,
               StockLot.remaining_quantity > 0,
               SupplierReceiptItem.customer_order_item_id.is_not(None))
        .group_by(StockLot.storage_location_id)
    )).all()
    cross = {location_id: int(qty) for location_id, qty in cross_rows}
    global_reserved = reserved.get(None, 0)
    return max(0, sum(
        max(0, int(qty) - reserved.get(location_id, 0)
            - cross.get(location_id, 0))
        for location_id, qty in balances
    ) - global_reserved)


async def _unlabelled_own_claims(
    session: AsyncSession, autopart_id: int,
    *, exclude_stock_item_id: int | None = None,
) -> int:
    """Count open allocations that have not become label-backed reserves."""
    reserved_for_row = (
        select(func.coalesce(func.sum(StockReserve.quantity), 0))
        .where(StockReserve.stock_order_item_id == StockOrderItem.id,
               StockReserve.status == ReserveStatus.ACTIVE)
        .correlate(StockOrderItem)
        .scalar_subquery()
    )
    stmt = (
        select(func.coalesce(func.sum(func.greatest(
            StockOrderItem.quantity - reserved_for_row, 0,
        )), 0))
        .join(StockOrder, StockOrder.id == StockOrderItem.stock_order_id)
        .join(CustomerOrderItem,
              CustomerOrderItem.id == StockOrderItem.customer_order_item_id)
        .join(CustomerOrder, CustomerOrder.id == CustomerOrderItem.order_id)
        .where(
            StockOrderItem.autopart_id == autopart_id,
            StockOrderItem.supplier_receipt_item_id.is_(None),
            StockOrder.status == STOCK_ORDER_STATUS.NEW,
            StockOrder.shipment_document_id.is_(None),
            CustomerOrderItem.status == CUSTOMER_ORDER_ITEM_STATUS.OWN_STOCK,
            CustomerOrder.deleted_at.is_(None),
        )
    )
    if exclude_stock_item_id is not None:
        stmt = stmt.where(StockOrderItem.id != exclude_stock_item_id)
    return int((await session.execute(stmt)).scalar_one())


async def _switch_blocker(session: AsyncSession, item: CustomerOrderItem,
                          stock_row: StockOrderItem,
                          stock_order: StockOrder,
                          *, allow_cancelled_labels: bool = False) -> str | None:
    if stock_order.status != STOCK_ORDER_STATUS.NEW:
        return "складской заказ уже закрыт"
    if stock_order.shipment_document_id is not None:
        return "складской заказ уже связан с реализацией"
    if int(stock_row.picked_quantity or 0) > 0 or (
        stock_row.picked_at and not allow_cancelled_labels
    ):
        return "сборка уже началась"
    label_stmt = select(CustomerOrderPickLabel.id).where(
        CustomerOrderPickLabel.stock_order_item_id == stock_row.id,
    )
    if allow_cancelled_labels:
        label_stmt = label_stmt.where(CustomerOrderPickLabel.cancelled_at.is_(None))
    if (await session.execute(label_stmt.limit(1))).scalar_one_or_none() is not None:
        return "этикетка уже печаталась"
    if (await session.execute(
        select(StockOrderPackageItem.id).where(
            StockOrderPackageItem.stock_order_item_id == stock_row.id,
        ).limit(1)
    )).scalar_one_or_none() is not None:
        return "позиция уже упакована"
    if (await session.execute(
        select(StockReserve.id).where(
            StockReserve.status == ReserveStatus.ACTIVE,
            (StockReserve.customer_order_item_id == item.id)
            | (StockReserve.stock_order_item_id == stock_row.id),
        ).limit(1)
    )).scalar_one_or_none() is not None:
        return "по позиции есть резерв"
    if (await session.execute(
        select(ShipmentDocumentItem.id).join(ShipmentDocument).where(
            ShipmentDocumentItem.customer_order_item_id == item.id,
            ShipmentDocument.status != ShipmentDocumentStatus.CANCELLED,
        ).limit(1)
    )).scalar_one_or_none() is not None:
        return "позиция уже включена в реализацию"
    if (await session.execute(
        select(SupplierReceiptItem.id).where(
            SupplierReceiptItem.customer_order_item_id == item.id,
        ).limit(1)
    )).scalar_one_or_none() is not None:
        return "по позиции уже есть связанный приход поставщика"
    return None


async def _allowed_dragonzap_crosses(
    session: AsyncSession, autopart_id: int,
) -> tuple[set[tuple[str, str]], set[tuple[str, str, int]]]:
    """Use the same confirmed catalogue and published aliases as order routing."""
    catalog_rows = (await session.execute(
        select(AutoPartCross.cross_oem_number, Brand.name)
        .join(Brand, Brand.id == AutoPartCross.cross_brand_id)
        .where(AutoPartCross.source_autopart_id == autopart_id)
    )).all()
    catalog = {
        (preprocess_oem_number(oem or ""), normalize_brand_name(brand or ""))
        for oem, brand in catalog_rows
    }
    alias_customers = (
        select(CustomerPriceList.customer_id)
        .join(CustomerPriceListPublishedAlias)
        .where(CustomerPriceListPublishedAlias.source_autopart_id == autopart_id)
        .distinct()
    )
    latest_published = (
        select(
            CustomerPriceList.id.label("pricelist_id"),
            CustomerPriceList.customer_id.label("customer_id"),
            func.row_number().over(
                partition_by=CustomerPriceList.customer_id,
                order_by=(CustomerPriceList.date.desc(),
                          CustomerPriceList.id.desc()),
            ).label("position"),
        )
        .where(or_(CustomerPriceList.published_at.is_not(None),
                   CustomerPriceList.sent_at.is_not(None)),
               CustomerPriceList.customer_id.in_(alias_customers))
        .subquery()
    )
    alias_rows = (await session.execute(
        select(CustomerPriceListPublishedAlias.normalized_oem,
               CustomerPriceListPublishedAlias.normalized_brand,
               latest_published.c.customer_id)
        .join(latest_published,
              latest_published.c.pricelist_id
              == CustomerPriceListPublishedAlias.customer_pricelist_id)
        .where(
            CustomerPriceListPublishedAlias.source_autopart_id == autopart_id,
            latest_published.c.position == 1,
        )
    )).all()
    published = {
        (preprocess_oem_number(oem or ""),
         normalize_brand_name(brand or ""), int(customer_id))
        for oem, brand, customer_id in alias_rows if customer_id is not None
    }
    return catalog, published


async def _source_document_is_posted(session: AsyncSession, notice: dict) -> bool:
    if notice.get("source_type") == "stock_document":
        doc = await session.get(StockDocument, notice.get("document_id"))
        return bool(doc and doc.status == StockDocumentStatus.POSTED)
    if notice.get("source_type") == "supplier_receipt":
        receipt = await session.get(SupplierReceipt, notice.get("document_id"))
        return bool(receipt and receipt.posted_at is not None)
    return False


async def switch_unpicked_crosses_after_receipt(
    session: AsyncSession, *, autopart_id: int, source_type: str,
    document_id: int, target_item_id: int | None = None,
    allow_cancelled_labels: bool = False,
    decided_by_user_id: int | None = None,
) -> list[dict]:
    """Switch to an exact part, or to a confirmed available Dragonzap cross.

    Called in the same transaction as posting the receipt. The row locks keep
    label printing and concurrent receipts from changing these rows mid-check.
    """
    part = (await session.execute(
        select(AutoPart, Brand.name).join(Brand).where(AutoPart.id == autopart_id)
    )).one_or_none()
    if part is None:
        return []
    direct_part, direct_brand = part
    normalized_oem = preprocess_oem_number(direct_part.oem_number or "")
    normalized_brand = normalize_brand_name(direct_brand or "")
    if not normalized_oem or not normalized_brand:
        return []
    catalog_crosses = set()
    published_crosses = set()
    if normalized_brand == "DRAGONZAP":
        catalog_crosses, published_crosses = await _allowed_dragonzap_crosses(
            session, autopart_id,
        )
    requested_oems = {normalized_oem}
    requested_oems.update(oem for oem, _ in catalog_crosses)
    requested_oems.update(oem for oem, _, _ in published_crosses)

    # Customer articles may contain dashes/spaces. The expression is indexed
    # by the accompanying Alembic migration.
    rows = (await session.execute(
        select(StockOrderItem, CustomerOrderItem, StockOrder,
               CustomerOrder.customer_id)
        .join(CustomerOrderItem,
              CustomerOrderItem.id == StockOrderItem.customer_order_item_id)
        .join(CustomerOrder, CustomerOrder.id == CustomerOrderItem.order_id)
        .join(StockOrder, StockOrder.id == StockOrderItem.stock_order_id)
        .where(
            CustomerOrder.deleted_at.is_(None),
            CustomerOrderItem.status == CUSTOMER_ORDER_ITEM_STATUS.OWN_STOCK,
            CustomerOrderItem.autopart_id != autopart_id,
            StockOrder.status == STOCK_ORDER_STATUS.NEW,
            StockOrder.shipment_document_id.is_(None),
            StockOrderItem.supplier_receipt_item_id.is_(None),
            func.regexp_replace(
                func.upper(CustomerOrderItem.oem),
                literal_column("'[^A-Z0-9]'"), literal_column("''"),
                literal_column("'g'"),
            ).in_(requested_oems),
        )
        .order_by(CustomerOrder.received_at, CustomerOrderItem.id)
        .with_for_update(of=(StockOrderItem, CustomerOrderItem), skip_locked=True)
    )).all()
    free = max(
        0, await _free_own_stock(session, autopart_id)
        - await _unlabelled_own_claims(session, autopart_id),
    )
    outcomes = []
    seen_items = set()
    for stock_row, item, stock_order, customer_id in rows:
        if target_item_id is not None and item.id != target_item_id:
            continue
        if item.id in seen_items:
            continue
        seen_items.add(item.id)
        requested_key = (
            preprocess_oem_number(item.oem or ""),
            normalize_brand_name(item.brand or ""),
        )
        if requested_key == (normalized_oem, normalized_brand):
            basis = "exact"
        elif requested_key in catalog_crosses:
            basis = "confirmed_dragonzap_cross"
        elif (*requested_key, int(customer_id)) in published_crosses:
            basis = "published_dragonzap_cross"
        else:
            continue
        payload = dict(item.source_payload or {})
        active_notice = payload.get(SWITCH_KEY) or {}
        pending_notice = payload.get(SUGGESTION_KEY) or {}
        if target_item_id is None and any(
            notice.get("status") == "kept"
            and notice.get("autopart_id") == autopart_id
            for notice in (active_notice, pending_notice)
        ):
            continue
        if stock_row.autopart_id != item.autopart_id:
            continue
        other_row = (await session.execute(
            select(StockOrderItem.id).where(
                StockOrderItem.customer_order_item_id == item.id,
                StockOrderItem.id != stock_row.id,
            ).limit(1)
        )).scalar_one_or_none()
        if other_row is not None:
            continue
        needed = int(stock_row.quantity or 0)
        if needed <= 0 or needed != int(item.ship_qty or 0):
            continue
        selected_is_exact = (
            preprocess_oem_number(item.actual_oem or ""),
            normalize_brand_name(item.actual_brand or ""),
        ) == requested_key
        if basis != "exact" or selected_is_exact:
            # Preserve a usable selected source. A newly received exact part
            # still has priority over a selected cross.
            current_free = max(
                0, await _free_own_stock(
                    session, item.autopart_id, lock=False,
                ) - await _unlabelled_own_claims(
                    session, item.autopart_id,
                    exclude_stock_item_id=stock_row.id,
                ),
            )
            if current_free >= needed:
                continue
        reason = await _switch_blocker(
            session, item, stock_row, stock_order,
            allow_cancelled_labels=allow_cancelled_labels,
        )
        if reason is None and free < needed:
            reason = "свободного остатка альтернативы пока недостаточно"
        notice = {
            "status": "blocked" if reason else "switched",
            "reason": reason,
            "basis": basis,
            "source_type": source_type,
            "document_id": document_id,
            "autopart_id": autopart_id,
            "oem": direct_part.oem_number,
            "brand": direct_brand,
            "checked_at": now_moscow().isoformat(),
        }
        if decided_by_user_id is not None:
            notice["decided_by_user_id"] = decided_by_user_id
        if reason is None:
            notice["previous"] = {
                "autopart_id": item.autopart_id,
                "match_type": item.match_type,
                "actual_oem": item.actual_oem,
                "actual_brand": item.actual_brand,
                "actual_name": item.actual_name,
                "source_resolution_status": item.source_resolution_status,
                "preferred_stock_lot_id": stock_row.preferred_stock_lot_id,
            }
            if active_notice.get("status") == "switched":
                notice["previous"]["source_switch"] = active_notice
            stock_row.autopart_id = autopart_id
            stock_row.preferred_stock_lot_id = None
            item.autopart_id = autopart_id
            if basis == "exact":
                item.match_type = "receipt_exact"
            item.actual_oem = direct_part.oem_number
            item.actual_brand = direct_brand
            item.actual_name = direct_part.name
            item.source_resolution_status = (
                "receipt_exact_stock" if basis == "exact"
                else "receipt_dragonzap_cross"
            )
            free -= needed
            payload[SWITCH_KEY] = notice
            payload.pop(SUGGESTION_KEY, None)
        elif active_notice.get("status") == "switched":
            payload[SUGGESTION_KEY] = notice
        else:
            payload[SWITCH_KEY] = notice
        item.source_payload = payload
        outcomes.append({
            "order_id": item.order_id, "item_id": item.id,
            "status": notice["status"], "reason": reason,
            "oem": direct_part.oem_number, "basis": basis,
        })
    await session.flush()
    return outcomes


async def restore_unpicked_switches_for_unpost(
    session: AsyncSession, *, source_type: str, document_id: int,
) -> None:
    """Undo source changes when their receipt is unposted before assembly."""
    active_source = CustomerOrderItem.source_payload[SWITCH_KEY]
    pending_source = CustomerOrderItem.source_payload[SUGGESTION_KEY]
    rows = (await session.execute(
        select(CustomerOrderItem).where(
            or_(
                (active_source["source_type"].as_string() == source_type)
                & (active_source["document_id"].as_integer() == document_id),
                (pending_source["source_type"].as_string() == source_type)
                & (pending_source["document_id"].as_integer() == document_id),
            ),
        ).with_for_update(of=CustomerOrderItem)
    )).scalars().all()
    for item in rows:
        payload = dict(item.source_payload or {})
        pending = payload.get(SUGGESTION_KEY) or {}
        if (pending.get("source_type") == source_type
                and pending.get("document_id") == document_id):
            payload.pop(SUGGESTION_KEY, None)
        switch = payload.get(SWITCH_KEY) or {}
        if (switch.get("source_type") != source_type
                or switch.get("document_id") != document_id):
            item.source_payload = payload
            continue
        if switch.get("status") != "switched":
            payload.pop(SWITCH_KEY, None)
            item.source_payload = payload
            continue
        stock_rows = (await session.execute(
            select(StockOrderItem, StockOrder)
            .join(StockOrder)
            .where(StockOrderItem.customer_order_item_id == item.id,
                   StockOrderItem.supplier_receipt_item_id.is_(None))
            .with_for_update(of=StockOrderItem)
        )).all()
        if len(stock_rows) != 1:
            raise ValueError("Нельзя распровести: складская строка заказа изменилась")
        stock_row, stock_order = stock_rows[0]
        if (item.source_resolution_status not in (
                "receipt_exact_stock", "receipt_dragonzap_cross")
                or item.autopart_id != switch["autopart_id"]
                or stock_row.autopart_id != switch["autopart_id"]):
            raise ValueError("Нельзя распровести: источник позиции заказа изменился")
        reason = await _switch_blocker(
            session, item, stock_row, stock_order,
            allow_cancelled_labels=True,
        )
        if reason:
            raise ValueError(
                f"Нельзя распровести: заказ #{item.order_id}, "
                f"позиция #{item.id}: {reason}"
            )
        previous = switch["previous"]
        previous_switch = previous.get("source_switch") or {}
        while previous_switch.get("status") == "switched":
            prior_id = int(previous["autopart_id"])
            if (await _source_document_is_posted(session, previous_switch)
                    and await _free_own_stock(session, prior_id, lock=False)
                    - await _unlabelled_own_claims(session, prior_id)
                    >= int(stock_row.quantity or 0)):
                payload[SWITCH_KEY] = previous_switch
                break
            previous = previous_switch["previous"]
            previous_switch = previous.get("source_switch") or {}
        if previous_switch.get("status") != "switched":
            payload.pop(SWITCH_KEY, None)
        stock_row.autopart_id = previous["autopart_id"]
        stock_row.preferred_stock_lot_id = previous.get("preferred_stock_lot_id")
        item.autopart_id = previous["autopart_id"]
        item.match_type = previous["match_type"]
        item.actual_oem = previous["actual_oem"]
        item.actual_brand = previous["actual_brand"]
        item.actual_name = previous["actual_name"]
        item.source_resolution_status = previous["source_resolution_status"]
        item.source_payload = payload
    await session.flush()
