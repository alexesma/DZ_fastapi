from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Iterable, Optional

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload, selectinload

from dz_fastapi.core.time import now_moscow
from dz_fastapi.crud.customer_order import crud_supplier_order
from dz_fastapi.models.partner import (STOCK_ORDER_STATUS, CustomerOrder,
                                       CustomerOrderItem, StockOrder,
                                       StockOrderItem, SupplierOrderItem,
                                       SupplierReceipt, SupplierReceiptItem)
from dz_fastapi.models.user import User
from dz_fastapi.services.customer_orders import \
    try_finalize_customer_order_response


@dataclass(slots=True)
class StockPickResult:
    item: StockOrderItem
    stock_order_status: STOCK_ORDER_STATUS


def get_default_supplier_activity_window(
    current_date: Optional[date] = None,
) -> tuple[date, date]:
    today = current_date or now_moscow().date()
    days_back = 3 if today.weekday() == 0 else 1
    return today - timedelta(days=days_back), today


async def update_stock_order_item_pick(
    session: AsyncSession,
    *,
    item_id: int,
    user: User,
    picked_quantity: Optional[int] = None,
    increment: Optional[int] = None,
    pick_comment: Optional[str] = None,
    scan_code: Optional[str] = None,
) -> StockPickResult:
    if picked_quantity is None and increment is None:
        raise ValueError('Укажите количество или шаг изменения')
    if picked_quantity is not None and increment is not None:
        raise ValueError('Нельзя одновременно передавать количество и шаг')

    stmt = (
        select(StockOrderItem)
        .options(
            joinedload(StockOrderItem.stock_order)
            .selectinload(StockOrder.items),
            joinedload(StockOrderItem.picked_by_user),
        )
        .where(StockOrderItem.id == item_id)
    )
    item = (await session.execute(stmt)).scalar_one_or_none()
    if item is None:
        raise LookupError('Строка складского заказа не найдена')

    current_qty = int(item.picked_quantity or 0)
    if picked_quantity is not None:
        next_qty = min(max(int(picked_quantity), 0), int(item.quantity or 0))
    else:
        next_qty = min(
            current_qty + int(increment or 0),
            int(item.quantity or 0),
        )

    item.picked_quantity = next_qty
    if pick_comment is not None:
        item.pick_comment = pick_comment or None
    if scan_code:
        item.pick_last_scan_code = scan_code
    if next_qty > 0:
        item.picked_at = now_moscow()
        item.picked_by_user_id = user.id
    else:
        item.picked_at = None
        item.picked_by_user_id = None
        if scan_code:
            item.pick_last_scan_code = None

    stock_order = item.stock_order
    if stock_order:
        if all(
            int(order_item.picked_quantity or 0)
            >= int(order_item.quantity or 0)
            for order_item in (stock_order.items or [])
        ):
            stock_order.status = STOCK_ORDER_STATUS.COMPLETED
        else:
            stock_order.status = STOCK_ORDER_STATUS.NEW

    await session.commit()
    refreshed_item = (
        await session.execute(
            select(StockOrderItem)
            .options(
                joinedload(StockOrderItem.stock_order).selectinload(
                    StockOrder.items
                ),
                joinedload(StockOrderItem.picked_by_user),
            )
            .where(StockOrderItem.id == item.id)
        )
    ).scalar_one()
    if (
        refreshed_item.picked_by_user is None
        and refreshed_item.picked_by_user_id == user.id
    ):
        refreshed_item.picked_by_user = user

    customer_order_item_id = getattr(
        refreshed_item,
        "customer_order_item_id",
        None,
    )
    if customer_order_item_id is not None:
        customer_order_id = (
            await session.execute(
                select(CustomerOrderItem.order_id).where(
                    CustomerOrderItem.id == customer_order_item_id
                )
            )
        ).scalar_one_or_none()
        if customer_order_id is not None:
            try:
                await try_finalize_customer_order_response(
                    session,
                    order_id=int(customer_order_id),
                )
            except Exception:
                # Do not block picking flow on auto-reply checks.
                pass

    return StockPickResult(
        item=refreshed_item,
        stock_order_status=(
            refreshed_item.stock_order.status
            if refreshed_item.stock_order
            else STOCK_ORDER_STATUS.NEW
        ),
    )


def serialize_stock_order_item(item: StockOrderItem) -> dict:
    return {
        'id': item.id,
        'autopart_id': item.autopart_id,
        'customer_order_item_id': item.customer_order_item_id,
        'quantity': item.quantity,
        'picked_quantity': int(item.picked_quantity or 0),
        'picked_at': item.picked_at,
        'picked_by_user_id': item.picked_by_user_id,
        'picked_by_email': (
            item.picked_by_user.email if item.picked_by_user else None
        ),
        'pick_comment': item.pick_comment,
        'pick_last_scan_code': item.pick_last_scan_code,
        'autopart': item.autopart,
    }


def serialize_stock_order(order: StockOrder) -> dict:
    return {
        'id': order.id,
        'customer_id': order.customer_id,
        'customer_name': order.customer.name if order.customer else None,
        'status': order.status,
        'created_at': order.created_at,
        'items': [serialize_stock_order_item(item) for item in order.items],
    }


async def list_supplier_receipt_candidates(
    session: AsyncSession,
    *,
    provider_id: Optional[int] = None,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
) -> list[dict]:
    if date_from is None or date_to is None:
        default_from, default_to = get_default_supplier_activity_window()
        date_from = date_from or default_from
        date_to = date_to or default_to

    orders = await crud_supplier_order.list_supplier_orders(
        session=session,
        provider_id=provider_id,
        date_from=date_from,
        date_to=date_to,
        use_sent_at_for_period=True,
        limit=500,
    )
    rows: list[dict] = []
    for order in orders:
        provider_name = getattr(order.provider, 'name', None)
        for item in order.items or []:
            if item.receipt_items:
                continue
            customer_order = None
            customer = None
            if item.customer_order_item and item.customer_order_item.order:
                customer_order = item.customer_order_item.order
                customer = customer_order.customer
            latest_receipt = None
            if item.receipt_items:
                latest_receipt = max(
                    item.receipt_items,
                    key=lambda receipt_item: (
                        receipt_item.receipt.posted_at
                        or receipt_item.receipt.created_at
                    ),
                )
            already_received = int(item.received_quantity or 0)
            expected_quantity = (
                int(item.confirmed_quantity)
                if item.confirmed_quantity is not None
                else int(item.quantity or 0)
            )
            pending_quantity = max(expected_quantity - already_received, 0)
            if pending_quantity <= 0:
                continue
            rows.append(
                {
                    'supplier_order_item_id': item.id,
                    'supplier_order_id': order.id,
                    'provider_id': order.provider_id,
                    'provider_name': provider_name,
                    'supplier_order_created_at': order.created_at,
                    'supplier_order_sent_at': order.sent_at,
                    'supplier_order_status': order.status,
                    'customer_order_id': (
                        customer_order.id if customer_order else None
                    ),
                    'customer_order_number': (
                        customer_order.order_number
                        if customer_order
                        else None
                    ),
                    'customer_name': customer.name if customer else None,
                    'oem_number': item.oem_number,
                    'brand_name': item.brand_name,
                    'autopart_name': item.autopart_name,
                    'ordered_quantity': int(item.quantity or 0),
                    'confirmed_quantity': item.confirmed_quantity,
                    'already_received_quantity': already_received,
                    'pending_quantity': pending_quantity,
                    'price': item.price,
                    'response_price': item.response_price,
                    'response_comment': item.response_comment,
                    'response_status_raw': (
                        item.response_status_raw
                        or order.response_status_raw
                    ),
                    'response_status_normalized': (
                        item.response_status_normalized
                        or order.response_status_normalized
                    ),
                    'min_delivery_day': item.min_delivery_day,
                    'max_delivery_day': item.max_delivery_day,
                    'last_receipt_at': (
                        latest_receipt.receipt.posted_at
                        if latest_receipt
                        else None
                    ),
                    'last_receipt_number': (
                        latest_receipt.receipt.document_number
                        if latest_receipt
                        else None
                    ),
                }
            )
    rows.sort(
        key=lambda row: (
            row['supplier_order_created_at'],
            row['supplier_order_id'],
            row['supplier_order_item_id'],
        ),
        reverse=True,
    )
    return rows


async def list_supplier_receipts(
    session: AsyncSession,
    *,
    provider_id: Optional[int] = None,
    posted: Optional[bool] = None,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
) -> list[SupplierReceipt]:
    stmt = (
        select(SupplierReceipt)
        .options(
            joinedload(SupplierReceipt.provider),
            joinedload(SupplierReceipt.created_by_user),
            selectinload(SupplierReceipt.items),
        )
        .order_by(SupplierReceipt.created_at.desc(), SupplierReceipt.id.desc())
    )
    if provider_id is not None:
        stmt = stmt.where(SupplierReceipt.provider_id == provider_id)
    if posted is True:
        stmt = stmt.where(SupplierReceipt.posted_at.is_not(None))
    elif posted is False:
        stmt = stmt.where(SupplierReceipt.posted_at.is_(None))
    if date_from is not None:
        stmt = stmt.where(
            SupplierReceipt.created_at
            >= datetime.combine(date_from, datetime.min.time())
        )
    if date_to is not None:
        stmt = stmt.where(
            SupplierReceipt.created_at
            <= datetime.combine(date_to, datetime.max.time())
        )
    return (await session.execute(stmt)).scalars().unique().all()


async def create_supplier_receipt(
    session: AsyncSession,
    *,
    user: User,
    provider_id: int,
    items_payload: Iterable[dict],
    post_now: bool = False,
    document_number: Optional[str] = None,
    document_date: Optional[date] = None,
    comment: Optional[str] = None,
) -> SupplierReceipt:
    items_payload = list(items_payload)
    if not items_payload:
        raise ValueError('Добавьте строки для поступления')

    item_ids = [int(item['supplier_order_item_id']) for item in items_payload]
    stmt = (
        select(SupplierOrderItem)
        .options(
            joinedload(SupplierOrderItem.supplier_order),
            joinedload(SupplierOrderItem.customer_order_item)
            .joinedload(CustomerOrderItem.order),
        )
        .where(SupplierOrderItem.id.in_(item_ids))
    )
    db_items = (await session.execute(stmt)).scalars().all()
    items_by_id = {item.id: item for item in db_items}
    if len(items_by_id) != len(set(item_ids)):
        raise LookupError('Не все строки заказов поставщикам найдены')

    supplier_order_ids: set[int] = set()
    affected_customer_order_ids: set[int] = set()
    if post_now:
        receipt = SupplierReceipt(
            provider_id=provider_id,
            supplier_order_id=None,
            document_number=document_number or None,
            document_date=document_date or now_moscow().date(),
            created_by_user_id=user.id,
            created_at=now_moscow(),
            posted_at=now_moscow(),
            comment=comment,
        )
        session.add(receipt)
        await session.flush()
    else:
        open_stmt = (
            select(SupplierReceipt)
            .where(
                SupplierReceipt.provider_id == provider_id,
                SupplierReceipt.posted_at.is_(None),
            )
            .order_by(
                SupplierReceipt.created_at.desc(),
                SupplierReceipt.id.desc(),
            )
        )
        receipt = (await session.execute(open_stmt)).scalars().first()
        if receipt is None:
            receipt = SupplierReceipt(
                provider_id=provider_id,
                supplier_order_id=None,
                document_number=document_number or None,
                document_date=document_date or now_moscow().date(),
                created_by_user_id=user.id,
                created_at=now_moscow(),
                posted_at=None,
                comment=comment,
            )
            session.add(receipt)
            await session.flush()
        else:
            if document_number:
                receipt.document_number = document_number
            if document_date:
                receipt.document_date = document_date
            if comment:
                receipt.comment = comment
            if receipt.created_by_user_id is None:
                receipt.created_by_user_id = user.id

    for payload in items_payload:
        item = items_by_id[int(payload['supplier_order_item_id'])]
        if item.supplier_order.provider_id != provider_id:
            raise ValueError(
                'Все строки должны относиться к одному поставщику'
            )
        supplier_order_ids.add(int(item.supplier_order_id))
        if item.customer_order_item and item.customer_order_item.order:
            affected_customer_order_ids.add(
                int(item.customer_order_item.order.id)
            )
        received_quantity = int(payload.get('received_quantity') or 0)
        if received_quantity > int(item.quantity or 0):
            raise ValueError(
                f'Полученное количество превышает заказанное для OEM '
                f'{item.oem_number or item.autopart_name or item.id}'
            )

        current_received = int(item.received_quantity or 0)
        expected_quantity = (
            int(item.confirmed_quantity)
            if item.confirmed_quantity is not None
            else int(item.quantity or 0)
        )
        pending_quantity = max(expected_quantity - current_received, 0)
        received_quantity = min(received_quantity, pending_quantity)
        if received_quantity <= 0:
            continue
        item.received_quantity = current_received + received_quantity
        item.received_at = now_moscow()

        receipt_item = SupplierReceiptItem(
            receipt_id=receipt.id,
            supplier_order_id=item.supplier_order_id,
            supplier_order_item_id=item.id,
            customer_order_item_id=item.customer_order_item_id,
            autopart_id=item.autopart_id,
            oem_number=item.oem_number,
            brand_name=item.brand_name,
            autopart_name=item.autopart_name,
            ordered_quantity=item.quantity,
            confirmed_quantity=item.confirmed_quantity,
            received_quantity=received_quantity,
            price=item.response_price or item.price,
            comment=payload.get('comment') or None,
        )
        session.add(receipt_item)

    if len(supplier_order_ids) == 1:
        receipt.supplier_order_id = next(iter(supplier_order_ids))

    receipt_id = int(receipt.id)
    await session.commit()

    for customer_order_id in sorted(affected_customer_order_ids):
        try:
            await try_finalize_customer_order_response(
                session,
                order_id=customer_order_id,
            )
        except Exception:
            await session.rollback()

    stmt = (
        select(SupplierReceipt)
        .options(
            joinedload(SupplierReceipt.provider),
            joinedload(SupplierReceipt.created_by_user),
            selectinload(SupplierReceipt.items),
        )
        .where(SupplierReceipt.id == receipt_id)
    )
    return (await session.execute(stmt)).scalar_one()


async def post_supplier_receipt(
    session: AsyncSession,
    *,
    receipt_id: int,
    user: User,
) -> SupplierReceipt:
    stmt = (
        select(SupplierReceipt)
        .options(
            joinedload(SupplierReceipt.provider),
            joinedload(SupplierReceipt.created_by_user),
            selectinload(SupplierReceipt.items).joinedload(
                SupplierReceiptItem.supplier_order_item
            ),
        )
        .where(SupplierReceipt.id == receipt_id)
    )
    receipt = (await session.execute(stmt)).scalar_one_or_none()
    if receipt is None:
        raise LookupError('Документ поступления не найден')

    if receipt.posted_at is None:
        current_dt = now_moscow()
        order_item_ids = [
            int(receipt_item.supplier_order_item_id)
            for receipt_item in (receipt.items or [])
            if receipt_item.supplier_order_item_id is not None
        ]
        other_receipts_totals: dict[int, int] = {}
        if order_item_ids:
            totals_stmt = (
                select(
                    SupplierReceiptItem.supplier_order_item_id,
                    func.coalesce(
                        func.sum(SupplierReceiptItem.received_quantity),
                        0,
                    ),
                )
                .where(
                    SupplierReceiptItem.supplier_order_item_id.in_(
                        order_item_ids
                    ),
                    SupplierReceiptItem.receipt_id != receipt.id,
                )
                .group_by(SupplierReceiptItem.supplier_order_item_id)
            )
            totals_rows = (await session.execute(totals_stmt)).all()
            other_receipts_totals = {
                int(item_id): int(total_qty or 0)
                for item_id, total_qty in totals_rows
                if item_id is not None
            }

        for receipt_item in receipt.items or []:
            order_item = receipt_item.supplier_order_item
            if order_item is None:
                continue
            requested_quantity = int(receipt_item.received_quantity or 0)
            if requested_quantity <= 0:
                continue
            expected_quantity = (
                int(order_item.confirmed_quantity)
                if order_item.confirmed_quantity is not None
                else int(order_item.quantity or 0)
            )
            other_total = other_receipts_totals.get(int(order_item.id), 0)
            target_total = min(
                expected_quantity,
                max(other_total + requested_quantity, 0),
            )
            current_received = int(order_item.received_quantity or 0)
            if current_received >= target_total:
                continue
            order_item.received_quantity = target_total
            order_item.received_at = current_dt

        receipt.posted_at = current_dt
        if receipt.created_by_user_id is None:
            receipt.created_by_user_id = user.id
        await session.commit()

    refresh_stmt = (
        select(SupplierReceipt)
        .options(
            joinedload(SupplierReceipt.provider),
            joinedload(SupplierReceipt.created_by_user),
            selectinload(SupplierReceipt.items),
        )
        .where(SupplierReceipt.id == receipt.id)
    )
    return (await session.execute(refresh_stmt)).scalar_one()


async def _recalculate_supplier_order_items_received(
    session: AsyncSession,
    *,
    order_item_ids: set[int],
) -> None:
    if not order_item_ids:
        return
    item_ids = {int(item_id) for item_id in order_item_ids if item_id}
    if not item_ids:
        return

    order_items = (
        await session.execute(
            select(SupplierOrderItem).where(SupplierOrderItem.id.in_(item_ids))
        )
    ).scalars().all()
    if not order_items:
        return

    totals_rows = (
        await session.execute(
            select(
                SupplierReceiptItem.supplier_order_item_id,
                func.coalesce(
                    func.sum(SupplierReceiptItem.received_quantity),
                    0,
                ),
            )
            .where(SupplierReceiptItem.supplier_order_item_id.in_(item_ids))
            .group_by(SupplierReceiptItem.supplier_order_item_id)
        )
    ).all()
    totals_by_item_id = {
        int(item_id): int(total_qty or 0)
        for item_id, total_qty in totals_rows
        if item_id is not None
    }
    current_dt = now_moscow()
    for item in order_items:
        expected_quantity = (
            int(item.confirmed_quantity)
            if item.confirmed_quantity is not None
            else int(item.quantity or 0)
        )
        expected_quantity = max(expected_quantity, 0)
        target_received = min(
            max(totals_by_item_id.get(int(item.id), 0), 0),
            expected_quantity,
        )
        item.received_quantity = target_received
        item.received_at = current_dt if target_received > 0 else None


async def unpost_supplier_receipt(
    session: AsyncSession,
    *,
    receipt_id: int,
    user: User,
) -> SupplierReceipt:
    stmt = (
        select(SupplierReceipt)
        .options(
            joinedload(SupplierReceipt.provider),
            joinedload(SupplierReceipt.created_by_user),
            selectinload(SupplierReceipt.items),
        )
        .where(SupplierReceipt.id == receipt_id)
    )
    receipt = (await session.execute(stmt)).scalar_one_or_none()
    if receipt is None:
        raise LookupError('Документ поступления не найден')

    if receipt.posted_at is not None:
        receipt.posted_at = None
        if receipt.created_by_user_id is None:
            receipt.created_by_user_id = user.id
        await session.commit()

    refresh_stmt = (
        select(SupplierReceipt)
        .options(
            joinedload(SupplierReceipt.provider),
            joinedload(SupplierReceipt.created_by_user),
            selectinload(SupplierReceipt.items),
        )
        .where(SupplierReceipt.id == receipt.id)
    )
    return (await session.execute(refresh_stmt)).scalar_one()


async def delete_supplier_receipt(
    session: AsyncSession,
    *,
    receipt_id: int,
) -> None:
    stmt = (
        select(SupplierReceipt)
        .options(selectinload(SupplierReceipt.items))
        .where(SupplierReceipt.id == receipt_id)
    )
    receipt = (await session.execute(stmt)).scalar_one_or_none()
    if receipt is None:
        raise LookupError('Документ поступления не найден')
    if receipt.posted_at is not None:
        raise ValueError(
            'Нельзя удалить проведённый документ. Сначала распроведите его.'
        )

    affected_order_item_ids = {
        int(item.supplier_order_item_id)
        for item in (receipt.items or [])
        if item.supplier_order_item_id is not None
    }
    await session.delete(receipt)
    await session.flush()
    await _recalculate_supplier_order_items_received(
        session,
        order_item_ids=affected_order_item_ids,
    )
    await session.commit()


def _serialize_receipt_item(item: SupplierReceiptItem) -> dict:
    customer_name = None
    customer_order_number = None
    try:
        coi = item.customer_order_item
        if coi is not None:
            co = coi.order
            if co is not None:
                customer_order_number = co.order_number
                if co.customer is not None:
                    customer_name = co.customer.name
    except Exception:
        pass
    return {
        'id': item.id,
        'supplier_order_id': item.supplier_order_id,
        'supplier_order_item_id': item.supplier_order_item_id,
        'customer_order_item_id': item.customer_order_item_id,
        'autopart_id': item.autopart_id,
        'oem_number': item.oem_number,
        'brand_name': item.brand_name,
        'autopart_name': item.autopart_name,
        'ordered_quantity': item.ordered_quantity,
        'confirmed_quantity': item.confirmed_quantity,
        'received_quantity': item.received_quantity,
        'price': item.price,
        'total_price_with_vat': item.total_price_with_vat,
        'gtd_code': item.gtd_code,
        'country_code': item.country_code,
        'country_name': item.country_name,
        'comment': item.comment,
        'customer_name': customer_name,
        'customer_order_number': customer_order_number,
    }


def serialize_supplier_receipt(receipt: SupplierReceipt) -> dict:
    return {
        'id': receipt.id,
        'provider_id': receipt.provider_id,
        'provider_name': receipt.provider.name if receipt.provider else None,
        'provider_is_vat_payer': (
            receipt.provider.is_vat_payer
            if receipt.provider is not None
            else False
        ),
        'supplier_order_id': receipt.supplier_order_id,
        'source_message_id': receipt.source_message_id,
        'document_number': receipt.document_number,
        'document_date': receipt.document_date,
        'created_by_user_id': receipt.created_by_user_id,
        'created_by_email': (
            receipt.created_by_user.email
            if receipt.created_by_user
            else None
        ),
        'created_at': receipt.created_at,
        'posted_at': receipt.posted_at,
        'comment': receipt.comment,
        'items': [_serialize_receipt_item(item) for item in receipt.items],
    }


async def get_supplier_receipt_detail(
    session: AsyncSession,
    *,
    receipt_id: int,
) -> SupplierReceipt:
    stmt = (
        select(SupplierReceipt)
        .options(
            joinedload(SupplierReceipt.provider),
            joinedload(SupplierReceipt.created_by_user),
            selectinload(SupplierReceipt.items).joinedload(
                SupplierReceiptItem.customer_order_item
            ).joinedload(
                CustomerOrderItem.order
            ).joinedload(CustomerOrder.customer),
        )
        .where(SupplierReceipt.id == receipt_id)
    )
    receipt = (await session.execute(stmt)).scalar_one_or_none()
    if receipt is None:
        raise LookupError('Документ поступления не найден')
    return receipt


async def _reload_receipt_detail(
    session: AsyncSession, receipt_id: int
) -> SupplierReceipt:
    return await get_supplier_receipt_detail(session, receipt_id=receipt_id)


async def update_supplier_receipt(
    session: AsyncSession,
    *,
    receipt_id: int,
    document_number: Optional[str] = None,
    document_date: Optional[date] = None,
    comment: Optional[str] = None,
) -> SupplierReceipt:
    stmt = select(SupplierReceipt).where(SupplierReceipt.id == receipt_id)
    receipt = (await session.execute(stmt)).scalar_one_or_none()
    if receipt is None:
        raise LookupError('Документ поступления не найден')
    if receipt.posted_at is not None:
        raise ValueError('Нельзя редактировать проведённый документ')
    if document_number is not None:
        receipt.document_number = document_number or None
    if document_date is not None:
        receipt.document_date = document_date
    if comment is not None:
        receipt.comment = comment or None
    await session.commit()
    return await _reload_receipt_detail(session, receipt_id)


async def update_supplier_receipt_item(
    session: AsyncSession,
    *,
    item_id: int,
    **fields,
) -> SupplierReceipt:
    stmt = (
        select(SupplierReceiptItem)
        .where(SupplierReceiptItem.id == item_id)
    )
    item = (await session.execute(stmt)).scalar_one_or_none()
    if item is None:
        raise LookupError('Строка документа не найдена')
    receipt_stmt = select(SupplierReceipt).where(
        SupplierReceipt.id == item.receipt_id
    )
    receipt = (await session.execute(receipt_stmt)).scalar_one_or_none()
    if receipt is None or receipt.posted_at is not None:
        raise ValueError('Нельзя редактировать строку проведённого документа')

    allowed = {
        'autopart_id', 'oem_number', 'brand_name', 'autopart_name',
        'received_quantity', 'price', 'total_price_with_vat',
        'gtd_code', 'country_code', 'country_name', 'comment',
    }
    for key, value in fields.items():
        if key in allowed and value is not None:
            setattr(item, key, value)
    await session.commit()
    return await _reload_receipt_detail(session, item.receipt_id)


async def add_supplier_receipt_items(
    session: AsyncSession,
    *,
    receipt_id: int,
    items_payload: list[dict],
) -> SupplierReceipt:
    stmt = select(SupplierReceipt).where(SupplierReceipt.id == receipt_id)
    receipt = (await session.execute(stmt)).scalar_one_or_none()
    if receipt is None:
        raise LookupError('Документ поступления не найден')
    if receipt.posted_at is not None:
        raise ValueError('Нельзя добавлять строки в проведённый документ')

    for payload in items_payload:
        supplier_order_item_id = payload.get('supplier_order_item_id')
        customer_order_item_id = None
        supplier_order_id = None
        oem_number = payload.get('oem_number')
        brand_name = payload.get('brand_name')
        autopart_name = payload.get('autopart_name')
        ordered_quantity = None
        confirmed_quantity = None

        if supplier_order_item_id:
            soi_stmt = (
                select(SupplierOrderItem)
                .options(
                    joinedload(SupplierOrderItem.supplier_order),
                    joinedload(SupplierOrderItem.customer_order_item),
                )
                .where(SupplierOrderItem.id == supplier_order_item_id)
            )
            soi = (await session.execute(soi_stmt)).scalar_one_or_none()
            if soi:
                supplier_order_id = soi.supplier_order_id
                customer_order_item_id = soi.customer_order_item_id
                oem_number = oem_number or soi.oem
                brand_name = brand_name or soi.brand
                autopart_name = autopart_name or soi.name
                ordered_quantity = soi.ship_qty or soi.requested_qty
                confirmed_quantity = soi.ship_qty

        new_item = SupplierReceiptItem(
            receipt_id=receipt_id,
            supplier_order_id=supplier_order_id,
            supplier_order_item_id=supplier_order_item_id,
            customer_order_item_id=customer_order_item_id,
            autopart_id=payload.get('autopart_id'),
            oem_number=oem_number,
            brand_name=brand_name,
            autopart_name=autopart_name,
            ordered_quantity=ordered_quantity,
            confirmed_quantity=confirmed_quantity,
            received_quantity=int(payload.get('received_quantity', 0)),
            price=payload.get('price'),
            total_price_with_vat=payload.get('total_price_with_vat'),
            gtd_code=payload.get('gtd_code'),
            country_code=payload.get('country_code'),
            country_name=payload.get('country_name'),
            comment=payload.get('comment'),
        )
        session.add(new_item)

    await session.commit()
    return await _reload_receipt_detail(session, receipt_id)


async def delete_supplier_receipt_item(
    session: AsyncSession,
    *,
    item_id: int,
) -> SupplierReceipt:
    stmt = select(SupplierReceiptItem).where(SupplierReceiptItem.id == item_id)
    item = (await session.execute(stmt)).scalar_one_or_none()
    if item is None:
        raise LookupError('Строка документа не найдена')
    receipt_stmt = select(SupplierReceipt).where(
        SupplierReceipt.id == item.receipt_id
    )
    receipt = (await session.execute(receipt_stmt)).scalar_one_or_none()
    if receipt is None or receipt.posted_at is not None:
        raise ValueError('Нельзя удалять строки проведённого документа')
    receipt_id = item.receipt_id
    await session.delete(item)
    await session.commit()
    return await _reload_receipt_detail(session, receipt_id)


async def create_manual_supplier_receipt(
    session: AsyncSession,
    *,
    user: User,
    provider_id: int,
    items_payload: list[dict],
    post_now: bool = False,
    document_number: Optional[str] = None,
    document_date: Optional[date] = None,
    comment: Optional[str] = None,
) -> SupplierReceipt:
    receipt = SupplierReceipt(
        provider_id=provider_id,
        document_number=document_number or None,
        document_date=document_date or now_moscow().date(),
        created_by_user_id=user.id,
        created_at=now_moscow(),
        posted_at=now_moscow() if post_now else None,
        comment=comment,
    )
    session.add(receipt)
    await session.flush()

    for payload in items_payload:
        new_item = SupplierReceiptItem(
            receipt_id=receipt.id,
            autopart_id=payload.get('autopart_id'),
            oem_number=payload.get('oem_number'),
            brand_name=payload.get('brand_name'),
            autopart_name=payload.get('autopart_name'),
            received_quantity=int(payload.get('received_quantity', 0)),
            price=payload.get('price'),
            total_price_with_vat=payload.get('total_price_with_vat'),
            gtd_code=payload.get('gtd_code'),
            country_code=payload.get('country_code'),
            country_name=payload.get('country_name'),
            comment=payload.get('comment'),
        )
        session.add(new_item)

    await session.commit()
    return await _reload_receipt_detail(session, receipt.id)
