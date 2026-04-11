from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Iterable, Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload, selectinload

from dz_fastapi.core.time import now_moscow
from dz_fastapi.crud.customer_order import crud_supplier_order
from dz_fastapi.models.partner import (STOCK_ORDER_STATUS, CustomerOrderItem,
                                       StockOrder, StockOrderItem,
                                       SupplierOrderItem, SupplierReceipt,
                                       SupplierReceiptItem)
from dz_fastapi.models.user import User


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
                    'pending_quantity': max(
                        expected_quantity - already_received,
                        0,
                    ),
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
        received_quantity = int(payload.get('received_quantity') or 0)
        if received_quantity > int(item.quantity or 0):
            raise ValueError(
                f'Полученное количество превышает заказанное для OEM '
                f'{item.oem_number or item.autopart_name or item.id}'
            )

        if post_now:
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

    await session.commit()
    stmt = (
        select(SupplierReceipt)
        .options(
            joinedload(SupplierReceipt.provider),
            joinedload(SupplierReceipt.created_by_user),
            selectinload(SupplierReceipt.items),
        )
        .where(SupplierReceipt.id == receipt.id)
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
            current_received = int(order_item.received_quantity or 0)
            pending_quantity = max(expected_quantity - current_received, 0)
            applied_quantity = min(requested_quantity, pending_quantity)
            receipt_item.received_quantity = applied_quantity
            if applied_quantity <= 0:
                continue
            order_item.received_quantity = current_received + applied_quantity
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


def serialize_supplier_receipt(receipt: SupplierReceipt) -> dict:
    return {
        'id': receipt.id,
        'provider_id': receipt.provider_id,
        'provider_name': receipt.provider.name if receipt.provider else None,
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
        'items': [
            {
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
                'comment': item.comment,
            }
            for item in receipt.items
        ],
    }
