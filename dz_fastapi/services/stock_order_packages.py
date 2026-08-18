"""Упаковка складского заказа по клиентским коробкам."""

from __future__ import annotations

from collections import defaultdict

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload, selectinload

from dz_fastapi.core.time import now_moscow
from dz_fastapi.models.autopart import AutoPart
from dz_fastapi.models.inventory import (
    StockOrderPackage,
    StockOrderPackageEvent,
    StockOrderPackageEventType,
    StockOrderPackageItem,
    StockOrderPackageStatus,
)
from dz_fastapi.models.partner import STOCK_ORDER_STATUS, StockOrder, StockOrderItem


def _normalize_code(value: str | None) -> str:
    return "".join(char for char in str(value or "").upper() if char.isalnum())


def _user_name(user) -> str | None:
    if user is None:
        return None
    return getattr(user, "name", None) or getattr(user, "email", None)


def _order_options():
    return (
        joinedload(StockOrder.customer),
        selectinload(StockOrder.items)
        .joinedload(StockOrderItem.autopart)
        .joinedload(AutoPart.brand),
        selectinload(StockOrder.items).joinedload(
            StockOrderItem.customer_order_item
        ),
        selectinload(StockOrder.packages)
        .selectinload(StockOrderPackage.items)
        .joinedload(StockOrderPackageItem.stock_order_item)
        .joinedload(StockOrderItem.autopart)
        .joinedload(AutoPart.brand),
        selectinload(StockOrder.packages)
        .selectinload(StockOrderPackage.items)
        .joinedload(StockOrderPackageItem.stock_order_item)
        .joinedload(StockOrderItem.customer_order_item),
        selectinload(StockOrder.packages)
        .selectinload(StockOrderPackage.events)
        .joinedload(StockOrderPackageEvent.user),
        selectinload(StockOrder.packages).joinedload(
            StockOrderPackage.created_by_user
        ),
        selectinload(StockOrder.packages).joinedload(
            StockOrderPackage.sealed_by_user
        ),
        selectinload(StockOrder.packages).joinedload(
            StockOrderPackage.verified_by_user
        ),
        selectinload(StockOrder.packages).joinedload(
            StockOrderPackage.last_printed_by_user
        ),
    )


async def get_stock_order_packing(
    session: AsyncSession,
    *,
    stock_order_id: int,
    for_update: bool = False,
) -> StockOrder:
    statement = (
        select(StockOrder)
        .where(StockOrder.id == stock_order_id)
        .options(*_order_options())
    )
    if for_update:
        statement = statement.with_for_update(of=StockOrder)
    order = (await session.execute(statement)).scalar_one_or_none()
    if order is None:
        raise LookupError("Складской заказ не найден")
    return order


def _allocated_by_item(order: StockOrder) -> dict[int, int]:
    result: dict[int, int] = defaultdict(int)
    for package in order.packages or []:
        for package_item in package.items or []:
            result[int(package_item.stock_order_item_id)] += int(
                package_item.quantity or 0
            )
    return dict(result)


def _ensure_packable(order: StockOrder) -> None:
    if order.shipment_document_id is not None or order.status == (
        STOCK_ORDER_STATUS.DISPATCHED
    ):
        raise ValueError("Отгруженный заказ нельзя переупаковать")
    if order.status != STOCK_ORDER_STATUS.COMPLETED:
        raise ValueError("Сначала полностью соберите складской заказ")


def _add_event(
    package: StockOrderPackage,
    *,
    event_type: StockOrderPackageEventType,
    user_id: int | None,
    reason: str | None = None,
    details: dict | None = None,
) -> None:
    package.events.append(
        StockOrderPackageEvent(
            event_type=event_type,
            user_id=user_id,
            reason=reason,
            details=details,
        )
    )


async def create_stock_order_package(
    session: AsyncSession,
    *,
    stock_order_id: int,
    user_id: int | None,
    comment: str | None = None,
    pack_all_unallocated: bool = False,
) -> StockOrder:
    order = await get_stock_order_packing(
        session,
        stock_order_id=stock_order_id,
        for_update=True,
    )
    _ensure_packable(order)
    sequence_number = max(
        (int(package.sequence_number) for package in order.packages or []),
        default=0,
    ) + 1
    package = StockOrderPackage(
        stock_order_id=order.id,
        sequence_number=sequence_number,
        barcode=f"BOX-{order.id:08d}-{sequence_number:03d}",
        status=StockOrderPackageStatus.OPEN,
        comment=str(comment or "").strip() or None,
        created_by_user_id=user_id,
    )
    order.packages.append(package)
    order.packing_required = True
    _add_event(
        package,
        event_type=StockOrderPackageEventType.CREATED,
        user_id=user_id,
    )
    if pack_all_unallocated:
        allocated = _allocated_by_item(order)
        for order_item in order.items or []:
            remaining = int(order_item.picked_quantity or 0) - int(
                allocated.get(int(order_item.id), 0)
            )
            if remaining > 0:
                package.items.append(
                    StockOrderPackageItem(
                        stock_order_item_id=order_item.id,
                        quantity=remaining,
                    )
                )
    await session.flush()
    return await get_stock_order_packing(
        session,
        stock_order_id=stock_order_id,
    )


async def replace_stock_order_package_contents(
    session: AsyncSession,
    *,
    package_id: int,
    user_id: int | None,
    items: list[dict],
) -> StockOrder:
    package = await session.get(StockOrderPackage, package_id)
    if package is None:
        raise LookupError("Коробка не найдена")
    order = await get_stock_order_packing(
        session,
        stock_order_id=int(package.stock_order_id),
        for_update=True,
    )
    package = next(row for row in order.packages if row.id == package_id)
    _ensure_packable(order)
    if package.status != StockOrderPackageStatus.OPEN:
        raise ValueError("Изменять состав можно только у открытой коробки")

    requested: dict[int, int] = {}
    for payload in items:
        item_id = int(payload.get("stock_order_item_id") or 0)
        quantity = int(payload.get("quantity") or 0)
        if item_id and quantity > 0:
            requested[item_id] = quantity
    order_items = {int(item.id): item for item in order.items or []}
    unknown = set(requested) - set(order_items)
    if unknown:
        raise ValueError("В составе указана строка другого складского заказа")

    allocated_elsewhere: dict[int, int] = defaultdict(int)
    for other_package in order.packages or []:
        if other_package.id == package.id:
            continue
        for package_item in other_package.items or []:
            allocated_elsewhere[int(package_item.stock_order_item_id)] += int(
                package_item.quantity or 0
            )
    for item_id, quantity in requested.items():
        available = int(order_items[item_id].picked_quantity or 0) - int(
            allocated_elsewhere.get(item_id, 0)
        )
        if quantity > available:
            raise ValueError(
                f"В коробку нельзя положить {quantity} шт.: доступно {available}"
            )

    package.items.clear()
    for item_id, quantity in requested.items():
        package.items.append(
            StockOrderPackageItem(
                stock_order_item_id=item_id,
                quantity=quantity,
            )
        )
    _add_event(
        package,
        event_type=StockOrderPackageEventType.CONTENTS_CHANGED,
        user_id=user_id,
        details={"items": requested},
    )
    await session.flush()
    return await get_stock_order_packing(
        session,
        stock_order_id=order.id,
    )


async def seal_stock_order_package(
    session: AsyncSession,
    *,
    package_id: int,
    user_id: int | None,
) -> StockOrder:
    package = await session.get(StockOrderPackage, package_id)
    if package is None:
        raise LookupError("Коробка не найдена")
    order = await get_stock_order_packing(
        session,
        stock_order_id=int(package.stock_order_id),
        for_update=True,
    )
    package = next(row for row in order.packages if row.id == package_id)
    _ensure_packable(order)
    if package.status != StockOrderPackageStatus.OPEN:
        raise ValueError("Коробка уже закрыта")
    if not package.items:
        raise ValueError("Нельзя закрыть пустую коробку")
    package.status = StockOrderPackageStatus.SEALED
    package.sealed_at = now_moscow()
    package.sealed_by_user_id = user_id
    for item in package.items:
        item.verified_quantity = 0
    _add_event(
        package,
        event_type=StockOrderPackageEventType.SEALED,
        user_id=user_id,
    )
    await session.flush()
    return await get_stock_order_packing(session, stock_order_id=order.id)


async def scan_stock_order_package_item(
    session: AsyncSession,
    *,
    package_id: int,
    user_id: int | None,
    scan_code: str,
) -> StockOrder:
    package = await session.get(StockOrderPackage, package_id)
    if package is None:
        raise LookupError("Коробка не найдена")
    order = await get_stock_order_packing(
        session,
        stock_order_id=int(package.stock_order_id),
        for_update=True,
    )
    package = next(row for row in order.packages if row.id == package_id)
    if package.status != StockOrderPackageStatus.SEALED:
        raise ValueError("Скан-проверка доступна только для закрытой коробки")
    normalized = _normalize_code(scan_code)
    if not normalized:
        raise ValueError("Пустой код нельзя проверить")

    matches: list[StockOrderPackageItem] = []
    for package_item in package.items or []:
        if int(package_item.verified_quantity or 0) >= int(
            package_item.quantity or 0
        ):
            continue
        order_item = package_item.stock_order_item
        autopart = order_item.autopart if order_item else None
        customer_item = order_item.customer_order_item if order_item else None
        codes = {
            _normalize_code(getattr(autopart, "barcode", None)),
            _normalize_code(getattr(autopart, "oem_number", None)),
            _normalize_code(getattr(customer_item, "oem", None)),
        }
        if normalized in codes:
            matches.append(package_item)
    if not matches:
        raise ValueError(
            "Код не найден в коробке или нужное количество уже проверено"
        )
    target = matches[0]
    target.verified_quantity = int(target.verified_quantity or 0) + 1
    target.last_scan_code = str(scan_code).strip()
    target.last_verified_at = now_moscow()
    target.last_verified_by_user_id = user_id
    _add_event(
        package,
        event_type=StockOrderPackageEventType.SCANNED,
        user_id=user_id,
        details={
            "package_item_id": target.id,
            "stock_order_item_id": target.stock_order_item_id,
            "scan_code": str(scan_code).strip(),
            "verified_quantity": target.verified_quantity,
        },
    )
    await session.flush()
    return await get_stock_order_packing(session, stock_order_id=order.id)


async def verify_stock_order_package(
    session: AsyncSession,
    *,
    package_id: int,
    user_id: int | None,
) -> StockOrder:
    package = await session.get(StockOrderPackage, package_id)
    if package is None:
        raise LookupError("Коробка не найдена")
    order = await get_stock_order_packing(
        session,
        stock_order_id=int(package.stock_order_id),
        for_update=True,
    )
    package = next(row for row in order.packages if row.id == package_id)
    if package.status != StockOrderPackageStatus.SEALED:
        raise ValueError("Для финальной проверки сначала закройте коробку")
    incomplete = [
        item
        for item in package.items or []
        if int(item.verified_quantity or 0) != int(item.quantity or 0)
    ]
    if incomplete:
        raise ValueError("Не все позиции коробки подтверждены сканированием")
    package.status = StockOrderPackageStatus.VERIFIED
    package.verified_at = now_moscow()
    package.verified_by_user_id = user_id
    _add_event(
        package,
        event_type=StockOrderPackageEventType.VERIFIED,
        user_id=user_id,
    )
    await session.flush()
    return await get_stock_order_packing(session, stock_order_id=order.id)


async def reopen_stock_order_package(
    session: AsyncSession,
    *,
    package_id: int,
    user_id: int | None,
    reason: str,
) -> StockOrder:
    normalized_reason = str(reason or "").strip()
    if not normalized_reason:
        raise ValueError("Укажите причину переоткрытия коробки")
    package = await session.get(StockOrderPackage, package_id)
    if package is None:
        raise LookupError("Коробка не найдена")
    order = await get_stock_order_packing(
        session,
        stock_order_id=int(package.stock_order_id),
        for_update=True,
    )
    package = next(row for row in order.packages if row.id == package_id)
    _ensure_packable(order)
    if package.status == StockOrderPackageStatus.OPEN:
        raise ValueError("Коробка уже открыта")
    package.status = StockOrderPackageStatus.OPEN
    package.sealed_at = None
    package.sealed_by_user_id = None
    package.verified_at = None
    package.verified_by_user_id = None
    for item in package.items or []:
        item.verified_quantity = 0
        item.last_scan_code = None
        item.last_verified_at = None
        item.last_verified_by_user_id = None
    _add_event(
        package,
        event_type=StockOrderPackageEventType.REOPENED,
        user_id=user_id,
        reason=normalized_reason,
    )
    await session.flush()
    return await get_stock_order_packing(session, stock_order_id=order.id)


async def mark_stock_order_package_label_printed(
    session: AsyncSession,
    *,
    package_id: int,
    user_id: int | None,
    reason: str | None = None,
) -> StockOrder:
    package = await session.get(StockOrderPackage, package_id)
    if package is None:
        raise LookupError("Коробка не найдена")
    order = await get_stock_order_packing(
        session,
        stock_order_id=int(package.stock_order_id),
        for_update=True,
    )
    package = next(row for row in order.packages if row.id == package_id)
    normalized_reason = str(reason or "").strip() or None
    if int(package.print_count or 0) > 0 and not normalized_reason:
        raise ValueError("Для повторной печати укажите причину")
    package.print_count = int(package.print_count or 0) + 1
    package.last_printed_at = now_moscow()
    package.last_printed_by_user_id = user_id
    package.last_print_reason = normalized_reason
    _add_event(
        package,
        event_type=StockOrderPackageEventType.LABEL_PRINTED,
        user_id=user_id,
        reason=normalized_reason,
        details={"print_number": package.print_count},
    )
    await session.flush()
    return await get_stock_order_packing(session, stock_order_id=order.id)


async def delete_stock_order_package(
    session: AsyncSession,
    *,
    package_id: int,
) -> StockOrder:
    package = await session.get(StockOrderPackage, package_id)
    if package is None:
        raise LookupError("Коробка не найдена")
    order_id = int(package.stock_order_id)
    order = await get_stock_order_packing(
        session,
        stock_order_id=order_id,
        for_update=True,
    )
    package = next(row for row in order.packages if row.id == package_id)
    _ensure_packable(order)
    if package.status != StockOrderPackageStatus.OPEN:
        raise ValueError("Удалить можно только открытую коробку")
    order.packages.remove(package)
    await session.flush()
    return await get_stock_order_packing(session, stock_order_id=order_id)


def assert_stock_order_packing_ready(order: StockOrder) -> None:
    if not bool(order.packing_required):
        return
    packages = list(order.packages or [])
    if not packages:
        raise ValueError("Перед отгрузкой упакуйте заказ по клиентским коробкам")
    if any(
        package.status != StockOrderPackageStatus.VERIFIED
        for package in packages
    ):
        raise ValueError("Перед отгрузкой проверьте и подтвердите все коробки")
    allocated = _allocated_by_item(order)
    for order_item in order.items or []:
        if int(allocated.get(int(order_item.id), 0)) != int(
            order_item.quantity or 0
        ):
            raise ValueError(
                "Не все собранные позиции полностью распределены по коробкам"
            )


async def get_allocated_stock_order_item_quantity(
    session: AsyncSession,
    *,
    stock_order_item_id: int,
) -> int:
    return int(
        (
            await session.execute(
                select(func.coalesce(func.sum(StockOrderPackageItem.quantity), 0))
                .join(StockOrderPackage)
                .where(
                    StockOrderPackageItem.stock_order_item_id
                    == stock_order_item_id
                )
            )
        ).scalar_one()
        or 0
    )


def serialize_stock_order_packing(order: StockOrder) -> dict:
    allocated = _allocated_by_item(order)
    packages = list(order.packages or [])
    package_count = len(packages)
    return {
        "stock_order_id": order.id,
        "customer_id": order.customer_id,
        "customer_name": order.customer.name if order.customer else None,
        "stock_order_status": order.status,
        "packing_required": bool(order.packing_required),
        "packing_ready": bool(packages)
        and all(
            package.status == StockOrderPackageStatus.VERIFIED
            for package in packages
        )
        and all(
            int(allocated.get(int(item.id), 0)) == int(item.quantity or 0)
            for item in order.items or []
        ),
        "items": [
            {
                "stock_order_item_id": item.id,
                "autopart_id": item.autopart_id,
                "actual_oem": item.autopart.oem_number if item.autopart else None,
                "actual_brand": (
                    item.autopart.brand.name
                    if item.autopart and item.autopart.brand
                    else None
                ),
                "customer_oem": (
                    item.customer_order_item.oem
                    if item.customer_order_item
                    else None
                ),
                "customer_brand": (
                    item.customer_order_item.brand
                    if item.customer_order_item
                    else None
                ),
                "name": (
                    item.customer_order_item.name
                    if item.customer_order_item
                    else item.autopart.name if item.autopart else None
                ),
                "barcode": item.autopart.barcode if item.autopart else None,
                "quantity": int(item.quantity or 0),
                "picked_quantity": int(item.picked_quantity or 0),
                "allocated_quantity": int(allocated.get(int(item.id), 0)),
                "unallocated_quantity": max(
                    int(item.picked_quantity or 0)
                    - int(allocated.get(int(item.id), 0)),
                    0,
                ),
            }
            for item in order.items or []
        ],
        "packages": [
            {
                "id": package.id,
                "sequence_number": package.sequence_number,
                "total_packages": package_count,
                "barcode": package.barcode,
                "status": package.status,
                "comment": package.comment,
                "created_at": package.created_at,
                "created_by_name": _user_name(package.created_by_user),
                "sealed_at": package.sealed_at,
                "sealed_by_name": _user_name(package.sealed_by_user),
                "verified_at": package.verified_at,
                "verified_by_name": _user_name(package.verified_by_user),
                "print_count": int(package.print_count or 0),
                "last_printed_at": package.last_printed_at,
                "last_printed_by_name": _user_name(
                    package.last_printed_by_user
                ),
                "last_print_reason": package.last_print_reason,
                "total_quantity": sum(
                    int(item.quantity or 0) for item in package.items or []
                ),
                "verified_quantity": sum(
                    int(item.verified_quantity or 0)
                    for item in package.items or []
                ),
                "items": [
                    {
                        "id": package_item.id,
                        "stock_order_item_id": (
                            package_item.stock_order_item_id
                        ),
                        "quantity": int(package_item.quantity or 0),
                        "verified_quantity": int(
                            package_item.verified_quantity or 0
                        ),
                        "actual_oem": (
                            package_item.stock_order_item.autopart.oem_number
                            if package_item.stock_order_item
                            and package_item.stock_order_item.autopart
                            else None
                        ),
                        "customer_oem": (
                            package_item.stock_order_item.customer_order_item.oem
                            if package_item.stock_order_item
                            and package_item.stock_order_item.customer_order_item
                            else None
                        ),
                        "customer_brand": (
                            package_item.stock_order_item.customer_order_item.brand
                            if package_item.stock_order_item
                            and package_item.stock_order_item.customer_order_item
                            else None
                        ),
                        "name": (
                            package_item.stock_order_item.customer_order_item.name
                            if package_item.stock_order_item
                            and package_item.stock_order_item.customer_order_item
                            else None
                        ),
                        "last_scan_code": package_item.last_scan_code,
                    }
                    for package_item in package.items or []
                ],
                "events": [
                    {
                        "id": event.id,
                        "event_type": event.event_type,
                        "created_at": event.created_at,
                        "user_name": _user_name(event.user),
                        "reason": event.reason,
                        "details": event.details,
                    }
                    for event in package.events or []
                ],
            }
            for package in packages
        ],
    }
