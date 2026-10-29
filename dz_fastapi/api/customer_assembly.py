"""Customer-order assembly and realization packing endpoints."""

from datetime import date
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from dz_fastapi.api.deps import get_current_user
from dz_fastapi.core.db import get_session
from dz_fastapi.core.time import now_moscow
from dz_fastapi.models.autopart import AutoPart
from dz_fastapi.models.inventory import (
    CrossDockingLabel,
    CustomerOrderPickLabel,
    ReserveStatus,
    ShipmentDocument,
    ShipmentDocumentItem,
    ShipmentDocumentStatus,
    ShipmentPackage,
    ShipmentPackageItem,
    StockReserve,
)
from dz_fastapi.models.partner import CustomerOrder, CustomerOrderItem, StockOrderItem
from dz_fastapi.models.user import User, UserRole
from dz_fastapi.services.customer_assembly import (
    create_realization_draft,
    list_assembly,
    list_assembly_page,
    print_own_labels,
    report_own_stock_shortage,
    update_cross_picks_bulk,
)
from dz_fastapi.services.customer_order_stock_switch import (
    SUGGESTION_KEY,
    SWITCH_KEY,
    switch_unpicked_crosses_after_receipt,
)
from dz_fastapi.services.scan_codes import find_autoparts

router = APIRouter(
    prefix="/customer-orders/assembly",
    tags=["customer-assembly"],
    dependencies=[Depends(get_current_user)],
)


class Selection(BaseModel):
    order_ids: list[int] = Field(min_length=1, max_length=100)


class ShortageReport(BaseModel):
    found_quantity: int = Field(ge=0)
    storage_location_id: int | None = None
    comment: str | None = Field(default=None, max_length=300)


class BulkPickRow(BaseModel):
    id: int
    picked_quantity: int = Field(ge=0)


class BulkPickSelection(BaseModel):
    items: list[BulkPickRow] = Field(min_length=1, max_length=200)


class LabelSelection(BaseModel):
    stock_item_ids: list[int] = Field(min_length=1, max_length=200)
    storage_location_id: int | None = None


class BoxItem(BaseModel):
    shipment_item_id: int
    quantity: int = Field(gt=0)


class BoxScan(BaseModel):
    code: str = Field(min_length=1, max_length=128)


class BoxContents(BaseModel):
    items: list[BoxItem] = Field(default_factory=list)


@router.get("")
async def get_assembly(customer_id: int | None = None,
                       customer_group: Literal["wholesale", "retail"] | None = None,
                       date_from: date | None = None,
                       date_to: date | None = None,
                       search: str | None = None,
                       limit: int = 200,
                       session: AsyncSession = Depends(get_session)):
    return await list_assembly(session, customer_id=customer_id,
                               customer_group=customer_group,
                               date_from=date_from, date_to=date_to,
                               search=search,
                               limit=min(max(limit, 1), 500))


@router.get("/page")
async def get_assembly_page(
    customer_id: int | None = None,
    customer_group: Literal["wholesale", "retail"] | None = None,
    date_from: date | None = None,
    date_to: date | None = None,
    search: str | None = None,
    view: Literal["work", "dates"] = "work",
    page: int = 1,
    page_size: int = 50,
    session: AsyncSession = Depends(get_session),
):
    return await list_assembly_page(
        session, customer_id=customer_id, customer_group=customer_group,
        date_from=date_from, date_to=date_to, search=search,
        open_only=view == "work", page=max(page, 1),
        page_size=min(max(page_size, 1), 100),
    )


@router.post("/cross-picks/bulk")
async def bulk_cross_picks(
    payload: BulkPickSelection,
    session: AsyncSession = Depends(get_session),
    user: User = Depends(get_current_user),
):
    ids = [row.id for row in payload.items]
    if len(ids) != len(set(ids)):
        raise HTTPException(status_code=400, detail="Позиция повторяется в запросе")
    try:
        return await update_cross_picks_bulk(
            session, {row.id: row.picked_quantity for row in payload.items},
            user_id=user.id,
        )
    except ValueError as exc:
        await session.rollback()
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.post("/source-switch/{item_id}/approve")
async def approve_exact_stock_switch(
    item_id: int, session: AsyncSession = Depends(get_session),
    user: User = Depends(get_current_user),
):
    if user.role not in (UserRole.ADMIN, UserRole.MANAGER):
        raise HTTPException(status_code=403, detail="Недостаточно прав")
    item = (await session.execute(
        select(CustomerOrderItem).where(CustomerOrderItem.id == item_id)
        .with_for_update(of=CustomerOrderItem)
    )).scalar_one_or_none()
    if item is None:
        raise HTTPException(status_code=404, detail="Позиция заказа не найдена")
    payload = item.source_payload or {}
    notice = payload.get(SUGGESTION_KEY) or payload.get(SWITCH_KEY) or {}
    if notice.get("status") != "blocked":
        raise HTTPException(status_code=409, detail="Нет предложения о переключении")
    outcomes = await switch_unpicked_crosses_after_receipt(
        session, autopart_id=int(notice["autopart_id"]),
        source_type=notice["source_type"],
        document_id=int(notice["document_id"]), target_item_id=item_id,
        allow_cancelled_labels=True, decided_by_user_id=user.id,
    )
    if not outcomes or outcomes[0]["status"] != "switched":
        await session.rollback()
        reason = outcomes[0]["reason"] if outcomes else "источник заказа изменился"
        raise HTTPException(status_code=409, detail=reason)
    await session.commit()
    return outcomes[0]


@router.post("/source-switch/{item_id}/keep-cross")
async def keep_cross_source(
    item_id: int, session: AsyncSession = Depends(get_session),
    user: User = Depends(get_current_user),
):
    if user.role not in (UserRole.ADMIN, UserRole.MANAGER):
        raise HTTPException(status_code=403, detail="Недостаточно прав")
    item = (await session.execute(
        select(CustomerOrderItem).where(CustomerOrderItem.id == item_id)
        .with_for_update(of=CustomerOrderItem)
    )).scalar_one_or_none()
    if item is None:
        raise HTTPException(status_code=404, detail="Позиция заказа не найдена")
    payload = dict(item.source_payload or {})
    notice_key = (
        SUGGESTION_KEY if payload.get(SUGGESTION_KEY, {}).get("status") == "blocked"
        else SWITCH_KEY
    )
    notice = dict(payload.get(notice_key) or {})
    if notice.get("status") != "blocked":
        raise HTTPException(status_code=409, detail="Нет предложения о переключении")
    notice["status"] = "kept"
    notice["decided_at"] = now_moscow().isoformat()
    notice["decided_by_user_id"] = user.id
    payload[notice_key] = notice
    item.source_payload = payload
    await session.commit()
    return {"item_id": item_id, "status": "kept"}


@router.post("/own-labels/print")
async def print_own_stock_labels(
    payload: LabelSelection,
    session: AsyncSession = Depends(get_session),
    user: User = Depends(get_current_user),
):
    try:
        result = await print_own_labels(
            session, payload.stock_item_ids, user.id,
            storage_location_id=payload.storage_location_id,
        )
        await session.commit()
        return result
    except ValueError as exc:
        await session.rollback()
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.get("/own-labels")
async def get_own_stock_labels(stock_item_id: int,
                               session: AsyncSession = Depends(get_session)):
    rows = (await session.execute(
        select(CustomerOrderPickLabel).where(
            CustomerOrderPickLabel.stock_order_item_id == stock_item_id,
            CustomerOrderPickLabel.cancelled_at.is_(None),
        ).options(selectinload(CustomerOrderPickLabel.storage_location))
    )).scalars().all()
    return [{"id": row.id, "quantity": row.quantity, "barcode": row.barcode,
             "location": row.storage_location.name, "print_count": row.print_count}
            for row in rows]


@router.post("/own-labels/{label_id}/reprint")
async def reprint_own_stock_label(label_id: int,
                                  session: AsyncSession = Depends(get_session)):
    label = await session.get(CustomerOrderPickLabel, label_id)
    if label is None or label.cancelled_at is not None:
        raise HTTPException(status_code=404, detail="Этикетка не найдена")
    label.print_count += 1
    label.printed_at = now_moscow()
    await session.commit()
    return {"id": label.id, "barcode": label.barcode, "quantity": label.quantity,
            "print_count": label.print_count}


@router.post("/own-labels/{label_id}/cancel")
async def cancel_own_stock_label(label_id: int,
                                 session: AsyncSession = Depends(get_session)):
    initial = await session.get(CustomerOrderPickLabel, label_id)
    if initial is None:
        raise HTTPException(status_code=404, detail="Этикетка не найдена")
    stock_item = await session.get(StockOrderItem, initial.stock_order_item_id)
    customer_item = (await session.get(CustomerOrderItem, stock_item.customer_order_item_id)
                     if stock_item else None)
    if customer_item is None:
        raise HTTPException(status_code=404, detail="Позиция заказа не найдена")
    await session.execute(
        select(CustomerOrder.id).where(CustomerOrder.id == customer_item.order_id)
        .with_for_update()
    )
    label = (await session.execute(
        select(CustomerOrderPickLabel).where(CustomerOrderPickLabel.id == label_id)
        .with_for_update(of=CustomerOrderPickLabel)
    )).scalar_one_or_none()
    if label is None or label.cancelled_at is not None:
        raise HTTPException(status_code=404, detail="Этикетка не найдена")
    used = (await session.execute(
        select(func.count(ShipmentDocumentItem.id))
        .join(ShipmentDocument)
        .where(ShipmentDocumentItem.assembly_pick_label_id == label.id,
               ShipmentDocument.status != ShipmentDocumentStatus.CANCELLED)
    )).scalar_one()
    if used:
        raise HTTPException(status_code=409,
                            detail="Сначала удалите позицию из черновика реализации")
    label.cancelled_at = now_moscow()
    if label.reserve_id:
        reserve = await session.get(StockReserve, label.reserve_id)
        if reserve and reserve.status == ReserveStatus.ACTIVE:
            reserve.status = ReserveStatus.RELEASED
            reserve.released_at = now_moscow()
    row = await session.get(StockOrderItem, label.stock_order_item_id)
    if row:
        row.picked_quantity = max(0, int(row.picked_quantity or 0) - int(label.quantity))
    await session.commit()
    return {"cancelled": True}


@router.post("/realizations/preview")
async def preview_realization(
    payload: Selection,
    session: AsyncSession = Depends(get_session),
):
    ids = sorted(set(payload.order_ids))
    orders = await list_assembly(session, order_ids=ids, limit=len(ids))
    if len(orders) != len(ids):
        raise HTTPException(status_code=404, detail="Один из заказов не найден")
    if len({order["customer_id"] for order in orders}) != 1:
        raise HTTPException(status_code=409, detail="Выберите заказы одного клиента")
    lines = []
    for order in orders:
        for item in order["items"]:
            shippable = int(item["ordered"])
            committed = int(item["committed"])
            ready = int(item["ready"])
            if shippable <= 0:
                continue
            lines.append({
                "order_id": order["id"], "item_id": item["id"],
                "order_number": order["number"],
                "brand": item["brand"], "oem": item["oem"],
                "ordered": shippable, "committed": committed,
                "ready": ready,
                "remaining_after_draft": max(0, shippable - committed - ready),
            })
    return {
        "customer_name": orders[0]["customer_name"],
        "orders": len(orders), "lines": lines,
        "ready_total": sum(row["ready"] for row in lines),
        "remaining_total": sum(row["remaining_after_draft"] for row in lines),
    }


@router.post("/realizations", status_code=201)
async def create_assembly_realization(
    payload: Selection,
    session: AsyncSession = Depends(get_session),
):
    try:
        doc = await create_realization_draft(session, payload.order_ids)
        await session.commit()
        return {"id": doc.id, "doc_number": doc.doc_number}
    except ValueError as exc:
        await session.rollback()
        raise HTTPException(status_code=409, detail=str(exc)) from exc


async def _draft(session: AsyncSession, document_id: int):
    doc = (await session.execute(
        select(ShipmentDocument).where(ShipmentDocument.id == document_id)
        .with_for_update(of=ShipmentDocument)
    )).scalar_one_or_none()
    if doc is None or not doc.assembly_managed:
        raise HTTPException(status_code=404, detail="Черновик комплектации не найден")
    if doc.status != ShipmentDocumentStatus.DRAFT:
        raise HTTPException(status_code=409, detail="Редактировать можно только черновик")
    return doc


def _box_out(box):
    return {"id": box.id, "number": box.number,
            "sealed_at": box.sealed_at,
            "items": [{"shipment_item_id": row.shipment_item_id,
                       "quantity": row.quantity} for row in box.items]}


@router.get("/realizations/{document_id}/boxes")
async def list_boxes(document_id: int,
                     session: AsyncSession = Depends(get_session)):
    doc = await session.get(ShipmentDocument, document_id)
    if doc is None or not doc.assembly_managed:
        raise HTTPException(status_code=404, detail="Реализация не найдена")
    boxes = (await session.execute(
        select(ShipmentPackage).where(ShipmentPackage.document_id == document_id)
        .options(selectinload(ShipmentPackage.items))
        .order_by(ShipmentPackage.number)
    )).scalars().all()
    return [_box_out(box) for box in boxes]


@router.post("/realizations/{document_id}/boxes", status_code=201)
async def create_box(document_id: int,
                     session: AsyncSession = Depends(get_session)):
    await _draft(session, document_id)
    number = (await session.execute(
        select(func.coalesce(func.max(ShipmentPackage.number), 0))
        .where(ShipmentPackage.document_id == document_id)
    )).scalar_one() + 1
    box = ShipmentPackage(document_id=document_id, number=number)
    session.add(box)
    await session.commit()
    return {"id": box.id, "number": box.number, "sealed_at": None, "items": []}


@router.put("/realizations/{document_id}/boxes/{box_id}")
async def update_box(document_id: int, box_id: int, payload: BoxContents,
                     session: AsyncSession = Depends(get_session)):
    await _draft(session, document_id)
    box = (await session.execute(
        select(ShipmentPackage).where(ShipmentPackage.id == box_id,
                                      ShipmentPackage.document_id == document_id)
        .options(selectinload(ShipmentPackage.items))
        .with_for_update()
    )).scalar_one_or_none()
    if box is None:
        raise HTTPException(status_code=404, detail="Коробка не найдена")
    if box.sealed_at is not None:
        raise HTTPException(status_code=409, detail="Откройте коробку для изменения")
    if len({row.shipment_item_id for row in payload.items}) != len(payload.items):
        raise HTTPException(status_code=400, detail="Повторяющиеся позиции в коробке")
    item_ids = [row.shipment_item_id for row in payload.items]
    items = (await session.execute(
        select(ShipmentDocumentItem).where(
            ShipmentDocumentItem.id.in_(item_ids),
            ShipmentDocumentItem.document_id == document_id,
        )
    )).scalars().all() if item_ids else []
    if len(items) != len(item_ids):
        raise HTTPException(status_code=400, detail="Позиция не входит в реализацию")
    other = (await session.execute(
        select(ShipmentPackageItem.shipment_item_id,
               func.sum(ShipmentPackageItem.quantity))
        .join(ShipmentPackage)
        .where(ShipmentPackage.document_id == document_id,
               ShipmentPackage.id != box_id)
        .group_by(ShipmentPackageItem.shipment_item_id)
    )).all()
    other_qty = {key: int(value) for key, value in other}
    item_map = {row.id: row for row in items}
    for row in payload.items:
        if (row.quantity + other_qty.get(row.shipment_item_id, 0)
                > item_map[row.shipment_item_id].quantity):
            raise HTTPException(status_code=409, detail="В коробках больше товара, чем в черновике")
    for row in box.items:
        await session.delete(row)
    await session.flush()
    for row in payload.items:
        session.add(ShipmentPackageItem(package_id=box.id,
                                        shipment_item_id=row.shipment_item_id,
                                        quantity=row.quantity))
    await session.flush()
    await session.refresh(box, ["items"])
    await session.commit()
    return _box_out(box)


@router.post("/realizations/{document_id}/boxes/{box_id}/scan")
async def scan_into_box(document_id: int, box_id: int, payload: BoxScan,
                        session: AsyncSession = Depends(get_session)):
    """Скан этикетки позиции кладёт её свободное количество в коробку.

    Скан штрихкода товара добавляет одну штуку позиции с этим товаром.
    """
    await _draft(session, document_id)
    code = payload.code.strip()
    box = (await session.execute(
        select(ShipmentPackage).where(ShipmentPackage.id == box_id,
                                      ShipmentPackage.document_id == document_id)
        .options(selectinload(ShipmentPackage.items))
        .with_for_update()
    )).scalar_one_or_none()
    if box is None:
        raise HTTPException(status_code=404, detail="Коробка не найдена")
    if box.sealed_at is not None:
        raise HTTPException(status_code=409, detail="Откройте коробку для изменения")
    items = (await session.execute(
        select(ShipmentDocumentItem).where(ShipmentDocumentItem.document_id == document_id)
        .order_by(ShipmentDocumentItem.id)
    )).scalars().all()
    pick_ids = set((await session.execute(
        select(CustomerOrderPickLabel.id).where(CustomerOrderPickLabel.barcode == code)
    )).scalars().all())
    cross_ids = set((await session.execute(
        select(CrossDockingLabel.id).where(CrossDockingLabel.barcode == code)
    )).scalars().all())
    by_label = bool(pick_ids or cross_ids)
    if by_label:
        matches = [row for row in items
                   if row.assembly_pick_label_id in pick_ids
                   or row.cross_docking_label_id in cross_ids]
    else:
        part_ids = {part.id for part in await find_autoparts(session, code)}
        matches = [row for row in items if row.autopart_id in part_ids]
    if not matches:
        raise HTTPException(
            status_code=404,
            detail="Код не относится к позициям этой реализации",
        )
    placed_rows = (await session.execute(
        select(ShipmentPackageItem.shipment_item_id, ShipmentPackage.number,
               ShipmentPackageItem.package_id, ShipmentPackageItem.quantity)
        .join(ShipmentPackage)
        .where(ShipmentPackage.document_id == document_id)
    )).all()
    placed: dict[int, int] = {}
    for item_id, _number, _pkg, qty in placed_rows:
        placed[item_id] = placed.get(item_id, 0) + int(qty)
    target = next((row for row in matches
                   if placed.get(row.id, 0) < row.quantity), None)
    if target is None:
        elsewhere = sorted({number for item_id, number, pkg, _q in placed_rows
                            if item_id in {row.id for row in matches}
                            and pkg != box.id})
        where = (f" (коробка {', '.join(map(str, elsewhere))})"
                 if elsewhere else "")
        raise HTTPException(status_code=409,
                            detail=f"Позиция уже полностью разложена{where}")
    free = target.quantity - placed.get(target.id, 0)
    add = free if by_label else 1
    current = next((row for row in box.items
                    if row.shipment_item_id == target.id), None)
    if current is None:
        session.add(ShipmentPackageItem(package_id=box.id,
                                        shipment_item_id=target.id, quantity=add))
    else:
        current.quantity += add
    await session.flush()
    await session.refresh(box, ["items"])
    await session.commit()
    title = " ".join(part for part in (target.customer_brand,
                                       target.customer_oem) if part)
    if target.autopart_id:
        part = (await session.execute(
            select(AutoPart).where(AutoPart.id == target.autopart_id)
            .options(selectinload(AutoPart.brand))
        )).scalar_one_or_none()
        if part is not None:
            title = f"{part.brand.name if part.brand else ''} {part.oem_number}".strip()
    return {"box": _box_out(box), "shipment_item_id": target.id, "added": add,
            "label": by_label, "title": title}


@router.post("/realizations/{document_id}/boxes/{box_id}/seal")
async def seal_box(document_id: int, box_id: int,
                   session: AsyncSession = Depends(get_session)):
    await _draft(session, document_id)
    box = (await session.execute(
        select(ShipmentPackage).where(ShipmentPackage.id == box_id,
                                      ShipmentPackage.document_id == document_id)
        .options(selectinload(ShipmentPackage.items))
    )).scalar_one_or_none()
    if box is None:
        raise HTTPException(status_code=404, detail="Коробка не найдена")
    if not box.items:
        raise HTTPException(status_code=409, detail="Коробка пуста")
    box.sealed_at = now_moscow()
    await session.commit()
    return _box_out(box)


@router.post("/realizations/{document_id}/boxes/{box_id}/open")
async def open_box(document_id: int, box_id: int,
                   session: AsyncSession = Depends(get_session)):
    await _draft(session, document_id)
    box = await session.get(ShipmentPackage, box_id)
    if box is None or box.document_id != document_id:
        raise HTTPException(status_code=404, detail="Коробка не найдена")
    box.sealed_at = None
    await session.commit()
    return {"id": box.id, "sealed_at": None}


@router.delete("/realizations/{document_id}/boxes/{box_id}", status_code=204)
async def delete_box(document_id: int, box_id: int,
                     session: AsyncSession = Depends(get_session)):
    await _draft(session, document_id)
    box = await session.get(ShipmentPackage, box_id)
    if box is None or box.document_id != document_id:
        raise HTTPException(status_code=404, detail="Коробка не найдена")
    await session.delete(box)
    await session.commit()


@router.post("/own-stock/{stock_item_id}/shortage")
async def report_own_stock_shortage_endpoint(
    stock_item_id: int,
    payload: ShortageReport,
    session: AsyncSession = Depends(get_session),
    user: User = Depends(get_current_user),
):
    """Недостача при сборке: остальное — в отказ, инвентаризация по месту."""
    try:
        result = await report_own_stock_shortage(
            session, stock_item_id=stock_item_id,
            found_quantity=payload.found_quantity,
            storage_location_id=payload.storage_location_id,
            comment=payload.comment, user_id=user.id,
        )
        await session.commit()
    except ValueError as exc:
        await session.rollback()
        raise HTTPException(status_code=409, detail=str(exc)) from exc

    # После фиксации: обновить ответ клиенту и уведомить администраторов
    from dz_fastapi.models.notification import AppNotificationLevel
    from dz_fastapi.services.customer_orders import (
        refresh_order_response_file,
        try_finalize_customer_order_response,
    )
    from dz_fastapi.services.notifications import notify_admin_all

    try:
        if await refresh_order_response_file(session, order_id=result["order_id"]):
            await try_finalize_customer_order_response(
                session, order_id=result["order_id"]
            )
    except Exception:  # noqa: BLE001
        await session.rollback()
    try:
        sent_note = (
            " Ответ клиенту уже отправлен — согласуйте отказ с клиентом."
            if result["response_already_sent"] else ""
        )
        await notify_admin_all(
            session,
            title=f"Недостача на месте {result['location']}",
            message=(
                f"Заказ {result['order_number']}: {result['brand']} {result['oem']} — "
                f"по системе {result['system_quantity']}, найдено {result['found']}, "
                f"в отказ {result['rejected_quantity']} шт. Создана инвентаризация "
                f"по месту (сеанс №{result['inventory_session_id']}).{sent_note}"
            ),
            level=AppNotificationLevel.WARNING,
            link="/warehouse/inventory",
        )
    except Exception:  # noqa: BLE001
        await session.rollback()
    return result
