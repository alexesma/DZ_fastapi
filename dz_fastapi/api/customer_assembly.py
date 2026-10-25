"""Customer-order assembly and realization packing endpoints."""

from datetime import date

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from dz_fastapi.api.deps import get_current_user
from dz_fastapi.core.db import get_session
from dz_fastapi.core.time import now_moscow
from dz_fastapi.models.inventory import (
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
    print_own_labels,
)
from dz_fastapi.services.customer_order_stock_switch import (
    SUGGESTION_KEY,
    SWITCH_KEY,
    switch_unpicked_crosses_after_receipt,
)

router = APIRouter(
    prefix="/customer-orders/assembly",
    tags=["customer-assembly"],
    dependencies=[Depends(get_current_user)],
)


class Selection(BaseModel):
    order_ids: list[int] = Field(min_length=1, max_length=100)


class LabelSelection(BaseModel):
    stock_item_ids: list[int] = Field(min_length=1, max_length=200)
    storage_location_id: int | None = None


class BoxItem(BaseModel):
    shipment_item_id: int
    quantity: int = Field(gt=0)


class BoxContents(BaseModel):
    items: list[BoxItem] = Field(default_factory=list)


@router.get("")
async def get_assembly(customer_id: int | None = None,
                       date_from: date | None = None,
                       date_to: date | None = None,
                       search: str | None = None,
                       limit: int = 200,
                       session: AsyncSession = Depends(get_session)):
    return await list_assembly(session, customer_id=customer_id,
                               date_from=date_from, date_to=date_to,
                               search=search,
                               limit=min(max(limit, 1), 500))


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
