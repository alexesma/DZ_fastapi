"""Приёмка сканером: сверка поступления с документом и размещение по местам."""

from typing import Literal

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from dz_fastapi.api.deps import get_current_user
from dz_fastapi.core.db import get_session
from dz_fastapi.models.autopart import AutoPart, StorageLocation
from dz_fastapi.models.inventory import ReceiptScanCount, StockByLocation
from dz_fastapi.models.partner import SupplierReceipt
from dz_fastapi.services.inventory_stock import (
    RECEIVING_LOCATION_CODE,
    resolve_receipt_item_autopart_id,
    transfer_stock_with_lot_trace,
)

router = APIRouter(
    prefix="/receiving-scan",
    tags=["receiving-scan"],
    dependencies=[Depends(get_current_user)],
)


class CountPayload(BaseModel):
    autopart_id: int
    quantity: int = Field(ge=0)
    mode: Literal["set", "add"] = "add"


class PlacePayload(BaseModel):
    autopart_id: int
    storage_location_id: int
    quantity: int = Field(gt=0)


async def _load_receipt(session: AsyncSession, receipt_id: int) -> SupplierReceipt:
    receipt = (await session.execute(
        select(SupplierReceipt).where(SupplierReceipt.id == receipt_id)
        .options(selectinload(SupplierReceipt.items),
                 selectinload(SupplierReceipt.provider))
    )).scalar_one_or_none()
    if receipt is None:
        raise HTTPException(status_code=404, detail="Поступление не найдено")
    return receipt


async def _lines(session: AsyncSession, receipt: SupplierReceipt) -> list[dict]:
    item_ids = [item.id for item in receipt.items]
    counts = {row.receipt_item_id: row for row in (await session.execute(
        select(ReceiptScanCount).where(
            ReceiptScanCount.receipt_item_id.in_(item_ids))
    )).scalars().all()} if item_ids else {}
    merged: dict[int | str, dict] = {}
    for item in receipt.items:
        autopart_id = await resolve_receipt_item_autopart_id(session, item)
        key = autopart_id if autopart_id is not None else f"i{item.id}"
        row = merged.setdefault(key, {
            "autopart_id": autopart_id, "item_ids": [],
            "oem": item.oem_number, "brand": item.brand_name,
            "name": item.autopart_name, "document_qty": 0,
            "accepted_qty": 0, "placed_qty": 0,
        })
        row["item_ids"].append(item.id)
        row["document_qty"] += int(item.received_quantity or 0)
        count = counts.get(item.id)
        if count is not None:
            row["accepted_qty"] += count.accepted_qty
            row["placed_qty"] += count.placed_qty
    return list(merged.values())


@router.get("/receipts/{receipt_id}")
async def receiving_plan(receipt_id: int,
                         session: AsyncSession = Depends(get_session)):
    receipt = await _load_receipt(session, receipt_id)
    return {
        "id": receipt.id,
        "document_number": receipt.document_number,
        "provider": receipt.provider.name if receipt.provider else None,
        "posted": receipt.posted_at is not None,
        "lines": await _lines(session, receipt),
    }


def _row_for(lines: list[dict], autopart_id: int) -> dict | None:
    return next((r for r in lines if r["autopart_id"] == autopart_id), None)


@router.post("/receipts/{receipt_id}/count")
async def count_received(receipt_id: int, payload: CountPayload,
                         session: AsyncSession = Depends(get_session)):
    """Сверка: фактически принятое количество товара (документ не меняется)."""
    receipt = await _load_receipt(session, receipt_id)
    lines = await _lines(session, receipt)
    row = _row_for(lines, payload.autopart_id)
    if row is None:
        raise HTTPException(status_code=404,
                            detail="Этого товара нет в документе поступления")
    # излишек пишем в первую строку товара, чтобы не терять факт
    item_id = row["item_ids"][0]
    existing = {c.receipt_item_id: c for c in (await session.execute(
        select(ReceiptScanCount)
        .where(ReceiptScanCount.receipt_item_id.in_(row["item_ids"]))
        .with_for_update()
    )).scalars().all()}
    target = payload.quantity
    if payload.mode == "add":
        target += row["accepted_qty"]
    for other in row["item_ids"]:
        if other in existing:
            existing[other].accepted_qty = 0
    count = existing.get(item_id)
    if count is None:
        count = ReceiptScanCount(receipt_item_id=item_id, placed_qty=0)
        session.add(count)
    count.accepted_qty = target
    await session.commit()
    row["accepted_qty"] = target
    return row


@router.post("/receipts/{receipt_id}/place")
async def place_received(receipt_id: int, payload: PlacePayload,
                         session: AsyncSession = Depends(get_session)):
    """Размещение: перемещение товара из зоны приёмки в выбранное место."""
    receipt = await _load_receipt(session, receipt_id)
    if receipt.posted_at is None:
        raise HTTPException(
            status_code=409,
            detail="Поступление не проведено — товар ещё не на остатке",
        )
    lines = await _lines(session, receipt)
    row = _row_for(lines, payload.autopart_id)
    if row is None:
        raise HTTPException(status_code=404,
                            detail="Этого товара нет в документе поступления")
    target = await session.get(StorageLocation, payload.storage_location_id)
    if target is None:
        raise HTTPException(status_code=404, detail="Место не найдено")
    if target.system_code == RECEIVING_LOCATION_CODE:
        raise HTTPException(status_code=400,
                            detail="Это зона приёмки, выберите место хранения")
    sources = (await session.execute(
        select(StockByLocation).join(
            StorageLocation,
            StorageLocation.id == StockByLocation.storage_location_id)
        .where(StockByLocation.autopart_id == payload.autopart_id,
               StockByLocation.quantity > 0,
               StorageLocation.system_code == RECEIVING_LOCATION_CODE)
        .order_by(StockByLocation.quantity.desc())
    )).scalars().all()
    available = sum(s.quantity for s in sources)
    if available < payload.quantity:
        raise HTTPException(
            status_code=409,
            detail=f"В зоне приёмки только {available} шт. этого товара",
        )
    left = payload.quantity
    for source in sources:
        take = min(left, source.quantity)
        await transfer_stock_with_lot_trace(
            session, autopart_id=payload.autopart_id,
            from_location_id=source.storage_location_id,
            to_location_id=target.id, quantity=take,
            notes=f"Размещение по поступлению #{receipt.id}",
        )
        left -= take
        if left == 0:
            break
    item_id = row["item_ids"][0]
    count = (await session.execute(
        select(ReceiptScanCount)
        .where(ReceiptScanCount.receipt_item_id == item_id).with_for_update()
    )).scalar_one_or_none()
    if count is None:
        count = ReceiptScanCount(receipt_item_id=item_id, accepted_qty=0,
                                 placed_qty=0)
        session.add(count)
    count.placed_qty += payload.quantity
    await session.commit()
    row["placed_qty"] += payload.quantity
    return {"placed": payload.quantity, "line": row,
            "location": target.name,
            "autopart": (await session.get(AutoPart, payload.autopart_id)).oem_number}
