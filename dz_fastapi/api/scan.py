"""Universal barcode lookup: what does a scanned code belong to."""

import re

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from dz_fastapi.api.deps import get_current_user
from dz_fastapi.core.db import get_session
from dz_fastapi.models.autopart import AutoPart
from dz_fastapi.models.inventory import (
    CrossDockingLabel,
    CustomerOrderPickLabel,
    ProductionWaveLabel,
    ShipmentDocument,
    StockOrderPackage,
)
from dz_fastapi.models.partner import StockOrderItem

router = APIRouter(
    prefix="/scan",
    tags=["scan"],
    dependencies=[Depends(get_current_user)],
)

_BOX_RE = re.compile(r"^DZ-BOX-(\d+)-(\d+)$")


def _found(kind: str, title: str, route: str, **extra) -> dict:
    return {"kind": kind, "title": title, "route": route, **extra}


@router.get("/resolve")
async def resolve_scan(code: str,
                       session: AsyncSession = Depends(get_session)):
    code = code.strip()
    if not code or len(code) > 128:
        raise HTTPException(status_code=400, detail="Пустой или слишком длинный код")

    box = _BOX_RE.match(code)
    if box:
        doc = await session.get(ShipmentDocument, int(box.group(1)))
        if doc is not None:
            return _found(
                "shipment_box",
                f"Коробка {box.group(2)} · {doc.doc_number or f'#{doc.id}'}",
                f"/warehouse/shipments/{doc.id}",
                document_id=doc.id, box_number=int(box.group(2)),
            )

    own = (await session.execute(
        select(CustomerOrderPickLabel)
        .where(CustomerOrderPickLabel.barcode == code)
        .options(selectinload(CustomerOrderPickLabel.storage_location))
    )).scalar_one_or_none()
    if own is not None:
        item = await session.get(StockOrderItem, own.stock_order_item_id)
        return _found(
            "own_label", "Этикетка сборки со склада", "/customer-orders/stock",
            quantity=own.quantity,
            location=own.storage_location.name if own.storage_location else None,
            cancelled=own.cancelled_at is not None,
            stock_order_id=item.stock_order_id if item else None,
        )

    cross = (await session.execute(
        select(CrossDockingLabel).where(CrossDockingLabel.barcode == code)
    )).scalar_one_or_none()
    if cross is not None:
        return _found(
            "cross_docking_label",
            f"Кросс-докинг: {cross.requested_brand} {cross.requested_oem}",
            "/customer-orders/stock",
            quantity=cross.quantity, order_number=cross.order_number,
            customer_name=cross.customer_name, status=str(cross.status),
        )

    wave = (await session.execute(
        select(ProductionWaveLabel).where(ProductionWaveLabel.barcode == code)
    )).scalar_one_or_none()
    if wave is not None:
        return _found(
            "production_label",
            f"Этикетка волны: {wave.requested_brand} {wave.requested_oem}",
            "/warehouse/production-groups",
            quantity=wave.quantity, order_number=wave.order_number,
            customer_name=wave.customer_name, status=str(wave.status),
        )

    package = (await session.execute(
        select(StockOrderPackage).where(StockOrderPackage.barcode == code)
    )).scalar_one_or_none()
    if package is not None:
        return _found(
            "stock_package", f"Коробка клиента №{package.sequence_number}",
            "/customer-orders/stock/scan",
            package_id=package.id, stock_order_id=package.stock_order_id,
            status=str(package.status),
        )

    part = (await session.execute(
        select(AutoPart).where(AutoPart.barcode == code)
        .options(selectinload(AutoPart.brand))
    )).scalar_one_or_none()
    if part is not None:
        brand = part.brand.name if part.brand else ""
        return _found(
            "autopart", f"{brand} {part.oem_number} · {part.name}".strip(),
            f"/autoparts/nomenclature?q={part.oem_number}",
            autopart_id=part.id,
        )

    raise HTTPException(status_code=404, detail="Штрихкод не найден")
