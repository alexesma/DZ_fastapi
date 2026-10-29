"""Universal barcode lookup: what does a scanned code belong to."""

import re

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from dz_fastapi.api.deps import get_current_user, require_admin
from dz_fastapi.core.db import get_session
from dz_fastapi.core.time import now_moscow
from dz_fastapi.models.autopart import AutoPart, ScanLog, ScannedCodeAlias, StorageLocation
from dz_fastapi.models.brand import Brand
from dz_fastapi.models.inventory import (
    CrossDockingLabel,
    CustomerOrderPickLabel,
    ProductionWaveLabel,
    ShipmentDocument,
    StockOrderPackage,
)
from dz_fastapi.models.partner import StockOrderItem
from dz_fastapi.models.user import User
from dz_fastapi.services.scan_codes import find_autoparts

router = APIRouter(
    prefix="/scan",
    tags=["scan"],
    dependencies=[Depends(get_current_user)],
)

_BOX_RE = re.compile(r"^DZ-BOX-(\d+)-(\d+)$")


def _found(kind: str, title: str, route: str, **extra) -> dict:
    return {"kind": kind, "title": title, "route": route, **extra}


async def _resolve(session: AsyncSession, code: str) -> dict:
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

    location = (await session.execute(
        select(StorageLocation).where(
            func.upper(StorageLocation.name) == code.upper())
    )).scalars().first()
    if location is not None:
        return _found(
            "location", f"Место {location.name}", "/warehouse/storage",
            storage_location_id=location.id, name=location.name,
        )

    parts = await find_autoparts(session, code)
    if parts:
        options = [{
            "autopart_id": part.id,
            "brand": part.brand.name if part.brand else "",
            "oem": part.oem_number, "name": part.name,
        } for part in parts]
        part = parts[0]
        return _found(
            "autopart" if len(parts) == 1 else "autopart_choice",
            f"{options[0]['brand']} {part.oem_number} · {part.name}".strip(),
            f"/autoparts/nomenclature?autopart_id={part.id}&edit=1",
            autopart_id=part.id, options=options,
        )

    raise HTTPException(status_code=404, detail="Штрихкод не найден")


class AliasPayload(BaseModel):
    code: str = Field(min_length=1, max_length=128)
    autopart_id: int


@router.get("/resolve")
async def resolve_scan(code: str, context: str | None = None,
                       session: AsyncSession = Depends(get_session),
                       user: User = Depends(get_current_user)):
    try:
        result = await _resolve(session, code)
    except HTTPException as exc:
        if exc.status_code == 404:
            session.add(ScanLog(code=code.strip()[:128], found=False,
                                source="server", context=context,
                                user_id=user.id))
            await session.commit()
        raise
    session.add(ScanLog(code=code.strip()[:128], found=True,
                        kind=result.get("kind"), source="server",
                        context=context, user_id=user.id))
    await session.commit()
    return result


class ScanEvent(BaseModel):
    code: str = Field(min_length=1, max_length=128)
    kind: str | None = Field(default=None, max_length=40)
    found: bool
    context: str | None = Field(default=None, max_length=60)


class ScanEvents(BaseModel):
    events: list[ScanEvent] = Field(max_length=200)


@router.post("/log", status_code=204)
async def log_local_scans(payload: ScanEvents,
                          session: AsyncSession = Depends(get_session),
                          user: User = Depends(get_current_user)):
    """Сканы, распознанные на устройстве по локальному кешу."""
    for event in payload.events:
        session.add(ScanLog(code=event.code, kind=event.kind,
                            found=event.found, source="local",
                            context=event.context, user_id=user.id))
    await session.commit()


@router.get("/log")
async def list_scan_log(limit: int = 200, only_missed: bool = False,
                        session: AsyncSession = Depends(get_session),
                        _admin: User = Depends(require_admin)):
    query = select(ScanLog).order_by(ScanLog.id.desc()).limit(
        max(1, min(limit, 1000)))
    if only_missed:
        query = query.where(ScanLog.found.is_(False))
    rows = (await session.execute(query)).scalars().all()
    return [{"id": r.id, "code": r.code, "kind": r.kind, "found": r.found,
             "source": r.source, "context": r.context, "user_id": r.user_id,
             "created_at": r.created_at} for r in rows]


@router.get("/catalog")
async def scan_catalog(session: AsyncSession = Depends(get_session)):
    """Компактный каталог для распознавания кодов на устройстве без сервера."""
    parts = (await session.execute(
        select(AutoPart.id, AutoPart.oem_number, AutoPart.barcode,
               Brand.name, AutoPart.name)
        .join(Brand, Brand.id == AutoPart.brand_id, isouter=True)
    )).all()
    aliases = (await session.execute(
        select(ScannedCodeAlias.code, ScannedCodeAlias.autopart_id)
    )).all()
    locations = (await session.execute(
        select(StorageLocation.id, StorageLocation.name)
    )).all()
    return {
        "generated_at": now_moscow().isoformat(),
        "parts": [list(row) for row in parts],
        "aliases": [list(row) for row in aliases],
        "locations": [list(row) for row in locations],
    }


@router.post("/alias", status_code=201)
async def bind_code(payload: AliasPayload,
                    session: AsyncSession = Depends(get_session),
                    user: User = Depends(get_current_user)):
    """Запомнить, что этот код относится к товару (для неузнанных кодов)."""
    code = payload.code.strip()
    if await session.get(AutoPart, payload.autopart_id) is None:
        raise HTTPException(status_code=404, detail="Товар не найден")
    existing = (await session.execute(
        select(ScannedCodeAlias).where(ScannedCodeAlias.code == code)
    )).scalar_one_or_none()
    if existing is None:
        session.add(ScannedCodeAlias(
            code=code, autopart_id=payload.autopart_id,
            created_by_user_id=user.id,
        ))
    else:
        existing.autopart_id = payload.autopart_id
    await session.commit()
    return {"code": code, "autopart_id": payload.autopart_id}
