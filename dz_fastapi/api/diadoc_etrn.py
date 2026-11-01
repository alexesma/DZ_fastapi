"""ЭТрН через Диадок: справочники, перевозки, подписание Т1/Т3."""
import json
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.api.deps import require_admin
from dz_fastapi.core.db import get_session
from dz_fastapi.models.etrn import (
    EtrnWaybill,
    TransportAddress,
    TransportCarrier,
    TransportDriver,
    TransportVehicle,
)
from dz_fastapi.models.inventory import ShipmentDocument
from dz_fastapi.models.user import User
from dz_fastapi.schemas.diadoc import DiadocCloudSignTaskOut
from dz_fastapi.schemas.etrn import (
    AddressIn,
    CarrierIn,
    DriverIn,
    T3AcceptIn,
    T6SignIn,
    VehicleIn,
    WaybillDraftIn,
)
from dz_fastapi.services.diadoc_integration import get_diadoc_client_for_session
from dz_fastapi.services.diadoc_outgoing import resolve_diadoc_box_for_customer
from dz_fastapi.services.diadoc_transport import kl_status_label
from dz_fastapi.services.etrn import (
    EtrnError,
    address_snapshot,
    refresh_waybill,
    start_t1_sign,
    start_t3_sign,
    start_t6_sign,
    sync_etrn,
    upsert_outgoing_draft,
)
from dz_fastapi.services.etrn_xml import T3_RESULT_LABELS, EtrnDataError

router = APIRouter(prefix="/diadoc/etrn", tags=["diadoc-etrn"])

DIRECTORIES = {
    "carriers": (TransportCarrier, CarrierIn),
    "drivers": (TransportDriver, DriverIn),
    "vehicles": (TransportVehicle, VehicleIn),
    "addresses": (TransportAddress, AddressIn),
}


def _row(obj: Any) -> dict[str, Any]:
    data = {}
    for column in obj.__table__.columns:
        value = getattr(obj, column.name)
        if hasattr(value, "isoformat"):
            value = value.isoformat()
        elif value is not None and column.type.__class__.__name__ == "Numeric":
            value = float(value)
        data[column.name] = value
    return data


def _http(exc: Exception):
    from dz_fastapi.api.diadoc import _raise_diadoc_http_error

    if isinstance(exc, (EtrnError, EtrnDataError)):
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    _raise_diadoc_http_error(exc)


def waybill_out(item: EtrnWaybill) -> dict[str, Any]:
    payload = item.payload or {}
    summary = payload.get("summary") or {}
    return {
        "id": item.id,
        "direction": item.direction,
        "status": item.status,
        "number": item.number,
        "waybill_date": item.waybill_date.isoformat()
        if item.waybill_date
        else None,
        "mt_id": item.mt_id,
        "kl_status": item.kl_status,
        "stage_label": kl_status_label(item.kl_status),
        "gis_status_type": item.gis_status_type,
        "gis_status_text": item.gis_status_text,
        "t3_state": item.t3_state,
        "t3_result": item.t3_result,
        "t3_result_label": T3_RESULT_LABELS.get(item.t3_result or ""),
        "t3_data": item.t3_data,
        "last_error": item.last_error,
        "shipment_id": item.shipment_id,
        "customer_id": item.customer_id,
        "customer_name": item.customer.name if item.customer else None,
        "carrier_id": item.carrier_id,
        "carrier_name": (
            item.carrier.name
            if item.carrier
            else (payload.get("carrier") or {}).get("name")
            or (summary.get("carrier") or {}).get("name")
        ),
        "shipper": summary.get("shipper"),
        "consignee": summary.get("consignee"),
        "delivery_address": summary.get("delivery_address"),
        "cargo": summary.get("cargo") or payload.get("cargo"),
        "summary": summary or None,
        "payload": payload if item.direction == "outgoing" else None,
        "t1_message_id": item.t1_message_id,
        "driver_confirm": payload.get("driver_confirm"),
        "cost": payload.get("cost"),
        "last_synced_at": item.last_synced_at.isoformat()
        if item.last_synced_at
        else None,
        "sent_at": item.sent_at.isoformat() if item.sent_at else None,
    }


async def _integration(session: AsyncSession):
    return await get_diadoc_client_for_session(session)


# ------------------------------------------------------- справочники


def _register(name: str, model: Any, schema: Any) -> None:
    @router.get(f"/{name}", name=f"list_{name}")
    async def list_items(
        include_inactive: bool = False,
        session: AsyncSession = Depends(get_session),
        _: User = Depends(require_admin),
    ):
        stmt = select(model).order_by(model.id.desc())
        if not include_inactive:
            stmt = stmt.where(model.is_active.is_(True))
        return [_row(r) for r in (await session.execute(stmt)).scalars()]

    @router.post(f"/{name}", name=f"create_{name}", status_code=201)
    async def create_item(
        payload: schema,  # type: ignore[valid-type]
        session: AsyncSession = Depends(get_session),
        _: User = Depends(require_admin),
    ):
        obj = model(**payload.model_dump())
        session.add(obj)
        await session.commit()
        await session.refresh(obj)
        return _row(obj)

    @router.put(f"/{name}/{{item_id}}", name=f"update_{name}")
    async def update_item(
        item_id: int,
        payload: schema,  # type: ignore[valid-type]
        session: AsyncSession = Depends(get_session),
        _: User = Depends(require_admin),
    ):
        obj = await session.get(model, item_id)
        if obj is None:
            raise HTTPException(status_code=404, detail="Не найдено")
        for key, value in payload.model_dump().items():
            setattr(obj, key, value)
        await session.commit()
        await session.refresh(obj)
        return _row(obj)

    @router.delete(f"/{name}/{{item_id}}", name=f"delete_{name}")
    async def delete_item(
        item_id: int,
        session: AsyncSession = Depends(get_session),
        _: User = Depends(require_admin),
    ):
        obj = await session.get(model, item_id)
        if obj is None:
            raise HTTPException(status_code=404, detail="Не найдено")
        # Мягкое удаление: записи могут быть в снимках прошлых перевозок.
        obj.is_active = False
        await session.commit()
        return {"ok": True}


for _name, (_model, _schema) in DIRECTORIES.items():
    _register(_name, _model, _schema)


# ---------------------------------------------------------- перевозки


@router.get("/waybills")
async def list_waybills(
    direction: Optional[str] = None,
    limit: int = 100,
    session: AsyncSession = Depends(get_session),
    _: User = Depends(require_admin),
):
    stmt = (
        select(EtrnWaybill)
        .order_by(EtrnWaybill.id.desc())
        .limit(max(1, min(limit, 300)))
    )
    if direction:
        stmt = stmt.where(EtrnWaybill.direction == direction)
    rows = (await session.execute(stmt)).scalars().all()
    return [waybill_out(r) for r in rows]


async def _waybill(session: AsyncSession, waybill_id: int) -> EtrnWaybill:
    row = await session.get(EtrnWaybill, waybill_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Перевозка не найдена")
    return row


@router.get("/waybills/{waybill_id}")
async def get_waybill(
    waybill_id: int,
    session: AsyncSession = Depends(get_session),
    _: User = Depends(require_admin),
):
    return waybill_out(await _waybill(session, waybill_id))


@router.get("/shipments/{shipment_id}/prefill")
async def prefill_from_shipment(
    shipment_id: int,
    session: AsyncSession = Depends(get_session),
    _: User = Depends(require_admin),
):
    """Данные отгрузки клиенту для формы Т1."""
    shipment = await session.get(ShipmentDocument, shipment_id)
    if shipment is None:
        raise HTTPException(status_code=404, detail="Отгрузка не найдена")
    customer_box = None
    if shipment.customer_id:
        customer_box = await resolve_diadoc_box_for_customer(
            session, customer_id=int(shipment.customer_id)
        )
    addresses = []
    if shipment.customer_id:
        addresses = [
            _row(a)
            for a in (
                await session.execute(
                    select(TransportAddress).where(
                        TransportAddress.customer_id == shipment.customer_id,
                        TransportAddress.is_active.is_(True),
                    )
                )
            ).scalars()
        ]
    number = shipment.doc_number or f"SHIP-{shipment.id}"
    return {
        "shipment_id": int(shipment.id),
        "customer_id": shipment.customer_id,
        "number": f"ТрН-{number}",
        "date": shipment.doc_date.date().isoformat(),
        "order_number": number,
        "consignee_box_id": customer_box,
        "customer_addresses": addresses,
    }


@router.post("/waybills", status_code=201)
async def create_waybill_draft(
    body: WaybillDraftIn,
    session: AsyncSession = Depends(get_session),
    _: User = Depends(require_admin),
):
    try:
        integration, _client = await _integration(session)
        data = json.loads(body.model_dump_json())
        waybill = await upsert_outgoing_draft(
            session, integration=integration, data=data
        )
        return waybill_out(waybill)
    except Exception as exc:
        _http(exc)


@router.put("/waybills/{waybill_id}")
async def update_waybill_draft(
    waybill_id: int,
    body: WaybillDraftIn,
    session: AsyncSession = Depends(get_session),
    _: User = Depends(require_admin),
):
    try:
        integration, _client = await _integration(session)
        waybill = await _waybill(session, waybill_id)
        data = json.loads(body.model_dump_json())
        waybill = await upsert_outgoing_draft(
            session, integration=integration, data=data, waybill=waybill
        )
        return waybill_out(waybill)
    except HTTPException:
        raise
    except Exception as exc:
        _http(exc)


@router.post(
    "/waybills/{waybill_id}/sign", response_model=DiadocCloudSignTaskOut
)
async def sign_t1(
    waybill_id: int,
    session: AsyncSession = Depends(get_session),
    _: User = Depends(require_admin),
):
    """Шаг 1: Т1 уходит на облачную подпись директора (SMS)."""
    try:
        integration, client = await _integration(session)
        waybill = await _waybill(session, waybill_id)
        task = await start_t1_sign(
            session, client=client, integration=integration, waybill=waybill
        )
        return DiadocCloudSignTaskOut.model_validate(task)
    except HTTPException:
        raise
    except Exception as exc:
        _http(exc)


@router.post(
    "/waybills/{waybill_id}/accept", response_model=DiadocCloudSignTaskOut
)
async def accept_t3(
    waybill_id: int,
    body: T3AcceptIn,
    session: AsyncSession = Depends(get_session),
    _: User = Depends(require_admin),
):
    """Шаг 1: Т3 (приёмка полная/частичная/отказ) на облачную подпись."""
    try:
        integration, client = await _integration(session)
        waybill = await _waybill(session, waybill_id)
        form = json.loads(body.model_dump_json())
        if body.delivery_address_id:
            address = await session.get(
                TransportAddress, body.delivery_address_id
            )
            if address is None:
                raise EtrnError("Адрес выгрузки не найден")
            form["delivery_address"] = address_snapshot(address)
        task = await start_t3_sign(
            session,
            client=client,
            integration=integration,
            waybill=waybill,
            form=form,
        )
        return DiadocCloudSignTaskOut.model_validate(task)
    except HTTPException:
        raise
    except Exception as exc:
        _http(exc)


@router.post(
    "/waybills/{waybill_id}/t6", response_model=DiadocCloudSignTaskOut
)
async def sign_t6(
    waybill_id: int,
    body: T6SignIn,
    session: AsyncSession = Depends(get_session),
    _: User = Depends(require_admin),
):
    """Т6 (стоимость) — только при включённом флаге Т6."""
    try:
        integration, client = await _integration(session)
        waybill = await _waybill(session, waybill_id)
        task = await start_t6_sign(
            session,
            client=client,
            integration=integration,
            waybill=waybill,
            form=json.loads(body.model_dump_json()),
        )
        return DiadocCloudSignTaskOut.model_validate(task)
    except HTTPException:
        raise
    except Exception as exc:
        _http(exc)


@router.post("/waybills/{waybill_id}/refresh")
async def refresh_one(
    waybill_id: int,
    session: AsyncSession = Depends(get_session),
    _: User = Depends(require_admin),
):
    try:
        _integration_row, client = await _integration(session)
        waybill = await _waybill(session, waybill_id)
        await refresh_waybill(session, client=client, waybill=waybill)
        await session.commit()
        await session.refresh(waybill)
        return waybill_out(waybill)
    except HTTPException:
        raise
    except Exception as exc:
        _http(exc)


@router.post("/sync")
async def sync_waybills(
    session: AsyncSession = Depends(get_session),
    _: User = Depends(require_admin),
):
    try:
        integration, client = await _integration(session)
        return await sync_etrn(
            session, client=client, integration=integration
        )
    except Exception as exc:
        _http(exc)
