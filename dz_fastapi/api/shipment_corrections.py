"""Agreed changes to posted customer realizations."""

from fastapi import APIRouter, Depends, HTTPException, Query, Response
from fastapi.responses import HTMLResponse
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.api.deps import get_current_user, require_admin
from dz_fastapi.core.db import get_session
from dz_fastapi.models.diadoc import DiadocOutgoingDocument
from dz_fastapi.models.user import User
from dz_fastapi.schemas.shipment_correction import ShipmentCorrectionOut, ShipmentCorrectionSave
from dz_fastapi.services.diadoc_integration import get_diadoc_client_for_session
from dz_fastapi.services.shipment_corrections import (
    get_correction,
    list_corrections,
    post_correction,
    save_correction,
    serialize_correction,
)

router = APIRouter(
    prefix="/inventory/shipment-corrections",
    tags=["inventory"],
    dependencies=[Depends(get_current_user)],
)


def _out(doc) -> ShipmentCorrectionOut:
    return ShipmentCorrectionOut.model_validate(serialize_correction(doc))


@router.get("", response_model=list[ShipmentCorrectionOut])
async def list_shipment_corrections(
    shipment_id: int | None = Query(default=None),
    session: AsyncSession = Depends(get_session),
):
    rows = await list_corrections(session, shipment_id=shipment_id)
    return [_out(row) for row in rows]


@router.post("", response_model=ShipmentCorrectionOut)
async def create_shipment_correction(
    payload: ShipmentCorrectionSave,
    session: AsyncSession = Depends(get_session),
    user: User = Depends(require_admin),
):
    try:
        return _out(await save_correction(session, payload, user_id=user.id))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/{correction_id}", response_model=ShipmentCorrectionOut)
async def get_shipment_correction(
    correction_id: int,
    session: AsyncSession = Depends(get_session),
):
    doc = await get_correction(session, correction_id)
    if doc is None:
        raise HTTPException(status_code=404, detail="Корректировка не найдена")
    return _out(doc)


@router.put("/{correction_id}", response_model=ShipmentCorrectionOut)
async def update_shipment_correction(
    correction_id: int,
    payload: ShipmentCorrectionSave,
    session: AsyncSession = Depends(get_session),
    _: User = Depends(require_admin),
):
    try:
        return _out(await save_correction(
            session, payload, correction_id=correction_id
        ))
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/{correction_id}/post", response_model=ShipmentCorrectionOut)
async def post_shipment_correction(
    correction_id: int,
    session: AsyncSession = Depends(get_session),
    _: User = Depends(require_admin),
):
    try:
        return _out(await post_correction(session, correction_id))
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/{correction_id}/print/ukd", response_class=HTMLResponse)
async def print_shipment_correction(
    correction_id: int,
    session: AsyncSession = Depends(get_session),
):
    from dz_fastapi.services.shipment_correction_print import render_ukd

    doc = await get_correction(session, correction_id)
    if doc is None:
        raise HTTPException(status_code=404, detail="Корректировка не найдена")
    return HTMLResponse(await render_ukd(session, doc))


@router.get("/{correction_id}/print/diadoc-pdf")
async def print_formalized_shipment_correction(
    correction_id: int,
    session: AsyncSession = Depends(get_session),
    _: User = Depends(require_admin),
):
    doc = await get_correction(session, correction_id)
    if doc is None:
        raise HTTPException(status_code=404, detail="Корректировка не найдена")
    outgoing = (
        await session.get(DiadocOutgoingDocument, doc.diadoc_outgoing_document_id)
        if doc.diadoc_outgoing_document_id else None
    )
    if outgoing is None or not outgoing.message_id or not outgoing.entity_id:
        raise HTTPException(
            status_code=409, detail="УКД ещё не сформирован в Диадоке"
        )
    _integration, client = await get_diadoc_client_for_session(session)
    content = await client.generate_print_form(
        box_id_guid=outgoing.from_box_id_guid,
        message_id=outgoing.message_id,
        document_id=outgoing.entity_id,
    )
    return Response(
        content=content,
        media_type="application/pdf",
        headers={"Content-Disposition": f'inline; filename="ukd-{doc.id}.pdf"'},
    )
