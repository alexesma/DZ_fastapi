"""API рекламаций (претензий клиентов)."""
import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.api.deps import get_current_user
from dz_fastapi.core.db import get_session
from dz_fastapi.models.partner import Customer, Reclamation
from dz_fastapi.models.user import User
from dz_fastapi.schemas.reclamation import (
    ReclamationAssignCustomerIn,
    ReclamationCreateIn,
    ReclamationDetail,
    ReclamationRow,
    ReclamationSummary,
    ReclamationSyncResult,
)
from dz_fastapi.services.reclamations import (
    assign_reclamation_customer,
    create_manual_reclamation,
    get_reclamations_summary,
    list_reclamations,
    sync_reclamation_mailbox,
)

logger = logging.getLogger("dz_fastapi")

router = APIRouter(
    prefix="/reclamations",
    tags=["reclamation"],
    dependencies=[Depends(get_current_user)],
)


def _detail_from_model(rec: Reclamation, customer_name: Optional[str]):
    return ReclamationDetail(
        id=int(rec.id),
        source=str(getattr(rec.source, "value", rec.source)),
        status=str(getattr(rec.status, "value", rec.status)),
        reclamation_type=(
            str(getattr(rec.reclamation_type, "value", rec.reclamation_type))
            if rec.reclamation_type
            else None
        ),
        customer_id=rec.customer_id,
        customer_name=customer_name,
        sender_email=rec.sender_email,
        source_link=rec.source_link,
        email_subject=rec.email_subject,
        email_body=rec.email_body,
        email_received_at=rec.email_received_at,
        stated_document_number=rec.stated_document_number,
        stated_document_date=rec.stated_document_date,
        stated_reason=rec.stated_reason,
        extracted_data=rec.extracted_data or {},
        check_result=rec.check_result or {},
        recommendation=rec.recommendation,
        resolution=rec.resolution,
        resolution_comment=rec.resolution_comment,
        resolved_at=rec.resolved_at,
        return_from_customer_id=rec.return_from_customer_id,
        created_at=rec.created_at,
        items=list(rec.items or []),
        attachments=list(rec.attachments or []),
    )


@router.get("/summary", response_model=ReclamationSummary)
async def reclamations_summary(
    session: AsyncSession = Depends(get_session),
):
    return await get_reclamations_summary(session)


@router.get("", response_model=list[ReclamationRow])
async def reclamations_list(
    status_filter: Optional[str] = Query(default=None, alias="status"),
    customer_id: Optional[int] = Query(default=None),
    without_customer: bool = Query(default=False),
    limit: int = Query(default=100, ge=1, le=500),
    session: AsyncSession = Depends(get_session),
):
    return await list_reclamations(
        session,
        status=status_filter,
        customer_id=customer_id,
        without_customer=without_customer,
        limit=limit,
    )


@router.post(
    "",
    response_model=ReclamationDetail,
    status_code=status.HTTP_201_CREATED,
    summary="Завести рекламацию вручную",
)
async def reclamations_create(
    payload: ReclamationCreateIn,
    session: AsyncSession = Depends(get_session),
):
    rec = await create_manual_reclamation(
        session,
        customer_id=payload.customer_id,
        sender_email=payload.sender_email,
        subject=payload.subject,
        body=payload.body,
        source_link=payload.source_link,
    )
    customer_name = None
    if rec.customer_id:
        customer = await session.get(Customer, rec.customer_id)
        customer_name = getattr(customer, "name", None)
    return _detail_from_model(rec, customer_name)


@router.post(
    "/sync",
    response_model=ReclamationSyncResult,
    summary="Прочитать ящик рекламаций и создать новые",
)
async def reclamations_sync(
    session: AsyncSession = Depends(get_session),
    _: User = Depends(get_current_user),
):
    try:
        result = await sync_reclamation_mailbox(session)
        return ReclamationSyncResult(**result)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/{reclamation_id}", response_model=ReclamationDetail)
async def reclamations_get(
    reclamation_id: int,
    session: AsyncSession = Depends(get_session),
):
    rec = await session.get(Reclamation, reclamation_id)
    if rec is None:
        raise HTTPException(status_code=404, detail="Рекламация не найдена")
    customer_name = None
    if rec.customer_id:
        customer = await session.get(Customer, rec.customer_id)
        customer_name = getattr(customer, "name", None)
    return _detail_from_model(rec, customer_name)


@router.post(
    "/{reclamation_id}/assign-customer",
    response_model=ReclamationDetail,
    summary="Привязать клиента к рекламации",
)
async def reclamations_assign_customer(
    reclamation_id: int,
    payload: ReclamationAssignCustomerIn,
    session: AsyncSession = Depends(get_session),
):
    try:
        rec = await assign_reclamation_customer(
            session,
            reclamation_id=reclamation_id,
            customer_id=payload.customer_id,
            remember_email=payload.remember_email,
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    customer = await session.get(Customer, rec.customer_id)
    # перезагружаем связи
    rec = (
        await session.execute(
            select(Reclamation).where(Reclamation.id == reclamation_id)
        )
    ).scalar_one()
    return _detail_from_model(rec, getattr(customer, "name", None))
