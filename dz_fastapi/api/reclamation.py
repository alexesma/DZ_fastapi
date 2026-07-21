"""API рекламаций (претензий клиентов)."""
import logging
import os
from datetime import date
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.responses import FileResponse
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from dz_fastapi.api.deps import get_current_user
from dz_fastapi.core.db import get_session
from dz_fastapi.models.partner import Customer, Reclamation, ReclamationAttachment
from dz_fastapi.models.user import User
from dz_fastapi.schemas.reclamation import (
    EmailOutboxOut,
    ReclamationAssignCustomerIn,
    ReclamationCreateIn,
    ReclamationDetail,
    ReclamationFrozaDecisionIn,
    ReclamationItemUpdateIn,
    ReclamationReplyIn,
    ReclamationRow,
    ReclamationStats,
    ReclamationSummary,
    ReclamationSyncResult,
    ReclamationUpdateIn,
    ReplyTemplateOut,
)
from dz_fastapi.services.email_outbox import list_outbox_for_source
from dz_fastapi.services.reclamation_check import run_reclamation_check
from dz_fastapi.services.reclamation_froza import (
    FrozaPortalError,
    refresh_froza_status,
    send_froza_decision,
)
from dz_fastapi.services.reclamation_replies import (
    OUTBOX_SOURCE_CUSTOMER,
    OUTBOX_SOURCE_SUPPLIER,
    REPLY_KINDS,
    build_customer_reply_template,
    enqueue_customer_reply,
    enqueue_supplier_request,
)
from dz_fastapi.services.reclamation_stats import get_reclamation_statistics
from dz_fastapi.services.reclamations import (
    assign_reclamation_customer,
    create_manual_reclamation,
    get_reclamations_summary,
    list_reclamations,
    sync_reclamation_mailbox,
    update_reclamation,
    update_reclamation_item,
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


@router.get("/stats", response_model=ReclamationStats)
async def reclamations_stats(
    date_from: Optional[date] = Query(default=None),
    date_to: Optional[date] = Query(default=None),
    top_limit: int = Query(default=10, ge=1, le=50),
    session: AsyncSession = Depends(get_session),
):
    return await get_reclamation_statistics(
        session,
        date_from=date_from,
        date_to=date_to,
        top_limit=top_limit,
    )


@router.get("", response_model=list[ReclamationRow])
async def reclamations_list(
    status_filter: Optional[str] = Query(default=None, alias="status"),
    customer_id: Optional[int] = Query(default=None),
    without_customer: bool = Query(default=False),
    order: str = Query(default="newest"),
    limit: int = Query(default=100, ge=1, le=500),
    session: AsyncSession = Depends(get_session),
):
    return await list_reclamations(
        session,
        status=status_filter,
        customer_id=customer_id,
        without_customer=without_customer,
        order=order,
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


@router.get(
    "/{reclamation_id}/attachments/{attachment_id}/download",
    summary="Скачать вложение рекламации",
)
async def reclamations_download_attachment(
    reclamation_id: int,
    attachment_id: int,
    session: AsyncSession = Depends(get_session),
):
    attachment = await session.get(ReclamationAttachment, attachment_id)
    if (
        attachment is None
        or int(attachment.reclamation_id) != reclamation_id
        or not attachment.local_file_path
    ):
        raise HTTPException(status_code=404, detail="Вложение не найдено")

    file_path = os.path.abspath(attachment.local_file_path)
    allowed_root = os.path.abspath("uploads/reclamation_attachments")
    if (
        os.path.commonpath((file_path, allowed_root)) != allowed_root
        or not os.path.isfile(file_path)
    ):
        raise HTTPException(status_code=404, detail="Файл вложения не найден")

    return FileResponse(
        file_path,
        media_type=attachment.content_type or "application/octet-stream",
        filename=attachment.file_name or os.path.basename(file_path),
    )


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


async def _reload_detail(session: AsyncSession, reclamation_id: int):
    rec = (
        await session.execute(
            select(Reclamation).where(Reclamation.id == reclamation_id)
        )
    ).scalar_one()
    customer_name = None
    if rec.customer_id:
        customer = await session.get(Customer, rec.customer_id)
        customer_name = getattr(customer, "name", None)
    return _detail_from_model(rec, customer_name)


@router.get(
    "/{reclamation_id}/reply-template",
    response_model=ReplyTemplateOut,
    summary="Шаблон ответа клиенту по виду решения",
)
async def reclamations_reply_template(
    reclamation_id: int,
    kind: str = Query(default="ack"),
    session: AsyncSession = Depends(get_session),
):
    if kind not in REPLY_KINDS:
        raise HTTPException(status_code=400, detail="Неизвестный вид ответа")
    rec = (
        await session.execute(
            select(Reclamation)
            .where(Reclamation.id == reclamation_id)
            .options(selectinload(Reclamation.items))
        )
    ).scalar_one_or_none()
    if rec is None:
        raise HTTPException(status_code=404, detail="Рекламация не найдена")
    subject, body_text = build_customer_reply_template(rec, kind)
    return ReplyTemplateOut(kind=kind, subject=subject, body_text=body_text)


@router.post(
    "/{reclamation_id}/reply",
    response_model=EmailOutboxOut,
    summary="Поставить ответ клиенту в очередь отправки",
)
async def reclamations_reply(
    reclamation_id: int,
    payload: ReclamationReplyIn,
    session: AsyncSession = Depends(get_session),
):
    try:
        row = await enqueue_customer_reply(
            session,
            reclamation_id=reclamation_id,
            subject=payload.subject,
            body_text=payload.body_text,
            kind=payload.kind,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return row


@router.post(
    "/{reclamation_id}/notify-supplier",
    response_model=list[EmailOutboxOut],
    summary="Поставить запрос поставщику в очередь отправки",
)
async def reclamations_notify_supplier(
    reclamation_id: int,
    session: AsyncSession = Depends(get_session),
):
    try:
        rows = await enqueue_supplier_request(
            session, reclamation_id=reclamation_id
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return rows


@router.get(
    "/{reclamation_id}/emails",
    response_model=list[EmailOutboxOut],
    summary="Письма очереди по рекламации",
)
async def reclamations_emails(
    reclamation_id: int,
    session: AsyncSession = Depends(get_session),
):
    rows = await list_outbox_for_source(
        session,
        source_type=OUTBOX_SOURCE_CUSTOMER,
        source_id=reclamation_id,
    )
    rows += await list_outbox_for_source(
        session,
        source_type=OUTBOX_SOURCE_SUPPLIER,
        source_id=reclamation_id,
    )
    rows.sort(key=lambda r: int(r.id), reverse=True)
    return rows


@router.post(
    "/{reclamation_id}/check",
    response_model=ReclamationDetail,
    summary="Прогнать движок проверки и получить рекомендацию",
)
async def reclamations_check(
    reclamation_id: int,
    session: AsyncSession = Depends(get_session),
):
    try:
        await run_reclamation_check(
            session, reclamation_id=reclamation_id
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return await _reload_detail(session, reclamation_id)


@router.post(
    "/{reclamation_id}/froza/refresh",
    response_model=ReclamationDetail,
    summary="Проверить текущее состояние рекламации во Froza",
)
async def reclamations_froza_refresh(
    reclamation_id: int,
    session: AsyncSession = Depends(get_session),
):
    try:
        await refresh_froza_status(
            session,
            reclamation_id=reclamation_id,
        )
    except FrozaPortalError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from None
    return await _reload_detail(session, reclamation_id)


@router.post(
    "/{reclamation_id}/froza/send-decision",
    response_model=ReclamationDetail,
    summary="Передать сохранённое решение по рекламации во Froza",
)
async def reclamations_froza_send_decision(
    reclamation_id: int,
    payload: ReclamationFrozaDecisionIn,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    try:
        await send_froza_decision(
            session,
            reclamation_id=reclamation_id,
            user_id=getattr(current_user, "id", None),
            comment=payload.comment,
        )
    except FrozaPortalError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from None
    return await _reload_detail(session, reclamation_id)


@router.patch(
    "/{reclamation_id}",
    response_model=ReclamationDetail,
    summary="Обновить статус/тип/решение рекламации",
)
async def reclamations_update(
    reclamation_id: int,
    payload: ReclamationUpdateIn,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    data = payload.model_dump(exclude_unset=True)
    try:
        await update_reclamation(
            session,
            reclamation_id=reclamation_id,
            status=data.get("status"),
            reclamation_type=data.get("reclamation_type"),
            resolution=data.get("resolution"),
            resolution_comment=data.get("resolution_comment"),
            resolved_by_user_id=getattr(current_user, "id", None),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return await _reload_detail(session, reclamation_id)


@router.patch(
    "/{reclamation_id}/items/{item_id}",
    response_model=ReclamationDetail,
    summary="Обновить позицию рекламации (источник/причина/кол-во)",
)
async def reclamations_update_item(
    reclamation_id: int,
    item_id: int,
    payload: ReclamationItemUpdateIn,
    session: AsyncSession = Depends(get_session),
):
    data = payload.model_dump(exclude_unset=True)
    try:
        await update_reclamation_item(
            session,
            reclamation_id=reclamation_id,
            item_id=item_id,
            item_source=data.get("item_source"),
            reason=data.get("reason"),
            quantity=data.get("quantity"),
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return await _reload_detail(session, reclamation_id)
