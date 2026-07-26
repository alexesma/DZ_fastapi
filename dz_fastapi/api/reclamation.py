"""API рекламаций (претензий клиентов)."""
import logging
import os
from datetime import date
from typing import Optional

from fastapi import APIRouter, Depends, File, HTTPException, Query, UploadFile, status
from fastapi.responses import FileResponse
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from dz_fastapi.api.deps import get_current_user
from dz_fastapi.core.db import get_session
from dz_fastapi.models.partner import Customer, Reclamation, ReclamationAttachment
from dz_fastapi.models.user import User, UserStatus
from dz_fastapi.schemas.reclamation import (
    EmailOutboxOut,
    ReclamationApplyAndReplyIn,
    ReclamationArmtekDecisionIn,
    ReclamationArmtekSyncResult,
    ReclamationAssignCustomerIn,
    ReclamationAssigneeOut,
    ReclamationCreateIn,
    ReclamationDetail,
    ReclamationFrozaDecisionIn,
    ReclamationItemUpdateIn,
    ReclamationReplyIn,
    ReclamationRow,
    ReclamationShortageAssignIn,
    ReclamationShortageConfirmIn,
    ReclamationShortagePostponeIn,
    ReclamationStats,
    ReclamationSummary,
    ReclamationSyncResult,
    ReclamationUpdateIn,
    ReplyTemplateOut,
)
from dz_fastapi.services.email_outbox import list_outbox_for_source
from dz_fastapi.services.reclamation_armtek import (
    ARMTEK_NOTICE_SENDER,
    ArmtekPortalError,
    refresh_armtek_status,
    send_armtek_decision,
    sync_armtek_open_returns,
)
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
    apply_and_enqueue_customer_reply,
    build_customer_reply_template,
    enqueue_customer_reply,
    enqueue_supplier_request,
)
from dz_fastapi.services.reclamation_stats import get_reclamation_statistics
from dz_fastapi.services.reclamations import (
    add_shortage_evidence,
    assign_reclamation_customer,
    assign_shortage_reviewer,
    confirm_shortage,
    create_manual_reclamation,
    get_reclamations_summary,
    list_reclamations,
    postpone_shortage_review,
    resolve_customer_by_email,
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
    assigned_user = getattr(rec, "shortage_assigned_to_user", None)
    confirmed_user = getattr(rec, "shortage_confirmed_by_user", None)

    def user_name(user: Optional[User]) -> Optional[str]:
        if user is None:
            return None
        return user.name or user.email

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
        shortage_assigned_to_user_id=rec.shortage_assigned_to_user_id,
        shortage_assigned_to_user_name=user_name(assigned_user),
        shortage_assigned_at=rec.shortage_assigned_at,
        shortage_status=rec.shortage_status,
        shortage_confirmed_by_user_id=rec.shortage_confirmed_by_user_id,
        shortage_confirmed_by_user_name=user_name(confirmed_user),
        shortage_confirmed_at=rec.shortage_confirmed_at,
        shortage_comment=rec.shortage_comment,
        shortage_snoozed_until=rec.shortage_snoozed_until,
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


@router.get(
    "/assignees",
    response_model=list[ReclamationAssigneeOut],
    summary="Активные сотрудники для назначения проверки",
)
async def reclamations_assignees(
    session: AsyncSession = Depends(get_session),
):
    users = (
        await session.execute(
            select(User)
            .where(User.status == UserStatus.ACTIVE)
            .order_by(User.name.asc().nullslast(), User.email.asc())
        )
    ).scalars().all()
    return [
        ReclamationAssigneeOut(id=user.id, name=user.name, email=user.email)
        for user in users
    ]


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
            select(Reclamation)
            .where(Reclamation.id == reclamation_id)
            .execution_options(populate_existing=True)
        )
    ).scalar_one()
    customer_name = None
    if rec.customer_id:
        customer = await session.get(Customer, rec.customer_id)
        customer_name = getattr(customer, "name", None)
    return _detail_from_model(rec, customer_name)


@router.post(
    "/{reclamation_id}/shortage/assign",
    response_model=ReclamationDetail,
    summary="Назначить сотрудника для проверки недовоза",
)
async def reclamations_assign_shortage(
    reclamation_id: int,
    payload: ReclamationShortageAssignIn,
    session: AsyncSession = Depends(get_session),
):
    try:
        await run_reclamation_check(
            session,
            reclamation_id=reclamation_id,
        )
        await assign_shortage_reviewer(
            session,
            reclamation_id=reclamation_id,
            user_id=payload.user_id,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return await _reload_detail(session, reclamation_id)


@router.post(
    "/{reclamation_id}/shortage/confirm",
    response_model=ReclamationDetail,
    summary="Подтвердить или опровергнуть недовоз",
)
async def reclamations_confirm_shortage(
    reclamation_id: int,
    payload: ReclamationShortageConfirmIn,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    try:
        await confirm_shortage(
            session,
            reclamation_id=reclamation_id,
            confirmed=payload.confirmed,
            comment=payload.comment,
            user_id=int(current_user.id),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return await _reload_detail(session, reclamation_id)


@router.post(
    "/{reclamation_id}/shortage/postpone",
    response_model=ReclamationDetail,
    summary="Отложить проверку недовоза",
)
async def reclamations_postpone_shortage(
    reclamation_id: int,
    payload: ReclamationShortagePostponeIn,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    try:
        await postpone_shortage_review(
            session,
            reclamation_id=reclamation_id,
            minutes=payload.minutes,
            user_id=int(current_user.id),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return await _reload_detail(session, reclamation_id)


@router.post(
    "/{reclamation_id}/shortage/evidence",
    response_model=ReclamationDetail,
    summary="Приложить фото или видео проверки отгрузки",
)
async def reclamations_upload_shortage_evidence(
    reclamation_id: int,
    files: list[UploadFile] = File(...),
    session: AsyncSession = Depends(get_session),
):
    if not files or len(files) > 5:
        raise HTTPException(
            status_code=400,
            detail="Приложите от 1 до 5 файлов",
        )
    for upload in files:
        content_type = str(upload.content_type or "").lower()
        if not (
            content_type.startswith("image/")
            or content_type.startswith("video/")
        ):
            raise HTTPException(
                status_code=400,
                detail="Разрешены только фото и видео",
            )
        content = await upload.read()
        if len(content) > 50 * 1024 * 1024:
            raise HTTPException(
                status_code=413,
                detail=f"Файл {upload.filename} больше 50 МБ",
            )
        try:
            await add_shortage_evidence(
                session,
                reclamation_id=reclamation_id,
                filename=upload.filename or "evidence",
                payload=content,
                content_type=upload.content_type,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
    return await _reload_detail(session, reclamation_id)


@router.get(
    "/{reclamation_id}/reply-template",
    response_model=ReplyTemplateOut,
    summary="Шаблон ответа клиенту по виду решения",
)
async def reclamations_reply_template(
    reclamation_id: int,
    kind: str = Query(default="ack"),
    resolution_comment: Optional[str] = Query(default=None, max_length=4000),
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
    subject, body_text = build_customer_reply_template(
        rec,
        kind,
        resolution_comment=resolution_comment,
    )
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
    "/{reclamation_id}/apply-and-reply",
    response_model=EmailOutboxOut,
    summary="Применить действие и поставить ответ клиенту в очередь",
)
async def reclamations_apply_and_reply(
    reclamation_id: int,
    payload: ReclamationApplyAndReplyIn,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    try:
        row = await apply_and_enqueue_customer_reply(
            session,
            reclamation_id=reclamation_id,
            action=payload.action,
            resolution_comment=payload.resolution_comment,
            resolved_by_user_id=getattr(current_user, "id", None),
            subject=payload.subject,
            body_text=payload.body_text,
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


@router.post(
    "/armtek/sync",
    response_model=ReclamationArmtekSyncResult,
    summary="Загрузить открытые возвраты из Armtek",
)
async def reclamations_armtek_sync(
    session: AsyncSession = Depends(get_session),
):
    try:
        customer_id = await resolve_customer_by_email(
            session,
            ARMTEK_NOTICE_SENDER,
        )
        return await sync_armtek_open_returns(
            session,
            customer_id=customer_id,
        )
    except ArmtekPortalError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from None


@router.post(
    "/{reclamation_id}/armtek/refresh",
    response_model=ReclamationDetail,
    summary="Проверить текущее состояние рекламации в Armtek",
)
async def reclamations_armtek_refresh(
    reclamation_id: int,
    session: AsyncSession = Depends(get_session),
):
    try:
        await refresh_armtek_status(
            session,
            reclamation_id=reclamation_id,
        )
    except ArmtekPortalError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from None
    return await _reload_detail(session, reclamation_id)


@router.post(
    "/{reclamation_id}/armtek/send-decision",
    response_model=ReclamationDetail,
    summary="Передать сохранённое решение по рекламации в Armtek",
)
async def reclamations_armtek_send_decision(
    reclamation_id: int,
    payload: ReclamationArmtekDecisionIn,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    try:
        await send_armtek_decision(
            session,
            reclamation_id=reclamation_id,
            user_id=getattr(current_user, "id", None),
            comment=payload.comment,
        )
    except ArmtekPortalError as exc:
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
            source_provider_id=data.get("source_provider_id"),
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return await _reload_detail(session, reclamation_id)
