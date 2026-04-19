import logging
import time
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.api.deps import get_current_user, require_admin
from dz_fastapi.core.db import get_session
from dz_fastapi.crud.inbox_email import (create_rule_pattern,
                                         delete_rule_pattern, get_inbox_email,
                                         get_rule_pattern, list_inbox_emails,
                                         list_rule_patterns,
                                         update_rule_pattern)
from dz_fastapi.models.inbox_email import InboxEmail
from dz_fastapi.models.user import User
from dz_fastapi.schemas.inbox_email import (AssignRuleRequest,
                                            AssignRuleResponse,
                                            ConfigSetupInfo,
                                            EmailRulePatternCreate,
                                            EmailRulePatternOut,
                                            EmailRulePatternUpdate,
                                            FetchInboxRequest,
                                            FetchInboxResponse,
                                            ForceProcessRequest,
                                            ForceProcessResponse,
                                            InboxEmailBrief, InboxEmailDetail,
                                            InboxEmailListResponse,
                                            InboxSetupOptions,
                                            InboxSetupRequest,
                                            InboxSetupResponse, SetupOption)
from dz_fastapi.services.inbox_email import (
    assign_rule, fetch_and_store_emails, force_process_email,
    inbox_attachment_exists, read_attachment_preview,
    resolve_inbox_attachment_fs_path,
    restore_inbox_email_attachments_from_source, setup_email_rule)

logger = logging.getLogger('dz_fastapi')

router = APIRouter(prefix='/inbox', tags=['inbox'])


# ---------------------------------------------------------------------------
# Письма
# ---------------------------------------------------------------------------

@router.get(
    '/emails',
    response_model=InboxEmailListResponse,
    summary='Список входящих писем',
)
async def list_emails(
    email_account_id: Optional[int] = Query(
        default=None, description='ID почтового ящика (None = все)'
    ),
    days: int = Query(
        default=3, ge=1, le=7,
        description='Глубина выборки в днях (1..7, по умолчанию 3)',
    ),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
    only_unprocessed: bool = Query(
        default=False, description='Только письма без правила'
    ),
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    items, total = await list_inbox_emails(
        session,
        email_account_id=email_account_id,
        days=days,
        page=page,
        page_size=page_size,
        only_unprocessed=only_unprocessed,
    )
    return InboxEmailListResponse(
        items=[InboxEmailBrief.model_validate(e) for e in items],
        total=total,
        page=page,
        page_size=page_size,
    )


@router.get(
    '/emails/{email_id}',
    response_model=InboxEmailDetail,
    summary='Детали письма (с полным телом)',
)
async def get_email_detail(
    email_id: int,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    email = await get_inbox_email(session, email_id)
    if email is None:
        raise HTTPException(status_code=404, detail='Письмо не найдено')
    return InboxEmailDetail.model_validate(email)


@router.get(
    '/emails/{email_id}/attachment-preview',
    summary='Предпросмотр вложения письма (XLS/XLSX/CSV)',
)
async def get_attachment_preview(
    email_id: int,
    attachment_index: int = Query(default=0, ge=0),
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    """
    Возвращает первые 25 строк вложения (XLS/XLSX/CSV) для предпросмотра.
    Если файл не был сохранён на диск (письмо получено до введения
    этой функции) — возвращает 404.
    """
    started = time.perf_counter()

    email = await get_inbox_email(session, email_id)
    if email is None:
        raise HTTPException(status_code=404, detail='Письмо не найдено')

    att_info = email.attachment_info or []
    if attachment_index >= len(att_info):
        raise HTTPException(
            status_code=404,
            detail=f'Вложение с индексом {attachment_index} не найдено',
        )

    att = att_info[attachment_index]
    file_path = att.get('path')

    if file_path and not inbox_attachment_exists(file_path) and email.uid:
        # Файл у текущей записи может отсутствовать после переносов/пересборок.
        # Пытаемся найти запись того же UID с живым файлом и восстановить путь.
        siblings = (
            await session.execute(
                select(InboxEmail)
                .where(
                    InboxEmail.email_account_id == email.email_account_id,
                    InboxEmail.uid == email.uid,
                    InboxEmail.id != email.id,
                )
                .order_by(InboxEmail.fetched_at.desc(), InboxEmail.id.desc())
                .limit(20)
            )
        ).scalars().all()
        recovered_path: Optional[str] = None
        current_name = str(att.get('name') or '').strip()
        for sibling in siblings:
            sibling_info = sibling.attachment_info or []
            # 1) пробуем тот же индекс
            if attachment_index < len(sibling_info):
                candidate = (sibling_info[attachment_index] or {}).get('path')
                if candidate and inbox_attachment_exists(candidate):
                    recovered_path = candidate
                    break
            # 2) пробуем по имени файла
            if current_name:
                for sibling_att in sibling_info:
                    if (
                        str(sibling_att.get('name') or '').strip()
                        == current_name
                    ):
                        candidate = sibling_att.get('path')
                        if candidate and inbox_attachment_exists(candidate):
                            recovered_path = candidate
                            break
                if recovered_path:
                    break

        if recovered_path:
            att_info = list(att_info)
            repaired_att = dict(att_info[attachment_index] or {})
            repaired_att['path'] = recovered_path
            att_info[attachment_index] = repaired_att
            email.attachment_info = att_info
            session.add(email)
            await session.commit()
            file_path = recovered_path
            logger.info(
                'Recovered missing inbox attachment '
                'path: email_id=%s uid=%s path=%s',
                email.id,
                email.uid,
                recovered_path,
            )

    # Если локальный файл по-прежнему недоступен, пробуем восстановить
    # вложения из исходного почтового сервера по UID.
    if (not file_path or not inbox_attachment_exists(file_path)) and email.uid:
        restored = await restore_inbox_email_attachments_from_source(
            session,
            inbox_email=email,
        )
        if restored:
            att_info = email.attachment_info or []
            if attachment_index < len(att_info):
                att = att_info[attachment_index]
                file_path = att.get('path')

    if not file_path:
        raise HTTPException(
            status_code=404,
            detail=(
                'Файл не сохранён на диске. '
                'Письма, полученные до введения функции предпросмотра, '
                'не имеют сохранённых вложений. '
                'Загрузите письмо повторно для сохранения вложений.'
            ),
        )

    if not inbox_attachment_exists(file_path):
        raise HTTPException(
            status_code=404,
            detail=(
                f'Файл вложения не найден на диске: {file_path}. '
                'Попробуйте повторно загрузить письма — система восстановит '
                'сохранённые вложения для уже известных UID.'
            ),
        )

    fs_path = resolve_inbox_attachment_fs_path(file_path)
    try:
        preview = await read_attachment_preview(fs_path, max_rows=25)
    except Exception as e:
        logger.exception('Ошибка чтения вложения %s: %s', fs_path, e)
        raise HTTPException(
            status_code=500,
            detail=f'Не удалось прочитать файл: {e}',
        )

    logger.info(
        'Attachment preview prepared: email_id=%s '
        'attachment_index=%s rows=%s total_rows=%s columns=%s elapsed_ms=%.1f',
        email_id,
        attachment_index,
        len(preview.get('rows', []) or []),
        preview.get('total_rows', 0),
        preview.get('columns', 0),
        (time.perf_counter() - started) * 1000,
    )

    return {
        'filename': att.get('name', ''),
        'rows': preview['rows'],
        'total_rows': preview['total_rows'],
        'columns': preview['columns'],
    }


@router.post(
    '/emails/fetch',
    response_model=FetchInboxResponse,
    status_code=status.HTTP_200_OK,
    summary='Загрузить новые письма с почтового сервера',
)
async def fetch_emails(
    payload: FetchInboxRequest,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    """
    Принудительно загружает письма с IMAP/Resend для указанного ящика
    (или всех активных ящиков) за последние N дней.
    Новые письма сохраняются в БД. Автоматически применяются существующие
    паттерны правил.
    """
    days = max(1, min(payload.days, 7))
    try:
        result = await fetch_and_store_emails(
            session,
            email_account_id=payload.email_account_id,
            days=days,
        )
    except Exception as e:
        logger.exception('Ошибка при загрузке писем: %s', e)
        raise HTTPException(status_code=500, detail=f'Ошибка загрузки: {e}')
    return result


@router.post(
    '/emails/{email_id}/rule',
    response_model=AssignRuleResponse,
    summary='Назначить правило письму',
)
async def assign_rule_to_email(
    email_id: int,
    payload: AssignRuleRequest,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    """
    Менеджер назначает правило входящему письму.
    Письмо немедленно обрабатывается согласно правилу.
    Если save_pattern=True (по умолчанию) — создаётся паттерн для
    автоматической разметки похожих писем в будущем.
    """
    try:
        updated = await assign_rule(
            session,
            email_id=email_id,
            rule_type=payload.rule_type,
            user_id=current_user.id,
            save_pattern=payload.save_pattern,
        )
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.exception('Ошибка назначения правила: %s', e)
        raise HTTPException(status_code=500, detail=f'Ошибка обработки: {e}')

    return AssignRuleResponse(
        id=updated.id,
        rule_type=updated.rule_type,
        processed=updated.processed,
        processing_result=updated.processing_result,
        processing_error=updated.processing_error,
    )


@router.post(
    '/emails/{email_id}/force-process',
    response_model=ForceProcessResponse,
    summary='Принудительно обработать письмо по назначенному правилу',
)
async def force_process_email_route(
    email_id: int,
    payload: ForceProcessRequest,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    """
    Принудительно запускает обработку письма с уже назначенным правилом.

    Поддерживаемые типы:
    - customer_order
    - order_reply
    - document
    """
    try:
        result = await force_process_email(
            session,
            email_id=email_id,
            user_id=current_user.id,
            allow_reprocess=payload.allow_reprocess,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except LookupError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.exception('Ошибка принудительной обработки письма: %s', e)
        raise HTTPException(
            status_code=500,
            detail=f'Ошибка принудительной обработки: {e}',
        )

    return ForceProcessResponse(**result)


# ---------------------------------------------------------------------------
# Мастер настройки
# ---------------------------------------------------------------------------

@router.get(
    '/setup-options',
    response_model=InboxSetupOptions,
    summary='Списки поставщиков и клиентов для мастера настройки',
)
async def get_setup_options(
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    """
    Возвращает списки поставщиков и клиентов для выпадающих списков
    в мастере назначения правила.
    """
    from dz_fastapi.models.partner import Customer, Provider

    started = time.perf_counter()

    # Для мастера нужны только id/name/email.
    # Не грузим связанные price_lists/customer_price_lists — это сильно
    # ускоряет ответ на больших продовых базах.
    providers_rows = (
        await session.execute(
            select(
                Provider.id,
                Provider.name,
                Provider.email_incoming_price,
            ).order_by(Provider.name.asc())
        )
    ).all()
    providers = [
        SetupOption(id=row.id, name=row.name, email=row.email_incoming_price)
        for row in providers_rows
    ]

    customers_rows = (
        await session.execute(
            select(Customer.id, Customer.name).order_by(Customer.name.asc())
        )
    ).all()
    customers = [
        SetupOption(id=row.id, name=row.name) for row in customers_rows
    ]

    logger.info(
        'Inbox setup options prepared: '
        'providers=%s customers=%s elapsed_ms=%.1f',
        len(providers),
        len(customers),
        (time.perf_counter() - started) * 1000,
    )

    return InboxSetupOptions(providers=providers, customers=customers)


@router.get(
    '/provider/{provider_id}/configs',
    summary='Конфигурации поставщика для мастера настройки',
)
async def get_provider_configs_for_wizard(
    provider_id: int,
    rule_type: str = Query(
        ...,
        description='price_list | order_reply | document'
    ),
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    """
    Возвращает существующие конфигурации поставщика в зависимости от
    типа правила: ProviderPriceListConfig (price_list) или
    SupplierResponseConfig (order_reply / document).
    """
    from dz_fastapi.crud.partner import (crud_provider_pricelist_config,
                                         crud_supplier_response_config)

    if rule_type == 'price_list':
        configs = await crud_provider_pricelist_config.get_configs(
            provider_id=provider_id, session=session
        )
        return [
            {
                'id': c.id,
                'label': (
                    f'#{c.id} • {c.name_price}'
                    if c.name_price else f'Конфигурация #{c.id}'
                ),
                'filename_pattern': getattr(c, 'filename_pattern', None),
                'name_price': c.name_price,
                'name_mail': c.name_mail,
                'start_row': c.start_row,
                'oem_col': c.oem_col,
                'qty_col': c.qty_col,
                'price_col': c.price_col,
                'brand_col': c.brand_col,
                'multiplicity_col': c.multiplicity_col,
                'name_col': c.name_col,
            }
            for c in (configs or [])
        ]

    if rule_type in ('order_reply', 'document'):
        payload_type = (
            'response' if rule_type == 'order_reply' else 'document'
        )
        all_cfgs = await crud_supplier_response_config.get_configs(
            provider_id=provider_id, session=session
        )
        filtered = [
            c for c in (all_cfgs or [])
            if getattr(c, 'file_payload_type', 'response') == payload_type
        ]
        return [
            {
                'id': c.id,
                'label': f'#{c.id} • {c.name}' if c.name else f'#{c.id}',
                'name': c.name,
                'response_type': getattr(c, 'response_type', 'file'),
                'file_payload_type': getattr(c, 'file_payload_type', None),
                'filename_pattern': getattr(c, 'filename_pattern', None),
                'start_row': getattr(c, 'start_row', 1),
                'oem_col': getattr(c, 'oem_col', None),
                'qty_col': getattr(c, 'qty_col', None),
                'price_col': getattr(c, 'price_col', None),
                'brand_col': getattr(c, 'brand_col', None),
                'status_col': getattr(c, 'status_col', None),
                'comment_col': getattr(c, 'comment_col', None),
                'confirm_keywords': getattr(c, 'confirm_keywords', None),
                'reject_keywords': getattr(c, 'reject_keywords', None),
                'value_after_article_type': getattr(
                    c, 'value_after_article_type', None
                ),
                'document_number_col': getattr(
                    c, 'document_number_col', None
                ),
                'document_date_col': getattr(c, 'document_date_col', None),
            }
            for c in filtered
        ]

    return []


@router.post(
    '/emails/{email_id}/setup',
    response_model=InboxSetupResponse,
    summary=(
        'Мастер настройки: назначить правило + '
        'создать/обновить конфигурацию в системе'
    ),
)
async def setup_email(
    email_id: int,
    payload: InboxSetupRequest,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    """
    Полный мастер назначения правила:
    - Привязывает email-отправителя к поставщику или клиенту
      (обновляет email_incoming_price / order_email
       в реальных конфигах системы)
    - Сохраняет паттерн EmailRulePattern для будущей авто-разметки
    - Немедленно обрабатывает письмо по выбранному правилу

    Таким образом, настройка делается один раз прямо из Inbox,
    и система будет автоматически распознавать и обрабатывать
    будущие письма от того же отправителя.
    """
    try:
        result = await setup_email_rule(
            session,
            email_id=email_id,
            rule_type=payload.rule_type,
            user_id=current_user.id,
            save_pattern=payload.save_pattern,
            provider_config=payload.provider_config,
            customer_config=payload.customer_config,
        )
    except ValueError as e:
        detail = str(e)
        status_code = (
            404 if 'не найден' in detail.lower() else 400
        )
        raise HTTPException(status_code=status_code, detail=detail)
    except Exception as e:
        logger.exception('Ошибка мастера настройки: %s', e)
        raise HTTPException(status_code=500, detail=f'Ошибка настройки: {e}')

    return InboxSetupResponse(
        email_id=result['email_id'],
        rule_type=result['rule_type'],
        processed=result['processed'],
        processing_result=result.get('processing_result'),
        processing_error=result.get('processing_error'),
        configs_set=[
            ConfigSetupInfo(**c) for c in result.get('configs_set', [])
        ],
    )


# ---------------------------------------------------------------------------
# Паттерны правил
# ---------------------------------------------------------------------------

@router.get(
    '/rule-patterns',
    response_model=list[EmailRulePatternOut],
    summary='Список паттернов авто-разметки',
)
async def list_patterns(
    email_account_id: Optional[int] = Query(default=None),
    rule_type: Optional[str] = Query(default=None),
    active_only: bool = Query(default=True),
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    patterns = await list_rule_patterns(
        session,
        email_account_id=email_account_id,
        rule_type=rule_type,
        active_only=active_only,
    )
    return [EmailRulePatternOut.model_validate(p) for p in patterns]


@router.post(
    '/rule-patterns',
    response_model=EmailRulePatternOut,
    status_code=status.HTTP_201_CREATED,
    summary='Создать паттерн правила вручную',
)
async def create_pattern(
    payload: EmailRulePatternCreate,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    pattern = await create_rule_pattern(
        session,
        email_account_id=payload.email_account_id,
        from_email_pattern=payload.from_email_pattern,
        from_domain_pattern=payload.from_domain_pattern,
        subject_keywords=payload.subject_keywords,
        requires_attachments=payload.requires_attachments,
        attachment_extensions=payload.attachment_extensions,
        rule_type=payload.rule_type,
        created_by_id=current_user.id,
    )
    await session.commit()
    await session.refresh(pattern)
    return EmailRulePatternOut.model_validate(pattern)


@router.patch(
    '/rule-patterns/{pattern_id}',
    response_model=EmailRulePatternOut,
    summary='Обновить паттерн правила',
)
async def update_pattern(
    pattern_id: int,
    payload: EmailRulePatternUpdate,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    pattern = await get_rule_pattern(session, pattern_id)
    if pattern is None:
        raise HTTPException(status_code=404, detail='Паттерн не найден')
    updated = await update_rule_pattern(
        session,
        pattern=pattern,
        **payload.model_dump(exclude_none=True),
    )
    await session.commit()
    await session.refresh(updated)
    return EmailRulePatternOut.model_validate(updated)


@router.delete(
    '/rule-patterns/{pattern_id}',
    status_code=status.HTTP_204_NO_CONTENT,
    summary='Удалить паттерн правила',
)
async def remove_pattern(
    pattern_id: int,
    session: AsyncSession = Depends(get_session),
    _: User = Depends(require_admin),
):
    ok = await delete_rule_pattern(session, pattern_id)
    if not ok:
        raise HTTPException(status_code=404, detail='Паттерн не найден')
