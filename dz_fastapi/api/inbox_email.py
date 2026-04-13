import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.api.deps import get_current_user, require_admin
from dz_fastapi.core.db import get_session
from dz_fastapi.crud.inbox_email import (
    create_rule_pattern,
    delete_rule_pattern,
    get_inbox_email,
    get_rule_pattern,
    list_inbox_emails,
    list_rule_patterns,
    update_rule_pattern,
)
from dz_fastapi.models.user import User
from dz_fastapi.schemas.inbox_email import (
    AssignRuleRequest,
    AssignRuleResponse,
    ConfigSetupInfo,
    EmailRulePatternCreate,
    EmailRulePatternOut,
    EmailRulePatternUpdate,
    FetchInboxRequest,
    FetchInboxResponse,
    InboxEmailBrief,
    InboxEmailDetail,
    InboxEmailListResponse,
    InboxSetupOptions,
    InboxSetupRequest,
    InboxSetupResponse,
    SetupOption,
)
from dz_fastapi.services.inbox_email import (
    assign_rule,
    fetch_and_store_emails,
    setup_email_rule,
)

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
    from dz_fastapi.crud.partner import crud_provider, crud_customer

    # Используем проверенные CRUD-методы, которые уже корректно
    # обрабатывают joined-table inheritance (Client → Provider/Customer)
    all_providers = await crud_provider.get_multi(session, skip=0, limit=2000)
    providers = sorted(
        [
            SetupOption(
                id=p.id,
                name=p.name,
                email=getattr(p, 'email_incoming_price', None),
            )
            for p in all_providers
        ],
        key=lambda x: x.name or '',
    )

    all_customers = await crud_customer.get_multi(session, skip=0, limit=2000)
    customers = sorted(
        [SetupOption(id=c.id, name=c.name) for c in all_customers],
        key=lambda x: x.name or '',
    )

    return InboxSetupOptions(providers=providers, customers=customers)


@router.post(
    '/emails/{email_id}/setup',
    response_model=InboxSetupResponse,
    summary='Мастер настройки: назначить правило + создать/обновить конфигурацию в системе',
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
      (обновляет email_incoming_price / order_email в реальных конфигах системы)
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
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.exception('Ошибка мастера настройки: %s', e)
        raise HTTPException(status_code=500, detail=f'Ошибка настройки: {e}')

    return InboxSetupResponse(
        email_id=result['email_id'],
        rule_type=result['rule_type'],
        processed=result['processed'],
        processing_result=result.get('processing_result'),
        processing_error=result.get('processing_error'),
        configs_set=[ConfigSetupInfo(**c) for c in result.get('configs_set', [])],
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
