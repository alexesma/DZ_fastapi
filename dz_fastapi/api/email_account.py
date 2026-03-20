import asyncio
import logging
from typing import List

from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.responses import HTMLResponse
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.api.deps import require_admin
from dz_fastapi.core.db import get_session
from dz_fastapi.core.time import now_moscow
from dz_fastapi.crud.email_account import crud_email_account
from dz_fastapi.models.email_account import EmailAccount
from dz_fastapi.schemas.email_account import (EmailAccountCreate,
                                              EmailAccountResponse,
                                              EmailAccountTestRequest,
                                              EmailAccountTestResponse,
                                              EmailAccountUpdate)
from dz_fastapi.services.email import (build_email_delivery_kwargs,
                                       send_test_outbound_email)
from dz_fastapi.services.email_account_checks import (test_imap_connection,
                                                      test_smtp_connection)
from dz_fastapi.services.google_oauth import (build_google_auth_url,
                                              exchange_code_for_tokens,
                                              parse_oauth_state,
                                              test_google_gmail_access)

logger = logging.getLogger('dz_fastapi')

router = APIRouter(prefix='/email-accounts', tags=['email-accounts'])


@router.get(
    '/',
    response_model=List[EmailAccountResponse],
    status_code=status.HTTP_200_OK,
)
async def list_email_accounts(
    session: AsyncSession = Depends(get_session),
    _: EmailAccount = Depends(require_admin),
):
    accounts = await crud_email_account.get_multi(session)
    return [EmailAccountResponse.model_validate(a) for a in accounts]


@router.post(
    '/',
    response_model=EmailAccountResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_email_account(
    payload: EmailAccountCreate,
    session: AsyncSession = Depends(get_session),
    _: EmailAccount = Depends(require_admin),
):
    account = await crud_email_account.create(payload, session)
    return EmailAccountResponse.model_validate(account)


@router.patch(
    '/{account_id}',
    response_model=EmailAccountResponse,
    status_code=status.HTTP_200_OK,
)
async def update_email_account(
    account_id: int,
    payload: EmailAccountUpdate,
    session: AsyncSession = Depends(get_session),
    _: EmailAccount = Depends(require_admin),
):
    account = await crud_email_account.get(session, account_id)
    if not account:
        raise HTTPException(status_code=404, detail='Account not found')
    account = await crud_email_account.update(
        db_obj=account, obj_in=payload, session=session, commit=True
    )
    return EmailAccountResponse.model_validate(account)


@router.delete(
    '/{account_id}',
    status_code=status.HTTP_204_NO_CONTENT,
)
async def delete_email_account(
    account_id: int,
    session: AsyncSession = Depends(get_session),
    _: EmailAccount = Depends(require_admin),
):
    account = await crud_email_account.get(session, account_id)
    if not account:
        raise HTTPException(status_code=404, detail='Account not found')
    await crud_email_account.remove(account, session)
    return None


@router.post(
    '/{account_id}/test',
    response_model=EmailAccountTestResponse,
    status_code=status.HTTP_200_OK,
)
async def test_email_account(
    account_id: int,
    payload: EmailAccountTestRequest,
    session: AsyncSession = Depends(get_session),
    _: EmailAccount = Depends(require_admin),
):
    account = await crud_email_account.get(session, account_id)
    if not account:
        raise HTTPException(status_code=404, detail='Account not found')

    response = EmailAccountTestResponse()
    response.outbound_transport = account.transport or 'smtp'
    if payload.imap:
        if account.oauth_provider == 'google' and account.oauth_refresh_token:
            try:
                await test_google_gmail_access(account.oauth_refresh_token)
                response.imap_ok = True
            except Exception as exc:
                response.imap_ok = False
                response.imap_error = str(exc)
        elif not account.imap_host:
            response.imap_ok = False
            response.imap_error = 'IMAP host не указан'
        else:
            try:
                folder = (
                    payload.folder
                    or account.imap_folder
                    or 'INBOX'
                )
                await asyncio.to_thread(
                    test_imap_connection,
                    account.imap_host,
                    account.imap_port or 993,
                    account.email,
                    account.password,
                    folder,
                    True,
                )
                response.imap_ok = True
            except Exception as exc:
                response.imap_ok = False
                response.imap_error = str(exc)

    if payload.smtp:
        if (account.transport or 'smtp') == 'http_api':
            provider = (account.http_api_provider or '').strip().lower()
            api_url = account.http_api_url
            api_key = account.http_api_key
            if provider not in {'resend', 'brevo'}:
                response.smtp_ok = False
                response.smtp_error = (
                    'Не выбран поддерживаемый HTTP API провайдер'
                )
            elif not api_key:
                response.smtp_ok = False
                response.smtp_error = 'API ключ не указан'
            else:
                response.smtp_ok = True
                response.outbound_note = (
                    'HTTP API настроен. Реальная отправка тестового письма '
                    'не выполнялась.'
                )
                if not api_url:
                    response.outbound_note += (
                        ' Используется стандартный URL провайдера.'
                    )
        elif not account.smtp_host:
            response.smtp_ok = False
            response.smtp_error = 'SMTP host не указан'
        else:
            use_ssl = bool(account.smtp_use_ssl)
            port = (
                account.smtp_port
                or (465 if use_ssl else 587)
            )
            try:
                await asyncio.to_thread(
                    test_smtp_connection,
                    account.smtp_host,
                    port,
                    account.email,
                    account.password,
                    use_ssl,
                )
                response.smtp_ok = True
                response.outbound_note = 'SMTP авторизация успешна'
            except Exception as exc:
                response.smtp_ok = False
                response.smtp_error = str(exc)

        if payload.real_send:
            if not payload.to_email:
                response.smtp_ok = False
                response.smtp_error = 'Укажите email получателя для теста'
            elif response.smtp_ok is False:
                pass
            else:
                kwargs = build_email_delivery_kwargs(account)
                try:
                    sent = await asyncio.to_thread(
                        send_test_outbound_email,
                        to_email=str(payload.to_email),
                        **kwargs,
                    )
                    if sent:
                        response.smtp_ok = True
                        response.outbound_note = (
                            f'Тестовое письмо отправлено на {payload.to_email}'
                        )
                    else:
                        response.smtp_ok = False
                        response.smtp_error = (
                            'Тестовая отправка не удалась. '
                            'Смотрите backend-логи.'
                        )
                except Exception as exc:
                    response.smtp_ok = False
                    response.smtp_error = str(exc)

    return response


@router.post(
    '/{account_id}/google-oauth/init',
    status_code=status.HTTP_200_OK,
)
async def init_google_oauth(
    account_id: int,
    session: AsyncSession = Depends(get_session),
    _: EmailAccount = Depends(require_admin),
):
    account = await crud_email_account.get(session, account_id)
    if not account:
        raise HTTPException(status_code=404, detail='Account not found')
    auth_url = build_google_auth_url(account_id)
    return {'auth_url': auth_url}


@router.post(
    '/{account_id}/google-oauth/disconnect',
    response_model=EmailAccountResponse,
    status_code=status.HTTP_200_OK,
)
async def disconnect_google_oauth(
    account_id: int,
    session: AsyncSession = Depends(get_session),
    _: EmailAccount = Depends(require_admin),
):
    account = await crud_email_account.get(session, account_id)
    if not account:
        raise HTTPException(status_code=404, detail='Account not found')
    account.oauth_provider = None
    account.oauth_refresh_token = None
    account.oauth_connected_at = None
    session.add(account)
    await session.commit()
    await session.refresh(account)
    return EmailAccountResponse.model_validate(account)


@router.get(
    '/google-oauth/callback',
    include_in_schema=False,
)
async def google_oauth_callback(
    code: str | None = Query(default=None),
    state: str | None = Query(default=None),
    error: str | None = Query(default=None),
    session: AsyncSession = Depends(get_session),
):
    if error:
        return HTMLResponse(
            f'<h3>OAuth error</h3><p>{error}</p>', status_code=400
        )
    if not code or not state:
        return HTMLResponse(
            '<h3>OAuth error</h3><p>Missing code or state.</p>',
            status_code=400,
        )
    account_id = parse_oauth_state(state)
    if not account_id:
        return HTMLResponse(
            '<h3>OAuth error</h3><p>Invalid state.</p>',
            status_code=400,
        )
    account = await crud_email_account.get(session, account_id)
    if not account:
        return HTMLResponse(
            '<h3>OAuth error</h3><p>Account not found.</p>',
            status_code=404,
        )
    try:
        tokens = await exchange_code_for_tokens(code)
    except Exception as exc:
        logger.error('Google OAuth exchange failed: %s', exc)
        return HTMLResponse(
            '<h3>OAuth error</h3><p>Token exchange failed.</p>',
            status_code=400,
        )
    refresh_token = tokens.get('refresh_token')
    if not refresh_token and not account.oauth_refresh_token:
        return HTMLResponse(
            '<h3>OAuth error</h3>'
            '<p>Refresh token not returned. '
            'Try reconnecting with prompt=consent.</p>',
            status_code=400,
        )
    if refresh_token:
        account.oauth_refresh_token = refresh_token
    account.oauth_provider = 'google'
    account.oauth_connected_at = now_moscow()
    session.add(account)
    await session.commit()
    return HTMLResponse(
        '<h3>Google OAuth подключен</h3>'
        '<p>Можно закрыть это окно.</p>'
    )
