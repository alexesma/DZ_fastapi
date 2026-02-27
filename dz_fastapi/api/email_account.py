import asyncio
import logging
from typing import List

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.api.deps import require_admin
from dz_fastapi.core.db import get_session
from dz_fastapi.crud.email_account import crud_email_account
from dz_fastapi.models.email_account import EmailAccount
from dz_fastapi.schemas.email_account import (EmailAccountCreate,
                                              EmailAccountResponse,
                                              EmailAccountTestRequest,
                                              EmailAccountTestResponse,
                                              EmailAccountUpdate)
from dz_fastapi.services.email_account_checks import (test_imap_connection,
                                                      test_smtp_connection)

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
    if payload.imap:
        if not account.imap_host:
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
        if not account.smtp_host:
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
            except Exception as exc:
                response.smtp_ok = False
                response.smtp_error = str(exc)

    return response
