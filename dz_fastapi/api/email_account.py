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
                                              EmailAccountUpdate)

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
