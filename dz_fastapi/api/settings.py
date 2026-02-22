from fastapi import APIRouter, Depends, Query, status
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.core.db import get_session
from dz_fastapi.crud.settings import (crud_price_check_log,
                                      crud_price_check_schedule,
                                      crud_price_stale_alert)
from dz_fastapi.schemas.settings import (PriceCheckLogOut,
                                         PriceCheckScheduleOut,
                                         PriceCheckScheduleUpdate,
                                         PriceListStaleAlertOut)

router = APIRouter()


@router.get(
    '/settings/price-check',
    tags=['settings'],
    status_code=status.HTTP_200_OK,
    response_model=PriceCheckScheduleOut,
)
async def get_price_check_schedule(
    session: AsyncSession = Depends(get_session),
):
    schedule = await crud_price_check_schedule.get_or_create(session)
    return PriceCheckScheduleOut.model_validate(schedule)


@router.put(
    '/settings/price-check',
    tags=['settings'],
    status_code=status.HTTP_200_OK,
    response_model=PriceCheckScheduleOut,
)
async def update_price_check_schedule(
    payload: PriceCheckScheduleUpdate,
    session: AsyncSession = Depends(get_session),
):
    schedule = await crud_price_check_schedule.update(
        session, payload.model_dump()
    )
    return PriceCheckScheduleOut.model_validate(schedule)


@router.get(
    '/alerts/pricelist-stale',
    tags=['alerts'],
    status_code=status.HTTP_200_OK,
    response_model=list[PriceListStaleAlertOut],
)
async def list_pricelist_stale_alerts(
    provider_id: int | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    session: AsyncSession = Depends(get_session),
):
    alerts = await crud_price_stale_alert.list(
        session=session,
        provider_id=provider_id,
        limit=limit,
        offset=offset,
    )
    return [PriceListStaleAlertOut.model_validate(a) for a in alerts]


@router.get(
    '/alerts/price-check-logs',
    tags=['alerts'],
    status_code=status.HTTP_200_OK,
    response_model=list[PriceCheckLogOut],
)
async def list_price_check_logs(
    limit: int = Query(default=100, ge=1, le=500),
    session: AsyncSession = Depends(get_session),
):
    logs = await crud_price_check_log.list(session=session, limit=limit)
    return [PriceCheckLogOut.model_validate(log) for log in logs]
