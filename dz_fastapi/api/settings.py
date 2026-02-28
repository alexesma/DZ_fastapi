from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.core.db import get_session
from dz_fastapi.core.scheduler_settings import SCHEDULER_SETTING_DEFAULTS
from dz_fastapi.crud.settings import (crud_customer_order_inbox_settings,
                                      crud_price_check_log,
                                      crud_price_check_schedule,
                                      crud_price_stale_alert,
                                      crud_scheduler_setting,
                                      crud_system_metric_snapshot)
from dz_fastapi.schemas.settings import (CustomerOrderInboxSettingsOut,
                                         CustomerOrderInboxSettingsUpdate,
                                         MonitorSummaryOut, PriceCheckLogOut,
                                         PriceCheckScheduleOut,
                                         PriceCheckScheduleUpdate,
                                         PriceListStaleAlertOut,
                                         SchedulerSettingOut,
                                         SchedulerSettingUpdate,
                                         SystemMetricSnapshotOut)
from dz_fastapi.services.monitoring import (build_snapshot_payload,
                                            get_monitor_summary)

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


@router.get(
    '/settings/scheduler',
    tags=['settings'],
    status_code=status.HTTP_200_OK,
    response_model=list[SchedulerSettingOut],
)
async def list_scheduler_settings(
    session: AsyncSession = Depends(get_session),
):
    settings = []
    for key, defaults in SCHEDULER_SETTING_DEFAULTS.items():
        setting = await crud_scheduler_setting.get_or_create(
            session=session, key=key, defaults=defaults
        )
        settings.append(SchedulerSettingOut.model_validate(setting))
    return settings


@router.put(
    '/settings/scheduler/{key}',
    tags=['settings'],
    status_code=status.HTTP_200_OK,
    response_model=SchedulerSettingOut,
)
async def update_scheduler_setting(
    key: str,
    payload: SchedulerSettingUpdate,
    session: AsyncSession = Depends(get_session),
):
    if key not in SCHEDULER_SETTING_DEFAULTS:
        raise HTTPException(status_code=404, detail='Unknown scheduler key')
    update_data = payload.model_dump(exclude_unset=True)
    defaults = SCHEDULER_SETTING_DEFAULTS.get(key, {})
    if 'days' in update_data and not update_data.get('days'):
        update_data['days'] = defaults.get('days', [])
    if 'times' in update_data and not update_data.get('times'):
        update_data['times'] = defaults.get('times', [])
    setting = await crud_scheduler_setting.update(
        session=session,
        key=key,
        data=update_data,
        defaults=defaults,
    )
    return SchedulerSettingOut.model_validate(setting)


@router.get(
    '/settings/orders-inbox',
    tags=['settings'],
    status_code=status.HTTP_200_OK,
    response_model=CustomerOrderInboxSettingsOut,
)
async def get_customer_order_inbox_settings(
    session: AsyncSession = Depends(get_session),
):
    setting = await crud_customer_order_inbox_settings.get_or_create(
        session=session
    )
    return CustomerOrderInboxSettingsOut.model_validate(setting)


@router.put(
    '/settings/orders-inbox',
    tags=['settings'],
    status_code=status.HTTP_200_OK,
    response_model=CustomerOrderInboxSettingsOut,
)
async def update_customer_order_inbox_settings(
    payload: CustomerOrderInboxSettingsUpdate,
    session: AsyncSession = Depends(get_session),
):
    data = payload.model_dump(exclude_unset=True)
    if 'lookback_days' in data:
        data['lookback_days'] = max(1, int(data['lookback_days']))
    setting = await crud_customer_order_inbox_settings.update(
        session=session, data=data
    )
    return CustomerOrderInboxSettingsOut.model_validate(setting)


@router.get(
    '/settings/monitor/summary',
    tags=['settings'],
    status_code=status.HTTP_200_OK,
    response_model=MonitorSummaryOut,
)
async def get_monitor_summary_api(
    request: Request,
    session: AsyncSession = Depends(get_session),
):
    summary = await get_monitor_summary(session=session, app=request.app)
    return summary


@router.post(
    '/settings/monitor/snapshot',
    tags=['settings'],
    status_code=status.HTTP_200_OK,
    response_model=SystemMetricSnapshotOut,
)
async def create_monitor_snapshot(
    request: Request,
    session: AsyncSession = Depends(get_session),
):
    summary = await get_monitor_summary(session=session, app=request.app)
    payload = build_snapshot_payload(summary)
    snapshot = await crud_system_metric_snapshot.create(
        session=session, payload=payload
    )
    return SystemMetricSnapshotOut.model_validate(snapshot)


@router.get(
    '/settings/monitor/snapshots',
    tags=['settings'],
    status_code=status.HTTP_200_OK,
    response_model=list[SystemMetricSnapshotOut],
)
async def list_monitor_snapshots(
    limit: int = Query(default=200, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
    session: AsyncSession = Depends(get_session),
):
    snapshots = await crud_system_metric_snapshot.list(
        session=session, limit=limit, offset=offset
    )
    return [SystemMetricSnapshotOut.model_validate(s) for s in snapshots]
