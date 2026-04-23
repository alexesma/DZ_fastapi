from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.core.db import get_session
from dz_fastapi.core.scheduler_settings import SCHEDULER_SETTING_DEFAULTS
from dz_fastapi.crud.settings import (crud_customer_order_inbox_settings,
                                      crud_price_check_log,
                                      crud_price_check_schedule,
                                      crud_price_stale_alert,
                                      crud_scheduler_setting,
                                      crud_system_metric_snapshot)
from dz_fastapi.models.settings import SupplierHoliday
from dz_fastapi.schemas.settings import (CustomerOrderInboxSettingsOut,
                                         CustomerOrderInboxSettingsUpdate,
                                         MonitorSummaryOut, PriceCheckLogOut,
                                         PriceCheckScheduleOut,
                                         PriceCheckScheduleUpdate,
                                         PriceListStaleAlertOut,
                                         SchedulerSettingOut,
                                         SchedulerSettingUpdate,
                                         SupplierHolidayCreate,
                                         SupplierHolidayOut,
                                         SystemMetricSnapshotOut)
from dz_fastapi.services.holidays import (_auto_holidays_for_years,
                                          get_manual_holidays)
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
    if 'error_file_retention_days' in data:
        data['error_file_retention_days'] = max(
            1, int(data['error_file_retention_days'])
        )
    if 'supplier_response_lookback_days' in data:
        data['supplier_response_lookback_days'] = max(
            1, int(data['supplier_response_lookback_days'])
        )
    if 'supplier_order_stub_email' in data:
        value = str(data.get('supplier_order_stub_email') or '').strip()
        data['supplier_order_stub_email'] = value or 'info@dragonzap.ru'
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


# ---------------------------------------------------------------------------
# Supplier holiday calendar
# ---------------------------------------------------------------------------

@router.get(
    '/settings/holidays',
    tags=['settings'],
    status_code=status.HTTP_200_OK,
    response_model=list[SupplierHolidayOut],
)
async def list_holidays(
    year: int = Query(
        default=None, description='Год (по умолчанию — текущий)'
    ),
    session: AsyncSession = Depends(get_session),
):
    """Return combined holiday list for a given year:
    auto-detected (python-holidays) + manual overrides from DB.
    """
    import datetime as _dt
    from datetime import date
    if year is None:
        year = _dt.date.today().year

    auto_dates = _auto_holidays_for_years([year])
    manual_entries = await get_manual_holidays(session, [year])
    manual_map: dict[
        date,
        SupplierHoliday
    ] = {e.date: e for e in manual_entries}

    result: list[SupplierHolidayOut] = []

    # Manual entries first (they may override auto ones)
    for entry in manual_entries:
        result.append(
            SupplierHolidayOut(
                id=entry.id,
                date=entry.date,
                description=entry.description,
                is_working_day=entry.is_working_day,
                source='manual',
                created_at=entry.created_at,
            )
        )

    # Auto entries not already in manual_map
    for d in sorted(auto_dates):
        if d not in manual_map:
            result.append(
                SupplierHolidayOut(
                    id=0,
                    date=d,
                    description=None,
                    is_working_day=False,
                    source='auto',
                    created_at=None,
                )
            )

    result.sort(key=lambda x: x.date)
    return result


@router.post(
    '/settings/holidays',
    tags=['settings'],
    status_code=status.HTTP_201_CREATED,
    response_model=SupplierHolidayOut,
)
async def create_holiday(
    payload: SupplierHolidayCreate,
    session: AsyncSession = Depends(get_session),
):
    """Add a manual holiday (or workday override) to the DB."""
    existing = await session.execute(
        select(SupplierHoliday).where(SupplierHoliday.date == payload.date)
    )
    if existing.scalar_one_or_none():
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f'Запись для даты {payload.date} уже существует',
        )
    entry = SupplierHoliday(
        date=payload.date,
        description=payload.description,
        is_working_day=payload.is_working_day,
    )
    session.add(entry)
    await session.commit()
    await session.refresh(entry)
    return SupplierHolidayOut(
        id=entry.id,
        date=entry.date,
        description=entry.description,
        is_working_day=entry.is_working_day,
        source='manual',
        created_at=entry.created_at,
    )


@router.delete(
    '/settings/holidays/{holiday_id}',
    tags=['settings'],
    status_code=status.HTTP_204_NO_CONTENT,
)
async def delete_holiday(
    holiday_id: int,
    session: AsyncSession = Depends(get_session),
):
    """Delete a manual holiday entry."""
    entry = await session.get(SupplierHoliday, holiday_id)
    if not entry:
        raise HTTPException(status_code=404, detail='Запись не найдена')
    await session.delete(entry)
    await session.commit()
