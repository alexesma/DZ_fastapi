# scheduler.py
import asyncio
import logging
import os
import time
from contextlib import asynccontextmanager
from datetime import datetime, timedelta

import aiofiles
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import FastAPI
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.orm import selectinload

from dz_fastapi.core.constants import (CONFIG_DATA_CUSTOMER,
                                       CONFIG_DATA_PROVIDER, CUSTOMER,
                                       CUSTOMER_IN, PROVIDER_IN)
from dz_fastapi.core.scheduler_settings import SCHEDULER_SETTING_DEFAULTS
from dz_fastapi.core.time import now_moscow
from dz_fastapi.crud.partner import (crud_customer, crud_customer_pricelist,
                                     crud_customer_pricelist_config,
                                     crud_pricelist, crud_provider,
                                     crud_provider_pricelist_config)
from dz_fastapi.crud.settings import (crud_customer_order_inbox_settings,
                                      crud_price_check_log,
                                      crud_price_check_schedule,
                                      crud_price_stale_alert,
                                      crud_scheduler_setting,
                                      crud_system_metric_snapshot)
from dz_fastapi.models.partner import (CustomerPriceListConfig, PriceList,
                                       Provider, ProviderPriceListConfig)
from dz_fastapi.models.price_control import PriceControlConfig
from dz_fastapi.models.settings import PriceListStaleAlert
from dz_fastapi.schemas.partner import (CustomerCreate,
                                        CustomerPriceListConfigCreate,
                                        CustomerPriceListCreate,
                                        ProviderCreate,
                                        ProviderPriceListConfigCreate)
from dz_fastapi.services.customer_orders import (
    cleanup_order_error_files, cleanup_order_reports, process_customer_orders,
    send_scheduled_supplier_orders)
from dz_fastapi.services.email import get_emails
from dz_fastapi.services.monitoring import (build_snapshot_payload,
                                            get_monitor_summary)
from dz_fastapi.services.notifications import create_admin_notifications
from dz_fastapi.services.placed_orders import cleanup_old_tracking_history
from dz_fastapi.services.price_control import run_price_control
from dz_fastapi.services.process import (process_customer_pricelist,
                                         process_provider_pricelist)
from dz_fastapi.services.watchlist import send_watchlist_daily_notifications
from dz_fastapi.services.watchlist_site import check_watchlist_site

logger = logging.getLogger('dz_fastapi')
EMAIL_NAME_ORDER = os.getenv('EMAIL_NAME_ORDERS')
EMAIL_PASSWORD_ORDER = os.getenv('EMAIL_PASSWORD_ORDERS')
EMAIL_HOST_ORDER = os.getenv('EMAIL_HOST_ORDERS')
PRICELIST_STALE_ALERT_RETENTION_DAYS = int(
    os.getenv('PRICELIST_STALE_ALERT_RETENTION_DAYS', '7')
)
ENABLE_LEGACY_ZZAP_AUTO_SEND = os.getenv(
    'ENABLE_LEGACY_ZZAP_AUTO_SEND', '0'
).strip().lower() in {'1', 'true', 'yes', 'on'}


async def _notify_scheduler_issue(
    session: AsyncSession,
    *,
    subject: str,
    text: str,
) -> None:
    try:
        await session.rollback()
        await create_admin_notifications(
            session=session,
            title=subject,
            message=text,
            level='error',
            link='/admin/settings',
        )
    except Exception as exc:
        logger.error(
            'Failed to create scheduler app notification: %s',
            exc,
            exc_info=True,
        )


def start_scheduler(app: FastAPI):
    scheduler = AsyncIOScheduler()
    scheduler.configure(
        timezone='Europe/Moscow',
        job_defaults={'coalesce': True, 'max_instances': 1},
    )

    # Добавляем задачи в планировщик
    scheduler.add_job(
        func=download_price_provider_task,
        trigger='cron',  # или 'interval'
        args=[app],
        id='download_price_provider',
        name='Download price provider',
        minute=0,  # каждый час
        jitter=5,
        replace_existing=True,
        # hour='9',
    )

    scheduler.add_job(
        func=cleanup_old_pricelists_task,
        trigger='cron',
        args=[app],
        id='cleanup_old_pricelists',
        name='Cleanup old pricelists keep last 5',
        minute='*',
        replace_existing=True,
    )

    scheduler.add_job(
        func=send_scheduled_customer_pricelists_task,
        trigger='cron',
        args=[app],
        id='send_customer_pricelists',
        name='Send scheduled customer pricelists',
        minute='*',
        replace_existing=True,
    )

    scheduler.add_job(
        func=price_control_run_task,
        trigger='cron',
        args=[app],
        id='price_control_run',
        name='Price control run',
        minute='*',
        replace_existing=True,
    )

    scheduler.add_job(
        func=download_customer_orders_task,
        trigger='cron',
        args=[app],
        id='download_customer_orders',
        name='Download customer orders',
        minute='*/5',
        jitter=5,
        replace_existing=True,
    )

    scheduler.add_job(
        func=check_provider_pricelist_staleness_task,
        trigger='cron',
        args=[app],
        id='check_provider_pricelist_staleness',
        name='Check provider pricelist staleness',
        minute=10,
        replace_existing=True,
    )

    scheduler.add_job(
        func=check_watchlist_site_task,
        trigger='cron',
        args=[app],
        id='check_watchlist_site',
        name='Check watchlist site offers',
        minute='*',
        replace_existing=True,
    )

    scheduler.add_job(
        func=notify_watchlist_task,
        trigger='cron',
        args=[app],
        id='notify_watchlist',
        name='Notify watchlist',
        minute='*',
        replace_existing=True,
    )

    scheduler.add_job(
        func=notify_pricelist_stale_task,
        trigger='cron',
        args=[app],
        id='notify_pricelist_stale',
        name='Notify stale pricelists',
        minute='*',
        replace_existing=True,
    )

    scheduler.add_job(
        func=cleanup_pricelist_stale_alerts_task,
        trigger='cron',
        args=[app],
        id='cleanup_pricelist_stale_alerts',
        name='Cleanup stale pricelist alerts',
        minute='*',
        replace_existing=True,
    )

    scheduler.add_job(
        func=collect_system_metrics_snapshot_task,
        trigger='cron',
        args=[app],
        id='metrics_snapshot',
        name='Collect system metrics snapshot',
        minute='*',
        replace_existing=True,
    )

    scheduler.add_job(
        func=send_scheduled_supplier_orders_task,
        trigger='cron',
        args=[app],
        id='send_supplier_orders',
        name='Send scheduled supplier orders',
        minute='*/5',
        jitter=5,
        replace_existing=True,
    )

    scheduler.add_job(
        func=cleanup_order_reports_task,
        trigger='cron',
        args=[app],
        id='cleanup_order_reports',
        name='Cleanup order reports',
        hour=3,
        minute=0,
        replace_existing=True,
    )

    scheduler.add_job(
        func=cleanup_tracking_orders_task,
        trigger='cron',
        args=[app],
        id='cleanup_tracking_orders',
        name='Cleanup tracking orders older than 1 year',
        hour=3,
        minute=20,
        replace_existing=True,
    )

    # scheduler.add_job(
    #     func=,
    #     trigger='cron',
    #     args=[app],
    #     id='download_all_price_providers',
    #     name='Download prices over providers',
    #     minute='*/5',  # каждые 5 минут
    #     # hour='9',  # каждый день в 9 утра
    # )

    scheduler.start()
    logger.info('Scheduler started.')
    return scheduler


@asynccontextmanager
async def new_session_from_app(app: FastAPI):
    session_factory = app.state.session_factory
    session = session_factory()
    try:
        yield session
    finally:
        try:
            await session.close()
        except (asyncio.CancelledError, Exception) as e:
            if getattr(app.state, 'is_shutting_down', False):
                logger.debug(
                    'Ignoring session close error during shutdown: %s', e
                )
            else:
                raise


async def _process_one(item, app: FastAPI, sem: asyncio.Semaphore):
    provider, filepath, provider_conf = item
    try:
        async with sem:
            try:
                async with new_session_from_app(app) as session:
                    file_extension = filepath.split('.')[-1].lower()
                    async with aiofiles.open(filepath, 'rb') as f:
                        file_content = await f.read()
                    logger.info(
                        f'Скачан прайс для провайдера {provider.id} '
                        f'({provider.name}), размер: {len(file_content)} байт'
                    )
                    await process_provider_pricelist(
                        provider=provider,
                        file_content=file_content,
                        file_extension=file_extension,
                        provider_list_conf=provider_conf,
                        use_stored_params=True,
                        start_row=None,
                        oem_col=None,
                        brand_col=None,
                        name_col=None,
                        multiplicity_col=None,
                        qty_col=None,
                        price_col=None,
                        session=session,
                    )
                    logger.info(
                        f'Успешно обработан прайс для провайдера {provider.id}'
                    )

                    if provider.name == PROVIDER_IN['name']:
                        if ENABLE_LEGACY_ZZAP_AUTO_SEND:
                            logger.info(
                                'Legacy ZZAP auto-send is enabled; '
                                'running send_price_list_task'
                            )
                            await send_price_list_task(app)
                        else:
                            logger.info(
                                'Legacy ZZAP auto-send is disabled; '
                                'skip send_price_list_task and use '
                                'CustomerPriceListConfig schedule instead'
                            )
            except Exception as e:
                logger.error(
                    f'Ошибка обработки прайса для провайдера {provider.id}: '
                    f'{e}',
                    exc_info=True,
                )
                raise
    except asyncio.CancelledError:
        if getattr(app.state, 'is_shutting_down', False):
            logger.info(
                'Отмена обработки прайса провайдера %s при остановке',
                provider.id,
            )
            return
        raise


async def send_price_list_task(app: FastAPI):
    # logger.info('Starting send_price_list_task')
    # async_session_factory = get_async_session()
    logger.info('Starting send_price_list_task')
    async_session_factory = app.state.session_factory
    async with async_session_factory() as session:
        try:
            customer_in_model = CustomerCreate(**CUSTOMER_IN)

            customer = await crud_customer.get_customer_or_none(
                customer=CUSTOMER, session=session
            )
            if not customer:
                customer = await crud_customer.create(
                    obj_in=customer_in_model, session=session
                )
            config_in_model = CustomerPriceListConfigCreate(
                **CONFIG_DATA_CUSTOMER
            )
            configs = await crud_customer_pricelist_config.get_by_customer_id(
                customer_id=customer.id, session=session
            )
            if not configs:
                config = await crud_customer_pricelist_config.create_config(
                    customer_id=customer.id,
                    config_in=config_in_model,
                    session=session,
                )
            else:
                config = configs[-1]

            provider = await crud_provider.get_provider_or_none(
                provider=PROVIDER_IN['name'], session=session
            )
            if not provider:
                logger.error(f"Provider {PROVIDER_IN['name']} not found.")
                raise ValueError(f"Provider {PROVIDER_IN['name']} not found.")
            pricelist_ids = await crud_pricelist.get_pricelist_ids_by_provider(
                provider_id=provider.id, session=session
            )
            if not pricelist_ids:
                logger.error(
                    f'No pricelists found for provider {provider.name}.'
                )
                raise ValueError(
                    f'No pricelists found for provider {provider.name}.'
                )
            logger.debug(f'Using pricelist_ids[-1]: {pricelist_ids[-1]}')
            # Создаем или получаем объект запроса
            request = CustomerPriceListCreate(
                customer_id=customer.id,
                config_id=config.id,
                items=[pricelist_ids[-1]],
            )

            await process_customer_pricelist(
                customer=customer, request=request, session=session
            )

            logger.info(
                f'Pricelist created and sent for customer {customer.name}'
            )
        except Exception as e:
            logger.error(
                f'Error process. pricelist for customer {customer.name}: {e}',
                exc_info=True,
            )


async def download_customer_orders_task(app: FastAPI):
    logger.info('Starting download_customer_orders_task')
    async_session_factory = app.state.session_factory
    async with async_session_factory() as session:
        try:
            should_run, setting = await _should_run_scheduled_job(
                session, 'customer_orders_check'
            )
            if not should_run:
                return
            await process_customer_orders(session)
            if setting:
                await _mark_scheduler_ran(session, setting, now_moscow())
        except Exception as e:
            logger.error(
                f'Error processing customer orders: {e}', exc_info=True
            )
            await _notify_scheduler_issue(
                session,
                subject='Ошибка регламента обработки заказов',
                text=(
                    'Ошибка при автоматической обработке заказов клиентов.\n'
                    f'Текст ошибки: {e}'
                ),
            )


async def send_scheduled_supplier_orders_task(app: FastAPI):
    logger.info('Starting send_scheduled_supplier_orders_task')
    async_session_factory = app.state.session_factory
    async with async_session_factory() as session:
        try:
            await send_scheduled_supplier_orders(session)
        except Exception as e:
            logger.error(
                f'Error sending scheduled supplier orders: {e}',
                exc_info=True,
            )
            await _notify_scheduler_issue(
                session,
                subject='Ошибка регламента отправки заказов поставщикам',
                text=(
                    'Ошибка при автоматической отправке заказов '
                    f'поставщикам.\nТекст ошибки: {e}'
                ),
            )


async def cleanup_order_reports_task(app: FastAPI):
    async_session_factory = app.state.session_factory
    async with async_session_factory() as session:
        try:
            inbox_settings = (
                await crud_customer_order_inbox_settings.get_or_create(
                    session
                )
            )
            reports_removed = await asyncio.to_thread(cleanup_order_reports)
            error_days = max(
                1, int(inbox_settings.error_file_retention_days or 5)
            )
            error_removed = await asyncio.to_thread(
                cleanup_order_error_files, error_days
            )
            logger.info(
                'Cleanup order reports removed %s reports and %s error files',
                reports_removed,
                error_removed,
            )
        except Exception as e:
            logger.error(
                'Error in cleanup_order_reports_task: %s',
                e,
                exc_info=True,
            )
            await _notify_scheduler_issue(
                session,
                subject='Ошибка регламента очистки отчетов заказов',
                text=(
                    'Ошибка при автоматической очистке отчетов по заказам.\n'
                    f'Текст ошибки: {e}'
                ),
            )


async def cleanup_tracking_orders_task(app: FastAPI):
    async_session_factory = app.state.session_factory
    async with async_session_factory() as session:
        try:
            summary = await cleanup_old_tracking_history(session=session)
            logger.info('Cleanup tracking orders summary: %s', summary)
        except Exception as e:
            logger.error(
                'Error in cleanup_tracking_orders_task: %s',
                e,
                exc_info=True,
            )
            await _notify_scheduler_issue(
                session,
                subject='Ошибка очистки истории наших заказов',
                text=(
                    'Ошибка при автоматической очистке истории заказов'
                    ' из поиска по артикулу.\n'
                    f'Текст ошибки: {e}'
                ),
            )


def _day_key(now: datetime) -> str:
    mapping = {
        0: 'mon',
        1: 'tue',
        2: 'wed',
        3: 'thu',
        4: 'fri',
        5: 'sat',
        6: 'sun',
    }
    return mapping[now.weekday()]


async def _should_run_scheduled_job(
    session: AsyncSession, key: str
) -> tuple[bool, object | None]:
    defaults = SCHEDULER_SETTING_DEFAULTS.get(key)
    if not defaults:
        return True, None
    setting = await crud_scheduler_setting.get_or_create(
        session=session, key=key, defaults=defaults
    )
    if not setting.enabled:
        return False, setting
    now = now_moscow()
    days = setting.days or defaults.get('days', [])
    times = setting.times or defaults.get('times', [])
    day_key = _day_key(now)
    time_key = now.strftime('%H:%M')
    if days and day_key not in days:
        return False, setting
    if times and time_key not in times:
        return False, setting
    if setting.last_run_at:
        last_key = setting.last_run_at.strftime('%Y-%m-%d %H:%M')
        now_key = now.strftime('%Y-%m-%d %H:%M')
        if last_key == now_key:
            return False, setting
    return True, setting


async def _mark_scheduler_ran(
    session: AsyncSession, setting, when: datetime
) -> None:
    setting.last_run_at = when
    session.add(setting)
    await session.commit()


async def send_scheduled_customer_pricelists_task(app: FastAPI):
    """
    Проверяет расписания customer pricelist configs и отправляет,
    если текущий день/время совпадают.
    """
    async_session_factory = app.state.session_factory
    now = now_moscow()
    day_key = _day_key(now)
    time_key = now.strftime('%H:%M')

    async with async_session_factory() as session:
        try:
            stmt = (
                select(CustomerPriceListConfig)
                .options(selectinload(CustomerPriceListConfig.customer))
                .where(CustomerPriceListConfig.is_active.is_(True))
            )
            configs = (await session.execute(stmt)).scalars().all()

            for config in configs:
                if not config.schedule_days or not config.schedule_times:
                    continue
                if day_key not in (config.schedule_days or []):
                    continue
                if time_key not in (config.schedule_times or []):
                    continue
                if config.last_sent_at:
                    last_key = config.last_sent_at.strftime('%Y-%m-%d %H:%M')
                    now_key = now.strftime('%Y-%m-%d %H:%M')
                    if last_key == now_key:
                        continue

                customer = config.customer
                if not customer:
                    continue

                request = CustomerPriceListCreate(
                    customer_id=customer.id,
                    config_id=config.id,
                    items=[],
                )
                try:
                    await process_customer_pricelist(
                        customer=customer, request=request, session=session
                    )
                except Exception as exc:
                    logger.error(
                        'Error in send_scheduled_customer_pricelists_task '
                        'for config %s: %s',
                        config.id,
                        exc,
                        exc_info=True,
                    )
                    await _notify_scheduler_issue(
                        session,
                        subject='Ошибка регламента отправки прайсов клиентам',
                        text=(
                            'Ошибка при автоматической отправке прайса '
                            f'клиенту для config_id={config.id}.\n'
                            f'Текст ошибки: {exc}'
                        ),
                    )
        except Exception as e:
            logger.error(
                f'Error in send_scheduled_customer_pricelists_task: {e}',
                exc_info=True,
            )
            await _notify_scheduler_issue(
                session,
                subject='Ошибка регламента отправки прайсов клиентам',
                text=(
                    'Ошибка при автоматической отправке прайсов клиентам.\n'
                    f'Текст ошибки: {e}'
                ),
            )


async def price_control_run_task(app: FastAPI):
    async_session_factory = app.state.session_factory
    now = now_moscow()
    day_key = _day_key(now)
    time_key = now.strftime('%H:%M')
    async with async_session_factory() as session:
        try:
            stmt = select(PriceControlConfig).where(
                PriceControlConfig.is_active.is_(True)
            )
            configs = (await session.execute(stmt)).scalars().all()
            if not configs:
                return
            for config in configs:
                schedule_days = config.schedule_days or []
                schedule_times = config.schedule_times or ['09:00']
                if schedule_days and day_key not in schedule_days:
                    continue
                if schedule_times and time_key not in schedule_times:
                    continue
                if config.last_run_at:
                    last_key = config.last_run_at.strftime('%Y-%m-%d %H:%M')
                    now_key = now.strftime('%Y-%m-%d %H:%M')
                    if last_key == now_key:
                        continue
                try:
                    await run_price_control(session, config)
                except Exception as exc:
                    logger.error(
                        'Error running price control for config %s: %s',
                        config.id,
                        exc,
                        exc_info=True,
                    )
                    await _notify_scheduler_issue(
                        session,
                        subject='Ошибка регламента контроля цен',
                        text=(
                            'Ошибка при автоматическом контроле цен '
                            f'для config_id={config.id}.\n'
                            f'Текст ошибки: {exc}'
                        ),
                    )
            logger.info('Completed price_control_run_task')
        except Exception as exc:
            logger.error(
                f'Error in price_control_run_task: {exc}',
                exc_info=True,
            )
            await _notify_scheduler_issue(
                session,
                subject='Ошибка регламента контроля цен',
                text=(
                    'Ошибка при автоматическом запуске контроля цен.\n'
                    f'Текст ошибки: {exc}'
                ),
            )


async def process_new_provider_emails(session: AsyncSession, app: FastAPI):
    """
    Обрабатывает все новые письма за сегодня,
    скачивая файлы для провайдеров и далее
    запускает функцию обработки прайслеста для каждого провайдера.
    """
    logger.info('Начинаем обработку писем провайдеров...')
    start_time = time.perf_counter()
    downloaded = await get_emails(session=session)

    email_time = time.perf_counter()
    logger.info(
        f'get_emails() выполнена за {email_time - start_time:.2f} секунд'
    )

    if not downloaded:
        logger.info('Новых писем для обработки не найдено.')
        return
    sem = asyncio.Semaphore(2)
    process_start = time.perf_counter()
    tasks = [
        asyncio.create_task(_process_one(item, app, sem))
        for item in downloaded
    ]
    try:
        results = await asyncio.gather(*tasks, return_exceptions=True)
    except asyncio.CancelledError:
        if getattr(app.state, 'is_shutting_down', False):
            logger.info('Отмена обработки писем провайдеров при остановке')
            return
        raise
    process_end = time.perf_counter()
    logger.info(
        f'Обработка прайса выполнена '
        f'за {process_end - process_start:.2f} секунд'
    )
    successful = 0
    errors = 0
    for result in results:
        if isinstance(result, Exception):
            errors += 1
            logger.error(
                f'Ошибка обработки прайс-листа: {result}', exc_info=True
            )
        else:
            successful += 1

    total_time = time.perf_counter() - start_time
    logger.info(
        f'process_new_provider_emails завершена за {total_time:.2f} секунд. '
        f'Успешно: {successful}, Ошибок: {errors}'
    )


def _is_price_check_due(schedule) -> bool:
    if not schedule.enabled:
        return False
    if not schedule.days or not schedule.times:
        return True
    now = now_moscow()
    day_key = now.strftime('%a').lower()[:3]
    time_key = now.strftime('%H:%M')
    return day_key in (schedule.days or []) and time_key in (
        schedule.times or []
    )


async def download_price_provider_task(app: FastAPI):
    logger.info('Starting download_price_provider_task')
    async_session_factory = app.state.session_factory
    async with async_session_factory() as session:
        try:
            schedule = await crud_price_check_schedule.get_or_create(session)
            if not _is_price_check_due(schedule):
                logger.info('Price check skipped by schedule')
                await crud_price_check_log.create(
                    session=session,
                    status='SKIP',
                    message='Skipped by schedule',
                )
                return
            # Проверяю наличие поставщика
            provider = await crud_provider.get_provider_or_none(
                provider=PROVIDER_IN['name'], session=session
            )
            if not provider:
                provider_in_model = ProviderCreate(**PROVIDER_IN)
                provider = await crud_provider.create(
                    obj_in=provider_in_model, session=session
                )
                config_in_model = ProviderPriceListConfigCreate(
                    **CONFIG_DATA_PROVIDER
                )
                await crud_provider_pricelist_config.create(
                    provider_id=provider.id,
                    config_in=config_in_model,
                    session=session,
                )
                logger.info(f'Created initial provider with id: {provider.id}')
            await process_new_provider_emails(session, app)
            schedule.last_checked_at = now_moscow()
            session.add(schedule)
            await session.commit()
            await crud_price_check_log.create(
                session=session,
                status='OK',
                message='Price check completed',
            )
            logger.info('Completed download_price_provider_task')
        except asyncio.CancelledError:
            if getattr(app.state, 'is_shutting_down', False):
                logger.info(
                    'download_price_provider_task отменена при остановке'
                )
                return
            raise
        except Exception as e:
            logger.error(f'Error in download_price_provider_task: {e}')
            await _notify_scheduler_issue(
                session,
                subject='Ошибка регламента загрузки прайсов',
                text=(
                    'Ошибка при автоматической загрузке прайсов '
                    'поставщиков.\n'
                    f'Текст ошибки: {e}'
                ),
            )
            try:
                await crud_price_check_log.create(
                    session=session,
                    status='ERROR',
                    message=str(e)[:240],
                )
            except Exception:
                pass


async def cleanup_old_pricelists_task(app: FastAPI):
    async_session_factory = app.state.session_factory
    async with async_session_factory() as session:
        try:
            should_run, setting = await _should_run_scheduled_job(
                session, 'cleanup_old_pricelists'
            )
            if not should_run:
                return
            logger.info('Starting cleanup_old_pricelists_task')
            total_deleted = 0
            total_deleted_customer = 0
            while True:
                cleanup = crud_pricelist.cleanup_old_pricelists_keep_last_n
                deleted = await cleanup(
                    session=session,
                    keep_last_n=5,
                    batch_size=500,
                )
                total_deleted += deleted
                if deleted == 0:
                    break
            while True:
                cleanup = (
                    crud_customer_pricelist.cleanup_old_pricelists_keep_last_n
                )
                deleted = await cleanup(
                    session=session,
                    keep_last_n=10,
                    batch_size=500,
                )
                total_deleted_customer += deleted
                if deleted == 0:
                    break
            logger.info(
                f'Cleanup finished. '
                f'Deleted provider pricelists: {total_deleted}; '
                f'deleted customer pricelists: {total_deleted_customer}'
            )
            if setting:
                await _mark_scheduler_ran(session, setting, now_moscow())
        except Exception as e:
            logger.error(
                f'Error in cleanup_old_pricelists_task: {e}',
                exc_info=True
            )
            await _notify_scheduler_issue(
                session,
                subject='Ошибка регламента очистки прайсов',
                text=(
                    'Ошибка при автоматической очистке старых прайсов.\n'
                    f'Текст ошибки: {e}'
                ),
            )
            await session.rollback()


async def check_provider_pricelist_staleness_task(app: FastAPI):
    logger.info('Starting check_provider_pricelist_staleness_task')
    async_session_factory = app.state.session_factory
    async with async_session_factory() as session:
        try:
            now = now_moscow()
            stmt = (
                select(ProviderPriceListConfig)
                .options(selectinload(ProviderPriceListConfig.provider))
                .where(ProviderPriceListConfig.is_active.is_(True))
            )
            configs = (await session.execute(stmt)).scalars().all()

            for config in configs:
                threshold = config.max_days_without_update or 3
                if threshold <= 0:
                    continue
                last_price_stmt = (
                    select(PriceList.date)
                    .where(PriceList.provider_config_id == config.id)
                    .order_by(PriceList.date.desc())
                    .limit(1)
                )
                last_date = (await session.execute(last_price_stmt)).scalar()
                if not last_date:
                    continue
                days_diff = (now.date() - last_date).days
                if days_diff <= threshold:
                    continue

                if (
                    config.last_stale_alert_at
                    and config.last_stale_alert_at.date() == now.date()
                ):
                    continue

                if config.provider_id:
                    await crud_price_stale_alert.create(
                        session=session,
                        provider_id=config.provider_id,
                        provider_config_id=config.id,
                        days_diff=days_diff,
                        last_price_date=last_date,
                    )
                config.last_stale_alert_at = now
                session.add(config)

            await session.commit()
            logger.info('Completed check_provider_pricelist_staleness_task')
        except Exception as e:
            logger.error(
                f'Error in check_provider_pricelist_staleness_task: {e}',
                exc_info=True
            )
            await _notify_scheduler_issue(
                session,
                subject='Ошибка регламента проверки устаревших прайсов',
                text=(
                    'Ошибка при автоматической проверке давности прайсов.\n'
                    f'Текст ошибки: {e}'
                ),
            )
            await session.rollback()


async def notify_pricelist_stale_task(app: FastAPI):
    async_session_factory = app.state.session_factory
    async with async_session_factory() as session:
        try:
            should_run, setting = await _should_run_scheduled_job(
                session, 'pricelist_stale_notify'
            )
            if not should_run:
                return
            logger.info('Starting notify_pricelist_stale_task')
            now = now_moscow()
            start = now.replace(hour=0, minute=0, second=0, microsecond=0)
            end = start + timedelta(days=1)
            stmt = (
                select(PriceListStaleAlert, ProviderPriceListConfig, Provider)
                .join(
                    ProviderPriceListConfig,
                    ProviderPriceListConfig.id
                    == PriceListStaleAlert.provider_config_id,
                )
                .join(
                    Provider,
                    Provider.id == PriceListStaleAlert.provider_id,
                )
                .where(
                    PriceListStaleAlert.created_at >= start,
                    PriceListStaleAlert.created_at < end,
                )
                .order_by(
                    Provider.name.asc(),
                    ProviderPriceListConfig.name_price.asc().nulls_last(),
                    PriceListStaleAlert.days_diff.desc(),
                )
            )
            rows = (await session.execute(stmt)).all()
            if not rows:
                logger.info('No stale pricelist alerts for today')
                return

            seen_keys = set()
            unique_rows = []
            for alert, config, provider in rows:
                key = (alert.provider_id, alert.provider_config_id)
                if key in seen_keys:
                    continue
                seen_keys.add(key)
                unique_rows.append((alert, config, provider))

            lines = ['Проблемы с обновлением прайсов:']
            for alert, config, provider in unique_rows:
                config_label = config.name_price or f'#{config.id}'
                lines.append(
                    f'- {provider.name} ({config_label}) — '
                    f'{alert.days_diff} дн. Последний: {alert.last_price_date}'
                )
            await create_admin_notifications(
                session=session,
                title='Проблемы с обновлением прайсов',
                message='\n'.join(lines),
                level='warning',
                link='/admin/settings',
                commit=False,
            )
            await session.commit()
            logger.info('Sent stale pricelist notification to admins')
            if setting:
                await _mark_scheduler_ran(session, setting, now)
        except Exception as e:
            logger.error(
                f'Error in notify_pricelist_stale_task: {e}',
                exc_info=True
            )
            await _notify_scheduler_issue(
                session,
                subject='Ошибка регламента уведомлений об устаревших прайсах',
                text=(
                    'Ошибка при автоматической отправке уведомлений '
                    f'об устаревших прайсах.\nТекст ошибки: {e}'
                ),
            )


async def cleanup_pricelist_stale_alerts_task(app: FastAPI):
    async_session_factory = app.state.session_factory
    async with async_session_factory() as session:
        try:
            should_run, setting = await _should_run_scheduled_job(
                session, 'pricelist_stale_cleanup'
            )
            if not should_run:
                return
            logger.info('Starting cleanup_pricelist_stale_alerts_task')
            now = now_moscow()
            cutoff = now - timedelta(days=PRICELIST_STALE_ALERT_RETENTION_DAYS)
            stmt = delete(PriceListStaleAlert).where(
                PriceListStaleAlert.created_at < cutoff
            )
            result = await session.execute(stmt)
            await session.commit()
            logger.info(
                'Cleanup stale pricelist alerts removed %s rows',
                result.rowcount or 0,
            )
            if setting:
                await _mark_scheduler_ran(session, setting, now)
        except Exception as e:
            logger.error(
                f'Error in cleanup_pricelist_stale_alerts_task: {e}',
                exc_info=True
            )
            await _notify_scheduler_issue(
                session,
                subject='Ошибка регламента очистки алертов по прайсам',
                text=(
                    'Ошибка при автоматической очистке алертов '
                    f'по прайсам.\nТекст ошибки: {e}'
                ),
            )
            await session.rollback()


async def collect_system_metrics_snapshot_task(app: FastAPI):
    async_session_factory = app.state.session_factory
    async with async_session_factory() as session:
        try:
            should_run, setting = await _should_run_scheduled_job(
                session, 'metrics_snapshot'
            )
            if not should_run:
                return
            logger.info('Starting collect_system_metrics_snapshot_task')
            summary = await get_monitor_summary(session=session, app=app)
            payload = build_snapshot_payload(summary)
            await crud_system_metric_snapshot.create(
                session=session, payload=payload
            )
            if setting:
                await _mark_scheduler_ran(session, setting, now_moscow())
            logger.info('Completed collect_system_metrics_snapshot_task')
        except Exception as e:
            logger.error(
                f'Error in collect_system_metrics_snapshot_task: {e}',
                exc_info=True
            )
            await _notify_scheduler_issue(
                session,
                subject='Ошибка регламента сбора системных метрик',
                text=(
                    'Ошибка при автоматическом сборе системных метрик.\n'
                    f'Текст ошибки: {e}'
                ),
            )


async def check_watchlist_site_task(app: FastAPI):
    async_session_factory = app.state.session_factory
    async with async_session_factory() as session:
        try:
            should_run, setting = await _should_run_scheduled_job(
                session, 'watchlist_site_check'
            )
            if not should_run:
                return
            logger.info('Starting check_watchlist_site_task')
            await check_watchlist_site(session)
            logger.info('Completed check_watchlist_site_task')
            if setting:
                await _mark_scheduler_ran(session, setting, now_moscow())
        except Exception as e:
            logger.error(f'Error in check_watchlist_site_task: {e}')
            await _notify_scheduler_issue(
                session,
                subject='Ошибка регламента проверки watchlist сайта',
                text=(
                    'Ошибка при автоматической проверке watchlist сайта.\n'
                    f'Текст ошибки: {e}'
                ),
            )


async def notify_watchlist_task(app: FastAPI):
    async_session_factory = app.state.session_factory
    async with async_session_factory() as session:
        try:
            should_run, setting = await _should_run_scheduled_job(
                session, 'watchlist_notify'
            )
            if not should_run:
                return
            logger.info('Starting notify_watchlist_task')
            await send_watchlist_daily_notifications(session)
            logger.info('Completed notify_watchlist_task')
            if setting:
                await _mark_scheduler_ran(session, setting, now_moscow())
        except Exception as e:
            logger.error(f'Error in notify_watchlist_task: {e}')
            await _notify_scheduler_issue(
                session,
                subject='Ошибка регламента уведомлений watchlist',
                text=(
                    'Ошибка при автоматической отправке уведомлений '
                    f'watchlist.\nТекст ошибки: {e}'
                ),
            )
