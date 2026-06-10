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

from dz_fastapi.core.constants import (
    CONFIG_DATA_CUSTOMER,
    CONFIG_DATA_PROVIDER,
    CUSTOMER,
    CUSTOMER_IN,
    PROVIDER_IN,
)
from dz_fastapi.core.scheduler_settings import SCHEDULER_SETTING_DEFAULTS
from dz_fastapi.core.time import now_moscow
from dz_fastapi.crud.partner import (
    crud_customer,
    crud_customer_pricelist,
    crud_customer_pricelist_config,
    crud_pricelist,
    crud_provider,
    crud_provider_pricelist_config,
)
from dz_fastapi.crud.settings import (
    crud_customer_order_inbox_settings,
    crud_diadoc_integration_settings,
    crud_price_check_log,
    crud_price_check_schedule,
    crud_price_stale_alert,
    crud_scheduler_setting,
    crud_system_metric_snapshot,
)
from dz_fastapi.models.inventory import ReserveStatus, StockReserve
from dz_fastapi.models.partner import (
    CustomerPriceListConfig,
    PriceList,
    Provider,
    ProviderPriceListConfig,
    SupplierOrderMessage,
)
from dz_fastapi.models.price_control import PriceControlConfig
from dz_fastapi.models.settings import PriceListStaleAlert
from dz_fastapi.schemas.partner import (
    CustomerCreate,
    CustomerPriceListConfigCreate,
    CustomerPriceListCreate,
    ProviderCreate,
    ProviderPriceListConfigCreate,
)
from dz_fastapi.services.autopurchase import execute_next_autopurchase_run
from dz_fastapi.services.crosses import sync_automatic_oem_crosses
from dz_fastapi.services.customer_orders import (
    cleanup_order_error_files,
    cleanup_order_reports,
    process_customer_orders,
    send_scheduled_supplier_orders,
)
from dz_fastapi.services.diadoc_documents import sync_diadoc_incoming_documents
from dz_fastapi.services.diadoc_integration import get_diadoc_client_for_session
from dz_fastapi.services.email import get_emails
from dz_fastapi.services.inbox_email import cleanup_inbox_emails, fetch_and_store_emails
from dz_fastapi.services.monitoring import (
    build_snapshot_payload,
    get_monitor_summary,
    tracked_execution,
)
from dz_fastapi.services.notifications import create_admin_notifications, notify_admin_all
from dz_fastapi.services.order_timing import (
    OUTSIDE_WINDOW_SLOW_SECONDS,
    get_active_supplier_response_provider_ids,
    get_overdue_customer_windows,
    get_overdue_supplier_responses,
    is_in_any_order_window,
)
from dz_fastapi.services.placed_orders import (
    cleanup_old_tracking_history,
    sync_site_tracking_statuses,
)
from dz_fastapi.services.price_control import run_price_control
from dz_fastapi.services.process import process_customer_pricelist, process_provider_pricelist
from dz_fastapi.services.runtime_memory import process_rss_mb, trim_process_memory
from dz_fastapi.services.supplier_order_responses import process_supplier_response_messages
from dz_fastapi.services.supplier_workflow import mark_auto_refused_supplier_items
from dz_fastapi.services.watchlist import send_watchlist_daily_notifications
from dz_fastapi.services.watchlist_site import check_watchlist_site

logger = logging.getLogger("dz_fastapi")
EMAIL_NAME_ORDER = os.getenv("EMAIL_NAME_ORDERS")
EMAIL_PASSWORD_ORDER = os.getenv("EMAIL_PASSWORD_ORDERS")
EMAIL_HOST_ORDER = os.getenv("EMAIL_HOST_ORDERS")
PRICELIST_STALE_ALERT_RETENTION_DAYS = int(
    os.getenv("PRICELIST_STALE_ALERT_RETENTION_DAYS", "7")
)
CLEANUP_OLD_PRICELISTS_CATCH_UP_MINUTES = int(
    os.getenv("CLEANUP_OLD_PRICELISTS_CATCH_UP_MINUTES", "360")
)
SCHEDULER_CATCH_UP_MINUTES = {
    "watchlist_site_check": 180,
    "watchlist_notify": 180,
    "pricelist_stale_notify": 180,
    "cleanup_old_pricelists": CLEANUP_OLD_PRICELISTS_CATCH_UP_MINUTES,
    "pricelist_stale_cleanup": CLEANUP_OLD_PRICELISTS_CATCH_UP_MINUTES,
    "metrics_snapshot": 120,
}
ENABLE_LEGACY_ZZAP_AUTO_SEND = os.getenv(
    "ENABLE_LEGACY_ZZAP_AUTO_SEND", "0"
).strip().lower() in {"1", "true", "yes", "on"}


def _env_int_with_min(
    name: str,
    default: int,
    min_value: int = 1,
    max_value: int = 59,
) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        value = int(str(raw).strip())
    except (TypeError, ValueError):
        logger.warning(
            "Invalid integer env %s=%r. Using default=%s",
            name,
            raw,
            default,
        )
        return default
    if value < min_value or value > max_value:
        logger.warning(
            "Out-of-range env %s=%s. Clamping to [%s, %s].",
            name,
            value,
            min_value,
            max_value,
        )
    return max(min_value, min(value, max_value))


CUSTOMER_ORDERS_CHECK_MINUTES = _env_int_with_min(
    "SCHED_CUSTOMER_ORDERS_EVERY_MINUTES", 2
)
SUPPLIER_RESPONSES_CHECK_MINUTES = _env_int_with_min(
    "SCHED_SUPPLIER_RESPONSES_EVERY_MINUTES", 2
)
FETCH_INBOX_EMAILS_MINUTES = _env_int_with_min(
    "SCHED_FETCH_INBOX_EVERY_MINUTES", 30
)
SUPPLIER_DOCUMENTS_CHECK_MINUTES = _env_int_with_min(
    "SCHED_SUPPLIER_DOCUMENTS_EVERY_MINUTES", 30
)
DIADOC_INBOUND_SYNC_MINUTES = _env_int_with_min(
    "SCHED_DIADOC_INBOUND_EVERY_MINUTES", 15, min_value=5, max_value=59
)
PRICE_PROVIDER_PROCESS_PARALLELISM = _env_int_with_min(
    "PRICE_PROVIDER_PROCESS_PARALLELISM", 1, min_value=1, max_value=4
)
AUTOPURCHASE_QUEUE_POLL_SECONDS = _env_int_with_min(
    "AUTOPURCHASE_QUEUE_POLL_SECONDS", 15, min_value=5, max_value=300
)
# How long to "slow poll" for orders when outside expected windows (minutes)
ORDERS_SLOW_POLL_MINUTES = _env_int_with_min(
    "SCHED_ORDERS_SLOW_POLL_MINUTES", 20, min_value=5, max_value=59
)


def _process_rss_mb() -> float | None:
    return process_rss_mb()


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
            level="error",
            link="/admin/settings",
        )
    except Exception as exc:
        logger.error(
            "Failed to create scheduler app notification: %s",
            exc,
            exc_info=True,
        )


async def _close_stale_supplier_response_messages(
    session: AsyncSession,
) -> tuple[int, int]:
    settings = await crud_customer_order_inbox_settings.get_or_create(
        session=session
    )
    auto_close_enabled = bool(
        getattr(
            settings,
            "supplier_response_auto_close_stale_enabled",
            True,
        )
    )
    stale_days = max(
        1,
        int(getattr(settings, "supplier_response_stale_days", 7) or 7),
    )
    if not auto_close_enabled:
        return 0, stale_days

    cutoff_dt = now_moscow() - timedelta(days=stale_days)
    rows = (
        (
            await session.execute(
                select(SupplierOrderMessage).where(
                    SupplierOrderMessage.message_type.in_(
                        ["IMPORT_ERROR", "RETRY_PENDING"]
                    ),
                    SupplierOrderMessage.received_at <= cutoff_dt,
                )
            )
        )
        .scalars()
        .all()
    )
    if not rows:
        return 0, stale_days

    closed_note = f"Автозакрыто как устаревшее: старше {stale_days} дн."
    for row in rows:
        details = str(row.import_error_details or "").strip()
        if details:
            if closed_note not in details:
                row.import_error_details = (f"{details}; {closed_note}")[:500]
        else:
            row.import_error_details = closed_note[:500]
        row.message_type = "IGNORED"
        session.add(row)
    await session.commit()
    return len(rows), stale_days


async def expire_reserves_task(app: FastAPI):
    """Переводит ACTIVE резервы с истёкшим expires_at в статус EXPIRED."""
    async with new_session_from_app(app) as session:
        now = now_moscow()
        stmt = select(StockReserve).where(
            StockReserve.status == ReserveStatus.ACTIVE,
            StockReserve.expires_at.isnot(None),
            StockReserve.expires_at <= now,
        )
        rows = (await session.execute(stmt)).scalars().all()
        if rows:
            for reserve in rows:
                reserve.status = ReserveStatus.EXPIRED
            await session.commit()
            logger.info("expire_reserves: expired %d reserves", len(rows))


async def process_autopurchase_runs_task(app: FastAPI):
    async with new_session_from_app(app) as session:
        run_id = await execute_next_autopurchase_run(session)
    if run_id is not None:
        logger.info("Processed autopurchase run id=%s", run_id)


def start_scheduler(app: FastAPI):
    scheduler = AsyncIOScheduler()
    scheduler.configure(
        timezone="Europe/Moscow",
        job_defaults={
            "coalesce": True,       # Несколько пропущенных триггеров → 1 запуск,
                                    # не очередь. Если задача опоздала на 5 минут,
                                    # она запустится 1 раз, а не 20.
            "max_instances": 1,     # Никогда не запускать параллельные копии.
            "misfire_grace_time": None,  # Запускать всегда, без ограничения по
                                         # времени опоздания. Вместо "was missed"
                                         # задача встаёт в очередь и выполняется
                                         # как только event loop освободится.
        },
    )

    scheduler.add_job(
        func=process_autopurchase_runs_task,
        trigger="interval",
        args=[app],
        id="process_autopurchase_runs",
        name="Process queued autopurchase runs",
        seconds=AUTOPURCHASE_QUEUE_POLL_SECONDS,
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        next_run_time=now_moscow(),
    )

    # ── РАСПИСАНИЕ ПО ВРЕМЕННЫМ ОКНАМ (московское время) ────────────────────
    # Все часы — московское время (timezone='Europe/Moscow').
    # Задачи разнесены по секундам старта (second=N) чтобы не создавать
    # «гром стада» на первой секунде каждой минуты.
    #
    # Приоритеты нагрузки:
    #   1. Заказы клиентов — критичные; интенсивно в 08–10 и 13–16 МСК,
    #      фоново только вне вечернего окна сотрудников.
    #   2. Ответы поставщиков — активный цикл оставляем в рабочее время,
    #      фоновый запуск только утром; ночью и вечером не нужен.
    #   3. Документы поставщиков, Диадок, inbox — рабочий день 09–18 МСК.
    #   4. Прайсы поставщиков и трекинг сайта — только ночью, без старта
    #      в вечернее окно 19:00–00:30 МСК.
    #   5. Технические задачи очистки — 02–04 МСК (минимум активности).

    # ── 1. Заказы клиентов ────────────────────────────────────────────────
    # Цикл 1: 08:XX–09:XX МСК — интенсивный (каждые N мин)
    scheduler.add_job(
        func=download_customer_orders_task,
        trigger="cron",
        args=[app],
        id="download_customer_orders_cycle1",
        name="Download customer orders — цикл 1 (08–10 МСК)",
        hour="8-9",
        minute=f"*/{CUSTOMER_ORDERS_CHECK_MINUTES}",
        second=15,
        replace_existing=True,
    )
    # Цикл 2: 13:XX–15:XX МСК — интенсивный
    scheduler.add_job(
        func=download_customer_orders_task,
        trigger="cron",
        args=[app],
        id="download_customer_orders_cycle2",
        name="Download customer orders — цикл 2 (13–16 МСК)",
        hour="13-15",
        minute=f"*/{CUSTOMER_ORDERS_CHECK_MINUTES}",
        second=15,
        replace_existing=True,
    )
    # Фоновый режим: вне циклов и вне вечернего окна сотрудников.
    # Вечер 19:00–00:30 МСК оставляем свободным от этого регламента.
    scheduler.add_job(
        func=download_customer_orders_task,
        trigger="cron",
        args=[app],
        id="download_customer_orders_bg",
        name="Download customer orders — фон (вне циклов)",
        hour="1-7,10-12,16-18",
        minute=f"*/{ORDERS_SLOW_POLL_MINUTES}",
        second=15,
        replace_existing=True,
    )

    # ── 2. Ответы поставщиков ─────────────────────────────────────────────
    # Активный приём: 09–15 МСК, каждые N мин
    scheduler.add_job(
        func=process_supplier_responses_task,
        trigger="cron",
        args=[app],
        id="process_supplier_responses_active",
        name="Process supplier responses — активный (09–16 МСК)",
        hour="9-15",
        minute=f"*/{SUPPLIER_RESPONSES_CHECK_MINUTES}",
        second=25,
        replace_existing=True,
    )
    # Фоновый режим: только раннее утро. Ночью и вечером этот регламент
    # не запускаем, потому что ответы поставщиков приходят утром.
    scheduler.add_job(
        func=process_supplier_responses_task,
        trigger="cron",
        args=[app],
        id="process_supplier_responses_bg",
        name="Process supplier responses — фон (вне 09–16 МСК)",
        hour="7-8",
        minute="*/30",
        second=25,
        replace_existing=True,
    )

    # ── 3. Документы поставщиков (УПД/накладные) ─────────────────────────
    # Рабочий день 09–17 МСК, каждые N мин
    scheduler.add_job(
        func=process_supplier_documents_task,
        trigger="cron",
        args=[app],
        id="process_supplier_documents",
        name="Process supplier documents (УПД/накладные)",
        hour="9-17",
        minute=f"*/{SUPPLIER_DOCUMENTS_CHECK_MINUTES}",
        second=35,
        replace_existing=True,
    )

    # ── 4. Прайсы поставщиков ─────────────────────────────────────────────
    # Ночной прогон: 01–07 МСК — каждый час (тяжёлый, pandas).
    # Убираем старты в 22:00, 23:00 и 00:00, чтобы не мешать вечерней работе.
    scheduler.add_job(
        func=download_price_provider_task,
        trigger="cron",
        args=[app],
        id="download_price_provider_night",
        name="Download price provider — ночь (01–07 МСК)",
        hour="1-7",
        minute=0,
        second=0,
        jitter=10,
        replace_existing=True,
    )
    # Дневной однократный лёгкий прогон: 13:00 МСК
    scheduler.add_job(
        func=download_price_provider_task,
        trigger="cron",
        args=[app],
        id="download_price_provider_noon",
        name="Download price provider — полдень (13:00 МСК)",
        hour=13,
        minute=0,
        second=0,
        replace_existing=True,
    )
    # Ночная синхронизация автокроссов Dragonzap/original.
    scheduler.add_job(
        func=sync_auto_oem_crosses_task,
        trigger="cron",
        args=[app],
        id="sync_auto_oem_crosses",
        name="Sync automatic Dragonzap/original crosses",
        hour=2,
        minute=10,
        second=0,
        replace_existing=True,
    )

    # ── 5. Отправка прайсов клиентам ─────────────────────────────────────
    # Рабочий день 08–18 МСК, каждые 5 мин
    scheduler.add_job(
        func=send_scheduled_customer_pricelists_task,
        trigger="cron",
        args=[app],
        id="send_customer_pricelists",
        name="Send scheduled customer pricelists",
        hour="8-18",
        minute="*/5",
        second=5,
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )

    # ── 6. Контроль цен ───────────────────────────────────────────────────
    # Рабочий день 08–18 МСК, каждые 5 мин
    scheduler.add_job(
        func=price_control_run_task,
        trigger="cron",
        args=[app],
        id="price_control_run",
        name="Price control run",
        hour="8-18",
        minute="*/5",
        second=10,
        replace_existing=True,
    )

    # ── 7. Алерты тайминга заказов ────────────────────────────────────────
    # 07–20 МСК, каждые 2 мин (вне рабочих часов нет смысла)
    scheduler.add_job(
        func=check_order_timing_alerts_task,
        trigger="cron",
        args=[app],
        id="check_order_timing_alerts",
        name="Check missing orders and supplier responses",
        hour="7-20",
        minute="*/2",
        second=45,
        replace_existing=True,
    )

    # ── 8. Синхронизация Диадок-входящих ─────────────────────────────────
    # Рабочий день 09–18 МСК, каждые N мин
    scheduler.add_job(
        func=sync_diadoc_inbound_task,
        trigger="cron",
        args=[app],
        id="sync_diadoc_inbound",
        name="Sync Diadoc inbound documents",
        hour="9-18",
        minute=f"*/{DIADOC_INBOUND_SYNC_MINUTES}",
        second=55,
        replace_existing=True,
    )

    # ── 9. Watchlist — проверка сайта ─────────────────────────────────────
    # Ночь 01–07 МСК (guard в задаче дополнительно ограничивает по настройкам)
    scheduler.add_job(
        func=check_watchlist_site_task,
        trigger="cron",
        args=[app],
        id="check_watchlist_site",
        name="Check watchlist site offers",
        hour="1-7",
        minute="*/10",
        second=20,
        replace_existing=True,
    )

    # ── 10. Watchlist — уведомления ──────────────────────────────────────
    # 08–22 МСК, каждые 10 мин
    scheduler.add_job(
        func=notify_watchlist_task,
        trigger="cron",
        args=[app],
        id="notify_watchlist",
        name="Notify watchlist",
        hour="8-22",
        minute="*/10",
        second=30,
        replace_existing=True,
    )

    # ── 11. Уведомления об устаревших прайсах ─────────────────────────────
    # Рабочий день 09–18 МСК, каждые 10 мин (guard дедуплицирует на день)
    scheduler.add_job(
        func=notify_pricelist_stale_task,
        trigger="cron",
        args=[app],
        id="notify_pricelist_stale",
        name="Notify stale pricelists",
        hour="9-18",
        minute="*/10",
        second=40,
        replace_existing=True,
    )

    # ── 12. Проверка устаревания прайсов поставщиков ──────────────────────
    # Один раз в день в 09:10 МСК (было каждый час в :10)
    scheduler.add_job(
        func=check_provider_pricelist_staleness_task,
        trigger="cron",
        args=[app],
        id="check_provider_pricelist_staleness",
        name="Check provider pricelist staleness",
        hour=9,
        minute=10,
        second=0,
        replace_existing=True,
    )

    # ── 13. Снимок системных метрик ───────────────────────────────────────
    # 07–22 МСК, каждые 5 мин
    scheduler.add_job(
        func=collect_system_metrics_snapshot_task,
        trigger="cron",
        args=[app],
        id="metrics_snapshot",
        name="Collect system metrics snapshot",
        hour="7-22",
        minute="*/5",
        second=55,
        replace_existing=True,
    )

    # ── 14. Отправка заказов поставщикам ──────────────────────────────────
    # Рабочий день 08–17 МСК, каждые 5 мин (смещение 2 мин)
    scheduler.add_job(
        func=send_scheduled_supplier_orders_task,
        trigger="cron",
        args=[app],
        id="send_supplier_orders",
        name="Send scheduled supplier orders",
        hour="8-17",
        minute="2-59/5",
        second=0,
        replace_existing=True,
    )

    # ── 15. Синхронизация статусов треккинга ──────────────────────────────
    # Только ночь 01–06 МСК — каждые 30 мин.
    # Вечернее окно сотрудников 19:00–00:30 не трогаем.
    scheduler.add_job(
        func=sync_site_tracking_statuses_task,
        trigger="cron",
        args=[app],
        id="sync_site_tracking_statuses",
        name="Sync Dragonzap tracking statuses",
        hour="1-6",
        minute="*/30",
        second=0,
        jitter=30,
        replace_existing=True,
    )

    # ── 16. Получение писем в inbox ───────────────────────────────────────
    # Рабочий день 08–18 МСК, каждые N мин
    scheduler.add_job(
        func=fetch_inbox_emails_task,
        trigger="cron",
        args=[app],
        id="fetch_inbox_emails",
        name="Fetch inbox emails (all accounts)",
        hour="8-18",
        minute=f"*/{FETCH_INBOX_EMAILS_MINUTES}",
        second=0,
        jitter=60,
        replace_existing=True,
    )

    # ── 17. Истекающие резервы ────────────────────────────────────────────
    # Рабочий день + вечер 08–21 МСК, каждые 15 мин
    scheduler.add_job(
        func=expire_reserves_task,
        trigger="cron",
        args=[app],
        id="expire_reserves",
        name="Expire overdue stock reserves",
        hour="8-21",
        minute="*/15",
        second=15,
        replace_existing=True,
    )

    # ── Ночные технические задачи (02–04 МСК) ─────────────────────────────
    # Очистка старых прайсов:
    # просыпаемся каждые 10 минут, а точное расписание берём из настройки.
    # Это даёт шанс догнать пропущенный запуск после OOM/рестарта.
    scheduler.add_job(
        func=cleanup_old_pricelists_task,
        trigger="cron",
        args=[app],
        id="cleanup_old_pricelists",
        name="Cleanup old pricelists keep last 5",
        hour="0-23",
        minute="*/10",
        second=0,
        replace_existing=True,
    )

    # 03:00 — очистка старых отчётов заказов
    scheduler.add_job(
        func=cleanup_order_reports_task,
        trigger="cron",
        args=[app],
        id="cleanup_order_reports",
        name="Cleanup order reports",
        hour=3,
        minute=0,
        replace_existing=True,
    )

    # 03:20 — очистка старых треккинг-заказов
    scheduler.add_job(
        func=cleanup_tracking_orders_task,
        trigger="cron",
        args=[app],
        id="cleanup_tracking_orders",
        name="Cleanup tracking orders older than 1 year",
        hour=3,
        minute=20,
        replace_existing=True,
    )

    # Очистка алертов по прайсам:
    # так же просыпаемся часто и полагаемся на guard/догоняющий запуск.
    scheduler.add_job(
        func=cleanup_pricelist_stale_alerts_task,
        trigger="cron",
        args=[app],
        id="cleanup_pricelist_stale_alerts",
        name="Cleanup stale pricelist alerts",
        hour="0-23",
        minute="*/10",
        second=0,
        replace_existing=True,
    )

    # 04:00 — очистка старых inbox-писем
    scheduler.add_job(
        func=cleanup_inbox_emails_task,
        trigger="cron",
        args=[app],
        id="cleanup_inbox_emails",
        name="Cleanup old inbox emails",
        hour=4,
        minute=0,
        replace_existing=True,
    )

    # 23:00 — авто-отказ неподтверждённых позиций поставщиков
    scheduler.add_job(
        func=auto_refuse_supplier_items_task,
        trigger="cron",
        args=[app],
        id="auto_refuse_supplier_items",
        name="Auto-refuse unconfirmed supplier order items",
        hour=23,
        minute=0,
        replace_existing=True,
    )

    # ── Ночные задачи очистки накапливаемых таблиц ────────────────────────
    # 01:00 — история цен (autopartpricehistory): хранить 90 дней
    scheduler.add_job(
        func=cleanup_price_history_task,
        trigger="cron",
        args=[app],
        id="cleanup_price_history",
        name="Cleanup AutoPartPriceHistory older than 90 days",
        hour=1,
        minute=0,
        replace_existing=True,
    )

    # 20:00 — уведомления (app_notification): хранить 7 дней
    scheduler.add_job(
        func=cleanup_app_notifications_task,
        trigger="cron",
        args=[app],
        id="cleanup_app_notifications",
        name="Cleanup old AppNotifications",
        hour=20,
        minute=0,
        replace_existing=True,
    )

    # 02:30 — снимки метрик (systemmetricsnapshot): хранить 60 дней
    scheduler.add_job(
        func=cleanup_metric_snapshots_task,
        trigger="cron",
        args=[app],
        id="cleanup_metric_snapshots",
        name="Cleanup old SystemMetricSnapshots",
        hour=2,
        minute=30,
        replace_existing=True,
    )

    # 02:45 — лог проверки прайсов (pricechecklist) + сообщения поставщиков
    scheduler.add_job(
        func=cleanup_misc_logs_task,
        trigger="cron",
        args=[app],
        id="cleanup_misc_logs",
        name="Cleanup PriceCheckLog and old SupplierOrderMessages",
        hour=2,
        minute=45,
        replace_existing=True,
    )

    # 08:00 — перевод просроченных счетов в статус overdue
    scheduler.add_job(
        func=mark_overdue_invoices_task,
        trigger="cron",
        args=[app],
        id="mark_overdue_invoices",
        name="Mark overdue payment invoices",
        hour=8,
        minute=0,
        replace_existing=True,
    )

    scheduler.start()
    logger.info("Scheduler started.")
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
            if getattr(app.state, "is_shutting_down", False):
                logger.debug(
                    "Ignoring session close error during shutdown: %s", e
                )
            else:
                raise


async def _process_one(item, app: FastAPI, sem: asyncio.Semaphore):
    provider, filepath, provider_conf = item
    try:
        async with sem:
            try:
                async with tracked_execution(
                    app,
                    trace_type="provider_pricelist",
                    job_key="process_provider_pricelist",
                    job_name="Process provider pricelist",
                    provider_id=provider.id,
                    provider_config_id=provider_conf.id if provider_conf else None,
                    source_filename=os.path.basename(filepath),
                    details={
                        "provider_name": provider.name,
                        "provider_config_name": (
                            provider_conf.name_price if provider_conf else None
                        ),
                    },
                ) as trace:
                    async with new_session_from_app(app) as session:
                        file_extension = filepath.split(".")[-1].lower()
                        async with aiofiles.open(filepath, "rb") as f:
                            file_content = await f.read()
                        trace.details.update(
                            {
                                "file_extension": file_extension,
                                "file_size_bytes": len(file_content),
                            }
                        )
                        logger.info(
                            f"Скачан прайс для провайдера {provider.id} "
                            f"({provider.name}), размер: {len(file_content)} байт"
                        )
                        rss_before = _process_rss_mb()
                        if rss_before is not None:
                            logger.info(
                                "Provider pricelist processing start: "
                                "provider_id=%s config_id=%s rss_mb=%.1f",
                                provider.id,
                                provider_conf.id if provider_conf else None,
                                rss_before,
                            )
                        pricelist, stats = await process_provider_pricelist(
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
                            return_stats=True,
                            include_autoparts_response=False,
                        )
                        trace.details.update(
                            {
                                "pricelist_id": getattr(pricelist, "id", None),
                                "stats": stats,
                            }
                        )
                        logger.info(
                            f"Успешно обработан прайс для провайдера {provider.id}"
                        )
                        rss_after = _process_rss_mb()
                        if rss_after is not None:
                            logger.info(
                                "Provider pricelist processing end: "
                                "provider_id=%s config_id=%s rss_mb=%.1f",
                                provider.id,
                                provider_conf.id if provider_conf else None,
                                rss_after,
                            )

                        if ENABLE_LEGACY_ZZAP_AUTO_SEND and (
                            provider.id == 1
                            or provider.name == PROVIDER_IN["name"]
                        ):
                            logger.info(
                                "Auto-send CUSTOMER_IN branch is enabled for "
                                "provider_id=%s (%s); "
                                "running send_price_list_task.",
                                provider.id,
                                provider.name,
                            )
                            await send_price_list_task(app)
            except Exception as e:
                logger.error(
                    f"Ошибка обработки прайса для провайдера {provider.id}: "
                    f"{e}",
                    exc_info=True,
                )
                raise
    except asyncio.CancelledError:
        if getattr(app.state, "is_shutting_down", False):
            logger.info(
                "Отмена обработки прайса провайдера %s при остановке",
                provider.id,
            )
            return
        raise


async def send_price_list_task(app: FastAPI):
    # logger.info('Starting send_price_list_task')
    # async_session_factory = get_async_session()
    logger.info("Starting send_price_list_task")
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
                provider=PROVIDER_IN["name"], session=session
            )
            if not provider:
                logger.error(f"Provider {PROVIDER_IN['name']} not found.")
                raise ValueError(f"Provider {PROVIDER_IN['name']} not found.")
            pricelist_ids = await crud_pricelist.get_pricelist_ids_by_provider(
                provider_id=provider.id, session=session
            )
            if not pricelist_ids:
                logger.error(
                    f"No pricelists found for provider {provider.name}."
                )
                raise ValueError(
                    f"No pricelists found for provider {provider.name}."
                )
            logger.debug(f"Using pricelist_ids[-1]: {pricelist_ids[-1]}")
            # Создаем или получаем объект запроса
            request = CustomerPriceListCreate(
                customer_id=customer.id,
                config_id=config.id,
                items=[pricelist_ids[-1]],
            )

            await process_customer_pricelist(
                customer=customer,
                request=request,
                session=session,
                include_autoparts_response=False,
            )

            logger.info(
                f"Pricelist created and sent for customer {customer.name}"
            )
        except Exception as e:
            logger.error(
                f"Error process. pricelist for customer {customer.name}: {e}",
                exc_info=True,
            )


async def download_customer_orders_task(app: FastAPI):
    async with tracked_execution(
        app,
        trace_type="scheduler_job",
        job_key="download_customer_orders",
        job_name="Download customer orders task",
    ) as trace:
        logger.info("Starting download_customer_orders_task")
        rss_before = _process_rss_mb()
        if rss_before is not None:
            logger.info("download_customer_orders_task rss_before=%.1f", rss_before)
        async_session_factory = app.state.session_factory
        async with async_session_factory() as session:
            try:
                should_run, setting = await _should_run_scheduled_job(
                    session, "customer_orders_check"
                )
                if not should_run:
                    trace.details["skipped_by_scheduler_setting"] = True
                    return
                in_window = False
                try:
                    in_window = await is_in_any_order_window(session)
                    trace.details["in_expected_window"] = bool(in_window)
                except Exception as win_exc:
                    logger.warning("Could not compute order windows: %s", win_exc)
                    trace.details["window_check_error"] = str(win_exc)[:500]
                if not in_window:
                    last_run = setting.last_run_at if setting else None
                    if last_run is not None:
                        elapsed = (now_moscow() - last_run).total_seconds()
                        trace.details["elapsed_since_last_run_sec"] = round(
                            elapsed, 1
                        )
                        if elapsed < OUTSIDE_WINDOW_SLOW_SECONDS:
                            logger.debug(
                                "Outside order windows, slow mode: elapsed=%.0fs",
                                elapsed,
                            )
                            trace.details["skipped_by_slow_mode"] = True
                            return
                await process_customer_orders(session)
                trace.details["processed"] = True
                if setting:
                    await _mark_scheduler_ran(session, setting, now_moscow())
            except Exception as e:
                logger.error(
                    f"Error processing customer orders: {e}", exc_info=True
                )
                trace.details["error"] = str(e)[:2000]
                trace.details["__trace_status"] = "error"
                await _notify_scheduler_issue(
                    session,
                    subject="Ошибка регламента обработки заказов",
                    text=(
                        "Ошибка при автоматической обработке заказов клиентов.\n"
                        f"Текст ошибки: {e}"
                    ),
                )
            finally:
                trim_process_memory(
                    logger, context="download_customer_orders_task"
                )


async def process_supplier_responses_task(app: FastAPI):
    async with tracked_execution(
        app,
        trace_type="scheduler_job",
        job_key="process_supplier_responses",
        job_name="Process supplier responses task",
    ) as trace:
        logger.info("Starting process_supplier_responses_task")
        async_session_factory = app.state.session_factory
        async with async_session_factory() as session:
            try:
                should_run, setting = await _should_run_scheduled_job(
                    session, "supplier_responses_check"
                )
                if not should_run:
                    trace.details["skipped_by_scheduler_setting"] = True
                    return
                active_providers: list[int] = []
                try:
                    active_providers = (
                        await get_active_supplier_response_provider_ids(session)
                    )
                    trace.details["active_provider_ids_count"] = len(
                        active_providers
                    )
                except Exception as win_exc:
                    logger.warning(
                        "Could not check supplier response window: %s", win_exc
                    )
                    trace.details["window_check_error"] = str(win_exc)[:500]
                if not active_providers:
                    last_run = setting.last_run_at if setting else None
                    if last_run is not None:
                        elapsed = (now_moscow() - last_run).total_seconds()
                        trace.details["elapsed_since_last_run_sec"] = round(
                            elapsed, 1
                        )
                        if elapsed < OUTSIDE_WINDOW_SLOW_SECONDS:
                            logger.debug(
                                "No suppliers in response window,"
                                "slow mode: elapsed=%.0fs",
                                elapsed,
                            )
                            trace.details["skipped_by_slow_mode"] = True
                            return
                summary = await process_supplier_response_messages(
                    session, file_payload_mode="responses"
                )
                trace.details["summary"] = summary
                logger.info(
                    "Completed process_supplier_responses_task summary=%s",
                    summary,
                )
                try:
                    closed_count, stale_days = (
                        await _close_stale_supplier_response_messages(session)
                    )
                    trace.details["stale_closed_count"] = closed_count
                    trace.details["stale_days"] = stale_days
                    if closed_count > 0:
                        logger.info(
                            (
                                "Supplier response stale cleanup completed: "
                                "closed=%s stale_days=%s"
                            ),
                            closed_count,
                            stale_days,
                        )
                except Exception as stale_exc:
                    logger.warning(
                        "Supplier response stale cleanup failed: %s",
                        stale_exc,
                        exc_info=True,
                    )
                    trace.details["stale_cleanup_error"] = str(stale_exc)[:500]
                if setting:
                    await _mark_scheduler_ran(session, setting, now_moscow())
            except Exception as e:
                logger.error(
                    "Error processing supplier responses: %s",
                    e,
                    exc_info=True,
                )
                trace.details["error"] = str(e)[:2000]
                trace.details["__trace_status"] = "error"
                await _notify_scheduler_issue(
                    session,
                    subject="Ошибка регламента обработки ответов поставщиков",
                    text=(
                        "Ошибка при автоматической обработке ответов "
                        f"поставщиков.\nТекст ошибки: {e}"
                    ),
                )


async def process_supplier_documents_task(app: FastAPI):
    async with tracked_execution(
        app,
        trace_type="scheduler_job",
        job_key="process_supplier_documents",
        job_name="Process supplier documents task",
    ) as trace:
        logger.info("Starting process_supplier_documents_task")
        async_session_factory = app.state.session_factory
        async with async_session_factory() as session:
            try:
                summary = await process_supplier_response_messages(
                    session, file_payload_mode="documents"
                )
                trace.details["summary"] = summary
                logger.info(
                    "Completed process_supplier_documents_task summary=%s",
                    summary,
                )
            except Exception as e:
                logger.error(
                    "Error processing supplier documents: %s", e, exc_info=True
                )
                trace.details["error"] = str(e)[:2000]
                trace.details["__trace_status"] = "error"
                await _notify_scheduler_issue(
                    session,
                    subject="Ошибка обработки документов поставщиков",
                    text=(
                        "Ошибка при автоматической обработке документов "
                        f"(УПД/накладных).\nТекст ошибки: {e}"
                    ),
                )


async def sync_diadoc_inbound_task(app: FastAPI):
    async with tracked_execution(
        app,
        trace_type="scheduler_job",
        job_key="sync_diadoc_inbound",
        job_name="Sync Diadoc inbound task",
    ) as trace:
        logger.info("Starting sync_diadoc_inbound_task")
        async_session_factory = app.state.session_factory
        async with async_session_factory() as session:
            try:
                should_run, setting = await _should_run_scheduled_job(
                    session, "diadoc_inbound_sync"
                )
                if not should_run:
                    trace.details["skipped_by_scheduler_setting"] = True
                    return
                integration = await crud_diadoc_integration_settings.get_or_create(
                    session
                )
                if not integration.refresh_token or not integration.box_id_guid:
                    logger.debug(
                        "Diadoc inbound sync skipped: not connected or box not selected"
                    )
                    trace.details["skipped_not_connected"] = True
                    return
                if not bool(integration.inbound_sync_enabled):
                    logger.debug(
                        "Diadoc inbound sync skipped: disabled in settings"
                    )
                    trace.details["skipped_disabled"] = True
                    return
                integration, client = await get_diadoc_client_for_session(session)
                result = await sync_diadoc_incoming_documents(
                    session=session,
                    client=client,
                    environment=str(integration.environment or "staging"),
                    box_id_guid=str(integration.box_id_guid),
                    filter_category="Any.Inbound",
                    count=max(
                        1, min(int(integration.inbound_sync_count or 50), 200)
                    ),
                    download_content=bool(integration.inbound_download_content),
                    register_supplier_message=bool(
                        integration.inbound_process_enabled
                    ),
                    process_supplier_message=bool(
                        integration.inbound_process_enabled
                    ),
                )
                trace.details["summary"] = result
                integration.last_sync_at = now_moscow()
                integration.last_error = None
                session.add(integration)
                await session.commit()
                logger.info(
                    "Completed sync_diadoc_inbound_task summary=%s",
                    result,
                )
                if setting:
                    await _mark_scheduler_ran(session, setting, now_moscow())
            except Exception as e:
                logger.error(
                    "Error syncing Diadoc inbound documents: %s",
                    e,
                    exc_info=True,
                )
                trace.details["error"] = str(e)[:2000]
                trace.details["__trace_status"] = "error"
                try:
                    integration = (
                        await crud_diadoc_integration_settings.get_or_create(
                            session
                        )
                    )
                    integration.last_error = str(e)[:2000]
                    session.add(integration)
                    await session.commit()
                except Exception:
                    await session.rollback()
                await _notify_scheduler_issue(
                    session,
                    subject="Ошибка синхронизации входящих Диадок",
                    text=(
                        "Ошибка при автоматической синхронизации "
                        f"входящих документов Диадок.\nТекст ошибки: {e}"
                    ),
                )


async def check_order_timing_alerts_task(app: FastAPI):
    """
    Checks for:
    1. Customers whose expected order
    window has passed without an order arriving.
    2. Supplier orders that sent 2+ hours ago with no response received.
    Sends AppNotification + Telegram for
    each issue (deduplicated by title per day).
    """
    logger.info("Starting check_order_timing_alerts_task")
    async_session_factory = app.state.session_factory
    async with async_session_factory() as session:
        try:
            from sqlalchemy import func as _func

            from dz_fastapi.core.time import now_moscow as _now
            from dz_fastapi.models.notification import AppNotification

            today_date = _now().date()

            async def _already_notified(title: str) -> bool:
                """Check if we already sent this notification today."""
                stmt = (
                    select(AppNotification.id)
                    .where(
                        AppNotification.title == title,
                        _func.date(AppNotification.created_at) == today_date,
                    )
                    .limit(1)
                )
                return (
                    await session.execute(stmt)
                ).scalar_one_or_none() is not None

            # --- Missing customer orders ---
            missing_orders = await get_overdue_customer_windows(session)
            for alert in missing_orders:
                title = f"Заказ не получен: {alert.customer_name}"
                if await _already_notified(title):
                    continue
                received = alert.received_count
                expected = alert.expected_count
                if received > 0:
                    orders_txt = f"получено {received} из {expected} заказов"
                else:
                    orders_txt = "заказы не получены"
                msg = (
                    f"Клиент «{alert.customer_name}» — {orders_txt}. "
                    f'Окно: {alert.expected_start.strftime("%H:%M")}–'
                    f'{alert.expected_end.strftime("%H:%M")}.'
                )
                await notify_admin_all(
                    session,
                    title=title,
                    message=msg,
                    level="warning",
                    link="/orders",
                    commit=False,
                )
            await session.commit()

            # --- Missing supplier responses ---
            missing_responses = await get_overdue_supplier_responses(session)
            for alert in missing_responses:
                title = f"Нет ответа от поставщика: {alert.provider_name}"
                if await _already_notified(title):
                    continue
                msg = (
                    f"Заказ #{alert.supplier_order_id} поставщику "
                    f"«{alert.provider_name}» отправлен в "
                    f'{alert.sent_at.strftime("%H:%M")}, '
                    f"но ответ не получен до "
                    f'{alert.window_ended_at.strftime("%H:%M")}.'
                )
                await notify_admin_all(
                    session,
                    title=title,
                    message=msg,
                    level="warning",
                    link="/supplier-orders",
                    commit=False,
                )
            await session.commit()

        except Exception as e:
            logger.error(
                "Error in check_order_timing_alerts_task: %s", e, exc_info=True
            )


async def send_scheduled_supplier_orders_task(app: FastAPI):
    logger.info("Starting send_scheduled_supplier_orders_task")
    async_session_factory = app.state.session_factory
    async with async_session_factory() as session:
        try:
            should_run, setting = await _should_run_scheduled_job(
                session, "supplier_orders_send"
            )
            if not should_run:
                return
            summary = await send_scheduled_supplier_orders(
                session,
                use_provider_schedule=False,
            )
            logger.info(
                "Completed send_scheduled_supplier_orders_task summary=%s",
                summary,
            )
            if setting:
                await _mark_scheduler_ran(session, setting, now_moscow())
        except Exception as e:
            logger.error(
                f"Error sending scheduled supplier orders: {e}",
                exc_info=True,
            )
            await _notify_scheduler_issue(
                session,
                subject="Ошибка регламента отправки заказов поставщикам",
                text=(
                    "Ошибка при автоматической отправке заказов "
                    f"поставщикам.\nТекст ошибки: {e}"
                ),
            )


async def cleanup_order_reports_task(app: FastAPI):
    async_session_factory = app.state.session_factory
    async with async_session_factory() as session:
        try:
            inbox_settings = (
                await crud_customer_order_inbox_settings.get_or_create(session)
            )
            reports_removed = await asyncio.to_thread(cleanup_order_reports)
            error_days = max(
                1, int(inbox_settings.error_file_retention_days or 5)
            )
            error_removed = await asyncio.to_thread(
                cleanup_order_error_files, error_days
            )
            logger.info(
                "Cleanup order reports removed %s reports and %s error files",
                reports_removed,
                error_removed,
            )
        except Exception as e:
            logger.error(
                "Error in cleanup_order_reports_task: %s",
                e,
                exc_info=True,
            )
            await _notify_scheduler_issue(
                session,
                subject="Ошибка регламента очистки отчетов заказов",
                text=(
                    "Ошибка при автоматической очистке отчетов по заказам.\n"
                    f"Текст ошибки: {e}"
                ),
            )


async def cleanup_tracking_orders_task(app: FastAPI):
    async_session_factory = app.state.session_factory
    async with async_session_factory() as session:
        try:
            summary = await cleanup_old_tracking_history(session=session)
            logger.info("Cleanup tracking orders summary: %s", summary)
        except Exception as e:
            logger.error(
                "Error in cleanup_tracking_orders_task: %s",
                e,
                exc_info=True,
            )
            await _notify_scheduler_issue(
                session,
                subject="Ошибка очистки истории наших заказов",
                text=(
                    "Ошибка при автоматической очистке истории заказов"
                    " из поиска по артикулу.\n"
                    f"Текст ошибки: {e}"
                ),
            )


async def sync_site_tracking_statuses_task(app: FastAPI):
    async with tracked_execution(
        app,
        trace_type="scheduler_job",
        job_key="sync_site_tracking_statuses",
        job_name="Sync Dragonzap tracking statuses task",
    ) as trace:
        async_session_factory = app.state.session_factory
        async with async_session_factory() as session:
            try:
                summary = await sync_site_tracking_statuses(session=session)
                trace.details["summary"] = summary
                logger.info(
                    "Dragonzap tracking statuses sync summary: %s",
                    summary,
                )
            except Exception as e:
                logger.error(
                    "Error in sync_site_tracking_statuses_task: %s",
                    e,
                    exc_info=True,
                )
                trace.details["error"] = str(e)[:2000]
                trace.details["__trace_status"] = "error"
                await _notify_scheduler_issue(
                    session,
                    subject="Ошибка синхронизации статусов заказов с сайта",
                    text=(
                        "Ошибка при автоматической синхронизации статусов "
                        "заказов Dragonzap.\n"
                        f"Текст ошибки: {e}"
                    ),
                )


def _day_key(now: datetime) -> str:
    mapping = {
        0: "mon",
        1: "tue",
        2: "wed",
        3: "thu",
        4: "fri",
        5: "sat",
        6: "sun",
    }
    return mapping[now.weekday()]


async def _should_run_scheduled_job(
    session: AsyncSession,
    key: str,
    allow_missed_for: timedelta | None = None,
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
    days = setting.days or defaults.get("days", [])
    times = setting.times or defaults.get("times", [])
    if key == "supplier_orders_send" and not times:
        return False, setting
    day_key = _day_key(now)
    time_key = now.strftime("%H:%M")
    if days and day_key not in days:
        return False, setting
    if allow_missed_for is None:
        catch_up_minutes = SCHEDULER_CATCH_UP_MINUTES.get(key, 0)
        if catch_up_minutes > 0:
            allow_missed_for = timedelta(minutes=catch_up_minutes)
    now_minute = now.replace(second=0, microsecond=0)
    if allow_missed_for and times:
        scheduled_datetimes = []
        for raw_time in times:
            try:
                hour_str, minute_str = str(raw_time).split(":", 1)
                scheduled_at = now_minute.replace(
                    hour=int(hour_str),
                    minute=int(minute_str),
                )
            except (TypeError, ValueError):
                logger.warning(
                    "Invalid scheduler time %r for key=%s",
                    raw_time,
                    key,
                )
                continue
            scheduled_datetimes.append(scheduled_at)
        for scheduled_at in sorted(scheduled_datetimes, reverse=True):
            delay = now_minute - scheduled_at
            if delay < timedelta(0):
                continue
            if delay > allow_missed_for:
                continue
            if setting.last_run_at and setting.last_run_at >= scheduled_at:
                continue
            if delay > timedelta(0):
                logger.warning(
                    "Running scheduler job %s in catch-up mode: "
                    "scheduled_for=%s now=%s delay_minutes=%s",
                    key,
                    scheduled_at.isoformat(),
                    now_minute.isoformat(),
                    int(delay.total_seconds() // 60),
                )
            return True, setting
    if times and time_key not in times:
        return False, setting
    if setting.last_run_at:
        last_key = setting.last_run_at.strftime("%Y-%m-%d %H:%M")
        now_key = now.strftime("%Y-%m-%d %H:%M")
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

    Каждый конфиг обрабатывается в отдельной сессии, чтобы не держать
    соединение к БД открытым на весь цикл (что блокировало пул при
    параллельных HTTP-запросах).
    """
    async with tracked_execution(
        app,
        trace_type="scheduler_job",
        job_key="send_scheduled_customer_pricelists",
        job_name="Send scheduled customer pricelists task",
    ) as trace:
        async_session_factory = app.state.session_factory
        now = now_moscow()
        day_key = _day_key(now)
        time_key = now.strftime("%H:%M")

        try:
            async with async_session_factory() as session:
                stmt = (
                    select(CustomerPriceListConfig)
                    .options(selectinload(CustomerPriceListConfig.customer))
                    .where(CustomerPriceListConfig.is_active.is_(True))
                )
                configs = (await session.execute(stmt)).scalars().all()
                pending = []
                for config in configs:
                    if not config.schedule_days or not config.schedule_times:
                        continue
                    if day_key not in (config.schedule_days or []):
                        continue
                    if time_key not in (config.schedule_times or []):
                        continue
                    if config.last_sent_at:
                        last_key = config.last_sent_at.strftime("%Y-%m-%d %H:%M")
                        now_key = now.strftime("%Y-%m-%d %H:%M")
                        if last_key == now_key:
                            continue
                    if not config.customer:
                        continue
                    pending.append((config.id, config.customer))
                trace.details["pending_configs"] = len(pending)
        except Exception as e:
            logger.error(
                "Error loading configs in "
                "send_scheduled_customer_pricelists_task: %s",
                e,
                exc_info=True,
            )
            trace.details["error"] = str(e)[:2000]
            trace.details["__trace_status"] = "error"
            async with async_session_factory() as err_session:
                await _notify_scheduler_issue(
                    err_session,
                    subject="Ошибка регламента отправки прайсов клиентам",
                    text=(
                        "Ошибка при загрузке конфигов авторассылки прайсов.\n"
                        f"Текст ошибки: {e}"
                    ),
                )
            return

        success_count = 0
        error_count = 0
        for config_id, customer in pending:
            request = CustomerPriceListCreate(
                customer_id=customer.id,
                config_id=config_id,
                items=[],
            )
            try:
                async with async_session_factory() as session:
                    await process_customer_pricelist(
                        customer=customer,
                        request=request,
                        session=session,
                        include_autoparts_response=False,
                    )
                success_count += 1
            except Exception as exc:
                error_count += 1
                logger.error(
                    "Error in send_scheduled_customer_pricelists_task "
                    "for config %s: %s",
                    config_id,
                    exc,
                    exc_info=True,
                )
                try:
                    async with async_session_factory() as err_session:
                        await _notify_scheduler_issue(
                            err_session,
                            subject="Ошибка регламента отправки прайсов клиентам",
                            text=(
                                "Ошибка при автоматической отправке прайса "
                                f"клиенту для config_id={config_id}.\n"
                                f"Текст ошибки: {exc}"
                            ),
                        )
                except Exception as notify_exc:
                    logger.error(
                        "Failed to send error notification for config %s: %s",
                        config_id,
                        notify_exc,
                    )
        trace.details["success_count"] = success_count
        trace.details["error_count"] = error_count
        if error_count > 0:
            trace.details["__trace_status"] = "error"


async def price_control_run_task(app: FastAPI):
    async with tracked_execution(
        app,
        trace_type="scheduler_job",
        job_key="price_control_run",
        job_name="Price control run task",
    ) as trace:
        async_session_factory = app.state.session_factory
        now = now_moscow()
        day_key = _day_key(now)
        time_key = now.strftime("%H:%M")
        async with async_session_factory() as session:
            try:
                stmt = select(PriceControlConfig).where(
                    PriceControlConfig.is_active.is_(True)
                )
                configs = (await session.execute(stmt)).scalars().all()
                if not configs:
                    trace.details["active_configs"] = 0
                    return
                trace.details["active_configs"] = len(configs)
                run_count = 0
                error_count = 0
                for config in configs:
                    schedule_days = config.schedule_days or []
                    schedule_times = config.schedule_times or ["09:00"]
                    if schedule_days and day_key not in schedule_days:
                        continue
                    if schedule_times and time_key not in schedule_times:
                        continue
                    if config.last_run_at:
                        last_key = config.last_run_at.strftime("%Y-%m-%d %H:%M")
                        now_key = now.strftime("%Y-%m-%d %H:%M")
                        if last_key == now_key:
                            continue
                    try:
                        await run_price_control(session, config)
                        run_count += 1
                    except Exception as exc:
                        error_count += 1
                        logger.error(
                            "Error running price control for config %s: %s",
                            config.id,
                            exc,
                            exc_info=True,
                        )
                        await _notify_scheduler_issue(
                            session,
                            subject="Ошибка регламента контроля цен",
                            text=(
                                "Ошибка при автоматическом контроле цен "
                                f"для config_id={config.id}.\n"
                                f"Текст ошибки: {exc}"
                            ),
                        )
                trace.details["run_count"] = run_count
                trace.details["error_count"] = error_count
                if error_count > 0:
                    trace.details["__trace_status"] = "error"
                logger.info("Completed price_control_run_task")
            except Exception as exc:
                logger.error(
                    f"Error in price_control_run_task: {exc}",
                    exc_info=True,
                )
                trace.details["error"] = str(exc)[:2000]
                trace.details["__trace_status"] = "error"
                await _notify_scheduler_issue(
                    session,
                    subject="Ошибка регламента контроля цен",
                    text=(
                        "Ошибка при автоматическом запуске контроля цен.\n"
                        f"Текст ошибки: {exc}"
                    ),
                )


async def process_new_provider_emails(session: AsyncSession, app: FastAPI):
    """
    Обрабатывает все новые письма за сегодня,
    скачивая файлы для провайдеров и далее
    запускает функцию обработки прайслеста для каждого провайдера.
    """
    logger.info("Начинаем обработку писем провайдеров...")
    start_time = time.perf_counter()
    downloaded = await get_emails(session=session)

    email_time = time.perf_counter()
    logger.info(
        f"get_emails() выполнена за {email_time - start_time:.2f} секунд"
    )

    if not downloaded:
        logger.info("Новых писем для обработки не найдено.")
        return {
            "downloaded": 0,
            "successful": 0,
            "errors": 0,
            "processing_seconds": 0.0,
            "total_seconds": time.perf_counter() - start_time,
        }
    rss_before = _process_rss_mb()
    logger.info(
        "Starting provider pricelist processing: files=%s parallelism=%s rss_mb=%s",
        len(downloaded),
        PRICE_PROVIDER_PROCESS_PARALLELISM,
        f"{rss_before:.1f}" if rss_before is not None else "n/a",
    )
    sem = asyncio.Semaphore(PRICE_PROVIDER_PROCESS_PARALLELISM)
    process_start = time.perf_counter()
    tasks = [
        asyncio.create_task(_process_one(item, app, sem))
        for item in downloaded
    ]
    try:
        results = await asyncio.gather(*tasks, return_exceptions=True)
    except asyncio.CancelledError:
        if getattr(app.state, "is_shutting_down", False):
            logger.info("Отмена обработки писем провайдеров при остановке")
            return
        raise
    process_end = time.perf_counter()
    logger.info(
        f"Обработка прайса выполнена "
        f"за {process_end - process_start:.2f} секунд"
    )
    successful = 0
    errors = 0
    for result in results:
        if isinstance(result, Exception):
            errors += 1
            logger.error(
                f"Ошибка обработки прайс-листа: {result}", exc_info=True
            )
        else:
            successful += 1

    total_time = time.perf_counter() - start_time
    logger.info(
        f"process_new_provider_emails завершена за {total_time:.2f} секунд. "
        f"Успешно: {successful}, Ошибок: {errors}"
    )
    rss_after = _process_rss_mb()
    if rss_after is not None:
        logger.info(
            "Finished provider pricelist processing: rss_mb=%.1f",
            rss_after,
        )
    return {
        "downloaded": len(downloaded),
        "successful": successful,
        "errors": errors,
        "processing_seconds": process_end - process_start,
        "total_seconds": total_time,
    }


def _is_price_check_due(schedule) -> bool:
    if not schedule.enabled:
        return False
    if not schedule.days or not schedule.times:
        return True
    now = now_moscow()
    day_key = now.strftime("%a").lower()[:3]
    time_key = now.strftime("%H:%M")
    return day_key in (schedule.days or []) and time_key in (
        schedule.times or []
    )


async def download_price_provider_task(app: FastAPI):
    async with tracked_execution(
        app,
        trace_type="scheduler_job",
        job_key="download_price_provider",
        job_name="Download price provider task",
    ) as trace:
        logger.info("Starting download_price_provider_task")
        rss_before = _process_rss_mb()
        if rss_before is not None:
            logger.info("download_price_provider_task rss_before=%.1f", rss_before)
        async_session_factory = app.state.session_factory
        async with async_session_factory() as session:
            try:
                schedule = await crud_price_check_schedule.get_or_create(session)
                if not _is_price_check_due(schedule):
                    logger.info("Price check skipped by schedule")
                    trace.details["skipped_by_schedule"] = True
                    await crud_price_check_log.create(
                        session=session,
                        status="SKIP",
                        message="Skipped by schedule",
                    )
                    return
                # Проверяю наличие поставщика
                provider = await crud_provider.get_provider_or_none(
                    provider=PROVIDER_IN["name"], session=session
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
                    logger.info(f"Created initial provider with id: {provider.id}")
                trace.details["provider_id"] = getattr(provider, "id", None)
                trace.details["provider_name"] = getattr(provider, "name", None)
                email_summary = await process_new_provider_emails(session, app)
                trace.details["email_processing_summary"] = email_summary
                if int(email_summary.get("errors") or 0) > 0:
                    trace.details["__trace_status"] = "error"
                schedule.last_checked_at = now_moscow()
                session.add(schedule)
                await session.commit()
                await crud_price_check_log.create(
                    session=session,
                    status="OK",
                    message="Price check completed",
                )
                logger.info("Completed download_price_provider_task")
                rss_after = _process_rss_mb()
                if rss_after is not None:
                    logger.info(
                        "download_price_provider_task rss_after=%.1f",
                        rss_after,
                    )
            except asyncio.CancelledError:
                if getattr(app.state, "is_shutting_down", False):
                    logger.info(
                        "download_price_provider_task отменена при остановке"
                    )
                    trace.details["cancelled_on_shutdown"] = True
                    return
                raise
            except Exception as e:
                logger.error(f"Error in download_price_provider_task: {e}")
                trace.details["error"] = str(e)[:2000]
                trace.details["__trace_status"] = "error"
                await _notify_scheduler_issue(
                    session,
                    subject="Ошибка регламента загрузки прайсов",
                    text=(
                        "Ошибка при автоматической загрузке прайсов "
                        "поставщиков.\n"
                        f"Текст ошибки: {e}"
                    ),
                )
                try:
                    await crud_price_check_log.create(
                        session=session,
                        status="ERROR",
                        message=str(e)[:240],
                    )
                except Exception:
                    pass
                raise
            finally:
                trim_process_memory(
                    logger, context="download_price_provider_task"
                )


async def sync_auto_oem_crosses_task(app: FastAPI):
    logger.info("Starting sync_auto_oem_crosses_task")
    async_session_factory = app.state.session_factory
    async with async_session_factory() as session:
        try:
            result = await sync_automatic_oem_crosses(session)
            await session.commit()
            logger.info(
                "Completed sync_auto_oem_crosses_task: groups_checked=%s rows_created=%s",
                result.get("groups_checked", 0),
                result.get("rows_created", 0),
            )
        except asyncio.CancelledError:
            if getattr(app.state, "is_shutting_down", False):
                logger.info(
                    "sync_auto_oem_crosses_task отменена при остановке"
                )
                return
            raise
        except Exception as e:
            await session.rollback()
            logger.error("Error in sync_auto_oem_crosses_task: %s", e)
            await _notify_scheduler_issue(
                session,
                subject="Ошибка автосоздания кроссов",
                text=(
                    "Ошибка при ночной синхронизации автокроссов "
                    "Dragonzap/original.\n"
                    f"Текст ошибки: {e}"
                ),
            )


async def cleanup_old_pricelists_task(app: FastAPI):
    async_session_factory = app.state.session_factory
    async with async_session_factory() as session:
        try:
            should_run, setting = await _should_run_scheduled_job(
                session, "cleanup_old_pricelists"
            )
            if not should_run:
                return
            logger.info("Starting cleanup_old_pricelists_task")
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
                f"Cleanup finished. "
                f"Deleted provider pricelists: {total_deleted}; "
                f"deleted customer pricelists: {total_deleted_customer}"
            )
            if setting:
                await _mark_scheduler_ran(session, setting, now_moscow())
        except Exception as e:
            logger.error(
                f"Error in cleanup_old_pricelists_task: {e}", exc_info=True
            )
            await _notify_scheduler_issue(
                session,
                subject="Ошибка регламента очистки прайсов",
                text=(
                    "Ошибка при автоматической очистке старых прайсов.\n"
                    f"Текст ошибки: {e}"
                ),
            )
            await session.rollback()


async def check_provider_pricelist_staleness_task(app: FastAPI):
    logger.info("Starting check_provider_pricelist_staleness_task")
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
            logger.info("Completed check_provider_pricelist_staleness_task")
        except Exception as e:
            logger.error(
                f"Error in check_provider_pricelist_staleness_task: {e}",
                exc_info=True,
            )
            await _notify_scheduler_issue(
                session,
                subject="Ошибка регламента проверки устаревших прайсов",
                text=(
                    "Ошибка при автоматической проверке давности прайсов.\n"
                    f"Текст ошибки: {e}"
                ),
            )
            await session.rollback()


async def notify_pricelist_stale_task(app: FastAPI):
    async_session_factory = app.state.session_factory
    async with async_session_factory() as session:
        try:
            should_run, setting = await _should_run_scheduled_job(
                session, "pricelist_stale_notify"
            )
            if not should_run:
                return
            logger.info("Starting notify_pricelist_stale_task")
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
                logger.info("No stale pricelist alerts for today")
                return

            seen_keys = set()
            unique_rows = []
            for alert, config, provider in rows:
                key = (alert.provider_id, alert.provider_config_id)
                if key in seen_keys:
                    continue
                seen_keys.add(key)
                unique_rows.append((alert, config, provider))

            lines = ["Проблемы с обновлением прайсов:"]
            for alert, config, provider in unique_rows:
                config_label = config.name_price or f"#{config.id}"
                lines.append(
                    f"- {provider.name} ({config_label}) — "
                    f"{alert.days_diff} дн. Последний: {alert.last_price_date}"
                )
            await create_admin_notifications(
                session=session,
                title="Проблемы с обновлением прайсов",
                message="\n".join(lines),
                level="warning",
                link="/admin/settings",
                commit=False,
            )
            await session.commit()
            logger.info("Sent stale pricelist notification to admins")
            if setting:
                await _mark_scheduler_ran(session, setting, now)
        except Exception as e:
            logger.error(
                f"Error in notify_pricelist_stale_task: {e}", exc_info=True
            )
            await _notify_scheduler_issue(
                session,
                subject="Ошибка регламента уведомлений об устаревших прайсах",
                text=(
                    "Ошибка при автоматической отправке уведомлений "
                    f"об устаревших прайсах.\nТекст ошибки: {e}"
                ),
            )


async def cleanup_pricelist_stale_alerts_task(app: FastAPI):
    async_session_factory = app.state.session_factory
    async with async_session_factory() as session:
        try:
            should_run, setting = await _should_run_scheduled_job(
                session, "pricelist_stale_cleanup"
            )
            if not should_run:
                return
            logger.info("Starting cleanup_pricelist_stale_alerts_task")
            now = now_moscow()
            cutoff = now - timedelta(days=PRICELIST_STALE_ALERT_RETENTION_DAYS)
            stmt = delete(PriceListStaleAlert).where(
                PriceListStaleAlert.created_at < cutoff
            )
            result = await session.execute(stmt)
            await session.commit()
            logger.info(
                "Cleanup stale pricelist alerts removed %s rows",
                result.rowcount or 0,
            )
            if setting:
                await _mark_scheduler_ran(session, setting, now)
        except Exception as e:
            logger.error(
                f"Error in cleanup_pricelist_stale_alerts_task: {e}",
                exc_info=True,
            )
            await _notify_scheduler_issue(
                session,
                subject="Ошибка регламента очистки алертов по прайсам",
                text=(
                    "Ошибка при автоматической очистке алертов "
                    f"по прайсам.\nТекст ошибки: {e}"
                ),
            )
            await session.rollback()


async def collect_system_metrics_snapshot_task(app: FastAPI):
    async_session_factory = app.state.session_factory
    async with async_session_factory() as session:
        try:
            should_run, setting = await _should_run_scheduled_job(
                session, "metrics_snapshot"
            )
            if not should_run:
                return
            logger.info("Starting collect_system_metrics_snapshot_task")
            summary = await get_monitor_summary(session=session, app=app)
            payload = build_snapshot_payload(summary)
            await crud_system_metric_snapshot.create(
                session=session, payload=payload
            )
            if setting:
                await _mark_scheduler_ran(session, setting, now_moscow())
            logger.info("Completed collect_system_metrics_snapshot_task")
        except Exception as e:
            logger.error(
                f"Error in collect_system_metrics_snapshot_task: {e}",
                exc_info=True,
            )
            await _notify_scheduler_issue(
                session,
                subject="Ошибка регламента сбора системных метрик",
                text=(
                    "Ошибка при автоматическом сборе системных метрик.\n"
                    f"Текст ошибки: {e}"
                ),
            )


async def check_watchlist_site_task(app: FastAPI):
    async_session_factory = app.state.session_factory
    async with async_session_factory() as session:
        try:
            should_run, setting = await _should_run_scheduled_job(
                session, "watchlist_site_check"
            )
            if not should_run:
                return
            logger.info("Starting check_watchlist_site_task")
            await check_watchlist_site(session)
            logger.info("Completed check_watchlist_site_task")
            if setting:
                await _mark_scheduler_ran(session, setting, now_moscow())
        except Exception as e:
            logger.error(f"Error in check_watchlist_site_task: {e}")
            await _notify_scheduler_issue(
                session,
                subject="Ошибка регламента проверки watchlist сайта",
                text=(
                    "Ошибка при автоматической проверке watchlist сайта.\n"
                    f"Текст ошибки: {e}"
                ),
            )


async def notify_watchlist_task(app: FastAPI):
    async_session_factory = app.state.session_factory
    async with async_session_factory() as session:
        try:
            should_run, setting = await _should_run_scheduled_job(
                session, "watchlist_notify"
            )
            if not should_run:
                return
            logger.info("Starting notify_watchlist_task")
            await send_watchlist_daily_notifications(session)
            logger.info("Completed notify_watchlist_task")
            if setting:
                await _mark_scheduler_ran(session, setting, now_moscow())
        except Exception as e:
            logger.error(f"Error in notify_watchlist_task: {e}")
            await _notify_scheduler_issue(
                session,
                subject="Ошибка регламента уведомлений watchlist",
                text=(
                    "Ошибка при автоматической отправке уведомлений "
                    f"watchlist.\nТекст ошибки: {e}"
                ),
            )


async def fetch_inbox_emails_task(app: FastAPI):
    """Автоматически забирает письма со всех активных IMAP-ящиков."""
    async with tracked_execution(
        app,
        trace_type="scheduler_job",
        job_key="fetch_inbox_emails",
        job_name="Fetch inbox emails task",
    ) as trace:
        async_session_factory = app.state.session_factory
        async with async_session_factory() as session:
            try:
                logger.info("Starting fetch_inbox_emails_task")
                result = await fetch_and_store_emails(session, days=3)
                trace.details.update(
                    {
                        "fetched": getattr(result, "fetched", None),
                        "stored": getattr(result, "stored", None),
                        "auto_processed": getattr(result, "auto_processed", None),
                    }
                )
                logger.info(
                    "fetch_inbox_emails_task done: "
                    "fetched=%s stored=%s auto_processed=%s",
                    result.fetched,
                    result.stored,
                    result.auto_processed,
                )
            except Exception as e:
                logger.error("Error in fetch_inbox_emails_task: %s", e)
                trace.details["error"] = str(e)[:2000]
                trace.details["__trace_status"] = "error"
            finally:
                trim_process_memory(
                    logger, context="fetch_inbox_emails_task"
                )


async def cleanup_inbox_emails_task(app: FastAPI):
    """Удаляет письма из InboxEmail старше 7 дней."""
    async_session_factory = app.state.session_factory
    async with async_session_factory() as session:
        try:
            deleted = await cleanup_inbox_emails(session, max_days=7)
            logger.info(
                "cleanup_inbox_emails_task: deleted %s old emails", deleted
            )
        except Exception as e:
            logger.error("Error in cleanup_inbox_emails_task: %s", e)


async def auto_refuse_supplier_items_task(app: FastAPI):
    """Помечает позиции заказов поставщику как авто-отказ если
    прошёл рабочий день без подтверждения или поступления товара."""
    async_session_factory = app.state.session_factory
    async with async_session_factory() as session:
        try:
            marked = await mark_auto_refused_supplier_items(session)
            if marked:
                logger.info(
                    "auto_refuse_supplier_items_task: marked %s items "
                    "as auto-refused",
                    marked,
                )
            else:
                logger.debug(
                    "auto_refuse_supplier_items_task: nothing to mark"
                )
        except Exception as e:
            logger.error("Error in auto_refuse_supplier_items_task: %s", e)


# ── Cleanup накапливаемых таблиц ─────────────────────────────────────────────

PRICE_HISTORY_RETENTION_DAYS = int(
    os.getenv("PRICE_HISTORY_RETENTION_DAYS", "365")
)
APP_NOTIFICATION_RETENTION_DAYS = int(
    os.getenv("APP_NOTIFICATION_RETENTION_DAYS", "7")
)
METRIC_SNAPSHOT_RETENTION_DAYS = int(
    os.getenv("METRIC_SNAPSHOT_RETENTION_DAYS", "60")
)
PRICE_CHECK_LOG_RETENTION_DAYS = int(
    os.getenv("PRICE_CHECK_LOG_RETENTION_DAYS", "30")
)
SUPPLIER_MSG_RETENTION_DAYS = int(
    os.getenv("SUPPLIER_MSG_RETENTION_DAYS", "7")
)
EXECUTION_TRACE_RETENTION_DAYS = int(
    os.getenv("EXECUTION_TRACE_RETENTION_DAYS", "3")
)


async def cleanup_price_history_task(app: FastAPI):
    """Удаляет записи AutoPartPriceHistory старше PRICE_HISTORY_RETENTION_DAYS.

    История цен нужна для графиков (~30–90 дней), старше — балласт.
    Удаляем батчами чтобы не держать тяжёлый DELETE долго.
    """
    from dz_fastapi.models.autopart import AutoPartPriceHistory

    async_session_factory = app.state.session_factory
    cutoff = now_moscow() - timedelta(days=PRICE_HISTORY_RETENTION_DAYS)
    total_deleted = 0
    batch_size = 5000
    async with async_session_factory() as session:
        try:
            while True:
                # Выбираем ID батчем, затем удаляем — чтобы не держать lock
                ids_stmt = (
                    select(AutoPartPriceHistory.id)
                    .where(AutoPartPriceHistory.created_at < cutoff)
                    .limit(batch_size)
                )
                ids = (await session.execute(ids_stmt)).scalars().all()
                if not ids:
                    break
                await session.execute(
                    delete(AutoPartPriceHistory).where(
                        AutoPartPriceHistory.id.in_(ids)
                    )
                )
                await session.commit()
                total_deleted += len(ids)
                if len(ids) < batch_size:
                    break
            if total_deleted:
                logger.info(
                    "cleanup_price_history_task: deleted %s rows "
                    "(older than %s days)",
                    total_deleted,
                    PRICE_HISTORY_RETENTION_DAYS,
                )
        except Exception as exc:
            logger.error(
                "Error in cleanup_price_history_task: %s",
                exc,
                exc_info=True,
            )
            await session.rollback()


async def cleanup_app_notifications_task(app: FastAPI):
    """Удаляет AppNotification старше APP_NOTIFICATION_RETENTION_DAYS.

    Уведомления нужны только для текущей работы менеджера.
    Прочитанные удаляем быстрее (14 дней), непрочитанные — медленнее (30 дней).
    """
    from dz_fastapi.models.notification import AppNotification

    async_session_factory = app.state.session_factory
    now = now_moscow()
    cutoff_read = now - timedelta(days=APP_NOTIFICATION_RETENTION_DAYS)
    cutoff_unread = now - timedelta(days=APP_NOTIFICATION_RETENTION_DAYS)
    total_deleted = 0
    async with async_session_factory() as session:
        try:
            # Удаляем прочитанные (read_at IS NOT NULL) старше 14 дней
            r1 = await session.execute(
                delete(AppNotification).where(
                    AppNotification.read_at.isnot(None),
                    AppNotification.created_at < cutoff_read,
                )
            )
            total_deleted += r1.rowcount or 0

            # Удаляем непрочитанные старше 30 дней
            r2 = await session.execute(
                delete(AppNotification).where(
                    AppNotification.read_at.is_(None),
                    AppNotification.created_at < cutoff_unread,
                )
            )
            total_deleted += r2.rowcount or 0

            await session.commit()
            if total_deleted:
                logger.info(
                    "cleanup_app_notifications_task: deleted %s notifications",
                    total_deleted,
                )
        except Exception as exc:
            logger.error(
                "Error in cleanup_app_notifications_task: %s",
                exc,
                exc_info=True,
            )
            await session.rollback()


async def cleanup_metric_snapshots_task(app: FastAPI):
    """Удаляет SystemMetricSnapshot старше METRIC_SNAPSHOT_RETENTION_DAYS."""
    from dz_fastapi.models.settings import SystemMetricSnapshot

    async_session_factory = app.state.session_factory
    cutoff = now_moscow() - timedelta(days=METRIC_SNAPSHOT_RETENTION_DAYS)
    async with async_session_factory() as session:
        try:
            result = await session.execute(
                delete(SystemMetricSnapshot).where(
                    SystemMetricSnapshot.created_at < cutoff
                )
            )
            await session.commit()
            deleted = result.rowcount or 0
            if deleted:
                logger.info(
                    "cleanup_metric_snapshots_task: deleted %s snapshots "
                    "(older than %s days)",
                    deleted,
                    METRIC_SNAPSHOT_RETENTION_DAYS,
                )
        except Exception as exc:
            logger.error(
                "Error in cleanup_metric_snapshots_task: %s",
                exc,
                exc_info=True,
            )
            await session.rollback()


async def cleanup_misc_logs_task(app: FastAPI):
    """Очищает служебные логи и старые IGNORED/закрытые SupplierOrderMessage."""
    from dz_fastapi.models.partner import SupplierOrderMessage
    from dz_fastapi.models.settings import ExecutionTrace, PriceCheckLog

    async_session_factory = app.state.session_factory
    now = now_moscow()
    log_cutoff = now - timedelta(days=PRICE_CHECK_LOG_RETENTION_DAYS)
    msg_cutoff = now - timedelta(days=SUPPLIER_MSG_RETENTION_DAYS)
    trace_cutoff = now - timedelta(days=EXECUTION_TRACE_RETENTION_DAYS)
    async with async_session_factory() as session:
        try:
            # PriceCheckLog — хранить 30 дней
            r1 = await session.execute(
                delete(PriceCheckLog).where(
                    PriceCheckLog.checked_at < log_cutoff
                )
            )
            deleted_logs = r1.rowcount or 0

            # SupplierOrderMessage со статусом IGNORED — старше 90 дней
            # (записи в процессе — не трогаем)
            r2 = await session.execute(
                delete(SupplierOrderMessage).where(
                    SupplierOrderMessage.message_type == "IGNORED",
                    SupplierOrderMessage.received_at < msg_cutoff,
                )
            )
            deleted_msgs = r2.rowcount or 0

            # ExecutionTrace — хранить 14 дней
            r3 = await session.execute(
                delete(ExecutionTrace).where(
                    ExecutionTrace.started_at < trace_cutoff
                )
            )
            deleted_traces = r3.rowcount or 0

            await session.commit()
            logger.info(
                "cleanup_misc_logs_task: deleted %s price_check_logs, "
                "%s ignored supplier_messages, %s execution_traces",
                deleted_logs,
                deleted_msgs,
                deleted_traces,
            )
        except Exception as exc:
            logger.error(
                "Error in cleanup_misc_logs_task: %s",
                exc,
                exc_info=True,
            )
            await session.rollback()


async def mark_overdue_invoices_task(app: FastAPI):
    """Переводит просроченные счета на оплату в статус 'overdue'.

    Затрагивает счета в статусах 'sent' и 'partially_paid',
    у которых due_date < сегодня (по московскому времени).
    """
    from sqlalchemy import update

    from dz_fastapi.models.finance import InvoiceStatus, PaymentInvoice

    today = now_moscow().date()
    async_session_factory = app.state.session_factory

    async with async_session_factory() as session:
        try:
            result = await session.execute(
                update(PaymentInvoice)
                .where(
                    PaymentInvoice.due_date < today,
                    PaymentInvoice.status.in_(
                        [
                            InvoiceStatus.SENT,
                            InvoiceStatus.PARTIALLY_PAID,
                        ]
                    ),
                )
                .values(status=InvoiceStatus.OVERDUE)
                .execution_options(synchronize_session=False)
            )
            count = result.rowcount or 0
            await session.commit()
            logger.info(
                "mark_overdue_invoices_task: marked %s invoices as overdue",
                count,
            )
        except Exception as exc:
            logger.error(
                "Error in mark_overdue_invoices_task: %s",
                exc,
                exc_info=True,
            )
            await session.rollback()
