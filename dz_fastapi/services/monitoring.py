import os
import shutil
import time
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.core.scheduler_settings import SCHEDULER_SETTING_DEFAULTS
from dz_fastapi.core.time import now_moscow
from dz_fastapi.crud.settings import (crud_price_check_schedule,
                                      crud_scheduler_setting)


def _read_meminfo() -> tuple[int | None, int | None]:
    path = '/proc/meminfo'
    if not os.path.exists(path):
        return None, None
    values: dict[str, int] = {}
    try:
        with open(path, 'r', encoding='utf-8') as handle:
            for line in handle:
                parts = line.strip().split(':', 1)
                if len(parts) != 2:
                    continue
                key = parts[0]
                value_part = parts[1].strip().split(' ')[0]
                if not value_part.isdigit():
                    continue
                values[key] = int(value_part)
    except Exception:
        return None, None
    mem_total = values.get('MemTotal')
    mem_available = values.get('MemAvailable')
    if mem_total is None or mem_available is None:
        return None, None
    return mem_total * 1024, mem_available * 1024


def _get_disk_usage(path: str = '/') -> tuple[
    int | None, int | None, int | None
]:
    try:
        usage = shutil.disk_usage(path)
        return usage.total, usage.used, usage.free
    except Exception:
        return None, None, None


def _get_cpu_load() -> tuple[float | None, float | None, float | None]:
    try:
        load1, load5, load15 = os.getloadavg()
        return float(load1), float(load5), float(load15)
    except Exception:
        return None, None, None


async def get_db_metrics(
    session: AsyncSession,
    table_limit: int = 10,
) -> dict[str, Any]:
    size_bytes = 0
    size_pretty = '0 B'
    connections = 0
    max_connections = 0
    tables: list[dict[str, Any]] = []

    try:
        size_row = await session.execute(
            text(
                'SELECT '
                'pg_database_size(current_database()) AS size_bytes, '
                'pg_size_pretty(pg_database_size(current_database())) '
                'AS size_pretty'
            )
        )
        size = size_row.first()
        if size:
            size_bytes = int(size.size_bytes)
            size_pretty = str(size.size_pretty)
    except Exception:
        pass

    try:
        conn_row = await session.execute(
            text(
                'SELECT count(*) AS connections '
                'FROM pg_stat_activity '
                'WHERE datname = current_database()'
            )
        )
        connections = int(conn_row.scalar() or 0)
    except Exception:
        pass

    try:
        max_row = await session.execute(
            text(
                "SELECT setting::int AS max_connections "
                "FROM pg_settings WHERE name = 'max_connections'"
            )
        )
        max_connections = int(max_row.scalar() or 0)
    except Exception:
        pass

    try:
        tables_rows = await session.execute(
            text(
                'SELECT relname AS table_name, '
                'pg_total_relation_size(relid) AS size_bytes, '
                'pg_size_pretty(pg_total_relation_size(relid)) AS size_pretty '
                'FROM pg_catalog.pg_statio_user_tables '
                'ORDER BY size_bytes DESC '
                'LIMIT :limit'
            ),
            {'limit': table_limit},
        )
        tables = [
            {
                'table': row.table_name,
                'size_bytes': int(row.size_bytes),
                'size_pretty': str(row.size_pretty),
            }
            for row in tables_rows
        ]
    except Exception:
        tables = []

    return {
        'size_bytes': size_bytes,
        'size_pretty': size_pretty,
        'connections': connections,
        'max_connections': max_connections,
        'tables': tables,
    }


def get_system_metrics(app) -> dict[str, Any]:
    disk_total, disk_used, disk_free = _get_disk_usage('/')
    mem_total, mem_available = _read_meminfo()
    cpu1, cpu5, cpu15 = _get_cpu_load()
    uptime_seconds = None
    started_at = getattr(app.state, 'started_at', None)
    if started_at is not None:
        uptime_seconds = max(time.time() - float(started_at), 0)
    return {
        'disk_total_bytes': disk_total,
        'disk_used_bytes': disk_used,
        'disk_free_bytes': disk_free,
        'mem_total_bytes': mem_total,
        'mem_available_bytes': mem_available,
        'cpu_load_1': cpu1,
        'cpu_load_5': cpu5,
        'cpu_load_15': cpu15,
        'uptime_seconds': uptime_seconds,
    }


async def get_monitor_summary(
    session: AsyncSession,
    app,
) -> dict[str, Any]:
    db_metrics = await get_db_metrics(session=session, table_limit=10)
    system_metrics = get_system_metrics(app)
    schedule = await crud_price_check_schedule.get_or_create(session)
    scheduler_settings = []
    for key, defaults in SCHEDULER_SETTING_DEFAULTS.items():
        setting = await crud_scheduler_setting.get_or_create(
            session=session, key=key, defaults=defaults
        )
        scheduler_settings.append(setting)

    return {
        'db': db_metrics,
        'system': system_metrics,
        'app': {
            'last_price_check_at': schedule.last_checked_at,
            'scheduler_last_runs': scheduler_settings,
        },
    }


def build_snapshot_payload(summary: dict[str, Any]) -> dict[str, Any]:
    db = summary.get('db', {}) if summary else {}
    system = summary.get('system', {}) if summary else {}
    return {
        'created_at': now_moscow(),
        'db_size_bytes': db.get('size_bytes'),
        'disk_total_bytes': system.get('disk_total_bytes'),
        'disk_free_bytes': system.get('disk_free_bytes'),
        'mem_total_bytes': system.get('mem_total_bytes'),
        'mem_available_bytes': system.get('mem_available_bytes'),
    }
