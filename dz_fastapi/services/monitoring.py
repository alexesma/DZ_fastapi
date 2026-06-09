import os
import shutil
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.core.scheduler_settings import SCHEDULER_SETTING_DEFAULTS
from dz_fastapi.core.time import now_moscow
from dz_fastapi.crud.settings import (
    crud_execution_trace,
    crud_price_check_schedule,
    crud_scheduler_setting,
)
from dz_fastapi.services.runtime_memory import process_rss_mb


def _read_meminfo() -> tuple[int | None, int | None]:
    path = "/proc/meminfo"
    if not os.path.exists(path):
        return None, None
    values: dict[str, int] = {}
    try:
        with open(path, "r", encoding="utf-8") as handle:
            for line in handle:
                parts = line.strip().split(":", 1)
                if len(parts) != 2:
                    continue
                key = parts[0]
                value_part = parts[1].strip().split(" ")[0]
                if not value_part.isdigit():
                    continue
                values[key] = int(value_part)
    except Exception:
        return None, None
    mem_total = values.get("MemTotal")
    mem_available = values.get("MemAvailable")
    if mem_total is None or mem_available is None:
        return None, None
    return mem_total * 1024, mem_available * 1024


def _get_disk_usage(
    path: str = "/",
) -> tuple[int | None, int | None, int | None]:
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
    size_pretty = "0 B"
    connections = 0
    max_connections = 0
    tables: list[dict[str, Any]] = []

    try:
        size_row = await session.execute(
            text(
                "SELECT "
                "pg_database_size(current_database()) AS size_bytes, "
                "pg_size_pretty(pg_database_size(current_database())) "
                "AS size_pretty"
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
                "SELECT count(*) AS connections "
                "FROM pg_stat_activity "
                "WHERE datname = current_database()"
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
                "SELECT relname AS table_name, "
                "pg_total_relation_size(relid) AS size_bytes, "
                "pg_size_pretty(pg_total_relation_size(relid)) AS size_pretty "
                "FROM pg_catalog.pg_statio_user_tables "
                "ORDER BY size_bytes DESC "
                "LIMIT :limit"
            ),
            {"limit": table_limit},
        )
        tables = [
            {
                "table": row.table_name,
                "size_bytes": int(row.size_bytes),
                "size_pretty": str(row.size_pretty),
            }
            for row in tables_rows
        ]
    except Exception:
        tables = []

    return {
        "size_bytes": size_bytes,
        "size_pretty": size_pretty,
        "connections": connections,
        "max_connections": max_connections,
        "tables": tables,
    }


def get_system_metrics(app) -> dict[str, Any]:
    disk_total, disk_used, disk_free = _get_disk_usage("/")
    mem_total, mem_available = _read_meminfo()
    cpu1, cpu5, cpu15 = _get_cpu_load()
    uptime_seconds = None
    started_at = getattr(app.state, "started_at", None)
    if started_at is not None:
        uptime_seconds = max(time.time() - float(started_at), 0)
    return {
        "disk_total_bytes": disk_total,
        "disk_used_bytes": disk_used,
        "disk_free_bytes": disk_free,
        "mem_total_bytes": mem_total,
        "mem_available_bytes": mem_available,
        "cpu_load_1": cpu1,
        "cpu_load_5": cpu5,
        "cpu_load_15": cpu15,
        "uptime_seconds": uptime_seconds,
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
        "db": db_metrics,
        "system": system_metrics,
        "app": {
            "last_price_check_at": schedule.last_checked_at,
            "scheduler_last_runs": scheduler_settings,
        },
    }


def build_snapshot_payload(summary: dict[str, Any]) -> dict[str, Any]:
    db = summary.get("db", {}) if summary else {}
    system = summary.get("system", {}) if summary else {}
    return {
        "created_at": now_moscow(),
        "db_size_bytes": db.get("size_bytes"),
        "disk_total_bytes": system.get("disk_total_bytes"),
        "disk_free_bytes": system.get("disk_free_bytes"),
        "mem_total_bytes": system.get("mem_total_bytes"),
        "mem_available_bytes": system.get("mem_available_bytes"),
    }


@dataclass
class ExecutionTraceContext:
    app: Any
    trace_type: str
    job_key: str
    job_name: str
    provider_id: int | None = None
    provider_config_id: int | None = None
    source_filename: str | None = None
    trace_id: int | None = None
    started_at: Any = field(default_factory=now_moscow)
    rss_before_mb: float | None = None
    details: dict[str, Any] = field(default_factory=dict)


def _merge_trace_details(
    current_details: dict[str, Any] | None,
    extra_details: dict[str, Any] | None,
) -> dict[str, Any]:
    merged: dict[str, Any] = {}
    if isinstance(current_details, dict):
        merged.update(current_details)
    if isinstance(extra_details, dict):
        merged.update(extra_details)
    merged.pop("__trace_status", None)
    return merged


async def _create_execution_trace(context: ExecutionTraceContext) -> None:
    session_factory = context.app.state.session_factory
    async with session_factory() as session:
        trace = await crud_execution_trace.create(
            session=session,
            payload={
                "trace_type": context.trace_type,
                "job_key": context.job_key,
                "job_name": context.job_name,
                "status": "running",
                "provider_id": context.provider_id,
                "provider_config_id": context.provider_config_id,
                "source_filename": context.source_filename,
                "started_at": context.started_at,
                "rss_before_mb": context.rss_before_mb,
                "details": context.details or {},
            },
        )
        context.trace_id = int(trace.id)


async def _finish_execution_trace(
    context: ExecutionTraceContext,
    *,
    status: str,
    extra_details: dict[str, Any] | None = None,
) -> None:
    if context.trace_id is None:
        return
    session_factory = context.app.state.session_factory
    finished_at = now_moscow()
    rss_after_mb = process_rss_mb()
    duration_ms = int(
        max((finished_at - context.started_at).total_seconds(), 0) * 1000
    )
    memory_delta_mb = None
    if context.rss_before_mb is not None and rss_after_mb is not None:
        memory_delta_mb = float(rss_after_mb) - float(context.rss_before_mb)
    async with session_factory() as session:
        await crud_execution_trace.update(
            session=session,
            trace_id=context.trace_id,
            data={
                "status": status,
                "finished_at": finished_at,
                "duration_ms": duration_ms,
                "rss_after_mb": rss_after_mb,
                "memory_delta_mb": memory_delta_mb,
                "details": _merge_trace_details(context.details, extra_details),
            },
        )


@asynccontextmanager
async def tracked_execution(
    app,
    *,
    trace_type: str,
    job_key: str,
    job_name: str,
    provider_id: int | None = None,
    provider_config_id: int | None = None,
    source_filename: str | None = None,
    details: dict[str, Any] | None = None,
):
    context = ExecutionTraceContext(
        app=app,
        trace_type=trace_type,
        job_key=job_key,
        job_name=job_name,
        provider_id=provider_id,
        provider_config_id=provider_config_id,
        source_filename=source_filename,
        rss_before_mb=process_rss_mb(),
        details=dict(details or {}),
    )
    await _create_execution_trace(context)
    try:
        yield context
    except Exception as exc:
        await _finish_execution_trace(
            context,
            status="error",
            extra_details={"error": str(exc)[:2000]},
        )
        raise
    else:
        status = str(context.details.get("__trace_status") or "success")
        await _finish_execution_trace(context, status=status)
