from __future__ import annotations

import logging
import statistics
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from typing import Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.core.time import now_moscow
from dz_fastapi.models.partner import CustomerOrder, SupplierOrder

logger = logging.getLogger('dz_fastapi')

# History window for computing order arrival patterns
HISTORY_WEEKS = 4

# Window boundaries: mean ± half_window (clamped)
MIN_HALF_WINDOW_MINUTES = 20   # at minimum ±20 min
MAX_HALF_WINDOW_MINUTES = 60   # at most ±60 min

# Supplier response expected window
RESPONSE_WINDOW_START_MINUTES = 40   # start checking after 40 min
RESPONSE_WINDOW_END_MINUTES = 120    # stop checking after 2 hours

# Outside all windows → reduce polling to this interval (seconds)
OUTSIDE_WINDOW_SLOW_SECONDS = 20 * 60  # 20 minutes


@dataclass
class CustomerWindowInfo:
    customer_id: int
    customer_name: str
    weekday: int          # 0=Mon … 6=Sun
    window_start: time
    window_end: time
    sample_count: int     # number of historical orders used


@dataclass
class MissingOrderAlert:
    customer_id: int
    customer_name: str
    expected_start: datetime
    expected_end: datetime


@dataclass
class MissingResponseAlert:
    provider_id: int
    provider_name: str
    supplier_order_id: int
    sent_at: datetime
    window_ended_at: datetime


async def compute_customer_order_windows(
    session: AsyncSession,
) -> list[CustomerWindowInfo]:
    """
    Compute expected arrival time windows for customer orders
    based on HISTORY_WEEKS weeks of historical CustomerOrder.received_at data.
    Returns one window per (customer_id, weekday) pair with enough samples.
    """
    from dz_fastapi.models.partner import Customer

    cutoff = now_moscow() - timedelta(weeks=HISTORY_WEEKS)

    rows = (
        await session.execute(
            select(CustomerOrder.customer_id, CustomerOrder.received_at).where(
                CustomerOrder.received_at.is_not(None),
                CustomerOrder.received_at >= cutoff,
                CustomerOrder.customer_id.is_not(None),
            )
        )
    ).all()

    if not rows:
        return []

    customer_ids = {r.customer_id for r in rows}
    customer_names: dict[int, str] = {}
    if customer_ids:
        name_rows = (
            await session.execute(
                select(Customer.id, Customer.name).where(
                    Customer.id.in_(customer_ids)
                )
            )
        ).all()
        customer_names = {r.id: r.name for r in name_rows}

    # Group: (customer_id, weekday) → list of minutes-since-midnight
    groups: dict[tuple[int, int], list[float]] = defaultdict(list)
    for r in rows:
        if not r.customer_id or not r.received_at:
            continue
        weekday = r.received_at.weekday()
        minutes = float(r.received_at.hour * 60 + r.received_at.minute)
        groups[(r.customer_id, weekday)].append(minutes)

    windows: list[CustomerWindowInfo] = []
    for (customer_id, weekday), values in groups.items():
        if len(values) < 2:
            continue  # need at least 2 samples

        mean_m = statistics.mean(values)
        try:
            std_m = statistics.stdev(values)
        except statistics.StatisticsError:
            std_m = 30.0

        half = max(min(std_m, MAX_HALF_WINDOW_MINUTES), MIN_HALF_WINDOW_MINUTES)

        start_m = max(0, int(mean_m - half))
        end_m = min(23 * 60 + 59, int(mean_m + half))

        windows.append(
            CustomerWindowInfo(
                customer_id=customer_id,
                customer_name=customer_names.get(
                    customer_id, f"Customer #{customer_id}"
                ),
                weekday=weekday,
                window_start=time(start_m // 60, start_m % 60),
                window_end=time(end_m // 60, end_m % 60),
                sample_count=len(values),
            )
        )

    return windows


async def is_in_any_order_window(session: AsyncSession) -> bool:
    """
    Returns True if the current Moscow time falls within any customer's
    expected order window for today's weekday.
    Used to switch between aggressive (2 min) and slow (20 min) polling.
    """
    now = now_moscow()
    weekday = now.weekday()
    current_t = now.time().replace(second=0, microsecond=0)

    windows = await compute_customer_order_windows(session)
    for w in windows:
        if w.weekday == weekday and w.window_start <= current_t <= w.window_end:
            return True
    return False


async def get_overdue_customer_windows(
    session: AsyncSession,
) -> list[MissingOrderAlert]:
    """
    Find customers whose expected window for TODAY has already ended
    but no CustomerOrder was received during that window.
    Only returns entries where the window has fully passed.
    """
    now = now_moscow()
    today = now.date()
    weekday = now.weekday()
    tz = now.tzinfo

    windows = await compute_customer_order_windows(session)
    alerts: list[MissingOrderAlert] = []

    for w in windows:
        if w.weekday != weekday:
            continue
        window_end_dt = datetime.combine(today, w.window_end).replace(tzinfo=tz)
        if now <= window_end_dt:
            continue  # window not yet ended

        window_start_dt = datetime.combine(today, w.window_start).replace(tzinfo=tz)
        # Check if any order came in ±1h around this window
        stmt = (
            select(CustomerOrder.id)
            .where(
                CustomerOrder.customer_id == w.customer_id,
                CustomerOrder.received_at
                >= window_start_dt - timedelta(hours=1),
                CustomerOrder.received_at
                <= window_end_dt + timedelta(hours=1),
            )
            .limit(1)
        )
        found = (await session.execute(stmt)).scalar_one_or_none()
        if found is None:
            alerts.append(
                MissingOrderAlert(
                    customer_id=w.customer_id,
                    customer_name=w.customer_name,
                    expected_start=window_start_dt,
                    expected_end=window_end_dt,
                )
            )

    return alerts


async def get_active_supplier_response_provider_ids(
    session: AsyncSession,
) -> list[int]:
    """
    Returns provider_ids for which we currently expect an email response.
    Criteria: SupplierOrder sent between 40 min and 2 h ago, no response yet.
    """
    now = now_moscow()
    window_start = now - timedelta(minutes=RESPONSE_WINDOW_END_MINUTES)
    window_end = now - timedelta(minutes=RESPONSE_WINDOW_START_MINUTES)

    stmt = (
        select(SupplierOrder.provider_id)
        .where(
            SupplierOrder.sent_at.is_not(None),
            SupplierOrder.sent_at >= window_start,
            SupplierOrder.sent_at <= window_end,
            SupplierOrder.response_status_raw.is_(None),
        )
        .distinct()
    )
    return list((await session.execute(stmt)).scalars().all())


async def get_overdue_supplier_responses(
    session: AsyncSession,
) -> list[MissingResponseAlert]:
    """
    Find SupplierOrders where the 2-hour response window has expired
    without any response being recorded.
    Looks back at most 24 hours to avoid flooding old data.
    """
    from dz_fastapi.models.partner import Provider

    now = now_moscow()
    # Window ended before: sent_at <= now - 2h
    deadline = now - timedelta(minutes=RESPONSE_WINDOW_END_MINUTES)
    # Don't look further back than 24h
    lookback = deadline - timedelta(hours=22)

    stmt = (
        select(SupplierOrder, Provider)
        .join(Provider, Provider.id == SupplierOrder.provider_id)
        .where(
            SupplierOrder.sent_at.is_not(None),
            SupplierOrder.sent_at <= deadline,
            SupplierOrder.sent_at >= lookback,
            SupplierOrder.response_status_raw.is_(None),
        )
    )
    rows = (await session.execute(stmt)).all()
    alerts: list[MissingResponseAlert] = []
    for order, provider in rows:
        alerts.append(
            MissingResponseAlert(
                provider_id=provider.id,
                provider_name=provider.name,
                supplier_order_id=order.id,
                sent_at=order.sent_at,
                window_ended_at=order.sent_at
                + timedelta(minutes=RESPONSE_WINDOW_END_MINUTES),
            )
        )
    return alerts
