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

# After window ends: keep intensive polling for this long (seconds)
GRACE_PERIOD_SECONDS = 60 * 60  # 1 hour grace period after missed window


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
    Returns True if:
    - Current time is inside any customer's expected order window, OR
    - Current time is within the 1-hour grace period after window end
      (for customers who haven't received their order yet).
    Used to switch between aggressive (2 min) and slow (20 min) polling.
    """
    now = now_moscow()
    weekday = now.weekday()
    current_t = now.time().replace(second=0, microsecond=0)
    today = now.date()
    tz = now.tzinfo

    windows = await compute_customer_order_windows(session)
    for w in windows:
        if w.weekday != weekday:
            continue
        # Inside window
        if w.window_start <= current_t <= w.window_end:
            return True
        # In grace period: window ended, but less than GRACE_PERIOD_SECONDS ago
        window_end_dt = datetime.combine(today, w.window_end).replace(tzinfo=tz)
        if now > window_end_dt:
            elapsed_after_end = (now - window_end_dt).total_seconds()
            if elapsed_after_end <= GRACE_PERIOD_SECONDS:
                # Still in grace period — check if order already received
                stmt = (
                    select(CustomerOrder.id)
                    .where(
                        CustomerOrder.customer_id == w.customer_id,
                        CustomerOrder.received_at >= window_end_dt - timedelta(hours=2),
                        CustomerOrder.received_at <= now,
                    )
                    .limit(1)
                )
                received = (await session.execute(stmt)).scalar_one_or_none()
                if received is None:
                    return True  # Grace period, no order yet → stay aggressive
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


async def get_today_order_windows_status(
    session: AsyncSession,
) -> list[dict]:
    """
    Returns today's order window status for all customers that have a window today.
    Each entry has:
      customer_id, customer_name, window_start, window_end, sample_count,
      status: 'received' | 'pending' | 'overdue' | 'grace',
      order_received_at: datetime | None,
      order_id: int | None
    """
    from dz_fastapi.models.partner import Customer

    now = now_moscow()
    weekday = now.weekday()
    current_t = now.time().replace(second=0, microsecond=0)
    today = now.date()
    tz = now.tzinfo

    windows = await compute_customer_order_windows(session)
    today_windows = [w for w in windows if w.weekday == weekday]

    if not today_windows:
        return []

    # Fetch today's orders for relevant customers (broad window to catch late orders)
    customer_ids = [w.customer_id for w in today_windows]
    day_start = datetime.combine(today, time(0, 0)).replace(tzinfo=tz)
    stmt = (
        select(
            CustomerOrder.id,
            CustomerOrder.customer_id,
            CustomerOrder.received_at,
        )
        .where(
            CustomerOrder.customer_id.in_(customer_ids),
            CustomerOrder.received_at >= day_start,
        )
        .order_by(CustomerOrder.received_at.asc())
    )
    order_rows = (await session.execute(stmt)).all()

    # Map customer_id → first order today
    orders_today: dict[int, tuple[int, datetime]] = {}
    for row in order_rows:
        if row.customer_id not in orders_today:
            orders_today[row.customer_id] = (row.id, row.received_at)

    result = []
    for w in today_windows:
        window_end_dt = datetime.combine(today, w.window_end).replace(tzinfo=tz)
        window_start_dt = datetime.combine(today, w.window_start).replace(tzinfo=tz)

        order_entry = orders_today.get(w.customer_id)
        order_id = order_entry[0] if order_entry else None
        order_received_at = order_entry[1] if order_entry else None

        if order_received_at is not None:
            status = 'received'
        elif current_t <= w.window_end:
            status = 'pending'
        else:
            elapsed = (now - window_end_dt).total_seconds()
            status = 'grace' if elapsed <= GRACE_PERIOD_SECONDS else 'overdue'

        result.append({
            'customer_id': w.customer_id,
            'customer_name': w.customer_name,
            'window_start': w.window_start.strftime('%H:%M'),
            'window_end': w.window_end.strftime('%H:%M'),
            'sample_count': w.sample_count,
            'status': status,
            'order_received_at': order_received_at.isoformat() if order_received_at else None,
            'order_id': order_id,
        })

    # Sort: overdue first, then grace, then pending, then received
    STATUS_ORDER = {'overdue': 0, 'grace': 1, 'pending': 2, 'received': 3}
    result.sort(key=lambda x: (STATUS_ORDER.get(x['status'], 9), x['customer_name']))
    return result
