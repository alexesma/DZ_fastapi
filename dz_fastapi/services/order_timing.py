from __future__ import annotations

import logging
import statistics
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from typing import Optional

from sqlalchemy import func, select
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
    weekday: int                  # 0=Mon … 6=Sun
    window_start: time
    window_end: time
    sample_count: int             # total historical orders used
    expected_order_count: int     # typical number of orders per day in this window


@dataclass
class MissingOrderAlert:
    customer_id: int
    customer_name: str
    expected_start: datetime
    expected_end: datetime
    expected_count: int
    received_count: int


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
    Compute expected arrival time windows for customer orders based on
    HISTORY_WEEKS weeks of historical CustomerOrder.received_at data.

    For each (customer_id, weekday) pair with enough samples, returns:
    - the time window (mean ± stdev, clamped)
    - expected_order_count: average number of orders per day occurrence
      (e.g. 2 if the customer typically sends 2 orders around this time)

    A customer who sends 2 orders daily will have expected_order_count=2
    for the same window.
    """
    from dz_fastapi.models.partner import Customer

    cutoff = now_moscow() - timedelta(weeks=HISTORY_WEEKS)

    rows = (
        await session.execute(
            select(
                CustomerOrder.customer_id,
                CustomerOrder.received_at,
            ).where(
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

    # Group by (customer_id, weekday):
    #   minutes_list: all arrival times as minutes-since-midnight (for window calc)
    #   dates_set:    distinct calendar dates (for expected-count calc)
    minutes_by_group: dict[tuple[int, int], list[float]] = defaultdict(list)
    dates_by_group: dict[tuple[int, int], set[date]] = defaultdict(set)

    for r in rows:
        if not r.customer_id or not r.received_at:
            continue
        key = (r.customer_id, r.received_at.weekday())
        minutes_by_group[key].append(
            float(r.received_at.hour * 60 + r.received_at.minute)
        )
        dates_by_group[key].add(r.received_at.date())

    windows: list[CustomerWindowInfo] = []
    for (customer_id, weekday), values in minutes_by_group.items():
        distinct_days = len(dates_by_group[(customer_id, weekday)])
        if distinct_days < 2:
            continue  # need at least 2 different days

        # Expected orders per day = total orders / number of distinct days
        expected_count = max(1, round(len(values) / distinct_days))

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
                expected_order_count=expected_count,
            )
        )

    return windows


async def _count_orders_in_window(
    session: AsyncSession,
    *,
    customer_id: int,
    window_start_dt: datetime,
    window_end_dt: datetime,
) -> int:
    """Count orders received by a customer within ±1h of the given window."""
    stmt = (
        select(func.count(CustomerOrder.id))
        .where(
            CustomerOrder.customer_id == customer_id,
            CustomerOrder.received_at >= window_start_dt - timedelta(hours=1),
            CustomerOrder.received_at <= window_end_dt + timedelta(hours=1),
        )
    )
    result = await session.execute(stmt)
    return result.scalar_one() or 0


async def is_in_any_order_window(session: AsyncSession) -> bool:
    """
    Returns True if aggressive polling should be active:
    - Current time is inside any customer's expected order window, OR
    - Current time is within the 1-hour grace period after a window ended
      AND the customer hasn't yet received all their expected orders.
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
        # Inside window → always aggressive
        if w.window_start <= current_t <= w.window_end:
            return True
        # Past window end → check grace period
        window_end_dt = datetime.combine(today, w.window_end).replace(tzinfo=tz)
        if now <= window_end_dt:
            continue
        elapsed = (now - window_end_dt).total_seconds()
        if elapsed > GRACE_PERIOD_SECONDS:
            continue
        # In grace period — stay aggressive only if not all expected orders received
        window_start_dt = datetime.combine(today, w.window_start).replace(tzinfo=tz)
        received = await _count_orders_in_window(
            session,
            customer_id=w.customer_id,
            window_start_dt=window_start_dt,
            window_end_dt=window_end_dt,
        )
        if received < w.expected_order_count:
            return True  # Still missing orders → stay aggressive

    return False


async def get_overdue_customer_windows(
    session: AsyncSession,
) -> list[MissingOrderAlert]:
    """
    Find customers whose expected window for TODAY has ended but the number of
    received orders is less than expected. Returns one alert per customer.
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
        received = await _count_orders_in_window(
            session,
            customer_id=w.customer_id,
            window_start_dt=window_start_dt,
            window_end_dt=window_end_dt,
        )
        if received < w.expected_order_count:
            alerts.append(
                MissingOrderAlert(
                    customer_id=w.customer_id,
                    customer_name=w.customer_name,
                    expected_start=window_start_dt,
                    expected_end=window_end_dt,
                    expected_count=w.expected_order_count,
                    received_count=received,
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
    deadline = now - timedelta(minutes=RESPONSE_WINDOW_END_MINUTES)
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

    Status values:
      'received'  — received_count >= expected_count (all orders in)
      'partial'   — 0 < received_count < expected_count (some received, more expected)
      'pending'   — window not yet ended
      'grace'     — window ended, received_count < expected_count, within grace period
      'overdue'   — window ended + grace period passed, still missing orders
    """
    now = now_moscow()
    weekday = now.weekday()
    current_t = now.time().replace(second=0, microsecond=0)
    today = now.date()
    tz = now.tzinfo

    windows = await compute_customer_order_windows(session)
    today_windows = [w for w in windows if w.weekday == weekday]

    if not today_windows:
        return []

    # Fetch all of today's orders for relevant customers
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

    # Map customer_id → list of (order_id, received_at)
    orders_today: dict[int, list[tuple[int, datetime]]] = defaultdict(list)
    for row in order_rows:
        orders_today[row.customer_id].append((row.id, row.received_at))

    result = []
    for w in today_windows:
        window_end_dt = datetime.combine(today, w.window_end).replace(tzinfo=tz)

        customer_orders = orders_today.get(w.customer_id, [])
        received_count = len(customer_orders)
        first_order_id = customer_orders[0][0] if customer_orders else None
        first_received_at = customer_orders[0][1] if customer_orders else None
        last_received_at = customer_orders[-1][1] if customer_orders else None

        # Determine status
        window_ended = current_t > w.window_end
        if received_count >= w.expected_order_count:
            status = 'received'
        elif not window_ended:
            status = 'partial' if received_count > 0 else 'pending'
        else:
            elapsed = (now - window_end_dt).total_seconds()
            if elapsed <= GRACE_PERIOD_SECONDS:
                status = 'partial' if received_count > 0 else 'grace'
            else:
                status = 'partial' if received_count > 0 else 'overdue'

        result.append({
            'customer_id': w.customer_id,
            'customer_name': w.customer_name,
            'window_start': w.window_start.strftime('%H:%M'),
            'window_end': w.window_end.strftime('%H:%M'),
            'sample_count': w.sample_count,
            'expected_order_count': w.expected_order_count,
            'received_count': received_count,
            'status': status,
            'first_order_received_at': (
                first_received_at.isoformat() if first_received_at else None
            ),
            'last_order_received_at': (
                last_received_at.isoformat() if last_received_at else None
            ),
            'order_id': first_order_id,
        })

    # Sort: overdue first, grace, partial, pending, received
    STATUS_ORDER = {
        'overdue': 0,
        'grace': 1,
        'partial': 2,
        'pending': 3,
        'received': 4
    }
    result.sort(
        key=lambda x: (STATUS_ORDER.get(x['status'], 9), x['customer_name'])
    )
    return result
