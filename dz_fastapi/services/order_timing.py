from __future__ import annotations

import logging
import statistics
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta

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

# Minimum gap between two order clusters to treat as separate windows
TWO_WINDOW_MIN_GAP_MINUTES = 150  # 2.5 hours

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
    sample_count: int             # historical orders in this sub-window
    expected_order_count: int     # typical orders per day in this window
    window_index: int = 0         # 0 = only/morning, 1 = evening
    split_minute: float | None = None
    # minute-of-day divider for 2-window customers


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


def _try_split_two_windows(values: list[float]) -> float | None:
    """
    If the order times show two distinct clusters separated by at least
    TWO_WINDOW_MIN_GAP_MINUTES, return the midpoint minute between them.
    Both clusters must have at least 2 samples. Returns None otherwise.
    """
    if len(values) < 4:
        return None
    sorted_vals = sorted(values)
    max_gap = 0.0
    gap_after_idx = -1
    for i in range(len(sorted_vals) - 1):
        gap = sorted_vals[i + 1] - sorted_vals[i]
        if gap > max_gap:
            max_gap = gap
            gap_after_idx = i
    if max_gap < TWO_WINDOW_MIN_GAP_MINUTES:
        return None
    # Ensure both sides have at least 2 samples
    if gap_after_idx < 1 or gap_after_idx >= len(sorted_vals) - 2:
        return None
    return (sorted_vals[gap_after_idx] + sorted_vals[gap_after_idx + 1]) / 2.0


def _build_window(
    customer_id: int,
    customer_name: str,
    weekday: int,
    entries: list[tuple[float, date]],
    window_index: int,
    split_minute: float | None,
) -> CustomerWindowInfo | None:
    """Build a CustomerWindowInfo from a list of (minutes, date) entries."""
    if len(entries) < 2:
        return None
    values = [m for m, _ in entries]
    distinct_days = len({d for _, d in entries})
    if distinct_days < 2:
        return None

    expected_count = max(1, round(len(values) / distinct_days))
    mean_m = statistics.mean(values)
    try:
        std_m = statistics.stdev(values)
    except statistics.StatisticsError:
        std_m = 30.0

    half = max(min(std_m, MAX_HALF_WINDOW_MINUTES), MIN_HALF_WINDOW_MINUTES)
    start_m = max(0, int(mean_m - half))
    end_m = min(23 * 60 + 59, int(mean_m + half))

    return CustomerWindowInfo(
        customer_id=customer_id,
        customer_name=customer_name,
        weekday=weekday,
        window_start=time(start_m // 60, start_m % 60),
        window_end=time(end_m // 60, end_m % 60),
        sample_count=len(values),
        expected_order_count=expected_count,
        window_index=window_index,
        split_minute=split_minute,
    )


async def compute_customer_order_windows(
    session: AsyncSession,
) -> list[CustomerWindowInfo]:
    """
    Compute expected arrival time windows for customer orders.

    For each (customer_id, weekday) pair:
    - Detects whether the customer has 1 or 2 daily order sessions by looking
      for a gap of ≥ TWO_WINDOW_MIN_GAP_MINUTES between clusters.
    - If 2 clusters: returns two windows (morning + evening), each with
      window_index 0 / 1 and a shared split_minute boundary.
    - If 1 cluster: returns one window (window_index=0, split_minute=None).
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

    # Group (customer_id, weekday) → list of (minutes_since_midnight, date)
    entries_by_group: dict[
        tuple[int, int], list[tuple[float, date]]
    ] = defaultdict(list)
    for r in rows:
        if not r.customer_id or not r.received_at:
            continue
        key = (r.customer_id, r.received_at.weekday())
        entries_by_group[key].append(
            (float(
                r.received_at.hour * 60 + r.received_at.minute
            ), r.received_at.date())
        )

    windows: list[CustomerWindowInfo] = []
    for (customer_id, weekday), entries in entries_by_group.items():
        values = [m for m, _ in entries]
        name = customer_names.get(customer_id, f"Customer #{customer_id}")

        split_m = _try_split_two_windows(values)

        if split_m is not None:
            # Two-window customer: split entries by the gap midpoint
            morning_entries = [(m, d) for m, d in entries if m <= split_m]
            evening_entries = [(m, d) for m, d in entries if m > split_m]

            w0 = _build_window(
                customer_id,
                name,
                weekday,
                morning_entries,
                0,
                split_m
            )
            w1 = _build_window(
                customer_id,
                name,
                weekday,
                evening_entries,
                1,
                split_m
            )

            # Only use the 2-window split if BOTH clusters qualify
            if w0 is not None and w1 is not None:
                windows.append(w0)
                windows.append(w1)
                continue
            # Fall through to single-window if one cluster is too thin

        # Single window
        w = _build_window(customer_id, name, weekday, entries, 0, None)
        if w is not None:
            windows.append(w)

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
    Returns True if aggressive polling should be active.
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
        if w.window_start <= current_t <= w.window_end:
            return True
        window_end_dt = datetime.combine(
            today,
            w.window_end
        ).replace(tzinfo=tz)
        if now <= window_end_dt:
            continue
        elapsed = (now - window_end_dt).total_seconds()
        if elapsed > GRACE_PERIOD_SECONDS:
            continue
        window_start_dt = datetime.combine(
            today,
            w.window_start
        ).replace(tzinfo=tz)
        received = await _count_orders_in_window(
            session,
            customer_id=w.customer_id,
            window_start_dt=window_start_dt,
            window_end_dt=window_end_dt,
        )
        if received < w.expected_order_count:
            return True

    return False


async def get_overdue_customer_windows(
    session: AsyncSession,
) -> list[MissingOrderAlert]:
    """
    Find customers whose expected window for TODAY has ended but the number of
    received orders is less than expected.
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
        window_end_dt = datetime.combine(
            today,
            w.window_end
        ).replace(tzinfo=tz)
        if now <= window_end_dt:
            continue

        window_start_dt = datetime.combine(
            today,
            w.window_start
        ).replace(tzinfo=tz)
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
    Returns today's order window status per customer (and per sub-window
    if the customer has both morning and evening windows).

    Each row includes:
      window_index  – 0 = single/morning, 1 = evening
      window_label  – None (single), 'Утро', or 'Вечер'
      status        – 'received' | 'partial' | 'pending' | 'grace' | 'overdue'
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

    # Determine which customers have 2 windows today (for labelling)
    customers_with_two_windows: set[int] = set()
    customer_window_counts: dict[int, int] = defaultdict(int)
    for w in today_windows:
        customer_window_counts[w.customer_id] += 1
    for cid, cnt in customer_window_counts.items():
        if cnt >= 2:
            customers_with_two_windows.add(cid)

    # Fetch all of today's orders for relevant customers
    customer_ids = list({w.customer_id for w in today_windows})
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
        all_customer_orders = orders_today.get(w.customer_id, [])

        # Partition orders to this specific window using split_minute
        if w.split_minute is not None:
            split_t = w.split_minute  # minutes since midnight
            if w.window_index == 0:
                window_orders = [
                    (oid, rat) for oid, rat in all_customer_orders
                    if rat.hour * 60 + rat.minute <= split_t
                ]
            else:
                window_orders = [
                    (oid, rat) for oid, rat in all_customer_orders
                    if rat.hour * 60 + rat.minute > split_t
                ]
        else:
            window_orders = all_customer_orders

        received_count = len(window_orders)
        first_order_id = window_orders[0][0] if window_orders else None
        first_received_at = window_orders[0][1] if window_orders else None
        last_received_at = window_orders[-1][1] if window_orders else None

        window_end_dt = datetime.combine(
            today,
            w.window_end
        ).replace(tzinfo=tz)
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

        # Label only if this customer has 2 windows
        if w.customer_id in customers_with_two_windows:
            window_label = 'Утро' if w.window_index == 0 else 'Вечер'
        else:
            window_label = None

        result.append({
            'customer_id': w.customer_id,
            'customer_name': w.customer_name,
            'window_index': w.window_index,
            'window_label': window_label,
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

    STATUS_ORDER = {
        'overdue': 0,
        'grace': 1,
        'partial': 2,
        'pending': 3,
        'received': 4,
    }
    result.sort(
        key=lambda x: (
            STATUS_ORDER.get(x['status'], 9),
            x['customer_name'],
            x['window_index'],
        )
    )
    return result
