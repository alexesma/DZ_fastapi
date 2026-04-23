"""Holiday calendar service.

Combines:
  1. ``python-holidays`` automatic Russian public holidays.
  2. Manual overrides stored in ``SupplierHoliday`` table:
       - is_working_day=False → extra non-working day (adds to holiday set)
       - is_working_day=True  → forced working day (removes from holiday set,
                                 even if python-holidays or weekend)
"""
from __future__ import annotations

from datetime import date, timedelta
from typing import Iterable

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.models.settings import SupplierHoliday


def _auto_holidays_for_years(years: Iterable[int]) -> set[date]:
    """Return set of public holidays for Russia for the given years."""
    try:
        import holidays as _holidays
        result: set[date] = set()
        for year in years:
            ru = _holidays.Russia(years=year)
            result.update(ru.keys())
        return result
    except ImportError:
        return set()


async def get_manual_holidays(
    session: AsyncSession, years: Iterable[int]
) -> list[SupplierHoliday]:
    """Fetch all manual SupplierHoliday entries for given years."""
    year_list = list(years)
    if not year_list:
        return []
    stmt = (
        select(SupplierHoliday)
        .where(
            SupplierHoliday.date.between(
                date(min(year_list), 1, 1),
                date(max(year_list), 12, 31),
            )
        )
        .order_by(SupplierHoliday.date)
    )
    result = await session.execute(stmt)
    return list(result.scalars().all())


async def get_effective_holiday_set(
    session: AsyncSession,
    years: Iterable[int],
) -> set[date]:
    """Return the effective set of non-working dates.

    Starts from python-holidays, then applies manual overrides:
      - Manual holiday (is_working_day=False) → add to set
      - Manual workday override (is_working_day=True) → remove from set
    """
    year_list = list(years)
    holiday_set = _auto_holidays_for_years(year_list)

    manual = await get_manual_holidays(session, year_list)
    for entry in manual:
        if entry.is_working_day:
            holiday_set.discard(entry.date)
        else:
            holiday_set.add(entry.date)

    return holiday_set


def is_business_day(d: date, holiday_set: set[date]) -> bool:
    """Return True if *d* is a business day (Mon–Fri, not in holiday_set).

    Manual workday overrides (is_working_day=True) are already removed from
    holiday_set by get_effective_holiday_set, so we don't need to handle them
    separately here.
    """
    if d.weekday() >= 5:
        return False
    return d not in holiday_set


def next_business_day(d: date, holiday_set: set[date]) -> date:
    """Return the next calendar day after *d* that is a business day."""
    candidate = d + timedelta(days=1)
    for _ in range(20):  # safety cap — no holiday run longer than 20 days
        if is_business_day(candidate, holiday_set):
            return candidate
        candidate += timedelta(days=1)
    return candidate  # fallback (should never reach here)
