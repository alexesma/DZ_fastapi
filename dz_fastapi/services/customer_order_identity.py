from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable, Mapping

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

_ORDER_NUMBER_MARKER_RE = re.compile(
    r"(?:\bзаказ(?:\s+клиента)?\s*)?"
    r"(?:№|#|номер\s*|n(?:o|r)?\.?\s*)"
    r"(?P<number>[0-9a-zа-яё][0-9a-zа-яё._/-]*)",
    re.IGNORECASE,
)
_ORDER_LABEL_RE = re.compile(r"^\s*(?:заказ(?:\s+клиента)?|order)\s*", re.IGNORECASE)
_ORDER_DATE_SUFFIX_RE = re.compile(r"\s+от(?:\s+.*)?$", re.IGNORECASE)
_ORDER_NUMBER_NOISE_RE = re.compile(r"[^0-9a-zа-яё._/-]+", re.IGNORECASE)
_OEM_NOISE_RE = re.compile(r"[^0-9a-zа-яё]+", re.IGNORECASE)
_BRAND_NOISE_RE = re.compile(r"[^0-9a-zа-яё]+", re.IGNORECASE)
_GENERIC_BRAND_WORDS = {
    "genuine",
    "oem",
    "original",
    "originals",
    "оригинал",
    "оригинальный",
}


@dataclass(frozen=True)
class CustomerOrderMatch:
    order: Any
    basis: str


async def lock_customer_order_identity(
    session: AsyncSession,
    customer_id: int,
) -> None:
    """Serialize cross-source matching for one customer until transaction end."""

    bind = session.get_bind()
    if bind.dialect.name != "postgresql":
        return
    await session.execute(
        text("SELECT pg_advisory_xact_lock(:lock_key)"),
        {"lock_key": 6_730_000_000 + int(customer_id)},
    )


def _value(item: Any, *names: str) -> Any:
    for name in names:
        if isinstance(item, Mapping):
            value = item.get(name)
        else:
            value = getattr(item, name, None)
        if value not in (None, ""):
            return value
    return None


def _normalise_oem(value: Any) -> str:
    return _OEM_NOISE_RE.sub("", str(value or "").casefold())


def _normalise_brand(value: Any) -> str:
    words = [
        word
        for word in _BRAND_NOISE_RE.sub(" ", str(value or "").casefold()).split()
        if word not in _GENERIC_BRAND_WORDS
    ]
    return "".join(words)


def _money(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        return Decimal(str(value)).quantize(Decimal("0.01"))
    except (InvalidOperation, TypeError, ValueError):
        return None


def _item_signature(item: Any) -> tuple[str, str, int, Decimal | None]:
    return (
        _normalise_oem(_value(item, "oem", "oem_number")),
        _normalise_brand(_value(item, "make_name", "brand", "brand_name")),
        int(_value(item, "qnt", "requested_qty", "quantity") or 0),
        _money(_value(item, "cost", "requested_price", "price")),
    )


def _fingerprint(items: Iterable[Any]) -> tuple[tuple[str, str, int, Decimal | None], ...]:
    rows = [_item_signature(item) for item in items]
    return tuple(
        sorted(
            (row for row in rows if row[0]),
            key=lambda row: (
                row[0],
                row[1],
                row[2],
                row[3] is None,
                row[3] or Decimal("0"),
            ),
        )
    )


def _core_fingerprint(items: Iterable[Any]) -> tuple[tuple[str, str, int], ...]:
    quantities: dict[tuple[str, str], int] = {}
    for item in items:
        oem, brand, quantity, _ = _item_signature(item)
        if not oem or not brand:
            continue
        key = (oem, brand)
        quantities[key] = quantities.get(key, 0) + quantity
    return tuple(sorted((oem, brand, quantity) for (oem, brand), quantity in quantities.items()))


def _oem_quantity_fingerprint(items: Iterable[Any]) -> tuple[tuple[str, int], ...]:
    """Return the order shape without supplier-specific brand spelling.

    Email files and Parts-Soft occasionally describe the same item with a
    catalogue brand on one side and a commercial/alias brand on the other.
    Quantity by normalized OEM plus the exact order total is still strong
    duplicate evidence when the customer, business date and arrival window
    already match.
    """

    quantities: dict[str, int] = {}
    for item in items:
        oem, _brand, quantity, _price = _item_signature(item)
        if not oem:
            continue
        quantities[oem] = quantities.get(oem, 0) + quantity
    return tuple(sorted(quantities.items()))


def _total(items: Iterable[Any]) -> Decimal | None:
    total = Decimal("0")
    has_price = False
    for item in items:
        _, _, quantity, price = _item_signature(item)
        if price is None:
            continue
        has_price = True
        total += price * quantity
    return total.quantize(Decimal("0.01")) if has_price else None


def _order_date(order: Any) -> date | None:
    value = getattr(order, "order_date", None)
    if value is not None:
        return value
    received_at = getattr(order, "received_at", None)
    return received_at.date() if received_at is not None else None


def _timestamps_close(first: datetime | None, second: datetime | None, minutes: int) -> bool:
    if first is None or second is None:
        return False
    try:
        return abs((first - second).total_seconds()) <= minutes * 60
    except TypeError:
        return False


def match_customer_order(
    candidates: Iterable[Any],
    *,
    incoming_number: Any,
    incoming_date: date | None,
    incoming_at: datetime | None,
    incoming_items: Iterable[Any],
) -> CustomerOrderMatch | None:
    """Find one safe cross-source order match.

    A number is useful evidence but never enough on its own. Different display
    numbers are allowed when the complete item identity is the same. Ambiguous
    matches are deliberately returned as ``None`` so they can be reviewed.
    """

    items = list(incoming_items)
    number = canonical_order_number(incoming_number)
    fingerprint = _fingerprint(items)
    core = _core_fingerprint(items)
    oem_quantities = _oem_quantity_fingerprint(items)
    total = _total(items)
    ranked: list[tuple[int, str, Any]] = []

    for candidate in candidates:
        candidate_date = _order_date(candidate)
        same_date = bool(
            incoming_date is not None
            and candidate_date is not None
            and incoming_date == candidate_date
        )
        if incoming_date is not None and candidate_date is not None:
            if incoming_date != candidate_date:
                continue
        elif not _timestamps_close(incoming_at, getattr(candidate, "received_at", None), 24 * 60):
            continue

        candidate_items = list(getattr(candidate, "items", None) or [])
        candidate_number = canonical_order_number(getattr(candidate, "order_number", None))
        numbers_equal = bool(number and candidate_number and number == candidate_number)
        exact_items = bool(fingerprint and _fingerprint(candidate_items) == fingerprint)
        core_items = bool(core and _core_fingerprint(candidate_items) == core)
        oem_quantity_items = bool(
            oem_quantities and _oem_quantity_fingerprint(candidate_items) == oem_quantities
        )
        candidate_total = _total(candidate_items)
        totals_equal = bool(
            total is not None and candidate_total is not None and total == candidate_total
        )
        close_in_time = _timestamps_close(
            incoming_at,
            getattr(candidate, "received_at", None),
            120,
        )

        if numbers_equal and exact_items:
            ranked.append((400, "number_date_items", candidate))
        elif numbers_equal and core_items and totals_equal:
            ranked.append((350, "number_date_core_total", candidate))
        elif numbers_equal and not candidate_items and (same_date or close_in_time):
            ranked.append((300, "number_date_time", candidate))
        elif exact_items and (close_in_time or incoming_at is None):
            ranked.append((250, "date_items", candidate))
        elif core_items and totals_equal and close_in_time:
            ranked.append((200, "date_core_total_time", candidate))
        elif oem_quantity_items and totals_equal and close_in_time:
            ranked.append((175, "date_oem_qty_total_time", candidate))

    if not ranked:
        return None
    best_score = max(score for score, _, _ in ranked)
    best = [(basis, order) for score, basis, order in ranked if score == best_score]
    if len(best) != 1:
        return None
    basis, order = best[0]
    return CustomerOrderMatch(order=order, basis=basis)


def canonical_order_number(value: Any) -> str:
    """Return the same key for equivalent email and Parts-Soft order numbers."""

    raw = str(value or "").strip().casefold()
    if not raw:
        return ""

    marked = _ORDER_NUMBER_MARKER_RE.search(raw)
    if marked:
        raw = marked.group("number")
    else:
        # Real subjects often contain nested labels such as
        # ``Заказ ORDER-42``. Remove every leading label before comparing.
        while True:
            stripped = _ORDER_LABEL_RE.sub("", raw)
            if stripped == raw:
                break
            raw = stripped
        raw = _ORDER_DATE_SUFFIX_RE.sub("", raw)

    return _ORDER_NUMBER_NOISE_RE.sub("", raw).strip("._/-")
