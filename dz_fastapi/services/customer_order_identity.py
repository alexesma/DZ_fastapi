from __future__ import annotations

import re
from typing import Any

_ORDER_NUMBER_MARKER_RE = re.compile(
    r"(?:\bзаказ(?:\s+клиента)?\s*)?"
    r"(?:№|#|номер\s*|n(?:o|r)?\.?\s*)"
    r"(?P<number>[0-9a-zа-яё][0-9a-zа-яё._/-]*)",
    re.IGNORECASE,
)
_ORDER_LABEL_RE = re.compile(r"^\s*(?:заказ(?:\s+клиента)?|order)\s*", re.IGNORECASE)
_ORDER_DATE_SUFFIX_RE = re.compile(r"\s+от(?:\s+.*)?$", re.IGNORECASE)
_ORDER_NUMBER_NOISE_RE = re.compile(r"[^0-9a-zа-яё._/-]+", re.IGNORECASE)


def canonical_order_number(value: Any) -> str:
    """Return the same key for equivalent email and Parts-Soft order numbers."""

    raw = str(value or "").strip().casefold()
    if not raw:
        return ""

    marked = _ORDER_NUMBER_MARKER_RE.search(raw)
    if marked:
        raw = marked.group("number")
    else:
        raw = _ORDER_LABEL_RE.sub("", raw)
        raw = _ORDER_DATE_SUFFIX_RE.sub("", raw)

    return _ORDER_NUMBER_NOISE_RE.sub("", raw).strip("._/-")
