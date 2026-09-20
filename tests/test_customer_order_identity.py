from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace

from dz_fastapi.services.customer_order_identity import match_customer_order

NOW = datetime(2026, 9, 16, 8, 20, tzinfo=timezone.utc)


def _item(oem="BH3888E", brand="NOK", quantity=2, price="5056.00"):
    return SimpleNamespace(
        oem=oem,
        brand=brand,
        requested_qty=quantity,
        requested_price=Decimal(price),
    )


def _order(number, *, when=NOW, items=None):
    return SimpleNamespace(
        order_number=number,
        order_date=when.date(),
        received_at=when,
        items=list(items if items is not None else [_item()]),
    )


def _incoming(number, *, when=NOW, brand="NOK ORIGINAL", price="5056.00"):
    return {
        "number": number,
        "date": when.date(),
        "at": when,
        "items": [
            {
                "oem": "BH-3888-E",
                "make_name": brand,
                "qnt": 2,
                "cost": price,
            }
        ],
    }


def _match(candidates, incoming):
    return match_customer_order(
        candidates,
        incoming_number=incoming["number"],
        incoming_date=incoming["date"],
        incoming_at=incoming["at"],
        incoming_items=incoming["items"],
    )


def test_different_display_numbers_can_match_by_exact_content():
    order = _order("EMAIL-4072")
    match = _match([order], _incoming("SITE-217301"))
    assert match is not None
    assert match.order is order
    assert match.basis == "date_items"


def test_same_number_does_not_match_another_date_and_content():
    old = datetime(2026, 9, 15, 8, 20, tzinfo=timezone.utc)
    order = _order("ORDER-77", when=old, items=[_item("OLD-1", "BRAND-A", 1, "100")])
    assert _match([order], _incoming("ORDER-77")) is None


def test_fallback_does_not_ignore_brand_or_total():
    order = _order(None)
    assert _match([order], _incoming(None, brand="OTHER", price="9999")) is None


def test_ambiguous_equal_candidates_require_review():
    first = _order(None)
    second = _order(None)
    assert _match([first, second], _incoming(None)) is None


def test_same_number_and_date_can_link_stub_without_items():
    order = _order("№ 37137 от", items=[])
    incoming = _incoming("Заказ № 37137 от 16.09.2026 8:59:40")
    match = _match([order], incoming)
    assert match is not None
    assert match.basis == "number_date_time"


def test_missing_dates_require_close_timestamps():
    old = datetime(2026, 9, 14, 8, 20, tzinfo=timezone.utc)
    order = SimpleNamespace(
        order_number="ORDER-1",
        order_date=None,
        received_at=old,
        items=[_item()],
    )
    incoming = _incoming("ORDER-1")
    incoming["date"] = None
    assert _match([order], incoming) is None
