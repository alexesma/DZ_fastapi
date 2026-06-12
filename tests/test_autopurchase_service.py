from datetime import date, timedelta

from dz_fastapi.core.time import now_moscow
from dz_fastapi.services.autopurchase import (
    _apply_recovery_mode,
    _blend_average_daily_horizons,
    _brand_matches_allowed,
    _build_autopurchase_draft,
    _calculate_in_stock_days,
    _compute_availability_adjusted_daily,
    _estimate_consecutive_stockout_days,
    _get_cross_brand_priority,
    _get_target_cover_days,
    _is_dragonzap_brand,
    _normalize_brand_key,
    _plan_auto_allocations,
    _round_down_to_lot,
    _select_autopurchase_supplier,
    _select_best_site_supplier_by_lead_time,
    _select_best_site_supplier_by_price,
)


def test_select_best_site_supplier_by_price_ignores_non_positive_qty():
    supplier = _select_best_site_supplier_by_price(
        [
            {
                "provider_name": "BadStock",
                "current_price": 10.0,
                "current_qty": -1,
                "effective_lead_days": 1,
            },
            {
                "provider_name": "GoodStock",
                "current_price": 15.0,
                "current_qty": 5,
                "effective_lead_days": 2,
            },
        ]
    )

    assert supplier is not None
    assert supplier["provider_name"] == "GoodStock"


def test_select_best_site_supplier_by_lead_time_returns_none_without_positive_qty():
    supplier = _select_best_site_supplier_by_lead_time(
        [
            {
                "provider_name": "ZeroStock",
                "current_price": 10.0,
                "current_qty": 0,
                "effective_lead_days": 1,
            },
            {
                "provider_name": "NegativeStock",
                "current_price": 8.0,
                "current_qty": -1,
                "effective_lead_days": 2,
            },
        ]
    )

    assert supplier is None


def test_select_autopurchase_supplier_ignores_non_positive_qty_even_if_cheapest():
    supplier = _select_autopurchase_supplier(
        [
            {
                "provider_name": "NoStock",
                "current_price": 1.0,
                "current_qty": -1,
                "effective_lead_days": 1,
                "fill_rate": 100.0,
                "is_own_price": False,
            },
            {
                "provider_name": "Available",
                "current_price": 5.0,
                "current_qty": 3,
                "effective_lead_days": 4,
                "fill_rate": None,
                "is_own_price": False,
            },
        ],
        fill_rate_threshold=80.0,
        max_allowed_lead_days=7,
    )

    assert supplier is not None
    assert supplier["provider_name"] == "Available"


def test_build_autopurchase_draft_returns_none_for_zero_supplier_qty():
    draft = _build_autopurchase_draft(
        supplier={
            "provider_name": "Supplier",
            "current_price": 100.0,
            "current_qty": 0,
        },
        available_qty=0,
        in_transit_qty=0,
        target_qty=5,
        recommended_qty=5,
        lead_time_days_used=7.0,
        reason=None,
    )

    assert draft is None


def test_blend_average_daily_horizons_uses_long_windows_when_present():
    blended = _blend_average_daily_horizons(0.5, 0.4, 0.3, 0.2)

    assert blended == 0.41


def test_estimate_consecutive_stockout_days_counts_latest_zero_streak():
    stockout_days = _estimate_consecutive_stockout_days(
        [
            {
                "pricelist_date": date(2026, 5, 1),
                "qty_by_oem": {"OEM1": 3},
            },
            {
                "pricelist_date": date(2026, 5, 10),
                "qty_by_oem": {"OEM1": 0},
            },
            {
                "pricelist_date": date(2026, 5, 20),
                "qty_by_oem": {"OEM1": 0},
            },
        ],
        oem_number="OEM1",
    )

    assert stockout_days == 10


def test_apply_recovery_mode_raises_floor_for_long_stockout():
    avg_daily, applied = _apply_recovery_mode(
        current_quantity=0,
        consecutive_stockout_days=90,
        avg_daily_planning=0.02,
        avg_daily_180=0.08,
        avg_daily_365=0.05,
    )

    assert applied is True
    assert avg_daily == 0.08


def test_normalize_brand_key_strips_non_alnum_and_uppercases():
    assert _normalize_brand_key(" Lynx Auto ") == "LYNXAUTO"
    assert _normalize_brand_key("DragonZap") == "DRAGONZAP"
    assert _normalize_brand_key(None) == ""


def test_is_dragonzap_brand_tolerates_spelling():
    assert _is_dragonzap_brand("Dragonzap") is True
    assert _is_dragonzap_brand(" DRAGON ZAP ") is True
    assert _is_dragonzap_brand("Lynx") is False


def test_brand_matches_allowed_strict_for_other_brand():
    allowed = {_normalize_brand_key("HYUNDAI/KIA")}

    assert _brand_matches_allowed("Hyundai/Kia", allowed) is True
    # Бренд сайта может быть шире/уже по написанию — допускаем вхождение.
    assert _brand_matches_allowed("HYUNDAI", allowed) is True
    # Чужой бренд по тому же OEM должен отфильтровываться.
    assert _brand_matches_allowed("PMC", allowed) is False
    # Пустой make_name — доверяем фильтру самого сайта по make_name.
    assert _brand_matches_allowed("", allowed) is True


def test_build_autopurchase_draft_bumps_qty_to_supplier_min_lot():
    draft = _build_autopurchase_draft(
        supplier={
            "provider_name": "Supplier",
            "current_price": 100.0,
            "current_qty": 10,
            "current_min_qnt": 5,
        },
        available_qty=0,
        in_transit_qty=0,
        target_qty=2,
        recommended_qty=2,
        lead_time_days_used=7.0,
        reason=None,
    )

    assert draft is not None
    assert draft["proposed_order_qty"] == 5
    assert draft["remaining_gap_qty"] == 0


def test_build_autopurchase_draft_returns_none_when_min_lot_unreachable():
    draft = _build_autopurchase_draft(
        supplier={
            "provider_name": "Supplier",
            "current_price": 100.0,
            "current_qty": 3,
            "current_min_qnt": 5,
        },
        available_qty=0,
        in_transit_qty=0,
        target_qty=2,
        recommended_qty=2,
        lead_time_days_used=7.0,
        reason=None,
    )

    assert draft is None


def test_select_autopurchase_supplier_respects_purchase_price_cap():
    stats = [
        {
            "provider_name": "Expensive",
            "current_price": 95.0,
            "current_qty": 10,
            "effective_lead_days": 1,
            "fill_rate": 100.0,
            "is_own_price": False,
        },
        {
            "provider_name": "Cheap",
            "current_price": 60.0,
            "current_qty": 5,
            "effective_lead_days": 5,
            "fill_rate": 100.0,
            "is_own_price": False,
        },
    ]

    # Потолок закупки 90 руб: дорогой поставщик отфильтрован.
    supplier = _select_autopurchase_supplier(
        stats,
        fill_rate_threshold=80.0,
        max_allowed_lead_days=None,
        max_allowed_price=90.0,
    )
    assert supplier is not None
    assert supplier["provider_name"] == "Cheap"

    # Потолок ниже всех предложений — поставщика нет вовсе.
    supplier = _select_autopurchase_supplier(
        stats,
        fill_rate_threshold=80.0,
        max_allowed_lead_days=None,
        max_allowed_price=50.0,
    )
    assert supplier is None


def test_get_cross_brand_priority_prefers_dragonzap_donor_brands():
    assert _get_cross_brand_priority("CHERY") == 0
    assert _get_cross_brand_priority("Chery Automobile") == 0
    assert _get_cross_brand_priority("JAC") < _get_cross_brand_priority(
        "TOYOTA"
    )
    # Не из списка предпочтительных — в конец.
    assert _get_cross_brand_priority("BOSCH") == _get_cross_brand_priority(
        "TOYOTA"
    )


def test_build_autopurchase_draft_rounds_up_to_supplier_lot():
    # Пример из бизнес-правила: нужно 57, партия поставщика 20 → заказ 60.
    draft = _build_autopurchase_draft(
        supplier={
            "provider_name": "Supplier",
            "current_price": 100.0,
            "current_qty": 200,
            "current_min_qnt": 20,
        },
        available_qty=0,
        in_transit_qty=0,
        target_qty=57,
        recommended_qty=57,
        lead_time_days_used=7.0,
        reason=None,
    )

    assert draft is not None
    assert draft["proposed_order_qty"] == 60
    assert draft["remaining_gap_qty"] == 0


def test_build_autopurchase_draft_caps_lot_by_supplier_stock():
    # Нужно 57 (3 партии = 60), но у поставщика только 50 → 2 партии = 40.
    draft = _build_autopurchase_draft(
        supplier={
            "provider_name": "Supplier",
            "current_price": 100.0,
            "current_qty": 50,
            "current_min_qnt": 20,
        },
        available_qty=0,
        in_transit_qty=0,
        target_qty=57,
        recommended_qty=57,
        lead_time_days_used=7.0,
        reason=None,
    )

    assert draft is not None
    assert draft["proposed_order_qty"] == 40
    assert draft["remaining_gap_qty"] == 17


def test_round_down_to_lot():
    assert _round_down_to_lot(50, 20) == 40
    assert _round_down_to_lot(19, 20) == 0
    assert _round_down_to_lot(57, 1) == 57


def test_plan_auto_allocations_splits_across_suppliers_with_lots():
    offers = [
        {
            "provider_name": "Cheap",
            "current_price": 100.0,
            "current_qty": 25,
            "current_min_qnt": 10,
            "is_own_price": False,
        },
        {
            "provider_name": "Second",
            "current_price": 120.0,
            "current_qty": 100,
            "current_min_qnt": 20,
            "is_own_price": False,
        },
    ]

    allocations, covered = _plan_auto_allocations(offers, needed_qty=57)

    # Cheap: 2 целых партии по 10 = 20 шт; остаток 37 → Second: 2 партии
    # по 20 = 40 шт (округление вверх).
    assert [a["quantity"] for a in allocations] == [20, 40]
    assert covered == 60


def test_plan_auto_allocations_respects_price_cap():
    offers = [
        {
            "provider_name": "TooExpensive",
            "current_price": 200.0,
            "current_qty": 100,
            "current_min_qnt": 1,
            "is_own_price": False,
        },
        {
            "provider_name": "Ok",
            "current_price": 90.0,
            "current_qty": 100,
            "current_min_qnt": 1,
            "is_own_price": False,
        },
    ]

    allocations, covered = _plan_auto_allocations(
        offers,
        needed_qty=10,
        max_allowed_price=100.0,
    )

    assert len(allocations) == 1
    assert allocations[0]["provider_name"] == "Ok"
    assert covered == 10


def test_compute_availability_adjusted_daily_uses_in_stock_days():
    # 10 продаж за 30 дней, наличие 10 дней: поправка даёт 1.0 шт/день,
    # доверие 10/14 → 0.33 + (1.0 − 0.33) × 0.714 ≈ 0.81.
    assert _compute_availability_adjusted_daily(10, 30, 10) == 0.81
    # Полное наличие — обычная календарная средняя.
    assert _compute_availability_adjusted_daily(30, 30, 30) == 1.0
    # Один день наличия: доверие 1/14 → почти календарная средняя
    # (0.47 + (2.0 − 0.47) × 0.071 ≈ 0.58), потолок ×3 не достигнут.
    assert _compute_availability_adjusted_daily(14, 30, 1) == 0.58
    # Потолок ×3: 14 дней наличия из 90 — полное доверие поправке
    # (98/14 = 7.0), но не больше календарной ×3 (98/90 × 3 ≈ 3.27).
    assert _compute_availability_adjusted_daily(98, 90, 14) == 3.27
    assert _compute_availability_adjusted_daily(0, 30, 10) is None


def test_calculate_in_stock_days_counts_only_positive_spans():
    today = now_moscow().date()
    snapshots = [
        {
            "pricelist_date": today - timedelta(days=20),
            "qty_by_oem": {"OEM1": 5},
        },
        {
            "pricelist_date": today - timedelta(days=10),
            "qty_by_oem": {"OEM1": 0},
        },
        {
            "pricelist_date": today - timedelta(days=5),
            "qty_by_oem": {"OEM1": 3},
        },
    ]

    in_stock = _calculate_in_stock_days(snapshots, ["OEM1"], days=30)

    # 10 дней (20→10) товар был, 5 дней (10→5) не было,
    # 5 дней хвоста (5→сегодня) был.
    assert in_stock["OEM1"] == 15


def test_get_target_cover_days_differentiated_by_abc():
    assert _get_target_cover_days("A") == 45
    assert _get_target_cover_days("B") == 30
    assert _get_target_cover_days("C") == 21
    # Без класса — полные 1,5 месяца.
    assert _get_target_cover_days(None) == 45


def test_build_autopurchase_draft_keeps_backlog_qty():
    draft = _build_autopurchase_draft(
        supplier={
            "provider_name": "Supplier",
            "current_price": 100.0,
            "current_qty": 10,
        },
        available_qty=1,
        in_transit_qty=0,
        target_qty=5,
        recommended_qty=5,
        lead_time_days_used=7.0,
        reason=None,
        open_customer_backlog_qty=4,
    )

    assert draft is not None
    assert draft["open_customer_backlog_qty"] == 4
