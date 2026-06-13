from dz_fastapi.services.inventory_dashboard import (
    INVENTORY_STATE_DEAD,
    INVENTORY_STATE_HEALTHY,
    INVENTORY_STATE_OOS_DEMAND,
    INVENTORY_STATE_OOS_NO_DEMAND,
    INVENTORY_STATE_OVERSTOCK,
    INVENTORY_STATE_SLOW,
    INVENTORY_STATE_URGENT,
    classify_inventory_state,
)


def test_classify_out_of_stock_with_demand():
    assert (
        classify_inventory_state(
            current_quantity=0,
            avg_daily=0.5,
            estimated_days_left=None,
            sold_last_365_days=40,
        )
        == INVENTORY_STATE_OOS_DEMAND
    )


def test_classify_out_of_stock_no_demand_is_neutral():
    assert (
        classify_inventory_state(
            current_quantity=0,
            avg_daily=None,
            estimated_days_left=None,
            sold_last_365_days=0,
        )
        == INVENTORY_STATE_OOS_NO_DEMAND
    )


def test_classify_dead_stock_when_stock_but_no_demand():
    # Лежит 20 шт, продаж нет вообще — замороженные деньги.
    assert (
        classify_inventory_state(
            current_quantity=20,
            avg_daily=None,
            estimated_days_left=None,
            sold_last_365_days=0,
        )
        == INVENTORY_STATE_DEAD
    )


def test_classify_urgent_when_low_days_left():
    assert (
        classify_inventory_state(
            current_quantity=5,
            avg_daily=1.0,
            estimated_days_left=5,
            sold_last_365_days=300,
        )
        == INVENTORY_STATE_URGENT
    )


def test_classify_healthy_slow_overstock_by_cover():
    common = dict(current_quantity=100, avg_daily=1.0, sold_last_365_days=300)
    assert (
        classify_inventory_state(estimated_days_left=40, **common)
        == INVENTORY_STATE_HEALTHY
    )
    assert (
        classify_inventory_state(estimated_days_left=120, **common)
        == INVENTORY_STATE_SLOW
    )
    assert (
        classify_inventory_state(estimated_days_left=400, **common)
        == INVENTORY_STATE_OVERSTOCK
    )
