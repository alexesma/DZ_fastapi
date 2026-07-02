from dz_fastapi.services.autopurchase_feedback import (
    OUTCOME_ACCURATE,
    OUTCOME_NO_DEMAND,
    OUTCOME_OVERFORECAST,
    OUTCOME_STOCKOUT_AGAIN,
    OUTCOME_UNDERFORECAST,
    classify_forecast_outcome,
)


def test_no_demand_when_neither_forecast_nor_sales():
    outcome, error = classify_forecast_outcome(
        forecast_daily=0,
        actual_daily=0,
        current_quantity_at_eval=10,
    )
    assert outcome == OUTCOME_NO_DEMAND
    assert error is None


def test_overforecast_when_forecast_but_no_sales():
    outcome, error = classify_forecast_outcome(
        forecast_daily=2.0,
        actual_daily=0,
        current_quantity_at_eval=50,
    )
    assert outcome == OUTCOME_OVERFORECAST
    assert error is None


def test_accurate_within_tolerance():
    # Прогноз 1.2 при факте 1.0 — ошибка +20%, в пределах допуска 40%.
    outcome, error = classify_forecast_outcome(
        forecast_daily=1.2,
        actual_daily=1.0,
        current_quantity_at_eval=30,
    )
    assert outcome == OUTCOME_ACCURATE
    assert error == 20.0


def test_overforecast_above_tolerance():
    # Прогноз 2.0 при факте 1.0 — ошибка +100%.
    outcome, error = classify_forecast_outcome(
        forecast_daily=2.0,
        actual_daily=1.0,
        current_quantity_at_eval=30,
    )
    assert outcome == OUTCOME_OVERFORECAST
    assert error == 100.0


def test_underforecast_below_tolerance():
    # Прогноз 0.4 при факте 1.0 — ошибка −60%.
    outcome, error = classify_forecast_outcome(
        forecast_daily=0.4,
        actual_daily=1.0,
        current_quantity_at_eval=30,
    )
    assert outcome == OUTCOME_UNDERFORECAST
    assert error == -60.0


def test_stockout_again_beats_accuracy():
    # Спрос был, остаток снова ноль — потерянные продажи важнее
    # численной точности прогноза.
    outcome, error = classify_forecast_outcome(
        forecast_daily=1.0,
        actual_daily=1.0,
        current_quantity_at_eval=0,
    )
    assert outcome == OUTCOME_STOCKOUT_AGAIN
    assert error == 0.0
