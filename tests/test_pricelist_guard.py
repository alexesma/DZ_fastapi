from dz_fastapi.services.pricelist_guard import calculate_pricelist_anomaly


def _prices(count: int, price: float = 100.0):
    return {
        ("BRAND", f"OEM{index}"): price
        for index in range(count)
    }


def test_pricelist_guard_accepts_small_normal_changes():
    previous = _prices(100)
    candidate = {
        key: price * (1.05 if index < 10 else 1.0)
        for index, (key, price) in enumerate(previous.items())
    }

    result = calculate_pricelist_anomaly(previous, candidate)

    assert result.blocked is False
    assert result.reasons == []


def test_pricelist_guard_blocks_row_count_drop_over_25_percent():
    result = calculate_pricelist_anomaly(_prices(100), _prices(70))

    assert result.blocked is True
    assert any("Количество позиций" in reason for reason in result.reasons)


def test_pricelist_guard_blocks_empty_candidate():
    result = calculate_pricelist_anomaly(_prices(100), {})

    assert result.blocked is True
    assert result.metrics["positions_change_percent"] == -100.0


def test_pricelist_guard_blocks_overlap_below_50_percent():
    previous = _prices(100)
    candidate = {
        ("OTHER", f"NEW{index}"): 100.0
        for index in range(100)
    }

    result = calculate_pricelist_anomaly(previous, candidate)

    assert result.blocked is True
    assert result.metrics["overlap_percent"] == 0


def test_pricelist_guard_blocks_median_price_change_over_10_percent():
    result = calculate_pricelist_anomaly(
        _prices(100, 100.0),
        _prices(100, 111.0),
    )

    assert result.blocked is True
    assert any("Медианная цена" in reason for reason in result.reasons)


def test_pricelist_guard_blocks_when_over_20_percent_change_over_10_percent():
    previous = _prices(100, 100.0)
    candidate = dict(previous)
    for index in range(21):
        candidate[("BRAND", f"OEM{index}")] = 120.0

    result = calculate_pricelist_anomaly(previous, candidate)

    assert result.blocked is True
    assert result.metrics["changed_items_percent"] == 21.0
    assert any("совпадающих позиций" in reason for reason in result.reasons)
