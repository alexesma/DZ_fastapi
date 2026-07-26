from dz_fastapi.services.pricelist_guard import build_review_examples, calculate_pricelist_anomaly


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


def test_review_examples_prefer_new_positions_and_distinct_brands():
    items = [
        {
            "brand": f"BRAND {index}",
            "oem_number": f"NEW{index}",
            "name": f"Position {index}",
            "quantity": index + 1,
            "price": 100 + index,
        }
        for index in range(12)
    ]
    items.extend(
        {
            "brand": "BRAND 0",
            "oem_number": f"EXTRA{index}",
            "name": "Duplicate brand",
            "quantity": 1,
            "price": 50,
        }
        for index in range(5)
    )

    examples = build_review_examples(items, {}, limit=10)

    assert len(examples) == 10
    assert len({row["brand"] for row in examples}) == 10
    assert all(row["change_type"] == "new" for row in examples)
