from datetime import date, timedelta

import pytest

from dz_fastapi.models.partner import (
    PriceList,
    PriceListAutoPartAssociation,
    ProviderPricelistReview,
)
from dz_fastapi.services.pricelist_guard import (
    PricelistAnomalyResult,
    _create_pricelist_review,
    _load_previous_price_map,
    build_review_examples,
    calculate_pricelist_anomaly,
)


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


@pytest.mark.asyncio
@pytest.mark.parametrize("old_status", ["pending", "queued"])
async def test_new_review_supersedes_older_waiting_review(
    test_session,
    created_providers,
    created_pricelist_config,
    tmp_path,
    monkeypatch,
    old_status,
):
    provider = created_providers[0]
    old_review = ProviderPricelistReview(
        provider_id=provider.id,
        provider_config_id=created_pricelist_config.id,
        source_filename="old.xlsx",
        file_path=str(tmp_path / "old.xlsx"),
        file_extension="xlsx",
        file_sha256="a" * 64,
        status=old_status,
        reasons=["Старое предупреждение"],
        metrics={},
        examples=[],
    )
    test_session.add(old_review)
    await test_session.commit()

    monkeypatch.setattr(
        "dz_fastapi.services.pricelist_guard.PRICELIST_REVIEW_DIR",
        str(tmp_path),
    )
    result = PricelistAnomalyResult(
        blocked=True,
        reasons=["Количество позиций изменилось на +30.0%."],
        metrics={"previous_pricelist_id": None},
    )
    new_review, created = await _create_pricelist_review(
        session=test_session,
        provider=provider,
        provider_config=created_pricelist_config,
        result=result,
        items=[
            {
                "brand": "BRAND",
                "oem_number": "NEW",
                "name": "New position",
                "quantity": 2,
                "price": 100,
            }
        ],
        previous_prices={},
        file_content=b"new pricelist payload",
        file_extension="xlsx",
        source_filename="new.xlsx",
    )
    await test_session.commit()
    await test_session.refresh(old_review)

    assert created is True
    assert new_review.status == "pending"
    assert old_review.status == "superseded"
    assert old_review.decided_at is not None
    assert old_review.decision_reason == (
        "Заменён более свежим прайсом этой конфигурации"
    )


@pytest.mark.asyncio
async def test_guard_uses_latest_published_id_as_next_baseline(
    test_session,
    created_providers,
    created_pricelist_config,
    created_autopart,
):
    provider = created_providers[0]
    automatic_pricelist = PriceList(
        provider_id=provider.id,
        provider_config_id=created_pricelist_config.id,
        date=date.today() + timedelta(days=1),
        is_active=True,
    )
    test_session.add(automatic_pricelist)
    await test_session.flush()
    test_session.add(
        PriceListAutoPartAssociation(
            pricelist_id=automatic_pricelist.id,
            autopart_id=created_autopart.id,
            quantity=5,
            price=100,
            multiplicity=1,
        )
    )

    approved_pricelist = PriceList(
        provider_id=provider.id,
        provider_config_id=created_pricelist_config.id,
        date=date.today(),
        is_active=True,
    )
    test_session.add(approved_pricelist)
    await test_session.flush()
    test_session.add(
        PriceListAutoPartAssociation(
            pricelist_id=approved_pricelist.id,
            autopart_id=created_autopart.id,
            quantity=7,
            price=125,
            multiplicity=1,
        )
    )

    # A failed/interrupted technical row must not replace the baseline.
    empty_pricelist = PriceList(
        provider_id=provider.id,
        provider_config_id=created_pricelist_config.id,
        date=date.today() + timedelta(days=2),
        is_active=True,
    )
    test_session.add(empty_pricelist)
    await test_session.commit()

    baseline_id, prices = await _load_previous_price_map(
        test_session,
        created_pricelist_config.id,
    )

    assert baseline_id == approved_pricelist.id
    assert prices == {("TEST BRAND", "E4G163611091"): 125.0}
