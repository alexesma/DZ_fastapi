"""Эндпоинты сводки оборота: тултип в поиске и отчёт «хорошая оборачиваемость»."""

import pytest

from dz_fastapi.models.autopart import AutoPartTurnoverSummary


@pytest.mark.asyncio
async def test_turnover_summary_returns_null_when_no_data(async_client, created_autopart):
    response = await async_client.get(f"/autoparts/{created_autopart.id}/turnover-summary/")
    assert response.status_code == 200
    assert response.json() is None


@pytest.mark.asyncio
async def test_turnover_summary_returns_stored_row(async_client, test_session, created_autopart):
    test_session.add(
        AutoPartTurnoverSummary(
            autopart_id=created_autopart.id,
            sold_qty_30d=12,
            sold_qty_90d=30,
            daily_velocity_30d=0.4,
            daily_velocity_90d=0.33,
            supplier_count=3,
            min_purchase_price=100,
            avg_purchase_price=120,
            supplier_qty_trend_30d=-1.2,
            turnover_percentile=0.95,
            is_top=True,
        )
    )
    await test_session.commit()

    response = await async_client.get(f"/autoparts/{created_autopart.id}/turnover-summary/")
    assert response.status_code == 200
    body = response.json()
    assert body["sold_qty_30d"] == 12
    assert body["is_top"] is True
    assert body["min_purchase_price"] == 100.0


@pytest.mark.asyncio
async def test_turnover_report_filters_by_top_and_brand(async_client, test_session, created_brand):
    from dz_fastapi.models.autopart import AutoPart

    top_part = AutoPart(brand_id=created_brand.id, oem_number="TOP1", name="Топ деталь")
    other_part = AutoPart(brand_id=created_brand.id, oem_number="OTH1", name="Другая деталь")
    test_session.add_all([top_part, other_part])
    await test_session.flush()
    test_session.add_all(
        [
            AutoPartTurnoverSummary(
                autopart_id=top_part.id,
                daily_velocity_30d=1.0,
                turnover_percentile=0.95,
                is_top=True,
            ),
            AutoPartTurnoverSummary(
                autopart_id=other_part.id,
                daily_velocity_30d=0.1,
                turnover_percentile=0.2,
                is_top=False,
            ),
        ]
    )
    await test_session.commit()

    response = await async_client.get("/autoparts/turnover-report/", params={"only_top": True})
    assert response.status_code == 200
    body = response.json()
    ids = [row["autopart_id"] for row in body["items"]]
    assert body["total"] == 1
    assert top_part.id in ids
    assert other_part.id not in ids


@pytest.mark.asyncio
async def test_turnover_report_includes_market_signal_without_own_demand(
    async_client, test_session, created_brand
):
    from dz_fastapi.models.autopart import AutoPart

    market_part = AutoPart(
        brand_id=created_brand.id,
        oem_number="MARKET1",
        name="Рыночная позиция",
    )
    test_session.add(market_part)
    await test_session.flush()
    test_session.add(
        AutoPartTurnoverSummary(
            autopart_id=market_part.id,
            daily_velocity_30d=0,
            supplier_count=3,
            declining_supplier_count=2,
            market_score=80,
            recommendation_score=70,
            is_market_opportunity=True,
            turnover_percentile=None,
        )
    )
    await test_session.commit()

    response = await async_client.get("/autoparts/turnover-report/", params={"signal": "market"})
    assert response.status_code == 200
    body = response.json()
    assert market_part.id in [row["autopart_id"] for row in body["items"]]


@pytest.mark.asyncio
async def test_catalog_can_filter_and_mark_turnover_top(
    async_client, test_session, created_autopart
):
    test_session.add(
        AutoPartTurnoverSummary(
            autopart_id=created_autopart.id,
            recommendation_score=88,
            recommended_order_qty=4,
            is_top=True,
        )
    )
    await test_session.commit()

    response = await async_client.get("/autoparts/catalog/", params={"turnover": "top"})
    assert response.status_code == 200
    rows = response.json()["items"]
    row = next(item for item in rows if item["id"] == created_autopart.id)
    assert row["is_turnover_top"] is True
    assert row["turnover_score"] == 88
    assert row["recommended_order_qty"] == 4


@pytest.mark.asyncio
async def test_turnover_flags_return_only_flagged_parts(async_client, test_session, created_brand):
    from dz_fastapi.models.autopart import AutoPart

    top_part = AutoPart(brand_id=created_brand.id, oem_number="FLG1", name="Топ")
    plain_part = AutoPart(brand_id=created_brand.id, oem_number="FLG2", name="Обычная")
    test_session.add_all([top_part, plain_part])
    await test_session.flush()
    test_session.add_all(
        [
            AutoPartTurnoverSummary(autopart_id=top_part.id, is_top=True),
            AutoPartTurnoverSummary(autopart_id=plain_part.id, is_top=False),
        ]
    )
    await test_session.commit()

    response = await async_client.post(
        "/autoparts/turnover-flags/", json={"ids": [top_part.id, plain_part.id]}
    )
    assert response.status_code == 200
    flags = response.json()["flags"]
    assert [f["autopart_id"] for f in flags] == [top_part.id]
    assert flags[0]["is_top"] is True
