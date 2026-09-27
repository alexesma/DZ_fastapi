from datetime import timedelta
from decimal import Decimal
from zoneinfo import ZoneInfo

import pytest
from sqlalchemy import select

from dz_fastapi.analytics.turnover import refresh_turnover_summary
from dz_fastapi.core.time import now_moscow
from dz_fastapi.models.autopart import AutoPartPriceHistory, AutoPartTurnoverSummary
from dz_fastapi.models.partner import (
    CustomerOrder,
    CustomerOrderItem,
    PriceList,
    PriceListAutoPartAssociation,
    Provider,
    ProviderPriceListConfig,
)

TZ = ZoneInfo("Europe/Moscow")


async def _make_offer(session, autopart_id, price, quantity, suffix):
    provider = Provider(
        name=f"Provider {suffix}",
        email_contact=f"provider-{suffix}@example.com",
        email_incoming_price=f"price-{suffix}@example.com",
        type_prices="Retail",
    )
    session.add(provider)
    await session.flush()
    provider_id = provider.id
    config = ProviderPriceListConfig(
        provider_id=provider.id,
        start_row=1,
        oem_col=0,
        brand_col=1,
        name_col=2,
        qty_col=3,
        price_col=4,
    )
    session.add(config)
    await session.flush()
    pricelist = PriceList(
        date=now_moscow().date(),
        provider_id=provider.id,
        provider_config_id=config.id,
        is_active=True,
    )
    session.add(pricelist)
    await session.flush()
    session.add(
        PriceListAutoPartAssociation(
            pricelist_id=pricelist.id,
            autopart_id=autopart_id,
            quantity=quantity,
            price=Decimal(str(price)),
            multiplicity=1,
        )
    )
    return provider_id


@pytest.mark.asyncio
async def test_refresh_turnover_summary_computes_sold_qty_and_prices(
    test_session, created_autopart, created_customers
):
    now = now_moscow()

    order_recent = CustomerOrder(
        customer_id=created_customers[0].id,
        received_at=now - timedelta(days=5),
    )
    order_old_but_within_90 = CustomerOrder(
        customer_id=created_customers[0].id,
        received_at=now - timedelta(days=60),
    )
    order_too_old = CustomerOrder(
        customer_id=created_customers[0].id,
        received_at=now - timedelta(days=200),
    )
    order_deleted = CustomerOrder(
        customer_id=created_customers[0].id,
        received_at=now - timedelta(days=2),
        deleted_at=now - timedelta(days=1),
    )
    test_session.add_all([order_recent, order_old_but_within_90, order_too_old, order_deleted])
    await test_session.flush()

    test_session.add_all(
        [
            CustomerOrderItem(
                order_id=order_recent.id,
                oem=created_autopart.oem_number,
                brand=created_autopart.brand.name,
                autopart_id=created_autopart.id,
                requested_qty=10,
                ship_qty=8,
                status="OWN_STOCK",
            ),
            CustomerOrderItem(
                order_id=order_old_but_within_90.id,
                oem=created_autopart.oem_number,
                brand=created_autopart.brand.name,
                autopart_id=created_autopart.id,
                requested_qty=5,
                ship_qty=5,
                status="SUPPLIER",
            ),
            # За пределами 90 дней — не должно попасть ни в 30d, ни в 90d.
            CustomerOrderItem(
                order_id=order_too_old.id,
                oem=created_autopart.oem_number,
                brand=created_autopart.brand.name,
                autopart_id=created_autopart.id,
                requested_qty=100,
                ship_qty=100,
                status="OWN_STOCK",
            ),
            # Отказ — не продажа, не должен считаться.
            CustomerOrderItem(
                order_id=order_recent.id,
                oem=created_autopart.oem_number,
                brand=created_autopart.brand.name,
                autopart_id=created_autopart.id,
                requested_qty=50,
                ship_qty=50,
                status="REJECTED",
            ),
            # Удалённый заказ не должен завышать спрос.
            CustomerOrderItem(
                order_id=order_deleted.id,
                oem=created_autopart.oem_number,
                brand=created_autopart.brand.name,
                autopart_id=created_autopart.id,
                requested_qty=200,
                ship_qty=200,
                status="OWN_STOCK",
            ),
        ]
    )

    provider_a = await _make_offer(test_session, created_autopart.id, "100.00", 10, "a")
    await _make_offer(test_session, created_autopart.id, "120.00", 5, "b")

    # Старый сохранённый прайс той же конфигурации не является текущей ценой.
    config_a = (
        await test_session.execute(
            select(ProviderPriceListConfig).where(ProviderPriceListConfig.provider_id == provider_a)
        )
    ).scalar_one()
    old_pricelist = PriceList(
        date=(now - timedelta(days=30)).date(),
        provider_id=provider_a,
        provider_config_id=config_a.id,
        is_active=True,
    )
    test_session.add(old_pricelist)
    await test_session.flush()
    test_session.add(
        PriceListAutoPartAssociation(
            pricelist_id=old_pricelist.id,
            autopart_id=created_autopart.id,
            quantity=999,
            price=Decimal("1.00"),
            multiplicity=1,
        )
    )

    # Остаток у поставщика тает: три снимка с уменьшающимся количеством.
    test_session.add_all(
        [
            AutoPartPriceHistory(
                autopart_id=created_autopart.id,
                provider_id=provider_a,
                pricelist_id=1,
                created_at=now - timedelta(days=20),
                price=Decimal("100.00"),
                quantity=30,
            ),
            AutoPartPriceHistory(
                autopart_id=created_autopart.id,
                provider_id=provider_a,
                pricelist_id=1,
                created_at=now - timedelta(days=10),
                price=Decimal("100.00"),
                quantity=15,
            ),
            AutoPartPriceHistory(
                autopart_id=created_autopart.id,
                provider_id=provider_a,
                pricelist_id=1,
                created_at=now - timedelta(days=1),
                price=Decimal("100.00"),
                quantity=5,
            ),
        ]
    )
    await test_session.commit()

    await refresh_turnover_summary(test_session)

    summary = (
        await test_session.execute(
            select(AutoPartTurnoverSummary).where(
                AutoPartTurnoverSummary.autopart_id == created_autopart.id
            )
        )
    ).scalar_one()

    assert summary.sold_qty_30d == 8
    assert summary.sold_qty_90d == 13
    assert summary.supplier_count == 2
    assert summary.min_purchase_price == Decimal("100.00")
    assert summary.supplier_qty_trend_30d < 0


@pytest.mark.asyncio
async def test_refresh_turnover_uses_each_supplier_trend_independently(
    test_session, created_autopart
):
    now = now_moscow()
    provider_a = await _make_offer(test_session, created_autopart.id, "100.00", 100, "stable-a")
    provider_b = await _make_offer(test_session, created_autopart.id, "110.00", 10, "stable-b")
    for provider_id, quantity, days in (
        (provider_a, 100, (29, 25, 21)),
        (provider_b, 10, (9, 5, 1)),
    ):
        for days_ago in days:
            test_session.add(
                AutoPartPriceHistory(
                    autopart_id=created_autopart.id,
                    provider_id=provider_id,
                    pricelist_id=provider_id,
                    created_at=now - timedelta(days=days_ago),
                    price=Decimal("100.00"),
                    quantity=quantity,
                )
            )
    await test_session.commit()

    await refresh_turnover_summary(test_session)
    summary = (
        await test_session.execute(
            select(AutoPartTurnoverSummary).where(
                AutoPartTurnoverSummary.autopart_id == created_autopart.id
            )
        )
    ).scalar_one()

    assert abs(summary.supplier_qty_trend_30d or 0) < 0.001
    assert summary.trend_supplier_count == 2
    assert summary.declining_supplier_count == 0


@pytest.mark.asyncio
async def test_refresh_turnover_clears_stale_top_flag(test_session, created_autopart):
    await _make_offer(test_session, created_autopart.id, "100.00", 10, "current")
    test_session.add(
        AutoPartTurnoverSummary(
            autopart_id=created_autopart.id,
            sold_qty_30d=50,
            daily_velocity_30d=2,
            turnover_percentile=0.99,
            recommendation_score=99,
            is_top=True,
        )
    )
    await test_session.commit()

    await refresh_turnover_summary(test_session)
    summary = (
        await test_session.execute(
            select(AutoPartTurnoverSummary).where(
                AutoPartTurnoverSummary.autopart_id == created_autopart.id
            )
        )
    ).scalar_one()

    assert summary.sold_qty_30d == 0
    assert summary.is_top is False
