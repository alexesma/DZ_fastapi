from datetime import timedelta
from types import SimpleNamespace

import pytest

from dz_fastapi.api.dashboard import _build_supplier_reliability, get_order_dynamics
from dz_fastapi.core.time import now_moscow
from dz_fastapi.models.partner import (
    CustomerOrder,
    CustomerOrderItem,
    Order,
    OrderItem,
    SupplierOrder,
    SupplierOrderItem,
)


@pytest.mark.asyncio
async def test_order_dynamics_aggregates_daily_and_partner_totals(
    test_session,
    created_customers,
    created_providers,
):
    created_at = now_moscow() - timedelta(days=1)
    customer_order = CustomerOrder(
        customer_id=created_customers[0].id,
        received_at=created_at,
    )
    supplier_order = SupplierOrder(
        provider_id=created_providers[0].id,
        created_at=created_at,
    )
    site_order = Order(
        provider_id=created_providers[0].id,
        customer_id=created_customers[0].id,
        created_at=created_at,
    )
    test_session.add_all([customer_order, supplier_order, site_order])
    await test_session.flush()

    test_session.add_all(
        [
            CustomerOrderItem(
                order_id=customer_order.id,
                oem="OEM-1",
                brand="BRAND",
                requested_qty=2,
                requested_price=100,
            ),
            CustomerOrderItem(
                order_id=customer_order.id,
                oem="OEM-2",
                brand="BRAND",
                requested_qty=3,
                matched_price=150,
            ),
            SupplierOrderItem(
                supplier_order_id=supplier_order.id,
                oem_number="OEM-1",
                quantity=4,
                price=80,
            ),
            OrderItem(
                order_id=site_order.id,
                oem_number="OEM-SITE",
                quantity=6,
                price=50,
            ),
        ]
    )
    await test_session.commit()

    result = await get_order_dynamics(
        days=14,
        partner_limit=10,
        session=test_session,
    )

    assert result["summary"] == {
        "customer_order_count": 1,
        "customer_qty": 5,
        "customer_sum": 650.0,
        "supplier_order_count": 2,
        "supplier_qty": 10,
        "supplier_sum": 620.0,
        "purchase_coverage_pct": 200.0,
    }
    populated_day = next(
        row for row in result["daily"] if row["customer_order_count"]
    )
    assert populated_day["customer_position_count"] == 2
    assert populated_day["supplier_position_count"] == 2
    assert result["customers"][0]["partner_name"] == created_customers[0].name
    assert result["suppliers"][0]["partner_name"] == created_providers[0].name
    assert result["suppliers"][0]["order_count"] == 2
    assert result["suppliers"][0]["total_sum"] == 620.0


def test_supplier_reliability_excludes_not_due_lines_from_rating():
    generated_at = now_moscow()
    created_at = generated_at - timedelta(days=10)
    rows = [
        SimpleNamespace(
            order_id=1,
            provider_id=10,
            provider_name="Reliable Parts",
            created_at=created_at,
            quantity=10,
            price=100,
            received_quantity=8,
            received_at=created_at + timedelta(days=4),
            max_delivery_day=7,
        ),
        SimpleNamespace(
            order_id=1,
            provider_id=10,
            provider_name="Reliable Parts",
            created_at=created_at,
            quantity=5,
            price=100,
            received_quantity=0,
            received_at=None,
            max_delivery_day=3,
        ),
        SimpleNamespace(
            order_id=2,
            provider_id=10,
            provider_name="Reliable Parts",
            created_at=generated_at - timedelta(days=1),
            quantity=4,
            price=100,
            received_quantity=0,
            received_at=None,
            max_delivery_day=5,
        ),
    ]

    result = _build_supplier_reliability(
        rows,
        generated_at=generated_at,
    )[0]

    assert result["order_count"] == 2
    assert result["line_count"] == 3
    assert result["evaluated_line_count"] == 2
    assert result["ordered_qty"] == 19
    assert result["evaluated_qty"] == 15
    assert result["received_qty"] == 8
    assert result["pending_qty"] == 11
    assert result["ordered_sum"] == 1900.0
    assert result["evaluated_sum"] == 1500.0
    assert result["received_sum"] == 800.0
    assert result["pending_sum"] == 1100.0
    assert result["fill_rate_pct"] == 53.3
    assert result["on_time_pct"] == 50.0
    assert result["late_line_count"] == 1
    assert result["avg_lead_days"] == 4.0
