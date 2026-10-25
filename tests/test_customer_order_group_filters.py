"""Order lists use the same wholesale/retail grouping as the customer list."""

import pytest

from dz_fastapi.models.partner import TYPE_PRICES, Customer, CustomerOrder


@pytest.mark.asyncio
async def test_order_lists_filter_customer_groups(
    async_client, test_session, created_customers,
):
    cash_customer = Customer(name="Cash order customer", type_prices=TYPE_PRICES.CASH)
    test_session.add(cash_customer)
    await test_session.flush()
    customer_ids = [customer.id for customer in created_customers] + [cash_customer.id]
    orders = [
        CustomerOrder(customer_id=customer_id, status="PROCESSED")
        for customer_id in customer_ids
    ]
    test_session.add_all(orders)
    await test_session.commit()

    for group, expected_customers in (
        ("wholesale", {customer_ids[0]}),
        ("retail", {customer_ids[1], customer_ids[2]}),
        (None, set(customer_ids)),
    ):
        params = {"customer_group": group} if group else {}
        for path in (
            "/customer-orders/",
            "/customer-orders/summary-page",
            "/customer-orders/assembly",
        ):
            response = await async_client.get(path, params=params)
            assert response.status_code == 200, response.text
            payload = response.json()
            rows = payload["items"] if path.endswith("summary-page") else payload
            assert {row["customer_id"] for row in rows} == expected_customers
            if path.endswith("summary-page"):
                assert payload["total"] == len(expected_customers)

        count = await async_client.get("/customer-orders/count", params=params)
        assert count.status_code == 200, count.text
        assert count.json() == len(expected_customers)
