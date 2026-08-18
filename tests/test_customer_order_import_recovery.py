import pytest

from dz_fastapi.models.partner import CUSTOMER_ORDER_STATUS, CustomerOrder, CustomerOrderItem
from dz_fastapi.services.customer_orders import _is_resumable_import_stub


@pytest.mark.asyncio
async def test_new_email_import_without_items_is_resumable(
    test_session,
    created_customers,
):
    order = CustomerOrder(
        customer_id=created_customers[0].id,
        status=CUSTOMER_ORDER_STATUS.NEW,
        source_filename="order.xlsx",
        file_hash="a" * 64,
    )
    test_session.add(order)
    await test_session.flush()

    assert await _is_resumable_import_stub(test_session, order) is True


@pytest.mark.asyncio
async def test_new_import_with_items_is_not_resumable(
    test_session,
    created_customers,
):
    order = CustomerOrder(
        customer_id=created_customers[0].id,
        status=CUSTOMER_ORDER_STATUS.NEW,
        source_filename="order.xlsx",
        file_hash="b" * 64,
    )
    test_session.add(order)
    await test_session.flush()
    test_session.add(
        CustomerOrderItem(
            order_id=order.id,
            row_index=1,
            oem="TEST-1",
            brand="TEST",
            requested_qty=1,
        )
    )
    await test_session.flush()

    assert await _is_resumable_import_stub(test_session, order) is False
