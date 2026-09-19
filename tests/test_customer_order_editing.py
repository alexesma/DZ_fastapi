from datetime import date

import pytest

from dz_fastapi.crud.customer_order import crud_customer_order
from dz_fastapi.models.partner import CUSTOMER_ORDER_STATUS, Customer, CustomerOrder
from dz_fastapi.services.customer_orders import soft_delete_customer_order, update_customer_order


@pytest.mark.asyncio
async def test_customer_order_header_can_be_edited(test_session):
    customer = Customer(name="Первый клиент")
    replacement = Customer(name="Новый клиент")
    test_session.add_all([customer, replacement])
    await test_session.flush()
    order = CustomerOrder(
        customer_id=customer.id,
        status=CUSTOMER_ORDER_STATUS.NEW,
        order_number="OLD-1",
    )
    test_session.add(order)
    await test_session.commit()

    updated = await update_customer_order(
        test_session,
        order.id,
        customer_id=replacement.id,
        order_number="NEW-2",
        order_date=date(2026, 9, 19),
        fields_set={"customer_id", "order_number", "order_date"},
    )

    assert updated.customer_id == replacement.id
    assert updated.order_number == "NEW-2"
    assert updated.order_date == date(2026, 9, 19)


@pytest.mark.asyncio
async def test_processed_customer_order_customer_cannot_change(test_session):
    customer = Customer(name="Первый клиент")
    replacement = Customer(name="Новый клиент")
    test_session.add_all([customer, replacement])
    await test_session.flush()
    order = CustomerOrder(
        customer_id=customer.id,
        status=CUSTOMER_ORDER_STATUS.PROCESSED,
    )
    test_session.add(order)
    await test_session.commit()

    with pytest.raises(ValueError, match="только до обработки"):
        await update_customer_order(
            test_session,
            order.id,
            customer_id=replacement.id,
            order_number=None,
            order_date=None,
            fields_set={"customer_id"},
        )


@pytest.mark.asyncio
async def test_customer_order_soft_delete_hides_order(test_session):
    customer = Customer(name="Клиент")
    test_session.add(customer)
    await test_session.flush()
    order = CustomerOrder(
        customer_id=customer.id,
        status=CUSTOMER_ORDER_STATUS.PROCESSED,
        external_source="PARTS_SOFT",
        external_order_id="delete-901",
    )
    test_session.add(order)
    await test_session.commit()

    await soft_delete_customer_order(
        test_session,
        order.id,
        user_id=None,
    )

    assert await crud_customer_order.get_by_id(test_session, order.id) is None
    preserved = await crud_customer_order.get_by_id(
        test_session,
        order.id,
        include_deleted=True,
    )
    assert preserved is not None
    assert preserved.deleted_at is not None
    assert preserved.external_order_id == "delete-901"
