from datetime import date, datetime
from decimal import Decimal

import pytest

from dz_fastapi.core.time import now_moscow
from dz_fastapi.models.partner import CustomerOrder, CustomerOrderItem
from dz_fastapi.models.user import User, UserRole, UserStatus
from dz_fastapi.services.auth import get_password_hash


async def _create_user(session, email: str, role: UserRole):
    user = User(
        email=email,
        password_hash=get_password_hash('secret123'),
        role=role,
        status=UserStatus.ACTIVE,
    )
    session.add(user)
    await session.commit()
    await session.refresh(user)
    return user


async def _login(async_client, email: str):
    response = await async_client.post(
        '/auth/login',
        json={'email': email, 'password': 'secret123'},
    )
    assert response.status_code == 200


@pytest.mark.asyncio
async def test_customer_order_config_crud(
    async_client, test_session, created_customers
):
    await _create_user(test_session, 'manager@example.com', UserRole.MANAGER)
    await _login(async_client, 'manager@example.com')

    customer = created_customers[0]
    payload = {
        'customer_id': customer.id,
        'order_email': 'orders@client.com',
        'oem_col': 1,
        'brand_col': 2,
        'qty_col': 3,
        'price_tolerance_pct': 2,
        'price_warning_pct': 5,
    }

    response = await async_client.post('/customer-orders/config', json=payload)
    assert response.status_code == 201
    config = response.json()
    assert config['customer_id'] == customer.id
    assert config['oem_col'] == 1
    assert config['brand_col'] == 2
    assert config['qty_col'] == 3

    response = await async_client.get(
        f'/customer-orders/config/{customer.id}'
    )
    assert response.status_code == 200

    response = await async_client.put(
        f'/customer-orders/config/{customer.id}',
        json={'order_subject_pattern': 'test'},
    )
    assert response.status_code == 200
    assert response.json()['order_subject_pattern'] == 'test'


def _shift_to_previous_month(value: datetime) -> datetime:
    year = value.year
    month = value.month - 1
    if month == 0:
        month = 12
        year -= 1
    return value.replace(year=year, month=month)


@pytest.mark.asyncio
async def test_customer_order_item_stats_monthly_breakdown(
    async_client, test_session, created_customers
):
    await _create_user(test_session, 'stats@example.com', UserRole.MANAGER)
    await _login(async_client, 'stats@example.com')

    customer = created_customers[0]
    other_customer = created_customers[1]
    now = now_moscow().replace(day=15, hour=10, minute=0, second=0)
    previous_month = _shift_to_previous_month(now)

    order_one = CustomerOrder(
        customer_id=customer.id,
        status='SENT',
        received_at=now,
    )
    order_two = CustomerOrder(
        customer_id=customer.id,
        status='SENT',
        received_at=previous_month,
    )
    order_three = CustomerOrder(
        customer_id=other_customer.id,
        status='SENT',
        received_at=previous_month,
    )
    test_session.add_all([order_one, order_two, order_three])
    await test_session.flush()

    test_session.add_all([
        CustomerOrderItem(
            order_id=order_one.id,
            oem='SH0113TM3',
            brand='MAZDA',
            requested_qty=2,
            requested_price=Decimal('2325.00'),
            ship_qty=2,
            status='SUPPLIER',
        ),
        CustomerOrderItem(
            order_id=order_two.id,
            oem='SH0113TM3',
            brand='MAZDA',
            requested_qty=1,
            requested_price=Decimal('2100.00'),
            ship_qty=0,
            reject_qty=1,
            status='REJECTED',
        ),
        CustomerOrderItem(
            order_id=order_three.id,
            oem='SH0113TM3',
            brand='MAZDA',
            requested_qty=4,
            requested_price=Decimal('2200.00'),
            ship_qty=4,
            status='SUPPLIER',
        ),
    ])
    await test_session.commit()

    response = await async_client.get(
        '/customer-orders/item-stats',
        params={
            'kind': 'oem',
            'value': 'SH0113TM3',
            'customer_id': customer.id,
            'months': 3,
        },
    )
    assert response.status_code == 200, response.text
    payload = response.json()

    assert payload['kind'] == 'oem'
    assert payload['value'] == 'SH0113TM3'
    assert payload['current_customer_id'] == customer.id
    assert payload['current_customer_summary']['orders_count'] == 2
    assert payload['current_customer_summary']['rows_count'] == 2
    assert payload['current_customer_summary']['total_requested_qty'] == 3
    assert payload['all_customers_summary']['orders_count'] == 3
    assert payload['all_customers_summary']['total_requested_qty'] == 7
    assert len(payload['current_customer_monthly']) == 3
    assert len(payload['all_customers_monthly']) == 3
    assert (
        payload['current_customer_recent'][0]['requested_price']
        == '2325.00'
    )
    assert payload['all_customers_recent'][0]['customer_id'] == customer.id

    monthly_all = {
        row['month']: row for row in payload['all_customers_monthly']
    }
    assert (
        monthly_all[date(now.year, now.month, 1).isoformat()]['orders_count']
        == 1
    )
    assert monthly_all[
        date(previous_month.year, previous_month.month, 1).isoformat()
    ]['orders_count'] == 2
