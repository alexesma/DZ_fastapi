from types import SimpleNamespace

import pytest

from dz_fastapi.services import inbox_email as inbox_email_service


@pytest.mark.asyncio
async def test_process_customer_order_returns_missing_config_status(
    monkeypatch,
):
    notifications = []

    async def fake_find_matching_configs(*_args, **_kwargs):
        return []

    async def fake_create_admin_notifications(**kwargs):
        notifications.append(kwargs)

    monkeypatch.setattr(
        inbox_email_service,
        '_find_matching_customer_order_configs',
        fake_find_matching_configs,
    )
    monkeypatch.setattr(
        'dz_fastapi.services.notifications.create_admin_notifications',
        fake_create_admin_notifications,
    )

    inbox_email = SimpleNamespace(
        from_email='orders@example.com',
        subject='Заказ #123',
        email_account_id=1,
        attachment_info=[],
    )

    result, error = await inbox_email_service._process_customer_order(
        session=None,
        inbox_email=inbox_email,
    )

    assert error is None
    assert result is not None
    assert result['status'] == 'missing_config'
    assert 'Не найден активный конфиг заказа клиента' in result['reason']
    assert notifications
    assert notifications[0]['level'] == 'warning'


@pytest.mark.asyncio
async def test_process_customer_order_returns_queued_with_matched_config_ids(
    monkeypatch,
):
    notifications = []

    async def fake_find_matching_configs(*_args, **_kwargs):
        return [7, 9]

    async def fake_create_admin_notifications(**kwargs):
        notifications.append(kwargs)

    monkeypatch.setattr(
        inbox_email_service,
        '_find_matching_customer_order_configs',
        fake_find_matching_configs,
    )
    monkeypatch.setattr(
        'dz_fastapi.services.notifications.create_admin_notifications',
        fake_create_admin_notifications,
    )

    inbox_email = SimpleNamespace(
        from_email='orders@example.com',
        subject='Заказ #124',
        email_account_id=1,
        attachment_info=[{'name': 'order.xlsx'}],
    )

    result, error = await inbox_email_service._process_customer_order(
        session=None,
        inbox_email=inbox_email,
    )

    assert error is None
    assert result is not None
    assert result['status'] == 'queued'
    assert result['matched_config_ids'] == [7, 9]
    assert notifications
    assert notifications[0]['level'] == 'info'
