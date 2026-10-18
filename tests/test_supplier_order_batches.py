from datetime import datetime
from unittest.mock import AsyncMock
from zoneinfo import ZoneInfo

import pytest

from dz_fastapi.models.partner import SUPPLIER_ORDER_STATUS, SupplierOrder
from dz_fastapi.models.settings import SchedulerSetting
from dz_fastapi.services import customer_orders
from dz_fastapi.services.supplier_workflow import _auto_refuse_deadline


@pytest.mark.asyncio
async def test_individual_cutoff_overrides_general_and_seals_batch(
    test_session, created_providers, monkeypatch,
):
    moscow = ZoneInfo("Europe/Moscow")
    now = datetime(2026, 10, 19, 10, 2, tzinfo=moscow)
    monkeypatch.setattr(customer_orders, "now_moscow", lambda: now)
    individual, general = created_providers
    individual.order_schedule_enabled = True
    individual.order_schedule_days = ["mon"]
    individual.order_schedule_times = ["11:00"]
    setting = SchedulerSetting(
        key="supplier_orders_send", enabled=True,
        days=["mon"], times=["10:00"],
    )
    test_session.add(setting)
    individual_order = SupplierOrder(
        provider_id=individual.id,
        created_at=datetime(2026, 10, 19, 9, 0, tzinfo=moscow),
        status=SUPPLIER_ORDER_STATUS.NEW,
    )
    general_order = SupplierOrder(
        provider_id=general.id,
        created_at=datetime(2026, 10, 19, 9, 0, tzinfo=moscow),
        status=SUPPLIER_ORDER_STATUS.NEW,
    )
    test_session.add_all([individual_order, general_order])
    await test_session.commit()

    sender = AsyncMock(return_value={"sent": 1, "failed": 0})
    monkeypatch.setattr(customer_orders, "send_supplier_orders", sender)
    await customer_orders.send_scheduled_supplier_orders(test_session)

    sender.assert_awaited_once_with(test_session, [general_order.id])
    await test_session.refresh(individual_order)
    await test_session.refresh(general_order)
    assert individual_order.status == SUPPLIER_ORDER_STATUS.NEW
    assert general_order.status == SUPPLIER_ORDER_STATUS.SCHEDULED
    assert general_order.scheduled_at.astimezone(moscow).hour == 10

    next_batch = await customer_orders._get_or_create_open_supplier_order(
        test_session, provider_id=general.id, provider_config_id=None,
    )
    assert next_batch.id != general_order.id

    general_order.status = SUPPLIER_ORDER_STATUS.SENT
    setting.enabled = False
    await test_session.commit()
    monkeypatch.setattr(
        customer_orders, "now_moscow",
        lambda: datetime(2026, 10, 19, 11, 2, tzinfo=moscow),
    )
    await customer_orders.send_scheduled_supplier_orders(test_session)
    assert sender.await_args_list[-1].args[1] == [individual_order.id]
    await test_session.refresh(next_batch)
    assert next_batch.status == SUPPLIER_ORDER_STATUS.NEW


def test_latest_cutoff_waits_for_newer_batch():
    moscow = ZoneInfo("Europe/Moscow")
    cutoff = customer_orders._latest_supplier_order_cutoff(
        datetime(2026, 10, 19, 10, 2, tzinfo=moscow),
        days=["mon"], times=["10:00", "15:00"],
    )
    assert cutoff == datetime(2026, 10, 19, 10, 0, tzinfo=moscow)
    assert datetime(2026, 10, 19, 10, 1, tzinfo=moscow) > cutoff


def test_weekend_manual_send_has_next_business_day_response_deadline():
    moscow = ZoneInfo("Europe/Moscow")
    sent = datetime(2026, 10, 17, 12, 0, tzinfo=moscow)
    assert _auto_refuse_deadline(sent, set()) == datetime(
        2026, 10, 19, 23, 0, tzinfo=moscow
    )
