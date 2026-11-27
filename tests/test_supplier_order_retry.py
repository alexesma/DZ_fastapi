"""Failed supplier-order emails are retried with back-off and then escalated."""

from datetime import timedelta

import pytest

from dz_fastapi.core.time import now_moscow
from dz_fastapi.models.partner import (
    SUPPLIER_ORDER_STATUS,
    ProviderPriceListConfig,
    SupplierOrder,
    SupplierOrderItem,
)
from dz_fastapi.services import customer_orders as service


async def _order(session, provider, part, **kwargs):
    provider.email_contact = "supplier@example.com"
    order = SupplierOrder(
        provider_id=provider.id, status=SUPPLIER_ORDER_STATUS.NEW, **kwargs
    )
    session.add(order)
    await session.flush()
    session.add(
        SupplierOrderItem(
            supplier_order_id=order.id, autopart_id=part.id, quantity=1, price=10
        )
    )
    await session.commit()
    return order


def _patch_transport(monkeypatch, results):
    calls = []

    async def fake_send(*args, **kwargs):
        calls.append(args[0])
        return results.pop(0)

    async def no_account(session, purpose):
        return None

    monkeypatch.setattr(service, "_send_email_attachment_async", fake_send)
    monkeypatch.setattr(service, "_get_out_account", no_account)
    return calls


@pytest.mark.asyncio
async def test_failed_send_is_retried_after_delay_and_then_succeeds(
    monkeypatch, test_session, created_providers, created_autopart,
):
    order = await _order(test_session, created_providers[0], created_autopart)
    order_id = order.id
    calls = _patch_transport(monkeypatch, [False, True])

    first = await service.send_supplier_orders(test_session, [order_id])
    assert first["failed"] == 1
    order = await test_session.get(SupplierOrder, order_id)
    assert order.status == SUPPLIER_ORDER_STATUS.ERROR
    assert order.send_attempts == 1

    # Pause not over yet: the scheduler leaves the order alone.
    await service.send_scheduled_supplier_orders(test_session)
    assert len(calls) == 1

    order.last_send_attempt_at = now_moscow() - timedelta(minutes=6)
    await test_session.commit()
    await service.send_scheduled_supplier_orders(test_session)

    order = await test_session.get(SupplierOrder, order_id)
    await test_session.refresh(order)
    assert len(calls) == 2
    assert order.status == SUPPLIER_ORDER_STATUS.SENT
    assert order.send_attempts == 2
    assert order.last_send_error is None


@pytest.mark.asyncio
async def test_exhausted_order_stops_retrying_and_alerts_once(
    monkeypatch, test_session, created_providers, created_autopart,
):
    order = await _order(test_session, created_providers[0], created_autopart)
    order.status = SUPPLIER_ORDER_STATUS.ERROR
    order.last_send_error = "SMTP down"
    order.send_attempts = service.SUPPLIER_ORDER_MAX_SEND_ATTEMPTS
    order.last_send_attempt_at = now_moscow() - timedelta(hours=10)
    await test_session.commit()
    order_id = order.id
    calls = _patch_transport(monkeypatch, [True])

    await service.send_scheduled_supplier_orders(test_session)
    assert calls == []

    alerts = await service.collect_supplier_order_send_alerts(test_session)
    assert [a["id"] for a in alerts] == [order_id]
    assert await service.collect_supplier_order_send_alerts(test_session) == []


@pytest.mark.asyncio
async def test_old_error_order_is_not_resent_automatically(
    monkeypatch, test_session, created_providers, created_autopart,
):
    order = await _order(test_session, created_providers[0], created_autopart)
    order.status = SUPPLIER_ORDER_STATUS.ERROR
    order.last_send_error = "SMTP down"
    order.created_at = now_moscow() - timedelta(days=3)
    order.last_send_attempt_at = now_moscow() - timedelta(days=3)
    await test_session.commit()
    calls = _patch_transport(monkeypatch, [True])

    await service.send_scheduled_supplier_orders(test_session)

    assert calls == []


@pytest.mark.asyncio
async def test_manual_order_uses_order_addresses_of_all_provider_configs(
    monkeypatch, test_session, created_providers, created_autopart,
):
    provider = created_providers[0]
    provider.order_stub_enabled = False  # real addresses, not the test stub
    for index, emails in enumerate((
        ["sales@example.com", "buyer@example.com"],
        ["buyer@example.com", "copy@example.com"],
    )):
        test_session.add(ProviderPriceListConfig(
            provider_id=provider.id, start_row=1, oem_col=1, qty_col=3,
            price_col=4, name_price=f"price_{index}", order_emails=emails,
        ))
    await test_session.flush()
    # Manual item, no price config known; provider contact must NOT win.
    order = await _order(test_session, provider, created_autopart)
    calls = _patch_transport(monkeypatch, [True])

    await service.send_supplier_orders(test_session, [order.id])

    assert calls == ["sales@example.com,buyer@example.com,copy@example.com"]


@pytest.mark.asyncio
async def test_manual_order_is_bound_to_price_config_of_items(
    test_session, created_providers, created_autopart,
):
    provider = created_providers[0]
    config = ProviderPriceListConfig(
        provider_id=provider.id, start_row=1, oem_col=1, qty_col=3,
        price_col=4, name_price="main", order_emails="o@example.com",
    )
    test_session.add(config)
    await test_session.flush()

    bound = await service.create_manual_supplier_order(
        test_session,
        provider.id,
        [{
            "autopart_id": created_autopart.id, "oem": "X1", "brand": "B",
            "quantity": 1, "price": 5, "provider_config_id": config.id,
        }],
    )
    assert bound.provider_config_id == config.id

    unbound = await service.create_manual_supplier_order(
        test_session,
        provider.id,
        [{"oem": "X2", "brand": "B", "quantity": 1, "price": 5}],
    )
    assert unbound.provider_config_id is None


def test_supplier_order_oem_is_cleaned_of_special_characters():
    clean = service._supplier_order_clean_oem
    assert clean("90301-22013") == "9030122013"
    assert clean(" 04465/0K-240.A ") == "044650K240A"
    assert clean("ABC_12 3") == "ABC123"
    assert clean(None) == ""
