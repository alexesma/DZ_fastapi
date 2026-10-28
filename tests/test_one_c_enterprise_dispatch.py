"""EnterpriseData dispatch must not conflate FTP reads with UNF posting."""

from decimal import Decimal
from xml.etree import ElementTree as ET

import pytest

from dz_fastapi.core.time import now_moscow
from dz_fastapi.models.one_c import OneCExchangeEvent, OneCExchangeEventStatus
from dz_fastapi.models.partner import SupplierReceipt, SupplierReceiptItem
from dz_fastapi.services import one_c_enterprise_dispatch as dispatch
from dz_fastapi.services.one_c_outbox import CHANNEL_JSON, get_or_create_delivery_batch


def _event(index: int) -> OneCExchangeEvent:
    return OneCExchangeEvent(
        entity_type="supplier_receipt",
        entity_id=index,
        event_type="posted",
        payload={"document_date": "2026-10-28"},
        idempotency_key=f"enterprise-test:{index}",
        status=OneCExchangeEventStatus.PENDING,
    )


@pytest.mark.asyncio
async def test_auto_dispatch_waits_for_read_and_does_not_mark_event_succeeded(
    test_session, monkeypatch,
):
    event = _event(1)
    test_session.add(event)
    await test_session.flush()
    await dispatch.set_config(
        test_session, enabled=True, send_receipts=True,
        send_shipments=False, include_history=True,
    )
    await test_session.commit()

    transport = {"incoming_files": [], "pending_body": None, "last_confirmed_body": None}
    sent = []

    async def payload(_session, _event):
        return {"document_id": 1}

    async def company(_session):
        return {"short_name": "Test"}

    def send(objects, *, description):
        sent.append((objects, description))
        return {"message_no": 9}

    monkeypatch.setattr(dispatch, "get_exchange_status", lambda: transport)
    monkeypatch.setattr(dispatch, "_current_payload", payload)
    monkeypatch.setattr(dispatch, "_company", company)
    monkeypatch.setattr(dispatch, "event_body_objects", lambda *_: [ET.Element("Document")])
    monkeypatch.setattr(dispatch, "send_body_message", send)

    first = await dispatch.dispatch_next(test_session)
    await test_session.commit()
    assert first["sent_event_id"] == event.id
    assert len(sent) == 1
    assert event.ed_message_no == 9
    assert event.status == OneCExchangeEventStatus.PENDING
    assert event.ed_transport_confirmed_at is None

    body = {
        "message_no": 9,
        "description": f"supplier_receipt:1 event:{event.id}",
        "sent_at": "2026-10-28T12:00:00+03:00",
    }
    transport["pending_body"] = body
    second = await dispatch.dispatch_next(test_session)
    assert second["sent_event_id"] is None
    assert len(sent) == 1

    transport["pending_body"] = None
    transport["last_confirmed_body"] = {
        **body, "transport_confirmed_at": "2026-10-28T12:05:00+03:00",
    }
    third = await dispatch.dispatch_next(test_session)
    await test_session.commit()
    assert third["sent_event_id"] is None
    assert event.ed_transport_confirmed_at is not None
    assert event.status == OneCExchangeEventStatus.PENDING


@pytest.mark.asyncio
async def test_activation_skips_old_events_unless_history_requested(test_session):
    old = _event(10)
    test_session.add(old)
    await test_session.flush()
    config = await dispatch.set_config(
        test_session, enabled=True, send_receipts=True, send_shipments=False,
    )
    assert config.first_event_id == old.id + 1


@pytest.mark.asyncio
async def test_manual_message_is_recovered_from_ftp_confirmation(test_session):
    event = _event(20)
    test_session.add(event)
    await test_session.flush()
    changed = await dispatch.reconcile_transport(test_session, {
        "last_confirmed_body": {
            "message_no": 6,
            "description": f"supplier_receipt:1880 event {event.id}",
            "sent_at": "2026-10-28T21:09:22+03:00",
            "transport_confirmed_at": "2026-10-28T23:24:27+03:00",
        },
    })
    assert changed == [event.id]
    assert event.ed_message_no == 6
    assert event.ed_transport_confirmed_at is not None
    assert event.status == OneCExchangeEventStatus.PENDING


@pytest.mark.asyncio
async def test_history_audit_returns_empty_counts(test_session):
    audit = await dispatch.history_audit(test_session)
    assert audit["posted_receipts"]["count"] == 0
    assert audit["posted_shipments"]["count"] == 0
    assert audit["providers_missing_contract"] == []


@pytest.mark.asyncio
async def test_history_backfill_is_idempotent_and_isolated_from_json_queue(
    test_session, created_autopart, created_providers,
):
    receipt = SupplierReceipt(
        provider_id=created_providers[0].id,
        document_number="HIST-1",
        document_date=now_moscow().date(),
        posted_at=now_moscow(),
    )
    test_session.add(receipt)
    await test_session.flush()
    test_session.add(SupplierReceiptItem(
        receipt_id=receipt.id,
        autopart_id=created_autopart.id,
        received_quantity=1,
        price=Decimal("100.00"),
    ))
    await test_session.commit()

    before = await dispatch.history_audit(test_session)
    assert before["posted_receipts"]["count"] == 1
    assert before["posted_receipts"]["without_event"] == 1
    first = await dispatch.backfill_history_step(test_session)
    second = await dispatch.backfill_history_step(test_session)
    assert first["count"] == 1
    assert second["count"] == 0
    after = await dispatch.history_audit(test_session)
    assert after["posted_receipts"]["without_event"] == 0
    event = await test_session.get(OneCExchangeEvent, first["created_event_ids"][0])
    assert event.ed_only is True
    batch = await get_or_create_delivery_batch(
        test_session, channel=CHANNEL_JSON, entity_types=["supplier_receipt"]
    )
    assert batch is None
