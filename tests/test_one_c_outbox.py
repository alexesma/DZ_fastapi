import base64
from decimal import Decimal

import pytest
from sqlalchemy import func, select

from dz_fastapi.core.time import now_moscow
from dz_fastapi.models.inventory import (
    ShipmentDocument,
    ShipmentDocumentItem,
    ShipmentDocumentStatus,
    SyncStatus,
)
from dz_fastapi.models.one_c import (
    OneCExchangeBatch,
    OneCExchangeBatchStatus,
    OneCExchangeEvent,
    OneCExchangeEventStatus,
)
from dz_fastapi.services.one_c_outbox import (
    CHANNEL_COMMERCEML,
    CHANNEL_JSON,
    ENTITY_SHIPMENT,
    EVENT_POSTED,
    acknowledge_delivery_batch,
    backfill_pending_shipment_events,
    enqueue_shipment_event,
    get_or_create_delivery_batch,
    retry_one_c_event,
)


async def _create_posted_shipment(
    session,
    *,
    autopart_id: int,
    customer_id: int,
    number: str,
) -> ShipmentDocument:
    shipment = ShipmentDocument(
        doc_number=number,
        doc_date=now_moscow(),
        status=ShipmentDocumentStatus.POSTED,
        customer_id=customer_id,
        sync_status=SyncStatus.PENDING,
        posted_at=now_moscow(),
    )
    session.add(shipment)
    await session.flush()
    session.add(
        ShipmentDocumentItem(
            document_id=shipment.id,
            autopart_id=autopart_id,
            customer_oem="DZ-CLIENT-001",
            customer_brand="DRAGONZAP",
            customer_name="Клиентское наименование",
            quantity=2,
            price=Decimal("150.00"),
            vat_rate=Decimal("22.00"),
        )
    )
    await session.commit()
    return shipment


@pytest.mark.asyncio
async def test_one_c_outbox_is_idempotent_and_reuses_unconfirmed_batch(
    test_session,
    created_autopart,
    created_customers,
):
    shipment = await _create_posted_shipment(
        test_session,
        autopart_id=created_autopart.id,
        customer_id=created_customers[0].id,
        number="DZ-OUTBOX-1",
    )

    first = await enqueue_shipment_event(test_session, shipment.id)
    assert first.payload["customer"]["email_contact"] == (
        created_customers[0].email_contact
    )
    assert first.payload["customer"]["legal_address"] == (
        created_customers[0].legal_address
    )
    duplicate = await enqueue_shipment_event(test_session, shipment.id)
    assert duplicate.id == first.id
    assert await test_session.scalar(select(func.count(OneCExchangeEvent.id))) == 1

    batch = await get_or_create_delivery_batch(
        test_session,
        channel=CHANNEL_COMMERCEML,
        entity_types=[ENTITY_SHIPMENT],
        event_types=[EVENT_POSTED],
    )
    assert batch is not None
    assert batch.items[0].event.id == first.id
    assert batch.items[0].event.status == OneCExchangeEventStatus.IN_FLIGHT
    assert shipment.sync_status == SyncStatus.PENDING

    repeated = await get_or_create_delivery_batch(
        test_session,
        channel=CHANNEL_COMMERCEML,
        entity_types=[ENTITY_SHIPMENT],
        event_types=[EVENT_POSTED],
    )
    assert repeated.id == batch.id
    assert repeated.attempt_count == 2
    assert await test_session.scalar(select(func.count(OneCExchangeBatch.id))) == 1

    confirmed = await acknowledge_delivery_batch(
        test_session,
        batch_uid=batch.batch_uid,
        success=True,
        external_ids={first.event_uid: "1C-GUID-001"},
    )
    assert confirmed.status == OneCExchangeBatchStatus.SUCCEEDED
    assert first.status == OneCExchangeEventStatus.SUCCEEDED
    assert first.external_id == "1C-GUID-001"
    assert shipment.sync_status == SyncStatus.SYNCED
    assert shipment.external_id == "1C-GUID-001"


@pytest.mark.asyncio
async def test_one_c_backfill_does_not_duplicate_changed_shipment_snapshot(
    test_session,
    created_autopart,
    created_customers,
):
    shipment = await _create_posted_shipment(
        test_session,
        autopart_id=created_autopart.id,
        customer_id=created_customers[0].id,
        number="DZ-OUTBOX-CHANGED",
    )
    first = await enqueue_shipment_event(test_session, shipment.id)

    item = await test_session.scalar(
        select(ShipmentDocumentItem).where(
            ShipmentDocumentItem.document_id == shipment.id
        )
    )
    item.cost_total = Decimal("250.00")
    await test_session.commit()

    assert await backfill_pending_shipment_events(test_session) == 0
    repeated = await enqueue_shipment_event(test_session, shipment.id)
    assert repeated.id == first.id
    assert await test_session.scalar(select(func.count(OneCExchangeEvent.id))) == 1


@pytest.mark.asyncio
async def test_one_c_error_can_be_returned_to_queue(
    test_session,
    created_autopart,
    created_customers,
):
    shipment = await _create_posted_shipment(
        test_session,
        autopart_id=created_autopart.id,
        customer_id=created_customers[0].id,
        number="DZ-OUTBOX-ERROR",
    )
    event = await enqueue_shipment_event(test_session, shipment.id)
    batch = await get_or_create_delivery_batch(
        test_session,
        channel=CHANNEL_COMMERCEML,
        entity_types=[ENTITY_SHIPMENT],
        event_types=[EVENT_POSTED],
    )

    await acknowledge_delivery_batch(
        test_session,
        batch_uid=batch.batch_uid,
        success=False,
        error="Документ не проведён в 1С",
    )
    assert event.status == OneCExchangeEventStatus.ERROR
    assert shipment.sync_status == SyncStatus.ERROR

    retried = await retry_one_c_event(test_session, event.id)
    assert retried.status == OneCExchangeEventStatus.PENDING
    assert retried.last_error is None
    assert shipment.sync_status == SyncStatus.PENDING
    next_batch = await get_or_create_delivery_batch(
        test_session,
        channel=CHANNEL_COMMERCEML,
        entity_types=[ENTITY_SHIPMENT],
        event_types=[EVENT_POSTED],
    )
    assert next_batch.id != batch.id


@pytest.mark.asyncio
async def test_commerceml_marks_shipment_synced_only_after_success(
    async_client,
    test_session,
    created_autopart,
    created_customers,
    monkeypatch,
):
    monkeypatch.setenv("ONE_C_EXCHANGE_LOGIN", "onec-test")
    monkeypatch.setenv("ONE_C_EXCHANGE_PASSWORD", "secret")
    shipment = await _create_posted_shipment(
        test_session,
        autopart_id=created_autopart.id,
        customer_id=created_customers[0].id,
        number="DZ-PROTOCOL-1",
    )
    token = base64.b64encode(b"onec-test:secret").decode("ascii")
    headers = {"Authorization": f"Basic {token}"}

    first = await async_client.get("/1c/exchange?type=sale&mode=query", headers=headers)
    assert first.status_code == 200, first.text
    batch_uid = first.headers["X-DZ-1C-Batch-ID"]
    assert b"DZ-PROTOCOL-1" in first.content
    await test_session.refresh(shipment)
    assert shipment.sync_status == SyncStatus.PENDING

    repeated = await async_client.get("/1c/exchange?type=sale&mode=query", headers=headers)
    assert repeated.headers["X-DZ-1C-Batch-ID"] == batch_uid
    assert repeated.content == first.content
    await test_session.refresh(shipment)
    assert shipment.sync_status == SyncStatus.PENDING

    success = await async_client.get("/1c/exchange?type=sale&mode=success", headers=headers)
    assert success.status_code == 200
    assert success.text == "success"
    await test_session.refresh(shipment)
    assert shipment.sync_status == SyncStatus.SYNCED


@pytest.mark.asyncio
async def test_json_v1_contract_acknowledges_each_event(
    async_client,
    test_session,
    created_autopart,
    created_customers,
    monkeypatch,
):
    monkeypatch.setenv("ONE_C_EXCHANGE_LOGIN", "onec-test")
    monkeypatch.setenv("ONE_C_EXCHANGE_PASSWORD", "secret")
    shipment = await _create_posted_shipment(
        test_session,
        autopart_id=created_autopart.id,
        customer_id=created_customers[0].id,
        number="DZ-JSON-V1",
    )
    token = base64.b64encode(b"onec-test:secret").decode("ascii")
    headers = {"Authorization": f"Basic {token}"}

    ping = await async_client.get("/1c/outbox/v1/ping", headers=headers)
    assert ping.status_code == 200, ping.text
    assert ping.json()["contract_version"] == "1.0"

    response = await async_client.get("/1c/outbox/v1/query", headers=headers)
    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["protocol"] == "dz-1c-outbox"
    assert payload["contract_version"] == "1.0"
    assert payload["content_hash"]
    assert payload["formed_at"]
    assert len(payload["events"]) == 1
    event = payload["events"][0]
    assert event["entity_type"] == ENTITY_SHIPMENT
    assert event["payload"]["document_number"] == "DZ-JSON-V1"

    repeated = await async_client.get("/1c/outbox/v1/query", headers=headers)
    assert repeated.json()["batch_uid"] == payload["batch_uid"]
    assert repeated.json()["content_hash"] == payload["content_hash"]

    ack = await async_client.post(
        f"/1c/outbox/v1/{payload['batch_uid']}/ack",
        headers=headers,
        json={
            "contract_version": "1.0",
            "results": [
                {
                    "event_uid": event["event_uid"],
                    "success": True,
                    "external_id": "UT11-SALE-GUID",
                }
            ],
        },
    )
    assert ack.status_code == 200, ack.text
    assert ack.json()["status"] == "succeeded"
    await test_session.refresh(shipment)
    assert shipment.sync_status == SyncStatus.SYNCED
    assert shipment.external_id == "UT11-SALE-GUID"


@pytest.mark.asyncio
async def test_json_v1_rejects_incomplete_event_results(
    async_client,
    test_session,
    created_autopart,
    created_customers,
    monkeypatch,
):
    monkeypatch.setenv("ONE_C_EXCHANGE_LOGIN", "onec-test")
    monkeypatch.setenv("ONE_C_EXCHANGE_PASSWORD", "secret")
    await _create_posted_shipment(
        test_session,
        autopart_id=created_autopart.id,
        customer_id=created_customers[0].id,
        number="DZ-JSON-INCOMPLETE-1",
    )
    await _create_posted_shipment(
        test_session,
        autopart_id=created_autopart.id,
        customer_id=created_customers[0].id,
        number="DZ-JSON-INCOMPLETE-2",
    )
    token = base64.b64encode(b"onec-test:secret").decode("ascii")
    headers = {"Authorization": f"Basic {token}"}
    response = await async_client.get("/1c/outbox/v1/query", headers=headers)
    response_payload = response.json()
    batch_uid = response_payload["batch_uid"]
    first_event_uid = response_payload["events"][0]["event_uid"]

    ack = await async_client.post(
        f"/1c/outbox/v1/{batch_uid}/ack",
        headers=headers,
        json={
            "contract_version": "1.0",
            "results": [
                {
                    "event_uid": first_event_uid,
                    "success": True,
                    "external_id": "UT-GUID-1",
                }
            ],
        },
    )
    assert ack.status_code == 409, ack.text
    assert "нет результатов" in ack.json()["detail"]


@pytest.mark.asyncio
async def test_detailed_ack_rejects_unknown_event(test_session):
    event = OneCExchangeEvent(
        entity_type=ENTITY_SHIPMENT,
        entity_id=100,
        event_type=EVENT_POSTED,
        payload={"document_id": 100},
        idempotency_key="shipment:100:posted:test",
        status=OneCExchangeEventStatus.PENDING,
    )
    test_session.add(event)
    await test_session.flush()
    batch = await get_or_create_delivery_batch(
        test_session,
        channel=CHANNEL_JSON,
        entity_types=[ENTITY_SHIPMENT],
    )

    with pytest.raises(ValueError, match="неизвестные события"):
        await acknowledge_delivery_batch(
            test_session,
            batch_uid=batch.batch_uid,
            success=True,
            event_results={
                "not-in-batch": {
                    "success": True,
                    "external_id": "UT-GUID",
                }
            },
        )


@pytest.mark.asyncio
async def test_detailed_ack_preserves_success_and_error_per_event(test_session):
    events = []
    for entity_id in (201, 202):
        event = OneCExchangeEvent(
            entity_type=ENTITY_SHIPMENT,
            entity_id=entity_id,
            event_type=EVENT_POSTED,
            payload={"document_id": entity_id},
            idempotency_key=f"shipment:{entity_id}:posted:test",
            status=OneCExchangeEventStatus.PENDING,
        )
        test_session.add(event)
        events.append(event)
    await test_session.flush()
    batch = await get_or_create_delivery_batch(
        test_session,
        channel=CHANNEL_JSON,
        entity_types=[ENTITY_SHIPMENT],
    )

    acknowledged = await acknowledge_delivery_batch(
        test_session,
        batch_uid=batch.batch_uid,
        success=True,
        event_results={
            events[0].event_uid: {
                "success": True,
                "external_id": "UT-GUID-201",
            },
            events[1].event_uid: {
                "success": False,
                "error": "Не найден договор контрагента",
            },
        },
    )

    assert acknowledged.status == OneCExchangeBatchStatus.ERROR
    assert events[0].status == OneCExchangeEventStatus.SUCCEEDED
    assert events[0].external_id == "UT-GUID-201"
    assert events[1].status == OneCExchangeEventStatus.ERROR
    assert events[1].last_error == "Не найден договор контрагента"
