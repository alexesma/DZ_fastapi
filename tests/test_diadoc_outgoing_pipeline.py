from datetime import timedelta

import pytest

from dz_fastapi.core.time import now_moscow
from dz_fastapi.models.diadoc import DiadocOutgoingDocument
from dz_fastapi.models.inventory import ShipmentDocument, ShipmentDocumentStatus
from dz_fastapi.models.settings import DiadocIntegrationSettings
from dz_fastapi.services import diadoc_outgoing_pipeline as pipeline
from dz_fastapi.services.diadoc_outgoing_pipeline import (
    UpdDraftBlocked,
    auto_draft_posted_shipments,
    signing_queue,
)


async def _shipment(session, customer, *, posted_minutes_ago, number):
    shipment = ShipmentDocument(
        status=ShipmentDocumentStatus.POSTED,
        customer_id=customer.id,
        doc_number=number,
        posted_at=now_moscow() - timedelta(minutes=posted_minutes_ago),
    )
    session.add(shipment)
    await session.flush()
    return shipment.id


def _integration(enabled=True):
    return DiadocIntegrationSettings(
        box_id_guid="our-box",
        outbound_auto_draft_enabled=enabled,
        outbound_auto_draft_from=now_moscow() - timedelta(hours=1),
    )


@pytest.fixture
def fake_drafts(monkeypatch):
    """Вместо Диадока: чётные номера создаются, остальным что-то мешает."""
    calls = []

    async def create(session, *, client, integration, shipment_id):
        calls.append(shipment_id)
        shipment = await session.get(ShipmentDocument, shipment_id)
        if shipment.doc_number.endswith("blocked"):
            raise UpdDraftBlocked("КИЗ: в УПД пойдёт 1 кодов маркировки из 2 шт.")
        if shipment.doc_number.endswith("down"):
            raise RuntimeError("503 Service Unavailable")
        document = DiadocOutgoingDocument(
            environment="staging",
            from_box_id_guid="our-box",
            to_box_id_guid="their-box",
            customer_id=shipment.customer_id,
            source_type="shipment_document",
            source_id=shipment_id,
            file_name="upd.xml",
            local_file_path="/tmp/upd.xml",
            document_number=shipment.doc_number,
            is_draft=True,
            status="draft",
        )
        session.add(document)
        await session.flush()
        return document

    monkeypatch.setattr(pipeline, "create_shipment_upd_draft", create)
    return calls


@pytest.mark.asyncio
async def test_drafts_are_created_only_for_shipments_posted_after_enabling(
    test_session, created_customers, fake_drafts
):
    customer = created_customers[0]
    old = await _shipment(test_session, customer, posted_minutes_ago=600, number="OLD")
    ready = await _shipment(test_session, customer, posted_minutes_ago=5, number="ДЗ-000001")
    blocked = await _shipment(
        test_session, customer, posted_minutes_ago=4, number="ДЗ-000002-blocked"
    )
    down = await _shipment(test_session, customer, posted_minutes_ago=3, number="ДЗ-000003-down")
    await test_session.commit()

    result = await auto_draft_posted_shipments(
        test_session, client=None, integration=_integration()
    )

    assert result == {"created": 1, "blocked": 1, "errors": 1}
    assert old not in fake_drafts

    queue = await signing_queue(test_session)
    assert [row["shipment_id"] for row in queue["drafts"]] == [ready]
    assert queue["drafts"][0]["customer_name"] == customer.name
    reasons = {row["shipment_id"]: row["reason"] for row in queue["blocked"]}
    assert reasons[blocked].startswith("КИЗ")
    assert "503" in reasons[down]

    # Готовый документ второй раз не создаётся; остальные пробуются снова.
    fake_drafts.clear()
    again = await auto_draft_posted_shipments(
        test_session, client=None, integration=_integration()
    )
    assert again["created"] == 0
    assert sorted(fake_drafts) == sorted([blocked, down])


@pytest.mark.asyncio
async def test_disabled_pipeline_does_nothing(
    test_session, created_customers, fake_drafts
):
    await _shipment(test_session, created_customers[0], posted_minutes_ago=5, number="ДЗ-1")
    await test_session.commit()
    result = await auto_draft_posted_shipments(
        test_session, client=None, integration=_integration(enabled=False)
    )
    assert result["skipped"] == "disabled"
    assert fake_drafts == []


@pytest.mark.asyncio
async def test_queue_endpoint_and_enabling(async_client, test_session):
    response = await async_client.get("/diadoc/outbound-queue")
    assert response.status_code == 200, response.text
    assert response.json() == {
        "enabled": False,
        "since": None,
        "email_enabled": False,
        "drafts": [],
        "blocked": [],
    }

    response = await async_client.put(
        "/diadoc/settings", json={"outbound_auto_draft_enabled": True}
    )
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["outbound_auto_draft_enabled"] is True
    assert body["outbound_auto_draft_from"] is not None
