from datetime import date
from decimal import Decimal

import pytest
from sqlalchemy import select

from dz_fastapi.core.time import now_moscow
from dz_fastapi.models.diadoc import DiadocOutgoingDocument
from dz_fastapi.models.inventory import (
    ShipmentDocument,
    ShipmentDocumentItem,
    ShipmentDocumentStatus,
)
from dz_fastapi.models.one_c import OneCExchangeEvent
from dz_fastapi.models.partner import CustomerExternalReference
from dz_fastapi.models.settings import DiadocIntegrationSettings
from dz_fastapi.models.shipment_correction import ShipmentCorrection
from dz_fastapi.models.user import User, UserRole, UserStatus
from dz_fastapi.services.diadoc_outgoing import (
    build_formalized_diadoc_payload_from_shipment_correction,
)
from dz_fastapi.services.inventory_stock import unpost_shipment_document
from dz_fastapi.services.one_c_outbox import (
    CHANNEL_JSON,
    ENTITY_SHIPMENT_CORRECTION,
    acknowledge_delivery_batch,
    get_or_create_delivery_batch,
)


async def _shipment(session, autopart_id, customer_id):
    if await session.get(User, 1) is None:
        session.add(User(
            id=1,
            email="correction-test@example.com",
            password_hash="unused",
            role=UserRole.ADMIN,
            status=UserStatus.ACTIVE,
        ))
    doc = ShipmentDocument(
        doc_number="DZ-CORR-SOURCE",
        doc_date=now_moscow(),
        status=ShipmentDocumentStatus.POSTED,
        customer_id=customer_id,
        posted_at=now_moscow(),
    )
    session.add(doc)
    await session.flush()
    row = ShipmentDocumentItem(
        document_id=doc.id,
        autopart_id=autopart_id,
        customer_oem="DZ-001",
        customer_brand="DRAGONZAP",
        quantity=5,
        price=Decimal("100.00"),
        vat_rate=Decimal("22.00"),
    )
    session.add(row)
    await session.commit()
    return doc, row


def _payload(shipment_id, item_id, number, quantity, price):
    return {
        "shipment_id": shipment_id,
        "doc_number": number,
        "doc_date": date.today().isoformat(),
        "basis_number": "ACT-1",
        "basis_date": date.today().isoformat(),
        "basis_description": "Акт согласования",
        "reason": "Согласовано изменение отгрузки",
        "document_function": "ДИС",
        "items": [{
            "shipment_item_id": item_id,
            "quantity_after": quantity,
            "price_after": price,
        }],
    }


@pytest.mark.asyncio
async def test_shipment_correction_posts_immutable_snapshot_and_one_c_event(
    async_client, test_session, created_autopart, created_customers,
):
    shipment, item = await _shipment(
        test_session, created_autopart.id, created_customers[0].id
    )
    first = await async_client.post(
        "/inventory/shipment-corrections",
        json=_payload(shipment.id, item.id, "UKD-1", 3, "90.00"),
    )
    assert first.status_code == 200, first.text
    assert first.json()["items"][0]["quantity_before"] == 5
    assert first.json()["status"] == "draft"

    posted = await async_client.post(
        f"/inventory/shipment-corrections/{first.json()['id']}/post"
    )
    assert posted.status_code == 200, posted.text
    assert posted.json()["status"] == "posted"
    assert posted.json()["items"][0]["price_before"] == "100.00"
    with pytest.raises(ValueError, match="нельзя распровести"):
        await unpost_shipment_document(test_session, shipment.id)
    await test_session.rollback()
    events = list((await test_session.execute(select(OneCExchangeEvent))).scalars())
    assert len(events) == 1
    assert events[0].entity_type == "shipment_correction"
    assert events[0].payload["physical_stock_changed"] is False
    assert events[0].payload["items"][0]["quantity_after"] == 3
    batch = await get_or_create_delivery_batch(
        test_session,
        channel=CHANNEL_JSON,
        entity_types=[ENTITY_SHIPMENT_CORRECTION],
    )
    assert batch.items[0].event.id == events[0].id
    await acknowledge_delivery_batch(
        test_session,
        batch_uid=batch.batch_uid,
        success=True,
        external_ids={events[0].event_uid: "1C-UKD-1"},
    )
    corrected = await test_session.get(ShipmentCorrection, first.json()["id"])
    assert corrected.external_id == "1C-UKD-1"
    assert corrected.sync_status.value == "synced"
    await test_session.commit()

    second = await async_client.post(
        "/inventory/shipment-corrections",
        json=_payload(shipment.id, item.id, "UKD-2", 2, "110.00"),
    )
    assert second.status_code == 200, second.text
    assert second.json()["items"][0]["quantity_before"] == 3
    assert second.json()["items"][0]["price_before"] == "90.00"

    preview = await async_client.get(
        f"/inventory/shipment-corrections/{second.json()['id']}/print/ukd"
    )
    assert preview.status_code == 200
    assert "UKD-2" in preview.text
    assert "Предпросмотр данных УКД" in preview.text

    invalid = await async_client.put(
        f"/inventory/shipment-corrections/{first.json()['id']}",
        json=_payload(shipment.id, item.id, "UKD-1", 1, "80.00"),
    )
    assert invalid.status_code == 400
    assert (await test_session.get(ShipmentCorrection, first.json()["id"])).status.value == "posted"


@pytest.mark.asyncio
async def test_stale_correction_draft_cannot_be_posted(
    async_client, test_session, created_autopart, created_customers,
):
    shipment, item = await _shipment(
        test_session, created_autopart.id, created_customers[0].id
    )
    stale = await async_client.post(
        "/inventory/shipment-corrections",
        json=_payload(shipment.id, item.id, "UKD-STALE", 4, "100.00"),
    )
    fresh = await async_client.post(
        "/inventory/shipment-corrections",
        json=_payload(shipment.id, item.id, "UKD-FRESH", 3, "100.00"),
    )
    assert stale.status_code == fresh.status_code == 200
    assert (
        await async_client.post(
            f"/inventory/shipment-corrections/{fresh.json()['id']}/post"
        )
    ).status_code == 200
    refused = await async_client.post(
        f"/inventory/shipment-corrections/{stale.json()['id']}/post"
    )
    assert refused.status_code == 400
    assert "уже корректировалась" in refused.json()["detail"]


@pytest.mark.asyncio
async def test_correction_keeps_original_line_number_for_second_item(
    async_client, test_session, created_autopart, created_customers,
):
    shipment, _first = await _shipment(
        test_session, created_autopart.id, created_customers[0].id
    )
    second = ShipmentDocumentItem(
        document_id=shipment.id,
        autopart_id=created_autopart.id,
        customer_oem="DZ-002",
        quantity=2,
        price=Decimal("200.00"),
        vat_rate=Decimal("22.00"),
    )
    test_session.add(second)
    await test_session.commit()

    response = await async_client.post(
        "/inventory/shipment-corrections",
        json=_payload(shipment.id, second.id, "UKD-SECOND", 1, "200.00"),
    )
    assert response.status_code == 200, response.text
    assert response.json()["items"][0]["original_number"] == 2


@pytest.mark.parametrize(
    ("source_function", "correction_function", "price", "difference_tag"),
    [
        ("ДОП", "ДИС", "110.00", "AmountsInc"),
        ("СЧФДОП", "КСЧФДИС", "90.00", "AmountsDec"),
    ],
)
@pytest.mark.asyncio
async def test_formalized_ukd_uses_original_line_and_difference_amounts(
    async_client, test_session, created_autopart, created_customers,
    source_function, correction_function, price, difference_tag,
):
    shipment, item = await _shipment(
        test_session, created_autopart.id, created_customers[0].id
    )
    test_session.add(CustomerExternalReference(
        customer_id=shipment.customer_id,
        source_system="DIADOC_COUNTERAGENT_BOX",
        external_customer_name="buyer-box",
        is_active=True,
    ))
    test_session.add(DiadocOutgoingDocument(
        environment="staging",
        from_box_id_guid="seller-box",
        to_box_id_guid="buyer-box",
        customer_id=shipment.customer_id,
        source_type="shipment_document",
        source_id=shipment.id,
        type_named_id="UniversalTransferDocument",
        document_function=source_function,
        document_version="utd970_05_03_01",
        document_number=shipment.doc_number,
        document_date=shipment.doc_date.date(),
        file_name="source.xml",
        local_file_path="test/source.xml",
        is_draft=False,
        message_id="message-1",
        entity_id="entity-1",
        status="sent",
    ))
    await test_session.commit()

    payload = _payload(shipment.id, item.id, "UKD-PRICE", 5, price)
    payload["document_function"] = correction_function
    wrong_payload = {**payload, "document_function": (
        "ДИС" if correction_function != "ДИС" else "КСЧФДИС"
    )}
    wrong = await async_client.post(
        "/inventory/shipment-corrections", json=wrong_payload
    )
    assert wrong.status_code == 400
    assert "Функция УКД" in wrong.json()["detail"]
    created = await async_client.post(
        "/inventory/shipment-corrections",
        json=payload,
    )
    assert created.status_code == 200, created.text
    posted = await async_client.post(
        f"/inventory/shipment-corrections/{created.json()['id']}/post"
    )
    assert posted.status_code == 200, posted.text

    integration = DiadocIntegrationSettings(
        box_id_guid="seller-box",
        refresh_token="test-token",
        organization_name="Продавец",
        organization_inn="7700000000",
        organization_kpp="770001001",
        signer_full_name="Иванов Иван Иванович",
        signer_position="Директор",
        signer_basis="Устав",
    )

    class Client:
        xml = None

        async def get_document_types(self, box_id_guid):
            assert box_id_guid == "seller-box"
            return {"DocumentTypes": [{
                "TypeNamedId": "UniversalCorrectionDocument",
                "Functions": [{
                    "Name": correction_function,
                    "Versions": [{
                        "Version": "ucd736_05_01_02",
                        "IsActual": True,
                        "Titles": [{"Index": 0, "IsFormal": True}],
                    }],
                }],
            }]}

        async def get_organization(self, *, box_id_guid):
            return {
                "FullName": "Продавец" if box_id_guid == "seller-box" else "Покупатель",
                "Inn": "7700000000" if box_id_guid == "seller-box" else "7701111111",
                "Kpp": "770001001",
                "FnsParticipantId": f"2BM-{box_id_guid}",
                "Address": {"RussianAddress": {
                    "Region": "77", "City": "Москва", "Street": "Тестовая",
                    "Building": "1",
                }},
            }

        async def generate_title_xml(self, **kwargs):
            self.xml = kwargs["user_data_xml"].decode("utf-8")
            return "<Файл />".encode("utf-8"), "ukd.xml"

    client = Client()
    result = await build_formalized_diadoc_payload_from_shipment_correction(
        test_session,
        client=client,
        integration=integration,
        correction_id=created.json()["id"],
    )
    assert result["source_type"] == "shipment_correction"
    assert result["document_function"] == correction_function
    assert 'OriginalNumber="1"' in client.xml
    assert f'<{difference_tag}>50.00</{difference_tag}>' in client.xml
    assert 'BaseDocumentNumber="ACT-1"' in client.xml
