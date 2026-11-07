import xml.etree.ElementTree as ET
from types import SimpleNamespace

import pytest

from dz_fastapi.models.settings import DiadocIntegrationSettings
from dz_fastapi.services.diadoc_cloud_sign import (
    _apply_send_outgoing,
    build_signer_payload,
    build_utd970_buyer_title_user_data,
    split_signer_full_name,
)


def _integration(**overrides):
    integration = DiadocIntegrationSettings()
    integration.signer_full_name = overrides.get(
        "signer_full_name", "Ермоленко Александр Сергеевич"
    )
    integration.signer_position = overrides.get(
        "signer_position", "Директор"
    )
    integration.organization_name = overrides.get(
        "organization_name", 'ООО "Дрэгонзап"'
    )
    integration.organization_inn = overrides.get(
        "organization_inn", "7701234567"
    )
    return integration


def test_split_signer_full_name():
    assert split_signer_full_name("Иванов Иван Иванович") == (
        "Иванов",
        "Иван",
        "Иванович",
    )
    assert split_signer_full_name("Иванов Иван") == (
        "Иванов",
        "Иван",
        None,
    )


def test_split_signer_full_name_requires_two_parts():
    with pytest.raises(ValueError):
        split_signer_full_name("Иванов")
    with pytest.raises(ValueError):
        split_signer_full_name("")


def test_signer_payload():
    payload = build_signer_payload(_integration())
    details = payload["SignerDetails"]
    assert details["Surname"] == "Ермоленко"
    assert details["FirstName"] == "Александр"
    assert details["Patronymic"] == "Сергеевич"
    assert details["JobTitle"] == "Директор"
    assert details["Inn"] == "7701234567"


def test_buyer_title_user_data_is_valid_xml():
    xml_bytes = build_utd970_buyer_title_user_data(
        _integration(),
        acceptance_date="03.07.2026",
        total_code="1",
    )
    root = ET.fromstring(xml_bytes)
    assert root.tag == "UniversalTransferDocumentBuyerTitle"
    assert root.get("AcceptanceDate") == "03.07.2026"
    assert root.get("DocumentCreator") == 'ООО "Дрэгонзап"'

    content_oper = root.find("ContentOperCode")
    assert content_oper is not None
    assert content_oper.get("TotalCode") == "1"

    signer = root.find("Signers/Signer")
    assert signer is not None
    fio = signer.find("Fio")
    assert fio.get("LastName") == "Ермоленко"
    assert fio.get("FirstName") == "Александр"
    assert fio.get("MiddleName") == "Сергеевич"
    position = signer.find("Position")
    assert position.get("PositionSource") == "Manual"
    assert position.text == "Директор"


def test_buyer_title_requires_position():
    with pytest.raises(ValueError):
        build_utd970_buyer_title_user_data(
            _integration(signer_position=""),
            acceptance_date="03.07.2026",
        )


def test_buyer_title_escapes_quotes_in_names():
    xml_bytes = build_utd970_buyer_title_user_data(
        _integration(organization_name='ООО "А&Б"'),
        acceptance_date="03.07.2026",
    )
    root = ET.fromstring(xml_bytes)
    assert root.get("DocumentCreator") == 'ООО "А&Б"'


@pytest.mark.asyncio
async def test_send_outgoing_sends_existing_draft():
    document = SimpleNamespace(
        id=17,
        environment="staging",
        from_box_id_guid="from-box",
        to_box_id_guid="to-box",
        message_id="draft-id",
        entity_id="draft-entity",
        content_sha256="abc123",
        is_draft=True,
        status="draft",
        sent_at=None,
        raw_response={},
    )
    task = SimpleNamespace(outgoing_document_id=17)

    class Session:
        async def get(self, model, object_id):
            assert object_id == 17
            return document

        def add(self, value):
            assert value is document

    class Client:
        async def send_draft(self, *, draft, operation_id):
            assert draft == {
                "BoxId": "from-box",
                "ToBoxId": "to-box",
                "DraftId": "draft-id",
                "DocumentSignatures": [
                    {
                        "ParentEntityId": "draft-entity",
                        "Signature": "signature",
                    }
                ],
            }
            assert operation_id
            return {
                "MessageId": "sent-message",
                "Entities": [
                    {"EntityType": "Attachment", "EntityId": "sent-entity"}
                ],
            }

    result = await _apply_send_outgoing(
        Session(),
        client=Client(),
        task=task,
        signatures=["signature"],
    )

    assert result == {"message_id": "sent-message"}
    assert document.message_id == "sent-message"
    assert document.entity_id == "sent-entity"
    assert document.is_draft is False
    assert document.status == "sent"
