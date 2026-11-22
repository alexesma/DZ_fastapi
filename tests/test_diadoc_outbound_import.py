import pytest
from sqlalchemy import select

from dz_fastapi.models.diadoc import DiadocBoxEvent, DiadocOutgoingDocument
from dz_fastapi.services.diadoc_events import process_pending_events
from dz_fastapi.services.diadoc_outbound_import import (
    EXTERNAL_SOURCE_TYPE,
    outbound_attachments,
    sync_external_outgoing_documents,
)


def _message(direction="Outbound", entity="entity-1"):
    return {
        "MessageId": "message-1",
        "Entities": [
            {
                "EntityId": entity,
                "EntityType": "Attachment",
                "DocumentInfo": {"DocumentDirection": direction},
            },
            {"EntityId": "sign-1", "EntityType": "Signature"},
        ],
    }


class FakeClient:
    def __init__(self):
        self.document_calls = 0

    async def get_document(self, **kwargs):
        self.document_calls += 1
        return {
            "MessageId": kwargs["message_id"],
            "EntityId": kwargs["entity_id"],
            "DocumentDirection": "Outbound",
            "CounteragentBoxId": "box-customer",
            "TypeNamedId": "UniversalTransferDocument",
            "FileName": "UPD_100.xml",
            "DocumentNumber": "100",
            "DocumentDate": "20.11.2026",
            "SendTimestampTicks": 639000000000000000,
        }

    async def get_entity_content(self, **kwargs):
        return b"<xml/>"

    async def get_docflows(self, **kwargs):
        return {"Documents": [{"DocumentId": {"MessageId": "message-1", "EntityId": "entity-1"}}]}

    async def get_documents(self, **kwargs):
        return {
            "HasMoreResults": False,
            "Documents": [
                {
                    "MessageId": "message-1",
                    "EntityId": "entity-1",
                    "IndexKey": "k1",
                    "DocumentDirection": "Outbound",
                    "CounteragentBoxId": "box-customer",
                    "FileName": "UPD_100.xml",
                    "DocumentNumber": "100",
                }
            ],
        }


def test_outbound_attachments_only_outbound_documents():
    assert outbound_attachments(_message()) == [("message-1", "entity-1")]
    assert outbound_attachments(_message("Inbound")) == []


@pytest.mark.asyncio
async def test_event_creates_external_outgoing_document_once(test_session, tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    client = FakeClient()
    for index in (1, 2):  # повтор того же сообщения не создаёт дубль
        test_session.add(
            DiadocBoxEvent(
                environment="staging",
                box_id_guid="box-1",
                event_id=f"event-{index}",
                index_key=f"index-{index}",
                event_kind="message",
                message_id="message-1",
                payload={"EventId": f"event-{index}", "Message": _message()},
                status="pending",
            )
        )
    await test_session.commit()

    result = await process_pending_events(
        test_session,
        client=client,
        environment="staging",
        box_id_guid="box-1",
        download_content=True,
        process_supplier_message=False,
    )

    assert result["failed"] == 0
    rows = (await test_session.execute(select(DiadocOutgoingDocument))).scalars().all()
    assert len(rows) == 1
    document = rows[0]
    assert document.source_type == EXTERNAL_SOURCE_TYPE
    assert document.is_draft is False
    assert document.document_number == "100"
    assert document.to_box_id_guid == "box-customer"
    assert document.local_file_path
    assert document.metadata_json["external"] is True


@pytest.mark.asyncio
async def test_history_sync_skips_already_known_documents(test_session, tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    client = FakeClient()

    first = await sync_external_outgoing_documents(
        test_session, client=client, environment="staging", box_id_guid="box-1"
    )
    second = await sync_external_outgoing_documents(
        test_session, client=client, environment="staging", box_id_guid="box-1"
    )

    assert (first["created"], first["known"]) == (1, 0)
    assert (second["created"], second["known"]) == (0, 1)
    assert client.document_calls == 0  # данные берутся из списка GetDocuments


@pytest.mark.asyncio
async def test_parallel_duplicate_insert_is_rejected_by_unique_index(test_session):
    from sqlalchemy.exc import IntegrityError

    def row():
        return DiadocOutgoingDocument(
            environment="staging",
            from_box_id_guid="box-1",
            to_box_id_guid="box-c",
            source_type=EXTERNAL_SOURCE_TYPE,
            file_name="a.xml",
            local_file_path="",
            entity_id="entity-dup",
            message_id="message-dup",
        )

    test_session.add(row())
    await test_session.commit()
    test_session.add(row())
    with pytest.raises(IntegrityError):
        await test_session.commit()
    await test_session.rollback()
