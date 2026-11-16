import pytest

from dz_fastapi.models.diadoc import DiadocOutgoingDocument
from dz_fastapi.schemas.diadoc import DiadocCloudSignTaskOut
from dz_fastapi.services.diadoc_cloud_sign import (
    MAX_BATCH_DOCUMENTS,
    confirm_cloud_sign_task,
    start_send_outgoing_batch_task,
)


class FakeDiadoc:
    """Одна SMS на пачку: подписи возвращаются в порядке файлов."""

    def __init__(self, fail_draft=None):
        self.sign_calls = []
        self.sent = []
        self.fail_draft = fail_draft

    async def cloud_sign(self, *, files, certificate_thumbprint=None):
        self.sign_calls.append([item["FileName"] for item in files])
        return "token-1"

    async def cloud_sign_confirm(self, *, token, confirmation_code):
        assert (token, confirmation_code) == ("token-1", "123456")
        return [f"signature-{index}" for index in range(len(self.sign_calls[-1]))]

    async def send_draft(self, *, draft, operation_id=None):
        if draft["DraftId"] == self.fail_draft:
            raise RuntimeError("Диадок отклонил документ")
        self.sent.append(
            (draft["DraftId"], draft["DocumentSignatures"][0]["Signature"])
        )
        return {"MessageId": f"sent-{draft['DraftId']}", "Entities": []}


async def _draft(session, tmp_path, customer, number):
    path = tmp_path / f"{number}.xml"
    path.write_bytes(f"<upd number='{number}'/>".encode())
    document = DiadocOutgoingDocument(
        environment="staging",
        from_box_id_guid="our-box",
        to_box_id_guid="their-box",
        customer_id=customer.id,
        source_type="shipment_document",
        source_id=None,
        file_name=f"{number}.xml",
        local_file_path=str(path),
        document_number=number,
        message_id=f"draft-{number}",
        entity_id=f"entity-{number}",
        is_draft=True,
        status="draft",
    )
    session.add(document)
    await session.flush()
    return document


@pytest.mark.asyncio
async def test_several_drafts_are_signed_with_one_code(
    test_session, tmp_path, created_customers
):
    customer = created_customers[0]
    documents = [
        await _draft(test_session, tmp_path, customer, number)
        for number in ("ДЗ-000001", "ДЗ-000002", "ДЗ-000003")
    ]
    await test_session.commit()
    client = FakeDiadoc()

    task = await start_send_outgoing_batch_task(
        test_session, client=client, documents=documents
    )
    # Один запрос на подпись для всей пачки — значит, одна SMS.
    assert client.sign_calls == [["ДЗ-000001.xml", "ДЗ-000002.xml", "ДЗ-000003.xml"]]

    task = await confirm_cloud_sign_task(
        test_session, client=client, task=task, confirmation_code="123456"
    )

    assert task.state == "completed"
    # Каждому документу — его подпись, в порядке файлов.
    assert client.sent == [
        ("draft-ДЗ-000001", "signature-0"),
        ("draft-ДЗ-000002", "signature-1"),
        ("draft-ДЗ-000003", "signature-2"),
    ]
    out = DiadocCloudSignTaskOut.model_validate(task)
    assert len(out.sent_document_ids) == 3 and out.failed == []
    assert out.documents_total == 3
    for document in documents:
        await test_session.refresh(document)
        assert document.is_draft is False and document.status == "sent"


@pytest.mark.asyncio
async def test_one_rejected_document_does_not_cancel_the_others(
    test_session, tmp_path, created_customers
):
    customer = created_customers[0]
    first = await _draft(test_session, tmp_path, customer, "ДЗ-000001")
    second = await _draft(test_session, tmp_path, customer, "ДЗ-000002")
    await test_session.commit()
    first_id, second_id = first.id, second.id
    client = FakeDiadoc(fail_draft="draft-ДЗ-000001")

    task = await start_send_outgoing_batch_task(
        test_session, client=client, documents=[first, second]
    )
    task = await confirm_cloud_sign_task(
        test_session, client=client, task=task, confirmation_code="123456"
    )

    out = DiadocCloudSignTaskOut.model_validate(task)
    assert out.sent_document_ids == [second_id]
    assert [item["document_id"] for item in out.failed] == [first_id]
    assert "отклонил" in out.failed[0]["error"]
    # Неотправленный остаётся черновиком — его можно подписать снова.
    assert (await test_session.get(DiadocOutgoingDocument, first_id)).is_draft is True
    assert (await test_session.get(DiadocOutgoingDocument, second_id)).is_draft is False


@pytest.mark.asyncio
async def test_batch_limits(test_session, tmp_path, created_customers):
    customer = created_customers[0]
    sent = await _draft(test_session, tmp_path, customer, "ДЗ-000009")
    sent.is_draft = False
    with pytest.raises(ValueError):
        await start_send_outgoing_batch_task(
            test_session, client=FakeDiadoc(), documents=[sent]
        )
    with pytest.raises(ValueError):
        await start_send_outgoing_batch_task(
            test_session, client=FakeDiadoc(), documents=[]
        )
    with pytest.raises(ValueError):
        await start_send_outgoing_batch_task(
            test_session,
            client=FakeDiadoc(),
            documents=[sent] * (MAX_BATCH_DOCUMENTS + 1),
        )
