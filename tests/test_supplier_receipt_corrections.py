from types import SimpleNamespace

import pytest
from sqlalchemy import select

from dz_fastapi.models.diadoc import DiadocIncomingDocument
from dz_fastapi.models.partner import (
    SupplierOrderMessage,
    SupplierReceipt,
    SupplierReceiptCorrection,
)
from dz_fastapi.services.diadoc_documents import process_diadoc_incoming_document
from dz_fastapi.services.supplier_order_responses import process_supplier_response_messages
from dz_fastapi.services.supplier_receipt_corrections import is_correction_document


def test_correction_document_detection_covers_email_and_diadoc():
    assert is_correction_document("КСФ к накладной 123")
    assert is_correction_document("Ответ поставщика", "УКД_123.xml")
    assert is_correction_document("UniversalCorrectionDocument")
    assert not is_correction_document("УПД 123", "nakladnaya.xml")
    assert not is_correction_document("Отказ по позиции", "otvet.xlsx")
    assert not is_correction_document("Price correction for order 123")


@pytest.mark.asyncio
async def test_diadoc_correction_creates_review_draft_without_receipt(
    test_session,
    created_providers,
    tmp_path,
):
    path = tmp_path / "KSF_10.xml"
    path.write_text("<UniversalCorrectionDocument />", encoding="utf-8")
    document = DiadocIncomingDocument(
        environment="staging",
        box_id_guid="test-box",
        message_id="ksf-msg",
        entity_id="ksf-entity",
        provider_id=created_providers[0].id,
        file_name="KSF_10.xml",
        document_number="KSF-10",
        local_file_path=str(path),
        raw_metadata={"TypeNamedId": "UniversalCorrectionDocument"},
    )
    test_session.add(document)
    await test_session.commit()
    result = await process_diadoc_incoming_document(
        test_session,
        document=document,
        download_content_if_missing=False,
    )
    assert result["message_type"] == "CORRECTION_DOC"
    assert result["receipt_ids"] == []
    correction = await test_session.get(SupplierReceiptCorrection, result["correction_id"])
    assert correction.status == "draft"
    assert correction.receipt_id is None
    repeated = await process_diadoc_incoming_document(
        test_session,
        document=document,
        download_content_if_missing=False,
    )
    assert repeated["correction_id"] == correction.id
    assert len((await test_session.execute(select(SupplierReceiptCorrection))).scalars().all()) == 1


@pytest.mark.asyncio
async def test_email_correction_creates_review_draft_without_receipt(
    test_session,
    created_providers,
    monkeypatch,
    tmp_path,
):
    provider = created_providers[0]
    provider.email_contact = "corrections@supplier.example"
    await test_session.commit()

    async def fake_fetch_messages(session, *, date_from, date_to=None, **kwargs):
        return [
            (
                SimpleNamespace(
                    from_="corrections@supplier.example",
                    subject="КСФ к УПД 10",
                    text="Корректировка количества",
                    html=None,
                    attachments=[SimpleNamespace(filename="KSF_10.xml", payload=b"<Correction/>")],
                    received_at=None,
                    date=None,
                    external_id="ksf-email-1",
                    uid=None,
                    folder_name="INBOX",
                ),
                None,
            )
        ]

    monkeypatch.setattr(
        "dz_fastapi.services.supplier_order_responses._fetch_supplier_response_messages",
        fake_fetch_messages,
    )
    monkeypatch.setattr(
        "dz_fastapi.services.supplier_order_responses.SUPPLIER_RESPONSE_DIR",
        str(tmp_path),
    )
    result = await process_supplier_response_messages(test_session)
    assert result["processed_messages"] == 1
    correction = (await test_session.execute(select(SupplierReceiptCorrection))).scalar_one()
    source = await test_session.get(SupplierOrderMessage, correction.source_message_id)
    assert source.message_type == "CORRECTION_DOC"
    assert correction.source_kind == "email"
    assert correction.status == "draft"
    assert (await test_session.execute(select(SupplierReceipt))).scalars().all() == []
