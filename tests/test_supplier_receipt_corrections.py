from datetime import date
from types import SimpleNamespace

import pytest
from sqlalchemy import select

from dz_fastapi.core.time import now_moscow
from dz_fastapi.models.diadoc import DiadocIncomingDocument
from dz_fastapi.models.inventory import ReturnDocumentStatus, ReturnItem, ReturnToSupplier
from dz_fastapi.models.partner import (
    SupplierOrderMessage,
    SupplierReceipt,
    SupplierReceiptCorrection,
    SupplierReceiptItem,
)
from dz_fastapi.services.diadoc_documents import process_diadoc_incoming_document
from dz_fastapi.services.supplier_order_responses import process_supplier_response_messages
from dz_fastapi.services.supplier_receipt_corrections import is_correction_document
from dz_fastapi.services.supplier_return_ukd import sync_supplier_return_ukd_status
from dz_fastapi.services.upd_xml import parse_correction_xml

CODE_A = "0104610014828864215HNN7I3EEKRU3"
CODE_B = "0104610014828864215EK6K00LRPECI"


def _return_ukd_xml(*, before: list[str], after: list[str]) -> str:
    before_xml = "".join(f"<КИЗ>{code}</КИЗ>" for code in before)
    after_xml = "".join(f"<КИЗ>{code}</КИЗ>" for code in after)
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<Файл>
  <Документ>
    <СвКСчФ НомерКСчФ="UKD-42" ДатаКСчФ="21.11.2026">
      <СчФ НомерСчФ="SUP-42" ДатаСчФ="20.11.2026" />
      <ТаблКСчФ>
        <СведТов НомСтр="1" ПорНомТовВСЧФ="1" НаимТов="Деталь"
                  КолТовДо="2" КолТовПосле="1">
          <ДопСведТов КодТов="OEM-42" />
          <НомСредИдентТовДо>{before_xml}</НомСредИдентТовДо>
          <НомСредИдентТовПосле>{after_xml}</НомСредИдентТовПосле>
        </СведТов>
      </ТаблКСчФ>
    </СвКСчФ>
  </Документ>
</Файл>"""


def test_correction_parser_reads_marking_codes_before_and_after():
    parsed = parse_correction_xml(_return_ukd_xml(before=[CODE_A, CODE_B], after=[CODE_A]).encode())

    assert parsed is not None
    assert parsed.rows[0].marking_codes_before == [CODE_A, CODE_B]
    assert parsed.rows[0].marking_codes_after == [CODE_A]


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
@pytest.mark.parametrize(
    ("returned_code", "should_link"),
    [(CODE_B, True), (CODE_A, False)],
)
async def test_supplier_return_ukd_is_linked_and_status_is_synchronized(
    test_session,
    created_providers,
    tmp_path,
    returned_code,
    should_link,
):
    provider = created_providers[0]
    receipt = SupplierReceipt(
        provider_id=provider.id,
        document_number="SUP-42",
        document_date=date(2026, 11, 20),
    )
    receipt.items = [
        SupplierReceiptItem(
            oem_number="OEM-42",
            autopart_name="Деталь",
            received_quantity=2,
            marking_codes=[CODE_A, CODE_B],
        )
    ]
    test_session.add(receipt)
    await test_session.flush()
    supplier_return = ReturnToSupplier(
        doc_number="RET-42",
        status=ReturnDocumentStatus.SHIPPED,
        provider_id=provider.id,
        supplier_receipt_id=receipt.id,
    )
    supplier_return.items = [
        ReturnItem(
            supplier_receipt_item_id=receipt.items[0].id,
            oem_number="OEM-42",
            quantity=1,
            marking_codes=[returned_code],
        )
    ]
    test_session.add(supplier_return)

    path = tmp_path / "UKD-42.xml"
    path.write_text(
        _return_ukd_xml(before=[CODE_A, CODE_B], after=[CODE_A]),
        encoding="utf-8",
    )
    document = DiadocIncomingDocument(
        environment="staging",
        box_id_guid="test-box",
        message_id="return-ukd-msg",
        entity_id="return-ukd-entity",
        provider_id=provider.id,
        file_name="UKD-42.xml",
        document_number="UKD-42",
        document_date=date(2026, 11, 21),
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

    assert (result["return_to_supplier_id"] == supplier_return.id) is should_link
    await test_session.refresh(supplier_return)
    assert supplier_return.status == ReturnDocumentStatus.SHIPPED
    if not should_link:
        assert supplier_return.diadoc_incoming_document_id is None
        assert supplier_return.diadoc_status is None
        assert document.parsed_details["supplier_return_match"]["state"] == (
            "return_not_found"
        )
        return

    assert supplier_return.diadoc_incoming_document_id == document.id
    assert supplier_return.supplier_receipt_correction_id == result["correction_id"]
    assert supplier_return.diadoc_status == "received"
    assert document.parsed_details["supplier_return_match"]["state"] == "linked"

    document.signed_at = now_moscow()
    await sync_supplier_return_ukd_status(test_session, document)
    assert supplier_return.diadoc_status == "signed"
    assert supplier_return.status == ReturnDocumentStatus.SHIPPED

    document.revocation_status = "RevocationAccepted"
    await sync_supplier_return_ukd_status(test_session, document)
    assert supplier_return.diadoc_status == "revoked"
    assert supplier_return.status == ReturnDocumentStatus.SHIPPED


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
