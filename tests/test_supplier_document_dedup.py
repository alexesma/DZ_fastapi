from datetime import date, datetime, timezone
from decimal import Decimal
from types import SimpleNamespace

import pytest
from sqlalchemy import select

from dz_fastapi.models.diadoc import DiadocIncomingDocument
from dz_fastapi.models.partner import (
    SupplierOrderMessage,
    SupplierReceipt,
    SupplierReceiptCorrection,
    SupplierReceiptItem,
)
from dz_fastapi.services import supplier_document_dedup as dedup
from dz_fastapi.services.diadoc_documents import process_diadoc_incoming_document
from dz_fastapi.services.supplier_order_responses import (
    process_supplier_response_messages,
)

NOW = datetime(2026, 6, 15, tzinfo=timezone.utc)


def upd_xml(number="УТ-1042", day="15.06.2026", total="1806.00") -> bytes:
    return f"""<?xml version="1.0" encoding="utf-8"?>
<Файл ИдФайл="ON_NSCHFDOPPR_x" ВерсФорм="5.03">
  <Документ КНД="1115131" Функция="ДОП">
    <СвСчФакт НомерДок="{number}" ДатаДок="{day}"/>
    <ТаблСчФакт>
      <СведТов НомСтр="1" НаимТов="Фильтр" КолТов="10" ЦенаТов="150.50"
               СтТовУчНал="{total}">
        <ДопСведТов АртикулТов="A11-1012010"/>
      </СведТов>
      <ВсегоОпл СтТовУчНалВсего="{total}"/>
    </ТаблСчФакт>
  </Документ>
</Файл>""".encode()


def test_identity_normalization_and_totals():
    identity = dedup.identity_from_xml(upd_xml("№ 0012"), "ON_NSCHFDOPPR_x.xml")
    assert identity.kind == "upd"
    assert identity.number == "12"
    assert identity.date == date(2026, 6, 15)
    assert identity.total == Decimal("1806.00")
    assert dedup.identity_from_xml(b"%PDF", "invoice.pdf") is None
    assert not dedup.totals_differ("100.00", "100.01")
    assert dedup.totals_differ("100.00", "100.02")
    assert not dedup.totals_differ(None, "100.00")


def test_receipt_document_types():
    assert dedup.is_receipt_document("UniversalTransferDocument", "a.xml")
    assert dedup.is_receipt_document("UniversalCorrectionDocument", "a.xml")
    assert not dedup.is_receipt_document("Nonformalized", "Счет на оплату 5.pdf")
    assert not dedup.is_receipt_document("Invoice", "on_sfakt.xml")


async def make_diadoc(session, provider, tmp_path, *, total="1806.00",
                      number="УТ-1042", suffix="1", payload=None):
    payload = payload or upd_xml(number=number, total=total)
    path = tmp_path / f"ON_NSCHFDOPPR_{suffix}.xml"
    path.write_bytes(payload)
    doc = DiadocIncomingDocument(
        environment="staging", box_id_guid="box", message_id=f"m{suffix}",
        entity_id=f"e{suffix}", provider_id=provider.id,
        file_name=path.name, document_number=number,
        document_date=date(2026, 6, 15), local_file_path=str(path),
        raw_metadata={"TypeNamedId": "UniversalTransferDocument"},
    )
    test_session = session
    test_session.add(doc)
    await test_session.commit()
    return doc


async def make_email_with_receipt(session, provider, *, total="1806.00",
                                  posted=True, number="УТ-1042"):
    identity = dedup.identity_from_xml(upd_xml(number=number, total=total), "ON_NSCHFDOPPR_x.xml")
    message = SupplierOrderMessage(
        provider_id=provider.id, message_type="SHIPPING_DOC", received_at=NOW,
        source_uid="1", document_kind="upd",
        document_key=dedup.build_dedup_key(provider.id, identity),
        document_total=identity.total,
    )
    session.add(message)
    await session.flush()
    receipt = SupplierReceipt(
        provider_id=provider.id, source_message_id=message.id,
        document_number=number, document_date=date(2026, 6, 15),
        posted_at=NOW if posted else None,
    )
    session.add(receipt)
    await session.flush()
    session.add(SupplierReceiptItem(
        receipt_id=receipt.id, oem_number="A11-1012010", received_quantity=10,
        total_price_with_vat=Decimal(total),
    ))
    await session.commit()
    return message, receipt


async def receipts(session):
    return (await session.execute(select(SupplierReceipt))).scalars().all()


@pytest.mark.asyncio
async def test_email_then_diadoc_links_without_second_receipt(
    test_session, created_providers, tmp_path
):
    provider = created_providers[0]
    message, receipt = await make_email_with_receipt(test_session, provider)
    doc = await make_diadoc(test_session, provider, tmp_path)
    result = await process_diadoc_incoming_document(
        test_session, document=doc, download_content_if_missing=False
    )
    assert result["dedup_action"] == "linked"
    await test_session.refresh(doc)
    assert doc.supplier_order_message_id == message.id
    assert doc.duplicate_kind == "email"
    assert doc.status == "processed"
    assert [r.id for r in await receipts(test_session)] == [receipt.id]
    # повторная обработка ничего не меняет
    again = await process_diadoc_incoming_document(
        test_session, document=doc, download_content_if_missing=False
    )
    assert again["dedup_action"] == "linked"
    assert len(await receipts(test_session)) == 1


@pytest.mark.asyncio
async def test_diadoc_then_email_marks_email_duplicate(
    test_session, created_providers, tmp_path
):
    provider = created_providers[0]
    doc = await make_diadoc(test_session, provider, tmp_path)
    dedup.refresh_diadoc_identity_from_file(doc)
    await test_session.commit()
    message = SupplierOrderMessage(
        provider_id=provider.id, message_type="UNKNOWN", received_at=NOW
    )
    test_session.add(message)
    await test_session.flush()
    verdict = await dedup.apply_email_dedup(
        test_session, message_row=message, provider_id=provider.id,
        attachments=[("УПД.xml", upd_xml(), "h1")],
    )
    assert verdict == "duplicate"
    assert message.message_type == "DUPLICATE_DOC"
    assert message.duplicate_of_diadoc_id == doc.id


@pytest.mark.asyncio
async def test_same_file_sha_without_xml_key_is_duplicate(
    test_session, created_providers, tmp_path
):
    provider = created_providers[0]
    doc = await make_diadoc(test_session, provider, tmp_path, payload=b"%PDF-1.4 x")
    doc.content_sha256 = "abc"
    await test_session.commit()
    message = SupplierOrderMessage(
        provider_id=provider.id, message_type="UNKNOWN", received_at=NOW
    )
    test_session.add(message)
    await test_session.flush()
    verdict = await dedup.apply_email_dedup(
        test_session, message_row=message, provider_id=provider.id,
        attachments=[("x.pdf", b"%PDF-1.4 x", "abc")],
    )
    assert verdict == "duplicate"


@pytest.mark.asyncio
async def test_diadoc_then_email_with_other_total_is_mismatch(
    test_session, created_providers, tmp_path
):
    provider = created_providers[0]
    doc = await make_diadoc(test_session, provider, tmp_path)
    dedup.refresh_diadoc_identity_from_file(doc)
    await test_session.commit()
    message = SupplierOrderMessage(
        provider_id=provider.id, message_type="UNKNOWN", received_at=NOW
    )
    test_session.add(message)
    await test_session.flush()
    verdict = await dedup.apply_email_dedup(
        test_session, message_row=message, provider_id=provider.id,
        attachments=[("УПД.xml", upd_xml(total="1900.00"), "h2")],
    )
    assert verdict == "mismatch"
    assert message.message_type == "DOCUMENT_MISMATCH"
    assert doc.amount_mismatch_status == "applied"  # Диадок главнее
    assert doc.mismatch_other_total == Decimal("1900.00")


@pytest.mark.asyncio
async def test_diadoc_wins_over_unposted_email_receipt(
    test_session, created_providers, tmp_path, monkeypatch
):
    provider = created_providers[0]
    message, receipt = await make_email_with_receipt(
        test_session, provider, total="1806.00", posted=False
    )
    doc = await make_diadoc(test_session, provider, tmp_path, total="2000.00")
    processed = []

    async def fake_process(session, *, document, **kwargs):
        processed.append(document.id)
        return {"receipt_ids": []}

    monkeypatch.setattr(
        "dz_fastapi.services.diadoc_documents.process_diadoc_incoming_document",
        fake_process,
    )
    result = await dedup.evaluate_diadoc_document(test_session, document=doc)
    assert result["action"] == "applied"
    assert processed == [doc.id]
    assert await receipts(test_session) == []
    await test_session.refresh(doc)
    assert doc.amount_mismatch_status == "applied"
    assert doc.mismatch_log[0]["automatic"] is True
    assert doc.mismatch_log[0]["removed_receipts"][0]["receipt_id"] == receipt.id
    await test_session.refresh(message)
    assert message.message_type == "DUPLICATE_DOC"


@pytest.mark.asyncio
async def test_diadoc_vs_posted_email_receipt_creates_correction_only(
    test_session, created_providers, tmp_path
):
    provider = created_providers[0]
    message, receipt = await make_email_with_receipt(
        test_session, provider, total="1806.00", posted=True
    )
    doc = await make_diadoc(test_session, provider, tmp_path, total="2000.00")
    result = await process_diadoc_incoming_document(
        test_session, document=doc, download_content_if_missing=False
    )
    assert result["dedup_action"] == "applied"
    receipt_after = (await receipts(test_session))[0]
    assert receipt_after.id == receipt.id and receipt_after.posted_at is not None
    items = (await test_session.execute(select(SupplierReceiptItem))).scalars().all()
    assert [i.received_quantity for i in items] == [10]
    correction = (await test_session.execute(select(SupplierReceiptCorrection))).scalar_one()
    assert correction.status == "draft"
    await test_session.refresh(doc)
    assert doc.amount_mismatch_status == "correction_required"


@pytest.mark.asyncio
async def test_mismatch_keep_existing_links_to_email_receipt(
    test_session, created_providers, tmp_path
):
    provider = created_providers[0]
    message, _ = await make_email_with_receipt(test_session, provider)
    doc = await make_diadoc(test_session, provider, tmp_path, total="2000.00")
    doc.amount_mismatch_status = "pending"
    doc.mismatch_message_id = message.id
    await test_session.commit()
    result = await dedup.resolve_amount_mismatch(
        test_session, document_id=doc.id, action="keep_existing"
    )
    assert result["status"] == "dismissed"
    await test_session.refresh(doc)
    assert doc.supplier_order_message_id == message.id
    assert len(await receipts(test_session)) == 1


@pytest.mark.asyncio
async def test_payment_invoice_pdf_is_not_processed(
    test_session, created_providers, tmp_path
):
    provider = created_providers[0]
    path = tmp_path / "schet.pdf"
    path.write_bytes(b"%PDF")
    doc = DiadocIncomingDocument(
        environment="staging", box_id_guid="box", message_id="p", entity_id="p",
        provider_id=provider.id, file_name="Счет на оплату 5.pdf",
        local_file_path=str(path), raw_metadata={"TypeNamedId": "Nonformalized"},
    )
    test_session.add(doc)
    await test_session.commit()
    result = await process_diadoc_incoming_document(
        test_session, document=doc, download_content_if_missing=False
    )
    assert result["dedup_action"] == "skipped"
    assert doc.status == "skipped"
    assert doc.supplier_order_message_id is None


@pytest.mark.asyncio
async def test_second_diadoc_copy_is_duplicate(
    test_session, created_providers, tmp_path, monkeypatch
):
    provider = created_providers[0]
    first = await make_diadoc(test_session, provider, tmp_path, suffix="1")
    second = await make_diadoc(test_session, provider, tmp_path, suffix="2")
    dedup.refresh_diadoc_identity_from_file(first)
    await test_session.commit()
    verdict = await dedup.evaluate_diadoc_document(test_session, document=second)
    assert verdict["action"] == "duplicate"
    assert second.duplicate_of_document_id == first.id


@pytest.mark.asyncio
async def test_email_pipeline_skips_duplicate_of_diadoc(
    test_session, created_providers, monkeypatch, tmp_path
):
    provider = created_providers[0]
    provider.email_contact = "docs@supplier.example"
    doc = await make_diadoc(test_session, provider, tmp_path)
    dedup.refresh_diadoc_identity_from_file(doc)
    await test_session.commit()

    async def fake_fetch(session, *, date_from, date_to=None, **kwargs):
        return [(SimpleNamespace(
            from_="docs@supplier.example", subject="УПД УТ-1042", text="во вложении",
            html=None,
            attachments=[SimpleNamespace(filename="ON_NSCHFDOPPR_mail.xml", payload=upd_xml())],
            received_at=None, date=None, external_id="dup-1", uid=None,
            folder_name="INBOX",
        ), None)]

    monkeypatch.setattr(
        "dz_fastapi.services.supplier_order_responses._fetch_supplier_response_messages",
        fake_fetch,
    )
    monkeypatch.setattr(
        "dz_fastapi.services.supplier_order_responses.SUPPLIER_RESPONSE_DIR",
        str(tmp_path / "resp"),
    )
    await process_supplier_response_messages(test_session)
    message = (await test_session.execute(select(SupplierOrderMessage))).scalar_one()
    assert message.message_type == "DUPLICATE_DOC"
    assert message.duplicate_of_diadoc_id == doc.id
    assert await receipts(test_session) == []


def test_1c_number_matches_diadoc_serial_and_subject_is_parsed():
    from dz_fastapi.services.supplier_order_responses import (
        _parse_document_number_from_text,
    )

    subject = "Реализация товаров и услуг 0К00-005846 от 28.10.2026 6:59:45"
    assert _parse_document_number_from_text(subject) == "0К00-005846"
    assert _parse_document_number_from_text("УПД № 77 от 01.02.2026") == "77"
    assert (
        dedup.normalize_document_number("0К00-005846")
        == dedup.normalize_document_number("5846")
        == "5846"
    )
    assert dedup.normalize_document_number("УТ-1042") == "1042"
    assert dedup.normalize_document_number("ABC") == "ABC"
