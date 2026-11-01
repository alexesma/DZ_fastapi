from datetime import date, datetime, timezone

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
from dz_fastapi.services.upd_xml import parse_correction_header

NOW_DOC_DATE = date(2026, 6, 15)


def ukd_xml(number="КСФ-77", day="15.06.2026", legacy=False) -> bytes:
    attrs = (
        f'НомерКСчФ="{number}" ДатаКСчФ="{day}"'
        if legacy
        else f'НомерДок="{number}" ДатаДок="{day}"'
    )
    return f"""<?xml version="1.0" encoding="utf-8"?>
<Файл ИдФайл="ON_NKORSCHFDOPPR_x"><Документ КНД="1115132">
<СвКСчФ {attrs}/></Документ></Файл>""".encode()


@pytest.mark.parametrize(
    "type_id,name,category",
    [
        ("UniversalTransferDocument", "ON_NSCHFDOPPR_x.xml", "upd"),
        ("UniversalCorrectionDocument", "ON_NKORSCHFDOPPR_x.xml", "ukd"),
        (None, "ON_NKORSCHFDOPPR_x.xml", "ukd"),
        (None, "ON_NAKLPOST_x.xml", "torg12"),
        ("XmlTorg12", "x.xml", "torg12"),
        (None, "ON_AKTPRE_x.xml", "act"),
        ("XmlAcceptanceCertificate", "x.xml", "act"),
        ("RoadTransportWaybill", "x.xml", "transport"),
        ("LogisticsWaybill", "x.xml", "transport"),
        (None, "ON_TRNACLGROT_x.xml", "transport"),
        ("Nonformalized", "Счет на оплату 5.pdf", "payment_invoice"),
        ("Invoice", "ON_SFAKT_x.xml", "invoice"),
        ("Nonformalized", "price.xlsx", "other"),
    ],
)
def test_document_classification(type_id, name, category):
    assert dedup.classify_diadoc_document(type_id, name) == category


def test_receipt_eligibility_by_category():
    assert dedup.is_receipt_document("UniversalTransferDocument", "a.xml")
    assert dedup.is_receipt_document(None, "ON_NAKLPOST_x.xml")
    assert dedup.is_receipt_document(None, "ON_NKORSCHFDOPPR_x.xml")
    assert not dedup.is_receipt_document(None, "ON_AKTPRE_x.xml")
    assert not dedup.is_receipt_document("LogisticsWaybill", "x.xml")
    assert not dedup.is_receipt_document("Nonformalized", "schet.pdf")


def test_correction_header_both_attribute_generations():
    assert parse_correction_header(ukd_xml()) == ("КСФ-77", NOW_DOC_DATE)
    assert parse_correction_header(ukd_xml(legacy=True)) == ("КСФ-77", NOW_DOC_DATE)
    assert parse_correction_header(b"<a/>") is None
    identity = dedup.identity_from_xml(ukd_xml(), "ON_NKORSCHFDOPPR_x.xml")
    assert identity.kind == "ukd" and identity.number == "77" and identity.total is None


async def make_doc(session, provider, tmp_path, name, type_id, payload=b"<x/>", n="1"):
    path = tmp_path / name
    path.write_bytes(payload)
    doc = DiadocIncomingDocument(
        environment="staging", box_id_guid="box", message_id=f"m{n}-{name}",
        entity_id=f"e{n}-{name}", provider_id=provider.id, file_name=name,
        document_number="КСФ-77", document_date=NOW_DOC_DATE,
        local_file_path=str(path), raw_metadata={"TypeNamedId": type_id},
    )
    session.add(doc)
    await session.commit()
    return doc


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "type_id,name,status",
    [
        ("LogisticsWaybill", "ON_TRNACLGROT_x.xml", "transport"),
        (None, "ON_AKTPRE_x.xml", "service_act"),
    ],
)
async def test_transport_and_acts_are_not_receipts(
    test_session, created_providers, tmp_path, type_id, name, status
):
    provider = created_providers[0]
    doc = await make_doc(test_session, provider, tmp_path, name, type_id)
    dedup.apply_category_status(doc)  # как при синхронизации списка
    assert doc.status == status
    doc.status = "synced"
    result = await process_diadoc_incoming_document(
        test_session, document=doc, download_content_if_missing=False
    )
    assert result["dedup_action"] in {"transport", "service_act"}
    await test_session.refresh(doc)
    assert doc.status == status
    assert doc.supplier_order_message_id is None
    assert (await test_session.execute(select(SupplierOrderMessage))).scalars().all() == []


@pytest.mark.asyncio
@pytest.mark.parametrize("posted", [True, False])
async def test_correction_never_changes_existing_receipt(
    test_session, created_providers, tmp_path, posted
):
    provider = created_providers[0]
    receipt = SupplierReceipt(
        provider_id=provider.id, document_number="77", document_date=NOW_DOC_DATE,
        posted_at=datetime(2026, 6, 15, tzinfo=timezone.utc) if posted else None,
    )
    test_session.add(receipt)
    await test_session.flush()
    test_session.add(SupplierReceiptItem(
        receipt_id=receipt.id, oem_number="A1", received_quantity=10,
    ))
    await test_session.commit()
    doc = await make_doc(
        test_session, provider, tmp_path, "ON_NKORSCHFDOPPR_x.xml",
        "UniversalCorrectionDocument", payload=ukd_xml(),
    )
    result = await process_diadoc_incoming_document(
        test_session, document=doc, download_content_if_missing=False
    )
    assert result["message_type"] == "CORRECTION_DOC"
    await test_session.refresh(receipt)
    assert (receipt.posted_at is not None) is posted
    item = (await test_session.execute(select(SupplierReceiptItem))).scalar_one()
    assert item.received_quantity == 10
    correction = (await test_session.execute(select(SupplierReceiptCorrection))).scalar_one()
    assert correction.status == "draft" and correction.receipt_id is None


@pytest.mark.asyncio
async def test_diadoc_correction_then_email_correction_is_duplicate(
    test_session, created_providers, tmp_path
):
    provider = created_providers[0]
    doc = await make_doc(
        test_session, provider, tmp_path, "ON_NKORSCHFDOPPR_x.xml",
        "UniversalCorrectionDocument", payload=ukd_xml(),
    )
    dedup.refresh_diadoc_identity_from_file(doc)
    await test_session.commit()
    assert doc.dedup_key.endswith("|ukd|77|2026-06-15")
    message = SupplierOrderMessage(
        provider_id=provider.id, message_type="UNKNOWN", received_at=doc.created_at
    )
    test_session.add(message)
    await test_session.flush()
    verdict = await dedup.apply_email_dedup(
        test_session, message_row=message, provider_id=provider.id,
        attachments=[("ON_NKORSCHFDOPPR_mail.xml", ukd_xml(), "h")],
    )
    assert verdict == "duplicate"
    assert message.duplicate_of_diadoc_id == doc.id


@pytest.mark.asyncio
async def test_email_correction_then_diadoc_correction_is_linked_not_doubled(
    test_session, created_providers, tmp_path
):
    provider = created_providers[0]
    identity = dedup.identity_from_xml(ukd_xml(), "ON_NKORSCHFDOPPR_x.xml")
    email = SupplierOrderMessage(
        provider_id=provider.id, message_type="CORRECTION_DOC", source_uid="9",
        received_at=datetime(2026, 6, 15, tzinfo=timezone.utc),
        document_kind="ukd", document_key=dedup.build_dedup_key(provider.id, identity),
    )
    test_session.add(email)
    await test_session.commit()
    doc = await make_doc(
        test_session, provider, tmp_path, "ON_NKORSCHFDOPPR_x.xml",
        "UniversalCorrectionDocument", payload=ukd_xml(),
    )
    result = await process_diadoc_incoming_document(
        test_session, document=doc, download_content_if_missing=False
    )
    assert result["dedup_action"] == "linked"
    await test_session.refresh(doc)
    assert doc.supplier_order_message_id == email.id
    assert (await test_session.execute(select(SupplierReceiptCorrection))).scalars().all() == []
