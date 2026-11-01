from datetime import date, datetime, timezone
from decimal import Decimal
from types import SimpleNamespace

import pytest
from sqlalchemy import select

from dz_fastapi.models.diadoc import DiadocIncomingDocument
from dz_fastapi.models.notification import AppNotification
from dz_fastapi.models.partner import (
    SupplierOrderMessage,
    SupplierReceipt,
    SupplierReceiptCorrection,
    SupplierReceiptItem,
)
from dz_fastapi.models.user import User, UserRole, UserStatus
from dz_fastapi.services import supplier_document_dedup as dedup
from dz_fastapi.services.diadoc_documents import process_diadoc_incoming_document
from dz_fastapi.services.supplier_order_responses import process_supplier_response_messages

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
async def test_diadoc_reposts_posted_email_receipt(
    test_session, created_providers, tmp_path, monkeypatch
):
    provider = created_providers[0]
    message, receipt = await make_email_with_receipt(
        test_session, provider, total="1806.00", posted=True
    )
    doc = await make_diadoc(test_session, provider, tmp_path, total="2000.00")
    unposted = []

    async def fake_unpost(session, *, receipt_id, user):
        unposted.append(receipt_id)
        row = await session.get(SupplierReceipt, receipt_id)
        row.posted_at = None
        await session.commit()

    async def fake_process(session, *, document, **kwargs):
        return {"receipt_ids": []}

    monkeypatch.setattr(
        "dz_fastapi.services.supplier_workflow.unpost_supplier_receipt", fake_unpost
    )
    monkeypatch.setattr(
        "dz_fastapi.services.diadoc_documents.process_diadoc_incoming_document",
        fake_process,
    )
    result = await process_diadoc_incoming_document(
        test_session, document=doc, download_content_if_missing=False
    )
    assert result["dedup_action"] == "applied"
    assert unposted == [receipt.id]
    assert await receipts(test_session) == []
    await test_session.refresh(doc)
    assert doc.amount_mismatch_status == "applied"
    assert doc.mismatch_log[0]["result"] == "reposted"
    assert doc.mismatch_log[0]["old_total"] == "1806.00"


@pytest.mark.asyncio
async def test_unpostable_receipt_falls_back_to_correction(
    test_session, created_providers, tmp_path, monkeypatch
):
    provider = created_providers[0]
    message, receipt = await make_email_with_receipt(
        test_session, provider, total="1806.00", posted=True
    )
    doc = await make_diadoc(test_session, provider, tmp_path, total="2000.00")

    async def failing_unpost(session, *, receipt_id, user):
        raise ValueError("Товар уже расходован")

    monkeypatch.setattr(
        "dz_fastapi.services.supplier_workflow.unpost_supplier_receipt", failing_unpost
    )
    result = await process_diadoc_incoming_document(
        test_session, document=doc, download_content_if_missing=False
    )
    assert result["dedup_action"] == "applied"
    receipt_after = (await receipts(test_session))[0]
    assert receipt_after.id == receipt.id and receipt_after.posted_at is not None
    correction = (await test_session.execute(select(SupplierReceiptCorrection))).scalar_one()
    assert correction.status == "draft"
    await test_session.refresh(doc)
    assert doc.amount_mismatch_status == "correction_required"
    assert "расходован" in doc.import_error_details


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
    from dz_fastapi.services.supplier_order_responses import _parse_document_number_from_text

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


@pytest.mark.asyncio
async def test_mismatch_window_returns_until_admin_decides(
    test_session, created_providers, tmp_path, monkeypatch
):
    from datetime import timedelta

    from fastapi import HTTPException

    from dz_fastapi.api.notifications import handle_document_mismatch_action, mark_notification_read
    from dz_fastapi.core.time import now_moscow
    from dz_fastapi.schemas.notification import DocumentMismatchActionRequest

    admin = User(
        name="A", email="mm-admin@example.com", password_hash="x",
        role=UserRole.ADMIN, status=UserStatus.ACTIVE,
    )
    test_session.add(admin)
    provider = created_providers[0]
    await test_session.commit()
    await make_email_with_receipt(test_session, provider, total="1806.00", posted=False)
    doc = await make_diadoc(test_session, provider, tmp_path, total="2000.00")

    async def fake_process(session, *, document, **kwargs):
        return {"receipt_ids": []}

    monkeypatch.setattr(
        "dz_fastapi.services.diadoc_documents.process_diadoc_incoming_document",
        fake_process,
    )
    await dedup.evaluate_diadoc_document(test_session, document=doc)
    note = (
        await test_session.scalars(
            select(AppNotification).where(AppNotification.user_id == admin.id)
        )
    ).one()
    assert note.payload["notification_type"] == "supplier_document_mismatch_action"
    assert note.payload["diadoc_document_id"] == doc.id

    # без решения закрыть нельзя
    with pytest.raises(HTTPException) as exc:
        await mark_notification_read(note.id, session=test_session, current_user=admin)
    assert exc.value.status_code == 409

    # «Перейти к решению» откладывает окно на 30 минут: оно вернётся
    first = await handle_document_mismatch_action(
        note.id, DocumentMismatchActionRequest(action="go_to_resolution"),
        session=test_session, current_user=admin,
    )
    assert first.available_at >= now_moscow() + timedelta(minutes=29)
    again = await test_session.get(AppNotification, first.notification_id)
    assert again.read_at is None

    # решение администратора (проверено) закрывает окно навсегда
    await dedup.acknowledge_mismatch(test_session, document_id=doc.id)
    await test_session.refresh(again)
    assert again.read_at is not None


@pytest.mark.asyncio
async def test_price_over_order_by_more_than_one_ruble_is_flagged(
    test_session, created_providers
):
    from dz_fastapi.services.supplier_receipt_prices import (
        mark_receipt_price_discrepancies,
        price_exceeds_order,
    )

    assert price_exceeds_order(101.01, 100)
    assert not price_exceeds_order(101.00, 100)
    assert not price_exceeds_order(90, 100)  # дешевле — не помечаем
    assert not price_exceeds_order(100, None)

    provider = created_providers[0]
    message, receipt = await make_email_with_receipt(test_session, provider)
    item = (await test_session.execute(select(SupplierReceiptItem))).scalar_one()
    item.price = Decimal("120.00")
    await test_session.commit()
    # без связи с заказом сравнивать не с чем
    assert await mark_receipt_price_discrepancies(test_session, receipt_id=receipt.id) == 0
    await test_session.refresh(item)
    assert item.price_exceeds_order is False


@pytest.mark.asyncio
async def test_linked_item_priced_over_order_is_flagged_and_serialized(
    test_session, created_providers
):
    from dz_fastapi.models.partner import SUPPLIER_ORDER_STATUS, SupplierOrder, SupplierOrderItem
    from dz_fastapi.services.supplier_receipt_prices import mark_receipt_price_discrepancies
    from dz_fastapi.services.supplier_workflow import (
        get_supplier_receipt_detail,
        serialize_supplier_receipt,
    )

    provider = created_providers[0]
    order = SupplierOrder(provider_id=provider.id, status=SUPPLIER_ORDER_STATUS.SENT)
    test_session.add(order)
    await test_session.flush()
    order_item = SupplierOrderItem(
        supplier_order_id=order.id, quantity=10, price=Decimal("100.00")
    )
    test_session.add(order_item)
    await test_session.flush()
    message, receipt = await make_email_with_receipt(test_session, provider)
    item = (await test_session.execute(select(SupplierReceiptItem))).scalar_one()
    item.supplier_order_item_id = order_item.id
    item.price = Decimal("101.50")
    await test_session.commit()
    assert await mark_receipt_price_discrepancies(test_session, receipt_id=receipt.id) == 1
    await test_session.commit()
    detail = await get_supplier_receipt_detail(test_session, receipt_id=receipt.id)
    data = serialize_supplier_receipt(detail)
    assert data["has_price_alert"] is True
    assert data["items"][0]["price_exceeds_order"] is True
    assert data["items"][0]["order_price"] == Decimal("100.00")
    item.price = Decimal("100.80")
    assert await mark_receipt_price_discrepancies(test_session, receipt_id=receipt.id) == 0
