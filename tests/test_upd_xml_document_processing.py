from datetime import date
from types import SimpleNamespace

import pytest
from sqlalchemy import select

from dz_fastapi.core.time import now_moscow
from dz_fastapi.models.diadoc import DiadocIncomingDocument
from dz_fastapi.models.partner import (
    SUPPLIER_ORDER_STATUS,
    SupplierOrder,
    SupplierOrderItem,
    SupplierReceipt,
    SupplierReceiptItem,
    SupplierResponseConfig,
)
from dz_fastapi.services.diadoc_documents import process_diadoc_incoming_document
from dz_fastapi.services.supplier_order_responses import process_supplier_response_messages

MASK = r"^(UPD_Dop_\d{4}_.+\.xls|ON_NSCHFDOP.*\.xml)$"


def upd_xml(article: str, number="ТМРНБ-26-190198", qty=2, total="240.00") -> bytes:
    return f"""<?xml version="1.0" encoding="utf-8"?>
<Файл ИдФайл="ON_NSCHFDOPPR_x" ВерсФорм="5.03">
  <Документ КНД="1115131" Функция="СЧФДОП">
    <СвСчФакт НомерДок="{number}" ДатаДок="09.04.2026"/>
    <ТаблСчФакт>
      <СведТов НомСтр="1" НаимТов="Фильтр" КолТов="{qty}" ЦенаТов="100.00"
               СтТовУчНал="{total}">
        <ДопСведТов КодТов="{article}" КрНаимСтрПр="КИТАЙ"/>
        <СвДТ НомерДТ="10702070/120626/0011223"/>
      </СведТов>
      <ВсегоОпл СтТовУчНалВсего="{total}"/>
    </ТаблСчФакт>
  </Документ>
</Файл>""".encode()


async def prepare(session, provider, autopart):
    provider.email_contact = "supplier@example.com"
    order = SupplierOrder(provider_id=provider.id, status=SUPPLIER_ORDER_STATUS.SENT)
    session.add(order)
    await session.flush()
    item = SupplierOrderItem(
        supplier_order_id=order.id, autopart_id=autopart.id,
        oem_number=autopart.oem_number, brand_name=autopart.brand.name,
        autopart_name=autopart.name, quantity=3, price=100.0,
    )
    session.add(item)
    config = SupplierResponseConfig(
        provider_id=provider.id, name="Doc xml", sender_emails=["supplier@example.com"],
        response_type="file", file_payload_type="document",
        shipping_doc_filename_pattern=MASK, process_shipping_docs=True, is_active=True,
    )
    session.add(config)
    await session.commit()
    return order, item, config


async def receipts(session):
    return (await session.execute(select(SupplierReceipt))).scalars().all()


@pytest.mark.asyncio
async def test_email_upd_xml_marked_as_shipping_doc_creates_receipt(
    monkeypatch, test_session, created_providers, created_autopart, tmp_path
):
    provider = created_providers[0]
    order, item, _ = await prepare(test_session, provider, created_autopart)

    async def fake_fetch(session, *, date_from, date_to=None, **kwargs):
        return [(SimpleNamespace(
            from_="supplier@example.com", subject=f"Заказ поставщику #{order.id}: УПД",
            text="", html=None,
            attachments=[SimpleNamespace(
                filename="ON_NSCHFDOPPR_2BM_1.xml",
                payload=upd_xml(created_autopart.oem_number),
            )],
            received_at=None, date=None, external_id="xml-upd-1", uid=None,
            folder_name="INBOX",
        ), None)]

    monkeypatch.setattr(
        "dz_fastapi.services.supplier_order_responses._fetch_supplier_response_messages",
        fake_fetch,
    )
    monkeypatch.setattr(
        "dz_fastapi.services.supplier_order_responses.SUPPLIER_RESPONSE_DIR", str(tmp_path)
    )
    result = await process_supplier_response_messages(test_session)
    assert result["processed_messages"] == 1
    receipt = (await receipts(test_session))[0]
    assert receipt.document_number == "ТМРНБ-26-190198"
    row = (await test_session.execute(select(SupplierReceiptItem))).scalar_one()
    assert row.received_quantity == 2
    assert float(row.total_price_with_vat) == 240.0
    assert row.gtd_code == "10702070/120626/0011223"


@pytest.mark.asyncio
async def test_diadoc_upd_xml_creates_one_receipt_and_is_idempotent(
    test_session, created_providers, created_autopart, tmp_path
):
    provider = created_providers[0]
    order, item, config = await prepare(test_session, provider, created_autopart)
    path = tmp_path / "ON_NSCHFDOPPR_froza.xml"
    path.write_bytes(upd_xml(created_autopart.oem_number))
    doc = DiadocIncomingDocument(
        environment="staging", box_id_guid="box", message_id="m1", entity_id="e1",
        provider_id=provider.id, file_name=path.name, document_number="ТМРНБ-26-190198",
        document_date=date(2026, 4, 9), local_file_path=str(path),
        raw_metadata={"TypeNamedId": "UniversalTransferDocument"},
    )
    test_session.add(doc)
    await test_session.commit()
    result = await process_diadoc_incoming_document(
        test_session, document=doc, provider_id=provider.id,
        response_config_id=config.id, download_content_if_missing=False,
    )
    assert result.get("import_error_details") is None
    assert len(result["receipt_ids"]) == 1
    rows = (await test_session.execute(select(SupplierReceiptItem))).scalars().all()
    assert [r.received_quantity for r in rows] == [2]
    again = await process_diadoc_incoming_document(
        test_session, document=doc, provider_id=provider.id,
        response_config_id=config.id, download_content_if_missing=False,
    )
    assert again.get("already_processed") is True, again
    assert len(await receipts(test_session)) == 1


@pytest.mark.asyncio
async def test_diadoc_upd_xml_without_mask_is_recognised_by_file_name(
    test_session, created_providers, created_autopart, tmp_path
):
    provider = created_providers[0]
    order, item, config = await prepare(test_session, provider, created_autopart)
    config.shipping_doc_filename_pattern = None
    path = tmp_path / "ON_NSCHFDOPPR_nomask.xml"
    path.write_bytes(upd_xml(created_autopart.oem_number))
    doc = DiadocIncomingDocument(
        environment="staging", box_id_guid="box", message_id="m2", entity_id="e2",
        provider_id=provider.id, file_name=path.name, document_number="1",
        document_date=date(2026, 4, 9), local_file_path=str(path),
        raw_metadata={"TypeNamedId": "UniversalTransferDocument"},
    )
    test_session.add(doc)
    await test_session.commit()
    result = await process_diadoc_incoming_document(
        test_session, document=doc, provider_id=provider.id,
        response_config_id=config.id, download_content_if_missing=False,
    )
    assert len(result["receipt_ids"]) == 1


@pytest.mark.asyncio
async def test_non_upd_xml_does_not_break_processing(
    test_session, created_providers, created_autopart, tmp_path
):
    provider = created_providers[0]
    order, item, config = await prepare(test_session, provider, created_autopart)
    path = tmp_path / "ON_NSCHFDOPPR_broken.xml"
    path.write_bytes(b"<root><nothing/></root>")
    doc = DiadocIncomingDocument(
        environment="staging", box_id_guid="box", message_id="m3", entity_id="e3",
        provider_id=provider.id, file_name=path.name, document_number="2",
        document_date=date(2026, 4, 9), local_file_path=str(path),
        raw_metadata={"TypeNamedId": "UniversalTransferDocument"},
    )
    test_session.add(doc)
    await test_session.commit()
    result = await process_diadoc_incoming_document(
        test_session, document=doc, provider_id=provider.id,
        response_config_id=config.id, download_content_if_missing=False,
    )
    assert result["receipt_ids"] == []
    assert "ни одна товарная строка" in (result.get("import_error_details") or "")
    assert now_moscow()


def upd_with_shipment(article: str, invoice_date="09.04.2026", ship_date=None) -> bytes:
    ship = (
        f'<ДокПодтвОтгр НаимДокОтгр="УПД" НомДокОтгр="1" ДатаДокОтгр="{ship_date}"/>'
        if ship_date
        else ""
    )
    return f"""<?xml version="1.0" encoding="utf-8"?>
<Файл ВерсФорм="5.03"><Документ КНД="1115131">
<СвСчФакт НомерДок="55" ДатаДок="{invoice_date}">{ship}</СвСчФакт>
<ТаблСчФакт><СведТов НомСтр="1" НаимТов="Ф" КолТов="2" ЦенаТов="100.00" СтТовУчНал="240.00">
<ДопСведТов КодТов="{article}"/></СведТов><ВсегоОпл СтТовУчНалВсего="240.00"/></ТаблСчФакт>
</Документ></Файл>""".encode()


def test_shipment_date_preferred_over_invoice_date():
    from dz_fastapi.services.supplier_order_responses import _parse_upd_xml_attachment

    with_ship = _parse_upd_xml_attachment(upd_with_shipment("A1", ship_date="05.04.2026"), "a.xml")
    assert with_ship[0].document_date == date(2026, 4, 5)
    without = _parse_upd_xml_attachment(upd_with_shipment("A1"), "a.xml")
    assert without[0].document_date == date(2026, 4, 9)


@pytest.mark.asyncio
async def test_receipt_date_is_document_date_not_processing_date(
    test_session, created_providers, created_autopart, tmp_path
):
    """Строки без заказа (нет совпадения) раньше теряли дату документа."""
    provider = created_providers[0]
    order, item, config = await prepare(test_session, provider, created_autopart)
    path = tmp_path / "ON_NSCHFDOPPR_unmatched.xml"
    path.write_bytes(upd_with_shipment("NO-SUCH-ARTICLE-1", invoice_date="29.10.2026"))
    doc = DiadocIncomingDocument(
        environment="staging", box_id_guid="box", message_id="m9", entity_id="e9",
        provider_id=provider.id, file_name=path.name, document_number="55",
        document_date=date(2026, 10, 29), local_file_path=str(path),
        raw_metadata={"TypeNamedId": "UniversalTransferDocument"},
    )
    test_session.add(doc)
    await test_session.commit()
    await process_diadoc_incoming_document(
        test_session, document=doc, provider_id=provider.id,
        response_config_id=config.id, download_content_if_missing=False,
    )
    receipt = (await receipts(test_session))[0]
    assert receipt.document_date == date(2026, 10, 29)
    assert receipt.document_number == "55"


@pytest.mark.asyncio
async def test_fix_script_service_corrects_date_and_logs_in_comment(
    test_session, created_providers, tmp_path
):
    from dz_fastapi.models.partner import SupplierOrderAttachment, SupplierOrderMessage
    from dz_fastapi.services.supplier_receipt_fixes import fix_receipt_document_date

    provider = created_providers[0]
    path = tmp_path / "ON_NSCHFDOPPR_x.xml"
    path.write_bytes(upd_with_shipment("A1", invoice_date="29.10.2026"))
    message = SupplierOrderMessage(
        provider_id=provider.id, message_type="SHIPPING_DOC", received_at=now_moscow()
    )
    test_session.add(message)
    await test_session.flush()
    test_session.add(SupplierOrderAttachment(
        message_id=message.id, filename=path.name, file_path=str(path),
    ))
    receipt = SupplierReceipt(
        provider_id=provider.id, source_message_id=message.id,
        document_date=date(2026, 11, 1), posted_at=now_moscow(), comment="Авто",
    )
    test_session.add(receipt)
    await test_session.commit()
    dry = await fix_receipt_document_date(test_session, receipt_id=receipt.id)
    assert dry["status"] == "needs_fix" and dry["xml_date"] == date(2026, 10, 29)
    await test_session.refresh(receipt)
    assert receipt.document_date == date(2026, 11, 1)  # отчёт ничего не меняет
    done = await fix_receipt_document_date(test_session, receipt_id=receipt.id, apply=True)
    assert done["status"] == "fixed"
    await test_session.refresh(receipt)
    assert receipt.document_date == date(2026, 10, 29)
    assert receipt.posted_at is not None
    assert "Дата документа исправлена" in receipt.comment
