from datetime import datetime
from decimal import Decimal

import pytest
from sqlalchemy import select

from dz_fastapi.core.time import MOSCOW_TZ
from dz_fastapi.models.autopart import AutoPart, preprocess_oem_number
from dz_fastapi.models.brand import Brand
from dz_fastapi.models.diadoc import DiadocOutgoingDocument
from dz_fastapi.models.inventory import (
    ShipmentDocument,
    ShipmentDocumentItem,
    ShipmentDocumentStatus,
    SyncStatus,
)
from dz_fastapi.models.partner import Customer
from dz_fastapi.services.diadoc_shipment_import import (
    import_pending_upd_shipments,
    import_shipment_from_outgoing_upd,
)

UPD = """<?xml version="1.0" encoding="utf-8"?>
<Файл ВерсФорм="5.03">
  <Документ КНД="1115131" Функция="СЧФДОП">
    <СвСчФакт НомерДок="4890" ДатаДок="08.10.2026">
      <СвПрод><ИдСвед><СвЮЛУч НаимОрг="Мы" ИННЮЛ="7701234567"/></ИдСвед></СвПрод>
      <СвПокуп><ИдСвед><СвЮЛУч НаимОрг="Клиент" ИННЮЛ="7700000009"/></ИдСвед></СвПокуп>
    </СвСчФакт>
    <ТаблСчФакт>
      <СведТов НомСтр="1" НаимТов="Фильтр" КолТов="10" ЦенаТов="150.50"
               НалСт="20%" СтТовУчНал="1806.00">
        <ДопСведТов АртикулТов="A11-1012010" КрНаимСтрПр="КИТАЙ" КодПроисх="156"/>
        <СвДТ НомерДТ="10702070/120626/0011223"/>
        <НомСредИдентТов>010460123456789021ABC123</НомСредИдентТов>
      </СведТов>
      <СведТов НомСтр="2" НаимТов="Колодки" КолТов="4" ЦенаТов="820.00"
               СтТовУчНал="3936.00">
        <ДопСведТов АртикулТов="UNKNOWN-1"/>
      </СведТов>
    </ТаблСчФакт>
    <ВсегоОпл СтТовУчНалВсего="5742.00"/>
  </Документ>
</Файл>
"""


async def _setup(test_session, tmp_path, *, status="sent"):
    brand = Brand(name="TESTBRAND")
    customer = Customer(name="Клиент", inn="7700000009")
    test_session.add_all([brand, customer])
    await test_session.flush()
    test_session.add(
        AutoPart(
            brand_id=brand.id,
            oem_number=preprocess_oem_number("A11-1012010"),
            name="Фильтр",
        )
    )
    file_path = tmp_path / "upd.xml"
    file_path.write_text(UPD, encoding="utf-8")
    outgoing = DiadocOutgoingDocument(
        environment="staging",
        from_box_id_guid="box-1",
        to_box_id_guid="box-c",
        customer_id=customer.id,
        source_type="diadoc_external",
        type_named_id="UniversalTransferDocument",
        file_name="upd.xml",
        local_file_path=str(file_path),
        document_number="4890",
        entity_id="entity-1",
        message_id="message-1",
        status=status,
        is_draft=False,
    )
    test_session.add(outgoing)
    await test_session.commit()
    return customer, outgoing


@pytest.mark.asyncio
async def test_upd_becomes_imported_shipment_without_stock_effects(test_session, tmp_path):
    customer, outgoing = await _setup(test_session, tmp_path)

    result = await import_shipment_from_outgoing_upd(test_session, outgoing, our_inn="7701234567")
    await test_session.commit()

    assert result["created"] is True
    assert (result["matched"], result["unmatched"]) == (1, 1)
    shipment = (await test_session.execute(select(ShipmentDocument))).scalars().one()
    assert shipment.status == ShipmentDocumentStatus.IMPORTED
    assert shipment.sync_status == SyncStatus.SYNCED  # в 1С повторно не уходит
    assert shipment.customer_id == customer.id
    assert shipment.doc_number == "4890"
    assert shipment.source_diadoc_document_id == outgoing.id
    assert "На сопоставление: 1" in shipment.notes
    assert shipment.import_review[0]["article"] == "UNKNOWN-1"
    item = (await test_session.execute(select(ShipmentDocumentItem))).scalars().one()
    assert item.quantity == 10
    assert item.price == Decimal("180.60")  # 1806 / 10, с НДС
    assert item.vat_rate == Decimal("20")
    assert item.reserve_id is None and item.lot_id is None
    assert item.import_match == "exact"
    assert item.import_gtd_number == "10702070/120626/0011223"
    assert item.import_country_name == "КИТАЙ"
    assert item.import_marking_codes == ["010460123456789021ABC123"]
    assert item.import_vat_amount == Decimal("301.00")  # 1806 - 150.50 * 10

    again = await import_shipment_from_outgoing_upd(test_session, outgoing, our_inn="7701234567")
    assert again["created"] is False and again["reason"] == "already_imported"


@pytest.mark.asyncio
async def test_system_shipment_same_customer_and_total_is_not_duplicated(
    test_session, tmp_path
):
    customer, outgoing = await _setup(test_session, tmp_path)
    brand = (await test_session.execute(select(Brand))).scalars().one()
    part = AutoPart(brand_id=brand.id, oem_number="ZZ1", name="Деталь")
    test_session.add(part)
    await test_session.flush()
    ours = ShipmentDocument(
        doc_number="SHP-CO-000001",
        doc_date=datetime(2026, 10, 9, 10, 0, tzinfo=MOSCOW_TZ),
        status=ShipmentDocumentStatus.POSTED,
        customer_id=customer.id,
    )
    test_session.add(ours)
    await test_session.flush()
    test_session.add(
        ShipmentDocumentItem(
            document_id=ours.id,
            autopart_id=part.id,
            quantity=1,
            price=Decimal("5742.00"),
        )
    )
    await test_session.commit()

    result = await import_shipment_from_outgoing_upd(test_session, outgoing, our_inn="7701234567")

    assert result["created"] is False
    assert result["reason"] == "system_shipment_exists"
    assert result["shipment_id"] == ours.id


@pytest.mark.asyncio
async def test_revoked_and_foreign_seller_documents_are_skipped(test_session, tmp_path):
    _, revoked = await _setup(test_session, tmp_path, status="revoked")
    result = await import_shipment_from_outgoing_upd(test_session, revoked, our_inn="7701234567")
    assert result["reason"] == "revoked"

    revoked.status = "sent"
    revoked.metadata_json = {}
    foreign = await import_shipment_from_outgoing_upd(test_session, revoked, our_inn="9999999999")
    assert foreign["reason"] == "seller_is_not_us"


@pytest.mark.asyncio
async def test_pending_batch_imports_once_and_skips_known(test_session, tmp_path):
    await _setup(test_session, tmp_path)

    first = await import_pending_upd_shipments(test_session, environment="staging")
    second = await import_pending_upd_shipments(test_session, environment="staging")

    assert first["created"] == 1
    assert second["checked"] == 0


@pytest.mark.asyncio
async def test_same_article_of_other_product_is_not_linked_and_can_be_matched_by_hand(
    test_session, tmp_path
):
    from dz_fastapi.services.diadoc_shipment_import import name_score, resolve_review_line

    # Артикул совпал, но это другая деталь: связывать нельзя.
    assert name_score("Прокладка клапанной крышки TOYOTA", "С/б заднего рычага W204", "FEBI") < 0.5
    assert name_score("Фильтр", "Фильтр масляный", "X") >= 0.5

    customer, outgoing = await _setup(test_session, tmp_path)
    brand = (await test_session.execute(select(Brand))).scalars().one()
    other = AutoPart(brand_id=brand.id, oem_number="UNKNOWN1", name="Совсем другая деталь")
    test_session.add(other)
    await test_session.commit()

    await import_shipment_from_outgoing_upd(test_session, outgoing, our_inn="7701234567")
    await test_session.commit()
    shipment = (await test_session.execute(select(ShipmentDocument))).scalars().one()
    assert len(shipment.import_review) == 1  # артикул есть, название не совпало

    item = await resolve_review_line(test_session, shipment, index=0, autopart_id=other.id)
    await test_session.commit()
    assert item.import_match == "manual"
    assert item.quantity == 4
    assert shipment.import_review is None
