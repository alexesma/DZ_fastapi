from datetime import date, datetime, timezone

import pytest
from sqlalchemy import select

from dz_fastapi.api.diadoc import _needs_decision
from dz_fastapi.models.diadoc import DiadocIncomingDocument
from dz_fastapi.models.partner import (
    SupplierReceipt,
    SupplierReceiptCorrection,
    SupplierReceiptCorrectionItem,
    SupplierReceiptItem,
)
from dz_fastapi.services import supplier_document_dedup as dedup
from dz_fastapi.services.diadoc_documents import process_diadoc_incoming_document
from dz_fastapi.services.upd_xml import (
    parse_correction_xml,
    parse_discrepancy_act_xml,
    parse_torg12_xml,
)

SUPPLIER_INN = "7714388705"
OUR_INN = "9731061118"


def party(tag, inn, kpp):
    return f'<{tag}><ИдСв><СвЮЛУч НаимОрг="X" ИННЮЛ="{inn}" КПП="{kpp}"/></ИдСв></{tag}>'


def ukd(rows=None) -> bytes:
    rows = rows or [("1", "NL045", "2", "0", "949.56", "949.56", "1899.12", "0.00", "1899.12")]
    body = "".join(
        f'<СведТов НомСтр="{n}" НаимТов="Тов {art}" ПорНомТовВСЧФ="{n}" КолТовДо="{qb}" '
        f'КолТовПосле="{qa}" ЦенаТовДо="{pb}" ЦенаТовПосле="{pa}">'
        f'<СтТовБезНДС СтоимДоИзм="{tb}" СтоимПослеИзм="{ta}" СтоимУм="{tb}"/>'
        f'<СтТовУчНал СтоимДоИзм="{tb}" СтоимПослеИзм="{ta}" СтоимУм="{tb}"/>'
        f'<ДопСведТов КодТов="{art}"/></СведТов>'
        for n, art, qb, qa, pb, pa, tb, ta, _ in rows
    )
    return (
        '<?xml version="1.0" encoding="utf-8"?><Файл ИдФайл="ON_NKORSCHFDOPPR_x">'
        '<Документ КНД="1115132"><СвКСчФ НомерКСчФ="40826" ДатаКСчФ="29.10.2026">'
        '<СчФ НомерСчФ="37242" ДатаСчФ="05.10.2026"/>'
        f'{party("СвПрод", SUPPLIER_INN, "771401001")}{party("СвПокуп", OUR_INN, "773101001")}'
        f"</СвКСчФ><ТаблКСчФ>{body}<ВсегоУм СтТовУчНалВсего=\"1899.12\"/></ТаблКСчФ>"
        "</Документ></Файл>"
    ).encode()


def test_ukd_parser_reads_real_structure():
    doc = parse_correction_xml(ukd())
    assert (doc.number, doc.date) == ("40826", date(2026, 10, 29))
    assert (doc.original_number, doc.original_date) == ("37242", date(2026, 10, 5))
    assert doc.total_delta == 1899.12
    row = doc.rows[0]
    assert (row.article, row.quantity_before, row.quantity_after) == ("NL045", 2, 0)
    assert (row.total_before, row.total_after, row.total_delta) == (1899.12, 0.0, 1899.12)
    assert (doc.parties.seller_inn, doc.parties.buyer_inn) == (SUPPLIER_INN, OUR_INN)


async def make_doc(session, provider, tmp_path, name, type_id, payload, number="40826"):
    path = tmp_path / name
    path.write_bytes(payload)
    doc = DiadocIncomingDocument(
        environment="staging", box_id_guid="box", message_id=f"m-{name}",
        entity_id=f"e-{name}", provider_id=provider.id, file_name=name,
        document_number=number, document_date=date(2026, 10, 29),
        local_file_path=str(path), raw_metadata={"TypeNamedId": type_id},
    )
    session.add(doc)
    await session.commit()
    return doc


async def make_receipt(session, provider, *, posted, number="37242", day=date(2026, 10, 5)):
    receipt = SupplierReceipt(
        provider_id=provider.id, document_number=number, document_date=day,
        posted_at=datetime(2026, 10, 6, tzinfo=timezone.utc) if posted else None,
    )
    session.add(receipt)
    await session.flush()
    session.add_all([
        SupplierReceiptItem(receipt_id=receipt.id, oem_number="NL-045", received_quantity=2),
        SupplierReceiptItem(receipt_id=receipt.id, oem_number="ZZ1", received_quantity=5),
    ])
    await session.commit()
    return receipt


@pytest.mark.asyncio
@pytest.mark.parametrize("posted", [True, False])
async def test_correction_is_matched_to_original_receipt_without_changing_it(
    test_session, created_providers, tmp_path, posted
):
    provider = created_providers[0]
    receipt = await make_receipt(test_session, provider, posted=posted)
    doc = await make_doc(
        test_session, provider, tmp_path, "ON_NKORSCHFDOPPR_x.xml",
        "UniversalCorrectionDocument", ukd(),
    )
    result = await process_diadoc_incoming_document(
        test_session, document=doc, download_content_if_missing=False
    )
    correction = await test_session.get(SupplierReceiptCorrection, result["correction_id"])
    assert correction.receipt_id == receipt.id
    assert correction.status == "draft" and correction.posted_at is None
    items = (await test_session.execute(select(SupplierReceiptCorrectionItem))).scalars().all()
    assert [(i.quantity_before, i.quantity_after) for i in items] == [(2, 0)]
    await test_session.refresh(doc)
    assert doc.parsed_details["state"] == "matched"
    assert doc.parsed_details["receipt_posted"] is posted
    # исходное поступление и остатки не тронуты
    lines = (await test_session.execute(select(SupplierReceiptItem))).scalars().all()
    assert sorted(i.received_quantity for i in lines) == [2, 5]
    await test_session.refresh(receipt)
    assert (receipt.posted_at is not None) is posted


@pytest.mark.asyncio
async def test_correction_without_original_receipt_needs_manual_binding(
    test_session, created_providers, tmp_path
):
    provider = created_providers[0]
    await make_receipt(test_session, provider, posted=True, number="99999")
    doc = await make_doc(
        test_session, provider, tmp_path, "ON_NKORSCHFDOPPR_y.xml",
        "UniversalCorrectionDocument", ukd(),
    )
    result = await process_diadoc_incoming_document(
        test_session, document=doc, download_content_if_missing=False
    )
    correction = await test_session.get(SupplierReceiptCorrection, result["correction_id"])
    assert correction.receipt_id is None
    await test_session.refresh(doc)
    assert doc.parsed_details["state"] == "receipt_not_found"
    assert "вручную" in doc.import_error_details
    assert _needs_decision(doc, {correction.id: "draft"}) is True
    assert _needs_decision(doc, {correction.id: "posted"}) is False


@pytest.mark.asyncio
async def test_correction_flags_unmatched_rows_and_total_mismatch(
    test_session, created_providers, tmp_path
):
    provider = created_providers[0]
    await make_receipt(test_session, provider, posted=True)
    rows = [
        ("1", "NL045", "2", "0", "949.56", "949.56", "1899.12", "0.00", ""),
        ("2", "NOPE", "1", "0", "100", "100", "100.00", "0.00", ""),
    ]
    doc = await make_doc(
        test_session, provider, tmp_path, "ON_NKORSCHFDOPPR_z.xml",
        "UniversalCorrectionDocument", ukd(rows),
    )
    await process_diadoc_incoming_document(
        test_session, document=doc, download_content_if_missing=False
    )
    await test_session.refresh(doc)
    details = doc.parsed_details
    assert len(details["unmatched"]) == 1 and details["unmatched"][0]["article"] == "NOPE"
    assert details["total_mismatch"] == {"rows": 1999.12, "document": 1899.12}


def test_torg12_parser_is_tolerant_and_never_invents_rows():
    xml = (
        '<Файл><Документ КНД="1175004"><СвТНО НомТН="55" ДатаТН="05.10.2026"/>'
        f'{party("Поставщик", SUPPLIER_INN, "1")}{party("Плательщик", OUR_INN, "2")}'
        '<ТаблТов><СведТов НомСтр="1" НаимТов="Фильтр" КолТов="3" СтТовУчНал="300.00">'
        '<ДопСведТов КодТов="A1"/></СведТов></ТаблТов></Документ></Файл>'
    ).encode()
    doc = parse_torg12_xml(xml)
    assert (doc.document_number, doc.document_date) == ("55", date(2026, 10, 5))
    assert doc.items[0].oem_number == "A1" and doc.items[0].quantity == 3
    assert doc.total_with_vat == 300.0 and doc.seller_inn == SUPPLIER_INN
    assert parse_torg12_xml(b"<a><b/></a>").items == []
    identity = dedup.identity_from_xml(xml, "ON_NAKLPOST_x.xml")
    assert (identity.kind, identity.number) == ("torg12", "55")


@pytest.mark.asyncio
async def test_service_act_is_kept_with_parties_and_total(
    test_session, created_providers, tmp_path
):
    provider = created_providers[0]
    xml = (
        '<Файл><Документ>' + party("СвПрод", SUPPLIER_INN, "1")
        + party("СвПокуп", OUR_INN, "2") + '<ВсегоОпл СтТовУчНалВсего="5000.00"/>'
        "</Документ></Файл>"
    ).encode()
    doc = await make_doc(
        test_session, provider, tmp_path, "ON_AKTTPRE_x.xml", None, xml, number="A-1"
    )
    result = await process_diadoc_incoming_document(
        test_session, document=doc, download_content_if_missing=False
    )
    assert result["dedup_action"] == "service_act"
    await test_session.refresh(doc)
    assert doc.status == "service_act" and doc.document_kind == "act"
    assert float(doc.total_amount) == 5000.0
    assert doc.parsed_details["buyer_inn"] == OUR_INN
    assert _needs_decision(doc, {}) is True


@pytest.mark.asyncio
async def test_discrepancy_act_is_stored_for_decision_not_processed(
    test_session, created_providers, tmp_path
):
    provider = created_providers[0]
    xml = (
        '<Файл><СвУчДокОбор/><Документ><ИдентДокПР НомерДок="7" ДатаДок="30.10.2026"/>'
        '<СопрДок НомОсн="55" ДатаОсн="28.10.2026"/>'
        '<РезПрием><ПоДокум Кол="10"/><ПоФакту Кол="8"/><Брак Кол="1"/></РезПрием>'
        "</Документ></Файл>"
    ).encode()
    assert parse_discrepancy_act_xml(xml)["result"][0]["ПоФакту"] == [{"Кол": "8"}]
    doc = await make_doc(
        test_session, provider, tmp_path, "DP_PRIRASXPRIN_x.xml", None, xml, number="7"
    )
    result = await process_diadoc_incoming_document(
        test_session, document=doc, download_content_if_missing=False
    )
    assert result["dedup_action"] == "discrepancy_act"
    await test_session.refresh(doc)
    assert doc.status == "discrepancy_act"
    assert doc.parsed_details["basis"] == [{"НомОсн": "55", "ДатаОсн": "28.10.2026"}]
    assert doc.supplier_order_message_id is None


@pytest.mark.asyncio
async def test_unknown_type_is_kept_as_other_not_error(
    test_session, created_providers, tmp_path
):
    provider = created_providers[0]
    doc = await make_doc(
        test_session, provider, tmp_path, "something.zip", "WeirdType", b"PK", number="9"
    )
    result = await process_diadoc_incoming_document(
        test_session, document=doc, download_content_if_missing=False
    )
    assert result["dedup_action"] == "other_doc"
    await test_session.refresh(doc)
    assert doc.status == "other_doc" and doc.document_kind == "other"
    assert doc.supplier_order_message_id is None


def test_diadoc_type_does_not_override_upd_file_name():
    # XmlTorg12 / XmlAcceptanceCertificate с УПД внутри — это УПД
    for type_id in ("XmlTorg12", "XmlAcceptanceCertificate", "Nonformalized"):
        assert dedup.classify_diadoc_document(type_id, "ON_NSCHFDOPPR_x.xml") == "upd"
    assert dedup.classify_diadoc_document("XmlAcceptanceCertificate", "DP_REZRUISP_x.xml") == "act"


UPD_BODY = (
    '<?xml version="1.0" encoding="utf-8"?><Файл ВерсФорм="5.03"><Документ КНД="1115131">'
    '<СвСчФакт НомерДок="256176" ДатаДок="24.06.2026">{parties}</СвСчФакт>'
    '<ТаблСчФакт><СведТов НомСтр="1" НаимТов="Ф" КолТов="1" ЦенаТов="10.00" '
    'СтТовУчНал="10.00"><ДопСведТов КодТов="A1"/></СведТов>'
    '<ВсегоОпл СтТовУчНалВсего="10.00"/></ТаблСчФакт>{extra}</Документ></Файл>'
)


def upd_with(parties="", extra="") -> bytes:
    return UPD_BODY.format(parties=parties, extra=extra).encode()


@pytest.mark.asyncio
async def test_acceptance_certificate_type_with_upd_content_is_processed_as_upd(
    test_session, created_providers, tmp_path
):
    provider = created_providers[0]
    doc = await make_doc(
        test_session, provider, tmp_path, "document.xml", "XmlAcceptanceCertificate",
        upd_with(), number="256176",
    )
    assert dedup.classify_diadoc_document("XmlAcceptanceCertificate", "document.xml") == "act"
    assert dedup.effective_category(doc) == "upd"


@pytest.mark.asyncio
async def test_service_act_dp_rezruisp_keeps_parties_total_basis_and_works(
    test_session, created_providers, tmp_path
):
    provider = created_providers[0]
    xml = (
        '<Файл ВерсФорм="5.02"><СвУчДокОбор/><Документ><СвДокПРУ>'
        '<Исполнитель><ИдСв><СвИП ИННФЛ="500100732259"/></ИдСв></Исполнитель>'
        f'{party("Заказчик", OUR_INN, "1")}'
        '<Основание НаимОсн="Оферта" ДатаОсн="13.05.2022"/>'
        '<ОписРабот><Работа Описание="Услуги по настройке"/></ОписРабот>'
        "</СвДокПРУ></Документ></Файл>"
    ).encode()
    doc = await make_doc(
        test_session, provider, tmp_path, "DP_REZRUISP_x.xml", "XmlAcceptanceCertificate",
        xml, number="БП-1443",
    )
    doc.raw_metadata = {
        "TypeNamedId": "XmlAcceptanceCertificate",
        "XmlTorg12Metadata": {"Total": "15000.00", "Grounds": "Оферта от 13.05.2022",
                              "DocumentStatus": "InboundWaitingForRecipientSignature"},
    }
    await test_session.commit()
    result = await process_diadoc_incoming_document(
        test_session, document=doc, download_content_if_missing=False
    )
    assert result["dedup_action"] == "service_act"
    await test_session.refresh(doc)
    details = doc.parsed_details
    assert float(doc.total_amount) == 15000.0
    assert details["works"] == ["Услуги по настройке"]
    assert details["buyer_inn"] == OUR_INN and details["seller_inn"] == "500100732259"
    assert details["grounds"] == "Оферта от 13.05.2022"


async def bind_both(session, provider, customer):
    from dz_fastapi.models.partner import CustomerExternalReference, ProviderExternalReference
    from dz_fastapi.models.settings import DiadocIntegrationSettings

    session.add_all([
        DiadocIntegrationSettings(environment="staging", organization_inn=OUR_INN),
        ProviderExternalReference(
            provider_id=provider.id, source_system="DIADOC_COUNTERAGENT_BOX",
            external_supplier_name="emex-box",
        ),
        CustomerExternalReference(
            customer_id=customer.id, source_system="DIADOC_COUNTERAGENT_BOX",
            external_customer_name="emex-box",
        ),
    ])
    await session.commit()


@pytest.mark.asyncio
@pytest.mark.parametrize("bound_as_customer", [True, False])
async def test_return_grounds_from_dual_counteragent_is_possible_return(
    test_session, created_providers, created_customers, tmp_path, bound_as_customer
):
    provider, customer = created_providers[0], created_customers[0]
    parties = party("СвПрод", SUPPLIER_INN, "1") + party("СвПокуп", OUR_INN, "2")
    doc = await make_doc(
        test_session, provider, tmp_path, "ON_NSCHFDOPPR_ret.xml", "XmlTorg12",
        upd_with(parties), number="256176",
    )
    doc.counteragent_box_id = "emex-box"
    doc.raw_metadata = {
        "TypeNamedId": "XmlTorg12",
        "XmlTorg12Metadata": {"Grounds": "возврат товара по документу УПД 2076 от 20.05.2026"},
    }
    if bound_as_customer:
        await bind_both(test_session, provider, customer)
    else:
        from dz_fastapi.models.settings import DiadocIntegrationSettings

        test_session.add(DiadocIntegrationSettings(environment="staging", organization_inn=OUR_INN))
        await test_session.commit()
    await test_session.commit()
    if not bound_as_customer:
        # возврата не видим: документ идёт дальше как обычная закупка
        # (здесь останавливается лишь на отсутствии настройки поставщика)
        with pytest.raises(ValueError, match="response config"):
            await process_diadoc_incoming_document(
                test_session, document=doc, download_content_if_missing=False
            )
        await test_session.refresh(doc)
        assert doc.status != "possible_return"
        return
    result = await process_diadoc_incoming_document(
        test_session, document=doc, download_content_if_missing=False
    )
    await test_session.refresh(doc)
    if bound_as_customer:
        assert result["dedup_action"] == "possible_return"
        assert doc.status == "possible_return"
        assert doc.parsed_details["possible_return"] is True
        assert doc.supplier_order_message_id is None
        assert _needs_decision(doc, {}) is True


def test_return_marker_detects_text_in_xml_and_metadata(tmp_path):
    from dz_fastapi.services.diadoc_details import detect_return_marker

    path = tmp_path / "x.xml"
    path.write_bytes(upd_with(extra='<ИнфПолФХЖ1 Текст="Возврат товара по УПД 5"/>'))
    doc = DiadocIncomingDocument(local_file_path=str(path), raw_metadata={})
    assert "Возврат" in detect_return_marker(doc)
    path.write_bytes(upd_with())
    assert detect_return_marker(doc) is None
    doc.raw_metadata = {"XmlTorg12Metadata": {"Grounds": "Возвр. по накладной"}}
    assert detect_return_marker(doc) == "Возвр. по накладной"
