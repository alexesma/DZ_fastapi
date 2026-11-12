from datetime import date

import pytest
from sqlalchemy import select

from dz_fastapi.models.diadoc import DiadocIncomingDocument
from dz_fastapi.models.partner import (
    CustomerExternalReference,
    ProviderExternalReference,
    SupplierOrderMessage,
    SupplierReceipt,
)
from dz_fastapi.models.settings import DiadocIntegrationSettings
from dz_fastapi.services.diadoc_direction import (
    DIRECTION_CUSTOMER,
    DIRECTION_SUPPLIER,
    DIRECTION_UNKNOWN,
    detect_document_direction,
)
from dz_fastapi.services.diadoc_documents import (
    process_diadoc_incoming_document,
    register_diadoc_document_as_supplier_message,
)
from dz_fastapi.services.upd_xml import extract_document_parties

OUR_INN = "9731061118"
FROZA_INN = "7714388705"
BOX = "box-froza"


def party(tag, inn, kpp):
    return (
        f'<{tag}><ИдСвед><СвЮЛУч НаимОрг="X" ИННЮЛ="{inn}" КПП="{kpp}"/>'
        f"</ИдСвед></{tag}>"
    )


def upd(seller, buyer, root="СвСчФакт", file_id="ON_NSCHFDOPPR_x") -> bytes:
    return (
        f'<?xml version="1.0" encoding="utf-8"?><Файл ИдФайл="{file_id}">'
        f'<Документ КНД="1115131"><{root} НомерДок="77" ДатаДок="15.06.2026">'
        f"{party('СвПрод', *seller)}{party('СвПокуп', *buyer)}</{root}>"
        "</Документ></Файл>"
    ).encode()


US = (OUR_INN, "773101001")
FROZA = (FROZA_INN, "771401001")


async def setup(session, provider, customer, *, bind_provider=True, bind_customer=True):
    session.add(
        DiadocIntegrationSettings(environment="staging", organization_inn=OUR_INN)
    )
    if bind_provider:
        session.add(
            ProviderExternalReference(
                provider_id=provider.id, source_system="DIADOC_COUNTERAGENT_BOX",
                external_supplier_name=BOX,
            )
        )
    if bind_customer:
        session.add(
            CustomerExternalReference(
                customer_id=customer.id, source_system="DIADOC_COUNTERAGENT_BOX",
                external_customer_name=BOX,
            )
        )
    await session.commit()


async def make_doc(session, tmp_path, provider, payload, name, type_id, number="77"):
    path = tmp_path / name
    path.write_bytes(payload)
    doc = DiadocIncomingDocument(
        environment="staging", box_id_guid="mybox", message_id=f"m-{name}",
        entity_id=f"e-{name}", counteragent_box_id=BOX,
        provider_id=provider.id, file_name=name, document_number=number,
        document_date=date(2026, 6, 15), local_file_path=str(path),
        raw_metadata={"TypeNamedId": type_id},
    )
    session.add(doc)
    await session.commit()
    return doc


def test_parties_extracted_from_upd_and_ukd():
    parties = extract_document_parties(upd(FROZA, US))
    assert (parties.seller_inn, parties.buyer_inn) == (FROZA_INN, OUR_INN)
    assert parties.buyer_kpp == "773101001"
    # УКД/КСЧФ: нет СвСчФакт, но стороны читаются
    ukd = extract_document_parties(upd(US, FROZA, root="СвКСчФ"))
    assert (ukd.seller_inn, ukd.buyer_inn) == (OUR_INN, FROZA_INN)
    assert extract_document_parties(b"%PDF") is None


@pytest.mark.asyncio
async def test_supplier_upd_with_dual_bound_counteragent_is_supplier(
    test_session, created_providers, created_customers, tmp_path
):
    provider, customer = created_providers[0], created_customers[0]
    await setup(test_session, provider, customer)
    doc = await make_doc(
        test_session, tmp_path, provider, upd(FROZA, US), "ON_NSCHFDOPPR_a.xml",
        "UniversalTransferDocument",
    )
    assert await detect_document_direction(test_session, doc) == DIRECTION_SUPPLIER
    assert doc.customer_id is None


@pytest.mark.asyncio
async def test_ukd_for_our_sale_is_customer_document_not_receipt(
    test_session, created_providers, created_customers, tmp_path
):
    provider, customer = created_providers[0], created_customers[0]
    await setup(test_session, provider, customer)
    doc = await make_doc(
        test_session, tmp_path, provider,
        upd(US, FROZA, root="СвКСчФ", file_id="ON_KORSCHFDOPPR_x"),
        "ON_KORSCHFDOPPR_b.xml", "UniversalCorrectionDocument",
    )
    result = await process_diadoc_incoming_document(
        test_session, document=doc, download_content_if_missing=False
    )
    assert result["dedup_action"] == "customer"
    await test_session.refresh(doc)
    assert doc.direction == DIRECTION_CUSTOMER
    assert doc.customer_id == customer.id
    assert doc.status == "customer_doc"
    assert doc.supplier_order_message_id is None
    assert (await test_session.execute(select(SupplierOrderMessage))).scalars().all() == []
    assert (await test_session.execute(select(SupplierReceipt))).scalars().all() == []
    with pytest.raises(ValueError):
        await register_diadoc_document_as_supplier_message(test_session, document=doc)


@pytest.mark.asyncio
async def test_dual_bound_without_xml_needs_manual_decision(
    test_session, created_providers, created_customers, tmp_path
):
    provider, customer = created_providers[0], created_customers[0]
    await setup(test_session, provider, customer)
    doc = await make_doc(
        test_session, tmp_path, provider, b"PK-not-xml", "nakladnaya.xlsx", "Nonformalized"
    )
    result = await process_diadoc_incoming_document(
        test_session, document=doc, download_content_if_missing=False
    )
    assert result["dedup_action"] == "unknown"
    await test_session.refresh(doc)
    assert doc.direction == DIRECTION_UNKNOWN
    assert doc.status == "direction_unknown"
    assert doc.supplier_order_message_id is None


@pytest.mark.asyncio
async def test_provider_only_binding_without_xml_stays_supplier(
    test_session, created_providers, created_customers, tmp_path
):
    provider, customer = created_providers[0], created_customers[0]
    await setup(test_session, provider, customer, bind_customer=False)
    doc = await make_doc(
        test_session, tmp_path, provider, b"PK-not-xml", "nakladnaya.xlsx", "Nonformalized"
    )
    assert await detect_document_direction(test_session, doc) == DIRECTION_SUPPLIER


@pytest.mark.asyncio
async def test_xml_overrides_provider_only_binding(
    test_session, created_providers, created_customers, tmp_path
):
    provider, customer = created_providers[0], created_customers[0]
    await setup(test_session, provider, customer, bind_customer=False)
    doc = await make_doc(
        test_session, tmp_path, provider, upd(US, FROZA), "ON_NSCHFDOPPR_c.xml",
        "UniversalTransferDocument",
    )
    # продавец — мы, но клиентской привязки нет: customer без customer_id
    assert await detect_document_direction(test_session, doc) == DIRECTION_CUSTOMER
    assert doc.customer_id is None


@pytest.mark.asyncio
async def test_dual_bound_payment_invoice_pdf_is_supplier(
    test_session, created_providers, created_customers, tmp_path
):
    provider, customer = created_providers[0], created_customers[0]
    await setup(test_session, provider, customer)
    doc = await make_doc(
        test_session, tmp_path, provider, b"%PDF-1.4",
        "Счет на оплату №77 от 15.06.2026.pdf", "Nonformalized",
    )
    assert await detect_document_direction(test_session, doc) == DIRECTION_SUPPLIER
    assert doc.customer_id is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "file_name",
    ["Счет на оплату №77 от 15.06.2026.pdf", "scheta.pdf", "BuhSverka_11112026.xls"],
)
async def test_dual_bound_non_goods_document_is_skipped_without_decision(
    test_session, created_providers, created_customers, tmp_path, file_name
):
    provider, customer = created_providers[0], created_customers[0]
    await setup(test_session, provider, customer)
    doc = await make_doc(
        test_session, tmp_path, provider, b"%PDF-1.4", file_name, "Nonformalized"
    )
    result = await process_diadoc_incoming_document(
        test_session, document=doc, download_content_if_missing=False
    )
    assert result["dedup_action"] == "skipped"
    await test_session.refresh(doc)
    assert doc.status == "skipped"
    assert doc.supplier_order_message_id is None
    assert (await test_session.execute(select(SupplierReceipt))).scalars().all() == []
