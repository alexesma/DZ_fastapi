"""Pure EnterpriseData serializers: required references and stable identities."""

from xml.etree import ElementTree as ET

import pytest

from dz_fastapi.services.one_c_enterprise_data_export import (
    DATA_NS,
    event_body_objects,
    object_xml,
    preview_outbox_event,
)

COMPANY = {"short_name": "Драгонзап", "inn": "1234567890", "kpp": "123456789"}
PARTY = {"id": 42, "name": "Контрагент", "inn": "1234567890"}
ITEM = {
    "item_id": 77,
    "autopart": {"autopart_id": 5, "brand": "TEST", "oem": "A-123", "name": "Деталь"},
    "quantity": 2,
    "price": "100.00",
    "vat_rate": "22",
}
PROVIDER = {
    **PARTY,
    "one_c_contract": {
        "name": "Договор поставки 42",
        "number": "42/26",
        "date": "2026-01-01",
    },
}
DOCUMENT = {
    "document_id": 123,
    "document_number": "ДЗ-123",
    "document_date": "2026-10-26",
    "items": [ITEM],
    "customer": PARTY,
    "provider": PROVIDER,
    "warehouse": {"name": "Основной"},
}


def _value(root: ET.Element, path: str) -> str | None:
    node = root.find("/".join(f"{{{DATA_NS}}}{part}" for part in path.split("/")))
    return node.text if node is not None else None


@pytest.mark.parametrize(
    "kind,party_path",
    [
        ("Документ.ЗаказКлиента", "Контрагент/Наименование"),
        ("Документ.ЗаказПоставщику", "КлючевыеСвойства/Контрагент/Наименование"),
        ("Документ.РеализацияТоваровУслуг", "Контрагент/Наименование"),
        ("Документ.ПоступлениеТоваровУслуг", "КлючевыеСвойства/Контрагент/Наименование"),
    ],
)
def test_document_references_and_stable_guid(kind: str, party_path: str):
    first = ET.fromstring(object_xml(kind, DOCUMENT, COMPANY))
    second = ET.fromstring(object_xml(kind, DOCUMENT, COMPANY))

    assert first.tag == f"{{{DATA_NS}}}{kind}"
    assert _value(first, party_path) == "Контрагент"
    assert _value(first, "КлючевыеСвойства/Дата") == "2026-10-26T00:00:00"
    assert _value(first, "КлючевыеСвойства/Номер") == "DZ000000123"
    assert _value(first, "ОбщиеСвойстваОбъектовФормата/Комментарий") == (
        "Исходный номер DragonZap: ДЗ-123"
    )
    assert _value(first, "КлючевыеСвойства/Ссылка") == _value(
        second, "КлючевыеСвойства/Ссылка"
    )
    assert _value(first, "Товары/Строка/ДанныеНоменклатуры/Номенклатура/Артикул") == "A-123"
    assert _value(first, "Товары/Строка/Количество") == "2"
    assert _value(first, "Склад/Наименование") == "Основной склад"


def test_logical_order_warehouse_uses_the_physical_unf_warehouse():
    document = {**DOCUMENT, "warehouse": {"name": "Склад Заказа"}}
    root = ET.fromstring(object_xml("Документ.ПоступлениеТоваровУслуг", document, COMPANY))
    assert _value(root, "Склад/Наименование") == "Основной склад"


def test_unnumbered_supplier_receipt_keeps_stable_unf_number():
    document = {**DOCUMENT, "document_number": None}
    root = ET.fromstring(object_xml("Документ.ПоступлениеТоваровУслуг", document, COMPANY))
    assert _value(root, "КлючевыеСвойства/Номер") == "DZ000000123"
    assert _value(root, "ОбщиеСвойстваОбъектовФормата/Комментарий") is None


def test_directory_objects_are_stable():
    customer = ET.fromstring(object_xml("Справочник.Контрагенты", PARTY))
    product = ET.fromstring(object_xml("Справочник.Номенклатура", ITEM["autopart"]))

    assert _value(customer, "КлючевыеСвойства/Наименование") == "Контрагент"
    assert _value(customer, "КлючевыеСвойства/Ссылка")
    assert _value(product, "КлючевыеСвойства/Артикул") == "A-123"
    assert _value(product, "КлючевыеСвойства/Ссылка")
    assert _value(product, "ЕдиницаИзмерения/ДанныеКлассификатора/Код") == "796"


def test_counterparty_identity_is_shared_between_customer_and_provider_roles():
    customer = ET.fromstring(object_xml("Справочник.Контрагенты", {**PARTY, "kind": "customer"}))
    provider = ET.fromstring(object_xml("Справочник.Контрагенты", {**PARTY, "kind": "provider"}))
    assert _value(customer, "КлючевыеСвойства/Ссылка") == _value(
        provider, "КлючевыеСвойства/Ссылка"
    )


def test_line_identity_includes_document_identity():
    first = ET.fromstring(object_xml("Документ.РеализацияТоваровУслуг", DOCUMENT, COMPANY))
    second = ET.fromstring(object_xml(
        "Документ.РеализацияТоваровУслуг", {**DOCUMENT, "document_id": 124}, COMPANY
    ))
    assert _value(first, "Товары/Строка/ИдентификаторСтроки") != _value(
        second, "Товары/Строка/ИдентификаторСтроки"
    )


def test_unsupported_correction_is_not_serialized_as_incomplete_document():
    with pytest.raises(ValueError, match="не реализован"):
        object_xml("Документ.КорректировкаРеализацииТоваров", DOCUMENT, COMPANY)


def test_outbox_preview_maps_only_supported_posted_events():
    shipment = ET.fromstring(preview_outbox_event("shipment", "posted", DOCUMENT, COMPANY))
    receipt = ET.fromstring(preview_outbox_event("supplier_receipt", "posted", DOCUMENT, COMPANY))
    assert shipment.tag == f"{{{DATA_NS}}}Документ.РеализацияТоваровУслуг"
    assert receipt.tag == f"{{{DATA_NS}}}Документ.ПоступлениеТоваровУслуг"
    with pytest.raises(ValueError, match="проведённых"):
        preview_outbox_event("shipment", "cancelled", DOCUMENT, COMPANY)
    with pytest.raises(ValueError, match="пока не реализована"):
        preview_outbox_event("shipment_correction", "posted", DOCUMENT, COMPANY)


def test_supplier_receipt_sends_contract_before_document():
    objects = event_body_objects("supplier_receipt", "posted", DOCUMENT, COMPANY)
    kinds = [node.tag.rsplit("}", 1)[-1] for node in objects]
    assert kinds == [
        "Справочник.Контрагенты",
        "Справочник.Договоры",
        "Справочник.Номенклатура",
        "Документ.ПоступлениеТоваровУслуг",
    ]
    contract = objects[1]
    receipt = objects[-1]
    assert _value(contract, "КлючевыеСвойства/ВидДоговора") == "СПоставщиком"
    assert _value(contract, "КлючевыеСвойства/Номер") == "42/26"
    assert _value(receipt, "ДанныеВзаиморасчетов/Договор/Ссылка") == _value(
        contract, "КлючевыеСвойства/Ссылка"
    )


def test_existing_one_c_contract_is_referenced_without_rewriting_it():
    provider = {
        **PROVIDER,
        "one_c_contract": {
            **PROVIDER["one_c_contract"],
            "guid": "5bd6a8b9-642b-4b56-94ef-a40cfcc96467",
        },
    }
    objects = event_body_objects(
        "supplier_receipt", "posted", {**DOCUMENT, "provider": provider}, COMPANY
    )
    assert "Справочник.Договоры" not in [node.tag.rsplit("}", 1)[-1] for node in objects]
    assert _value(objects[-1], "ДанныеВзаиморасчетов/Договор/Ссылка") == (
        "5bd6a8b9-642b-4b56-94ef-a40cfcc96467"
    )


def test_supplier_receipt_without_contract_fails_before_send():
    with pytest.raises(ValueError, match="договор 1С"):
        event_body_objects(
            "supplier_receipt", "posted", {**DOCUMENT, "provider": PARTY}, COMPANY
        )
