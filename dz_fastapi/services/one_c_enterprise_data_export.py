"""EnterpriseData 1.22 object serialization for the DragonZap-owned data.

The emitted object order and required fields follow the official 1C XSD.
The caller must still negotiate each object type with the target UNF node
before placing a Body in the FTP exchange.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any
from uuid import NAMESPACE_URL, uuid5
from xml.etree import ElementTree as ET

DATA_NS = "http://v8.1c.ru/edi/edi_stnd/EnterpriseData/1.22"
SUPPORTED_OBJECTS = frozenset({
    "Справочник.Контрагенты",
    "Справочник.Номенклатура",
    "Документ.ЗаказКлиента",
    "Документ.ЗаказПоставщику",
    "Документ.РеализацияТоваровУслуг",
    "Документ.ПоступлениеТоваровУслуг",
})


def _tag(name: str) -> str:
    return f"{{{DATA_NS}}}{name}"


def _add(parent: ET.Element, name: str, value: Any = None) -> ET.Element:
    node = ET.SubElement(parent, _tag(name))
    if value is not None:
        node.text = str(value)
    return node


def _required(value: Any, label: str) -> str:
    result = str(value or "").strip()
    if not result:
        raise ValueError(f"Для EnterpriseData не заполнено поле «{label}»")
    return result


def _optional(parent: ET.Element, name: str, value: Any) -> None:
    if value is not None and str(value).strip():
        _add(parent, name, value)


def _guid(kind: str, identity: Any) -> str:
    return str(uuid5(NAMESPACE_URL, f"https://dragonzap.online/1c/{kind}/{identity}"))


def _datetime(value: Any) -> str:
    if isinstance(value, datetime):
        return value.replace(tzinfo=None).isoformat(timespec="seconds")
    if isinstance(value, date):
        return datetime.combine(value, datetime.min.time()).isoformat(timespec="seconds")
    raw = _required(value, "дата документа")
    if "T" not in raw:
        return f"{raw}T00:00:00"
    return raw[:19]


def _decimal(value: Any) -> str:
    return format(Decimal(str(value or "0")), "f")


def _amount(items: list[dict[str, Any]]) -> Decimal:
    total = Decimal("0")
    for item in items:
        quantity = Decimal(str(item.get("quantity") or item.get("requested_qty") or 0))
        if item.get("total_price_with_vat") is not None:
            total += Decimal(str(item["total_price_with_vat"]))
        else:
            total += quantity * Decimal(str(item.get("price") or item.get("requested_price") or 0))
    return total


def _organization(parent: ET.Element, company: dict[str, Any]) -> None:
    org = _add(parent, "Организация")
    name = company.get("short_name") or company.get("full_name")
    _add(org, "Наименование", _required(name, "наименование организации"))
    _optional(org, "НаименованиеСокращенное", company.get("short_name"))
    _optional(org, "НаименованиеПолное", company.get("full_name"))
    _optional(org, "ИНН", company.get("inn"))
    _optional(org, "КПП", company.get("kpp"))
    _add(
        org,
        "ЮридическоеФизическоеЛицо",
        "ФизическоеЛицо" if company.get("is_individual_entrepreneur") else "ЮридическоеЛицо",
    )


def _counterparty_key(parent: ET.Element, party: dict[str, Any], kind: str) -> None:
    name = party.get("legal_name") or party.get("name")
    inn = str(party.get("inn") or "").strip()
    kpp = str(party.get("kpp") or "").strip()
    # Один и тот же контрагент может быть клиентом и поставщиком DragonZap.
    # ИНН/КПП объединяют роли; локальный id нужен только без налогового ключа.
    identity = f"inn:{inn}:kpp:{kpp}" if inn else (
        f"{kind}:{party['id']}" if party.get("id") else None
    )
    _optional(parent, "Ссылка", _guid("counterparty", identity) if identity else None)
    _add(parent, "Наименование", _required(name, "наименование контрагента"))
    _optional(parent, "НаименованиеПолное", party.get("legal_name"))
    _optional(parent, "ИНН", party.get("inn"))
    _optional(parent, "КПП", party.get("kpp"))
    _add(
        parent,
        "ЮридическоеФизическоеЛицо",
        "ФизическоеЛицо" if len(inn) == 12 else "ЮридическоеЛицо",
    )


def _product_key(parent: ET.Element, item: dict[str, Any]) -> None:
    product = item.get("physical") or item.get("autopart") or item
    product_id = product.get("autopart_id") or product.get("id")
    article = product.get("oem") or product.get("oem_number") or item.get("oem")
    brand = product.get("brand") or item.get("brand")
    title = product.get("name") or item.get("name")
    name = " ".join(str(v).strip() for v in (brand, article, title) if v)
    _optional(parent, "Ссылка", _guid("autopart", product_id) if product_id else None)
    _optional(parent, "НаименованиеПолное", name)
    _optional(parent, "КодВПрограмме", f"DZ-{product_id}" if product_id else None)
    _optional(parent, "Артикул", article)
    _add(parent, "Наименование", _required(name, "наименование номенклатуры"))


def _currency(parent: ET.Element) -> None:
    currency = _add(parent, "Валюта")
    classifier = _add(currency, "ДанныеКлассификатора")
    _add(classifier, "Код", "643")
    _add(classifier, "Наименование", "Российский рубль")


def _unit(parent: ET.Element) -> None:
    unit = _add(parent, "ЕдиницаИзмерения")
    classifier = _add(unit, "ДанныеКлассификатора")
    _add(classifier, "Код", "796")
    _add(classifier, "Наименование", "шт")


def _vat(parent: ET.Element, rate: Any = "22") -> None:
    if rate is None:
        return
    vat = _add(parent, "СтавкаНДС")
    _add(vat, "Ставка", _decimal(rate))
    _add(vat, "РасчетнаяСтавка", "false")
    _add(vat, "НеОблагается", "false")
    _add(vat, "ВидСтавки", "Общая")


def _warehouse(parent: ET.Element, warehouse: dict[str, Any] | None) -> None:
    if not warehouse or not warehouse.get("name"):
        return
    node = _add(parent, "Склад")
    _add(node, "Наименование", warehouse["name"])


def _document_key(
    parent: ET.Element,
    payload: dict[str, Any],
    company: dict[str, Any],
    kind: str,
) -> None:
    key = _add(parent, "КлючевыеСвойства")
    _optional(key, "Ссылка", _guid(kind, payload["document_id"]))
    _add(key, "Дата", _datetime(payload.get("document_date") or payload.get("created_at")))
    _add(key, "Номер", _required(payload.get("document_number"), "номер документа"))
    _organization(key, company)


def _goods_row(
    parent: ET.Element,
    item: dict[str, Any],
    *,
    require_unit: bool,
    document_identity: str,
) -> None:
    row = _add(parent, "Строка")
    _optional(row, "НомерСтрокиДокумента", item.get("row_index"))
    product = _add(row, "ДанныеНоменклатуры")
    _product_key(_add(product, "Номенклатура"), item)
    if require_unit:
        _unit(row)
    quantity = item.get("quantity")
    if quantity is None:
        quantity = item.get("requested_qty")
    if quantity is None:
        raise ValueError("В строке EnterpriseData нет количества")
    _add(row, "Количество", _decimal(quantity))
    amount = item.get("total_price_with_vat")
    price = item.get("price") if item.get("price") is not None else item.get("requested_price")
    if amount is None and price is not None:
        amount = Decimal(str(quantity)) * Decimal(str(price))
    _optional(row, "Сумма", _decimal(amount) if amount is not None else None)
    _optional(row, "Цена", _decimal(price) if price is not None else None)
    _vat(row, item.get("vat_rate", "22"))
    _optional(
        row,
        "ИдентификаторСтроки",
        _guid("line", f"{document_identity}:{item['item_id']}")
        if item.get("item_id") else None,
    )


def serialize_object(
    object_type: str,
    payload: dict[str, Any],
    company: dict[str, Any] | None = None,
) -> ET.Element:
    """Build one XSD-ordered business object; no FTP side effect."""
    if object_type not in SUPPORTED_OBJECTS:
        raise ValueError(f"Тип EnterpriseData не реализован: {object_type}")
    node = ET.Element(_tag(object_type))
    if object_type == "Справочник.Контрагенты":
        _counterparty_key(_add(node, "КлючевыеСвойства"), payload, payload.get("kind", "customer"))
        return node
    if object_type == "Справочник.Номенклатура":
        _product_key(_add(node, "КлючевыеСвойства"), payload)
        _add(node, "ТипНоменклатуры", "Товар")
        return node

    if company is None:
        raise ValueError("Для документа EnterpriseData нужны реквизиты организации")
    kind = {
        "Документ.ЗаказКлиента": "customer-order",
        "Документ.ЗаказПоставщику": "supplier-order",
        "Документ.РеализацияТоваровУслуг": "shipment",
        "Документ.ПоступлениеТоваровУслуг": "receipt",
    }[object_type]
    document_identity = f"{kind}:{payload['document_id']}"
    _document_key(node, payload, company, kind)

    if object_type == "Документ.ЗаказКлиента":
        _currency(node)
        _add(node, "Сумма", _decimal(_amount(payload.get("items") or [])))
        _warehouse(node, payload.get("warehouse"))
        _counterparty_key(_add(node, "Контрагент"), payload["customer"], "customer")
        _add(node, "СуммаВключаетНДС", "true")
        rows = _add(node, "Товары")
        for item in payload.get("items") or []:
            _goods_row(rows, item, require_unit=False, document_identity=document_identity)
    elif object_type == "Документ.ЗаказПоставщику":
        key = node.find(_tag("КлючевыеСвойства"))
        _counterparty_key(_add(key, "Контрагент"), payload["provider"], "provider")
        _currency(node)
        _add(node, "Сумма", _decimal(_amount(payload.get("items") or [])))
        _warehouse(node, payload.get("warehouse"))
        _add(node, "ДанныеВзаиморасчетов")
        _add(node, "СуммаВключаетНДС", "true")
        rows = _add(node, "Товары")
        for item in payload.get("items") or []:
            _goods_row(rows, item, require_unit=True, document_identity=document_identity)
    elif object_type == "Документ.РеализацияТоваровУслуг":
        _add(node, "ВидОперации", "РеализацияКлиенту")
        _currency(node)
        _add(node, "Сумма", _decimal(_amount(payload.get("items") or [])))
        _add(node, "СуммаВключаетНДС", "true")
        _warehouse(node, payload.get("warehouse"))
        _counterparty_key(_add(node, "Контрагент"), payload["customer"], "customer")
        _add(node, "ДанныеВзаиморасчетов")
        rows = _add(node, "Товары")
        for item in payload.get("items") or []:
            _goods_row(rows, item, require_unit=True, document_identity=document_identity)
    elif object_type == "Документ.ПоступлениеТоваровУслуг":
        key = node.find(_tag("КлючевыеСвойства"))
        _counterparty_key(_add(key, "Контрагент"), payload["provider"], "provider")
        rows = _add(node, "Товары")
        for item in payload.get("items") or []:
            _goods_row(rows, item, require_unit=True, document_identity=document_identity)
        _currency(node)
        _add(node, "ВидОперации", "ПокупкаУПоставщика")
        _add(node, "Сумма", _decimal(_amount(payload.get("items") or [])))
        _add(node, "СуммаВключаетНДС", "true")
        _warehouse(node, payload.get("warehouse"))
        _add(node, "ДанныеВзаиморасчетов")
    return node


def object_xml(
    object_type: str,
    payload: dict[str, Any],
    company: dict[str, Any] | None = None,
) -> bytes:
    """Expose individual objects for schema validation and test imports."""
    ET.register_namespace("", DATA_NS)
    return ET.tostring(
        serialize_object(object_type, payload, company),
        encoding="utf-8",
        xml_declaration=True,
    )


_EVENT_OBJECT_TYPES = {
    "shipment": "Документ.РеализацияТоваровУслуг",
    "supplier_receipt": "Документ.ПоступлениеТоваровУслуг",
}


def _event_object_type(entity_type: str, event_type: str) -> str:
    if event_type != "posted":
        raise ValueError("Предпросмотр EnterpriseData доступен только для проведённых документов")
    object_type = _EVENT_OBJECT_TYPES.get(entity_type)
    if object_type is None:
        raise ValueError(f"Выгрузка EnterpriseData для {entity_type} пока не реализована")
    return object_type


def preview_outbox_event(
    entity_type: str,
    event_type: str,
    payload: dict[str, Any],
    company: dict[str, Any],
) -> bytes:
    """Render a posted accounting event for inspection without sending it."""
    return object_xml(_event_object_type(entity_type, event_type), payload, company)


def event_body_objects(
    entity_type: str,
    event_type: str,
    payload: dict[str, Any],
    company: dict[str, Any],
) -> list[ET.Element]:
    """Document plus the counterparty and nomenclature it refers to.

    Справочники идут перед документом, чтобы УНФ создала их раньше, чем
    документ сошлётся на них. Повторяющиеся элементы выводятся один раз.
    """
    object_type = _event_object_type(entity_type, event_type)
    party_key = "customer" if object_type == "Документ.РеализацияТоваровУслуг" else "provider"
    party_kind = "customer" if party_key == "customer" else "provider"

    objects: list[ET.Element] = []
    seen: set[tuple[str, str]] = set()

    def add_reference(kind: str, data: dict[str, Any]) -> None:
        node = serialize_object(kind, data, company)
        ref = node.find(f"{_tag('КлючевыеСвойства')}/{_tag('Ссылка')}")
        marker = (kind, ref.text if ref is not None and ref.text else str(len(objects)))
        if marker not in seen:
            seen.add(marker)
            objects.append(node)

    add_reference("Справочник.Контрагенты", {**payload[party_key], "kind": party_kind})
    for item in payload.get("items") or []:
        add_reference("Справочник.Номенклатура", item)
    objects.append(serialize_object(object_type, payload, company))
    return objects
