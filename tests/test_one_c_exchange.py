import xml.etree.ElementTree as ET
from datetime import datetime
from decimal import Decimal
from types import SimpleNamespace

from dz_fastapi.services.one_c_exchange import build_commerceml_sale_xml


def _shipment_stub():
    brand = SimpleNamespace(name="CHERY")
    autopart = SimpleNamespace(
        oem_number="A11-1012010",
        name="Фильтр масляный",
        brand=brand,
    )
    item = SimpleNamespace(
        autopart_id=77,
        autopart=autopart,
        quantity=10,
        price=Decimal("150.50"),
        cost_total=Decimal("1000.00"),
    )
    customer = SimpleNamespace(
        id=5,
        name='ООО "Ромашка"',
        inn="7701234567",
        kpp="770101001",
    )
    return SimpleNamespace(
        id=123,
        doc_number="DZ-000123",
        doc_date=datetime(2026, 7, 1, 12, 30),
        created_at=datetime(2026, 7, 1, 12, 30),
        notes="Комментарий к отгрузке",
        customer=customer,
        customer_order_id=42,
        items=[item],
    )


def test_commerceml_xml_structure():
    payload = build_commerceml_sale_xml([_shipment_stub()])
    root = ET.fromstring(payload)
    assert root.tag == "КоммерческаяИнформация"
    assert root.get("ВерсияСхемы") == "2.05"

    document = root.find("Документ")
    assert document is not None
    assert document.findtext("Ид") == "dz-shipment-123"
    assert document.findtext("Номер") == "DZ-000123"
    assert document.findtext("Дата") == "2026-07-01"
    assert document.findtext("ХозОперация") == "Заказ товара"
    assert document.findtext("Сумма") == "1505.00"

    counterparty = document.find("Контрагенты/Контрагент")
    assert counterparty.findtext("Наименование") == 'ООО "Ромашка"'
    assert counterparty.findtext("ИНН") == "7701234567"
    assert counterparty.findtext("КПП") == "770101001"
    assert counterparty.findtext("Роль") == "Покупатель"

    good = document.find("Товары/Товар")
    assert good.findtext("Артикул") == "A11-1012010"
    assert (
        good.findtext("Наименование")
        == "CHERY A11-1012010 Фильтр масляный"
    )
    assert good.findtext("ЦенаЗаЕдиницу") == "150.50"
    assert good.findtext("Количество") == "10"
    assert good.findtext("Сумма") == "1505.00"
    assert (
        good.findtext("СтавкиНалогов/СтавкаНалога/Ставка") == "20"
    )


def test_commerceml_xml_without_customer():
    shipment = _shipment_stub()
    shipment.customer = None
    payload = build_commerceml_sale_xml([shipment])
    root = ET.fromstring(payload)
    counterparty = root.find("Документ/Контрагенты/Контрагент")
    assert (
        counterparty.findtext("Наименование") == "Розничный покупатель"
    )
    assert counterparty.find("ИНН") is None


def test_commerceml_xml_empty_list():
    payload = build_commerceml_sale_xml([])
    root = ET.fromstring(payload)
    assert root.tag == "КоммерческаяИнформация"
    assert root.find("Документ") is None
