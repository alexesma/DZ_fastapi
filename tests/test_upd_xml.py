import pytest

from dz_fastapi.services.supplier_order_responses import _parse_upd_xml_attachment
from dz_fastapi.services.upd_xml import looks_like_upd_xml, parse_upd_xml

UPD_503_SAMPLE = """<?xml version="1.0" encoding="utf-8"?>
<Файл ИдФайл="ON_NSCHFDOPPR_2BM-test" ВерсФорм="5.03">
  <Документ КНД="1115131" Функция="ДОП">
    <СвСчФакт НомерДок="УТ-1042" ДатаДок="15.06.2026">
      <СвПрод>
        <ИдСвед>
          <СвЮЛУч НаимОрг="ООО Автодеталь" ИННЮЛ="7701234567"/>
        </ИдСвед>
      </СвПрод>
    </СвСчФакт>
    <ТаблСчФакт>
      <СведТов НомСтр="1" НаимТов="Фильтр масляный A11-1012010"
               ОКЕИ_Тов="796" КолТов="10" ЦенаТов="150.50"
               СтТовБезНДС="1505.00" НалСт="20%" СтТовУчНал="1806.00">
        <ДопСведТов АртикулТов="A11-1012010" КрНаимСтрПр="КИТАЙ"/>
        <СвДТ НомерДТ="10702070/120626/0011223"/>
        <НомСредИдентТов>010460123456789021ABC123</НомСредИдентТов>
        <НомСредИдентТов КИЗ="010460123456789021ABC124"/>
      </СведТов>
      <СведТов НомСтр="2" НаимТов="Колодки тормозные"
               КолТов="4" ЦенаТов="820.00" СтТовУчНал="3936.00">
        <ДопСведТов АртикулТов="T11-3501080"/>
      </СведТов>
      <СведТов НомСтр="3" НаимТов="Услуга доставки" КолТов="1"
               СтТовУчНал="500.00"/>
    </ТаблСчФакт>
  </Документ>
</Файл>
"""

UPD_501_SAMPLE = """<?xml version="1.0" encoding="utf-8"?>
<Файл ИдФайл="ON_NSCHFDOPPR_old" ВерсФорм="5.01">
  <Документ КНД="1115131">
    <СвСчФакт НомерСчФ="77" ДатаСчФ="01.02.2026">
      <СвПрод>
        <ИдСвед>
          <СвИП ФИО="Иванов И.И." ИННФЛ="770100000000"/>
        </ИдСвед>
      </СвПрод>
    </СвСчФакт>
    <ТаблСчФакт>
      <СведТов НомСтр="1" НаимТов="Свеча зажигания"
               КолТов="8.000" ЦенаТов="95,50">
        <ДопСведТов КодТов="A11-3707110CA"/>
      </СведТов>
    </ТаблСчФакт>
  </Документ>
</Файл>
"""


def test_looks_like_upd_by_filename():
    assert looks_like_upd_xml(b"<x/>", "ON_NSCHFDOPPR_2BM_123.xml")
    assert not looks_like_upd_xml(b"<x/>", "price.xml")


def test_looks_like_upd_by_content_marker():
    assert looks_like_upd_xml(UPD_503_SAMPLE.encode("utf-8"), "doc.xml")
    cp1251_payload = UPD_503_SAMPLE.replace(
        'encoding="utf-8"', 'encoding="windows-1251"'
    ).encode("cp1251")
    assert looks_like_upd_xml(cp1251_payload, "doc.xml")


def test_parse_upd_503():
    document = parse_upd_xml(UPD_503_SAMPLE.encode("utf-8"))
    assert document.document_number == "УТ-1042"
    assert str(document.document_date) == "2026-06-15"
    assert document.seller_name == "ООО Автодеталь"
    assert document.seller_inn == "7701234567"
    assert len(document.items) == 3
    assert document.items_without_article == 1

    first = document.items[0]
    assert first.oem_number == "A11-1012010"
    assert first.quantity == 10
    assert first.price == 150.5
    assert first.total_with_vat == 1806.0
    assert first.gtd_code == "10702070/120626/0011223"
    assert first.country_name == "КИТАЙ"
    assert first.marking_codes == [
        "010460123456789021ABC123",
        "010460123456789021ABC124",
    ]


def test_parse_upd_501_legacy_fields():
    document = parse_upd_xml(UPD_501_SAMPLE.encode("utf-8"))
    assert document.document_number == "77"
    assert str(document.document_date) == "2026-02-01"
    assert document.seller_name == "Иванов И.И."
    item = document.items[0]
    # КодТов используется как запасной артикул, запятая в цене — норм
    assert item.oem_number == "A11-3707110CA"
    assert item.quantity == 8
    assert item.price == 95.5


def test_parse_rejects_non_upd_xml():
    with pytest.raises(ValueError):
        parse_upd_xml(b"<?xml version='1.0'?><root><foo/></root>")


def test_attachment_conversion_to_rows():
    rows = _parse_upd_xml_attachment(
        UPD_503_SAMPLE.encode("utf-8"),
        "ON_NSCHFDOPPR_test.xml",
    )
    # Строка без артикула (услуга доставки) пропущена
    assert len(rows) == 2
    first = rows[0]
    assert first.oem_number  # нормализован
    assert first.confirmed_quantity == 10
    assert first.response_price == 150.5
    assert first.document_number == "УТ-1042"
    assert first.total_price_with_vat == 1806.0
    assert first.source_name == "Фильтр масляный A11-1012010"
    assert first.brand_name is None
    assert first.marking_codes == [
        "010460123456789021ABC123",
        "010460123456789021ABC124",
    ]


def test_attachment_conversion_skips_non_upd():
    rows = _parse_upd_xml_attachment(
        b"<?xml version='1.0'?><price><row/></price>",
        "price.xml",
    )
    assert rows == []
