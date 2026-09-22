"""Обязательные реквизиты в выгрузке клиентского прайса.

Пять колонок (ТН ВЭД, ОКПД 2, Честный знак, номер сертификата, ссылка
ФГИС) требуют от нас покупатели, поэтому их состав и порядок фиксируем
тестом: молча уехавшая клиенту пустая или переименованная колонка
означает отказ в приёмке прайса.
"""
import math

from dz_fastapi.services.utils import (
    CERTIFICATION_NOT_REQUIRED_TEXT,
    REGULATORY_COLUMNS,
    prepare_excel_data_from_records,
    regulatory_columns_for,
)

BASE_COLUMNS = [
    "Производитель",
    "Наименование",
    "Артикул",
    "Количество",
    "Цена",
    "Кратность",
]


def _record(autopart_id=1, oem="DZ456"):
    return {
        "autopart_id": autopart_id,
        "brand": "DRAGONZAP",
        "name": "Фильтр масляный",
        "oem_number": oem,
        "quantity": 46,
        "multiplicity": 5,
        "price": 300,
    }


def test_columns_present_and_ordered():
    df = prepare_excel_data_from_records([_record()], {})
    assert list(df.columns) == BASE_COLUMNS + REGULATORY_COLUMNS


def test_columns_present_without_attribute_map():
    """Старые вызовы без карты не должны падать и терять колонки."""
    df = prepare_excel_data_from_records([_record()])
    assert list(df.columns) == BASE_COLUMNS + REGULATORY_COLUMNS
    assert df.iloc[0]["ТН ВЭД"] == ""


def test_multiplicity_defaults_to_one_when_missing_or_invalid():
    record = _record()
    record.pop("multiplicity")
    assert prepare_excel_data_from_records([record]).iloc[0]["Кратность"] == 1

    record["multiplicity"] = 0
    assert prepare_excel_data_from_records([record]).iloc[0]["Кратность"] == 1


def test_multiplicity_is_exported_from_record():
    assert prepare_excel_data_from_records([_record()]).iloc[0]["Кратность"] == 5


def test_filled_attributes_land_in_row():
    attrs = {
        1: {
            "tnved_code": "8421230000",
            "okpd2_code": "28.29.13.110",
            "honest_sign_category": "Фильтры",
            "certification_required": True,
            "eac_cert_number": "ЕАЭС RU Д-CN.РА01.В.12345/24",
            "eac_cert_url": "https://pub.fsa.gov.ru/rds/declaration/view/1/common",
        }
    }
    row = prepare_excel_data_from_records([_record()], attrs).iloc[0]
    assert row["ТН ВЭД"] == "8421230000"
    assert row["ОКПД 2"] == "28.29.13.110"
    assert row["Честный знак"] == "Фильтры"
    assert row["Номер сертификата ЕАС"] == "ЕАЭС RU Д-CN.РА01.В.12345/24"
    assert row["Ссылка ФГИС"].startswith("https://pub.fsa.gov.ru/")


def test_supplier_offer_certificate_wins_and_url_works_without_number():
    """Выгружаем документ выбранного предложения, а не другого поставщика."""
    record = {
        **_record(),
        "certification_required": True,
        "eac_cert_number": None,
        "eac_cert_url": "https://swis.trade.kg/Doc/cosmo-document",
    }
    card_attrs = {
        1: {
            "certification_required": True,
            "eac_cert_number": "ЧУЖОЙ СЕРТИФИКАТ",
            "eac_cert_url": "https://example.invalid/other-supplier",
        }
    }

    row = prepare_excel_data_from_records([record], card_attrs).iloc[0]

    assert row["Номер сертификата ЕАС"] == ""
    assert row["Ссылка ФГИС"] == (
        "https://swis.trade.kg/Doc/cosmo-document"
    )


def test_empty_legacy_offer_falls_back_to_product_card_certificate():
    record = {
        **_record(),
        "certification_required": math.nan,
        "eac_cert_number": math.nan,
        "eac_cert_url": math.nan,
    }
    card_attrs = {
        1: {
            "certification_required": True,
            "eac_cert_number": "ЕАЭС RU Д-CN.РА01.В.12345/24",
            "eac_cert_url": "https://pub.fsa.gov.ru/document",
        }
    }

    row = prepare_excel_data_from_records([record], card_attrs).iloc[0]

    assert row["Номер сертификата ЕАС"] == "ЕАЭС RU Д-CN.РА01.В.12345/24"
    assert row["Ссылка ФГИС"] == "https://pub.fsa.gov.ru/document"


def test_not_required_wins_over_stale_number():
    """Снятая необходимость сертификации перекрывает старый номер."""
    attrs = {
        1: {
            "certification_required": False,
            "eac_cert_number": "ЕАЭС RU Д-CN.устаревший",
            "eac_cert_url": "https://example.invalid/old",
        }
    }
    row = prepare_excel_data_from_records([_record()], attrs).iloc[0]
    assert row["Номер сертификата ЕАС"] == CERTIFICATION_NOT_REQUIRED_TEXT
    assert row["Ссылка ФГИС"] == ""


def test_unknown_certification_stays_empty():
    """None — это «не определено», а не «не требует»."""
    result = regulatory_columns_for({"certification_required": None})
    assert result["Номер сертификата ЕАС"] == ""


def test_missing_autopart_gives_empty_columns():
    row = prepare_excel_data_from_records([_record(autopart_id=99)], {}).iloc[0]
    for column in REGULATORY_COLUMNS:
        assert row[column] == ""


def test_record_without_autopart_id_does_not_crash():
    record = _record()
    record.pop("autopart_id")
    row = prepare_excel_data_from_records([record], {}).iloc[0]
    assert row["Артикул"] == "DZ456"
    assert row["ТН ВЭД"] == ""
