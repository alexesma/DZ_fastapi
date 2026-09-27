from dz_fastapi.services.autopart_name_enrichment import (
    choose_remote_name,
    classify_suspicious_name,
    extract_saved_partssoft_name,
)


def test_classifies_only_code_like_names_as_suspicious():
    assert classify_suspicious_name("123456", oem="123456") == "same_as_oem"
    assert classify_suspicious_name("A16910", oem="A16910") == "same_as_oem"
    assert classify_suspicious_name("180108 (6008 2RS)", oem="60082RS") == "code_only"
    assert classify_suspicious_name("/FY-11/KX-11/VX-11/", oem="X1") == "code_only"


def test_preserves_descriptive_names_in_any_language():
    assert classify_suspicious_name("Фильтр масляный", oem="123") is None
    assert classify_suspicious_name("Колодки тормозные SN892", oem="SN892") is None
    assert classify_suspicious_name("AIR FILTER", oem="123") is None
    assert classify_suspicious_name("MANN FILTER C 1234", oem="123") is None


def test_reads_useful_name_from_saved_partssoft_payload():
    payload = {"detail_name": "Фильтр воздушный", "name": None}

    assert extract_saved_partssoft_name(payload) == "Фильтр воздушный"


def test_ignores_code_only_saved_partssoft_name():
    payload = {"detail_name": "A16910", "name": None}

    assert extract_saved_partssoft_name(payload) is None


def test_remote_name_requires_unambiguous_majority():
    selected, status = choose_remote_name(
        [
            {"_resolved_name": "Фильтр масляный"},
            {"_resolved_name": "Фильтр масляный"},
            {"_resolved_name": "Фильтр воздушный"},
        ]
    )

    assert selected == "Фильтр масляный"
    assert status == "matched"
