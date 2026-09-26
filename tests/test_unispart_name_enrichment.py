from scripts.enrich_unispart_names_once import (
    choose_remote_name,
    extract_applicability,
    is_replaceable_name,
    is_useful_partssoft_name,
    merge_applicability,
)


def test_extracts_geely_body_codes_from_supplier_name():
    assert extract_applicability(
        "/FY-11/KX-11/LYNK&CO 09/FX-11/VX-11G836/FS-11/",
        brand="GEELY",
    ) == [
        "GEELY FY-11",
        "GEELY KX-11",
        "LYNK&CO 09",
        "GEELY FX-11",
        "GEELY VX-11G836",
        "GEELY FS-11",
    ]


def test_name_replacement_is_limited_to_non_russian_or_generic_names():
    assert is_replaceable_name("180108 (6008 2RS)", oem="60082RS")
    assert is_replaceable_name("/FY-11/KX-11/", oem="1056528800")
    assert is_replaceable_name("Деталь", oem="1056528800")
    assert not is_replaceable_name(
        "Подшипник шариковый 6008 2RS",
        oem="60082RS",
    )
    assert is_useful_partssoft_name("Подшипник шариковый")
    assert not is_useful_partssoft_name("BEARING 6008 2RS")


def test_ambiguous_partssoft_names_are_not_selected():
    assert choose_remote_name(
        [
            {"_resolved_name": "Втулка стабилизатора"},
            {"_resolved_name": "Опора стабилизатора"},
        ]
    ) == (None, "partssoft_name_ambiguous")


def test_partssoft_consensus_name_is_selected_despite_case_and_punctuation():
    assert choose_remote_name(
        [
            {"_resolved_name": "ФИЛЬТР МАСЛЯНЫЙ В СБОРЕ"},
            {"_resolved_name": "Фильтр масляный в сборе."},
            {"_resolved_name": "Фильтр масляный в сборе"},
            {"_resolved_name": "Фильтр масляный"},
        ]
    ) == ("Фильтр масляный в сборе", "matched")


def test_applicability_merge_preserves_existing_values_and_removes_duplicates():
    assert (
        merge_applicability(
            "Geely Tugella; GEELY FY-11",
            ["GEELY FY-11", "GEELY KX-11"],
        )
        == "Geely Tugella; GEELY FY-11; GEELY KX-11"
    )
