from dz_fastapi.services.marking_codes import normalize_marking_codes


def test_normalize_strips_whitespace_and_dedupes():
    codes = normalize_marking_codes(
        [
            " 0104601234567890 21ABC123 ",
            "0104601234567890\n21ABC123",
            "0104601234567890\t21XYZ999",
            "",
            None,
        ]
    )
    assert codes == [
        "010460123456789021ABC123",
        "010460123456789021XYZ999",
    ]


def test_normalize_accepts_single_string():
    assert normalize_marking_codes("01046 ABC") == ["01046ABC"]


def test_normalize_none_and_empty():
    assert normalize_marking_codes(None) == []
    assert normalize_marking_codes([]) == []
    assert normalize_marking_codes(["", "  ", None]) == []


def test_normalize_preserves_gs1_symbols():
    # GS-разделитель (\x1d) — значимый символ GS1 DataMatrix,
    # он не должен вырезаться.
    code = "0104601234567890215abc\x1d93test"
    assert normalize_marking_codes([code]) == [code]


def test_withdrawal_document_builder():
    from dz_fastapi.services.gis_mt import build_withdrawal_product_document

    document = build_withdrawal_product_document(
        inn="7701234567",
        codes=["01abc", "01def"],
        action="retail",
        document_number="DZ-WD-1",
        action_date="2026-07-04",
    )
    assert document["inn"] == "7701234567"
    assert document["action"] == "RETAIL"
    assert document["action_date"] == "2026-07-04"
    assert document["document_number"] == "DZ-WD-1"
    assert document["products"] == [
        {"cis": "01abc"},
        {"cis": "01def"},
    ]


def test_withdrawal_document_rejects_bad_action():
    import pytest

    from dz_fastapi.services.gis_mt import build_withdrawal_product_document

    with pytest.raises(ValueError):
        build_withdrawal_product_document(
            inn="7701234567",
            codes=["01abc"],
            action="UNKNOWN",
            document_number="X",
        )
    with pytest.raises(ValueError):
        build_withdrawal_product_document(
            inn="7701234567",
            codes=[],
            action="RETAIL",
            document_number="X",
        )


def test_extract_cis_and_status_variants():
    from dz_fastapi.services.gis_mt import _extract_cis_and_status

    assert _extract_cis_and_status(
        {"cisInfo": {"cis": "01a", "status": "INTRODUCED"}}
    ) == ("01a", "INTRODUCED")
    assert _extract_cis_and_status(
        {"cis": "01b", "status": "RETIRED"}
    ) == ("01b", "RETIRED")
    assert _extract_cis_and_status({}) == ("", "")
