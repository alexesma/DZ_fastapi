import pytest

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


def test_normalize_preserves_gs1_symbols_outside_marking_codes():
    # GS-разделитель (\x1d) значим в GS1, поэтому из прочих кодов не вырезается.
    code = "AB\x1dCD"
    assert normalize_marking_codes([code]) == [code]


def test_normalize_cuts_scanned_code_to_ki_and_keeps_full_code():
    from dz_fastapi.services.marking_codes import normalize_marking_codes_with_full

    ki = "0104610014828864215HNN7I3EEKRU3"
    full = f"{ki}\x1d91EE10\x1d92aNj6j3V+Xe/9h0Az9sUuaCxOqo64CCYJAI="
    assert normalize_marking_codes([full, ki]) == [ki]
    assert normalize_marking_codes_with_full([ki, full]) == [(ki, full)]
    assert normalize_marking_codes_with_full([ki]) == [(ki, None)]


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


@pytest.mark.asyncio
async def test_marking_discrepancies_static_route_is_not_treated_as_code_id(
    async_client,
    test_session,
):
    response = await async_client.get(
        "/inventory/marking-codes/discrepancies",
        params={"limit": 100},
    )

    assert response.status_code == 200, response.text
    assert isinstance(response.json(), list)
