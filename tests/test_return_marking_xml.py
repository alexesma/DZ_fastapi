from __future__ import annotations

from types import SimpleNamespace
from xml.etree import ElementTree as ET

import pytest

from dz_fastapi.services.return_marking_xml import (
    ReturnMarkingBlocked,
    supplier_return_marking_blocker,
    verify_return_title,
)

CODE_A = "010460123456789021ABCDEFGHIJKLM"
CODE_B = "010460123456789021NOPQRSTUVWXYZ"


def _ukd(before: list[str], after: list[str]) -> bytes:
    root = ET.Element("Файл")
    line = ET.SubElement(
        root,
        "СведТов",
        {
            "ПорНомТовВСЧФ": "1",
            "КолТовДо": str(len(before)),
            "КолТовПосле": str(len(after)),
        },
    )
    details = ET.SubElement(line, "ДопСведТов")
    for name, codes in (
        ("НомСредИдентТовДо", before),
        ("НомСредИдентТовПосле", after),
    ):
        if codes:
            group = ET.SubElement(details, name)
            for code in codes:
                ET.SubElement(group, "КИЗ").text = code
    return ET.tostring(root, encoding="utf-8")


def test_return_ukd_contains_exact_units_before_and_after():
    states = {
        7: {
            "original_number": 1,
            "original_codes": [CODE_A, CODE_B],
            "corrected_codes": [CODE_A],
        }
    }

    verify_return_title(_ukd([CODE_A, CODE_B], [CODE_A]), states)


def test_return_ukd_rejects_a_substituted_unit():
    states = {
        7: {
            "original_number": 1,
            "original_codes": [CODE_A, CODE_B],
            "corrected_codes": [CODE_A],
        }
    }

    with pytest.raises(ReturnMarkingBlocked, match="отличаются от возврата"):
        verify_return_title(_ukd([CODE_A, CODE_B], [CODE_B]), states)


def test_marked_supplier_return_requires_correction_from_supplier():
    document = SimpleNamespace(
        items=[
            SimpleNamespace(
                marking_codes=[CODE_A],
                supplier_receipt_item=SimpleNamespace(
                    document_marking_codes=[CODE_A],
                    marking_codes=[CODE_A],
                ),
            )
        ]
    )

    assert "исходного продавца" in supplier_return_marking_blocker(document)
