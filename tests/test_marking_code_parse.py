"""Разбор скана кода «Честный знак» в КИ, как его показывает личный кабинет."""

from dz_fastapi.services.marking_code_parse import (
    fix_russian_layout,
    marking_code_ki,
    parse_marking_code,
)

KI = "0104610014828864215HNN7I3EEKRU3"
GS = "\x1d"


def test_short_code_from_site_is_its_own_ki():
    assert marking_code_ki(KI) == KI
    assert marking_code_ki("0104610014828864215EK6K00LRPECI") == (
        "0104610014828864215EK6K00LRPECI"
    )


def test_full_code_with_gs_is_cut_to_ki():
    parsed = parse_marking_code(f"{KI}{GS}91EE10{GS}92aNj6j3V+Xe/9h0Az9sUuaCxOqo64CCYJAI=")
    assert parsed.ki == KI
    assert parsed.gtin == "04610014828864"
    assert parsed.serial == "5HNN7I3EEKRU3"
    assert parsed.key == "EE10"
    assert parsed.crypto == "aNj6j3V+Xe/9h0Az9sUuaCxOqo64CCYJAI="
    assert parsed.has_crypto_tail


def test_lost_gs_and_symbology_prefix_are_tolerated():
    assert marking_code_ki(f"]d2{KI}91EE10") == KI
    parsed = parse_marking_code(f"{KI}91EE1092abcdefgh12=")
    assert parsed.ki == KI and parsed.key == "EE10" and parsed.crypto == "abcdefgh12="


def test_gs_turned_into_line_break_is_a_separator():
    parsed = parse_marking_code(f"{KI}\n91EE10\r\n92abcdefgh12=")
    assert parsed.ki == KI and parsed.crypto == "abcdefgh12="


def test_scan_made_with_russian_layout_is_restored():
    raw = "0104610014828864215РТТ7Ш3УУЛКГ391УУ1щ64ССНОФШ="
    parsed = parse_marking_code(raw)
    assert parsed.ki == KI
    assert parsed.layout_fixed
    assert fix_russian_layout("фТо6о3М+Чу.9р0Фя9ыГгфСчЩйщ64ССНОФШ=")[0] == (
        "aNj6j3V+Xe/9h0Az9sUuaCxOqo64CCYJAI="
    )


def test_not_a_marking_code_returns_none():
    assert marking_code_ki("52245-SBA-A21") is None
    assert marking_code_ki("") is None
    assert marking_code_ki(None) is None
    assert marking_code_ki("0104610014828864") is None
