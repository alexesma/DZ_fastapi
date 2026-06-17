from dz_fastapi.services.cross_import import _is_junk_oem, _normalize_oem, group_rows_by_identifier


def test_group_rows_by_identifier_groups_and_normalizes():
    rows = [
        ("g1", "3903041009"),
        ("g1", "DZ3903041009"),
        ("g1", " dz3903041009 "),  # дубль после нормализации
        ("g2", "1291800500"),
        ("g2", "DZ1291800500"),
    ]
    groups = group_rows_by_identifier(rows)
    assert set(groups.keys()) == {"g1", "g2"}
    # Регистр/пробелы нормализованы, дубль схлопнут.
    assert groups["g1"] == {"3903041009", "DZ3903041009"}
    assert groups["g2"] == {"1291800500", "DZ1291800500"}


def test_group_rows_skips_junk_and_empty():
    rows = [
        ("g1", "0"),          # мусор — одни нули
        ("g1", "  "),         # пусто
        ("", "ABC123"),       # пустой идентификатор
        ("g1", "AB"),         # слишком короткий
        ("g1", "VALID12345"),
    ]
    groups = group_rows_by_identifier(rows)
    assert groups == {"g1": {"VALID12345"}}


def test_is_junk_oem():
    assert _is_junk_oem("0") is True
    assert _is_junk_oem("00") is True
    assert _is_junk_oem("") is True
    assert _is_junk_oem("AB") is True
    assert _is_junk_oem("ABC") is False
    assert _is_junk_oem("DZ12345") is False


def test_normalize_oem_uppercases_and_strips():
    assert _normalize_oem(" dz12-34/5 ") == _normalize_oem("DZ12345")
    assert _normalize_oem(None) == ""
