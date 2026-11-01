"""Ящики Диадока приходят в разных формах; список контрагентов вложен в Organization."""

from dz_fastapi.services.diadoc_boxes import diadoc_box_variants, flatten_counteragent

HEX = "47509ad9892e4198a94944bfe92b5367"
GUID = "47509ad9-892e-4198-a949-44bfe92b5367"


def test_box_variants_cover_all_forms():
    expected = {HEX, GUID, f"{HEX}@diadoc.ru"}
    for value in (HEX, GUID, f"{HEX}@diadoc.ru", f"{HEX.upper()}@DIADOC.RU"):
        assert expected <= diadoc_box_variants(value)
    assert diadoc_box_variants("") == set()
    assert diadoc_box_variants(None) == set()
    assert diadoc_box_variants("not-a-box") == {"not-a-box"}


def test_flatten_counteragent_reads_nested_organization():
    row = {
        "CurrentStatus": "IsMyCounteragent",
        "Organization": {
            "FullName": "ООО Ромашка",
            "ShortName": "Ромашка",
            "Inn": "7714388705",
            "Kpp": "772401001",
            "Boxes": [{"BoxId": f"{HEX}@diadoc.ru", "BoxIdGuid": GUID}],
        },
    }
    flat = flatten_counteragent(row)
    assert (flat["Inn"], flat["Kpp"]) == ("7714388705", "772401001")
    assert flat["FullName"] == "ООО Ромашка"
    assert flat["BoxIdGuid"] == GUID and flat["BoxId"] == f"{HEX}@diadoc.ru"
    assert flat["CurrentStatusNamedId"] == "IsMyCounteragent"
    assert flatten_counteragent({"Inn": "1"}) == {"Inn": "1"}
