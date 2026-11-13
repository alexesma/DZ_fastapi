from types import SimpleNamespace

import pytest

from dz_fastapi.core.time import now_moscow
from dz_fastapi.models.diadoc import DiadocIncomingDocument
from dz_fastapi.models.partner import SupplierReceiptItem
from dz_fastapi.services.diadoc_acceptance import acceptance_summary, compare_upd_with_receipt
from dz_fastapi.services.upd_xml import UpdXmlItem


def _upd(article, qty, name="Фильтр"):
    return UpdXmlItem(
        line_number=1,
        name=name,
        oem_number=article,
        quantity=qty,
        price=100.0,
        total_with_vat=100.0 * qty,
    )


def _got(article, qty, name="Фильтр", **extra):
    return SupplierReceiptItem(
        oem_number=article, autopart_name=name, received_quantity=qty, **extra
    )


def test_equal_quantities_have_no_discrepancies():
    # Одна строка УПД может быть разнесена по нескольким строкам поступления,
    # а артикул записан с другим оформлением.
    lines = compare_upd_with_receipt(
        [_upd("AB-123", 5), _upd("X1", 1, name="Колодки")],
        [_got("ab123", 2), _got("AB 123", 3), _got("X1", 1, name="Колодки")],
    )
    assert lines == []


def test_shortage_missing_surplus_and_extra_are_reported():
    lines = compare_upd_with_receipt(
        [_upd("A1", 5), _upd("B2", 2, name="Колодки"), _upd("C3", 1, name="Свеча")],
        [
            _got("A1", 3),
            _got("C3", 2, name="Свеча"),
            _got("Z9", 1, name="Ремень"),
        ],
    )
    by_kind = {line["kind"]: line for line in lines}
    assert by_kind["shortage"]["document_quantity"] == 5
    assert by_kind["shortage"]["received_quantity"] == 3
    assert by_kind["missing"]["label"].startswith("B2")
    assert by_kind["surplus"]["received_quantity"] == 2
    assert by_kind["extra"]["label"].startswith("Z9")
    assert len(lines) == 4


def test_article_corrected_on_receiving_is_matched_by_name():
    lines = compare_upd_with_receipt(
        [_upd("OLD-1", 2, name="Фильтр масляный")],
        [_got("NEW-1", 2, name="Фильтр масляный")],
    )
    assert lines == []


def test_marking_errors_count_as_discrepancy():
    lines = compare_upd_with_receipt(
        [_upd("A1", 1)],
        [
            _got(
                "A1",
                1,
                marking_codes=["0104600000000000215abc"],
                marking_check={
                    "status": "error",
                    "issues": [
                        {"severity": "error", "code": "not_owned"},
                        {"severity": "warning", "code": "gis_unavailable"},
                    ],
                },
            )
        ],
    )
    assert [line["kind"] for line in lines] == ["marking"]
    assert lines[0]["marking_errors"] == 1


class _FakeSession:
    def __init__(self, items):
        self._items = items

    async def execute(self, _stmt):
        items = self._items
        return SimpleNamespace(
            scalars=lambda: SimpleNamespace(all=lambda: items)
        )


def _document(tmp_path, *, qty=2, posted=True):
    path = tmp_path / "ON_NSCHFDOPPR_x.xml"
    path.write_bytes(
        f"""<?xml version="1.0" encoding="utf-8"?>
<Файл ИдФайл="ON_NSCHFDOPPR_x" ВерсФорм="5.03">
  <Документ КНД="1115131" Функция="СЧФДОП">
    <СвСчФакт НомерДок="УПД-1" ДатаДок="09.11.2026"/>
    <ТаблСчФакт>
      <СведТов НомСтр="1" НаимТов="Фильтр" КолТов="{qty}" ЦенаТов="100.00"
               СтТовУчНал="200.00">
        <ДопСведТов КодТов="A1"/>
      </СведТов>
      <ВсегоОпл СтТовУчНалВсего="200.00"/>
    </ТаблСчФакт>
  </Документ>
</Файл>""".encode()
    )
    document = DiadocIncomingDocument(
        environment="staging",
        box_id_guid="box",
        message_id="message",
        entity_id="entity",
        provider_id=1,
        local_file_path=str(path),
    )
    receipt = SimpleNamespace(id=7, posted_at=now_moscow() if posted else None)
    document.__dict__["supplier_order_message"] = SimpleNamespace(receipts=[receipt])
    return document


@pytest.mark.asyncio
async def test_summary_suggests_signing_with_discrepancies(tmp_path):
    document = _document(tmp_path, qty=2)
    summary = await acceptance_summary(_FakeSession([_got("A1", 1)]), document)
    assert summary["checked"] is True
    assert summary["has_discrepancies"] is True
    assert summary["suggested_total_code"] == "2"
    assert summary["lines"][0]["kind"] == "shortage"


@pytest.mark.asyncio
async def test_summary_suggests_not_accepted_when_nothing_received(tmp_path):
    document = _document(tmp_path, qty=2)
    summary = await acceptance_summary(_FakeSession([_got("A1", 0)]), document)
    assert summary["suggested_total_code"] == "3"


@pytest.mark.asyncio
async def test_summary_without_discrepancies(tmp_path):
    document = _document(tmp_path, qty=2)
    summary = await acceptance_summary(_FakeSession([_got("A1", 2)]), document)
    assert summary["checked"] is True
    assert summary["has_discrepancies"] is False
    assert summary["suggested_total_code"] == "1"


@pytest.mark.asyncio
async def test_summary_is_not_checked_until_receipt_is_posted(tmp_path):
    document = _document(tmp_path, posted=False)
    summary = await acceptance_summary(_FakeSession([]), document)
    assert summary["checked"] is False
    assert summary["has_discrepancies"] is False
