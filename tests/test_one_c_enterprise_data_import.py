"""Приём номеров и статусов документов, которые УНФ присылает обратно."""

import zipfile

import pytest

from dz_fastapi.models.one_c import OneCExchangeEvent, OneCExchangeEventStatus
from dz_fastapi.services.one_c_enterprise_data import (
    drop_inbox,
    load_state,
    process_exchange_directory,
    read_inbox,
)
from dz_fastapi.services.one_c_enterprise_data_apply import apply_inbox
from dz_fastapi.services.one_c_enterprise_data_export import (
    event_document_number,
    event_document_ref,
)
from dz_fastapi.services.one_c_enterprise_data_import import importable_documents

DATA = "http://v8.1c.ru/edi/edi_stnd/EnterpriseData/1.22"


def _message(body: str, message_no: int = 7) -> bytes:
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<Message xmlns:msg="http://www.1c.ru/SSL/Exchange/Message">
  <msg:Header>
    <msg:Format>http://v8.1c.ru/edi/edi_stnd/EnterpriseData</msg:Format>
    <msg:CreationDate>2026-10-28T10:00:00</msg:CreationDate>
    <msg:Confirmation>
      <msg:ExchangePlan>СинхронизацияДанныхЧерезУниверсальныйФормат</msg:ExchangePlan>
      <msg:To>ДЗ</msg:To>
      <msg:From>НФ</msg:From>
      <msg:MessageNo>{message_no}</msg:MessageNo>
      <msg:ReceivedNo>3</msg:ReceivedNo>
    </msg:Confirmation>
    <msg:AvailableVersion>1.22</msg:AvailableVersion>
  </msg:Header>
  <Body xmlns="{DATA}">{body}</Body>
</Message>""".encode()


def _shipment(ref: str, number: str, extra: str = "") -> str:
    return f"""<Документ.РеализацияТоваровУслуг>
      <КлючевыеСвойства>
        <Ссылка>{ref}</Ссылка><Дата>2026-10-28T12:30:00</Дата><Номер>{number}</Номер>
      </КлючевыеСвойства>{extra}
    </Документ.РеализацияТоваровУслуг>"""


def _write(directory, payload: bytes) -> None:
    with zipfile.ZipFile(directory / "Message_НФ_ДЗ.zip", "w") as archive:
        archive.writestr("Message_НФ_ДЗ.xml", payload)


def test_importable_documents_reads_number_date_and_flags():
    xml = _message(
        _shipment("ref-1", "НФ-0000042", "<Проведен>true</Проведен>")
        + "<Справочник.Контрагенты><КлючевыеСвойства/></Справочник.Контрагенты>"
    )
    [document] = importable_documents(xml)

    assert document.object_type == "Документ.РеализацияТоваровУслуг"
    assert (document.ref, document.number) == ("ref-1", "НФ-0000042")
    assert document.date == "2026-10-28T12:30:00"
    assert document.posted is True and document.deleted is None


def test_importable_documents_refuses_foreign_objects_and_reads_dragonzap_id():
    foreign = _message("<Документ.ОприходованиеТоваров/>")
    assert importable_documents(foreign) is None

    [document] = importable_documents(_message(_shipment("r", "DZ000000009")))
    assert document.dragonzap_id() == 9
    assert document.posted is None


def test_directory_acks_tracked_documents_and_queues_them(tmp_path):
    _write(tmp_path, _message(_shipment("ref-1", "НФ-1")))
    result = process_exchange_directory(str(tmp_path))

    assert result["response_file"] == "Message_ДЗ_НФ.zip"
    assert (tmp_path / "processed" / "Message_НФ_ДЗ.zip").exists()
    assert load_state(str(tmp_path))["received_no"] == 7
    [entry] = read_inbox(str(tmp_path))
    assert entry["message_no"] == 7
    assert entry["documents"][0]["number"] == "НФ-1"

    drop_inbox(1, str(tmp_path))
    assert read_inbox(str(tmp_path)) == []


def test_directory_keeps_message_with_foreign_objects_unacked(tmp_path):
    _write(tmp_path, _message("<Документ.ОприходованиеТоваров/>"))
    result = process_exchange_directory(str(tmp_path))

    assert result["response_file"] is None
    assert (tmp_path / "Message_НФ_ДЗ.zip").exists()
    state = load_state(str(tmp_path))
    assert state["received_no"] == 0 and not state.get("inbox")


@pytest.mark.asyncio
async def test_apply_inbox_stores_unf_number_on_matching_event(test_session, tmp_path):
    event = OneCExchangeEvent(
        entity_type="shipment",
        entity_id=9,
        event_type="posted",
        payload={"document_id": 9},
        idempotency_key="import-test:9",
        status=OneCExchangeEventStatus.PENDING,
        ed_message_no=2,
    )
    test_session.add(event)
    await test_session.flush()

    ref = event_document_ref("shipment", 9)
    other = event_document_ref("shipment", 10)
    _write(
        tmp_path,
        _message(
            _shipment(ref, "НФ-0000042", "<Проведен>true</Проведен>")
            + _shipment(other, "НФ-0000099")
        ),
    )
    process_exchange_directory(str(tmp_path))

    outcome = await apply_inbox(test_session, str(tmp_path))
    await test_session.refresh(event)

    assert outcome["matched"] == 1 and len(outcome["unmatched"]) == 1
    assert event.ed_unf_number == "НФ-0000042"
    assert event.ed_unf_ref == ref
    assert event.ed_unf_posted is True
    assert event.ed_unf_date.date().isoformat() == "2026-10-28"
    assert event_document_number(9) == "DZ000000009"


@pytest.mark.asyncio
async def test_apply_inbox_matches_by_number_when_ref_is_unknown(test_session, tmp_path):
    event = OneCExchangeEvent(
        entity_type="supplier_receipt",
        entity_id=5,
        event_type="posted",
        payload={"document_id": 5},
        idempotency_key="import-test:5",
        status=OneCExchangeEventStatus.PENDING,
        ed_message_no=1,
    )
    test_session.add(event)
    await test_session.flush()
    body = """<Документ.ПоступлениеТоваровУслуг><КлючевыеСвойства>
        <Ссылка>unknown</Ссылка><Дата>2026-10-28T09:00:00</Дата><Номер>DZ000000005</Номер>
        </КлючевыеСвойства><ПометкаУдаления>true</ПометкаУдаления>
        </Документ.ПоступлениеТоваровУслуг>"""
    _write(tmp_path, _message(body))
    process_exchange_directory(str(tmp_path))

    outcome = await apply_inbox(test_session, str(tmp_path))
    await test_session.refresh(event)

    assert outcome["matched"] == 1
    assert event.ed_unf_posted is False
