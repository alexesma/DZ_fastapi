"""Тесты обмена с 1С:УНФ через универсальный формат EnterpriseData."""

import zipfile
from xml.etree import ElementTree as ET

from dz_fastapi.services.one_c_enterprise_data import (
    MSG_NS,
    build_response_xml,
    find_incoming_files,
    load_state,
    parse_message,
    process_exchange_directory,
    read_message_file,
    write_outgoing_message,
)

HANDSHAKE = """<?xml version="1.0" encoding="UTF-8"?>
<Message xmlns:msg="http://www.1c.ru/SSL/Exchange/Message">
  <msg:Header>
    <msg:Format>http://v8.1c.ru/edi/edi_stnd/EnterpriseData</msg:Format>
    <msg:CreationDate>2026-09-06T10:36:59</msg:CreationDate>
    <msg:Confirmation>
      <msg:ExchangePlan>СинхронизацияДанныхЧерезУниверсальныйФормат</msg:ExchangePlan>
      <msg:To>ДЗ</msg:To>
      <msg:From>НФ</msg:From>
      <msg:MessageNo>0</msg:MessageNo>
      <msg:ReceivedNo>0</msg:ReceivedNo>
    </msg:Confirmation>
    <msg:AvailableVersion>1.22</msg:AvailableVersion>
    <msg:AvailableVersion>1.10</msg:AvailableVersion>
    <msg:NewFrom>91d5a7dd-2d96-4af5-b855-90ad613450b9</msg:NewFrom>
    <msg:AvailableObjectTypes>
      <msg:ObjectType>
        <msg:Name>Документ.РеализацияТоваровУслуг</msg:Name>
        <msg:Sending>*</msg:Sending>
        <msg:Receiving>*</msg:Receiving>
      </msg:ObjectType>
      <msg:ObjectType>
        <msg:Name>Документ.АвансовыйОтчет</msg:Name>
        <msg:Sending>*</msg:Sending>
        <msg:Receiving/>
      </msg:ObjectType>
    </msg:AvailableObjectTypes>
    <msg:SynchronizationContour>
      <msg:Node>
        <msg:Name>0KPQvdGE</msg:Name>
        <msg:Code>91d5a7dd-2d96-4af5-b855-90ad613450b9</msg:Code>
        <msg:Prefix>НФ</msg:Prefix>
      </msg:Node>
    </msg:SynchronizationContour>
  </msg:Header>
</Message>
""".encode(
    "utf-8"
)


def _write_incoming(directory, name="Message_НФ_ДЗ.zip", payload=HANDSHAKE):
    path = directory / name
    if name.endswith(".zip"):
        with zipfile.ZipFile(path, "w") as archive:
            archive.writestr("Message_НФ_ДЗ.xml", payload)
    else:
        path.write_bytes(payload)
    return path


def test_parse_handshake():
    msg = parse_message(HANDSHAKE)
    assert msg.from_code == "НФ"
    assert msg.to_code == "ДЗ"
    assert msg.message_no == 0
    assert msg.is_handshake is True
    assert msg.peer_node_guid == "91d5a7dd-2d96-4af5-b855-90ad613450b9"
    assert msg.peer_node_prefix == "НФ"
    assert "1.22" in msg.format_versions
    # Направления читаются раздельно
    assert msg.object_types["Документ.РеализацияТоваровУслуг"] == {
        "sending": True,
        "receiving": True,
    }
    assert msg.object_types["Документ.АвансовыйОтчет"]["receiving"] is False


def test_response_swaps_nodes_and_confirms_number():
    msg = parse_message(HANDSHAKE)
    state = {"node_guid": "our-guid", "sent_no": 3}
    xml = build_response_xml(msg, state)

    assert xml.startswith(b"\xef\xbb\xbf"), "1С ожидает BOM"
    root = ET.fromstring(xml.decode("utf-8-sig"))
    conf = root.find(f"{{{MSG_NS}}}Header/{{{MSG_NS}}}Confirmation")

    def val(tag):
        return conf.find(f"{{{MSG_NS}}}{tag}").text

    # Адресат и отправитель меняются местами
    assert val("To") == "НФ"
    assert val("From") == "ДЗ"
    assert val("MessageNo") == "3"
    # Подтверждаем номер принятого сообщения
    assert val("ReceivedNo") == "0"

    header = root.find(f"{{{MSG_NS}}}Header")
    assert header.find(f"{{{MSG_NS}}}NewFrom").text == "our-guid"
    # Выбирается версия, которую поддерживает 1С
    assert header.find(f"{{{MSG_NS}}}AvailableVersion").text == "1.22"


def test_response_declares_only_supported_types():
    msg = parse_message(HANDSHAKE)
    xml = build_response_xml(
        msg,
        {"node_guid": "g", "sent_no": 1},
        sending=("Документ.РеализацияТоваровУслуг",),
        receiving=("Справочник.Контрагенты",),
    )
    root = ET.fromstring(xml.decode("utf-8-sig"))
    types = root.findall(
        f"{{{MSG_NS}}}Header/{{{MSG_NS}}}AvailableObjectTypes/" f"{{{MSG_NS}}}ObjectType"
    )
    parsed = {
        t.find(f"{{{MSG_NS}}}Name").text: (
            t.find(f"{{{MSG_NS}}}Sending").text,
            t.find(f"{{{MSG_NS}}}Receiving").text,
        )
        for t in types
    }
    assert parsed["Документ.РеализацияТоваровУслуг"] == ("*", None)
    assert parsed["Справочник.Контрагенты"] == (None, "*")


def test_zip_round_trip(tmp_path):
    path = write_outgoing_message(str(tmp_path), b"<Message/>", from_code="ДЗ", to_code="НФ")
    assert path.endswith("Message_ДЗ_НФ.zip")
    assert read_message_file(path) == b"<Message/>"


def test_find_incoming_ignores_our_own_outgoing(tmp_path):
    _write_incoming(tmp_path)
    # Наш собственный ответ не должен приниматься за входящее
    write_outgoing_message(str(tmp_path), b"<Message/>", from_code="ДЗ", to_code="НФ")
    (tmp_path / "readme.txt").write_bytes(b"x")

    found = find_incoming_files(str(tmp_path))
    assert found == ["Message_НФ_ДЗ.zip"]


def test_process_directory_creates_response_and_archives(tmp_path):
    _write_incoming(tmp_path)
    result = process_exchange_directory(str(tmp_path))

    assert result["response_file"] == "Message_ДЗ_НФ.zip"
    assert result["processed"][0]["handshake"] is True
    assert (tmp_path / "Message_ДЗ_НФ.zip").exists()
    # Обработанное входящее убрано, чтобы не разобрать его повторно
    assert (tmp_path / "processed" / "Message_НФ_ДЗ.zip").exists()
    assert not (tmp_path / "Message_НФ_ДЗ.zip").exists()

    state = load_state(str(tmp_path))
    assert state["sent_no"] == 1
    assert state["peer_node_guid"] == "91d5a7dd-2d96-4af5-b855-90ad613450b9"


def test_declared_types_are_not_empty():
    """1С останавливает мастер, если корреспондент не объявил ни одного типа."""
    from dz_fastapi.services.one_c_enterprise_data import SUPPORTED_RECEIVING, SUPPORTED_SENDING

    assert SUPPORTED_SENDING, "без списка на отправку мастер 1С не пройти"
    # Приём объявлять нельзя, пока нет разбора данных: 1С пометит
    # отправленное доставленным, и объекты потеряются
    assert SUPPORTED_RECEIVING == ()


def test_regenerate_reuses_message_no_by_default(tmp_path):
    from dz_fastapi.services.one_c_enterprise_data import regenerate_outgoing_message, save_state

    save_state(
        str(tmp_path),
        {"node_guid": "our-guid", "sent_no": 1, "received_no": 0,
         "peer_node_guid": "peer-guid"},
    )
    result = regenerate_outgoing_message(str(tmp_path))

    # Сообщение 1С ещё не прочла — заменяем его, а не плодим новое
    assert result["message_no"] == 1
    assert result["response_file"] == "Message_ДЗ_НФ.zip"
    assert result["declared_sending"]

    xml = read_message_file(str(tmp_path / "Message_ДЗ_НФ.zip"))
    root = ET.fromstring(xml.decode("utf-8-sig"))
    names = [
        e.text
        for e in root.iter(f"{{{MSG_NS}}}Name")
    ]
    assert "Документ.РеализацияТоваровУслуг" in names

    bumped = regenerate_outgoing_message(str(tmp_path), bump_message_no=True)
    assert bumped["message_no"] == 2


def test_process_directory_without_messages(tmp_path):
    result = process_exchange_directory(str(tmp_path))
    assert result["response_file"] is None
    assert "нет" in result["note"].lower()


def test_message_with_body_is_flagged_not_silently_acked(tmp_path):
    with_body = HANDSHAKE.replace(
        b"</msg:Header>\n</Message>",
        b"</msg:Header>\n  <Document/>\n</Message>",
    )
    _write_incoming(tmp_path, payload=with_body)
    result = process_exchange_directory(str(tmp_path))

    assert result["processed"][0]["handshake"] is False
    # Разбор данных ещё не реализован: не подтверждаем и не убираем сообщение.
    assert "данными" in result["note"].lower()
    assert result["response_file"] is None
    assert (tmp_path / "Message_НФ_ДЗ.zip").exists()
    assert not (tmp_path / "processed" / "Message_НФ_ДЗ.zip").exists()
