"""Тесты обмена с 1С:УНФ через универсальный формат EnterpriseData."""

import zipfile
from xml.etree import ElementTree as ET

import pytest

from dz_fastapi.services.one_c_enterprise_data import (
    MSG_NS,
    build_response_xml,
    find_incoming_files,
    get_exchange_status,
    load_state,
    parse_message,
    process_exchange_directory,
    read_incoming_archive,
    read_message_file,
    save_state,
    write_outgoing_message,
)
from dz_fastapi.services.one_c_enterprise_data_export import DATA_NS, serialize_object

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
      <msg:ObjectType>
        <msg:Name>Справочник.Контрагенты</msg:Name>
        <msg:Sending>*</msg:Sending>
        <msg:Receiving>*</msg:Receiving>
      </msg:ObjectType>
      <msg:ObjectType>
        <msg:Name>Документ.КорректировкаРеализацииТоваров</msg:Name>
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


def test_empty_body_is_handshake_but_document_body_is_not():
    empty = HANDSHAKE.replace(
        b"</msg:Header>\n</Message>",
        b"</msg:Header>\n  <Body/>\n</Message>",
    )
    document = HANDSHAKE.replace(
        b"</msg:Header>\n</Message>",
        b"</msg:Header>\n  <Body><Document/></Body>\n</Message>",
    )

    assert parse_message(empty).is_handshake
    assert parse_message(document).has_body
    assert parse_message(document).body_types == ["Document"]


def test_real_unf_1_22_body_namespace_is_detected():
    namespaced_body = (
        '</msg:Header>\n  <Body xmlns="http://v8.1c.ru/edi/edi_stnd/EnterpriseData/1.22">'
        '<Документ.ОприходованиеТоваров/></Body>\n</Message>'
    ).encode("utf-8")
    body = HANDSHAKE.replace(
        b"</msg:Header>\n</Message>",
        namespaced_body,
    )
    parsed = parse_message(body)
    assert parsed.has_body
    assert parsed.body_types == ["Документ.ОприходованиеТоваров"]


def test_response_swaps_nodes_and_confirms_number():
    msg = parse_message(HANDSHAKE)
    state = {"node_guid": "our-guid", "sent_no": 3}
    xml = build_response_xml(msg, state)

    assert xml.startswith(b"\xef\xbb\xbf"), "1С ожидает BOM"
    root = ET.fromstring(xml.decode("utf-8-sig"))
    assert root.tag == "Message"
    body = root.find(f"{{{DATA_NS}}}Body")
    assert body is not None and len(body) == 0
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


def test_response_does_not_offer_document_that_unf_cannot_receive():
    msg = parse_message(HANDSHAKE)
    xml = build_response_xml(
        msg,
        {"node_guid": "g", "sent_no": 1},
        sending=(
            "Документ.РеализацияТоваровУслуг",
            "Документ.КорректировкаРеализацииТоваров",
        ),
    )
    root = ET.fromstring(xml.decode("utf-8-sig"))
    types = {
        node.text for node in root.findall(
            f"{{{MSG_NS}}}Header/{{{MSG_NS}}}AvailableObjectTypes/"
            f"{{{MSG_NS}}}ObjectType/{{{MSG_NS}}}Name"
        )
    }
    assert "Документ.РеализацияТоваровУслуг" in types
    assert "Документ.КорректировкаРеализацииТоваров" not in types


def test_data_message_uses_the_body_namespace_seen_in_unf_archive():
    msg = parse_message(HANDSHAKE)
    item = serialize_object(
        "Справочник.Контрагенты",
        {"id": 42, "name": "Тест", "inn": "1234567890"},
    )
    xml = build_response_xml(
        msg,
        {"node_guid": "g", "sent_no": 1},
        sending=("Справочник.Контрагенты",),
        body_objects=[item],
    )
    parsed = parse_message(xml.decode("utf-8-sig").encode("utf-8"))
    assert parsed.has_body
    assert parsed.body_types == ["Справочник.Контрагенты"]
    root = ET.fromstring(xml.decode("utf-8-sig"))
    assert root.tag == "Message"
    assert root.find(f"{{{DATA_NS}}}Body/{{{DATA_NS}}}Справочник.Контрагенты") is not None


def test_response_mirrors_real_unf_format_uri_and_declares_prefix():
    real_header = HANDSHAKE.replace(
        b"http://v8.1c.ru/edi/edi_stnd/EnterpriseData</msg:Format>",
        b"http://v8.1c.ru/edi/edi_stnd/EnterpriseData/1.22</msg:Format>",
    )
    xml = build_response_xml(parse_message(real_header), {"node_guid": "g", "sent_no": 1})
    root = ET.fromstring(xml.decode("utf-8-sig"))
    assert root.tag == "Message"
    header = root.find(f"{{{MSG_NS}}}Header")
    assert header.find(f"{{{MSG_NS}}}Format").text.endswith("/1.22")
    assert header.find(f"{{{MSG_NS}}}Prefix").text == "ДЗ"


def test_data_message_refuses_type_not_received_by_unf():
    msg = parse_message(HANDSHAKE)
    item = ET.Element(f"{{{DATA_NS}}}Документ.КорректировкаРеализацииТоваров")
    with pytest.raises(ValueError, match="не объявила приём"):
        build_response_xml(
            msg,
            {"node_guid": "g", "sent_no": 1},
            sending=("Документ.КорректировкаРеализацииТоваров",),
            body_objects=[item],
        )


def test_data_message_refuses_a_different_enterprise_data_version():
    msg = parse_message(HANDSHAKE)
    msg.format_versions = ["1.20"]
    item = serialize_object("Справочник.Контрагенты", {"id": 1, "name": "Тест"})
    with pytest.raises(ValueError, match="1.22"):
        build_response_xml(
            msg,
            {"node_guid": "g", "sent_no": 1},
            sending=("Справочник.Контрагенты",),
            body_objects=[item],
        )


def test_peer_capabilities_are_kept_for_later_announcement(tmp_path, monkeypatch):
    import dz_fastapi.services.one_c_enterprise_data as exchange

    _write_incoming(tmp_path)
    process_exchange_directory(str(tmp_path))
    state = load_state(str(tmp_path))
    assert state["peer_object_types"]["Документ.КорректировкаРеализацииТоваров"] == {
        "sending": True,
        "receiving": False,
    }

    monkeypatch.setattr(exchange, "SUPPORTED_SENDING", (
        "Документ.РеализацияТоваровУслуг",
        "Документ.КорректировкаРеализацииТоваров",
    ))
    exchange.regenerate_outgoing_message(str(tmp_path))
    xml = read_message_file(str(tmp_path / "Message_ДЗ_НФ.zip"))
    root = ET.fromstring(xml.decode("utf-8-sig"))
    declared = {
        node.text for node in root.findall(
            f"{{{MSG_NS}}}Header/{{{MSG_NS}}}AvailableObjectTypes/"
            f"{{{MSG_NS}}}ObjectType/{{{MSG_NS}}}Name"
        )
    }
    assert "Документ.РеализацияТоваровУслуг" in declared
    assert "Документ.КорректировкаРеализацииТоваров" not in declared


def test_zip_round_trip(tmp_path):
    path = write_outgoing_message(str(tmp_path), b"<Message/>", from_code="ДЗ", to_code="НФ")
    assert path.endswith("Message_ДЗ_НФ.zip")
    assert read_message_file(path) == b"<Message/>"
    assert read_message_file(str(tmp_path / "Message_DZ_NF.zip")) == b"<Message/>"
    with zipfile.ZipFile(tmp_path / "Message_DZ_NF.zip") as archive:
        assert archive.namelist() == ["Message_DZ_NF.xml"]


def test_compressed_xml_is_bounded_before_parsing(tmp_path):
    path = _write_incoming(tmp_path, payload=b"<Message>" + b"x" * 1024 + b"</Message>")
    with pytest.raises(ValueError, match="размера"):
        read_message_file(str(path), max_xml_size=100)


def test_find_incoming_ignores_our_own_outgoing(tmp_path):
    _write_incoming(tmp_path)
    # Наш собственный ответ не должен приниматься за входящее
    write_outgoing_message(str(tmp_path), b"<Message/>", from_code="ДЗ", to_code="НФ")
    (tmp_path / "readme.txt").write_bytes(b"x")

    found = find_incoming_files(str(tmp_path))
    assert found == ["Message_НФ_ДЗ.zip"]


def test_transliterated_unf_message_is_processed_and_answered(tmp_path):
    _write_incoming(tmp_path, name="Message_NF_DZ.zip")
    result = process_exchange_directory(str(tmp_path))

    assert result["processed"][0]["file"] == "Message_NF_DZ.zip"
    assert (tmp_path / "processed" / "Message_NF_DZ.zip").exists()
    guid_path = tmp_path / "Message_DZ_91d5a7dd-2d96-4af5-b855-90ad613450b9.zip"
    assert guid_path.exists()
    assert read_incoming_archive(guid_path.name, str(tmp_path), outgoing=True)


def test_guid_named_followup_is_detected_and_answered(tmp_path):
    node_guid = "e3fc851e-50d7-4ef5-a0b0-b64a5fa61e1a"
    peer_guid = "91d5a7dd-2d96-4af5-b855-90ad613450b9"
    save_state(str(tmp_path), {
        "node_guid": node_guid,
        "peer_node_guid": peer_guid,
        "sent_no": 1,
        "received_no": 0,
    })
    name = f"Message_NF_{node_guid}.zip"
    payload = (
        HANDSHAKE.decode("utf-8")
        .replace("<msg:To>ДЗ</msg:To>", f"<msg:To>{node_guid}</msg:To>")
        .replace("<msg:MessageNo>0</msg:MessageNo>", "<msg:MessageNo>17</msg:MessageNo>")
        .replace("<msg:ReceivedNo>0</msg:ReceivedNo>", "<msg:ReceivedNo>1</msg:ReceivedNo>")
        .encode("utf-8")
    )
    _write_incoming(tmp_path, name=name, payload=payload)

    assert find_incoming_files(str(tmp_path)) == [name]
    assert read_incoming_archive(name, str(tmp_path))
    process_exchange_directory(str(tmp_path))
    outgoing = f"Message_{node_guid}_{peer_guid}.zip"
    assert (tmp_path / outgoing).exists()
    assert read_incoming_archive(outgoing, str(tmp_path), outgoing=True)
    reply = parse_message(read_message_file(str(tmp_path / outgoing)))
    assert (reply.from_code, reply.to_code) == ("ДЗ", "НФ")
    assert reply.received_no == 17


def test_admin_archive_reader_rejects_other_paths_and_large_files(tmp_path):
    incoming_path = _write_incoming(tmp_path)
    assert read_incoming_archive(incoming_path.name, str(tmp_path)) == incoming_path.read_bytes()

    with pytest.raises(ValueError, match="имя"):
        read_incoming_archive("../Message_НФ_ДЗ.zip", str(tmp_path))
    with pytest.raises(ValueError, match="имя"):
        read_incoming_archive("Message_ДЗ_НФ.zip", str(tmp_path))
    with pytest.raises(ValueError, match="размера"):
        read_incoming_archive(incoming_path.name, str(tmp_path), max_size=10)


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


def test_repeated_handshake_keeps_response_number(tmp_path):
    _write_incoming(tmp_path)
    first = process_exchange_directory(str(tmp_path))
    first_xml = read_message_file(str(tmp_path / first["response_file"]))

    _write_incoming(tmp_path)
    repeated = process_exchange_directory(str(tmp_path))

    assert repeated["response_file"] == first["response_file"]
    assert "повтор" in repeated["note"].lower()
    assert load_state(str(tmp_path))["sent_no"] == 1
    assert read_message_file(str(tmp_path / first["response_file"])) == first_xml
    assert not (tmp_path / "Message_НФ_ДЗ.zip").exists()


def test_handshake_with_wrong_sender_is_not_acknowledged(tmp_path):
    _write_incoming(tmp_path, payload=HANDSHAKE.replace(
        b"<msg:From>\xd0\x9d\xd0\xa4</msg:From>",
        b"<msg:From>XX</msg:From>",
    ))

    result = process_exchange_directory(str(tmp_path))

    assert result["response_file"] is None
    assert result["processed"][0]["error"]
    assert (tmp_path / "Message_НФ_ДЗ.zip").exists()


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


def test_regenerate_bumps_when_1c_confirmed_previous(tmp_path):
    """1С не удаляет наш файл, а считает номера: повтор она проигнорирует."""
    from dz_fastapi.services.one_c_enterprise_data import regenerate_outgoing_message, save_state

    save_state(
        str(tmp_path),
        {
            "node_guid": "our-guid",
            "sent_no": 1,
            "received_no": 0,
            # 1С сообщила, что приняла наше сообщение №1
            "peer_confirmed_our_no": 1,
            "peer_node_guid": "peer-guid",
        },
    )
    result = regenerate_outgoing_message(str(tmp_path))
    assert result["message_no"] == 2, "номер должен вырасти без ручного флага"


def test_process_directory_without_messages(tmp_path):
    result = process_exchange_directory(str(tmp_path))
    assert result["response_file"] is None
    assert "нет" in result["note"].lower()


def test_message_with_body_is_flagged_not_silently_acked(tmp_path):
    with_body = HANDSHAKE.replace(
        b"</msg:Header>\n</Message>",
        b"</msg:Header>\n  <Document/>\n</Message>",
    ).replace(b"<msg:MessageNo>0<", b"<msg:MessageNo>7<")
    _write_incoming(tmp_path, payload=with_body)
    result = process_exchange_directory(str(tmp_path))

    assert result["processed"][0]["handshake"] is False
    # Разбор данных ещё не реализован: не подтверждаем и не убираем сообщение.
    assert "данными" in result["note"].lower()
    assert result["response_file"] is None
    assert (tmp_path / "Message_НФ_ДЗ.zip").exists()
    assert not (tmp_path / "processed" / "Message_НФ_ДЗ.zip").exists()
    assert load_state(str(tmp_path))["received_no"] == 0


def test_status_inspects_incoming_header_without_acknowledging_it(tmp_path):
    with_body = HANDSHAKE.replace(
        b"</msg:Header>\n</Message>",
        b"</msg:Header>\n  <Document/>\n</Message>",
    )
    _write_incoming(tmp_path, payload=with_body)

    status = get_exchange_status(str(tmp_path))

    assert status["incoming_messages"][0]["message_no"] == 0
    assert status["incoming_messages"][0]["modified_at"]
    assert status["incoming_messages"][0]["has_body"] is True
    assert "Документ.РеализацияТоваровУслуг" in (
        status["incoming_messages"][0]["object_types"]
    )
    assert (tmp_path / "Message_НФ_ДЗ.zip").exists()
    assert not (tmp_path / "Message_ДЗ_НФ.zip").exists()


def _state_with_handshake(tmp_path, **extra):
    from dz_fastapi.services.one_c_enterprise_data import save_state

    peer = parse_message(HANDSHAKE)
    save_state(
        str(tmp_path),
        {
            "node_guid": "our-guid",
            "sent_no": 1,
            "received_no": 0,
            "last_peer_message_no": 0,
            "peer_node_guid": peer.peer_node_guid,
            "peer_object_types": peer.object_types,
            **extra,
        },
    )


def _shipment_objects():
    from dz_fastapi.services.one_c_enterprise_data_export import event_body_objects

    item = {
        "item_id": 1,
        "autopart": {"autopart_id": 5, "brand": "TEST", "oem": "A-1", "name": "Деталь"},
        "quantity": 1,
        "price": "10.00",
    }
    payload = {
        "document_id": 9,
        "document_number": "ДЗ-9",
        "document_date": "2026-10-27",
        "items": [item, dict(item, item_id=2)],
        "customer": {"id": 3, "name": "Клиент", "inn": "1234567890"},
    }
    return event_body_objects("shipment", "posted", payload, {"short_name": "ДЗ"})


def test_send_body_message_writes_document_after_references(tmp_path):
    from dz_fastapi.services.one_c_enterprise_data import send_body_message

    _state_with_handshake(tmp_path)
    result = send_body_message(_shipment_objects(), directory=str(tmp_path))

    # Номенклатура не объявлена УНФ как принимаемая — её в сообщении нет,
    # дубли строк не размножаются, документ идёт последним.
    assert result["objects"] == [
        "Справочник.Контрагенты",
        "Документ.РеализацияТоваровУслуг",
    ]
    assert result["message_no"] == 1, "непрочитанный служебный файл заменяется"
    root = ET.fromstring(
        read_message_file(str(tmp_path / "Message_ДЗ_НФ.zip")).decode("utf-8-sig")
    )
    assert root.find(f"{{{DATA_NS}}}Body") is not None
    assert load_state(str(tmp_path))["pending_body"]["message_no"] == 1


def test_send_body_message_waits_until_unf_reads_previous_one(tmp_path):
    from dz_fastapi.services.one_c_enterprise_data import send_body_message

    _state_with_handshake(tmp_path)
    send_body_message(_shipment_objects(), directory=str(tmp_path))
    with pytest.raises(ValueError, match="ещё не прочитала"):
        send_body_message(_shipment_objects(), directory=str(tmp_path))

    state = load_state(str(tmp_path))
    state["peer_confirmed_our_no"] = 1
    from dz_fastapi.services.one_c_enterprise_data import save_state

    save_state(str(tmp_path), state)
    assert send_body_message(_shipment_objects(), directory=str(tmp_path))["message_no"] == 2


def test_send_body_message_requires_handshake(tmp_path):
    from dz_fastapi.services.one_c_enterprise_data import send_body_message

    with pytest.raises(ValueError, match="Рукопожатие"):
        send_body_message(_shipment_objects(), directory=str(tmp_path))


def test_data_message_header_records_what_unf_confirmed_without_acking_it(tmp_path):
    from dz_fastapi.services.one_c_enterprise_data import send_body_message

    _state_with_handshake(tmp_path)
    send_body_message(_shipment_objects(), directory=str(tmp_path))
    # УНФ ответила сообщением с данными и подтвердила наше № 1
    with_body = (
        HANDSHAKE.replace(b"<msg:ReceivedNo>0<", b"<msg:ReceivedNo>1<")
        .replace(
            b"</msg:Header>\n</Message>",
            b"</msg:Header>\n  <Document/>\n</Message>",
        )
    )
    _write_incoming(tmp_path, payload=with_body)

    result = process_exchange_directory(str(tmp_path))

    assert result["response_file"] is None
    assert (tmp_path / "Message_НФ_ДЗ.zip").exists()
    state = load_state(str(tmp_path))
    assert state["peer_confirmed_our_no"] == 1
    assert state["pending_body"] is None
    assert state["last_confirmed_body"]["message_no"] == 1
    assert send_body_message(_shipment_objects(), directory=str(tmp_path))["message_no"] == 2


def test_handshake_does_not_overwrite_unread_data_message(tmp_path):
    from dz_fastapi.services.one_c_enterprise_data import send_body_message

    _state_with_handshake(tmp_path)
    send_body_message(_shipment_objects(), directory=str(tmp_path))
    before = (tmp_path / "Message_ДЗ_НФ.zip").read_bytes()
    _write_incoming(tmp_path, payload=HANDSHAKE.replace(b"<msg:MessageNo>0<", b"<msg:MessageNo>7<"))

    result = process_exchange_directory(str(tmp_path))

    assert result["response_file"] is None
    assert (tmp_path / "Message_ДЗ_НФ.zip").read_bytes() == before
    assert load_state(str(tmp_path))["sent_no"] == 1


def test_outgoing_message_is_also_placed_under_the_name_unf_searches_for(tmp_path):
    from dz_fastapi.services.one_c_enterprise_data import send_body_message

    _state_with_handshake(
        tmp_path, node_guid="e3fc851e-50d7-4ef5-a0b0-b64a5fa61e1a"
    )
    send_body_message(_shipment_objects(), directory=str(tmp_path))

    guid = "91d5a7dd-2d96-4af5-b855-90ad613450b9"
    by_guid = tmp_path / f"Message_ДЗ_{guid}.zip"
    assert by_guid.exists(), "УНФ ищет Message*_ДЗ_<GUID узла УНФ>.*"
    assert by_guid.read_bytes() != b""
    assert read_message_file(str(by_guid)) == read_message_file(
        str(tmp_path / "Message_ДЗ_НФ.zip")
    )
    node_guid = load_state(str(tmp_path))["node_guid"]
    assert (tmp_path / f"Message_{node_guid}_{guid}.zip").exists()
    assert (tmp_path / f"Message_{node_guid}_NF.zip").exists()


def test_receiver_guid_from_a_message_cannot_inject_a_path(tmp_path):
    write_outgoing_message(
        str(tmp_path), b"<x/>", from_code="ДЗ", to_code="НФ",
        receiver_guid="../../evil",
    )
    assert sorted(p.name for p in tmp_path.iterdir()) == [
        "Message_DZ_NF.zip", "Message_ДЗ_НФ.zip"
    ]


def test_send_body_message_can_replace_unread_one_on_request(tmp_path):
    from dz_fastapi.services.one_c_enterprise_data import send_body_message

    _state_with_handshake(tmp_path)
    send_body_message(_shipment_objects(), directory=str(tmp_path))
    result = send_body_message(
        _shipment_objects(), directory=str(tmp_path), replace_unread=True
    )
    assert result["message_no"] == 1


def test_admin_can_read_our_outgoing_files_only_with_the_outgoing_flag(tmp_path):
    from dz_fastapi.services.one_c_enterprise_data import send_body_message

    _state_with_handshake(tmp_path)
    send_body_message(_shipment_objects(), directory=str(tmp_path))
    guid_name = "Message_ДЗ_91d5a7dd-2d96-4af5-b855-90ad613450b9.zip"

    assert read_incoming_archive(guid_name, str(tmp_path), outgoing=True)
    assert read_incoming_archive("Message_ДЗ_НФ.zip", str(tmp_path), outgoing=True)
    with pytest.raises(ValueError, match="имя"):
        read_incoming_archive(guid_name, str(tmp_path))
    with pytest.raises(ValueError, match="имя"):
        read_incoming_archive("Message_ДЗ_../x.zip", str(tmp_path), outgoing=True)
