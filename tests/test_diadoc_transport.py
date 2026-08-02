"""Тесты разбора статуса ГИС ЭПД из ответов Диадока.

Структура payload взята из документации Diadoc API (OuterDocflowInfo с
DocflowNamedId = "KlMt"). Форма Details в документации описана неточно,
поэтому проверяем оба варианта — список пар и плоский словарь.
"""
from dz_fastapi.services.diadoc_status import derive_outgoing_status_fields
from dz_fastapi.services.diadoc_transport import (
    extract_transport_status,
    is_transport_document,
    summarize_transport_status,
)


def _etrn_payload(details):
    return {
        "TypeNamedId": "RoadTransportWaybill",
        "OuterDocflowInfo": [
            {
                "DocflowNamedId": "KlMt",
                "Status": {
                    "NamedId": "4000211000",
                    "Type": "Success",
                    "FriendlyName": "Груз принят к перевозке",
                },
                "Details": details,
            }
        ],
    }


def test_extracts_status_from_details_list():
    payload = _etrn_payload(
        [
            {"Key": "mt-id", "Value": "MT-77-001"},
            {"Key": "mt-rid", "Value": "REQ-42"},
            {"Key": "kl-id", "Value": "CARRIAGE-9"},
        ]
    )
    status = extract_transport_status(payload)
    assert status["status_named_id"] == "4000211000"
    assert status["status_type"] == "Success"
    assert status["status_text"] == "Груз принят к перевозке"
    assert status["mintrans_id"] == "MT-77-001"
    assert status["request_id"] == "REQ-42"
    assert status["carriage_id"] == "CARRIAGE-9"


def test_extracts_status_from_details_dict():
    payload = _etrn_payload({"mt-id": "MT-77-002", "kl-id": "CARRIAGE-10"})
    status = extract_transport_status(payload)
    assert status["mintrans_id"] == "MT-77-002"
    assert status["carriage_id"] == "CARRIAGE-10"
    assert status["request_id"] is None


def test_accepts_single_object_outer_docflow():
    payload = _etrn_payload([])
    payload["OuterDocflowInfo"] = payload["OuterDocflowInfo"][0]
    status = extract_transport_status(payload)
    assert status["status_text"] == "Груз принят к перевозке"


def test_ignores_foreign_outer_docflow():
    payload = {
        "TypeNamedId": "UniversalTransferDocument",
        "OuterDocflowInfo": [
            {
                "DocflowNamedId": "SomethingElse",
                "Status": {"NamedId": "1", "FriendlyName": "не ГИС ЭПД"},
            }
        ],
    }
    assert extract_transport_status(payload) is None
    assert is_transport_document(payload) is False


def test_plain_utd_has_no_transport_status():
    payload = {
        "TypeNamedId": "UniversalTransferDocument",
        "DocflowStatus": {
            "PrimaryStatus": {
                "Severity": "Success",
                "StatusText": "Документооборот завершён",
            }
        },
    }
    assert extract_transport_status(payload) is None
    assert is_transport_document(payload) is False


def test_transport_document_detected_by_docflow_without_known_type():
    """Новый тип перевозочного документа опознаём по ГИС ЭПД."""
    payload = _etrn_payload([])
    payload["TypeNamedId"] = "SomeFutureTransportDoc"
    assert is_transport_document(payload) is True


def test_summarize_includes_mintrans_id():
    status = extract_transport_status(
        _etrn_payload([{"Key": "mt-id", "Value": "MT-5"}])
    )
    assert summarize_transport_status(status) == (
        "Груз принят к перевозке (ГИС ЭПД MT-5)"
    )
    assert summarize_transport_status(None) == "—"


def test_status_fields_carry_transport_status():
    payload = _etrn_payload([{"Key": "mt-id", "Value": "MT-9"}])
    payload["DocflowStatus"] = {
        "PrimaryStatus": {
            "Severity": "Success",
            "StatusText": "Документооборот завершён",
        }
    }
    fields = derive_outgoing_status_fields(
        payload, current_status="sent", is_draft=False
    )
    assert fields["status"] == "completed"
    assert fields["transport_status_named_id"] == "4000211000"
    assert fields["transport_status_text"] == "Груз принят к перевозке"
    assert fields["transport_mintrans_id"] == "MT-9"


def test_status_fields_leave_transport_empty_for_utd():
    fields = derive_outgoing_status_fields(
        {
            "DocflowStatus": {
                "PrimaryStatus": {
                    "Severity": "Success",
                    "StatusText": "Документооборот завершён",
                }
            }
        },
        current_status="sent",
        is_draft=False,
    )
    assert fields["transport_status_named_id"] is None
    assert fields["transport_status_text"] is None
    assert fields["transport_mintrans_id"] is None
