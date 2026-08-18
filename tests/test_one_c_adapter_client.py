import importlib.util
from pathlib import Path

import pytest

CLIENT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "one_c_ut_adapter" / "client.py"
SPEC = importlib.util.spec_from_file_location("one_c_ut_adapter_client", CLIENT_PATH)
client = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(client)


def test_adapter_validates_contract_and_events():
    client._validate_envelope(
        {
            "protocol": "dz-1c-outbox",
            "contract_version": "1.0",
            "events": [
                {
                    "event_uid": "event-1",
                    "entity_type": "shipment",
                    "entity_id": 1,
                    "event_type": "posted",
                    "payload_version": 1,
                    "idempotency_key": "shipment:1:posted:hash",
                    "payload": {"document_id": 1},
                }
            ],
        }
    )


def test_adapter_rejects_unknown_contract_version():
    with pytest.raises(client.AdapterError, match="Несовместимая версия"):
        client._validate_envelope(
            {
                "protocol": "dz-1c-outbox",
                "contract_version": "2.0",
                "events": [],
            }
        )


def test_adapter_state_is_idempotent(tmp_path):
    event = {
        "event_uid": "12345678-abcd",
        "entity_type": "shipment",
        "entity_id": 42,
    }
    external_id = client._simulated_external_id(event)
    path = tmp_path / "state.json"
    client._save_state(path, {"shipment:42": external_id})

    state = client._load_state(path)
    assert state["shipment:42"] == external_id
    assert external_id == "SIM-SHIPMENT-42-12345678"
