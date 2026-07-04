from dz_fastapi.services.diadoc_status import derive_outgoing_status_fields


def _derive(payload, current_status="sent", is_draft=False):
    return derive_outgoing_status_fields(
        payload,
        current_status=current_status,
        is_draft=is_draft,
    )


def test_completed_by_success_severity():
    fields = _derive(
        {
            "DocflowStatus": {
                "PrimaryStatus": {
                    "Severity": "Success",
                    "StatusText": "Документооборот завершен",
                }
            }
        }
    )
    assert fields["status"] == "completed"
    assert fields["docflow_status_text"] == "Документооборот завершен"
    assert fields["docflow_status_severity"] == "Success"


def test_completed_by_recipient_signature():
    fields = _derive(
        {"RecipientResponseStatus": "WithRecipientSignature"}
    )
    assert fields["status"] == "completed"
    assert (
        fields["recipient_response_status"] == "WithRecipientSignature"
    )


def test_delivered_when_only_delivery_ticks():
    fields = _derive(
        {
            "DocflowStatus": {
                "PrimaryStatus": {
                    "Severity": "Info",
                    "StatusText": "Ожидается подпись контрагента",
                }
            },
            "DeliveryTimestampTicks": 638000000000000000,
        }
    )
    assert fields["status"] == "delivered"
    assert fields["delivered_at"] is not None


def test_rejected_by_status_text():
    fields = _derive(
        {
            "DocflowStatus": {
                "PrimaryStatus": {
                    "Severity": "Warning",
                    "StatusText": "В подписи отказано",
                }
            }
        }
    )
    assert fields["status"] == "rejected"


def test_revoked_by_revocation_status():
    fields = _derive({"RevocationStatus": "RevocationAccepted"})
    assert fields["status"] == "revoked"
    assert fields["revocation_status"] == "RevocationAccepted"


def test_keeps_current_status_when_payload_empty():
    fields = _derive({}, current_status="sent")
    assert fields["status"] == "sent"


def test_draft_stays_draft_without_signals():
    fields = _derive({}, current_status="draft", is_draft=True)
    assert fields["status"] == "draft"
