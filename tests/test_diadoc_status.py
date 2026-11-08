from dz_fastapi.services.diadoc_docflow import derive_docflow_state


def _derive(payload, current_status="sent", is_draft=False):
    docflow = {
        "IsFinished": payload.get("IsFinished"),
        "DocflowStatus": payload.get("DocflowStatus"),
        "RecipientResponse": {
            "ResponseStatus": payload.get("RecipientResponseStatus")
        },
        "Revocation": {"RevocationStatus": payload.get("RevocationStatus")},
        "SenderTitle": {"DeliveredAt": payload.get("DeliveryTimestampTicks")},
    }
    return derive_docflow_state(
        {"LastEvent": {"Docflow": docflow}},
        current_status="draft" if is_draft else current_status,
    )


def test_completed_by_success_severity():
    fields = _derive(
        {
            "DocflowStatus": {
                "PrimaryStatus": {
                    "Severity": "Success",
                    "StatusText": "Документооборот завершен",
                }
            },
            "IsFinished": True,
        }
    )
    assert fields["status"] == "completed"
    assert fields["docflow_status_text"] == "Документооборот завершен"
    assert fields["docflow_status_severity"] == "Success"


def test_completed_by_recipient_signature():
    fields = _derive(
        {"RecipientResponseStatus": "WithRecipientSignature", "IsFinished": True}
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
    fields = _derive({"RecipientResponseStatus": "SignatureRequestRejected"})
    assert fields["status"] == "rejected"


def test_technical_error_is_not_counteragent_rejection():
    fields = _derive(
        {
            "DocflowStatus": {
                "PrimaryStatus": {
                    "Severity": "Error",
                    "StatusText": "Ошибка доставки в роуминг",
                }
            }
        }
    )
    assert fields["status"] == "technical_error"


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
