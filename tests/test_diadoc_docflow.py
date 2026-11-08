from dz_fastapi.services.diadoc_docflow import derive_docflow_state


def _state(docflow, current="sent"):
    return derive_docflow_state(
        {"LastEvent": {"Docflow": docflow}},
        current_status=current,
    )


def test_docflow_uses_finished_flag_instead_of_status_text():
    result = _state(
        {
            "IsFinished": True,
            "DocflowStatus": {
                "PrimaryStatus": {
                    "Severity": "Success",
                    "StatusText": "Произвольный локализованный текст",
                }
            },
        }
    )

    assert result["status"] == "completed"
    assert result["is_finished"] is True
    assert result["docflow_status_text"] == "Произвольный локализованный текст"


def test_docflow_keeps_rejection_and_revocation_separate():
    rejected = _state(
        {
            "IsFinished": True,
            "RecipientResponse": {
                "ResponseStatus": "SignatureRequestRejected",
            },
        }
    )
    revoked = _state(
        {
            "IsFinished": True,
            "Revocation": {"RevocationStatus": "RevocationAccepted"},
        }
    )

    assert rejected["status"] == "rejected"
    assert revoked["status"] == "revoked"


def test_docflow_technical_error_is_not_a_rejection():
    result = _state(
        {
            "IsFinished": False,
            "DocflowStatus": {
                "PrimaryStatus": {"Severity": "Error", "StatusText": "Ошибка роуминга"}
            },
        }
    )

    assert result["status"] == "technical_error"
    assert result["recipient_response_status"] is None
