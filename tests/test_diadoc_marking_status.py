from dz_fastapi.services.diadoc_marking_status import (
    awaiting_gis_result,
    contains_marked_goods,
    gis_receipts,
    retain_gis_receipts,
)


def outer(status, ticks, *, current=True):
    identifier = f"receipt-{ticks}"
    return {
        "DocflowNamedId": "TtGis",
        "ParentEntityId": "upd",
        "LatestStatusEntityId": identifier if current else "other",
        "StatusEntities": [
            {
                "Attachment": {
                    "Attachment": {
                        "AttachmentFilename": f"{identifier}.xml",
                        "Entity": {"EntityId": identifier, "CreationTimestamp": {"Ticks": ticks}},
                    }
                },
                "Status": {
                    "NamedId": status,
                    "Type": "Error" if status == "ProcessingError" else "Normal",
                    "Details": [{"Code": "24", "Text": "Ошибка кода"}],
                },
            }
        ],
    }


def test_latest_is_selected_by_entity_and_other_systems_are_ignored():
    flow = outer("ProcessingError", 20)
    flow["StatusEntities"].extend(outer("InProcessing", 10)["StatusEntities"])
    payload = {"Docflow": {"OuterDocflows": [flow, {**flow, "DocflowNamedId": "PkFns"}]}}
    rows = gis_receipts(payload)
    assert len(rows) == 2
    assert [row["status"] for row in rows if row["current"]] == ["ProcessingError"]
    assert rows[0]["details"][0]["Code"] == "24"
    assert rows[0]["filename"] == "receipt-20.xml"


def test_late_duplicate_and_empty_snapshots_preserve_history_and_revocation():
    initial = {"Docflow": {"OuterDocflows": [outer("InProcessing", 10)]}}
    latest = {"Docflow": {"OuterDocflows": [outer("SuccessProcessed", 20)]}}
    snapshot = retain_gis_receipts(initial, latest)
    for update in [initial, latest, {"Docflow": {}}]:
        snapshot = retain_gis_receipts(snapshot, update)
    rows = gis_receipts(snapshot)
    assert [(row["status"], row["current"]) for row in rows] == [
        ("SuccessProcessed", True),
        ("InProcessing", False),
    ]
    assert not awaiting_gis_result(rows, revoked=False)
    assert awaiting_gis_result(rows, revoked=True)
    revocation = {
        "LastEvent": {
            "Docflow": {
                "Revocation": {"OuterDocflows": [outer("SuccessProcessedFixationCancel", 30)]}
            }
        }
    }
    rows = gis_receipts(retain_gis_receipts(snapshot, revocation))
    assert rows[0]["scope"] == "revocation"
    assert not awaiting_gis_result(rows, revoked=True)


def test_fixation_is_not_transfer_and_unknown_status_keeps_polling():
    for status in ["SuccessProcessedFixation", "GisReceivingError", "NewUnknownStatus"]:
        rows = gis_receipts({"Docflow": {"OuterDocflows": [outer(status, 10)]}})
        assert awaiting_gis_result(rows, revoked=False)


def test_marking_detection_uses_formalized_xml():
    xml = "<Файл><НомСредИдентТов><КИЗ>123</КИЗ></НомСредИдентТов></Файл>".encode()
    assert contains_marked_goods(xml, "UniversalTransferDocument")
    assert not contains_marked_goods(xml, "Nonformalized")
    assert not contains_marked_goods(b"<File/>", "UniversalTransferDocument")


async def test_inbound_patch_exposes_receipt_and_error_on_supplier_document(test_session):
    from dz_fastapi.api.diadoc import _incoming_document_to_out
    from dz_fastapi.models.diadoc import DiadocBoxEvent, DiadocIncomingDocument
    from dz_fastapi.services.diadoc_events import process_pending_events

    doc = DiadocIncomingDocument(
        environment="staging",
        box_id_guid="our-box",
        message_id="supplier-message",
        entity_id="supplier-upd",
    )
    event = DiadocBoxEvent(
        environment="staging",
        box_id_guid="our-box",
        event_id="error-patch",
        index_key="11",
        event_kind="patch",
        message_id=doc.message_id,
        payload={"Patch": {"MessageId": doc.message_id}},
    )
    test_session.add_all([doc, event])
    await test_session.commit()

    class Client:
        async def get_docflows(self, *, box_id_guid, document_ids):
            assert box_id_guid == "our-box" and document_ids == [(doc.message_id, doc.entity_id)]
            return {
                "Documents": [
                    {
                        "DocumentId": {"MessageId": doc.message_id, "EntityId": doc.entity_id},
                        "Docflow": {"OuterDocflows": [outer("ProcessingError", 20)]},
                    }
                ]
            }

    result = await process_pending_events(
        test_session,
        client=Client(),
        environment="staging",
        box_id_guid="our-box",
        download_content=False,
        process_supplier_message=False,
    )
    assert result["processed"] == 1
    (receipt,) = _incoming_document_to_out(doc).gis_mt_receipts
    assert receipt["current"] and receipt["type"] == "Error"
    assert receipt["filename"] == "receipt-20.xml"
