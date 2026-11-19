"""Квитанции ГИС МТ из GetDocflows V5, включая отдельный оборот аннулирования."""

from __future__ import annotations

from xml.etree import ElementTree


def contains_marked_goods(content: bytes, document_type: str) -> bool:
    if document_type not in {
        "UniversalTransferDocument",
        "UniversalTransferDocumentRevision",
        "UniversalCorrectionDocument",
        "UniversalCorrectionDocumentRevision",
        "XmlTorg12",
    }:
        return False
    try:
        root = ElementTree.fromstring(content)
    except (ElementTree.ParseError, LookupError, ValueError):
        return False
    tags = {"НомСредИдентТов", "НомСредИдентТовДо", "НомСредИдентТовПосле"}
    return any(node.tag.rsplit("}", 1)[-1] in tags for node in root.iter())


def gis_receipts(payload: dict | None) -> list[dict]:
    payload = payload or {}
    if "_dz_gis_receipts" in payload:
        return payload["_dz_gis_receipts"]
    event = payload.get("LastEvent") or payload
    docflow = event.get("Docflow") or {}
    receipts = []
    for scope, block in (("document", docflow), ("revocation", docflow.get("Revocation") or {})):
        for flow in block.get("OuterDocflows") or []:
            if flow.get("DocflowNamedId") != "TtGis":
                continue
            for item in flow.get("StatusEntities") or []:
                status = item.get("Status") or {}
                attachment = (item.get("Attachment") or {}).get("Attachment") or {}
                entity = attachment.get("Entity") or {}
                entity_id = entity.get("EntityId") or status.get("EntityId")
                if not entity_id or not status.get("NamedId"):
                    continue
                receipts.append(
                    {
                        "scope": scope,
                        "parent_id": flow.get("ParentEntityId") or "",
                        "entity_id": entity_id,
                        "current": entity_id == flow.get("LatestStatusEntityId"),
                        "ticks": int((entity.get("CreationTimestamp") or {}).get("Ticks") or 0),
                        "status": status["NamedId"],
                        "label": status.get("FriendlyName") or status["NamedId"],
                        "type": status.get("Type"),
                        "details": status.get("Details") or [],
                        "description": status.get("Description"),
                        "filename": attachment.get("AttachmentFilename")
                        or (
                            f"{entity_id}.bin"
                            if (entity.get("Content") or {}).get("Size")
                            else None
                        ),
                    }
                )
    return receipts


def retain_gis_receipts(previous: dict | None, snapshot: dict) -> dict:
    """Сохраняем квитанции при повторных, частичных и запоздалых ответах."""
    receipts = {row["entity_id"]: row.copy() for row in gis_receipts(previous)}
    fresh = gis_receipts(snapshot)
    for row in fresh:
        old = receipts.get(row["entity_id"])
        if old and old["current"] and not row["current"]:
            row = {**row, "current": True}
        receipts[row["entity_id"]] = row
    latest: dict[tuple[str, str], dict] = {}
    for row in receipts.values():
        if row["current"]:
            key = (row["scope"], row["parent_id"])
            if key not in latest or row["ticks"] >= latest[key]["ticks"]:
                latest[key] = row
    if not receipts:
        return snapshot
    for row in receipts.values():
        row["current"] = latest.get((row["scope"], row["parent_id"])) is row
    return {
        **snapshot,
        "_dz_gis_receipts": sorted(receipts.values(), key=lambda row: row["ticks"], reverse=True),
    }


def awaiting_gis_result(receipts: list[dict], *, revoked: bool) -> bool:
    current = [row for row in receipts if row["current"]]
    if revoked:
        current = [
            row
            for row in current
            if row["scope"] == "revocation" or row["status"].endswith("FixationCancel")
        ]
    terminal = {
        "SuccessProcessed",
        "SuccessProcessedFixationCancel",
        "ProcessingError",
        "ProcessingErrorFixation",
        "ProcessingErrorFixationCancel",
    }
    return not current or any(row["status"] not in terminal for row in current)
