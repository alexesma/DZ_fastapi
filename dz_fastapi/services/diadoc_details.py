"""Разбор прочих входящих документов Диадока: акты, акты о расхождениях."""

from __future__ import annotations

import os
import re
import xml.etree.ElementTree as ET
from typing import Any, Optional

from dz_fastapi.models.diadoc import DiadocIncomingDocument
from dz_fastapi.services.upd_xml import extract_document_parties, parse_discrepancy_act_xml

_TOTAL_ATTRS = ("СтТовУчНалВсего", "СумУчНал", "ВсегоСНДС", "СтоимУчНал")


def _read(document: DiadocIncomingDocument) -> Optional[bytes]:
    path = document.local_file_path
    if not path or not os.path.exists(os.path.abspath(path)):
        return None
    with open(os.path.abspath(path), "rb") as fh:
        return fh.read()


def _act_total(payload: bytes) -> Optional[float]:
    try:
        root = ET.fromstring(payload)
    except ET.ParseError:
        return None
    for element in root.iter():
        for name in _TOTAL_ATTRS:
            raw = element.get(name)
            if raw:
                try:
                    return float(str(raw).replace(",", ".").replace(" ", ""))
                except ValueError:
                    continue
    return None


def collect_details(document: DiadocIncomingDocument, category: str) -> dict[str, Any]:
    """Сведения для списка входящих: стороны и сумма акта либо расхождения."""
    payload = _read(document)
    if payload is None:
        return {}
    if category == "discrepancy_act":
        return parse_discrepancy_act_xml(payload) or {}
    details: dict[str, Any] = {}
    parties = extract_document_parties(payload)
    if parties is not None:
        details["seller_inn"] = parties.seller_inn
        details["buyer_inn"] = parties.buyer_inn
    total = _act_total(payload)
    if total is None:
        for block in _metadata_blocks(document):
            try:
                total = float(str(block.get("Total")).replace(",", "."))
                break
            except (TypeError, ValueError):
                continue
    if total is not None:
        details["total"] = total
        document.total_amount = total
    works, basis = [], None
    try:
        root = ET.fromstring(payload)
        for element in root.iter():
            tag = element.tag.rsplit("}", 1)[-1]
            if tag == "Работа" and element.get("Описание"):
                works.append(element.get("Описание"))
            elif tag == "Основание" and basis is None:
                basis = (
                    " ".join(str(v) for v in element.attrib.values())
                    or (element.text or "").strip()
                    or None
                )
    except ET.ParseError:
        pass
    if works:
        details["works"] = works[:20]
    if basis:
        details["basis"] = basis
    for block in _metadata_blocks(document):
        details["grounds"] = block.get("Grounds") or details.get("basis")
        details["diadoc_status"] = block.get("DocumentStatus")
        break
    return details


_RETURN_RE = re.compile(r"возврат|возвр\.", re.I)
_RETURN_TAGS = ("ПередатДокум", "ИнфПолФХЖ", "СодФХЖ", "Основание", "ТекстИнф")


def _metadata_blocks(document: DiadocIncomingDocument) -> list[dict]:
    meta = document.raw_metadata or {}
    return [v for k, v in meta.items() if str(k).endswith("Metadata") and isinstance(v, dict)]


def detect_return_marker(document: DiadocIncomingDocument) -> Optional[str]:
    """Текст основания, говорящий о возврате товара («возврат», «возвр.»).

    Смотрим основание в метаданных Диадока (Grounds) и в XML: ПередатДокум,
    ИнфПолФХЖ, СодФХЖ*, Основание, ТекстИнф.
    """
    for block in _metadata_blocks(document):
        grounds = str(block.get("Grounds") or "")
        if _RETURN_RE.search(grounds):
            return grounds
    payload = _read(document)
    if payload is None:
        return None
    try:
        root = ET.fromstring(payload)
    except ET.ParseError:
        return None
    for element in root.iter():
        tag = element.tag.rsplit("}", 1)[-1]
        if not tag.startswith(_RETURN_TAGS):
            continue
        for value in [*element.attrib.values(), element.text or ""]:
            if _RETURN_RE.search(str(value)):
                return str(value).strip()
    return None
