"""Разбор прочих входящих документов Диадока: акты, акты о расхождениях."""

from __future__ import annotations

import os
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
    if total is not None:
        details["total"] = total
        document.total_amount = total
    return details
