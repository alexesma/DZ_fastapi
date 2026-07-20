"""Structured extraction from customer reclamation attachments."""

from __future__ import annotations

import logging
import os
import re
from datetime import date, datetime
from typing import Any, Optional

import xlrd

logger = logging.getLogger("dz_fastapi")

MAX_RECLAMATION_ATTACHMENT_BYTES = 15 * 1024 * 1024
MAX_SHEET_ROWS = 500
MAX_SHEET_COLS = 200

_SOURCE_DOCUMENT_RE = re.compile(
    r"(?:УПД|сч[её]т-фактура)\s*№?\s*"
    r"([A-Za-zА-Яа-я0-9][A-Za-zА-Яа-я0-9_./-]{0,79})\s+от\s+"
    r"(\d{1,2}[./-]\d{1,2}[./-]\d{2,4})",
    re.IGNORECASE,
)
_ITEM_REASON_RE = re.compile(
    r"^\s*([A-Za-zА-Яа-я0-9][A-Za-zА-Яа-я0-9./-]{3,39})\s*" r"[-–—]\s*(.+?)\s*$"
)


def _normalize_oem(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9]", "", str(value or "")).upper()


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return " ".join(str(value).replace("\xa0", " ").split()).strip()


def _parse_date(value: str) -> Optional[date]:
    normalized = str(value or "").replace("/", ".").replace("-", ".")
    for fmt in ("%d.%m.%Y", "%d.%m.%y"):
        try:
            return datetime.strptime(normalized, fmt).date()
        except ValueError:
            continue
    return None


def _number(value: Any) -> Optional[float]:
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    text = _clean_text(value).replace(" ", "").replace(",", ".")
    if not re.fullmatch(r"[-+]?\d+(?:\.\d+)?", text):
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _sheet_rows(sheet: Any) -> list[list[tuple[int, Any, str]]]:
    rows: list[list[tuple[int, Any, str]]] = []
    for row_index in range(min(int(sheet.nrows), MAX_SHEET_ROWS)):
        row: list[tuple[int, Any, str]] = []
        for col_index in range(min(int(sheet.ncols), MAX_SHEET_COLS)):
            raw = sheet.cell_value(row_index, col_index)
            text = _clean_text(raw)
            if text:
                row.append((col_index, raw, text))
        rows.append(row)
    return rows


def _row_text(row: list[tuple[int, Any, str]]) -> str:
    return " ".join(cell[2] for cell in row)


def _looks_like_torg2(rows: list[list[tuple[int, Any, str]]]) -> bool:
    text = "\n".join(_row_text(row) for row in rows).lower()
    return "унифицированная форма № торг-2" in text or (
        "об установленном расхождении" in text and "фактически оказалось" in text
    )


def _extract_document(rows: list[list[tuple[int, Any, str]]]) -> dict[str, Any]:
    for row in rows:
        match = _SOURCE_DOCUMENT_RE.search(_row_text(row))
        if not match:
            continue
        document_date = _parse_date(match.group(2))
        return {
            "document_number": match.group(1).strip(),
            "document_date": (document_date.isoformat() if document_date else None),
        }
    return {"document_number": None, "document_date": None}


def _extract_reason_items(
    rows: list[list[tuple[int, Any, str]]],
) -> list[dict[str, Any]]:
    start = None
    end = len(rows)
    for index, row in enumerate(rows):
        lowered = _row_text(row).lower()
        if "подробное описание дефектов" in lowered:
            start = index + 1
        elif start is not None and "заключение комиссии" in lowered:
            end = index
            break
    if start is None:
        return []

    result: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in rows[start:end]:
        for _, _, text in row:
            match = _ITEM_REASON_RE.match(text)
            if not match:
                continue
            oem = _normalize_oem(match.group(1))
            reason = match.group(2).strip(" .,:;-")
            if not oem or not reason or oem in seen:
                continue
            seen.add(oem)
            result.append({"oem_number": oem, "reason": reason})
    return result


def _find_actual_values(rows: list[list[tuple[int, Any, str]]], oem: str) -> list[float]:
    normalized = _normalize_oem(oem)
    for row in rows:
        oem_column = None
        for col_index, _, text in row:
            if _normalize_oem(text) == normalized:
                oem_column = col_index
                break
        if oem_column is None:
            continue
        values = [
            number
            for col_index, raw, _ in row
            if col_index > oem_column and (number := _number(raw)) is not None
        ]
        if values:
            return values
    return []


def _find_description(
    rows: list[list[tuple[int, Any, str]]], oem: str
) -> tuple[Optional[str], Optional[str]]:
    normalized = _normalize_oem(oem)
    for row in rows:
        for _, _, text in row:
            tokens = text.split()
            positions = [
                index for index, token in enumerate(tokens) if _normalize_oem(token) == normalized
            ]
            if not positions or len(tokens) < 2:
                continue
            oem_index = positions[-1]
            if oem_index + 1 >= len(tokens):
                continue
            brand = tokens[oem_index + 1].strip(" ,.;") or None
            name_tokens = tokens[slice(oem_index + 2, None)]
            name = " ".join(name_tokens).strip() or None
            return brand, name
    return None, None


def parse_torg2_sheet(sheet: Any) -> Optional[dict[str, Any]]:
    """Extracts stable fields from the customer's TORG-2 workbook sheet."""
    rows = _sheet_rows(sheet)
    if not _looks_like_torg2(rows):
        return None

    document = _extract_document(rows)
    items = _extract_reason_items(rows)
    for item in items:
        values = _find_actual_values(rows, item["oem_number"])
        brand, name = _find_description(rows, item["oem_number"])
        item["quantity"] = max(1, int(values[0])) if values else 1
        item["unit_price"] = values[1] if len(values) > 1 else None
        item["line_sum"] = values[2] if len(values) > 2 else None
        item["brand_name"] = brand
        item["autopart_name"] = name

    reasons = list(dict.fromkeys(item["reason"] for item in items))
    return {
        "parser": "torg2_xls",
        **document,
        "reason": "; ".join(reasons) or None,
        "items": items,
    }


def parse_reclamation_attachment(
    filename: Optional[str], payload: bytes
) -> Optional[dict[str, Any]]:
    """Returns structured data only for known, confidently detected files."""
    extension = os.path.splitext(str(filename or ""))[1].lower()
    if extension != ".xls" or not payload:
        return None
    if len(payload) > MAX_RECLAMATION_ATTACHMENT_BYTES:
        logger.warning(
            "Reclamation attachment %s is too large for parsing: %s bytes",
            filename,
            len(payload),
        )
        return None
    try:
        workbook = xlrd.open_workbook(
            file_contents=payload,
            on_demand=True,
            formatting_info=False,
        )
        for sheet in workbook.sheets():
            parsed = parse_torg2_sheet(sheet)
            if parsed:
                parsed["filename"] = filename
                return parsed
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "Failed to parse reclamation attachment %s: %s",
            filename,
            exc,
        )
    return None
