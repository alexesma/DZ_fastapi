"""Статусы перевозочных документов (ЭТрН, ЭПЛ, заказ-заявка) из Диадока.

Диадок сам обменивается данными с ГИС ЭПД и возвращает результат этого
обмена во внешнем документообороте `OuterDocflowInfo` с
`DocflowNamedId = "KlMt"`. Мы этот документооборот не ведём — только
читаем статус, чтобы менеджер видел, где перевозка, не выходя из системы.
Подписание титулов остаётся в 1С.

Модуль состоит из чистых функций: разбор payload не требует ни сессии,
ни сети, поэтому его можно тестировать на зафиксированных ответах API.
"""
from __future__ import annotations

from typing import Any, Optional

# Идентификатор внешнего документооборота ГИС ЭПД в ответах Диадока.
GIS_EPD_DOCFLOW_NAMED_ID = "KlMt"

# Типы перевозочных документов. Диадок называет их через TypeNamedId;
# набор держим отдельно, чтобы не считать перевозочным обычный УПД.
TRANSPORT_TYPE_NAMED_IDS = frozenset(
    {
        "RoadTransportWaybill",  # ЭТрН — электронная транспортная накладная
        "Waybill",  # ЭПЛ — электронный путевой лист
        "OrderRequest",  # Э33 — заказ-заявка
    }
)

# Ключи в Details внешнего документооборота.
_MINTRANS_ID_KEY = "mt-id"
_MINTRANS_REQUEST_ID_KEY = "mt-rid"
_CARRIAGE_ID_KEY = "kl-id"


def _as_list(value: Any) -> list[Any]:
    """OuterDocflowInfo приходит то объектом, то списком — сводим к списку."""
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def _normalize_details(details: Any) -> dict[str, str]:
    """Details бывает списком пар Key/Value и плоским словарём."""
    result: dict[str, str] = {}
    if isinstance(details, dict):
        for key, value in details.items():
            if key is None or value is None:
                continue
            result[str(key).strip().lower()] = str(value).strip()
        return result
    for entry in _as_list(details):
        if not isinstance(entry, dict):
            continue
        key = entry.get("Key") or entry.get("key") or entry.get("Name")
        value = entry.get("Value") or entry.get("value")
        if key is None or value is None:
            continue
        result[str(key).strip().lower()] = str(value).strip()
    return result


def is_transport_document(payload: dict[str, Any]) -> bool:
    """Перевозочный ли документ.

    Считаем перевозочным либо документ известного типа, либо любой,
    по которому Диадок ведёт документооборот с ГИС ЭПД: набор
    TypeNamedId может расшириться раньше, чем мы обновим константы.
    """
    if not isinstance(payload, dict):
        return False
    type_named_id = str(payload.get("TypeNamedId") or "").strip()
    if type_named_id in TRANSPORT_TYPE_NAMED_IDS:
        return True
    return _find_gis_epd_docflow(payload) is not None


def _find_gis_epd_docflow(payload: dict[str, Any]) -> Optional[dict[str, Any]]:
    if not isinstance(payload, dict):
        return None
    for docflow in _as_list(payload.get("OuterDocflowInfo")):
        if not isinstance(docflow, dict):
            continue
        named_id = str(docflow.get("DocflowNamedId") or "").strip()
        if named_id.lower() == GIS_EPD_DOCFLOW_NAMED_ID.lower():
            return docflow
    return None


def extract_transport_status(
    payload: dict[str, Any],
) -> Optional[dict[str, Any]]:
    """Статус перевозки из ответа GetDocument/GetDocuments.

    Возвращает None, если у документа нет документооборота с ГИС ЭПД —
    значит, это не перевозочный документ и трогать его поля не нужно.
    Иначе — dict с ключами: status_named_id, status_type, status_text,
    mintrans_id, carriage_id, request_id.
    """
    docflow = _find_gis_epd_docflow(payload)
    if docflow is None:
        return None
    status = docflow.get("Status")
    if not isinstance(status, dict):
        status = {}
    details = _normalize_details(docflow.get("Details"))
    return {
        "status_named_id": (
            str(status.get("NamedId") or "").strip() or None
        ),
        "status_type": str(status.get("Type") or "").strip() or None,
        "status_text": (
            str(status.get("FriendlyName") or "").strip() or None
        ),
        "mintrans_id": details.get(_MINTRANS_ID_KEY) or None,
        "request_id": details.get(_MINTRANS_REQUEST_ID_KEY) or None,
        "carriage_id": details.get(_CARRIAGE_ID_KEY) or None,
    }


def summarize_transport_status(status: Optional[dict[str, Any]]) -> str:
    """Короткая строка для списков и уведомлений."""
    if not status:
        return "—"
    text = status.get("status_text") or status.get("status_named_id")
    if not text:
        return "—"
    mintrans_id = status.get("mintrans_id")
    return f"{text} (ГИС ЭПД {mintrans_id})" if mintrans_id else str(text)
