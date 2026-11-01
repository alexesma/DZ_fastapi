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
        "LogisticsWaybill",  # ЭТрН по документации Диадока (kl_trn_mt_05_01)
        "RoadTransportWaybill",  # прежнее значение, оставлено для совместимости
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


# --- ЭТрН (LogisticsWaybill): стадия, УИД, ошибки ------------------------

# Стадия перевозки — тег kl-status в CustomData документа (ставится
# Диадоком автоматически, пишется только на Т1/Т2).
KL_STAGE_LABELS = {
    "new": "Новая перевозка",
    "wait-mintrans-api": "Ожидает УИД ГИС ЭПД",
    "on-the-way": "В пути",
    "completed": "Перевозка завершена",
    "revoked": "Аннулирована",
    "r-wait-storekeeper": "Погрузка: ждёт кладовщика",
    "r-wait-consignor-sign": "Т1: ждёт подписи грузоотправителя",
    "r-consignor-signed": "Т1 подписан, доставка перевозчику",
    "r-wait-driver-approve": "Т1: ждёт согласия водителя",
    "r-wait-carrier-sign": "Ждёт подписи Т2 перевозчиком",
    "r-carrier-signed": "Т2 подписан перевозчиком",
    "r-rejected": "Перевозчик отказался от груза",
    "d-wait-storekeeper": "Выгрузка: ждёт кладовщика",
    "d-wait-consignee-sign": "Ждёт подписи Т3 грузополучателем",
    "d-consingee-singed": "Т3 подписан, доставка перевозчику",
    "d-wait-driver-approve": "Т3: ждёт согласия водителя",
    "d-wait-carrier-sign": "Ждёт подписи Т4 перевозчиком",
    "d-carrier-signed": "Т4 подписан перевозчиком",
    "pd-carrier-signed": "Т4 подписан (частичная выгрузка)",
    "pd-wait-carrier-readdress-sign": "Ждёт Т7 перевозчика (переадресация)",
    "pd-on-the-way": "Остаток груза в пути",
    "pd-wait-storekeeper": "Новый получатель готовит Т3",
    "pd-wait-consignee-sign": "Ждёт подписи Т3 (частичная выгрузка)",
    "pd-consingee-singed": "Т3 подписан (частичная выгрузка)",
    "pd-wait-driver-approve": "Т3: ждёт согласия водителя (частичная)",
    "pd-wait-carrier-sign": "Ждёт подписи Т4 (частичная выгрузка)",
}

# Стадии, в которых титул получателя (Т3) уже подписан — повторно
# подписывать нельзя (например, если его уже подписали в 1С).
_T3_DONE_STAGES = frozenset(
    {"completed", "revoked", "d-consingee-singed", "d-wait-driver-approve",
     "d-wait-carrier-sign", "d-carrier-signed", "pd-carrier-signed",
     "pd-consingee-singed", "pd-wait-driver-approve", "pd-wait-carrier-sign"}
)
# Стадии, в которых Т1 уже подписан/отправлен.
_T1_DONE_PREFIXES = ("r-consignor-signed", "r-wait-driver-approve",
                     "r-wait-carrier-sign", "r-carrier-signed",
                     "d-", "pd-", "on-the-way", "completed",
                     "wait-mintrans-api")


def extract_custom_data(payload: dict[str, Any]) -> dict[str, str]:
    """CustomData документа (список Key/Value) в словарь."""
    result: dict[str, str] = {}
    for entry in _as_list((payload or {}).get("CustomData")):
        if not isinstance(entry, dict):
            continue
        key = entry.get("Key") or entry.get("key")
        value = entry.get("Value") or entry.get("value")
        if key is None or value is None:
            continue
        result[str(key).strip().lower()] = str(value).strip()
    return result


def extract_kl_status(payload: dict[str, Any]) -> Optional[str]:
    return extract_custom_data(payload).get("kl-status") or None


def kl_status_label(kl_status: Optional[str]) -> Optional[str]:
    if not kl_status:
        return None
    return KL_STAGE_LABELS.get(kl_status, kl_status)


def extract_mt_id(payload: dict[str, Any]) -> Optional[str]:
    """УИД ГИС ЭПД: из статуса KlMt, CustomData или Status.Details."""
    status = extract_transport_status(payload) or {}
    if status.get("mintrans_id"):
        return status["mintrans_id"]
    custom = extract_custom_data(payload).get(_MINTRANS_ID_KEY)
    if custom:
        return custom
    docflow = _find_gis_epd_docflow(payload) or {}
    detail_status = docflow.get("Status")
    if isinstance(detail_status, dict):
        for item in _as_list(detail_status.get("Details")):
            if not isinstance(item, dict):
                continue
            code = str(item.get("Code") or item.get("Key") or "").strip()
            if code.lower() == _MINTRANS_ID_KEY:
                value = item.get("Value") or item.get("Text")
                if value:
                    return str(value).strip()
    return None


def t3_already_signed(kl_status: Optional[str]) -> bool:
    return bool(kl_status) and kl_status in _T3_DONE_STAGES


def t1_already_sent(kl_status: Optional[str]) -> bool:
    return bool(kl_status) and kl_status.startswith(_T1_DONE_PREFIXES)


def find_transport_problem(payload: dict[str, Any]) -> Optional[str]:
    """Текст проблемы перевозки или None.

    Проблема — ошибка во внешнем документообороте (Status.Type = Error),
    отказ перевозчика (r-rejected) или аннулирование.
    """
    for docflow in _as_list((payload or {}).get("OuterDocflowInfo")):
        if not isinstance(docflow, dict):
            continue
        item = docflow.get("Status")
        if isinstance(item, dict) and (
            str(item.get("Type") or "").lower() == "error"
        ):
            return (
                str(item.get("FriendlyName") or "").strip()
                or str(item.get("NamedId") or "").strip()
                or "Ошибка обмена с ГИС ЭПД"
            )
    kl = extract_kl_status(payload)
    if kl == "r-rejected":
        return "Перевозчик отказался от груза"
    if kl == "revoked":
        return "Перевозка аннулирована"
    return None


def extract_driver_confirm(payload: dict[str, Any]) -> Optional[dict[str, Any]]:
    """Согласование водителя (OuterDocflow KlDriverConfirm), если есть."""
    for docflow in _as_list((payload or {}).get("OuterDocflowInfo")):
        if not isinstance(docflow, dict):
            continue
        if str(docflow.get("DocflowNamedId") or "").lower() != "kldriverconfirm":
            continue
        item = docflow.get("Status")
        item = item if isinstance(item, dict) else {}
        named_id = str(item.get("NamedId") or "").strip() or None
        text = str(item.get("FriendlyName") or "").strip() or named_id
        return {
            "named_id": named_id,
            "type": str(item.get("Type") or "").strip() or None,
            "text": text,
            "confirmed": (named_id or "").lower() == "confirm",
        }
    return None
