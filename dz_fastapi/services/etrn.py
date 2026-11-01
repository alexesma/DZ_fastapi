"""ЭТрН через Диадок: перевозки, подписание Т1/Т3, синхронизация статусов.

Подписание титулов ЭТрН по-прежнему возможно в 1С, поэтому перед любым
подписанием/отправкой мы заново читаем состояние документооборота в
Диадоке (стадия kl-status, список сообщений) и не отправляем титул,
если он уже подписан в другом месте. От двойной отправки защищают также
состояние `sign_pending`, блокировка строки и операция PostMessage с
детерминированным operationId.
"""
from __future__ import annotations

import logging
import re
from datetime import date, datetime
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.core.time import now_moscow
from dz_fastapi.http.diadoc_client import DiadocClient
from dz_fastapi.models.diadoc import DiadocCloudSignTask
from dz_fastapi.models.etrn import (
    EtrnWaybill,
    TransportAddress,
    TransportCarrier,
    TransportDriver,
    TransportVehicle,
)
from dz_fastapi.models.notification import AppNotificationLevel
from dz_fastapi.models.settings import DiadocIntegrationSettings
from dz_fastapi.services.diadoc_boxes import diadoc_box_variants
from dz_fastapi.services.diadoc_transport import (
    TRANSPORT_TYPE_NAMED_IDS,
    extract_custom_data,
    extract_driver_confirm,
    extract_kl_status,
    extract_mt_id,
    extract_transport_status,
    find_transport_problem,
    kl_status_label,
    t3_already_signed,
)
from dz_fastapi.services.etrn_xml import (
    ETRN_TYPE,
    ETRN_VERSION,
    T1_FUNCTION,
    T1_INDEX,
    T3_FUNCTION,
    T3_INDEX,
    T3_RESULT_LABELS,
    T3_RESULTS,
    T6_FUNCTION,
    T6_INDEX,
    EtrnDataError,
    build_t1_user_data,
    build_t3_user_data,
    build_t6_user_data,
    parse_t1_summary,
)

logger = logging.getLogger("dz_fastapi")

OPERATION_ETRN_T1 = "etrn_t1_send"
OPERATION_ETRN_T3 = "etrn_t3_send"
OPERATION_ETRN_T6 = "etrn_t6_send"

ADDRESS_FIELDS = (
    "label", "zip_code", "region_code", "city", "locality", "street",
    "building", "block", "apartment", "other_info",
)


class EtrnError(ValueError):
    """Ошибка бизнес-логики ЭТрН — текст показывается пользователю."""


def box_guid(value: Any) -> str:
    """ID ящика в виде GUID с дефисами (так его ждёт UserDataXml)."""
    text = str(value or "").strip()
    for variant in diadoc_box_variants(text):
        if re.fullmatch(r"[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}", variant):
            return variant
    return text


def ensure_enabled(integration: DiadocIntegrationSettings) -> None:
    if not integration.etrn_enabled:
        raise EtrnError(
            "Подписание ЭТрН в DragonZap выключено (режим «только чтение»): "
            "титулы подписываются в 1С. Включите «ЭТрН» в настройках Диадока."
        )


def address_snapshot(address: TransportAddress) -> dict[str, Any]:
    return {key: getattr(address, key) for key in ADDRESS_FIELDS}


# ------------------------------------------------------------ черновик


async def _get_or_fail(session: AsyncSession, model: Any, item_id: Any, what: str):
    row = await session.get(model, int(item_id))
    if row is None:
        raise EtrnError(f"{what} не найден (id={item_id})")
    return row


def _driver_snapshot(driver: TransportDriver) -> dict[str, Any]:
    return {
        "last_name": driver.last_name,
        "first_name": driver.first_name,
        "middle_name": driver.middle_name,
        "license_series": driver.license_series,
        "license_number": driver.license_number,
        "license_date": driver.license_date.isoformat()
        if driver.license_date
        else None,
        "phone": driver.phone,
    }


def _vehicle_snapshot(vehicle: TransportVehicle) -> dict[str, Any]:
    return {
        "plate_number": vehicle.plate_number,
        "brand": vehicle.brand,
        "vehicle_type": vehicle.vehicle_type,
        "max_weight": str(vehicle.max_weight)
        if vehicle.max_weight is not None
        else None,
        "capacity": str(vehicle.capacity)
        if vehicle.capacity is not None
        else None,
        "trailer_number": vehicle.trailer_number,
        "ownership": vehicle.ownership or "1",
    }


async def build_outgoing_payload(
    session: AsyncSession, data: dict[str, Any]
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Снимок данных Т1 из справочников и ручных значений.

    Возвращает (payload, ссылки): ссылки — id перевозчика/водителя/ТС.
    Для каждой сущности можно передать id из справочника либо «разовое»
    значение: carrier_manual / driver_manual / vehicle_manual /
    loading_address_manual / delivery_address_manual.
    """
    refs: dict[str, Any] = {}
    payload: dict[str, Any] = {
        "number": data.get("number"),
        "date": data.get("date"),
        "order_number": data.get("order_number"),
        "order_date": data.get("order_date"),
        "cargo": data.get("cargo") or [],
        "directives": data.get("directives"),
        "warrant": data.get("warrant"),
        "loading": {
            "arrival_at": data.get("loading_arrival_at"),
            "departure_at": data.get("loading_departure_at"),
            "gross_weight": data.get("gross_weight"),
            "places": data.get("places"),
            "weighing_method": data.get("weighing_method"),
        },
    }
    if data.get("carrier_id"):
        carrier = await _get_or_fail(
            session, TransportCarrier, data["carrier_id"], "Перевозчик"
        )
        refs["carrier_id"] = int(carrier.id)
        payload["carrier"] = {
            "name": carrier.name, "inn": carrier.inn, "kpp": carrier.kpp,
            "box_id": box_guid(carrier.diadoc_box_id)
            if carrier.diadoc_box_id else None,
            "phone": carrier.phone,
        }
    elif data.get("carrier_manual"):
        manual = dict(data["carrier_manual"])
        manual["box_id"] = box_guid(manual.get("box_id")) or None
        payload["carrier"] = manual
    if data.get("driver_id"):
        driver = await _get_or_fail(
            session, TransportDriver, data["driver_id"], "Водитель"
        )
        refs["driver_id"] = int(driver.id)
        payload["driver"] = _driver_snapshot(driver)
    elif data.get("driver_manual"):
        payload["driver"] = dict(data["driver_manual"])
    if data.get("vehicle_id"):
        vehicle = await _get_or_fail(
            session, TransportVehicle, data["vehicle_id"], "ТС"
        )
        refs["vehicle_id"] = int(vehicle.id)
        payload["vehicle"] = _vehicle_snapshot(vehicle)
    elif data.get("vehicle_manual"):
        payload["vehicle"] = dict(data["vehicle_manual"])
    for prefix, key in (
        ("loading", "loading_address"),
        ("delivery", "delivery_address"),
    ):
        if data.get(f"{prefix}_address_id"):
            address = await _get_or_fail(
                session,
                TransportAddress,
                data[f"{prefix}_address_id"],
                "Адрес",
            )
            payload[key] = address_snapshot(address)
        elif data.get(f"{prefix}_address_manual"):
            payload[key] = dict(data[f"{prefix}_address_manual"])
    return payload, refs


async def upsert_outgoing_draft(
    session: AsyncSession,
    *,
    integration: DiadocIntegrationSettings,
    data: dict[str, Any],
    waybill: Optional[EtrnWaybill] = None,
) -> EtrnWaybill:
    ensure_enabled(integration)
    if waybill is not None and (
        waybill.direction != "outgoing"
        or waybill.status not in ("draft", "error")
        or waybill.t1_message_id
    ):
        raise EtrnError("Редактировать можно только неотправленный черновик")
    payload, refs = await build_outgoing_payload(session, data)
    if waybill is None:
        waybill = EtrnWaybill(
            direction="outgoing",
            status="draft",
            environment=str(integration.environment or "staging"),
        )
        session.add(waybill)
    waybill.box_id_guid = integration.box_id_guid
    waybill.number = payload.get("number")
    waybill.waybill_date = _as_date(payload.get("date"))
    waybill.shipment_id = data.get("shipment_id")
    waybill.customer_id = data.get("customer_id")
    waybill.carrier_id = refs.get("carrier_id")
    waybill.driver_id = refs.get("driver_id")
    waybill.vehicle_id = refs.get("vehicle_id")
    waybill.counteragent_box_id = (
        (payload.get("carrier") or {}).get("box_id") or None
    )
    consignee_box = data.get("consignee_box_id")
    payload["consignee"] = {
        "box_id": box_guid(consignee_box) if consignee_box else None,
        "phone": data.get("consignee_phone"),
        "name": data.get("consignee_name"),
    }
    waybill.payload = payload
    waybill.last_error = None
    await session.commit()
    await session.refresh(waybill)
    return waybill


def _as_date(value: Any) -> Optional[date]:
    if isinstance(value, date):
        return value
    text = str(value or "").strip()
    for fmt in ("%Y-%m-%d", "%d.%m.%Y"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def _prepare_t1_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """Строки дат/времени из JSON снимка → типы для XML-билдера."""
    prepared = dict(payload)
    waybill_date = _as_date(payload.get("date"))
    prepared["date"] = waybill_date
    prepared["order_date"] = _as_date(payload.get("order_date")) or waybill_date
    loading = dict(payload.get("loading") or {})
    for key in ("arrival_at", "departure_at"):
        raw = loading.get(key)
        if isinstance(raw, str) and raw:
            loading[key] = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    if not loading.get("arrival_at") or not loading.get("departure_at"):
        raise EtrnDataError("Укажите время прибытия и убытия на погрузке")
    prepared["loading"] = loading
    driver = dict(payload.get("driver") or {})
    if driver.get("license_date"):
        driver["license_date"] = _as_date(driver["license_date"])
    prepared["driver"] = driver
    warrant = payload.get("warrant")
    if warrant and warrant.get("date"):
        prepared["warrant"] = {**warrant, "date": _as_date(warrant["date"])}
    return prepared


# --------------------------------------------------- проверки в Диадоке


async def find_remote_waybill(
    client: DiadocClient,
    *,
    box_id_guid: str,
    number: str,
    direction: str,
) -> Optional[dict[str, Any]]:
    """Есть ли в Диадоке уже ЭТрН с таким номером (например, из 1С)."""
    category = (
        "Any.OutboundNotRecycled"
        if direction == "outgoing"
        else "Any.InboundNotRecycled"
    )
    payload = await client.get_documents(
        box_id_guid=box_id_guid,
        filter_category=category,
        count=50,
        document_number=number,
    )
    for raw in payload.get("Documents") or []:
        if str(raw.get("TypeNamedId") or "") in TRANSPORT_TYPE_NAMED_IDS:
            return raw
    return None


async def _cancel_waiting_tasks(
    session: AsyncSession, operation: str, waybill_id: int
) -> None:
    rows = (
        await session.execute(
            select(DiadocCloudSignTask).where(
                DiadocCloudSignTask.operation == operation,
                DiadocCloudSignTask.state == "waiting_code",
            )
        )
    ).scalars().all()
    for task in rows:
        if (task.params or {}).get("etrn_id") == waybill_id:
            task.state = "cancelled"
            task.error_details = "Заменена новой попыткой подписания"
            session.add(task)


# ---------------------------------------------------------------- Т1


async def start_t1_sign(
    session: AsyncSession,
    *,
    client: DiadocClient,
    integration: DiadocIntegrationSettings,
    waybill: EtrnWaybill,
) -> DiadocCloudSignTask:
    ensure_enabled(integration)
    if waybill.direction != "outgoing" or waybill.t1_message_id:
        raise EtrnError("Т1 по этой перевозке уже отправлен")
    if waybill.status not in ("draft", "error", "sign_pending"):
        raise EtrnError(f"Нельзя подписать Т1 в статусе {waybill.status}")
    payload = waybill.payload or {}
    if not integration.box_id_guid or not integration.organization_inn:
        raise EtrnError("Диадок не подключён или не заполнен ИНН организации")
    remote = await find_remote_waybill(
        client,
        box_id_guid=integration.box_id_guid,
        number=str(waybill.number or ""),
        direction="outgoing",
    )
    if remote is not None:
        waybill.t1_message_id = remote.get("MessageId")
        waybill.t1_entity_id = remote.get("EntityId")
        waybill.status = "sent"
        waybill.last_error = None
        await session.commit()
        raise EtrnError(
            "ЭТрН с таким номером уже есть в Диадоке (подписана в 1С?) — "
            "повторно не отправляем, статус обновится при синхронизации"
        )
    user_data = build_t1_user_data(
        _prepare_t1_payload(payload),
        signer_full_name=str(integration.signer_full_name or ""),
        signer_position=str(integration.signer_position or ""),
        shipper_box_id=box_guid(integration.box_id_guid),
        shipper_inn=str(integration.organization_inn),
        shipper_phone=payload.get("shipper_phone"),
    )
    content, file_name = await client.generate_title_xml(
        box_id_guid=integration.box_id_guid,
        document_type_named_id=ETRN_TYPE,
        document_function=T1_FUNCTION,
        document_version=ETRN_VERSION,
        title_index=T1_INDEX,
        user_data_xml=user_data,
    )
    from dz_fastapi.services.diadoc_cloud_sign import _encode_file

    files = [
        {
            "file_name": file_name or f"etrn_t1_{waybill.id}.xml",
            "content_b64": _encode_file(content),
            "kind": "etrn_t1",
            "parent_entity_id": None,
        }
    ]
    token = await client.cloud_sign(
        files=[
            {
                "FileName": files[0]["file_name"],
                "Content": {"Content": files[0]["content_b64"]},
            }
        ]
    )
    await _cancel_waiting_tasks(session, OPERATION_ETRN_T1, int(waybill.id))
    task = DiadocCloudSignTask(
        environment=waybill.environment,
        operation=OPERATION_ETRN_T1,
        state="waiting_code",
        box_id_guid=integration.box_id_guid,
        cloud_sign_token=token,
        files=files,
        params={"etrn_id": int(waybill.id)},
    )
    session.add(task)
    waybill.status = "sign_pending"
    waybill.last_error = None
    await session.commit()
    await session.refresh(task)
    return task


async def apply_t1_sign(
    session: AsyncSession,
    *,
    client: DiadocClient,
    task: DiadocCloudSignTask,
    signatures: list[str],
) -> dict[str, Any]:
    from dz_fastapi.services.diadoc_outgoing import _extract_first_entity_id, _extract_message_id

    waybill = await _locked(session, (task.params or {}).get("etrn_id"))
    if waybill.t1_message_id or waybill.status != "sign_pending":
        raise EtrnError(
            "Т1 уже отправлен или попытка отменена — повторно не отправляем"
        )
    carrier_box = waybill.counteragent_box_id
    if not carrier_box:
        raise EtrnError("Не указан ящик перевозчика")
    item = task.files[0]
    response = await client.post_message(
        message={
            "FromBoxId": waybill.box_id_guid,
            "ToBoxId": carrier_box,
            "DocumentAttachments": [
                {
                    "SignedContent": {
                        "Content": item["content_b64"],
                        "Signature": signatures[0],
                    },
                    "TypeNamedId": ETRN_TYPE,
                    "Function": T1_FUNCTION,
                    "Version": ETRN_VERSION,
                }
            ],
        },
        operation_id=f"etrn-t1-{waybill.id}",
    )
    waybill.t1_message_id = _extract_message_id(response)
    waybill.t1_entity_id = _extract_first_entity_id(response)
    waybill.status = "sent"
    waybill.sent_at = now_moscow()
    waybill.last_error = None
    session.add(waybill)
    return {"message_id": waybill.t1_message_id}


async def _locked(session: AsyncSession, waybill_id: Any) -> EtrnWaybill:
    row = (
        await session.execute(
            select(EtrnWaybill)
            .where(EtrnWaybill.id == int(waybill_id or 0))
            .with_for_update()
        )
    ).scalar_one_or_none()
    if row is None:
        raise EtrnError("Перевозка не найдена")
    return row


# ------------------------------------------------------- статус / синхр.


def _status_from_kl(kl: Optional[str], problem: Optional[str]) -> Optional[str]:
    if kl in ("r-rejected", "revoked"):
        return "refused"
    if problem:
        return "error"
    if kl == "completed":
        return "delivered"
    if kl and (kl == "on-the-way" or kl.startswith(("d-", "pd-"))):
        return "in_transit"
    if kl:
        return "sent"
    return None


async def apply_remote_payload(
    session: AsyncSession,
    waybill: EtrnWaybill,
    payload: dict[str, Any],
) -> Optional[str]:
    """Переносит стадию/статус ГИС ЭПД из GetDocument; возвращает проблему."""
    transport = extract_transport_status(payload) or {}
    kl = extract_kl_status(payload)
    if kl:
        waybill.kl_status = kl
    mt_id = extract_mt_id(payload)
    if mt_id:
        waybill.mt_id = mt_id
    if transport:
        waybill.gis_status_type = transport.get("status_type")
        waybill.gis_status_text = transport.get("status_text")
    driver_confirm = extract_driver_confirm(payload)
    if driver_confirm:
        waybill.payload = {
            **(waybill.payload or {}),
            "driver_confirm": driver_confirm,
        }
    problem = find_transport_problem(payload)
    new_status = _status_from_kl(waybill.kl_status, problem)
    if new_status and waybill.status not in ("draft", "sign_pending"):
        waybill.status = new_status
    waybill.last_error = problem
    waybill.last_synced_at = now_moscow()
    if t3_already_signed(waybill.kl_status) and waybill.t3_state in (
        "none",
        "pending_sign",
    ):
        waybill.t3_state = "signed_external"
    session.add(waybill)
    return problem


async def notify_problem(
    session: AsyncSession, waybill: EtrnWaybill, problem: Optional[str]
) -> bool:
    """Одно уведомление админам на каждую новую проблему перевозки."""
    if not problem:
        return False
    key = f"{waybill.kl_status or ''}|{problem}"[:255]
    if waybill.problem_notified_key == key:
        return False
    from dz_fastapi.services.notifications import create_admin_notifications

    label = waybill.number or waybill.mt_id or f"#{waybill.id}"
    await create_admin_notifications(
        session,
        title=f"ЭТрН {label}: требуется внимание",
        message=(
            f"{problem}. Стадия: "
            f"{kl_status_label(waybill.kl_status) or 'неизвестна'}."
            " Откройте «Диадок → Перевозки» и проверьте документооборот."
        ),
        level=AppNotificationLevel.ERROR,
        link="/diadoc",
        payload={"kind": "etrn_problem", "waybill_id": int(waybill.id)},
        commit=False,
    )
    waybill.problem_notified_key = key
    session.add(waybill)
    return True


async def refresh_waybill(
    session: AsyncSession,
    *,
    client: DiadocClient,
    waybill: EtrnWaybill,
) -> Optional[str]:
    if not (waybill.box_id_guid and waybill.t1_message_id and waybill.t1_entity_id):
        return None
    payload = await client.get_document(
        box_id_guid=waybill.box_id_guid,
        message_id=waybill.t1_message_id,
        entity_id=waybill.t1_entity_id,
    )
    problem = await apply_remote_payload(session, waybill, payload)
    await notify_problem(session, waybill, problem)
    return problem


def _is_cost_title(raw: dict[str, Any]) -> bool:
    custom = extract_custom_data(raw)
    return (
        str(raw.get("Function") or "").lower() == "cost"
        or custom.get("kl-func") == "cost"
    )


def _is_delivery_title(raw: dict[str, Any]) -> bool:
    custom = extract_custom_data(raw)
    return (
        str(raw.get("Function") or "").lower() in ("fwd", "delivery")
        or custom.get("kl-func") == "delivery"
    )


async def sync_etrn(
    session: AsyncSession,
    *,
    client: DiadocClient,
    integration: DiadocIntegrationSettings,
    count: int = 100,
) -> dict[str, int]:
    """Читает ЭТрН из ящика: новые перевозки, стадия, УИД, проблемы."""
    box = integration.box_id_guid
    if not box:
        raise EtrnError("Диадок не подключён")
    env = str(integration.environment or "staging")
    result = {"created": 0, "updated": 0, "problems": 0, "linked_t3": 0}
    seen: list[EtrnWaybill] = []
    deliveries: list[dict[str, Any]] = []
    for direction, category in (
        ("incoming", "Any.InboundNotRecycled"),
        ("outgoing", "Any.OutboundNotRecycled"),
    ):
        listing = await client.get_documents(
            box_id_guid=box, filter_category=category, count=count
        )
        for raw in listing.get("Documents") or []:
            if str(raw.get("TypeNamedId") or "") not in (
                TRANSPORT_TYPE_NAMED_IDS
            ):
                continue
            if _is_cost_title(raw) or _is_delivery_title(raw):
                deliveries.append({**raw, "_direction": direction})
                continue
            message_id = str(raw.get("MessageId") or "")
            entity_id = str(raw.get("EntityId") or "")
            if not message_id or not entity_id:
                continue
            waybill = (
                await session.execute(
                    select(EtrnWaybill).where(
                        EtrnWaybill.environment == env,
                        EtrnWaybill.direction == direction,
                        EtrnWaybill.t1_message_id == message_id,
                    )
                )
            ).scalar_one_or_none()
            if waybill is None:
                waybill = EtrnWaybill(
                    environment=env,
                    direction=direction,
                    status="sent",
                    box_id_guid=box,
                    t1_message_id=message_id,
                    t1_entity_id=entity_id,
                    payload={},
                )
                session.add(waybill)
                result["created"] += 1
            else:
                result["updated"] += 1
            waybill.number = raw.get("DocumentNumber") or waybill.number
            waybill.counteragent_box_id = (
                raw.get("CounteragentBoxId") or waybill.counteragent_box_id
            )
            full = await client.get_document(
                box_id_guid=box, message_id=message_id, entity_id=entity_id
            )
            problem = await apply_remote_payload(session, waybill, full)
            if direction == "incoming" and not (waybill.payload or {}).get(
                "summary"
            ):
                try:
                    content = await client.get_entity_content(
                        box_id_guid=box,
                        message_id=message_id,
                        entity_id=entity_id,
                    )
                    summary = parse_t1_summary(content)
                    waybill.payload = {**(waybill.payload or {}), "summary": summary}
                    waybill.number = summary.get("number") or waybill.number
                    waybill.waybill_date = _as_date(summary.get("date"))
                except Exception as exc:  # титул мог быть ещё не доступен
                    logger.warning("ETrN summary %s: %s", message_id, exc)
            await session.flush()
            if await notify_problem(session, waybill, problem):
                result["problems"] += 1
            seen.append(waybill)
    for raw in deliveries:
        mt_id = extract_custom_data(raw).get("mt-id")
        if not mt_id:
            continue
        target = (
            await session.execute(
                select(EtrnWaybill).where(
                    EtrnWaybill.environment == env,
                    EtrnWaybill.mt_id == mt_id,
                )
            )
        ).scalars().first()
        if target is not None and _is_cost_title(raw):
            cost = dict((target.payload or {}).get("cost") or {})
            if raw["_direction"] == "incoming":
                if not cost.get("t5_entity_id"):
                    cost["t5_message_id"] = raw.get("MessageId")
                    cost["t5_entity_id"] = raw.get("EntityId")
                    cost.setdefault("t6_state", "none")
                    target.payload = {**(target.payload or {}), "cost": cost}
                    session.add(target)
            continue
        if target is None or target.t3_message_id:
            continue
        target.t3_message_id = raw.get("MessageId")
        if target.t3_state in ("none", "pending_sign"):
            target.t3_state = "signed_external"
        session.add(target)
        result["linked_t3"] += 1
    await session.commit()
    return result


# ---------------------------------------------------------------- Т3


def _our_role_is_consignee(
    waybill: EtrnWaybill, integration: DiadocIntegrationSettings
) -> bool:
    summary = (waybill.payload or {}).get("summary") or {}
    inn = str((summary.get("consignee") or {}).get("inn") or "").strip()
    ours = str(integration.organization_inn or "").strip()
    return bool(inn and ours and inn == ours)


async def _resolve_carrier_box(
    session: AsyncSession, waybill: EtrnWaybill, explicit: Optional[str]
) -> str:
    if explicit:
        return box_guid(explicit)
    summary = (waybill.payload or {}).get("summary") or {}
    inn = str((summary.get("carrier") or {}).get("inn") or "").strip()
    if inn:
        carrier = (
            await session.execute(
                select(TransportCarrier).where(
                    TransportCarrier.inn == inn,
                    TransportCarrier.diadoc_box_id.is_not(None),
                )
            )
        ).scalars().first()
        if carrier is not None:
            return box_guid(carrier.diadoc_box_id)
    raise EtrnError(
        "Не найден ящик перевозчика: добавьте перевозчика (ИНН "
        f"{inn or 'не определён'}) в справочник с ящиком Диадока "
        "или выберите его вручную"
    )


async def start_t3_sign(
    session: AsyncSession,
    *,
    client: DiadocClient,
    integration: DiadocIntegrationSettings,
    waybill: EtrnWaybill,
    form: dict[str, Any],
) -> DiadocCloudSignTask:
    ensure_enabled(integration)
    if waybill.direction != "incoming":
        raise EtrnError("Т3 формируется только по входящей перевозке")
    if not _our_role_is_consignee(waybill, integration):
        raise EtrnError(
            "Наша организация не указана грузополучателем в Т1 — Т3 не нужен"
        )
    result = str(form.get("result") or "")
    if result not in T3_RESULTS:
        raise EtrnError("Выберите результат приёмки")
    if waybill.t3_state in ("signed_here", "signed_external"):
        raise EtrnError("Т3 по этой перевозке уже подписан")
    # актуальное состояние документооборота — ЭТрН могли принять в 1С
    await refresh_waybill(session, client=client, waybill=waybill)
    if t3_already_signed(waybill.kl_status):
        waybill.t3_state = "signed_external"
        await session.commit()
        raise EtrnError(
            "Т3 уже подписан (вероятно, в 1С): стадия "
            f"«{kl_status_label(waybill.kl_status)}». Повторно не подписываем."
        )
    carrier_box = await _resolve_carrier_box(
        session, waybill, form.get("carrier_box_id")
    )
    summary = (waybill.payload or {}).get("summary") or {}
    delivery_address = form.get("delivery_address")
    if result != "rejected" and not delivery_address:
        raise EtrnError("Укажите адрес выгрузки")
    user_data = build_t3_user_data(
        organization_name=str(integration.organization_name or ""),
        signer_full_name=str(integration.signer_full_name or ""),
        signer_position=str(integration.signer_position or ""),
        result=result,
        delivery_address=delivery_address,
        arrival_at=_dt(form.get("arrival_at")),
        departure_at=_dt(form.get("departure_at")),
        stated_arrival_at=_dt(form.get("stated_arrival_at")),
        places_received=form.get("places_received"),
        gross_weight_received=form.get("gross_weight_received"),
        cargo_status=form.get("cargo_status"),
        rejection_reason=form.get("rejection_reason"),
        warrant=form.get("warrant"),
    )
    content, file_name = await client.generate_title_xml(
        box_id_guid=waybill.box_id_guid or integration.box_id_guid or "",
        document_type_named_id=ETRN_TYPE,
        document_function=T3_FUNCTION,
        document_version=ETRN_VERSION,
        title_index=T3_INDEX,
        user_data_xml=user_data,
        letter_id=waybill.t1_message_id,
        document_id=waybill.t1_entity_id,
    )
    from dz_fastapi.services.diadoc_cloud_sign import _encode_file

    files = [
        {
            "file_name": file_name or f"etrn_t3_{waybill.id}.xml",
            "content_b64": _encode_file(content),
            "kind": "etrn_t3",
            "parent_entity_id": waybill.t1_entity_id,
        }
    ]
    token = await client.cloud_sign(
        files=[
            {
                "FileName": files[0]["file_name"],
                "Content": {"Content": files[0]["content_b64"]},
            }
        ]
    )
    await _cancel_waiting_tasks(session, OPERATION_ETRN_T3, int(waybill.id))
    task = DiadocCloudSignTask(
        environment=waybill.environment,
        operation=OPERATION_ETRN_T3,
        state="waiting_code",
        box_id_guid=waybill.box_id_guid or integration.box_id_guid,
        message_id=waybill.t1_message_id,
        entity_id=waybill.t1_entity_id,
        cloud_sign_token=token,
        files=files,
        params={
            "etrn_id": int(waybill.id),
            "result": result,
            "carrier_box_id": carrier_box,
            "summary_number": summary.get("number"),
        },
    )
    session.add(task)
    waybill.t3_state = "pending_sign"
    waybill.t3_result = result
    waybill.t3_data = {
        k: form.get(k)
        for k in (
            "places_received", "gross_weight_received", "cargo_status",
            "rejection_reason",
        )
    }
    waybill.last_error = None
    await session.commit()
    await session.refresh(task)
    return task


def _dt(value: Any) -> Optional[datetime]:
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))


async def apply_t3_sign(
    session: AsyncSession,
    *,
    client: DiadocClient,
    task: DiadocCloudSignTask,
    signatures: list[str],
) -> dict[str, Any]:
    from dz_fastapi.services.diadoc_outgoing import _extract_message_id

    waybill = await _locked(session, (task.params or {}).get("etrn_id"))
    if waybill.t3_state != "pending_sign":
        raise EtrnError(
            "Т3 уже подписан или попытка отменена — повторно не отправляем"
        )
    # последняя проверка: за время ожидания SMS Т3 могли подписать в 1С
    await refresh_waybill(session, client=client, waybill=waybill)
    if t3_already_signed(waybill.kl_status):
        waybill.t3_state = "signed_external"
        session.add(waybill)
        await session.commit()
        raise EtrnError("Т3 уже подписан в другом месте — не отправляем")
    item = task.files[0]
    response = await client.post_message(
        message={
            "FromBoxId": waybill.box_id_guid,
            "ToBoxId": (task.params or {})["carrier_box_id"],
            "DocumentAttachments": [
                {
                    "SignedContent": {
                        "Content": item["content_b64"],
                        "Signature": signatures[0],
                    },
                    "TypeNamedId": ETRN_TYPE,
                    "Function": T3_FUNCTION,
                    "Version": ETRN_VERSION,
                    "InitialDocumentIds": [
                        {"ParentEntityId": waybill.t1_entity_id}
                    ],
                }
            ],
        },
        operation_id=f"etrn-t3-{waybill.id}",
    )
    waybill.t3_message_id = _extract_message_id(response)
    waybill.t3_state = "signed_here"
    waybill.last_error = None
    session.add(waybill)
    return {
        "message_id": waybill.t3_message_id,
        "result": T3_RESULT_LABELS.get(waybill.t3_result or "", ""),
    }


# ---------------------------------------------------------------- Т6


def _cost(waybill: EtrnWaybill) -> dict[str, Any]:
    return dict((waybill.payload or {}).get("cost") or {})


def _set_cost(waybill: EtrnWaybill, **changes: Any) -> None:
    cost = {**_cost(waybill), **changes}
    waybill.payload = {**(waybill.payload or {}), "cost": cost}


async def start_t6_sign(
    session: AsyncSession,
    *,
    client: DiadocClient,
    integration: DiadocIntegrationSettings,
    waybill: EtrnWaybill,
    form: dict[str, Any],
) -> DiadocCloudSignTask:
    """Т6 — ответ грузоотправителя на Т5 (стоимость), только по флагу."""
    ensure_enabled(integration)
    if not integration.etrn_t6_enabled:
        raise EtrnError("Т6 (стоимость) выключен в настройках Диадока")
    if waybill.direction != "outgoing":
        raise EtrnError("Т6 подписывает грузоотправитель по своей перевозке")
    cost = _cost(waybill)
    if not cost.get("t5_entity_id"):
        raise EtrnError("Перевозчик ещё не прислал Т5 (стоимость)")
    if cost.get("t6_state") in ("signed_here",):
        raise EtrnError("Т6 по этой перевозке уже подписан")
    user_data = build_t6_user_data(
        signer_full_name=str(integration.signer_full_name or ""),
        signer_position=str(integration.signer_position or ""),
        department=form.get("department"),
        okpo=form.get("okpo"),
        info=form.get("info"),
        bank=form.get("bank"),
        warrant=form.get("warrant"),
    )
    content, file_name = await client.generate_title_xml(
        box_id_guid=waybill.box_id_guid or integration.box_id_guid or "",
        document_type_named_id=ETRN_TYPE,
        document_function=T6_FUNCTION,
        document_version=ETRN_VERSION,
        title_index=T6_INDEX,
        user_data_xml=user_data,
        letter_id=cost["t5_message_id"],
        document_id=cost["t5_entity_id"],
    )
    from dz_fastapi.services.diadoc_cloud_sign import _encode_file

    files = [
        {
            "file_name": file_name or f"etrn_t6_{waybill.id}.xml",
            "content_b64": _encode_file(content),
            "kind": "etrn_t6",
            "parent_entity_id": cost["t5_entity_id"],
        }
    ]
    token = await client.cloud_sign(
        files=[
            {
                "FileName": files[0]["file_name"],
                "Content": {"Content": files[0]["content_b64"]},
            }
        ]
    )
    await _cancel_waiting_tasks(session, OPERATION_ETRN_T6, int(waybill.id))
    task = DiadocCloudSignTask(
        environment=waybill.environment,
        operation=OPERATION_ETRN_T6,
        state="waiting_code",
        box_id_guid=waybill.box_id_guid or integration.box_id_guid,
        message_id=cost["t5_message_id"],
        entity_id=cost["t5_entity_id"],
        cloud_sign_token=token,
        files=files,
        params={"etrn_id": int(waybill.id)},
    )
    session.add(task)
    _set_cost(waybill, t6_state="pending_sign")
    await session.commit()
    await session.refresh(task)
    return task


async def apply_t6_sign(
    session: AsyncSession,
    *,
    client: DiadocClient,
    task: DiadocCloudSignTask,
    signatures: list[str],
) -> dict[str, Any]:
    waybill = await _locked(session, (task.params or {}).get("etrn_id"))
    cost = _cost(waybill)
    if cost.get("t6_state") != "pending_sign":
        raise EtrnError(
            "Т6 уже подписан или попытка отменена — повторно не отправляем"
        )
    item = task.files[0]
    await client.post_message_patch(
        patch={
            "BoxId": task.box_id_guid,
            "MessageId": cost["t5_message_id"],
            "RecipientTitles": [
                {
                    "ParentEntityId": cost["t5_entity_id"],
                    "SignedContent": {
                        "Content": item["content_b64"],
                        "Signature": signatures[0],
                    },
                }
            ],
        },
        operation_id=f"etrn-t6-{waybill.id}",
    )
    _set_cost(waybill, t6_state="signed_here")
    session.add(waybill)
    return {"t6": "signed"}
