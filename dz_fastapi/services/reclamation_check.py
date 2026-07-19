"""Движок проверки рекламаций (этап 4).

По данным рекламации собирает чек-лист и формирует рекомендацию:
- была ли отгрузка клиенту и когда (старт срока возврата);
- укладывается ли в срок возврата клиента;
- источник позиции: наш склад или транзит поставщика;
- для транзита — правила поставщика (возврат разрешён? срок? бренд не в стопе?);
- для брака — комплект документов (снятие/установка/дефектовка/фото).

Результат сохраняется в Reclamation.check_result (JSON) и
Reclamation.recommendation (код). Ничего не отправляет и не создаёт возвраты —
это делает этап 5. Статусы new/recognized переводит в checked.
"""
import logging
from datetime import date, datetime
from typing import Any, Optional

from sqlalchemy import func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from dz_fastapi.core.time import now_moscow
from dz_fastapi.models.autopart import AutoPart, preprocess_oem_number
from dz_fastapi.models.inventory import (
    ShipmentDocument,
    ShipmentDocumentItem,
    ShipmentDocumentItemLotAllocation,
    ShipmentDocumentStatus,
)
from dz_fastapi.models.partner import (
    RECLAMATION_STATUS,
    RECLAMATION_TYPE,
    Customer,
    CustomerOrder,
    CustomerOrderItem,
    Provider,
    Reclamation,
)

logger = logging.getLogger("dz_fastapi")

# Срок возврата по умолчанию, если не задан у клиента
DEFAULT_RETURN_WINDOW_DAYS = 14

# Документы, обязательные для рекламации по браку
REQUIRED_DEFECT_DOCS = (
    "removal_order",       # заказ-наряд на снятие
    "installation_order",  # заказ-наряд на установку
    "defect_report",       # дефектовка
    "photo",               # фото запчасти
)
DOC_LABELS = {
    "removal_order": "Заказ-наряд на снятие",
    "installation_order": "Заказ-наряд на установку",
    "defect_report": "Дефектовка",
    "photo": "Фото запчасти",
}

# Коды рекомендаций (сохраняются в Reclamation.recommendation)
REC_APPROVE = "approve"
REC_REJECT = "reject"
REC_REQUEST_DOCUMENTS = "request_documents"
REC_REQUEST_SUPPLIER = "request_supplier"
REC_MANUAL = "manual"

RECOMMENDATION_TEXT = {
    REC_APPROVE: "Можно согласовать возврат — отгрузка найдена, в сроке",
    REC_REJECT: "Рекомендуется отклонить — вне срока возврата",
    REC_REQUEST_DOCUMENTS: (
        "Брак: не хватает документов — запросить у клиента"
    ),
    REC_REQUEST_SUPPLIER: (
        "Транзит поставщика: запросить согласование у поставщика"
    ),
    REC_MANUAL: "Требуется ручная проверка",
}

# Приоритет рекомендаций (чем меньше — тем «важнее»/раньше в цепочке)
_REC_PRIORITY = {
    REC_REQUEST_DOCUMENTS: 0,
    REC_MANUAL: 1,
    REC_REJECT: 2,
    REC_REQUEST_SUPPLIER: 3,
    REC_APPROVE: 4,
}


def _as_date(value: Any) -> Optional[date]:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return None


def _brand_blocked(provider: Provider, brand_name: Optional[str]) -> bool:
    if not brand_name:
        return False
    blocked = provider.return_blocked_brands or []
    target = str(brand_name).strip().upper()
    return any(str(b).strip().upper() == target for b in blocked)


async def _resolve_autopart_id(
    session: AsyncSession,
    *,
    oem_number: Optional[str],
    brand_name: Optional[str],
) -> Optional[int]:
    if not oem_number:
        return None
    normalized = preprocess_oem_number(oem_number)
    if not normalized:
        return None
    stmt = select(AutoPart.id).where(AutoPart.oem_number == normalized)
    rows = (await session.execute(stmt)).scalars().all()
    if not rows:
        return None
    return int(rows[0])


async def _find_latest_shipment(
    session: AsyncSession,
    *,
    customer_id: int,
    autopart_id: int,
) -> Optional[dict[str, Any]]:
    stmt = (
        select(
            ShipmentDocument.id,
            ShipmentDocument.doc_number,
            ShipmentDocument.doc_date,
            ShipmentDocumentItem.id.label("item_id"),
        )
        .join(
            ShipmentDocumentItem,
            ShipmentDocumentItem.document_id == ShipmentDocument.id,
        )
        .where(
            ShipmentDocument.customer_id == customer_id,
            ShipmentDocument.status == ShipmentDocumentStatus.POSTED,
            ShipmentDocumentItem.autopart_id == autopart_id,
        )
        .order_by(ShipmentDocument.doc_date.desc())
        .limit(1)
    )
    row = (await session.execute(stmt)).first()
    if row is None:
        return None
    return {
        "document_id": int(row[0]),
        "doc_number": row[1],
        "doc_date": row[2],
        "shipment_item_id": int(row[3]),
    }


async def _find_latest_customer_order(
    session: AsyncSession,
    *,
    customer_id: int,
    oem_number: Optional[str],
    autopart_id: Optional[int],
) -> Optional[dict[str, Any]]:
    normalized = preprocess_oem_number(oem_number or "")
    normalized_order_oem = func.upper(
        func.regexp_replace(
            CustomerOrderItem.oem,
            "[^A-Za-z0-9]",
            "",
            "g",
        )
    )
    matches = []
    if autopart_id is not None:
        matches.append(CustomerOrderItem.autopart_id == autopart_id)
    if normalized:
        matches.append(normalized_order_oem == normalized)
    if not matches:
        return None

    row = (
        await session.execute(
            select(
                CustomerOrder.id,
                CustomerOrder.order_number,
                CustomerOrder.order_date,
                CustomerOrder.received_at,
                CustomerOrderItem.requested_qty,
                CustomerOrderItem.ship_qty,
                CustomerOrderItem.autopart_id,
            )
            .join(
                CustomerOrderItem,
                CustomerOrderItem.order_id == CustomerOrder.id,
            )
            .where(
                CustomerOrder.customer_id == customer_id,
                or_(*matches),
            )
            .order_by(
                CustomerOrder.order_date.desc().nullslast(),
                CustomerOrder.received_at.desc(),
                CustomerOrder.id.desc(),
            )
            .limit(1)
        )
    ).first()
    if row is None:
        return None
    return {
        "order_id": int(row.id),
        "order_number": row.order_number,
        "order_date": row.order_date,
        "received_at": row.received_at,
        "requested_qty": int(row.requested_qty or 0),
        "ship_qty": int(row.ship_qty or 0),
        "autopart_id": int(row.autopart_id) if row.autopart_id else None,
    }


async def _supplier_for_shipment_item(
    session: AsyncSession, *, shipment_item_id: int
) -> Optional[int]:
    provider_id = (
        await session.execute(
            select(ShipmentDocumentItemLotAllocation.provider_id)
            .where(
                ShipmentDocumentItemLotAllocation.shipment_document_item_id
                == shipment_item_id,
                ShipmentDocumentItemLotAllocation.provider_id.isnot(None),
            )
            .limit(1)
        )
    ).scalar_one_or_none()
    return int(provider_id) if provider_id is not None else None


async def _check_item(
    session: AsyncSession,
    *,
    item,
    customer: Optional[Customer],
    window_days: int,
    today: date,
) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []
    autopart_id = item.autopart_id or await _resolve_autopart_id(
        session,
        oem_number=item.oem_number,
        brand_name=item.brand_name,
    )
    item_source = str(getattr(item.item_source, "value", item.item_source))

    result: dict[str, Any] = {
        "item_id": int(item.id),
        "oem_number": item.oem_number,
        "brand_name": item.brand_name,
        "quantity": int(item.quantity or 0),
        "item_source": item_source,
        "autopart_id": autopart_id,
        "customer_order_found": False,
        "customer_order_id": None,
        "customer_order_number": None,
        "customer_order_date": None,
        "customer_order_requested_qty": None,
        "customer_order_ship_qty": None,
        "shipment_found": False,
        "shipment_date": None,
        "shipment_doc_number": None,
        "days_since_shipment": None,
        "return_window_days": window_days,
        "within_window": None,
        "supplier_id": None,
        "supplier_name": None,
        "supplier_return_allowed": None,
        "supplier_brand_blocked": False,
        "checks": checks,
        "verdict": REC_MANUAL,
    }

    shipment = None
    customer_order = None
    if customer is not None:
        customer_order = await _find_latest_customer_order(
            session,
            customer_id=int(customer.id),
            oem_number=item.oem_number,
            autopart_id=autopart_id,
        )
    if customer_order is not None:
        order_date = _as_date(customer_order["order_date"]) or _as_date(
            customer_order["received_at"]
        )
        result.update(
            {
                "customer_order_found": True,
                "customer_order_id": customer_order["order_id"],
                "customer_order_number": customer_order["order_number"],
                "customer_order_date": (
                    order_date.isoformat() if order_date else None
                ),
                "customer_order_requested_qty": customer_order[
                    "requested_qty"
                ],
                "customer_order_ship_qty": customer_order["ship_qty"],
            }
        )

    if customer is not None and autopart_id is not None:
        shipment = await _find_latest_shipment(
            session,
            customer_id=int(customer.id),
            autopart_id=autopart_id,
        )

    if shipment is None:
        if customer_order is not None:
            order_date = result["customer_order_date"] or "—"
            order_label = customer_order["order_number"] or (
                f"#{customer_order['order_id']}"
            )
            checks.append({
                "key": "customer_order",
                "label": "Заказ клиента",
                "status": "ok",
                "detail": (
                    f"Найден заказ {order_label} "
                    f"от {order_date}: заказано {customer_order['requested_qty']} шт., "
                    f"подтверждено к отгрузке {customer_order['ship_qty']} шт."
                ),
            })
            shipment_detail = (
                "Заказ найден, но проведённой отгрузки в системе нет — "
                "срок возврата нужно проверить вручную"
            )
        else:
            shipment_detail = (
                "Ни заказ клиента, ни проведённая отгрузка этой позиции "
                "не найдены — проверьте артикул вручную"
            )
        checks.append({
            "key": "shipment",
            "label": "Отгрузка клиенту",
            "status": "warn" if customer_order is not None else "fail",
            "detail": shipment_detail,
        })
        result["verdict"] = REC_MANUAL
        return result

    ship_date = _as_date(shipment["doc_date"])
    result["shipment_found"] = True
    result["shipment_date"] = (
        ship_date.isoformat() if ship_date else None
    )
    result["shipment_doc_number"] = shipment["doc_number"]
    checks.append({
        "key": "shipment",
        "label": "Отгрузка клиенту",
        "status": "ok",
        "detail": (
            f"Отгружено {ship_date.strftime('%d.%m.%Y') if ship_date else '—'}"
            f" (документ {shipment['doc_number'] or '—'})"
        ),
    })

    within_window = None
    if ship_date is not None:
        days_since = (today - ship_date).days
        result["days_since_shipment"] = days_since
        within_window = days_since <= window_days
        result["within_window"] = within_window
        checks.append({
            "key": "window",
            "label": "Срок возврата",
            "status": "ok" if within_window else "fail",
            "detail": (
                f"Прошло {days_since} дн. из {window_days} — "
                + ("в сроке" if within_window else "срок истёк")
            ),
        })

    # Источник позиции
    if item_source == "supplier_transit":
        supplier_id = item.source_provider_id or (
            await _supplier_for_shipment_item(
                session, shipment_item_id=shipment["shipment_item_id"]
            )
        )
        provider = (
            await session.get(Provider, supplier_id)
            if supplier_id
            else None
        )
        result["supplier_id"] = supplier_id
        result["supplier_name"] = getattr(provider, "name", None)
        if provider is None:
            checks.append({
                "key": "supplier",
                "label": "Поставщик (транзит)",
                "status": "warn",
                "detail": "Поставщик не определён — уточните вручную",
            })
            result["verdict"] = REC_MANUAL
            return result

        allowed = bool(provider.return_allowed)
        result["supplier_return_allowed"] = allowed
        brand_blocked = _brand_blocked(provider, item.brand_name)
        result["supplier_brand_blocked"] = brand_blocked
        sup_window = provider.return_window_days
        sup_window_ok = True
        if sup_window is not None and result["days_since_shipment"] is not None:
            sup_window_ok = result["days_since_shipment"] <= int(sup_window)

        if not allowed:
            checks.append({
                "key": "supplier",
                "label": "Возврат поставщику",
                "status": "fail",
                "detail": f"Поставщик {provider.name} не принимает возвраты",
            })
            result["verdict"] = REC_REJECT
        elif brand_blocked:
            checks.append({
                "key": "supplier",
                "label": "Возврат поставщику",
                "status": "fail",
                "detail": (
                    f"Бренд {item.brand_name} в стоп-листе поставщика "
                    f"{provider.name}"
                ),
            })
            result["verdict"] = REC_REJECT
        elif not sup_window_ok:
            checks.append({
                "key": "supplier",
                "label": "Возврат поставщику",
                "status": "fail",
                "detail": (
                    f"Вне срока возврата поставщика ({sup_window} дн.)"
                ),
            })
            result["verdict"] = REC_REJECT
        else:
            checks.append({
                "key": "supplier",
                "label": "Возврат поставщику",
                "status": "ok",
                "detail": (
                    f"Поставщик {provider.name} принимает — нужен запрос "
                    "согласования"
                ),
            })
            result["verdict"] = REC_REQUEST_SUPPLIER
        return result

    # Наш склад / источник не указан
    if item_source == "unknown":
        checks.append({
            "key": "source",
            "label": "Источник позиции",
            "status": "warn",
            "detail": "Не указан (наш склад / транзит) — уточните в позиции",
        })

    if within_window is False:
        result["verdict"] = REC_REJECT
    elif within_window is True:
        result["verdict"] = REC_APPROVE
    else:
        result["verdict"] = REC_MANUAL
    return result


async def run_reclamation_check(
    session: AsyncSession, *, reclamation_id: int
) -> Reclamation:
    rec = (
        await session.execute(
            select(Reclamation)
            .where(Reclamation.id == reclamation_id)
            .options(
                selectinload(Reclamation.items),
                selectinload(Reclamation.attachments),
            )
        )
    ).scalar_one_or_none()
    if rec is None:
        raise ValueError("Рекламация не найдена")

    # Повторяем распознавание для уже загруженных писем: правила могли быть
    # улучшены после первичного приёма, а Message-ID не даст создать дубль.
    from dz_fastapi.services.reclamations import recognize_reclamation_items

    await recognize_reclamation_items(session, rec)

    customer = (
        await session.get(Customer, rec.customer_id)
        if rec.customer_id
        else None
    )
    window_days = DEFAULT_RETURN_WINDOW_DAYS
    if customer is not None and customer.return_window_days:
        window_days = int(customer.return_window_days)

    today = now_moscow().date()
    rec_type = str(getattr(rec.reclamation_type, "value", rec.reclamation_type))

    item_results: list[dict[str, Any]] = []
    for item in rec.items or []:
        item_results.append(
            await _check_item(
                session,
                item=item,
                customer=customer,
                window_days=window_days,
                today=today,
            )
        )

    # Документы для брака
    documents: Optional[dict[str, Any]] = None
    docs_missing: list[str] = []
    if rec_type == RECLAMATION_TYPE.DEFECT.value:
        present = {
            str(getattr(a.kind, "value", a.kind)) for a in (rec.attachments or [])
        }
        docs_missing = [d for d in REQUIRED_DEFECT_DOCS if d not in present]
        documents = {
            "required": list(REQUIRED_DEFECT_DOCS),
            "present": sorted(present & set(REQUIRED_DEFECT_DOCS)),
            "missing": docs_missing,
            "missing_labels": [DOC_LABELS.get(d, d) for d in docs_missing],
        }

    # Итоговая рекомендация
    candidate_codes: list[str] = []
    if documents is not None and docs_missing:
        candidate_codes.append(REC_REQUEST_DOCUMENTS)
    if not item_results:
        candidate_codes.append(REC_MANUAL)
    for res in item_results:
        candidate_codes.append(res["verdict"])
    recommendation_code = min(
        candidate_codes,
        key=lambda code: _REC_PRIORITY.get(code, 99),
    )

    check_result = {
        "checked_at": now_moscow().isoformat(),
        "type": rec_type,
        "customer_return_window_days": window_days,
        "documents": documents,
        "items": item_results,
        "recommendation_code": recommendation_code,
        "summary": RECOMMENDATION_TEXT.get(recommendation_code, ""),
    }

    rec.check_result = check_result
    rec.recommendation = recommendation_code
    if rec.status in (RECLAMATION_STATUS.NEW, RECLAMATION_STATUS.RECOGNIZED):
        rec.status = RECLAMATION_STATUS.CHECKED

    session.add(rec)
    await session.commit()
    await session.refresh(rec)
    return rec
