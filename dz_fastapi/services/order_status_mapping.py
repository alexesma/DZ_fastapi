from __future__ import annotations

import re
from typing import Any, Iterable, Optional

from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from dz_fastapi.core.time import now_moscow
from dz_fastapi.models.notification import AppNotificationLevel
from dz_fastapi.models.order_status_mapping import (ExternalStatusMapping,
                                                    ExternalStatusMatchMode,
                                                    ExternalStatusUnmapped,
                                                    SupplierResponseAction)
from dz_fastapi.models.partner import (TYPE_ORDER_ITEM_STATUS,
                                       TYPE_STATUS_ORDER, Order, OrderItem,
                                       SupplierOrder, SupplierOrderItem)
from dz_fastapi.services.notifications import create_admin_notifications

EXTERNAL_STATUS_SOURCE_DRAGONZAP = "DRAGONZAP_SITE"
EXTERNAL_STATUS_SOURCE_SUPPLIER_EMAIL = "SUPPLIER_EMAIL"

EXTERNAL_STATUS_SOURCE_LABELS = {
    EXTERNAL_STATUS_SOURCE_DRAGONZAP: "Dragonzap",
    EXTERNAL_STATUS_SOURCE_SUPPLIER_EMAIL: "Ответы поставщиков по email",
}

EXTERNAL_STATUS_MATCH_MODE_LABELS = {
    ExternalStatusMatchMode.EXACT.value: "Точное совпадение",
    ExternalStatusMatchMode.CONTAINS.value: "Содержит",
}

SUPPLIER_RESPONSE_ACTION_LABELS = {
    SupplierResponseAction.NO_CHANGE.value: "Только сохранить статус",
    SupplierResponseAction.FULL_CONFIRM.value: "Подтвердить всё",
    SupplierResponseAction.PARTIAL_CONFIRM.value: "Частичное подтверждение",
    SupplierResponseAction.REJECT_ALL.value: "Отказать по всем строкам",
    SupplierResponseAction.WAITING.value: "Ожидаем ответ / отложить",
}


def get_order_status_options() -> list[dict[str, str]]:
    return [
        {"value": status.name, "label": status.label}
        for status in TYPE_STATUS_ORDER
    ]


def get_order_item_status_options() -> list[dict[str, str]]:
    return [
        {"value": status.name, "label": status.label}
        for status in TYPE_ORDER_ITEM_STATUS
    ]


def get_external_status_source_options() -> list[dict[str, str]]:
    return [
        {"value": key, "label": label}
        for key, label in EXTERNAL_STATUS_SOURCE_LABELS.items()
    ]


def get_external_status_match_mode_options() -> list[dict[str, str]]:
    return [
        {"value": value, "label": label}
        for value, label in EXTERNAL_STATUS_MATCH_MODE_LABELS.items()
    ]


def get_supplier_response_action_options() -> list[dict[str, str]]:
    return [
        {"value": value, "label": label}
        for value, label in SUPPLIER_RESPONSE_ACTION_LABELS.items()
    ]


def normalize_external_status_source(value: Optional[str]) -> str:
    return str(value or "").strip().upper()


def normalize_external_status_text(value: Any) -> str:
    raw = str(value or "").strip().casefold()
    if not raw:
        return ""
    return re.sub(r"[^a-zа-я0-9]+", " ", raw).strip()


def collect_external_status_values(
    payload: dict[str, Any],
) -> list[str]:
    values: list[str] = []
    for key in (
        "status_code",
        "status",
        "status_name",
        "status_title",
        "status_text",
        "state",
        "state_name",
    ):
        value = payload.get(key)
        if value not in (None, ""):
            values.append(str(value).strip())
    sys_info = payload.get("sys_info")
    if isinstance(sys_info, dict):
        for key in (
            "status_code",
            "status",
            "status_name",
            "status_title",
            "status_text",
            "state",
            "state_name",
        ):
            value = sys_info.get(key)
            if value not in (None, ""):
                values.append(str(value).strip())

    unique: list[str] = []
    seen: set[str] = set()
    for value in values:
        if not value:
            continue
        marker = value.casefold()
        if marker in seen:
            continue
        seen.add(marker)
        unique.append(value)
    return unique


def build_external_status_raw(payload: dict[str, Any]) -> Optional[str]:
    values = collect_external_status_values(payload)
    if not values:
        return None
    return " | ".join(values)[:255]


def build_external_status_normalized(payload: dict[str, Any]) -> str:
    values = collect_external_status_values(payload)
    return " ".join(
        normalize_external_status_text(value)
        for value in values
        if normalize_external_status_text(value)
    ).strip()


def mapping_matches(
    mapping: ExternalStatusMapping, normalized_status: str
) -> bool:
    candidate = (mapping.normalized_status or "").strip()
    if not candidate or not normalized_status:
        return False
    if mapping.match_mode == ExternalStatusMatchMode.EXACT:
        return normalized_status == candidate
    return candidate in normalized_status


def select_best_mapping(
    mappings: Iterable[ExternalStatusMapping],
    *,
    normalized_status: str,
    provider_id: Optional[int],
) -> Optional[ExternalStatusMapping]:
    matches = [
        mapping
        for mapping in mappings
        if mapping_matches(mapping, normalized_status)
    ]
    if not matches:
        return None

    def sort_key(mapping: ExternalStatusMapping) -> tuple[int, int, int, int]:
        provider_rank = (
            0
            if provider_id is not None and mapping.provider_id == provider_id
            else 1
        )
        mode_rank = (
            0 if mapping.match_mode == ExternalStatusMatchMode.EXACT else 1
        )
        priority = int(mapping.priority or 100)
        specificity = -len(mapping.normalized_status or "")
        return provider_rank, mode_rank, priority, specificity

    return sorted(matches, key=sort_key)[0]


async def get_active_status_mappings(
    session: AsyncSession,
    *,
    source_key: str,
    provider_id: Optional[int] = None,
) -> list[ExternalStatusMapping]:
    normalized_source = normalize_external_status_source(source_key)
    stmt = select(ExternalStatusMapping).where(
        ExternalStatusMapping.source_key == normalized_source,
        ExternalStatusMapping.is_active.is_(True),
    )
    if provider_id is not None:
        stmt = stmt.where(
            or_(
                ExternalStatusMapping.provider_id == provider_id,
                ExternalStatusMapping.provider_id.is_(None),
            )
        )
    else:
        stmt = stmt.where(ExternalStatusMapping.provider_id.is_(None))
    stmt = stmt.order_by(
        ExternalStatusMapping.priority.asc(),
        ExternalStatusMapping.id.asc(),
    )
    return list((await session.execute(stmt)).scalars().all())


async def record_unmapped_external_status(
    session: AsyncSession,
    *,
    source_key: str,
    provider_id: Optional[int],
    raw_status: str,
    normalized_status: str,
    sample_order_id: Optional[int] = None,
    sample_item_id: Optional[int] = None,
    sample_payload: Optional[dict[str, Any]] = None,
) -> ExternalStatusUnmapped:
    normalized_source = normalize_external_status_source(source_key)
    stmt = select(ExternalStatusUnmapped).where(
        ExternalStatusUnmapped.source_key == normalized_source,
        ExternalStatusUnmapped.normalized_status == normalized_status,
    )
    if provider_id is None:
        stmt = stmt.where(ExternalStatusUnmapped.provider_id.is_(None))
    else:
        stmt = stmt.where(ExternalStatusUnmapped.provider_id == provider_id)

    row = (await session.execute(stmt)).scalar_one_or_none()
    created = row is None
    if row is None:
        row = ExternalStatusUnmapped(
            source_key=normalized_source,
            provider_id=provider_id,
            raw_status=raw_status,
            normalized_status=normalized_status,
            sample_order_id=sample_order_id,
            sample_item_id=sample_item_id,
            sample_payload=sample_payload,
            is_resolved=False,
        )
        session.add(row)
        await session.flush()
    else:
        row.raw_status = raw_status
        row.last_seen_at = row.last_seen_at or row.first_seen_at
        row.seen_count = int(row.seen_count or 0) + 1
        row.sample_order_id = sample_order_id or row.sample_order_id
        row.sample_item_id = sample_item_id or row.sample_item_id
        row.sample_payload = sample_payload or row.sample_payload
        row.is_resolved = False
        row.mapping_id = None

    if created:
        provider_note = f" поставщик #{provider_id}" if provider_id else ""
        await create_admin_notifications(
            session=session,
            title="Новый внешний статус без сопоставления",
            message=(
                f"Источник {normalized_source}{provider_note}: "
                f'"{raw_status}"'
            ),
            level=AppNotificationLevel.WARNING,
            link="/admin/order-status-mappings",
            commit=False,
        )
    return row


def resolve_internal_order_status(
    value: Optional[str],
) -> Optional[TYPE_STATUS_ORDER]:
    key = str(value or "").strip().upper()
    if not key:
        return None
    return TYPE_STATUS_ORDER[key]


def resolve_internal_item_status(
    value: Optional[str],
) -> Optional[TYPE_ORDER_ITEM_STATUS]:
    key = str(value or "").strip().upper()
    if not key:
        return None
    return TYPE_ORDER_ITEM_STATUS[key]


def resolve_supplier_response_action(
    value: Optional[str],
) -> Optional[SupplierResponseAction]:
    key = str(value or "").strip().upper()
    if not key:
        return None
    return SupplierResponseAction[key]


def apply_status_mapping_to_order_item(
    *,
    order: Order,
    item: OrderItem,
    mapping: ExternalStatusMapping,
) -> bool:
    changed = False

    mapped_order_status = resolve_internal_order_status(
        mapping.internal_order_status
    )
    if mapped_order_status and order.status != mapped_order_status:
        order.status = mapped_order_status
        changed = True

    mapped_item_status = resolve_internal_item_status(
        mapping.internal_item_status
    )
    if mapped_item_status and item.status != mapped_item_status:
        item.status = mapped_item_status
        changed = True

    if item.external_status_mapping_id != mapping.id:
        item.external_status_mapping_id = mapping.id
        changed = True

    return changed


def apply_supplier_response_action_to_order(
    *,
    order: SupplierOrder,
    mapping: ExternalStatusMapping,
    raw_status: Optional[str],
    normalized_status: Optional[str],
    allow_quantity_updates: bool = True,
) -> dict[str, int]:
    changed_orders = 0
    updated_items = 0
    action = resolve_supplier_response_action(mapping.supplier_response_action)
    synced_at = now_moscow()
    order_changed = False

    if order.response_status_raw != raw_status:
        order.response_status_raw = raw_status
        order_changed = True
    if order.response_status_normalized != (normalized_status or None):
        order.response_status_normalized = normalized_status or None
        order_changed = True
    if order.response_status_synced_at != synced_at:
        order.response_status_synced_at = synced_at
        order_changed = True

    if order_changed:
        changed_orders += 1

    for item in order.items or []:
        item_changed = False
        if item.response_status_raw != raw_status:
            item.response_status_raw = raw_status
            item_changed = True
        if item.response_status_normalized != (normalized_status or None):
            item.response_status_normalized = normalized_status or None
            item_changed = True
        if item.response_status_synced_at != synced_at:
            item.response_status_synced_at = synced_at
            item_changed = True

        if allow_quantity_updates and action is not None:
            target_qty: Optional[int] = None
            if action == SupplierResponseAction.FULL_CONFIRM:
                target_qty = int(item.quantity or 0)
            elif action == SupplierResponseAction.REJECT_ALL:
                target_qty = 0
            if (
                target_qty is not None
                and item.confirmed_quantity != target_qty
            ):
                item.confirmed_quantity = target_qty
                item_changed = True

        if item_changed:
            updated_items += 1

    return {
        "changed_orders": changed_orders,
        "updated_items": updated_items,
    }


async def apply_mapping_to_existing_items(
    session: AsyncSession,
    *,
    mapping: ExternalStatusMapping,
) -> dict[str, int]:
    if mapping.source_key == EXTERNAL_STATUS_SOURCE_SUPPLIER_EMAIL:
        return await _apply_mapping_to_existing_supplier_orders(
            session, mapping=mapping
        )

    stmt = (
        select(OrderItem, Order)
        .join(Order, Order.id == OrderItem.order_id)
        .where(
            OrderItem.external_status_source == mapping.source_key,
            OrderItem.external_status_normalized.is_not(None),
        )
    )
    if mapping.provider_id is not None:
        stmt = stmt.where(Order.provider_id == mapping.provider_id)

    rows = (await session.execute(stmt)).all()
    checked = 0
    updated = 0
    for item, order in rows:
        normalized_status = str(item.external_status_normalized or "").strip()
        if not mapping_matches(mapping, normalized_status):
            continue
        checked += 1
        if apply_status_mapping_to_order_item(
            order=order,
            item=item,
            mapping=mapping,
        ):
            updated += 1

    unresolved_stmt = select(ExternalStatusUnmapped).where(
        ExternalStatusUnmapped.source_key == mapping.source_key,
        ExternalStatusUnmapped.is_resolved.is_(False),
    )
    if mapping.provider_id is not None:
        unresolved_stmt = unresolved_stmt.where(
            ExternalStatusUnmapped.provider_id == mapping.provider_id
        )

    unresolved_rows = (await session.execute(unresolved_stmt)).scalars().all()
    resolved = 0
    for row in unresolved_rows:
        if mapping_matches(mapping, str(row.normalized_status or "").strip()):
            row.is_resolved = True
            row.mapping_id = mapping.id
            resolved += 1

    await session.commit()
    return {
        "checked_items": checked,
        "updated_items": updated,
        "resolved_unmapped": resolved,
    }


async def _apply_mapping_to_existing_supplier_orders(
    session: AsyncSession,
    *,
    mapping: ExternalStatusMapping,
) -> dict[str, int]:
    stmt = (
        select(SupplierOrder)
        .options(selectinload(SupplierOrder.items))
        .where(
            or_(
                SupplierOrder.response_status_normalized.is_not(None),
                SupplierOrder.items.any(
                    SupplierOrderItem.response_status_normalized.is_not(None)
                ),
            )
        )
    )
    if mapping.provider_id is not None:
        stmt = stmt.where(SupplierOrder.provider_id == mapping.provider_id)

    orders = (await session.execute(stmt)).scalars().all()
    checked = 0
    updated = 0
    for order in orders:
        matched = False
        raw_status = order.response_status_raw
        normalized_status = str(order.response_status_normalized or "").strip()
        if mapping_matches(mapping, normalized_status):
            matched = True
        else:
            for item in order.items or []:
                normalized_item_status = str(
                    item.response_status_normalized or ""
                ).strip()
                if mapping_matches(mapping, normalized_item_status):
                    raw_status = item.response_status_raw or raw_status
                    normalized_status = (
                        normalized_item_status or normalized_status
                    )
                    matched = True
                    break
        if not matched:
            continue
        checked += 1
        update_result = apply_supplier_response_action_to_order(
            order=order,
            mapping=mapping,
            raw_status=raw_status,
            normalized_status=normalized_status,
        )
        updated += update_result["updated_items"]

    unresolved_stmt = select(ExternalStatusUnmapped).where(
        ExternalStatusUnmapped.source_key == mapping.source_key,
        ExternalStatusUnmapped.is_resolved.is_(False),
    )
    if mapping.provider_id is not None:
        unresolved_stmt = unresolved_stmt.where(
            ExternalStatusUnmapped.provider_id == mapping.provider_id
        )

    unresolved_rows = (await session.execute(unresolved_stmt)).scalars().all()
    resolved = 0
    for row in unresolved_rows:
        if mapping_matches(mapping, str(row.normalized_status or "").strip()):
            row.is_resolved = True
            row.mapping_id = mapping.id
            resolved += 1

    await session.commit()
    return {
        "checked_items": checked,
        "updated_items": updated,
        "resolved_unmapped": resolved,
    }
