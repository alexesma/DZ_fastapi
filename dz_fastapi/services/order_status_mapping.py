from __future__ import annotations

import re
from typing import Any, Iterable, Optional

from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.models.notification import AppNotificationLevel
from dz_fastapi.models.order_status_mapping import (ExternalStatusMapping,
                                                    ExternalStatusMatchMode,
                                                    ExternalStatusUnmapped)
from dz_fastapi.models.partner import (TYPE_ORDER_ITEM_STATUS,
                                       TYPE_STATUS_ORDER, Order, OrderItem)
from dz_fastapi.services.notifications import create_admin_notifications

EXTERNAL_STATUS_SOURCE_DRAGONZAP = 'DRAGONZAP_SITE'

EXTERNAL_STATUS_SOURCE_LABELS = {
    EXTERNAL_STATUS_SOURCE_DRAGONZAP: 'Dragonzap',
}

EXTERNAL_STATUS_MATCH_MODE_LABELS = {
    ExternalStatusMatchMode.EXACT.value: 'Точное совпадение',
    ExternalStatusMatchMode.CONTAINS.value: 'Содержит',
}


def get_order_status_options() -> list[dict[str, str]]:
    return [
        {'value': status.name, 'label': status.label}
        for status in TYPE_STATUS_ORDER
    ]


def get_order_item_status_options() -> list[dict[str, str]]:
    return [
        {'value': status.name, 'label': status.label}
        for status in TYPE_ORDER_ITEM_STATUS
    ]


def get_external_status_source_options() -> list[dict[str, str]]:
    return [
        {'value': key, 'label': label}
        for key, label in EXTERNAL_STATUS_SOURCE_LABELS.items()
    ]


def get_external_status_match_mode_options() -> list[dict[str, str]]:
    return [
        {'value': value, 'label': label}
        for value, label in EXTERNAL_STATUS_MATCH_MODE_LABELS.items()
    ]


def normalize_external_status_source(value: Optional[str]) -> str:
    return str(value or '').strip().upper()


def normalize_external_status_text(value: Any) -> str:
    raw = str(value or '').strip().casefold()
    if not raw:
        return ''
    return re.sub(r'[^a-zа-я0-9]+', ' ', raw).strip()


def collect_external_status_values(
    payload: dict[str, Any],
) -> list[str]:
    values: list[str] = []
    for key in (
        'status_code',
        'status',
        'status_name',
        'status_title',
        'status_text',
        'state',
        'state_name',
    ):
        value = payload.get(key)
        if value not in (None, ''):
            values.append(str(value).strip())
    sys_info = payload.get('sys_info')
    if isinstance(sys_info, dict):
        for key in (
            'status_code',
            'status',
            'status_name',
            'status_title',
            'status_text',
            'state',
            'state_name',
        ):
            value = sys_info.get(key)
            if value not in (None, ''):
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
    return ' | '.join(values)[:255]


def build_external_status_normalized(payload: dict[str, Any]) -> str:
    values = collect_external_status_values(payload)
    return ' '.join(
        normalize_external_status_text(value)
        for value in values
        if normalize_external_status_text(value)
    ).strip()


def mapping_matches(
    mapping: ExternalStatusMapping, normalized_status: str
) -> bool:
    candidate = (mapping.normalized_status or '').strip()
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
            0
            if mapping.match_mode == ExternalStatusMatchMode.EXACT
            else 1
        )
        priority = int(mapping.priority or 100)
        specificity = -len(mapping.normalized_status or '')
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
        provider_note = f' поставщик #{provider_id}' if provider_id else ''
        await create_admin_notifications(
            session=session,
            title='Новый внешний статус без сопоставления',
            message=(
                f'Источник {normalized_source}{provider_note}: '
                f'"{raw_status}"'
            ),
            level=AppNotificationLevel.WARNING,
            link='/admin/order-status-mappings',
            commit=False,
        )
    return row


def resolve_internal_order_status(
    value: Optional[str],
) -> Optional[TYPE_STATUS_ORDER]:
    key = str(value or '').strip().upper()
    if not key:
        return None
    return TYPE_STATUS_ORDER[key]


def resolve_internal_item_status(
    value: Optional[str],
) -> Optional[TYPE_ORDER_ITEM_STATUS]:
    key = str(value or '').strip().upper()
    if not key:
        return None
    return TYPE_ORDER_ITEM_STATUS[key]


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


async def apply_mapping_to_existing_items(
    session: AsyncSession,
    *,
    mapping: ExternalStatusMapping,
) -> dict[str, int]:
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
        normalized_status = (
            str(item.external_status_normalized or '').strip()
        )
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

    unresolved_rows = (
        await session.execute(unresolved_stmt)
    ).scalars().all()
    resolved = 0
    for row in unresolved_rows:
        if mapping_matches(mapping, str(row.normalized_status or '').strip()):
            row.is_resolved = True
            row.mapping_id = mapping.id
            resolved += 1

    await session.commit()
    return {
        'checked_items': checked,
        'updated_items': updated,
        'resolved_unmapped': resolved,
    }
