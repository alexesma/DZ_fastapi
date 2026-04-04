from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import Select, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased

from dz_fastapi.api.deps import require_admin
from dz_fastapi.core.db import get_session
from dz_fastapi.models.order_status_mapping import (ExternalStatusMapping,
                                                    ExternalStatusMatchMode,
                                                    ExternalStatusUnmapped)
from dz_fastapi.models.partner import Provider
from dz_fastapi.models.user import User
from dz_fastapi.schemas.order_status_mapping import (
    ExternalStatusMappingApplyResult, ExternalStatusMappingCreate,
    ExternalStatusMappingOptionsOut, ExternalStatusMappingOut,
    ExternalStatusMappingUpdate, ExternalStatusUnmappedOut, StatusOptionOut)
from dz_fastapi.services.order_status_mapping import (
    EXTERNAL_STATUS_SOURCE_LABELS, apply_mapping_to_existing_items,
    get_external_status_match_mode_options, get_external_status_source_options,
    get_order_item_status_options, get_order_status_options,
    get_supplier_response_action_options, normalize_external_status_source,
    normalize_external_status_text, resolve_internal_item_status,
    resolve_internal_order_status)

router = APIRouter(
    prefix="/admin/order-status-mappings",
    tags=["admin", "order-status-mappings"],
    dependencies=[Depends(require_admin)],
)


def _validate_internal_statuses(
    *,
    order_status: Optional[str],
    item_status: Optional[str],
    supplier_response_action: Optional[str],
) -> None:
    if (
        not str(order_status or "").strip()
        and not str(item_status or "").strip()
        and not str(supplier_response_action or "").strip()
    ):
        raise HTTPException(
            status_code=400,
            detail=(
                "Нужно выбрать внутренний статус "
                "или действие для ответа поставщика"
            ),
        )
    try:
        resolve_internal_order_status(order_status)
        resolve_internal_item_status(item_status)
    except KeyError as exc:
        raise HTTPException(
            status_code=400,
            detail="Указан неизвестный внутренний статус",
        ) from exc


def _provider_filter(column, provider_id: Optional[int]):
    if provider_id is None:
        return column.is_(None)
    return column == provider_id


async def _find_duplicate_mapping(
    session: AsyncSession,
    *,
    source_key: str,
    provider_id: Optional[int],
    normalized_status: str,
    match_mode: ExternalStatusMatchMode,
    exclude_id: Optional[int] = None,
) -> ExternalStatusMapping | None:
    stmt = select(ExternalStatusMapping).where(
        ExternalStatusMapping.source_key == source_key,
        _provider_filter(ExternalStatusMapping.provider_id, provider_id),
        ExternalStatusMapping.normalized_status == normalized_status,
        ExternalStatusMapping.match_mode == match_mode,
    )
    if exclude_id is not None:
        stmt = stmt.where(ExternalStatusMapping.id != exclude_id)
    return (await session.execute(stmt)).scalar_one_or_none()


def _mapping_to_schema(
    mapping: ExternalStatusMapping,
    *,
    provider_name: Optional[str] = None,
    created_by_email: Optional[str] = None,
    updated_by_email: Optional[str] = None,
) -> ExternalStatusMappingOut:
    return ExternalStatusMappingOut(
        id=mapping.id,
        source_key=mapping.source_key,
        provider_id=mapping.provider_id,
        provider_name=provider_name,
        raw_status=mapping.raw_status,
        normalized_status=mapping.normalized_status,
        match_mode=str(mapping.match_mode.value),
        internal_order_status=mapping.internal_order_status,
        internal_item_status=mapping.internal_item_status,
        supplier_response_action=mapping.supplier_response_action,
        priority=mapping.priority,
        is_active=mapping.is_active,
        notes=mapping.notes,
        created_by_user_id=mapping.created_by_user_id,
        created_by_email=created_by_email,
        updated_by_user_id=mapping.updated_by_user_id,
        updated_by_email=updated_by_email,
        created_at=mapping.created_at,
        updated_at=mapping.updated_at,
    )


@router.get("/options", response_model=ExternalStatusMappingOptionsOut)
async def get_order_status_mapping_options():
    return ExternalStatusMappingOptionsOut(
        sources=[
            StatusOptionOut(**item)
            for item in get_external_status_source_options()
        ],
        match_modes=[
            StatusOptionOut(**item)
            for item in get_external_status_match_mode_options()
        ],
        order_statuses=[
            StatusOptionOut(**item) for item in get_order_status_options()
        ],
        item_statuses=[
            StatusOptionOut(**item) for item in get_order_item_status_options()
        ],
        supplier_response_actions=[
            StatusOptionOut(**item)
            for item in get_supplier_response_action_options()
        ],
    )


@router.get("", response_model=list[ExternalStatusMappingOut])
async def list_order_status_mappings(
    source_key: Optional[str] = None,
    provider_id: Optional[int] = None,
    is_active: Optional[bool] = None,
    session: AsyncSession = Depends(get_session),
):
    provider_alias = aliased(Provider)
    created_by_alias = aliased(User)
    updated_by_alias = aliased(User)
    stmt: Select = (
        select(
            ExternalStatusMapping,
            provider_alias.name,
            created_by_alias.email,
            updated_by_alias.email,
        )
        .outerjoin(
            provider_alias,
            provider_alias.id == ExternalStatusMapping.provider_id,
        )
        .outerjoin(
            created_by_alias,
            created_by_alias.id == ExternalStatusMapping.created_by_user_id,
        )
        .outerjoin(
            updated_by_alias,
            updated_by_alias.id == ExternalStatusMapping.updated_by_user_id,
        )
        .order_by(
            ExternalStatusMapping.source_key.asc(),
            ExternalStatusMapping.provider_id.asc().nullsfirst(),
            ExternalStatusMapping.priority.asc(),
            ExternalStatusMapping.raw_status.asc(),
        )
    )
    if source_key:
        stmt = stmt.where(
            ExternalStatusMapping.source_key
            == normalize_external_status_source(source_key)
        )
    if provider_id is not None:
        stmt = stmt.where(ExternalStatusMapping.provider_id == provider_id)
    if is_active is not None:
        stmt = stmt.where(ExternalStatusMapping.is_active.is_(is_active))

    rows = (await session.execute(stmt)).all()
    return [
        _mapping_to_schema(
            mapping,
            provider_name=provider_name,
            created_by_email=created_by_email,
            updated_by_email=updated_by_email,
        )
        for mapping, provider_name, created_by_email, updated_by_email in rows
    ]


@router.get("/unmapped", response_model=list[ExternalStatusUnmappedOut])
async def list_unmapped_external_statuses(
    source_key: Optional[str] = None,
    provider_id: Optional[int] = None,
    resolved: bool = Query(default=False),
    session: AsyncSession = Depends(get_session),
):
    provider_alias = aliased(Provider)
    stmt = (
        select(ExternalStatusUnmapped, provider_alias.name)
        .outerjoin(
            provider_alias,
            provider_alias.id == ExternalStatusUnmapped.provider_id,
        )
        .where(ExternalStatusUnmapped.is_resolved.is_(resolved))
        .order_by(
            ExternalStatusUnmapped.last_seen_at.desc(),
            ExternalStatusUnmapped.id.desc(),
        )
    )
    if source_key:
        stmt = stmt.where(
            ExternalStatusUnmapped.source_key
            == normalize_external_status_source(source_key)
        )
    if provider_id is not None:
        stmt = stmt.where(ExternalStatusUnmapped.provider_id == provider_id)

    rows = (await session.execute(stmt)).all()
    return [
        ExternalStatusUnmappedOut(
            id=row.id,
            source_key=row.source_key,
            source_label=EXTERNAL_STATUS_SOURCE_LABELS.get(
                row.source_key, row.source_key
            ),
            provider_id=row.provider_id,
            provider_name=provider_name,
            raw_status=row.raw_status,
            normalized_status=row.normalized_status,
            seen_count=row.seen_count,
            first_seen_at=row.first_seen_at,
            last_seen_at=row.last_seen_at,
            sample_order_id=row.sample_order_id,
            sample_item_id=row.sample_item_id,
            sample_payload=row.sample_payload,
            is_resolved=row.is_resolved,
            mapping_id=row.mapping_id,
        )
        for row, provider_name in rows
    ]


@router.post("", response_model=ExternalStatusMappingOut)
async def create_order_status_mapping(
    payload: ExternalStatusMappingCreate,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(require_admin),
):
    _validate_internal_statuses(
        order_status=payload.internal_order_status,
        item_status=payload.internal_item_status,
        supplier_response_action=(
            payload.supplier_response_action.value
            if payload.supplier_response_action
            else None
        ),
    )

    source_key = normalize_external_status_source(payload.source_key)
    normalized_status = normalize_external_status_text(payload.raw_status)
    if not normalized_status:
        raise HTTPException(
            status_code=400,
            detail="Внешний статус не может быть пустым",
        )

    duplicate = await _find_duplicate_mapping(
        session,
        source_key=source_key,
        provider_id=payload.provider_id,
        normalized_status=normalized_status,
        match_mode=payload.match_mode,
    )
    if duplicate is not None:
        raise HTTPException(
            status_code=400,
            detail="Такое правило уже существует",
        )

    mapping = ExternalStatusMapping(
        source_key=source_key,
        provider_id=payload.provider_id,
        raw_status=payload.raw_status.strip(),
        normalized_status=normalized_status,
        match_mode=payload.match_mode,
        internal_order_status=(
            str(payload.internal_order_status or "").strip().upper() or None
        ),
        internal_item_status=(
            str(payload.internal_item_status or "").strip().upper() or None
        ),
        supplier_response_action=(
            payload.supplier_response_action.value
            if payload.supplier_response_action
            else None
        ),
        priority=payload.priority,
        is_active=payload.is_active,
        notes=payload.notes,
        created_by_user_id=current_user.id,
        updated_by_user_id=current_user.id,
    )
    session.add(mapping)
    await session.commit()
    await session.refresh(mapping)

    if payload.apply_existing and mapping.is_active:
        await apply_mapping_to_existing_items(session, mapping=mapping)
        await session.refresh(mapping)

    provider_name = None
    if mapping.provider_id is not None:
        provider = await session.get(Provider, mapping.provider_id)
        provider_name = provider.name if provider else None
    return _mapping_to_schema(
        mapping,
        provider_name=provider_name,
        created_by_email=current_user.email,
        updated_by_email=current_user.email,
    )


@router.patch(
    "/{mapping_id}",
    response_model=ExternalStatusMappingOut,
)
async def update_order_status_mapping(
    mapping_id: int,
    payload: ExternalStatusMappingUpdate,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(require_admin),
):
    mapping = await session.get(ExternalStatusMapping, mapping_id)
    if mapping is None:
        raise HTTPException(status_code=404, detail="Правило не найдено")

    new_provider_id = mapping.provider_id
    if "provider_id" in payload.model_fields_set:
        new_provider_id = payload.provider_id

    new_match_mode = mapping.match_mode
    if payload.match_mode is not None:
        new_match_mode = payload.match_mode

    new_raw_status = mapping.raw_status
    if payload.raw_status is not None:
        new_raw_status = payload.raw_status.strip()

    new_internal_order_status = mapping.internal_order_status
    if "internal_order_status" in payload.model_fields_set:
        new_internal_order_status = (
            str(payload.internal_order_status or "").strip().upper() or None
        )

    new_internal_item_status = mapping.internal_item_status
    if "internal_item_status" in payload.model_fields_set:
        new_internal_item_status = (
            str(payload.internal_item_status or "").strip().upper() or None
        )

    new_supplier_response_action = mapping.supplier_response_action
    if "supplier_response_action" in payload.model_fields_set:
        new_supplier_response_action = (
            payload.supplier_response_action.value
            if payload.supplier_response_action
            else None
        )

    _validate_internal_statuses(
        order_status=new_internal_order_status,
        item_status=new_internal_item_status,
        supplier_response_action=new_supplier_response_action,
    )

    normalized_status = normalize_external_status_text(new_raw_status)
    if not normalized_status:
        raise HTTPException(
            status_code=400,
            detail="Внешний статус не может быть пустым",
        )

    duplicate = await _find_duplicate_mapping(
        session,
        source_key=mapping.source_key,
        provider_id=new_provider_id,
        normalized_status=normalized_status,
        match_mode=new_match_mode,
        exclude_id=mapping.id,
    )
    if duplicate is not None:
        raise HTTPException(
            status_code=400,
            detail="Такое правило уже существует",
        )

    mapping.provider_id = new_provider_id
    mapping.match_mode = new_match_mode
    mapping.raw_status = new_raw_status
    mapping.normalized_status = normalized_status
    mapping.internal_order_status = new_internal_order_status
    mapping.internal_item_status = new_internal_item_status
    mapping.supplier_response_action = new_supplier_response_action
    if payload.priority is not None:
        mapping.priority = payload.priority
    if payload.is_active is not None:
        mapping.is_active = payload.is_active
    if "notes" in payload.model_fields_set:
        mapping.notes = payload.notes
    mapping.updated_by_user_id = current_user.id

    await session.commit()
    await session.refresh(mapping)

    if payload.apply_existing and mapping.is_active:
        await apply_mapping_to_existing_items(session, mapping=mapping)
        await session.refresh(mapping)

    provider_name = None
    if mapping.provider_id is not None:
        provider = await session.get(Provider, mapping.provider_id)
        provider_name = provider.name if provider else None
    return _mapping_to_schema(
        mapping,
        provider_name=provider_name,
        created_by_email=None,
        updated_by_email=current_user.email,
    )


@router.post(
    "/{mapping_id}/apply",
    response_model=ExternalStatusMappingApplyResult,
)
async def apply_order_status_mapping(
    mapping_id: int,
    session: AsyncSession = Depends(get_session),
):
    mapping = await session.get(ExternalStatusMapping, mapping_id)
    if mapping is None:
        raise HTTPException(status_code=404, detail="Правило не найдено")
    return ExternalStatusMappingApplyResult(
        **await apply_mapping_to_existing_items(session, mapping=mapping)
    )
