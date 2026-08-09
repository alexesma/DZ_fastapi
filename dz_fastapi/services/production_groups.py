"""DragonZap production groups derived from the confirmed cross catalogue."""

from __future__ import annotations

from collections import defaultdict, deque
from decimal import Decimal
from typing import Any, Optional

from sqlalchemy import func, or_
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.orm import noload, selectinload

from dz_fastapi.core.time import now_moscow
from dz_fastapi.models.autopart import AutoPart
from dz_fastapi.models.brand import Brand
from dz_fastapi.models.cross import AutoPartCross
from dz_fastapi.models.inventory import (
    DragonzapProductionGroup,
    DragonzapProductionMaterialOverride,
    StockLot,
    StockLotRole,
)

DRAGONZAP_BRAND = "DRAGONZAP"
MAX_COMPONENT_NODES = 500


async def sync_production_groups(
    session: AsyncSession,
    *,
    user_id: Optional[int] = None,
) -> tuple[int, int]:
    """Create settings rows for DragonZap parts participating in real cross edges."""

    has_outgoing = (
        select(AutoPartCross.id)
        .where(
            AutoPartCross.source_autopart_id == AutoPart.id,
            AutoPartCross.cross_autopart_id.is_not(None),
            AutoPartCross.is_bidirectional.is_(True),
        )
        .exists()
    )
    has_incoming = (
        select(AutoPartCross.id)
        .where(
            AutoPartCross.cross_autopart_id == AutoPart.id,
            AutoPartCross.is_bidirectional.is_(True),
        )
        .exists()
    )
    output_ids = set(
        (
            await session.execute(
                select(AutoPart.id)
                .join(Brand, Brand.id == AutoPart.brand_id)
                .where(
                    func.upper(Brand.name) == DRAGONZAP_BRAND,
                    or_(has_outgoing, has_incoming),
                )
            )
        )
        .scalars()
        .all()
    )
    if not output_ids:
        return 0, 0

    existing_ids = set(
        (
            await session.execute(
                select(DragonzapProductionGroup.finished_autopart_id).where(
                    DragonzapProductionGroup.finished_autopart_id.in_(output_ids)
                )
            )
        )
        .scalars()
        .all()
    )
    missing_ids = sorted(output_ids - existing_ids)
    if not missing_ids:
        return 0, len(output_ids)

    timestamp = now_moscow()
    inserted_count = 0
    for start in range(0, len(missing_ids), 500):
        batch = missing_ids[start:start + 500]
        inserted = (
            await session.execute(
                pg_insert(DragonzapProductionGroup)
                .values(
                    [
                        {
                            "finished_autopart_id": autopart_id,
                            "is_active": True,
                            "packaging_cost": Decimal("0"),
                            "created_by_user_id": user_id,
                            "updated_by_user_id": user_id,
                            "created_at": timestamp,
                            "updated_at": timestamp,
                        }
                        for autopart_id in batch
                    ]
                )
                .on_conflict_do_nothing(index_elements=["finished_autopart_id"])
                .returning(DragonzapProductionGroup.id)
            )
        ).scalars().all()
        inserted_count += len(inserted)
    return inserted_count, len(output_ids)


async def _load_cross_graph(
    session: AsyncSession,
    output_ids: set[int],
) -> tuple[dict[int, set[int]], dict[tuple[int, int], int], bool]:
    """Load only components reachable from requested outputs, with a safety cap."""

    adjacency: dict[int, set[int]] = defaultdict(set)
    priorities: dict[tuple[int, int], int] = {}
    seen = set(output_ids)
    frontier = set(output_ids)
    truncated = False

    while frontier:
        row_limit = MAX_COMPONENT_NODES * 4 + 1
        rows = (
            await session.execute(
                select(
                    AutoPartCross.source_autopart_id,
                    AutoPartCross.cross_autopart_id,
                    AutoPartCross.priority,
                ).where(
                    AutoPartCross.cross_autopart_id.is_not(None),
                    AutoPartCross.is_bidirectional.is_(True),
                    or_(
                        AutoPartCross.source_autopart_id.in_(frontier),
                        AutoPartCross.cross_autopart_id.in_(frontier),
                    ),
                ).limit(row_limit)
            )
        ).all()
        if len(rows) >= row_limit:
            rows = rows[: row_limit - 1]
            truncated = True

        discovered: set[int] = set()
        for source_id, target_id, priority in rows:
            source_id = int(source_id)
            target_id = int(target_id)
            if source_id not in seen:
                discovered.add(source_id)
            if target_id not in seen:
                discovered.add(target_id)

        remaining_capacity = MAX_COMPONENT_NODES - len(seen)
        if len(discovered) > remaining_capacity:
            discovered = set(sorted(discovered)[: max(0, remaining_capacity)])
            truncated = True
        allowed_nodes = seen | discovered
        for source_id, target_id, priority in rows:
            source_id = int(source_id)
            target_id = int(target_id)
            if source_id not in allowed_nodes or target_id not in allowed_nodes:
                continue
            adjacency[source_id].add(target_id)
            adjacency[target_id].add(source_id)
            edge_priority = max(1, int(priority or 100))
            key = (min(source_id, target_id), max(source_id, target_id))
            priorities[key] = min(priorities.get(key, edge_priority), edge_priority)
        if not discovered:
            break
        seen.update(discovered)
        frontier = discovered

    return adjacency, priorities, truncated


def _component(adjacency: dict[int, set[int]], root_id: int) -> set[int]:
    found = {root_id}
    queue = deque([root_id])
    while queue:
        current = queue.popleft()
        for candidate in adjacency.get(current, set()):
            if candidate not in found:
                found.add(candidate)
                queue.append(candidate)
    return found


async def list_production_groups(
    session: AsyncSession,
    *,
    query: Optional[str] = None,
    is_active: Optional[bool] = None,
    limit: int = 20,
    offset: int = 0,
    group_id: Optional[int] = None,
) -> tuple[list[dict[str, Any]], int]:
    filters = []
    if group_id is not None:
        filters.append(DragonzapProductionGroup.id == group_id)
    if is_active is not None:
        filters.append(DragonzapProductionGroup.is_active == is_active)
    if query and query.strip():
        pattern = f"%{query.strip()}%"
        filters.append(
            or_(
                AutoPart.oem_number.ilike(pattern),
                AutoPart.name.ilike(pattern),
                Brand.name.ilike(pattern),
            )
        )

    base = (
        select(DragonzapProductionGroup)
        .join(
            AutoPart,
            AutoPart.id == DragonzapProductionGroup.finished_autopart_id,
        )
        .join(Brand, Brand.id == AutoPart.brand_id)
        .where(*filters)
    )
    total = int(
        (
            await session.execute(
                select(func.count()).select_from(base.subquery())
            )
        ).scalar_one()
    )
    groups = (
        (
            await session.execute(
                base.options(
                    selectinload(DragonzapProductionGroup.finished_autopart).selectinload(
                        AutoPart.brand
                    ),
                    selectinload(DragonzapProductionGroup.updated_by_user),
                )
                .order_by(AutoPart.oem_number, DragonzapProductionGroup.id)
                .limit(limit)
                .offset(offset)
            )
        )
        .scalars()
        .unique()
        .all()
    )
    if not groups:
        return [], total

    group_ids = [group.id for group in groups]
    output_ids = {group.finished_autopart_id for group in groups}
    adjacency, edge_priorities, truncated = await _load_cross_graph(
        session,
        output_ids,
    )
    candidates_by_group = {
        group.id: _component(adjacency, group.finished_autopart_id)
        - {group.finished_autopart_id}
        for group in groups
    }
    candidate_ids = set().union(*candidates_by_group.values())

    parts_by_id = {}
    if candidate_ids:
        parts_by_id = {
            int(autopart_id): {
                "brand": brand_name,
                "oem_number": oem_number,
                "name": name,
            }
            for autopart_id, brand_name, oem_number, name in (
                await session.execute(
                    select(
                        AutoPart.id,
                        Brand.name,
                        AutoPart.oem_number,
                        AutoPart.name,
                    )
                    .join(Brand, Brand.id == AutoPart.brand_id)
                    .where(AutoPart.id.in_(candidate_ids))
                )
            ).all()
        }

    availability: dict[int, tuple[int, int]] = {}
    if candidate_ids:
        availability = {
            int(autopart_id): (int(quantity or 0), int(lot_count or 0))
            for autopart_id, quantity, lot_count in (
                await session.execute(
                    select(
                        StockLot.autopart_id,
                        func.sum(StockLot.remaining_quantity),
                        func.count(StockLot.id),
                    )
                    .where(
                        StockLot.autopart_id.in_(candidate_ids),
                        StockLot.inventory_role == StockLotRole.DRAGONZAP_MATERIAL,
                        StockLot.remaining_quantity > 0,
                    )
                    .group_by(StockLot.autopart_id)
                )
            ).all()
        }

    overrides = (
        (
            await session.execute(
                select(DragonzapProductionMaterialOverride)
                .where(
                    DragonzapProductionMaterialOverride.production_group_id.in_(
                        group_ids
                    )
                )
                .options(
                    noload(
                        DragonzapProductionMaterialOverride.material_autopart
                    ),
                    selectinload(
                        DragonzapProductionMaterialOverride.updated_by_user
                    )
                )
            )
        )
        .scalars()
        .all()
    )
    overrides_by_key = {
        (row.production_group_id, row.material_autopart_id): row
        for row in overrides
    }

    result = []
    for group in groups:
        materials = []
        for material_id in candidates_by_group[group.id]:
            part = parts_by_id.get(material_id)
            if part is None:
                continue
            override = overrides_by_key.get((group.id, material_id))
            edge_key = (
                min(group.finished_autopart_id, material_id),
                max(group.finished_autopart_id, material_id),
            )
            quantity, lot_count = availability.get(material_id, (0, 0))
            materials.append(
                {
                    "autopart_id": material_id,
                    "brand": part["brand"] or "—",
                    "oem_number": part["oem_number"],
                    "name": part["name"],
                    "priority": (
                        override.priority
                        if override is not None
                        else edge_priorities.get(edge_key, 100)
                    ),
                    "is_allowed": override.is_allowed if override else True,
                    "reason": override.reason if override else None,
                    "available_material_quantity": quantity,
                    "active_lots_count": lot_count,
                    "has_override": override is not None,
                    "updated_by_user_id": (
                        override.updated_by_user_id if override else None
                    ),
                    "updated_by_name": (
                        override.updated_by_user.name
                        or override.updated_by_user.email
                        if override and override.updated_by_user
                        else None
                    ),
                    "updated_at": override.updated_at if override else None,
                }
            )
        materials.sort(
            key=lambda row: (
                not row["is_allowed"],
                row["priority"],
                row["brand"].casefold(),
                row["oem_number"],
            )
        )
        allowed = [row for row in materials if row["is_allowed"]]
        finished = group.finished_autopart
        result.append(
            {
                "id": group.id,
                "finished_autopart_id": group.finished_autopart_id,
                "finished_brand": finished.brand.name if finished.brand else "DRAGONZAP",
                "finished_oem_number": finished.oem_number,
                "finished_name": finished.name,
                "is_active": group.is_active,
                "packaging_cost": group.packaging_cost or Decimal("0"),
                "packaging_description": group.packaging_description,
                "notes": group.notes,
                "candidates_count": len(materials),
                "allowed_candidates_count": len(allowed),
                "available_material_quantity": sum(
                    row["available_material_quantity"] for row in allowed
                ),
                "graph_truncated": truncated,
                "materials": materials,
                "updated_by_user_id": group.updated_by_user_id,
                "updated_by_name": (
                    group.updated_by_user.name or group.updated_by_user.email
                    if group.updated_by_user
                    else None
                ),
                "created_at": group.created_at,
                "updated_at": group.updated_at,
            }
        )
    return result, total


async def update_production_group(
    session: AsyncSession,
    *,
    group_id: int,
    values: dict[str, Any],
    user_id: int,
) -> DragonzapProductionGroup:
    group = await session.get(DragonzapProductionGroup, group_id)
    if group is None:
        raise LookupError("Группа выпуска не найдена")
    for field, value in values.items():
        setattr(group, field, value)
    group.updated_by_user_id = user_id
    await session.flush()
    return group


async def upsert_material_override(
    session: AsyncSession,
    *,
    group_id: int,
    material_autopart_id: int,
    values: dict[str, Any],
    user_id: int,
) -> DragonzapProductionMaterialOverride:
    group = await session.get(DragonzapProductionGroup, group_id)
    if group is None:
        raise LookupError("Группа выпуска не найдена")
    adjacency, _, truncated = await _load_cross_graph(
        session,
        {group.finished_autopart_id},
    )
    if truncated:
        raise ValueError(
            "Граф кроссов слишком велик. Проверьте ошибочные связи до настройки материала."
        )
    candidates = _component(adjacency, group.finished_autopart_id) - {
        group.finished_autopart_id
    }
    if material_autopart_id not in candidates:
        raise ValueError(
            "Позиция больше не входит в актуальную группу подтверждённых кроссов"
        )

    override = (
        await session.execute(
            select(DragonzapProductionMaterialOverride).where(
                DragonzapProductionMaterialOverride.production_group_id == group_id,
                DragonzapProductionMaterialOverride.material_autopart_id
                == material_autopart_id,
            )
        )
    ).scalar_one_or_none()
    if override is None:
        override = DragonzapProductionMaterialOverride(
            production_group_id=group_id,
            material_autopart_id=material_autopart_id,
        )
        session.add(override)
    for field, value in values.items():
        setattr(override, field, value)
    override.updated_by_user_id = user_id
    await session.flush()
    return override


async def delete_material_override(
    session: AsyncSession,
    *,
    group_id: int,
    material_autopart_id: int,
) -> bool:
    override = (
        await session.execute(
            select(DragonzapProductionMaterialOverride).where(
                DragonzapProductionMaterialOverride.production_group_id == group_id,
                DragonzapProductionMaterialOverride.material_autopart_id
                == material_autopart_id,
            )
        )
    ).scalar_one_or_none()
    if override is None:
        return False
    await session.delete(override)
    await session.flush()
    return True
