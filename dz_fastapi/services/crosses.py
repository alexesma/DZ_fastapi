import logging
from dataclasses import dataclass
from itertools import combinations
from typing import Iterable, Optional

from sqlalchemy import Select, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.core.constants import AUTO_OEM_CROSS_BRANDS
from dz_fastapi.models.autopart import AutoPart, preprocess_oem_number
from dz_fastapi.models.brand import Brand
from dz_fastapi.models.cross import AutoPartCross, AutoPartInvalidCross

logger = logging.getLogger("dz_fastapi")

DRAGONZAP_BRAND_NAME = "DRAGONZAP"
AUTO_CROSS_COMMENT = "Автокросс по совпадению OEM Dragonzap/original"


@dataclass(frozen=True)
class CrossLinkState:
    source_autopart_id: int
    source_brand_id: int
    source_oem_number: str
    cross_brand_id: int
    cross_oem_number: str
    cross_autopart_id: Optional[int]
    is_bidirectional: bool


def _normalize_brand_name(name: str | None) -> str:
    return str(name or "").strip().upper()


def _build_cross_lookup_key(brand_id: int, oem_number: str) -> tuple[int, str]:
    return brand_id, preprocess_oem_number(oem_number)


def _base_oem_for_auto_cross(brand_name: str | None, oem_number: str) -> str:
    normalized_brand = _normalize_brand_name(brand_name)
    normalized_oem = preprocess_oem_number(oem_number)
    if (
        normalized_brand == DRAGONZAP_BRAND_NAME
        and normalized_oem.startswith("DZ")
        and len(normalized_oem) > 2
    ):
        return normalized_oem[2:]
    return normalized_oem


async def resolve_cross_autopart_id(
    session: AsyncSession,
    *,
    brand_id: int,
    oem_number: str,
) -> Optional[int]:
    normalized_oem = preprocess_oem_number(oem_number)
    return (
        await session.execute(
            select(AutoPart.id).where(
                AutoPart.brand_id == brand_id,
                AutoPart.oem_number == normalized_oem,
            )
        )
    ).scalar_one_or_none()


async def get_cross_row(
    session: AsyncSession,
    *,
    source_autopart_id: int,
    cross_brand_id: int,
    cross_oem_number: str,
) -> Optional[AutoPartCross]:
    normalized_oem = preprocess_oem_number(cross_oem_number)
    return (
        await session.execute(
            select(AutoPartCross).where(
                AutoPartCross.source_autopart_id == source_autopart_id,
                AutoPartCross.cross_brand_id == cross_brand_id,
                AutoPartCross.cross_oem_number == normalized_oem,
            )
        )
    ).scalar_one_or_none()


def snapshot_cross_state(
    cross: AutoPartCross,
    *,
    source_brand_id: int,
    source_oem_number: str,
) -> CrossLinkState:
    return CrossLinkState(
        source_autopart_id=cross.source_autopart_id,
        source_brand_id=source_brand_id,
        source_oem_number=preprocess_oem_number(source_oem_number),
        cross_brand_id=cross.cross_brand_id,
        cross_oem_number=preprocess_oem_number(cross.cross_oem_number),
        cross_autopart_id=cross.cross_autopart_id,
        is_bidirectional=bool(cross.is_bidirectional),
    )


async def _remove_reverse_counterpart_for_state(
    session: AsyncSession,
    state: CrossLinkState,
) -> bool:
    if (
        not state.is_bidirectional
        or not state.cross_autopart_id
        or state.cross_autopart_id == state.source_autopart_id
    ):
        return False
    reverse_row = await get_cross_row(
        session,
        source_autopart_id=state.cross_autopart_id,
        cross_brand_id=state.source_brand_id,
        cross_oem_number=state.source_oem_number,
    )
    if reverse_row is None or not reverse_row.is_bidirectional:
        return False
    await session.delete(reverse_row)
    return True


async def save_cross_relation(
    session: AsyncSession,
    *,
    source_autopart: AutoPart,
    cross_brand_id: int,
    cross_oem_number: str,
    is_bidirectional: bool = True,
    priority: int = 100,
    comment: str | None = None,
    overwrite_comment: bool = True,
    upgrade_existing_bidirectional: bool = True,
) -> tuple[AutoPartCross, bool]:
    normalized_oem = preprocess_oem_number(cross_oem_number)
    cross_autopart_id = await resolve_cross_autopart_id(
        session,
        brand_id=cross_brand_id,
        oem_number=normalized_oem,
    )
    existing = await get_cross_row(
        session,
        source_autopart_id=source_autopart.id,
        cross_brand_id=cross_brand_id,
        cross_oem_number=normalized_oem,
    )
    created = False
    if existing is None:
        existing = AutoPartCross(
            source_autopart_id=source_autopart.id,
            cross_brand_id=cross_brand_id,
            cross_oem_number=normalized_oem,
            cross_autopart_id=cross_autopart_id,
            is_bidirectional=is_bidirectional,
            priority=priority,
            comment=comment,
        )
        session.add(existing)
        created = True
    else:
        existing.cross_autopart_id = cross_autopart_id
        existing.priority = priority
        if is_bidirectional and upgrade_existing_bidirectional:
            existing.is_bidirectional = True
        elif not is_bidirectional:
            existing.is_bidirectional = False
        if overwrite_comment:
            existing.comment = comment
        elif comment and not existing.comment:
            existing.comment = comment

    if (
        existing.is_bidirectional
        and cross_autopart_id
        and cross_autopart_id != source_autopart.id
    ):
        reverse_oem = preprocess_oem_number(source_autopart.oem_number)
        reverse = await get_cross_row(
            session,
            source_autopart_id=cross_autopart_id,
            cross_brand_id=source_autopart.brand_id,
            cross_oem_number=reverse_oem,
        )
        if reverse is None:
            reverse = AutoPartCross(
                source_autopart_id=cross_autopart_id,
                cross_brand_id=source_autopart.brand_id,
                cross_oem_number=reverse_oem,
                cross_autopart_id=source_autopart.id,
                is_bidirectional=True,
                priority=priority,
                comment=comment,
            )
            session.add(reverse)
        else:
            reverse.cross_autopart_id = source_autopart.id
            reverse.priority = priority
            if upgrade_existing_bidirectional:
                reverse.is_bidirectional = True
            if overwrite_comment:
                reverse.comment = comment
            elif comment and not reverse.comment:
                reverse.comment = comment
    return existing, created


async def sync_cross_relation_update(
    session: AsyncSession,
    *,
    source_autopart: AutoPart,
    cross: AutoPartCross,
    old_state: CrossLinkState,
) -> None:
    current_state = snapshot_cross_state(
        cross,
        source_brand_id=source_autopart.brand_id,
        source_oem_number=source_autopart.oem_number,
    )
    if old_state != current_state:
        await _remove_reverse_counterpart_for_state(session, old_state)
    if not cross.is_bidirectional:
        return
    await save_cross_relation(
        session,
        source_autopart=source_autopart,
        cross_brand_id=cross.cross_brand_id,
        cross_oem_number=cross.cross_oem_number,
        is_bidirectional=True,
        priority=cross.priority,
        comment=cross.comment,
        overwrite_comment=True,
        upgrade_existing_bidirectional=True,
    )


async def delete_cross_relation(
    session: AsyncSession,
    *,
    cross: AutoPartCross,
    source_brand_id: int,
    source_oem_number: str,
) -> None:
    if cross.is_bidirectional:
        await _remove_reverse_counterpart_for_state(
            session,
            snapshot_cross_state(
                cross,
                source_brand_id=source_brand_id,
                source_oem_number=source_oem_number,
            ),
        )
    await session.delete(cross)


async def _load_auto_cross_autoparts(
    session: AsyncSession,
) -> list[tuple[int, int, str, str]]:
    brand_stmt: Select = select(Brand.id, Brand.name).where(
        func.upper(Brand.name).in_(AUTO_OEM_CROSS_BRANDS)
    )
    brand_rows = (await session.execute(brand_stmt)).all()
    brand_id_to_name = {
        brand_id: _normalize_brand_name(brand_name)
        for brand_id, brand_name in brand_rows
    }
    if not brand_id_to_name:
        return []
    autopart_rows = (
        await session.execute(
            select(AutoPart.id, AutoPart.brand_id, AutoPart.oem_number).where(
                AutoPart.brand_id.in_(list(brand_id_to_name))
            )
        )
    ).all()
    return [
        (
            autopart_id,
            brand_id,
            str(oem_number or ""),
            brand_id_to_name.get(brand_id, ""),
        )
        for autopart_id, brand_id, oem_number in autopart_rows
    ]


async def _load_invalid_pair_keys(
    session: AsyncSession,
) -> set[tuple[int, int, str]]:
    rows = (
        await session.execute(
            select(
                AutoPartInvalidCross.source_autopart_id,
                AutoPartInvalidCross.invalid_brand_id,
                AutoPartInvalidCross.invalid_oem_number,
            )
        )
    ).all()
    return {
        (source_id, brand_id, preprocess_oem_number(oem_number))
        for source_id, brand_id, oem_number in rows
    }


async def sync_automatic_oem_crosses(
    session: AsyncSession,
) -> dict[str, int]:
    autoparts = await _load_auto_cross_autoparts(session)
    invalid_keys = await _load_invalid_pair_keys(session)

    autoparts_by_id: dict[int, AutoPart] = {}
    group_map: dict[str, list[tuple[int, int, str, str]]] = {}
    for autopart_id, brand_id, oem_number, brand_name in autoparts:
        base_oem = _base_oem_for_auto_cross(brand_name, oem_number)
        if not base_oem:
            continue
        group_map.setdefault(base_oem, []).append(
            (autopart_id, brand_id, oem_number, brand_name)
        )

    created = 0
    touched_groups = 0
    for base_oem, group_rows in group_map.items():
        if len(group_rows) < 2:
            continue
        dragonzap_rows = [
            row for row in group_rows if row[3] == DRAGONZAP_BRAND_NAME
        ]
        if not dragonzap_rows:
            continue
        touched_groups += 1
        autopart_ids = [row[0] for row in group_rows]
        missing_ids = [part_id for part_id in autopart_ids if part_id not in autoparts_by_id]
        if missing_ids:
            loaded = (
                await session.execute(
                    select(AutoPart).where(AutoPart.id.in_(missing_ids))
                )
            ).scalars().all()
            autoparts_by_id.update({row.id: row for row in loaded})

        for left, right in combinations(group_rows, 2):
            left_id, left_brand_id, left_oem, _left_brand_name = left
            right_id, right_brand_id, right_oem, _right_brand_name = right
            if (
                (left_id, right_brand_id, preprocess_oem_number(right_oem))
                in invalid_keys
            ) or (
                (right_id, left_brand_id, preprocess_oem_number(left_oem))
                in invalid_keys
            ):
                continue

            left_autopart = autoparts_by_id.get(left_id)
            right_autopart = autoparts_by_id.get(right_id)
            if left_autopart is None or right_autopart is None:
                continue

            existing_forward = await get_cross_row(
                session,
                source_autopart_id=left_id,
                cross_brand_id=right_brand_id,
                cross_oem_number=right_oem,
            )
            if existing_forward is not None and not existing_forward.is_bidirectional:
                continue
            existing_reverse = await get_cross_row(
                session,
                source_autopart_id=right_id,
                cross_brand_id=left_brand_id,
                cross_oem_number=left_oem,
            )
            if existing_reverse is not None and not existing_reverse.is_bidirectional:
                continue

            _, created_now = await save_cross_relation(
                session,
                source_autopart=left_autopart,
                cross_brand_id=right_brand_id,
                cross_oem_number=right_oem,
                is_bidirectional=True,
                comment=AUTO_CROSS_COMMENT,
                overwrite_comment=False,
                upgrade_existing_bidirectional=False,
            )
            if created_now:
                created += 1
            if existing_reverse is None:
                created += 1

    if created:
        logger.info(
            "Auto OEM cross sync created %s rows across %s groups",
            created,
            touched_groups,
        )
    return {
        "groups_checked": touched_groups,
        "rows_created": created,
    }


def iter_auto_cross_brand_names() -> Iterable[str]:
    return AUTO_OEM_CROSS_BRANDS
