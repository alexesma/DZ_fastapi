import logging
from collections import defaultdict
from decimal import Decimal
from typing import Any, DefaultDict

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.crud.brand import brand_crud
from dz_fastapi.models.autopart import (AutoPart, AutoPartPriceHistory,
                                        AutoPartRestockDecision, Photo,
                                        autopart_category_association,
                                        autopart_storage_association)
from dz_fastapi.models.cross import AutoPartCross, AutoPartSubstitution
from dz_fastapi.models.partner import (CustomerOrderItem,
                                       CustomerPriceListAutoPartAssociation,
                                       OrderItem, PriceListAutoPartAssociation,
                                       StockOrderItem, SupplierOrderItem)
from dz_fastapi.models.price_control import (CustomerPriceListOverride,
                                             PriceControlRecommendation)

logger = logging.getLogger('dz_fastapi')


def _as_decimal(value: Any) -> Decimal:
    if value is None:
        return Decimal('0')
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def _prefer_source_offer(
    source_price: Any,
    target_price: Any,
    source_qty: int | None,
    target_qty: int | None,
) -> bool:
    if target_price is None and source_price is not None:
        return True
    if source_price is None:
        return False

    source_dec = _as_decimal(source_price)
    target_dec = _as_decimal(target_price)
    if source_dec < target_dec:
        return True
    if source_dec > target_dec:
        return False
    return int(source_qty or 0) > int(target_qty or 0)


async def _merge_pricelist_associations(
    session: AsyncSession,
    source_autopart_id: int,
    target_autopart_id: int,
    summary: DefaultDict[str, int],
) -> None:
    rows = (
        await session.execute(
            select(PriceListAutoPartAssociation).where(
                PriceListAutoPartAssociation.autopart_id.in_(
                    [source_autopart_id, target_autopart_id]
                )
            )
        )
    ).scalars().all()

    target_by_pricelist = {
        row.pricelist_id: row
        for row in rows
        if row.autopart_id == target_autopart_id
    }
    for source_row in rows:
        if source_row.autopart_id != source_autopart_id:
            continue
        target_row = target_by_pricelist.get(source_row.pricelist_id)
        if target_row is None:
            source_row.autopart_id = target_autopart_id
            summary['pricelist_assoc_moved'] += 1
            continue

        if _prefer_source_offer(
            source_row.price,
            target_row.price,
            source_row.quantity,
            target_row.quantity,
        ):
            target_row.price = source_row.price
            target_row.quantity = source_row.quantity
            target_row.multiplicity = source_row.multiplicity
            summary['pricelist_assoc_replaced'] += 1
        await session.delete(source_row)
        summary['pricelist_assoc_deleted'] += 1


async def _merge_customer_pricelist_associations(
    session: AsyncSession,
    source_autopart_id: int,
    target_autopart_id: int,
    summary: DefaultDict[str, int],
) -> None:
    rows = (
        await session.execute(
            select(CustomerPriceListAutoPartAssociation).where(
                CustomerPriceListAutoPartAssociation.autopart_id.in_(
                    [source_autopart_id, target_autopart_id]
                )
            )
        )
    ).scalars().all()

    target_by_pricelist = {
        row.customerpricelist_id: row
        for row in rows
        if row.autopart_id == target_autopart_id
    }
    for source_row in rows:
        if source_row.autopart_id != source_autopart_id:
            continue
        target_row = target_by_pricelist.get(source_row.customerpricelist_id)
        if target_row is None:
            source_row.autopart_id = target_autopart_id
            summary['customer_pricelist_assoc_moved'] += 1
            continue

        if _prefer_source_offer(
            source_row.price,
            target_row.price,
            source_row.quantity,
            target_row.quantity,
        ):
            target_row.price = source_row.price
            target_row.quantity = source_row.quantity
            summary['customer_pricelist_assoc_replaced'] += 1
        await session.delete(source_row)
        summary['customer_pricelist_assoc_deleted'] += 1


async def _merge_customer_pricelist_overrides(
    session: AsyncSession,
    source_autopart_id: int,
    target_autopart_id: int,
    summary: DefaultDict[str, int],
) -> None:
    rows = (
        await session.execute(
            select(CustomerPriceListOverride).where(
                CustomerPriceListOverride.autopart_id.in_(
                    [source_autopart_id, target_autopart_id]
                )
            )
        )
    ).scalars().all()

    target_by_config = {
        row.config_id: row
        for row in rows
        if row.autopart_id == target_autopart_id
    }
    for source_row in rows:
        if source_row.autopart_id != source_autopart_id:
            continue
        target_row = target_by_config.get(source_row.config_id)
        if target_row is None:
            source_row.autopart_id = target_autopart_id
            summary['override_moved'] += 1
            continue

        source_updated = source_row.updated_at or source_row.created_at
        target_updated = target_row.updated_at or target_row.created_at
        if source_updated and (
            target_updated is None or source_updated > target_updated
        ):
            target_row.price = source_row.price
            target_row.is_active = source_row.is_active
            target_row.updated_at = source_row.updated_at
            summary['override_replaced'] += 1
        await session.delete(source_row)
        summary['override_deleted'] += 1


async def _merge_link_table(
    session: AsyncSession,
    table,
    related_column: str,
    source_autopart_id: int,
    target_autopart_id: int,
    summary: DefaultDict[str, int],
    summary_key: str,
) -> None:
    existing_rows = await session.execute(
        select(table.c[related_column]).where(
            table.c.autopart_id == target_autopart_id
        )
    )
    existing_values = {row[0] for row in existing_rows.fetchall()}

    source_rows = await session.execute(
        select(table.c[related_column]).where(
            table.c.autopart_id == source_autopart_id
        )
    )
    for row in source_rows.fetchall():
        related_value = row[0]
        if related_value in existing_values:
            continue
        await session.execute(
            table.insert().values(
                autopart_id=target_autopart_id,
                **{related_column: related_value},
            )
        )
        existing_values.add(related_value)
        summary[summary_key] += 1

    await session.execute(
        table.delete().where(table.c.autopart_id == source_autopart_id)
    )


async def _merge_crosses(
    session: AsyncSession,
    source_autopart_id: int,
    target_autopart_id: int,
    summary: DefaultDict[str, int],
) -> None:
    rows = (
        await session.execute(
            select(AutoPartCross).where(
                (AutoPartCross.source_autopart_id == source_autopart_id)
                | (AutoPartCross.source_autopart_id == target_autopart_id)
            )
        )
    ).scalars().all()

    target_by_key = {
        (row.cross_brand_id, row.cross_oem_number): row
        for row in rows
        if row.source_autopart_id == target_autopart_id
    }

    for source_row in rows:
        if source_row.source_autopart_id != source_autopart_id:
            continue

        if source_row.cross_autopart_id == source_autopart_id:
            source_row.cross_autopart_id = target_autopart_id
        if source_row.cross_autopart_id == target_autopart_id:
            await session.delete(source_row)
            summary['cross_deleted'] += 1
            continue

        key = (source_row.cross_brand_id, source_row.cross_oem_number)
        target_row = target_by_key.get(key)
        if target_row is None:
            source_row.source_autopart_id = target_autopart_id
            summary['cross_moved'] += 1
            continue

        if target_row.cross_autopart_id is None:
            target_row.cross_autopart_id = source_row.cross_autopart_id
        target_row.priority = min(
            int(target_row.priority or 100),
            int(source_row.priority or 100),
        )
        if not target_row.comment and source_row.comment:
            target_row.comment = source_row.comment
        await session.delete(source_row)
        summary['cross_deleted'] += 1

    cross_ref_rows = (
        await session.execute(
            select(AutoPartCross).where(
                AutoPartCross.cross_autopart_id == source_autopart_id
            )
        )
    ).scalars().all()
    for row in cross_ref_rows:
        if row.source_autopart_id == target_autopart_id:
            await session.delete(row)
            summary['cross_deleted'] += 1
            continue
        row.cross_autopart_id = target_autopart_id
        summary['cross_ref_updated'] += 1


async def _merge_substitutions(
    session: AsyncSession,
    source_autopart_id: int,
    target_autopart_id: int,
    summary: DefaultDict[str, int],
) -> None:
    rows = (
        await session.execute(
            select(AutoPartSubstitution).where(
                AutoPartSubstitution.source_autopart_id.in_(
                    [source_autopart_id, target_autopart_id]
                )
            )
        )
    ).scalars().all()

    target_by_key = {
        (
            row.substitution_brand_id,
            row.substitution_oem_number,
            row.customer_config_id,
        ): row
        for row in rows
        if row.source_autopart_id == target_autopart_id
    }

    for source_row in rows:
        if source_row.source_autopart_id != source_autopart_id:
            continue
        key = (
            source_row.substitution_brand_id,
            source_row.substitution_oem_number,
            source_row.customer_config_id,
        )
        target_row = target_by_key.get(key)
        if target_row is None:
            source_row.source_autopart_id = target_autopart_id
            summary['substitution_moved'] += 1
            continue

        target_row.priority = min(target_row.priority, source_row.priority)
        target_row.min_source_quantity = min(
            target_row.min_source_quantity, source_row.min_source_quantity
        )
        target_row.quantity_reduction = min(
            target_row.quantity_reduction, source_row.quantity_reduction
        )
        target_row.is_active = target_row.is_active or source_row.is_active
        if not target_row.comment and source_row.comment:
            target_row.comment = source_row.comment
        await session.delete(source_row)
        summary['substitution_deleted'] += 1


async def merge_autopart_into_target(
    session: AsyncSession,
    source_autopart_id: int,
    target_autopart_id: int,
) -> dict[str, int]:
    if source_autopart_id == target_autopart_id:
        return {}

    source_autopart = await session.get(AutoPart, source_autopart_id)
    target_autopart = await session.get(AutoPart, target_autopart_id)
    if source_autopart is None or target_autopart is None:
        raise ValueError('Source or target autopart not found')

    summary: DefaultDict[str, int] = defaultdict(int)

    await _merge_pricelist_associations(
        session, source_autopart_id, target_autopart_id, summary
    )
    await _merge_customer_pricelist_associations(
        session, source_autopart_id, target_autopart_id, summary
    )
    await _merge_customer_pricelist_overrides(
        session, source_autopart_id, target_autopart_id, summary
    )
    await _merge_link_table(
        session,
        autopart_storage_association,
        'storage_location_id',
        source_autopart_id,
        target_autopart_id,
        summary,
        'storage_links_moved',
    )
    await _merge_link_table(
        session,
        autopart_category_association,
        'category_id',
        source_autopart_id,
        target_autopart_id,
        summary,
        'category_links_moved',
    )
    await _merge_crosses(
        session, source_autopart_id, target_autopart_id, summary
    )
    await _merge_substitutions(
        session, source_autopart_id, target_autopart_id, summary
    )

    for model in (
        AutoPartPriceHistory,
        AutoPartRestockDecision,
        CustomerOrderItem,
        SupplierOrderItem,
        StockOrderItem,
        OrderItem,
        Photo,
        PriceControlRecommendation,
    ):
        result = await session.execute(
            update(model)
            .where(model.autopart_id == source_autopart_id)
            .values(autopart_id=target_autopart_id)
        )
        summary[f'{model.__tablename__}_updated'] += int(
            result.rowcount or 0
        )

    await session.delete(source_autopart)
    summary['autoparts_deleted'] += 1
    await session.flush()
    return dict(summary)


async def canonicalize_autoparts_by_brand_synonyms(
    session: AsyncSession,
    dry_run: bool = False,
) -> dict[str, int]:
    summary: DefaultDict[str, int] = defaultdict(int)
    brands = await brand_crud.get_multi_with_synonyms(session)
    components = brand_crud.get_connected_brand_components(brands)

    for component in components:
        main_brands = [brand for brand in component if brand.main_brand]
        if len(main_brands) != 1:
            if len(component) > 1:
                summary['skipped_components_without_single_main_brand'] += 1
            continue
        canonical_brand = main_brands[0]
        brand_ids = [brand.id for brand in component]

        autoparts = (
            await session.execute(
                select(AutoPart)
                .where(AutoPart.brand_id.in_(brand_ids))
                .order_by(AutoPart.oem_number.asc(), AutoPart.id.asc())
            )
        ).scalars().all()

        by_oem: DefaultDict[str, list[AutoPart]] = defaultdict(list)
        for autopart in autoparts:
            by_oem[autopart.oem_number].append(autopart)

        for oem_number, grouped_autoparts in by_oem.items():
            if not grouped_autoparts:
                continue

            canonical_candidates = sorted(
                (
                    autopart
                    for autopart in grouped_autoparts
                    if autopart.brand_id == canonical_brand.id
                ),
                key=lambda autopart: autopart.id,
            )
            target = (
                canonical_candidates[0]
                if canonical_candidates
                else min(grouped_autoparts, key=lambda autopart: autopart.id)
            )

            needs_rebrand = target.brand_id != canonical_brand.id
            has_duplicates = len(grouped_autoparts) > 1
            if not needs_rebrand and not has_duplicates:
                continue

            logger.info(
                'Canonicalizing OEM=%s to brand=%s '
                'using target autopart_id=%s',
                oem_number,
                canonical_brand.name,
                target.id,
            )

            if needs_rebrand:
                summary['autoparts_rebranded'] += 1
                if not dry_run:
                    target.brand = canonical_brand
                    target.brand_id = canonical_brand.id
                    await session.flush()

            for source in grouped_autoparts:
                if source.id == target.id:
                    continue
                summary['autoparts_merged'] += 1
                if dry_run:
                    continue
                merge_summary = await merge_autopart_into_target(
                    session=session,
                    source_autopart_id=source.id,
                    target_autopart_id=target.id,
                )
                for key, value in merge_summary.items():
                    summary[key] += value

    return dict(summary)
