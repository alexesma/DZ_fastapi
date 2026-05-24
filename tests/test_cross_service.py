import pytest
from sqlalchemy import select

from dz_fastapi.models.autopart import AutoPart
from dz_fastapi.models.brand import Brand
from dz_fastapi.models.cross import AutoPartCross
from dz_fastapi.services.crosses import (
    load_bidirectional_cross_members,
    save_cross_relation,
    sync_automatic_oem_crosses,
)


@pytest.mark.asyncio
async def test_save_cross_relation_creates_reverse_row(test_session):
    dragonzap = Brand(name="DRAGONZAP")
    chery = Brand(name="CHERY")
    test_session.add_all([dragonzap, chery])
    await test_session.flush()

    source = AutoPart(
        name="Dragonzap Part",
        brand_id=dragonzap.id,
        oem_number="DZ123",
    )
    target = AutoPart(
        name="Chery Part",
        brand_id=chery.id,
        oem_number="123",
    )
    test_session.add_all([source, target])
    await test_session.flush()

    cross, created = await save_cross_relation(
        test_session,
        source_autopart=source,
        cross_brand_id=chery.id,
        cross_oem_number="123",
        is_bidirectional=True,
        comment="manual",
    )
    await test_session.commit()

    rows = (
        await test_session.execute(
            select(AutoPartCross).order_by(AutoPartCross.source_autopart_id)
        )
    ).scalars().all()

    assert created is True
    assert cross.cross_autopart_id == target.id
    assert len(rows) == 2
    assert all(row.is_bidirectional for row in rows)
    assert {
        (row.source_autopart_id, row.cross_brand_id, row.cross_oem_number)
        for row in rows
    } == {
        (source.id, chery.id, "123"),
        (target.id, dragonzap.id, "DZ123"),
    }


@pytest.mark.asyncio
async def test_sync_automatic_oem_crosses_creates_group_pairs(test_session):
    dragonzap = Brand(name="DRAGONZAP")
    chery = Brand(name="CHERY")
    test_session.add_all([dragonzap, chery])
    await test_session.flush()

    dz_prefixed = AutoPart(
        name="DZ Prefixed",
        brand_id=dragonzap.id,
        oem_number="DZ123",
    )
    dz_plain = AutoPart(
        name="DZ Plain",
        brand_id=dragonzap.id,
        oem_number="123",
    )
    chery_part = AutoPart(
        name="Chery Part",
        brand_id=chery.id,
        oem_number="123",
    )
    test_session.add_all([dz_prefixed, dz_plain, chery_part])
    await test_session.flush()

    result = await sync_automatic_oem_crosses(test_session)
    await test_session.commit()

    rows = (
        await test_session.execute(
            select(AutoPartCross).order_by(
                AutoPartCross.source_autopart_id,
                AutoPartCross.cross_brand_id,
                AutoPartCross.cross_oem_number,
            )
        )
    ).scalars().all()

    assert result["groups_checked"] == 1
    assert result["rows_created"] == 6
    assert len(rows) == 6
    assert all(row.is_bidirectional for row in rows)
    assert {
        (row.source_autopart_id, row.cross_brand_id, row.cross_oem_number)
        for row in rows
    } == {
        (dz_prefixed.id, dragonzap.id, "123"),
        (dz_prefixed.id, chery.id, "123"),
        (dz_plain.id, dragonzap.id, "DZ123"),
        (dz_plain.id, chery.id, "123"),
        (chery_part.id, dragonzap.id, "DZ123"),
        (chery_part.id, dragonzap.id, "123"),
    }


@pytest.mark.asyncio
async def test_sync_automatic_oem_crosses_respects_existing_one_way_cross(
    test_session,
):
    dragonzap = Brand(name="DRAGONZAP")
    chery = Brand(name="CHERY")
    test_session.add_all([dragonzap, chery])
    await test_session.flush()

    source = AutoPart(
        name="DZ Part",
        brand_id=dragonzap.id,
        oem_number="DZ123",
    )
    target = AutoPart(
        name="Chery Part",
        brand_id=chery.id,
        oem_number="123",
    )
    test_session.add_all([source, target])
    await test_session.flush()

    test_session.add(
        AutoPartCross(
            source_autopart_id=source.id,
            cross_brand_id=chery.id,
            cross_oem_number="123",
            cross_autopart_id=target.id,
            is_bidirectional=False,
        )
    )
    await test_session.commit()

    result = await sync_automatic_oem_crosses(test_session)
    await test_session.commit()

    rows = (
        await test_session.execute(select(AutoPartCross))
    ).scalars().all()

    assert result["rows_created"] == 0
    assert len(rows) == 1
    assert rows[0].is_bidirectional is False


@pytest.mark.asyncio
async def test_load_bidirectional_cross_members_resolves_transitive_group(
    test_session,
):
    brand = Brand(name="CHAIN")
    test_session.add(brand)
    await test_session.flush()

    part_a = AutoPart(name="Part A", brand_id=brand.id, oem_number="A123")
    part_b = AutoPart(name="Part B", brand_id=brand.id, oem_number="B123")
    part_c = AutoPart(name="Part C", brand_id=brand.id, oem_number="C123")
    part_d = AutoPart(name="Part D", brand_id=brand.id, oem_number="D123")
    test_session.add_all([part_a, part_b, part_c, part_d])
    await test_session.flush()

    await save_cross_relation(
        test_session,
        source_autopart=part_a,
        cross_brand_id=brand.id,
        cross_oem_number="B123",
        is_bidirectional=True,
        comment="A<->B",
    )
    await save_cross_relation(
        test_session,
        source_autopart=part_b,
        cross_brand_id=brand.id,
        cross_oem_number="C123",
        is_bidirectional=True,
        comment="B<->C",
    )
    await save_cross_relation(
        test_session,
        source_autopart=part_b,
        cross_brand_id=brand.id,
        cross_oem_number="D123",
        is_bidirectional=False,
        comment="B->D",
    )
    await test_session.commit()

    members = await load_bidirectional_cross_members(
        test_session,
        seed_autopart_ids=[part_a.id],
    )

    assert {
        (item.autopart_id, item.oem_number)
        for item in members
    } == {
        (part_a.id, "A123"),
        (part_b.id, "B123"),
        (part_c.id, "C123"),
    }
