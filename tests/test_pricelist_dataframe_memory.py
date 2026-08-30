"""Строки прайса читаются колонками, а не объектами ORM.

Прежняя пара fetch_pricelist_data + transform_to_dataframe грузила ради
одиннадцати скалярных полей полные объекты AutoPart (сорок с лишним
колонок), Brand, PriceList и Provider, и всё это оседало в карте
объектов сессии. При сборке прайса клиенту источники складывались один
к другому: память уходила в потолок, ядро убивало процесс, а однажды
перезагрузился сервер.
"""
import pytest
from sqlalchemy import select

from dz_fastapi.crud.partner import crud_pricelist
from dz_fastapi.models.autopart import AutoPart
from dz_fastapi.models.partner import PriceList, PriceListAutoPartAssociation


async def _pricelist_with_rows(
    session, brand, provider, config, rows: list[tuple[str, int, float]]
) -> PriceList:
    pricelist = PriceList(
        provider_id=provider.id, provider_config_id=config.id
    )
    session.add(pricelist)
    await session.flush()
    for oem, quantity, price in rows:
        part = AutoPart(
            brand_id=brand.id, oem_number=oem, name=f'Деталь {oem}'
        )
        session.add(part)
        await session.flush()
        session.add(
            PriceListAutoPartAssociation(
                pricelist_id=pricelist.id,
                autopart_id=part.id,
                quantity=quantity,
                price=price,
            )
        )
    await session.commit()
    return pricelist


@pytest.mark.anyio
async def test_dataframe_has_all_fields_consumers_need(
    test_session, created_brand, created_providers, created_pricelist_config
):
    pricelist = await _pricelist_with_rows(
        test_session,
        created_brand,
        created_providers[0],
        created_pricelist_config,
        [('DF0001', 5, 100.0), ('DF0002', 7, 250.5)],
    )

    frame = await crud_pricelist.fetch_pricelist_dataframe(
        pricelist.id, test_session
    )

    assert set(frame.columns) >= {
        'autopart_id', 'name', 'oem_number', 'brand_id', 'brand',
        'quantity', 'price', 'provider_id', 'provider_config_id',
        'pricelist_id', 'is_own_price',
    }
    assert len(frame) == 2
    row = frame[frame['oem_number'] == 'DF0001'].iloc[0]
    assert row['brand'] == created_brand.name
    assert row['quantity'] == 5
    assert row['price'] == pytest.approx(100.0)
    assert row['provider_id'] == created_providers[0].id
    assert row['pricelist_id'] == pricelist.id


@pytest.mark.anyio
async def test_no_orm_objects_land_in_session(
    test_session, created_brand, created_providers, created_pricelist_config
):
    """Главное свойство: чтение прайса не наполняет карту объектов.

    Именно накопление в ней съедало память, когда источники читались
    один за другим в одной сессии.
    """
    pricelist = await _pricelist_with_rows(
        test_session,
        created_brand,
        created_providers[0],
        created_pricelist_config,
        [('DF0003', 1, 10.0), ('DF0004', 2, 20.0)],
    )
    test_session.expunge_all()

    await crud_pricelist.fetch_pricelist_dataframe(pricelist.id, test_session)

    loaded = [
        obj
        for obj in test_session.identity_map.values()
        if isinstance(obj, (AutoPart, PriceListAutoPartAssociation))
    ]
    assert loaded == []


@pytest.mark.anyio
async def test_filter_by_oem_returns_only_asked_rows(
    test_session, created_brand, created_providers, created_pricelist_config
):
    pricelist = await _pricelist_with_rows(
        test_session,
        created_brand,
        created_providers[0],
        created_pricelist_config,
        [('DF0005', 1, 10.0), ('DF0006', 2, 20.0)],
    )

    frame = await crud_pricelist.fetch_pricelist_dataframe(
        pricelist.id, test_session, oem_numbers={'DF0005'}
    )

    assert list(frame['oem_number']) == ['DF0005']


@pytest.mark.anyio
async def test_empty_oem_filter_returns_empty_frame(
    test_session, created_brand, created_providers, created_pricelist_config
):
    """Пустой список артикулов — это «ничего не нужно», а не «всё»."""
    pricelist = await _pricelist_with_rows(
        test_session,
        created_brand,
        created_providers[0],
        created_pricelist_config,
        [('DF0007', 1, 10.0)],
    )

    frame = await crud_pricelist.fetch_pricelist_dataframe(
        pricelist.id, test_session, oem_numbers=set()
    )

    assert frame.empty
    assert 'oem_number' in frame.columns


@pytest.mark.anyio
async def test_missing_pricelist_gives_empty_frame(test_session):
    missing = (
        await test_session.execute(select(PriceList.id).limit(1))
    ).scalar_one_or_none()
    frame = await crud_pricelist.fetch_pricelist_dataframe(
        (missing or 0) + 10_000, test_session
    )
    assert frame.empty
