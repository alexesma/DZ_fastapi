from datetime import date, timedelta

import pytest
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.api.dashboard import (
    _load_pair_stats_batch,
    get_supplier_price_trends,
    get_supplier_pricelist_health,
)
from dz_fastapi.models.autopart import AutoPart
from dz_fastapi.models.partner import (
    PriceList,
    PriceListAutoPartAssociation,
    Provider,
    ProviderPriceListConfig,
)


@pytest.mark.asyncio
async def test_pair_stats_batch_calculates_price_change_without_parallel_shm(
    test_session: AsyncSession,
    created_providers: list[Provider],
    created_pricelist_config: ProviderPriceListConfig,
    created_autopart: AutoPart,
):
    provider = created_providers[0]
    previous = PriceList(
        provider_id=provider.id,
        provider_config_id=created_pricelist_config.id,
        date=date.today(),
        is_active=True,
    )
    current = PriceList(
        provider_id=provider.id,
        provider_config_id=created_pricelist_config.id,
        date=date.today(),
        is_active=True,
    )
    test_session.add_all([previous, current])
    await test_session.flush()
    test_session.add_all(
        [
            PriceListAutoPartAssociation(
                pricelist_id=previous.id,
                autopart_id=created_autopart.id,
                quantity=5,
                price=100,
                multiplicity=1,
            ),
            PriceListAutoPartAssociation(
                pricelist_id=current.id,
                autopart_id=created_autopart.id,
                quantity=5,
                price=110,
                multiplicity=1,
            ),
        ]
    )
    await test_session.commit()

    result = await _load_pair_stats_batch(
        test_session,
        {(previous.id, current.id)},
    )

    overlap, median_pct, changed_share_pct = result[(previous.id, current.id)]
    assert overlap == 1
    assert median_pct == pytest.approx(10.0)
    assert changed_share_pct == 100.0


@pytest.mark.asyncio
async def test_supplier_pricelist_health_reports_stale_and_extended_sources(
    test_session: AsyncSession,
    created_providers: list[Provider],
    created_pricelist_config: ProviderPriceListConfig,
):
    from datetime import timedelta

    from dz_fastapi.core.time import now_moscow

    config = created_pricelist_config
    config.max_days_without_update = 1
    pricelist = PriceList(
        provider_id=created_providers[0].id,
        provider_config_id=config.id,
        date=now_moscow().date() - timedelta(days=5),
        is_active=True,
    )
    test_session.add_all([config, pricelist])
    await test_session.commit()

    stale_result = await get_supplier_pricelist_health(session=test_session)
    stale_item = next(
        item for item in stale_result.items if item.provider_config_id == config.id
    )
    assert stale_item.status == "stale"
    assert stale_result.summary.stale >= 1

    config.stale_override_until = now_moscow() + timedelta(days=1)
    test_session.add(config)
    await test_session.commit()
    extended_result = await get_supplier_pricelist_health(session=test_session)
    extended_item = next(
        item for item in extended_result.items if item.provider_config_id == config.id
    )
    assert extended_item.status == "extended"


async def _прайс_с_позициями(
    session, provider, config, бренд, день, позиции: list[tuple[int, float]]
) -> PriceList:
    """Прайс с заданными остатками и ценами; деталь на каждую позицию своя."""
    прайс = PriceList(
        provider_id=provider.id,
        provider_config_id=config.id,
        date=день,
        is_active=True,
    )
    session.add(прайс)
    await session.flush()
    for номер, (остаток, цена) in enumerate(позиции):
        деталь = AutoPart(
            brand_id=бренд.id,
            oem_number=f'TREND{прайс.id}-{номер}',
            name=f'Деталь {номер}',
        )
        session.add(деталь)
        await session.flush()
        session.add(
            PriceListAutoPartAssociation(
                pricelist_id=прайс.id,
                autopart_id=деталь.id,
                quantity=остаток,
                price=цена,
                multiplicity=1,
            )
        )
    await session.commit()
    return прайс


@pytest.mark.asyncio
async def test_trends_keep_one_pricelist_per_day(
    test_session: AsyncSession,
    created_providers: list[Provider],
    created_pricelist_config: ProviderPriceListConfig,
    created_brand,
):
    """Несколько загрузок за день — одна точка, последняя.

    Поставщик присылает файл по нескольку раз в сутки (у Кунцево было три
    загрузки за день). Прежде каждая становилась отдельной точкой: график
    громоздил их на одну дату, а агрегаты считались по всем прайсам.
    """
    provider = created_providers[0]
    сегодня = date.today()
    вчера = сегодня - timedelta(days=1)

    await _прайс_с_позициями(
        test_session, provider, created_pricelist_config, created_brand,
        вчера, [(5, 100.0)]
    )
    await _прайс_с_позициями(
        test_session, provider, created_pricelist_config, created_brand,
        сегодня, [(5, 100.0)]
    )
    последний = await _прайс_с_позициями(
        test_session,
        provider,
        created_pricelist_config,
        created_brand,
        сегодня,
        [(7, 150.0), (3, 250.0)],
    )

    ответ = await get_supplier_price_trends(
        days=30,
        points_limit=10,
        smooth_window=3,
        provider_config_ids=[created_pricelist_config.id],
        session=test_session,
    )

    серия = next(
        item for item in ответ.series
        if item.provider_config_id == created_pricelist_config.id
    )
    # Два дня — две точки, а не три загрузки.
    assert [точка.date for точка in серия.points] == [вчера, сегодня]
    # За сегодня взят последний прайс: в нём две позиции, а не одна.
    assert серия.points[-1].pricelist_id == последний.id
    assert серия.points[-1].total_sku_count == 2


@pytest.mark.asyncio
async def test_pricelist_metrics_are_computed_once(
    test_session: AsyncSession,
    created_providers: list[Provider],
    created_pricelist_config: ProviderPriceListConfig,
    created_brand,
):
    """Показатели прайса считаются один раз и берутся из кэша.

    Пересчёт шёл по 9,4 млн строк таблицы связей при каждом открытии
    дашборда и не укладывался в таймаут браузера.
    """
    from dz_fastapi.api.dashboard import _load_pricelist_metrics
    from dz_fastapi.models.partner import PriceListMetricCache

    прайс = await _прайс_с_позициями(
        test_session,
        created_providers[0],
        created_pricelist_config,
        created_brand,
        date.today(),
        [(4, 100.0), (6, 200.0), (0, 999.0)],
    )

    первый = await _load_pricelist_metrics(test_session, [прайс.id])

    assert первый[прайс.id]['total_sku_count'] == 3
    assert первый[прайс.id]['sku_count'] == 2
    assert первый[прайс.id]['stock_total_qty'] == 10
    assert первый[прайс.id]['avg_price'] == pytest.approx(150.0)

    запомнено = (
        await test_session.execute(
            select(PriceListMetricCache).where(
                PriceListMetricCache.pricelist_id == прайс.id
            )
        )
    ).scalars().all()
    assert len(запомнено) == 1

    # Убираем строки прайса: если значения возьмутся из кэша, ответ
    # не изменится — а пересчёт дал бы нули.
    await test_session.execute(
        delete(PriceListAutoPartAssociation).where(
            PriceListAutoPartAssociation.pricelist_id == прайс.id
        )
    )
    await test_session.commit()

    второй = await _load_pricelist_metrics(test_session, [прайс.id])

    assert второй == первый


@pytest.mark.asyncio
async def test_empty_pricelist_metrics_are_remembered_too(
    test_session: AsyncSession,
    created_providers: list[Provider],
    created_pricelist_config: ProviderPriceListConfig,
):
    """Пустой прайс тоже запоминаем, иначе он пересчитывается всегда."""
    from dz_fastapi.api.dashboard import _load_pricelist_metrics
    from dz_fastapi.models.partner import PriceListMetricCache

    пустой = PriceList(
        provider_id=created_providers[0].id,
        provider_config_id=created_pricelist_config.id,
        date=date.today(),
        is_active=True,
    )
    test_session.add(пустой)
    await test_session.commit()

    показатели = await _load_pricelist_metrics(test_session, [пустой.id])

    assert показатели[пустой.id]['total_sku_count'] == 0
    assert показатели[пустой.id]['avg_price'] is None
    запомнено = (
        await test_session.execute(
            select(PriceListMetricCache).where(
                PriceListMetricCache.pricelist_id == пустой.id
            )
        )
    ).scalars().all()
    assert len(запомнено) == 1


@pytest.mark.asyncio
async def test_pair_stats_are_computed_once(
    test_session: AsyncSession,
    created_providers: list[Provider],
    created_pricelist_config: ProviderPriceListConfig,
    created_autopart: AutoPart,
):
    """Сравнение пары прайсов тоже считается один раз.

    Это была самая дорогая часть раздела: попарное соединение таблицы
    связей шло пачками по восемь, то есть десятками последовательных
    тяжёлых запросов.
    """
    from dz_fastapi.models.partner import PriceListPairStatCache

    provider = created_providers[0]
    прошлый = PriceList(
        provider_id=provider.id,
        provider_config_id=created_pricelist_config.id,
        date=date.today(),
        is_active=True,
    )
    текущий = PriceList(
        provider_id=provider.id,
        provider_config_id=created_pricelist_config.id,
        date=date.today(),
        is_active=True,
    )
    test_session.add_all([прошлый, текущий])
    await test_session.flush()
    test_session.add_all(
        [
            PriceListAutoPartAssociation(
                pricelist_id=прошлый.id,
                autopart_id=created_autopart.id,
                quantity=5,
                price=100,
                multiplicity=1,
            ),
            PriceListAutoPartAssociation(
                pricelist_id=текущий.id,
                autopart_id=created_autopart.id,
                quantity=5,
                price=110,
                multiplicity=1,
            ),
        ]
    )
    await test_session.commit()

    пара = (прошлый.id, текущий.id)
    первый = await _load_pair_stats_batch(test_session, {пара})
    assert первый[пара] == (1, pytest.approx(10.0), 100.0)

    запомнено = (
        await test_session.execute(
            select(PriceListPairStatCache).where(
                PriceListPairStatCache.prev_pricelist_id == прошлый.id
            )
        )
    ).scalars().all()
    assert len(запомнено) == 1

    await test_session.execute(
        delete(PriceListAutoPartAssociation).where(
            PriceListAutoPartAssociation.pricelist_id.in_(
                [прошлый.id, текущий.id]
            )
        )
    )
    await test_session.commit()

    второй = await _load_pair_stats_batch(test_session, {пара})
    assert второй[пара] == первый[пара]


@pytest.mark.asyncio
async def test_cached_metrics_match_freshly_computed(
    test_session: AsyncSession,
    created_providers: list[Provider],
    created_pricelist_config: ProviderPriceListConfig,
    created_brand,
):
    """Кэш обязан отдавать ровно те же числа, что и пересчёт.

    Сначала avg_price хранился как DECIMAL(14, 4) и терял точность на
    четвёртом знаке: кэшированный ответ отличался от посчитанного.
    """
    from dz_fastapi.api.dashboard import _load_pricelist_metrics
    from dz_fastapi.models.partner import PriceListMetricCache

    # Цены подобраны так, чтобы среднее было непериодической дробью.
    прайс = await _прайс_с_позициями(
        test_session,
        created_providers[0],
        created_pricelist_config,
        created_brand,
        date.today(),
        [(1, 100.01), (1, 200.02), (1, 300.07)],
    )

    посчитано = await _load_pricelist_metrics(test_session, [прайс.id])
    среднее = посчитано[прайс.id]['avg_price']
    assert среднее is not None

    test_session.expunge_all()
    из_кэша = await _load_pricelist_metrics(test_session, [прайс.id])

    assert из_кэша[прайс.id]['avg_price'] == среднее
    assert из_кэша == посчитано
    # И убеждаемся, что значение действительно пришло из кэша.
    записи = (
        await test_session.execute(
            select(PriceListMetricCache).where(
                PriceListMetricCache.pricelist_id == прайс.id
            )
        )
    ).scalars().all()
    assert len(записи) == 1
