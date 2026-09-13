"""Наличие позиции и её аналогов одним запросом.

Чтобы понять, есть ли замена, приходилось открывать карточку каждого
аналога по очереди. Ручка отдаёт сразу и предложения поставщиков по
самой позиции, и наличие по каждому аналогу — у нас и у поставщиков.
"""

from datetime import date, timedelta

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.models.autopart import (
    AutoPart,
    StorageLocation,
    autopart_storage_association,
)
from dz_fastapi.models.cross import AutoPartCross
from dz_fastapi.models.partner import (
    PriceList,
    PriceListAutoPartAssociation,
    Provider,
    ProviderPriceListConfig,
)


async def _деталь(session, бренд, oem: str, имя: str = "Деталь") -> AutoPart:
    деталь = AutoPart(brand_id=бренд.id, oem_number=oem, name=имя)
    session.add(деталь)
    await session.flush()
    return деталь


async def _прайс(
    session,
    поставщик,
    конфигурация,
    день: date,
    строки: list[tuple[AutoPart, int, float]],
) -> PriceList:
    прайс = PriceList(
        provider_id=поставщик.id,
        provider_config_id=конфигурация.id if конфигурация else None,
        date=день,
        is_active=True,
    )
    session.add(прайс)
    await session.flush()
    for деталь, остаток, цена in строки:
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
async def test_availability_uses_only_latest_pricelist(
    async_client,
    test_session: AsyncSession,
    created_brand,
    created_providers: list[Provider],
    created_pricelist_config: ProviderPriceListConfig,
):
    """Старые загрузки не учитываются.

    Поставщик присылает файл по нескольку раз, и прежние прайсы остаются
    в базе. Если считать по всем сразу, наличие завысится в разы.
    """
    деталь = await _деталь(test_session, created_brand, "AVAIL0001")
    поставщик = created_providers[0]
    поставщик.is_own_price = False
    await test_session.commit()

    вчера = date.today() - timedelta(days=1)
    await _прайс(
        test_session,
        поставщик,
        created_pricelist_config,
        вчера,
        [(деталь, 99, 100.0)],
    )
    await _прайс(
        test_session,
        поставщик,
        created_pricelist_config,
        date.today(),
        [(деталь, 4, 250.0)],
    )

    ответ = await async_client.get(f"/autoparts/{деталь.id}/availability/")

    assert ответ.status_code == 200, ответ.text
    данные = ответ.json()
    assert len(данные["offers"]) == 1
    предложение = данные["offers"][0]
    assert предложение["quantity"] == 4
    assert предложение["price"] == pytest.approx(250.0)
    assert данные["item"]["supplier_quantity"] == 4
    assert данные["item"]["best_price"] == pytest.approx(250.0)
    assert данные["item"]["suppliers_count"] == 1


@pytest.mark.asyncio
async def test_availability_separates_own_stock_from_suppliers(
    async_client,
    test_session: AsyncSession,
    created_brand,
    created_providers: list[Provider],
    created_pricelist_config: ProviderPriceListConfig,
):
    """Свой склад приходит тем же прайсом, но считается отдельно."""
    деталь = await _деталь(test_session, created_brand, "AVAIL0002")
    свой = created_providers[0]
    свой.is_own_price = True
    чужой = created_providers[1]
    чужой.is_own_price = False
    # Имя места хранения база ограничивает: ^[A-Z0-9 /]+$
    место = StorageLocation(name="RACK A1")
    test_session.add(место)
    await test_session.flush()
    # Через связь напрямую: обращение к отношению в асинхронной сессии
    # вызвало бы ленивую подгрузку.
    await test_session.execute(
        autopart_storage_association.insert().values(
            autopart_id=деталь.id, storage_location_id=место.id
        )
    )
    await test_session.commit()

    await _прайс(
        test_session,
        свой,
        created_pricelist_config,
        date.today(),
        [(деталь, 7, 900.0)],
    )
    await _прайс(
        test_session, чужой, None, date.today(), [(деталь, 3, 800.0)]
    )

    ответ = await async_client.get(f"/autoparts/{деталь.id}/availability/")

    assert ответ.status_code == 200, ответ.text
    позиция = ответ.json()["item"]
    assert позиция["own_quantity"] == 7
    assert позиция["supplier_quantity"] == 3
    assert позиция["suppliers_count"] == 1
    # Свой склад в лучшую цену поставщиков не попадает.
    assert позиция["best_price"] == pytest.approx(800.0)
    assert позиция["storage_locations"] == ["RACK A1"]


@pytest.mark.asyncio
async def test_availability_reports_crosses_with_stock(
    async_client,
    test_session: AsyncSession,
    created_brand,
    created_providers: list[Provider],
    created_pricelist_config: ProviderPriceListConfig,
):
    """По каждому аналогу видно, есть ли он и почём."""
    деталь = await _деталь(test_session, created_brand, "AVAIL0003")
    аналог = await _деталь(
        test_session, created_brand, "AVAIL0003A", "Аналог в наличии"
    )
    поставщик = created_providers[0]
    поставщик.is_own_price = False
    await test_session.commit()
    await _прайс(
        test_session,
        поставщик,
        created_pricelist_config,
        date.today(),
        [(аналог, 12, 555.5)],
    )
    test_session.add_all(
        [
            AutoPartCross(
                source_autopart_id=деталь.id,
                cross_brand_id=created_brand.id,
                cross_oem_number=аналог.oem_number,
                cross_autopart_id=аналог.id,
                is_bidirectional=True,
                priority=10,
            ),
            # Аналог, которого нет в номенклатуре: наличие неизвестно,
            # но сам номер показать надо.
            AutoPartCross(
                source_autopart_id=деталь.id,
                cross_brand_id=created_brand.id,
                cross_oem_number="AVAIL0003B",
                is_bidirectional=False,
                priority=20,
            ),
        ]
    )
    await test_session.commit()

    ответ = await async_client.get(f"/autoparts/{деталь.id}/availability/")

    assert ответ.status_code == 200, ответ.text
    аналоги = ответ.json()["crosses"]
    assert [item["oem_number"] for item in аналоги] == [
        "AVAIL0003A",
        "AVAIL0003B",
    ]

    известный = аналоги[0]
    assert известный["autopart_id"] == аналог.id
    assert известный["supplier_quantity"] == 12
    assert известный["best_price"] == pytest.approx(555.5)
    assert известный["suppliers_count"] == 1
    assert известный["is_bidirectional"] is True
    assert известный["priority"] == 10

    неизвестный = аналоги[1]
    assert неизвестный["autopart_id"] is None
    assert неизвестный["supplier_quantity"] == 0
    assert неизвестный["best_price"] is None


@pytest.mark.asyncio
async def test_availability_skips_zero_quantity(
    async_client,
    test_session: AsyncSession,
    created_brand,
    created_providers: list[Provider],
    created_pricelist_config: ProviderPriceListConfig,
):
    """Нулевой остаток — не предложение."""
    деталь = await _деталь(test_session, created_brand, "AVAIL0004")
    поставщик = created_providers[0]
    поставщик.is_own_price = False
    await test_session.commit()
    await _прайс(
        test_session,
        поставщик,
        created_pricelist_config,
        date.today(),
        [(деталь, 0, 700.0)],
    )

    ответ = await async_client.get(f"/autoparts/{деталь.id}/availability/")

    assert ответ.status_code == 200, ответ.text
    данные = ответ.json()
    assert данные["offers"] == []
    assert данные["item"]["supplier_quantity"] == 0
    assert данные["item"]["best_price"] is None


@pytest.mark.asyncio
async def test_availability_404_for_unknown_part(async_client):
    ответ = await async_client.get("/autoparts/99999999/availability/")
    assert ответ.status_code == 404
