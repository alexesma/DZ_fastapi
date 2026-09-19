"""Журнал печати товарных этикеток и бирок мест хранения.

Товарные этикетки и бирки мест хранения не хранятся в базе как объекты —
их собирают на лету из поиска по номенклатуре или из имени места. В
отличие от этикеток волны сборки и кросс-докинга у них не было ни следа
печати: отклеилась этикетка или принтер зажевал бумагу — узнать, кто и
когда её печатал, было неоткуда.
"""

import pytest
from sqlalchemy import select

from dz_fastapi.models.autopart import AutoPart, StorageLocation
from dz_fastapi.models.inventory import AdHocLabelPrintEvent
from dz_fastapi.models.user import User, UserRole, UserStatus


async def _создать_пользователя(session):
    """get_current_user в тестах подменён на User(id=1, ...) без записи в БД;
    для полей с FK на app_user эту же строку нужно реально сохранить."""
    session.add(
        User(
            id=1,
            name="Test Admin",
            email="test-admin@example.com",
            password_hash="unused",
            role=UserRole.ADMIN,
            status=UserStatus.ACTIVE,
        )
    )
    await session.commit()


@pytest.mark.asyncio
async def test_product_print_event_is_recorded(async_client, test_session):
    """Печать товарных этикеток пишет снимок всей партии."""
    await _создать_пользователя(test_session)
    ответ = await async_client.post(
        "/inventory/labels/product/print-events",
        json={
            "items": [
                {
                    "brand_name": "CHERY",
                    "oem_number": "AB1234",
                    "name": "Деталь",
                    "barcode": "CHERY AB1234",
                    "copies": 3,
                },
                {
                    "brand_name": "GEELY",
                    "oem_number": "CD5678",
                    "copies": 1,
                },
            ]
        },
    )

    assert ответ.status_code == 200, ответ.text
    данные = ответ.json()
    assert данные["kind"] == "product"
    assert данные["total_labels"] == 4
    assert len(данные["items"]) == 2
    assert данные["printed_by_name"]

    строка = await test_session.scalar(
        select(AdHocLabelPrintEvent).where(
            AdHocLabelPrintEvent.id == данные["id"]
        )
    )
    assert строка is not None
    assert строка.kind == "product"
    assert строка.total_labels == 4


@pytest.mark.asyncio
async def test_location_print_event_links_the_location(
    async_client, test_session
):
    """Печать бирки привязывается к конкретному месту хранения."""
    await _создать_пользователя(test_session)
    место = StorageLocation(name="LOGT1")
    test_session.add(место)
    await test_session.commit()

    ответ = await async_client.post(
        "/inventory/labels/location/print-events",
        json={
            "storage_location_id": место.id,
            "items": [{"name": "LOGT1", "barcode": "LOGT1", "copies": 1}],
        },
    )

    assert ответ.status_code == 200, ответ.text
    данные = ответ.json()
    assert данные["kind"] == "location"
    assert данные["storage_location_id"] == место.id
    assert данные["total_labels"] == 1


@pytest.mark.asyncio
async def test_empty_batch_is_rejected(async_client):
    """Пустая партия — ошибка, а не тихая запись ни о чём."""
    ответ = await async_client.post(
        "/inventory/labels/product/print-events", json={"items": []}
    )
    assert ответ.status_code == 400


@pytest.mark.asyncio
async def test_print_events_are_listed_and_filterable(
    async_client, test_session, created_brand
):
    """Журнал можно прочитать и отфильтровать по виду и позиции."""
    await _создать_пользователя(test_session)
    деталь = AutoPart(brand_id=created_brand.id, oem_number="LOGT2", name="Д")
    test_session.add(деталь)
    await test_session.commit()

    await async_client.post(
        "/inventory/labels/product/print-events",
        json={
            "autopart_id": деталь.id,
            "items": [{"oem_number": "LOGT2", "copies": 2}],
        },
    )
    await async_client.post(
        "/inventory/labels/product/print-events",
        json={"items": [{"oem_number": "OTHER", "copies": 1}]},
    )

    все = await async_client.get("/inventory/labels/print-events")
    assert все.status_code == 200
    assert len(все.json()) >= 2

    только_нужная = await async_client.get(
        "/inventory/labels/print-events",
        params={"autopart_id": деталь.id},
    )
    assert только_нужная.status_code == 200
    assert all(
        row["autopart_id"] == деталь.id for row in только_нужная.json()
    )
    assert len(только_нужная.json()) == 1
