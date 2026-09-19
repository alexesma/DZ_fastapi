"""Место хранения нельзя потерять вместе с остатком.

«Места хранения» в карточке товара — это метка членства, а реальное
количество лежит в StockByLocation и обновляется только складскими
движениями. Раньше карточку можно было сохранить, сняв метку с места,
где физически ещё лежит товар: связь пропадала, а строка StockByLocation
оставалась — невидимая ни в карточке, ни в проверке перед удалением
места (она тоже смотрела на метки). Такое «пустое» место можно было
удалить одним кликом, и по ON DELETE CASCADE вместе с ним исчезал
остаток — без единого движения и без следа.
"""

import pytest
from sqlalchemy import select

from dz_fastapi.models.autopart import AutoPart, StorageLocation
from dz_fastapi.models.inventory import StockByLocation


async def _деталь(session, бренд, oem: str) -> AutoPart:
    деталь = AutoPart(brand_id=бренд.id, oem_number=oem, name="Деталь")
    session.add(деталь)
    await session.flush()
    return деталь


async def _место(session, имя: str) -> StorageLocation:
    место = StorageLocation(name=имя)
    session.add(место)
    await session.flush()
    return место


async def _привязать_с_остатком(session, деталь, место, остаток: int):
    session.add(
        StockByLocation(
            autopart_id=деталь.id,
            storage_location_id=место.id,
            quantity=остаток,
        )
    )
    await session.execute(
        AutoPart.storage_locations.property.secondary.insert().values(
            autopart_id=деталь.id, storage_location_id=место.id
        )
    )
    await session.commit()


@pytest.mark.asyncio
async def test_cannot_unlink_location_with_real_stock(
    async_client, test_session, created_brand
):
    """Снять метку с места, где физически есть остаток, нельзя."""
    деталь = await _деталь(test_session, created_brand, "GUARD0001")
    место = await _место(test_session, "GRD A1")
    await _привязать_с_остатком(test_session, деталь, место, 5)

    ответ = await async_client.patch(
        f"/autoparts/{деталь.id}/update/",
        json={"storage_location_ids": []},
    )

    assert ответ.status_code == 409, ответ.text
    assert "GRD A1" in ответ.json()["detail"]
    assert "5" in ответ.json()["detail"]

    test_session.expunge_all()
    остаток = await test_session.scalar(
        select(StockByLocation.quantity).where(
            StockByLocation.autopart_id == деталь.id
        )
    )
    assert остаток == 5, "остаток не должен теряться при отказе сохранить"


@pytest.mark.asyncio
async def test_can_unlink_location_once_stock_is_zero(
    async_client, test_session, created_brand
):
    """Пустое место снять можно — блокирует только реальный остаток."""
    деталь = await _деталь(test_session, created_brand, "GUARD0002")
    место = await _место(test_session, "GRD A2")
    await _привязать_с_остатком(test_session, деталь, место, 0)

    ответ = await async_client.patch(
        f"/autoparts/{деталь.id}/update/",
        json={"storage_location_ids": []},
    )

    assert ответ.status_code == 200, ответ.text
    assert ответ.json()["storage_locations"] == []


@pytest.mark.asyncio
async def test_can_still_add_a_new_location(
    async_client, test_session, created_brand
):
    """Добавление места по-прежнему работает: блокируется только снятие."""
    деталь = await _деталь(test_session, created_brand, "GUARD0003")
    место = await _место(test_session, "GRD A3")
    await test_session.commit()

    ответ = await async_client.patch(
        f"/autoparts/{деталь.id}/update/",
        json={"storage_location_ids": [место.id]},
    )

    assert ответ.status_code == 200, ответ.text
    assert ответ.json()["storage_locations"] == ["GRD A3"]


@pytest.mark.asyncio
async def test_cannot_delete_location_with_real_stock(
    async_client, test_session, created_brand
):
    """Удаление места блокируется остатком, даже без метки в карточке.

    Метка и остаток могли разойтись до этой правки — проверка при
    удалении не должна полагаться на метку.
    """
    деталь = await _деталь(test_session, created_brand, "GUARD0004")
    место = await _место(test_session, "GRD A4")
    test_session.add(
        StockByLocation(
            autopart_id=деталь.id, storage_location_id=место.id, quantity=3
        )
    )
    await test_session.commit()

    ответ = await async_client.delete(f"/storage/{место.id}/")

    assert ответ.status_code == 409, ответ.text
    assert "3" in ответ.json()["detail"]

    осталось = await test_session.get(StorageLocation, место.id)
    assert осталось is not None


@pytest.mark.asyncio
async def test_can_delete_empty_location(
    async_client, test_session, created_brand
):
    """Место без остатка и без метки удаляется как раньше."""
    место = await _место(test_session, "GRD A5")
    await test_session.commit()

    ответ = await async_client.delete(f"/storage/{место.id}/")

    assert ответ.status_code == 204, ответ.text
    test_session.expunge_all()
    осталось = await test_session.get(StorageLocation, место.id)
    assert осталось is None


@pytest.mark.asyncio
async def test_storage_list_defaults_to_lightweight(
    async_client, test_session, created_brand
):
    """Список мест хранения по умолчанию не тянет запчасти.

    Тот же класс мины, что был у /customers/: полная версия грузит
    каждую запчасть каждого места вместе с категориями и обратной
    связью мест — опасно как умолчание для списка.
    """
    деталь = await _деталь(test_session, created_brand, "GUARD0006")
    место = await _место(test_session, "GRD A6")
    await _привязать_с_остатком(test_session, деталь, место, 1)

    по_умолчанию = await async_client.get("/storage/")
    assert по_умолчанию.status_code == 200, по_умолчанию.text
    найдено = next(
        item for item in по_умолчанию.json() if item["id"] == место.id
    )
    assert найдено["autoparts"] == []

    явно_полный = await async_client.get(
        "/storage/", params={"include_autoparts": True}
    )
    assert явно_полный.status_code == 200, явно_полный.text
    найдено_полный = next(
        item for item in явно_полный.json() if item["id"] == место.id
    )
    assert len(найдено_полный["autoparts"]) == 1
