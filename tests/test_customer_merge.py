"""Объединение дублей клиентов.

Карточку клиента заводят руками, и она же приезжает с сайта — получаются
два лица одного клиента. У одной карточки заполнены реквизиты, у другой
банк и телефоны, и заказы разъезжаются по обеим. Объединение переносит
связанные записи на основную карточку и заполняет её пустые поля из
дубля, ничего не теряя.

Тот же код работает при сверке заказов Parts-Soft — держать две копии
логики нельзя, они разойдутся.
"""

import pytest
from sqlalchemy import select

from dz_fastapi.models.partner import Client, Customer, CustomerOrder, CustomerReclamationEmail


async def _клиент(session, **поля) -> Customer:
    клиент = Customer(**поля)
    session.add(клиент)
    await session.commit()
    await session.refresh(клиент)
    return клиент


@pytest.mark.asyncio
async def test_merge_moves_orders_and_keeps_data(async_client, test_session):
    """Заказы переезжают, пустые поля основной карточки заполняются."""
    основной = await _клиент(
        test_session,
        name="Клиент с сайта",
        type_prices="Wholesale",
        inn="7701234567",
    )
    дубль = await _клиент(
        test_session,
        name="Клиент заведён руками",
        type_prices="Wholesale",
        legal_name="ООО «Эволюция»",
        kpp="772401001",
        phone="+7 495 000-00-00",
        comment="Возит сам",
    )
    заказ = CustomerOrder(customer_id=дубль.id, status="NEW")
    test_session.add(заказ)
    await test_session.commit()
    заказ_id = заказ.id

    ответ = await async_client.post(
        f"/customers/{основной.id}/merge",
        json={"source_customer_id": дубль.id},
    )

    assert ответ.status_code == 200, ответ.text
    assert ответ.json() == {
        "merged": True,
        "source_customer_id": дубль.id,
        "target_customer_id": основной.id,
    }

    test_session.expunge_all()
    # Заказ переехал на основную карточку.
    переехавший = await test_session.get(CustomerOrder, заказ_id)
    assert переехавший.customer_id == основной.id

    # Данные дубля перенесены, своё не затёрто.
    остался = await test_session.get(Customer, основной.id)
    assert остался.inn == "7701234567"
    assert остался.legal_name == "ООО «Эволюция»"
    assert остался.kpp == "772401001"
    assert остался.phone == "+7 495 000-00-00"
    assert остался.comment == "Возит сам"
    assert остался.name == "Клиент с сайта"

    # Дубль удалён из обеих таблиц наследования.
    assert await test_session.get(Customer, дубль.id) is None
    осталось_в_client = await test_session.scalar(select(Client.id).where(Client.id == дубль.id))
    assert осталось_в_client is None


@pytest.mark.asyncio
async def test_merge_does_not_overwrite_filled_fields(async_client, test_session):
    """Заполненное у основной карточки остаётся как есть."""
    основной = await _клиент(
        test_session,
        name="Основной",
        type_prices="Wholesale",
        inn="7701234567",
        comment="Правильный комментарий",
    )
    дубль = await _клиент(
        test_session,
        name="Дубль",
        type_prices="Wholesale",
        inn="9999999999",
        comment="Старый комментарий",
    )

    ответ = await async_client.post(
        f"/customers/{основной.id}/merge",
        json={"source_customer_id": дубль.id},
    )

    assert ответ.status_code == 200, ответ.text
    test_session.expunge_all()
    остался = await test_session.get(Customer, основной.id)
    assert остался.inn == "7701234567"
    assert остался.comment == "Правильный комментарий"


@pytest.mark.asyncio
async def test_merge_moves_unique_contact_email(async_client, test_session):
    """Почта дубля сначала освобождается, затем переносится в основную карточку."""
    основной = await _клиент(
        test_session,
        name="Основной без почты",
        type_prices="Wholesale",
    )
    дубль = await _клиент(
        test_session,
        name="Дубль с почтой",
        type_prices="Wholesale",
        email_contact="orders@example.com",
    )

    ответ = await async_client.post(
        f"/customers/{основной.id}/merge",
        json={"source_customer_id": дубль.id},
    )

    assert ответ.status_code == 200, ответ.text
    test_session.expunge_all()
    остался = await test_session.get(Customer, основной.id)
    assert остался.email_contact == "orders@example.com"
    assert await test_session.get(Customer, дубль.id) is None


@pytest.mark.asyncio
async def test_merge_survives_shared_reclamation_email(async_client, test_session):
    """Одинаковая почта рекламаций у дублей не ломает объединение.

    Единственное уникальное ограничение с customer_id — пара «клиент +
    почта». У дублей почта обычно одна и та же, и без снятия повтора
    перенос падал бы на нарушении уникальности.
    """
    основной = await _клиент(test_session, name="Основной клиент", type_prices="Wholesale")
    дубль = await _клиент(test_session, name="Дубль клиента", type_prices="Wholesale")
    test_session.add_all(
        [
            CustomerReclamationEmail(customer_id=основной.id, email="claims@example.com"),
            CustomerReclamationEmail(customer_id=дубль.id, email="claims@example.com"),
            CustomerReclamationEmail(customer_id=дубль.id, email="second@example.com"),
        ]
    )
    await test_session.commit()

    ответ = await async_client.post(
        f"/customers/{основной.id}/merge",
        json={"source_customer_id": дубль.id},
    )

    assert ответ.status_code == 200, ответ.text
    test_session.expunge_all()
    почты = sorted(
        (
            await test_session.execute(
                select(CustomerReclamationEmail.email).where(
                    CustomerReclamationEmail.customer_id == основной.id
                )
            )
        )
        .scalars()
        .all()
    )
    # Повтор снят, уникальный адрес дубля сохранён.
    assert почты == ["claims@example.com", "second@example.com"]


@pytest.mark.asyncio
async def test_merge_rejects_same_customer(async_client, test_session):
    клиент = await _клиент(test_session, name="Сам с собой", type_prices="Wholesale")

    ответ = await async_client.post(
        f"/customers/{клиент.id}/merge",
        json={"source_customer_id": клиент.id},
    )

    assert ответ.status_code == 400
    assert "саму с собой" in ответ.json()["detail"]


@pytest.mark.asyncio
async def test_merge_reports_missing_customers(async_client, test_session):
    клиент = await _клиент(test_session, name="Единственный", type_prices="Wholesale")

    нет_дубля = await async_client.post(
        f"/customers/{клиент.id}/merge",
        json={"source_customer_id": 99999999},
    )
    assert нет_дубля.status_code == 404

    нет_основного = await async_client.post(
        "/customers/99999999/merge",
        json={"source_customer_id": клиент.id},
    )
    assert нет_основного.status_code == 404
