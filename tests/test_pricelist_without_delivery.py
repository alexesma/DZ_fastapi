"""Прайс клиента можно собирать без рассылки.

У АртЗапа не было ни расписания, ни получателей, поэтому прайс не
собирался ни разу. А подбор под заказ подставляет наш аналог вместо
заказанного номера только по алиасам из прайса — и заказ на позицию,
которой у нас семь штук на складе, ушёл в отказ с «нет предложения».

Расписание без получателей — рабочий случай: клиент заказывает через
сайт. Конвейер должен отработать целиком, прайс стать действующим, а
письмо не уходить.
"""

from datetime import date

import pytest
from sqlalchemy import select

from dz_fastapi.models.partner import CustomerPriceList, CustomerPriceListConfig
from dz_fastapi.services.customer_orders import _load_latest_customer_pricelist


async def _конфигурация(session, клиент, имя: str) -> CustomerPriceListConfig:
    """Настоящая конфигурация: без неё срабатывает запасная ветка для
    старых прайсов, заведённых до появления привязки к конфигурации."""
    конфигурация = CustomerPriceListConfig(customer_id=клиент.id, name=имя)
    session.add(конфигурация)
    await session.commit()
    await session.refresh(конфигурация)
    return конфигурация


async def _прайс(
    session,
    клиент,
    конфигурация_id,
    *,
    sent_at=None,
    published_at=None,
    день=None,
) -> CustomerPriceList:
    прайс = CustomerPriceList(
        customer_id=клиент.id,
        customer_config_id=конфигурация_id,
        date=день or date.today(),
        sent_at=sent_at,
        published_at=published_at,
    )
    session.add(прайс)
    await session.commit()
    await session.refresh(прайс)
    return прайс


@pytest.mark.asyncio
async def test_pricelist_built_without_recipients_is_effective(
    test_session, created_customers
):
    """Собранный без рассылки прайс участвует в подборе под заказ."""
    клиент = created_customers[0]
    конфигурация = await _конфигурация(test_session, клиент, "Без рассылки")
    прайс = await _прайс(
        test_session,
        клиент,
        конфигурация_id=конфигурация.id,
        published_at=date.today(),
    )

    найденный = await _load_latest_customer_pricelist(
        test_session, клиент.id, конфигурация.id
    )

    assert найденный is not None, "прайс без рассылки снова не виден подбору"
    assert найденный.id == прайс.id


@pytest.mark.asyncio
async def test_sent_pricelist_still_found(test_session, created_customers):
    """Старые строки знают только sent_at — они должны работать как раньше."""
    клиент = created_customers[1]
    конфигурация = await _конфигурация(test_session, клиент, "С рассылкой")
    прайс = await _прайс(
        test_session,
        клиент,
        конфигурация_id=конфигурация.id,
        sent_at=date.today(),
    )

    найденный = await _load_latest_customer_pricelist(
        test_session, клиент.id, конфигурация.id
    )

    assert найденный is not None
    assert найденный.id == прайс.id


@pytest.mark.asyncio
async def test_pricelist_without_any_mark_is_ignored(
    test_session, created_customers
):
    """Черновик без отметок в подбор не идёт.

    Иначе клиенту можно было бы пообещать цену из файла, который ему не
    показывали и который ещё не стал действующим.
    """
    клиент = created_customers[0]
    конфигурация = await _конфигурация(test_session, клиент, "Черновик")
    await _прайс(test_session, клиент, конфигурация_id=конфигурация.id)

    найденный = await _load_latest_customer_pricelist(
        test_session, клиент.id, конфигурация.id
    )

    assert найденный is None


@pytest.mark.asyncio
async def test_published_at_is_stored(test_session, created_customers):
    """Отметка действительно сохраняется в базе."""
    клиент = created_customers[0]
    конфигурация = await _конфигурация(test_session, клиент, "Хранение")
    прайс = await _прайс(
        test_session,
        клиент,
        конфигурация_id=конфигурация.id,
        published_at=date.today(),
    )

    test_session.expunge_all()
    из_базы = await test_session.scalar(
        select(CustomerPriceList).where(CustomerPriceList.id == прайс.id)
    )

    assert из_базы.published_at is not None
    assert из_базы.sent_at is None
