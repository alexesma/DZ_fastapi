"""Сопоставление поставщика сайта с поставщиком каталога.

На рабочем сервере накопилось 47 виртуальных дублей, и причин было две.
Сайт присылает «Cosmopart», в каталоге заведён «COSMOPART» — точное
сравнение имён их не сводило. А привязка external_supplier_name
записывалась, но нигде не читалась: функции поиска по ней не было
вовсе. Из-за второго объединение дублей не помогало — перенесённая
привязка ни на что не влияла, и на следующий заказ дубль появлялся
снова.
"""
import pytest

from dz_fastapi.crud.partner import crud_provider
from dz_fastapi.models.partner import TYPE_PRICES, Provider, ProviderExternalReference

SOURCE = 'DRAGONZAP'


async def _provider(session, name: str, *, virtual: bool = False) -> Provider:
    provider = Provider(
        name=name,
        is_virtual=virtual,
        type_prices=TYPE_PRICES.WHOLESALE,
    )
    session.add(provider)
    await session.flush()
    return provider


# ── поиск по имени без учёта регистра ───────────────────────────────────


@pytest.mark.anyio
async def test_finds_provider_ignoring_case(test_session):
    """Ровно случай COSMOPART: сайт пишет иначе, поставщик тот же."""
    await _provider(test_session, 'COSMOPART')
    await test_session.commit()

    found = await crud_provider.get_provider_by_name_insensitive(
        name='Cosmopart', session=test_session
    )
    assert found is not None
    assert found.name == 'COSMOPART'


@pytest.mark.anyio
async def test_finds_provider_ignoring_edge_spaces(test_session):
    await _provider(test_session, 'MIKADO')
    await test_session.commit()

    found = await crud_provider.get_provider_by_name_insensitive(
        name='  mikado  ', session=test_session
    )
    assert found is not None
    assert found.name == 'MIKADO'


@pytest.mark.anyio
async def test_real_provider_wins_over_virtual_duplicate(test_session):
    """Пока дубль не убран, заказ должен идти к настоящему поставщику,
    а не к автоматически созданному."""
    await _provider(test_session, 'ARUDA')
    await _provider(test_session, 'Aruda', virtual=True)
    await test_session.commit()

    found = await crud_provider.get_provider_by_name_insensitive(
        name='aruda', session=test_session
    )
    assert found is not None
    assert found.is_virtual is False


@pytest.mark.anyio
async def test_different_names_do_not_match(test_session):
    """Регистр — не повод считать разные имена одним поставщиком."""
    await _provider(test_session, 'MOTEX')
    await test_session.commit()

    assert (
        await crud_provider.get_provider_by_name_insensitive(
            name='Motex Auto', session=test_session
        )
    ) is None


@pytest.mark.anyio
async def test_empty_name_matches_nothing(test_session):
    await _provider(test_session, 'JAPARTS')
    await test_session.commit()

    assert (
        await crud_provider.get_provider_by_name_insensitive(
            name='   ', session=test_session
        )
    ) is None


# ── поиск по привязке из внешней системы ────────────────────────────────


@pytest.mark.anyio
async def test_reference_by_name_is_readable(test_session):
    """Привязка без external_supplier_id должна работать: сайт id не
    присылает — на рабочем сервере он пуст у всех сорока привязок."""
    provider = await _provider(test_session, 'МЕПАРТ')
    test_session.add(
        ProviderExternalReference(
            provider_id=provider.id,
            source_system=SOURCE,
            external_supplier_id=None,
            external_supplier_name='Mepart(Тихий Океан)',
            is_active=True,
        )
    )
    await test_session.commit()

    reference = await crud_provider.get_external_reference_by_source_name(
        source_system=SOURCE,
        external_supplier_name='mepart(тихий океан)',
        session=test_session,
    )
    assert reference is not None
    assert reference.provider_id == provider.id


@pytest.mark.anyio
async def test_reference_of_other_source_is_ignored(test_session):
    provider = await _provider(test_session, 'ЮНИТ')
    test_session.add(
        ProviderExternalReference(
            provider_id=provider.id,
            source_system='OTHER',
            external_supplier_name='Юнит',
            is_active=True,
        )
    )
    await test_session.commit()

    assert (
        await crud_provider.get_external_reference_by_source_name(
            source_system=SOURCE,
            external_supplier_name='Юнит',
            session=test_session,
        )
    ) is None
