"""Сверка сертификатов с реестром: разбор ссылки, ответа и годности.

Реестр ФГИС недоступен из среды разработки, поэтому разбор ответа
проверяется на синтетических данных. Смысл в том, чтобы незнакомый ответ
не ронял сверку и не приводил к записи мусора в срок действия.
"""
from datetime import date, timedelta

import pytest

from dz_fastapi.models.autopart import AutoPart
from dz_fastapi.models.certificate import Certificate, autopart_certificate_association
from dz_fastapi.services.registry_lookup import extract_registry_fields, parse_registry_reference
from dz_fastapi.services.regulatory import refresh_autopart_certificate_cache
from dz_fastapi.services.utils import is_certificate_usable

# ── разбор ссылки на карточку реестра ───────────────────────────────────


def test_certificate_link_parsed():
    assert parse_registry_reference(
        'https://pub.fsa.gov.ru/rss/certificate/view/3246778/baseInfo'
    ) == ('rss', '3246778')


def test_declaration_link_parsed():
    assert parse_registry_reference(
        'https://pub.fsa.gov.ru/rds/declaration/view/21352747/common'
    ) == ('rds', '21352747')


@pytest.mark.parametrize(
    'url',
    [
        None,
        '',
        'https://tsouz.belgiss.by/#!/tsouz/certifs/3224079/view',
        'https://swis.trade.kg/Doc/76dd818a',
    ],
)
def test_other_registries_not_parsed(url):
    """Ссылки других реестров пропускаем: их API мы не знаем."""
    assert parse_registry_reference(url) is None


# ── разбор ответа реестра ───────────────────────────────────────────────


def test_fields_extracted_from_nested_payload():
    payload = {
        'result': {
            'certInfo': {'certRegDate': '2023-04-17', 'certEndDate': '2028-04-16'},
            'status': {'name': 'Действует'},
        }
    }
    assert extract_registry_fields(payload) == {
        'valid_from': date(2023, 4, 17),
        'valid_until': date(2028, 4, 16),
        'status': 'active',
    }


def test_russian_date_format_and_suspended_status():
    payload = {'declRegDate': '17.04.2023', 'statusName': 'Приостановлен'}
    fields = extract_registry_fields(payload)
    assert fields['valid_from'] == date(2023, 4, 17)
    assert fields['status'] == 'suspended'


def test_unknown_payload_yields_nothing():
    """Незнакомая структура не должна давать выдуманных дат."""
    assert extract_registry_fields({'foo': 'bar', 'items': [1, 2, 3]}) == {}
    assert extract_registry_fields(None) == {}


# ── годность документа ──────────────────────────────────────────────────


def test_future_start_is_not_usable():
    tomorrow = date.today() + timedelta(days=1)
    assert is_certificate_usable(valid_from=tomorrow) is False


def test_suspended_is_not_usable_even_within_term():
    assert (
        is_certificate_usable(
            valid_until=date.today() + timedelta(days=365),
            status='suspended',
        )
        is False
    )


def test_empty_dates_and_status_stay_usable():
    """Пустые значения означают «в реестре не сверялись»: 545 документов
    из прайсов поставщиков пришли без единой даты."""
    assert is_certificate_usable() is True


@pytest.mark.anyio
async def test_cache_drops_certificate_stopped_by_registry(
    test_session, created_brand
):
    """Реестр сообщил, что документ прекращён: номер обязан уйти из
    карточки, иначе он останется в прайсе."""
    part = AutoPart(
        brand_id=created_brand.id,
        oem_number='REG0001',
        name='Фильтр',
        certification_required=True,
    )
    certificate = Certificate(
        number='ЕАЭС RU С-BE.НВ07.В.00826/23',
        status='terminated',
        source='registry',
    )
    test_session.add_all([part, certificate])
    await test_session.flush()
    await test_session.execute(
        autopart_certificate_association.insert().values(
            autopart_id=part.id, certificate_id=certificate.id
        )
    )
    part.eac_cert_number = certificate.number
    await test_session.commit()

    await refresh_autopart_certificate_cache(test_session, [part.id])

    await test_session.refresh(part)
    assert part.eac_cert_number is None
    # Признак остаётся: товар по-прежнему подлежит сертификации,
    # просто действующего документа на него сейчас нет.
    assert part.certification_required is True
