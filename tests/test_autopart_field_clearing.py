"""Очистка полей карточки через обновление.

Раньше update_full пропускал None, и ошибочно введённый номер сертификата
нельзя было стереть — только заменить другим. Поведение изменено, поэтому
закрепляем обе стороны: очищаемые поля обнуляются, обязательные — нет.
"""
import pytest

from dz_fastapi.crud.autopart import crud_autopart


@pytest.mark.asyncio
async def test_none_clears_optional_field(test_session, created_autopart):
    await crud_autopart.update_full(
        test_session,
        created_autopart,
        {"eac_cert_number": "ЕАЭС RU С-BE.НВ07.В.00826/23"},
    )
    assert created_autopart.eac_cert_number is not None

    await crud_autopart.update_full(
        test_session, created_autopart, {"eac_cert_number": None}
    )
    assert created_autopart.eac_cert_number is None


@pytest.mark.asyncio
async def test_none_does_not_clear_required_field(
    test_session, created_autopart
):
    original_name = created_autopart.name
    original_oem = created_autopart.oem_number

    await crud_autopart.update_full(
        test_session,
        created_autopart,
        {"name": None, "oem_number": None, "brand_id": None},
    )
    # NOT NULL в модели: очистка свалила бы запрос на уровне БД.
    assert created_autopart.name == original_name
    assert created_autopart.oem_number == original_oem
    assert created_autopart.brand_id is not None


@pytest.mark.asyncio
async def test_absent_key_leaves_value_untouched(
    test_session, created_autopart
):
    await crud_autopart.update_full(
        test_session, created_autopart, {"tnved_code": "8708801000"}
    )
    # Ключа нет в payload — значение не трогаем (exclude_unset на схеме).
    await crud_autopart.update_full(
        test_session, created_autopart, {"okpd2_code": "29.32.30.390"}
    )
    assert created_autopart.tnved_code == "8708801000"
    assert created_autopart.okpd2_code == "29.32.30.390"


@pytest.mark.asyncio
async def test_false_is_not_treated_as_empty(test_session, created_autopart):
    """False — валидное значение «не требует», а не «не передано»."""
    await crud_autopart.update_full(
        test_session, created_autopart, {"certification_required": False}
    )
    assert created_autopart.certification_required is False
