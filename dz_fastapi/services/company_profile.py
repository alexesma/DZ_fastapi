"""Юридические данные нашей организации для документов и печатных форм.

Источник по приоритету: сохранённый профиль (Настройки → Реквизиты
организации), затем настройки Диадока и переменные окружения SELLER_*.
"""

from __future__ import annotations

import os
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.models.settings import CompanyProfile, DiadocIntegrationSettings

PROFILE_FIELDS = (
    "is_individual_entrepreneur", "short_name", "full_name", "inn", "kpp",
    "ogrn", "ogrn_assigned_at", "okpo", "legal_address", "postal_address",
    "shipping_address", "phone", "email", "website", "bank_name", "bank_bik",
    "bank_account", "bank_corr_account", "director_position", "director_name",
    "director_basis", "accountant_name", "vat_payer",
)


async def get_profile_row(session: AsyncSession) -> CompanyProfile | None:
    return (await session.execute(
        select(CompanyProfile).order_by(CompanyProfile.id).limit(1)
    )).scalar_one_or_none()


async def _diadoc_defaults(session: AsyncSession) -> dict[str, Any]:
    settings = (await session.execute(
        select(DiadocIntegrationSettings).limit(1)
    )).scalar_one_or_none()
    name = (
        getattr(settings, "organization_name", None)
        or os.getenv("SELLER_NAME", "")
    )
    return {
        "short_name": name or None,
        "full_name": name or None,
        "inn": (
            getattr(settings, "organization_inn", None)
            or os.getenv("SELLER_INN", "") or None
        ),
        "kpp": (
            getattr(settings, "organization_kpp", None)
            or os.getenv("SELLER_KPP", "") or None
        ),
        "legal_address": (
            getattr(settings, "seller_legal_address", None)
            or os.getenv("SELLER_ADDRESS", "") or None
        ),
        "postal_address": getattr(settings, "seller_postal_address", None),
        "phone": os.getenv("SELLER_PHONE", "") or None,
        "email": os.getenv("SELLER_EMAIL", "") or None,
        "director_name": getattr(settings, "signer_full_name", None),
        "director_position": getattr(settings, "signer_position", None),
        "director_basis": getattr(settings, "signer_basis", None),
    }


async def get_profile_payload(session: AsyncSession) -> dict[str, Any]:
    """Профиль для формы настроек; без сохранённой записи — предзаполнение."""
    row = await get_profile_row(session)
    if row is not None:
        data = {field: getattr(row, field) for field in PROFILE_FIELDS}
        data["prefilled"] = False
        data["updated_at"] = row.updated_at
        return data
    defaults = await _diadoc_defaults(session)
    data = {field: defaults.get(field) for field in PROFILE_FIELDS}
    data["is_individual_entrepreneur"] = False
    data["vat_payer"] = True
    data["prefilled"] = True
    data["updated_at"] = None
    return data


async def save_profile(session: AsyncSession, values: dict[str, Any]) -> None:
    row = await get_profile_row(session)
    if row is None:
        row = CompanyProfile()
        session.add(row)
    for field in PROFILE_FIELDS:
        if field in values:
            setattr(row, field, values[field])
    await session.commit()


async def load_company_requisites(session: AsyncSession) -> dict[str, Any]:
    """Реквизиты для документов в плоском виде (единые ключи для всех форм)."""
    data = await get_profile_payload(session)
    name = data.get("short_name") or data.get("full_name") or ""
    postal = data.get("postal_address") or data.get("legal_address") or ""
    return {
        "name": name,
        "full_name": data.get("full_name") or name,
        "inn": data.get("inn") or "",
        "kpp": data.get("kpp") or "",
        "ogrn": data.get("ogrn") or "",
        "ogrn_assigned_at": data.get("ogrn_assigned_at"),
        "okpo": data.get("okpo") or "",
        "is_ip": bool(data.get("is_individual_entrepreneur")),
        "address": data.get("legal_address") or "",
        "postal_address": postal,
        "shipping_address": data.get("shipping_address") or postal,
        "phone": data.get("phone") or "",
        "email": data.get("email") or "",
        "website": data.get("website") or "",
        "bank_name": data.get("bank_name") or "",
        "bank_bik": data.get("bank_bik") or "",
        "bank_account": data.get("bank_account") or "",
        "bank_corr_account": data.get("bank_corr_account") or "",
        "signer_position": data.get("director_position") or "Руководитель",
        "signer_name": data.get("director_name") or "",
        "signer_basis": data.get("director_basis") or "",
        "accountant_name": data.get("accountant_name") or "",
        "vat_payer": bool(data.get("vat_payer", True)),
    }
